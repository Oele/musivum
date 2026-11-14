// Ported from pion/turn, server.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust, on tokio.
// - `NewServer` gives an `Arc` and starts its loops as tasks (`go::time::spawn`, on the
//   runtime of the caller or the library's own), as the original starts goroutines; each loop
//   holds the server until it ends, which `Close` makes it do.
// - `readListener`'s goroutine per connection is a task: the TLS handshake of a TLS connection
//   is made first, within ten seconds, as there; the connection is read as a `StunConn`, and the
//   allocation of its 5-tuple deleted when it ends (Go's `conn.Close` is the close of the
//   `StunConn` and the drop of the connection).
// - `allocationManagers` is guarded by a lock: the original appends to it in `NewServer` only.
// - `nilAddressGenerator` has no `AllocateListener` and `AllocateConn` (see
//   `server_config.rs`).

use std::sync::Arc;

use parking_lot::Mutex;

use super::internal::allocation::{
    self, BoxError, EventHandler, FiveTuple, Manager, ManagerConfig, Protocol,
};
use super::internal::auth::AuthHandler;
use super::internal::proto::{DEFAULT_LIFETIME_GO, StunConn};
use super::internal::server::{self, NonceManager, Request, new_short_nonce_hash};
use super::net::{Addr, ConnectionState, Listener, PacketConn, is_closed};
use super::{
    Error, ListenerConfig, PacketConnConfig, PermissionHandler, QuotaHandler,
    RelayAddressGenerator, ServerConfig, default_permission_handler,
};
use crate::go::time::{self, Duration};
use crate::protocol::logger::Logger;

const DEFAULT_INBOUND_MTU: usize = 1600;

// Server is an instance of the Pion TURN Server.
pub struct Server {
    log: Logger,
    auth_handler: Option<AuthHandler>,
    quota_handler: Option<QuotaHandler>,
    realm: String,
    channel_bind_timeout: Duration,
    permission_timeout: Duration,
    allocation_lifetime: Duration,
    strict_address_family: bool,
    nonce_hash: Arc<dyn NonceManager>,
    event_handler: EventHandler,

    packet_conn_configs: Vec<PacketConnConfig>,
    listener_configs: Vec<ListenerConfig>,
    allocation_managers: Mutex<Vec<Arc<Manager>>>,
    inbound_mtu: usize,
}

impl Server {
    // NewServer creates the Pion TURN server.
    pub fn new(config: ServerConfig) -> Result<Arc<Server>, Error> {
        config.validate()?;

        let logger_factory = config.logger_factory.clone().unwrap_or_default();

        let mut mtu = DEFAULT_INBOUND_MTU;
        if config.inbound_mtu != 0 {
            mtu = config.inbound_mtu;
        }

        let nonce_hash: Arc<dyn NonceManager> = new_short_nonce_hash(0)?.into();

        let mut server = Server {
            log: logger_factory.with_name("turn"),
            auth_handler: config.auth_handler,
            quota_handler: config.quota_handler,
            realm: config.realm,
            channel_bind_timeout: config.channel_bind_timeout,
            permission_timeout: config.permission_timeout,
            allocation_lifetime: config.allocation_lifetime,
            strict_address_family: config.strict_address_family,
            packet_conn_configs: config.packet_conn_configs,
            listener_configs: config.listener_configs,
            nonce_hash,
            inbound_mtu: mtu,
            event_handler: config.event_handler,
            allocation_managers: Mutex::new(Vec::new()),
        };

        if server.channel_bind_timeout == Duration::ZERO {
            server.channel_bind_timeout = DEFAULT_LIFETIME_GO;
        }
        if server.permission_timeout == Duration::ZERO {
            server.permission_timeout = allocation::DEFAULT_PERMISSION_TIMEOUT;
        }
        if server.allocation_lifetime == Duration::ZERO {
            server.allocation_lifetime = DEFAULT_LIFETIME_GO;
        }
        let server = Arc::new(server);

        for cfg in &server.packet_conn_configs {
            let am = server
                .create_allocation_manager(
                    cfg.relay_address_generator.clone(),
                    cfg.permission_handler.clone(),
                )
                .map_err(Error::FailedToCreateAllocationManager)?;

            let s = server.clone();
            let conn = cfg.packet_conn.clone().expect("validated");
            time::spawn(async move {
                s.read_loop(conn, &am, None).await;

                if let Err(err) = am.close() {
                    errorf!(s.log, "Failed to close AllocationManager: {}", err);
                }
            });
        }

        for cfg in &server.listener_configs {
            let am = server
                .create_allocation_manager(
                    cfg.relay_address_generator.clone(),
                    cfg.permission_handler.clone(),
                )
                .map_err(Error::FailedToCreateAllocationManager)?;

            let s = server.clone();
            let listener = cfg.listener.clone().expect("validated");
            time::spawn(async move {
                s.clone().read_listener(listener, am.clone()).await;

                if let Err(err) = am.close() {
                    errorf!(s.log, "Failed to close AllocationManager: {}", err);
                }
            });
        }

        Ok(server)
    }

    // AllocationCount returns the number of active allocations.
    // It can be used to drain the server before closing.
    pub fn allocation_count(&self) -> usize {
        let mut allocs = 0;
        for am in self.allocation_managers.lock().iter() {
            allocs += am.allocation_count();
        }

        allocs
    }

    // Close stops the TURN Server.
    // It cleans up any associated state and closes all connections it is managing.
    pub fn close(&self) -> Result<(), Error> {
        let mut errors = Vec::new();

        for cfg in &self.packet_conn_configs {
            if let Some(conn) = &cfg.packet_conn
                && let Err(err) = conn.close()
            {
                errors.push(err);
            }
        }

        for cfg in &self.listener_configs {
            if let Some(listener) = &cfg.listener
                && let Err(err) = listener.close()
            {
                errors.push(err);
            }
        }

        if errors.is_empty() {
            return Ok(());
        }

        let mut err = String::new();
        for e in errors {
            err = format!("{err}; close error ({e}) ");
        }

        Err(Error::FailedToClose(err))
    }

    async fn read_listener(self: Arc<Self>, l: Arc<dyn Listener>, am: Arc<Manager>) {
        loop {
            let mut conn = match l.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    debugf!(self.log, "Failed to accept: {}", err);

                    return;
                }
            };

            let s = self.clone();
            let am = am.clone();
            time::spawn(async move {
                let mut tls_connection_state: Option<ConnectionState> = None;

                // Extract tls connection state if possible
                if let Some(tls_conn) = conn.as_tls() {
                    // Force TLS handshake to complete before extracting connection state.
                    // Per crypto/tls docs: handshakes are lazy (complete on first I/O).
                    // We must call Handshake() explicitly to populate PeerCertificates.
                    let handshake = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        tls_conn.handshake(),
                    )
                    .await;
                    match handshake {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            errorf!(s.log, "TLS handshake failed: {}", err);

                            return;
                        }
                        Err(_) => {
                            errorf!(
                                s.log,
                                "TLS handshake failed: {}",
                                "context deadline exceeded"
                            );

                            return;
                        }
                    }
                    tls_connection_state = Some(tls_conn.connection_state());
                }

                let stun_conn = Arc::new(StunConn::new(conn));
                s.read_loop(stun_conn.clone(), &am, tls_connection_state)
                    .await;

                // Delete allocation
                am.delete_allocation(&FiveTuple {
                    protocol: Protocol::Udp, // fixed UDP
                    src_addr: Some(stun_conn.remote_addr()),
                    dst_addr: Some(stun_conn.local_addr()),
                });

                if let Err(err) = stun_conn.close()
                    && !is_closed(&err)
                {
                    errorf!(s.log, "Failed to close conn: {}", err);
                }
            });
        }
    }

    fn create_allocation_manager(
        &self,
        addr_generator: Option<Arc<dyn RelayAddressGenerator>>,
        handler: Option<PermissionHandler>,
    ) -> Result<Arc<Manager>, allocation::Error> {
        let handler: PermissionHandler =
            handler.unwrap_or_else(|| Arc::new(default_permission_handler));
        let addr_generator: Arc<dyn RelayAddressGenerator> =
            addr_generator.unwrap_or_else(|| Arc::new(NilAddressGenerator));

        let am = Manager::new(ManagerConfig {
            allocate_packet_conn: Some(Arc::new(move |conf| {
                addr_generator
                    .allocate_packet_conn(conf)
                    .map_err(|err| Box::new(err) as BoxError)
            })),
            permission_handler: Some(handler),
            event_handler: self.event_handler.clone(),
            leveled_logger: Some(self.log.clone()),
        })?;

        self.allocation_managers.lock().push(am.clone());

        Ok(am)
    }

    async fn read_loop(
        &self,
        conn: Arc<dyn PacketConn>,
        allocation_manager: &Arc<Manager>,
        tls_state: Option<ConnectionState>,
    ) {
        let mut buf = vec![0u8; self.inbound_mtu];
        loop {
            let (n, addr) = match conn.read_from(&mut buf).await {
                Err(err) => {
                    debugf!(self.log, "Exit read loop on error: {}", err);

                    return;
                }
                Ok((n, _)) if n >= self.inbound_mtu => {
                    debugf!(
                        self.log,
                        "Read bytes exceeded MTU, packet is possibly truncated"
                    );

                    continue;
                }
                Ok(r) => r,
            };

            if let Err(err) = server::handle_request(&Request {
                conn: conn.clone(),
                src_addr: addr,
                buff: &buf[..n],
                tls: tls_state.clone(),
                log: self.log.clone(),
                auth_handler: self.auth_handler.clone(),
                quota_handler: self.quota_handler.clone(),
                realm: self.realm.clone(),
                allocation_manager: allocation_manager.clone(),
                channel_bind_timeout: self.channel_bind_timeout,
                permission_timeout: self.permission_timeout,
                allocation_lifetime: self.allocation_lifetime,
                strict_address_family: self.strict_address_family,
                nonce_hash: self.nonce_hash.clone(),
            })
            .await
            {
                if let Some(on_allocation_error) = &self.event_handler.on_allocation_error {
                    on_allocation_error(
                        Some(addr),
                        Some(conn.local_addr()),
                        &Protocol::Udp.to_string(),
                        &err.to_string(),
                    );
                }
                debugf!(self.log, "Failed to handle datagram: {}", err);
            }
        }
    }
}

struct NilAddressGenerator;

impl RelayAddressGenerator for NilAddressGenerator {
    fn validate(&self) -> Result<(), Error> {
        Err(Error::RelayAddressGeneratorNil)
    }

    fn allocate_packet_conn(
        &self,
        _: allocation::AllocateListenerConfig,
    ) -> Result<(Arc<dyn PacketConn>, Addr), Error> {
        Err(Error::RelayAddressGeneratorNil)
    }
}

#[cfg(test)]
#[path = "server_test.rs"]
mod server_test;
