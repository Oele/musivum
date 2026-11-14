//! Data channels of a connection.

use std::sync::{Arc, Weak};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use rtc::data_channel::{RTCDataChannel, RTCDataChannelId};

use super::{DataChannelState, Delivery, Inner};
use crate::webrtc::Error;

/// pion's `DataChannelInit`: what a data channel is created with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataChannelInit {
    /// Ordered indicates if data is allowed to be delivered out of order. The
    /// default value of true, guarantees that data will be delivered in order.
    pub ordered: Option<bool>,

    /// MaxPacketLifeTime limits the time (in milliseconds) during which the
    /// channel will transmit or retransmit data if not acknowledged. This value
    /// may be clamped if it exceeds the maximum value supported.
    pub max_packet_life_time: Option<u16>,

    /// MaxRetransmits limits the number of times a channel will retransmit data
    /// if not successfully delivered. This value may be clamped if it exceeds
    /// the maximum value supported.
    pub max_retransmits: Option<u16>,

    /// Protocol describes the subprotocol name used for this channel.
    pub protocol: Option<String>,

    /// Negotiated describes if the data channel is created by the local peer or
    /// the remote peer. The default value of false tells the user agent to
    /// announce the channel in-band and instruct the other peer to dispatch a
    /// corresponding DataChannel. If set to true, it is up to the application
    /// to negotiate the channel and create an DataChannel with the same id
    /// at the other peer.
    pub negotiated: Option<bool>,

    /// ID overrides the default selection of ID for this channel.
    pub id: Option<u16>,
}

/// pion's `DataChannelMessage`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataChannelMessage {
    pub is_string: bool,
    pub data: Bytes,
}

type Handler = Option<Arc<dyn Fn() + Send + Sync>>;

struct State {
    // The SCTP stream, once the channel has one.
    stream_id: Option<u16>,
    // Open was told, to the handler there was then or to nobody.
    open: bool,
    closed: bool,
    on_open: Handler,
    on_message: Option<Arc<dyn Fn(DataChannelMessage) + Send + Sync>>,
    on_buffered_amount_low: Handler,
    on_close: Handler,
}

/// pion's `DataChannel`. LiveKit detaches pion's channels and reads and writes them as
/// streams with deadlines; here a channel has `send` and a message callback, and a `send`
/// never waits: what cannot go out yet is counted in `buffered_amount`.
pub struct DataChannel {
    pc: Weak<Inner>,
    id: RTCDataChannelId,
    label: String,
    protocol: String,
    ordered: bool,
    max_packet_life_time: Option<u16>,
    max_retransmits: Option<u16>,
    negotiated: bool,
    state: Mutex<State>,
}

impl DataChannel {
    fn make(pc: Weak<Inner>, id: RTCDataChannelId, label: String) -> DataChannel {
        DataChannel {
            pc,
            id,
            label,
            protocol: String::new(),
            ordered: true,
            max_packet_life_time: None,
            max_retransmits: None,
            negotiated: false,
            state: Mutex::new(State {
                stream_id: None,
                open: false,
                closed: false,
                on_open: None,
                on_message: None,
                on_buffered_amount_low: None,
                on_close: None,
            }),
        }
    }

    /// A channel this side creates.
    pub(crate) fn new(
        pc: Weak<Inner>,
        id: RTCDataChannelId,
        label: String,
        init: &DataChannelInit,
    ) -> DataChannel {
        let mut channel = DataChannel::make(pc, id, label);
        channel.protocol = init.protocol.clone().unwrap_or_default();
        channel.ordered = init.ordered.unwrap_or(true);
        channel.max_packet_life_time = init.max_packet_life_time;
        channel.max_retransmits = init.max_retransmits;
        channel.negotiated = init.negotiated.unwrap_or(false);
        channel
    }

    /// A channel the other side opened.
    pub(crate) fn from_rtc(pc: Weak<Inner>, rtc: &RTCDataChannel<'_>) -> DataChannel {
        let mut channel = DataChannel::make(pc, rtc.id(), rtc.label().to_string());
        channel.protocol = rtc.protocol().to_string();
        channel.ordered = rtc.ordered();
        channel.max_packet_life_time = rtc.max_packet_life_time();
        channel.max_retransmits = rtc.max_retransmits();
        channel.negotiated = rtc.negotiated();
        channel
    }

    pub(crate) fn opened(&self, stream_id: Option<u16>) {
        self.state.lock().stream_id = stream_id;
    }

    pub(crate) fn closed(&self) {
        self.state.lock().closed = true;
    }

    pub(crate) fn tell_open(&self) {
        let handler = {
            let mut state = self.state.lock();
            state.open = true;
            state.on_open.clone()
        };
        if let Some(handler) = handler {
            handler();
        }
    }

    pub(crate) fn tell_message(&self, message: DataChannelMessage) {
        let handler = self.state.lock().on_message.clone();
        if let Some(handler) = handler {
            handler(message);
        }
    }

    pub(crate) fn tell_buffered_amount_low(&self) {
        let handler = self.state.lock().on_buffered_amount_low.clone();
        if let Some(handler) = handler {
            handler();
        }
    }

    pub(crate) fn tell_close(&self) {
        let handler = {
            let mut state = self.state.lock();
            // The last thing a channel tells: its callbacks are let go.
            state.on_open = None;
            state.on_message = None;
            state.on_buffered_amount_low = None;
            state.on_close.take()
        };
        if let Some(handler) = handler {
            handler();
        }
    }

    /// Label represents a label that can be used to distinguish this
    /// DataChannel object from other DataChannel objects. Scripts are
    /// allowed to create multiple DataChannel objects with the same label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Ordered returns true if the DataChannel is ordered, and false if
    /// out-of-order delivery is allowed.
    pub fn ordered(&self) -> bool {
        self.ordered
    }

    /// MaxPacketLifeTime represents the length of the time window (msec) during
    /// which transmissions and retransmissions may occur in unreliable mode.
    ///
    /// Known for a channel this side made. For one the other side opened `rtc` does not
    /// say, and this is `None` (so is `max_retransmits`).
    pub fn max_packet_life_time(&self) -> Option<u16> {
        self.max_packet_life_time
    }

    /// MaxRetransmits represents the maximum number of retransmissions that are
    /// attempted in unreliable mode.
    pub fn max_retransmits(&self) -> Option<u16> {
        self.max_retransmits
    }

    /// Protocol represents the name of the sub-protocol used with this
    /// DataChannel.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Negotiated represents whether this DataChannel was negotiated by the
    /// application (true), or not (false).
    pub fn negotiated(&self) -> bool {
        self.negotiated
    }

    /// ID represents the ID for this DataChannel. The value is initially
    /// null, which is what will be returned if the ID was not provided at
    /// channel creation time, and the DTLS role of the SCTP transport has not
    /// yet been negotiated. Otherwise, it will return the ID that was either
    /// selected by the script or generated. After the ID is set to a non-null
    /// value, it will not change.
    pub fn id(&self) -> Option<u16> {
        if let Some(id) = self.state.lock().stream_id {
            return Some(id);
        }
        let pc = self.pc.upgrade()?;
        let id = pc.read(|core| core.rtc.data_channel(self.id)?.stream_id())?;
        self.state.lock().stream_id = Some(id);
        Some(id)
    }

    /// ReadyState represents the state of the DataChannel object.
    pub fn ready_state(&self) -> DataChannelState {
        let Some(pc) = self.pc.upgrade() else {
            return DataChannelState::Closed;
        };
        pc.read(|core| {
            core.rtc
                .data_channel(self.id)
                .map_or(DataChannelState::Closed, |channel| channel.ready_state())
        })
    }

    /// BufferedAmount represents the number of bytes of application data
    /// (UTF-8 text and binary data) that have been queued using send(). Even
    /// though the data transmission can occur in parallel, the returned value
    /// MUST NOT be decreased before the current task yielded back to the event
    /// loop to prevent race conditions. The value does not include framing
    /// overhead incurred by the protocol, or buffering done by the operating
    /// system or network hardware. The value of BufferedAmount slot will only
    /// increase with each call to the send() method as long as the ReadyState is
    /// open; however, BufferedAmount does not reset to zero once the channel
    /// closes.
    ///
    /// Here: what was given to `send` and has not been acknowledged by the other side (or
    /// given up on) yet.
    pub fn buffered_amount(&self) -> u64 {
        let Some(pc) = self.pc.upgrade() else {
            return 0;
        };
        pc.read(|core| {
            core.rtc
                .data_channel(self.id)
                .map_or(0, |channel| channel.outstanding_bytes() as u64)
        })
    }

    /// BufferedAmountLowThreshold represents the threshold at which the
    /// bufferedAmount is considered to be low. When the bufferedAmount decreases
    /// from above this threshold to equal or below it, the bufferedamountlow
    /// event fires. BufferedAmountLowThreshold is initially zero on each new
    /// DataChannel, but the application may change its value at any time.
    /// The threshold is set to 0 by default.
    pub fn buffered_amount_low_threshold(&self) -> u64 {
        let Some(pc) = self.pc.upgrade() else {
            return 0;
        };
        pc.read(|core| {
            core.rtc
                .data_channel(self.id)
                .map_or(0, |channel| channel.buffered_amount_low_threshold() as u64)
        })
    }

    /// SetBufferedAmountLowThreshold is used to update the threshold.
    /// See BufferedAmountLowThreshold().
    pub fn set_buffered_amount_low_threshold(&self, th: u64) {
        let Some(pc) = self.pc.upgrade() else {
            return;
        };
        pc.act(|_, core, _| {
            if let Some(mut channel) = core.rtc.data_channel(self.id) {
                channel.set_buffered_amount_low_threshold(th.min(u32::MAX as u64) as u32);
            }
        });
    }

    /// OnOpen sets an event handler which is invoked when
    /// the underlying data transport has been established (or re-established).
    ///
    /// As with pion, a handler set on a channel that is open already is called (from the
    /// connection's task, like every callback).
    pub fn on_open(self: &Arc<Self>, f: impl Fn() + Send + Sync + 'static) {
        let already_open = {
            let mut state = self.state.lock();
            if state.closed {
                return;
            }
            let was_set = state.on_open.replace(Arc::new(f)).is_some();
            state.open && !was_set
        };
        if already_open && let Some(pc) = self.pc.upgrade() {
            pc.tell_later(Delivery::DataChannelOpen(self.clone()));
        }
    }

    /// OnMessage sets an event handler which is invoked on a binary
    /// message arrival over the sctp transport from a remote peer.
    /// OnMessage can currently receive messages up to 16384 bytes
    /// in size. Check out the detach API if you want to use larger
    /// message sizes. Note that browser support for larger messages
    /// is also limited.
    ///
    /// Messages that arrive before a handler is set are lost; set it where the channel is
    /// made, or in the `on_data_channel` callback that announces it.
    pub fn on_message(&self, f: impl Fn(DataChannelMessage) + Send + Sync + 'static) {
        let mut state = self.state.lock();
        if !state.closed {
            state.on_message = Some(Arc::new(f));
        }
    }

    /// OnBufferedAmountLow sets an event handler which is invoked when
    /// the number of bytes of outgoing data becomes lower than or equal to the
    /// BufferedAmountLowThreshold.
    pub fn on_buffered_amount_low(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut state = self.state.lock();
        if !state.closed {
            state.on_buffered_amount_low = Some(Arc::new(f));
        }
    }

    /// OnClose sets an event handler which is invoked when
    /// the underlying data transport has been closed.
    pub fn on_close(&self, f: impl Fn() + Send + Sync + 'static) {
        let mut state = self.state.lock();
        if !state.closed {
            state.on_close = Some(Arc::new(f));
        }
    }

    /// Send sends the binary message to the DataChannel peer.
    pub fn send(&self, data: &[u8]) -> Result<(), Error> {
        let pc = self.pc.upgrade().ok_or(Error::ClosedPipe)?;
        pc.act(|_, core, _| {
            if core.closed {
                return Err(Error::ClosedPipe);
            }
            let mut channel = core.rtc.data_channel(self.id).ok_or(Error::ClosedPipe)?;
            channel.send(Instant::now(), BytesMut::from(data))?;
            Ok(())
        })
    }

    /// SendText sends the text message to the DataChannel peer.
    pub fn send_text(&self, s: &str) -> Result<(), Error> {
        let pc = self.pc.upgrade().ok_or(Error::ClosedPipe)?;
        pc.act(|_, core, _| {
            if core.closed {
                return Err(Error::ClosedPipe);
            }
            let mut channel = core.rtc.data_channel(self.id).ok_or(Error::ClosedPipe)?;
            channel.send_text(Instant::now(), s)?;
            Ok(())
        })
    }

    /// Close Closes the DataChannel. It may be called regardless of whether
    /// the DataChannel object was created by this peer or the remote peer.
    pub fn close(&self) -> Result<(), Error> {
        let Some(pc) = self.pc.upgrade() else {
            return Ok(());
        };
        pc.act(|_, core, _| {
            if let Some(mut channel) = core.rtc.data_channel(self.id) {
                channel.close()?;
            }
            Ok(())
        })
    }
}
