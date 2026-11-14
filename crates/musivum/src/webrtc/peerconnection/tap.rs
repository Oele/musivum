//! The interceptor of musivum's own, closest to the wire: the counterpart of pion's buffer
//! factory, and of the one use LiveKit makes of pion's interceptors (learning about streams).
//!
//! `rtc` hands every decrypted RTP and RTCP packet of a connection to its interceptor chain
//! before it does anything else with it: before RTX is unwrapped, before padding-only packets
//! are dropped, before RTCP is thrown away because no interceptor asked for it. This is the
//! only interceptor in the chain, so what it sees is what arrived.
//!
//! - RTP is copied out as bytes (marshalled again from `rtc`'s parsed packet, the padding
//!   size kept) and then passed on, so that `rtc` goes on counting it.
//! - RTCP is taken out: `rtc` would drop it at the end of the chain anyway.
//! - What is written passes through untouched.
//!
//! It runs inside the connection's lock, so it only puts what it takes into an inbox; the
//! connection empties that and calls the application after it has let go of the lock.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use parking_lot::Mutex;
use rtc::interceptor::{Interceptor, Packet, StreamInfo, TaggedPacket};
use rtc::sansio::Protocol;
use rtc::shared::marshal::Marshal;

use crate::webrtc::rtcp;

/// What the tap took out of the connection.
pub(crate) enum Captured {
    Rtp { ssrc: u32, packet: Bytes },
    Rtcp(Vec<Box<dyn rtcp::Packet>>),
    BindRemoteStream(StreamInfo),
    UnbindRemoteStream(StreamInfo),
}

pub(crate) type Inbox = Arc<Mutex<Vec<Captured>>>;

pub(crate) struct Tap {
    inbox: Inbox,
    read_queue: VecDeque<TaggedPacket>,
    write_queue: VecDeque<TaggedPacket>,
}

impl Tap {
    pub(crate) fn new(inbox: Inbox) -> Tap {
        Tap {
            inbox,
            read_queue: VecDeque::new(),
            write_queue: VecDeque::new(),
        }
    }
}

impl Protocol<TaggedPacket, TaggedPacket, ()> for Tap {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, mut msg: TaggedPacket) -> Result<(), Self::Error> {
        match &mut msg.message.packet {
            Packet::Rtp(packet) => {
                // A packet that cannot be written out again cannot have been read either;
                // should it happen, `rtc` still gets it.
                if let Ok(raw) = packet.marshal() {
                    self.inbox.lock().push(Captured::Rtp {
                        ssrc: packet.header.ssrc,
                        packet: raw.freeze(),
                    });
                }
                self.read_queue.push_back(msg);
            }
            Packet::Rtcp(packets) => {
                self.inbox
                    .lock()
                    .push(Captured::Rtcp(std::mem::take(packets)));
            }
            _ => self.read_queue.push_back(msg),
        }
        Ok(())
    }

    fn poll_read(&mut self) -> Option<Self::Rout> {
        self.read_queue.pop_front()
    }

    fn handle_write(&mut self, msg: TaggedPacket) -> Result<(), Self::Error> {
        self.write_queue.push_back(msg);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.write_queue.pop_front()
    }
}

impl Interceptor for Tap {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}

    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}

    fn bind_remote_stream(&mut self, info: &StreamInfo) {
        self.inbox
            .lock()
            .push(Captured::BindRemoteStream(info.clone()));
    }

    fn unbind_remote_stream(&mut self, info: &StreamInfo) {
        self.inbox
            .lock()
            .push(Captured::UnbindRemoteStream(info.clone()));
    }
}
