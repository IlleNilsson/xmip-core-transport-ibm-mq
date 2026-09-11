#![forbid(unsafe_code)]

//! Streams that arrive as messages on an IBM MQ queue. One message is one
//! Stream.
//!
//! MQ is the queue of every bank and every airline that had a mainframe,
//! and its client protocol is what a connection to a queue manager speaks
//! on port 1414: transmission segments (`segment.rs`) carrying the initial
//! data two sides agree sizes with, then `MQCONN`, `MQOPEN`, `MQPUT`,
//! `MQGET`, `MQCLOSE` and `MQDISC`, each with the structures the MQI
//! declares (`descriptor.rs`) — the message descriptor, the put and get
//! options, the object descriptor. A Receive Location connects, opens its
//! queue for input and gets until the queue manager says `2033`, no
//! message available, each message a Stream with the id MQ gave it; a
//! Send Location connects, opens for output and puts the Stream as one
//! datagram. `client.rs` is Xmip's side; `manager.rs` is the queue
//! manager a test or the playground puts on loopback. No channel exit, no
//! user identification record and no TLS are spoken; a queue manager that
//! demands them refuses the connection with its reason, and this transport
//! says so.
//!
//! A message got is gone from the queue, which is the queue's own claim,
//! so [`Transport::claims`] answers `None`. The ceiling is the queue
//! manager's `MAXMSGL`, four mebibytes unless it says otherwise in its
//! initial data, and a Stream over it is refused before a byte is written.
//!
//! The origin URI is the queue manager and queue with the message id:
//! `ibm-mq://host:1414/QM1/ORDERS?msgid=414d5120...`. A send target is
//! `ibm-mq://host:1414/<queue manager>/<queue>`,
//! `host:1414/<queue manager>/<queue>`, `<queue manager>/<queue>` on the
//! configured server, or `<queue>` on the configured queue manager.

pub mod client;
pub mod descriptor;
pub mod manager;
pub mod segment;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, DEFAULT_CHANNEL, DEFAULT_MAX_MESSAGE};
pub use manager::{Event, QueueManager, Session};
use transport::error::{Result, TransportError};
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct IbmMqTransport {
    server: String,
    queue_manager: String,
    queue: String,
    channel: String,
    timeout: Option<Duration>,
}

impl IbmMqTransport {
    /// Speak to `queue_manager` at `server`, on `queue`.
    #[must_use]
    pub fn new(
        server: impl Into<String>,
        queue_manager: impl Into<String>,
        queue: impl Into<String>,
    ) -> Self {
        Self {
            server: server.into(),
            queue_manager: queue_manager.into(),
            queue: queue.into(),
            channel: DEFAULT_CHANNEL.to_string(),
            timeout: None,
        }
    }

    /// Connect on `channel` rather than `SYSTEM.DEF.SVRCONN`.
    #[must_use]
    pub fn on_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = channel.into();
        self
    }

    /// Give up on a queue manager that stops mid-reply.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the configured queue manager.
    ///
    /// # Errors
    /// Where the server could not be reached or refused the connection.
    pub fn connect(&self) -> Result<Client> {
        self.connect_to(&self.server, &self.queue_manager)
    }

    fn connect_to(&self, server: &str, queue_manager: &str) -> Result<Client> {
        Client::connect(server, queue_manager, &self.channel, "xmip", self.timeout)
    }

    /// Bind as the queue manager clients connect to, and report the
    /// address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener as this transport's
    /// queue manager, with this transport's queue defined and holding
    /// `messages`.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client did not
    /// open with initial data.
    pub fn accept_one(&self, listener: &TcpListener, messages: &[&[u8]]) -> Result<Session> {
        QueueManager::new(&self.queue_manager)
            .holding(&self.queue, messages)
            .accept(listener, self.timeout)
    }

    /// The origin every message of `queue` on `server` shares, up to the
    /// message id.
    fn origin(server: &str, queue_manager: &str, queue: &str) -> String {
        format!("ibm-mq://{server}/{queue_manager}/{queue}?msgid=")
    }

    /// Where a target names the server, queue manager and queue, or some
    /// suffix of them on what this transport is configured with.
    fn resolve<'a>(&'a self, target: &'a str) -> Result<(&'a str, &'a str, &'a str)> {
        let (server, path) = socket::target("ibm-mq", target)
            .or_else(|| socket::target("mq", target))
            .or_else(|| match target.split_once('/') {
                Some((peer, path)) if peer.contains(':') => Some((peer, path)),
                _ => None,
            })
            .unwrap_or((&self.server, target));
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match segments.as_slice() {
            [queue_manager, queue] => Ok((server, queue_manager, queue)),
            [queue] => Ok((server, &self.queue_manager, queue)),
            _ => Err(TransportError::permanent(format!(
                "{target:?} is not <queue manager>/<queue> or <queue>"
            ))),
        }
    }
}

impl Transport for IbmMqTransport {
    fn name(&self) -> &'static str {
        "ibm-mq"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Get every message waiting on the queue, oldest first.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        let handle = client.open(
            &self.queue,
            descriptor::OPEN_INPUT | descriptor::OPEN_FAIL_IF_QUIESCING,
        )?;
        let origin = Self::origin(&self.server, &self.queue_manager, &self.queue);
        let mut arrived = Vec::new();
        while let Some((id, bytes)) = client.get(handle)? {
            arrived.push(Arrived::new(
                format!("{origin}{}", manager::hex(&id)),
                bytes,
            ));
        }
        client.close(handle)?;
        client.disconnect()?;
        Ok(arrived)
    }

    /// Put the bytes as one message on the queue the target names.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, queue_manager, queue) = self.resolve(target)?;
        let mut client = self.connect_to(server, queue_manager)?;
        let handle = client.open(
            queue,
            descriptor::OPEN_OUTPUT | descriptor::OPEN_FAIL_IF_QUIESCING,
        )?;
        client.put(handle, bytes)?;
        client.close(handle)?;
        client.disconnect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_put_message_is_the_queue_managers_event_and_its_id_names_the_origin() {
        let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let long: Vec<u8> = (0..(1 << 20))
            .map(|n: u32| u8::try_from(n % 251).unwrap_or(0))
            .collect();
        let sent = long.clone();
        let sender = std::thread::spawn(move || {
            let near =
                IbmMqTransport::new(address.clone(), "QM1", "ORDERS").timing_out_after(secs(2));
            near.send(&format!("ibm-mq://{address}/QM1/ORDERS"), b"order\0\xff")?;
            near.send("ORDERS", &sent)?;
            near.send("ORDERS", b"")
        });
        let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
        assert_eq!(session.next_event().expect("conn"), Some(Event::Connected));
        assert_eq!(
            session.next_event().expect("open"),
            Some(Event::Opened("ORDERS".to_string()))
        );
        let first = session.next_put().expect("put").expect("one");
        assert_eq!(first.bytes, b"order\0\xff");
        assert!(
            first
                .origin_uri
                .starts_with("ibm-mq://QM1/ORDERS?msgid=414d5120"),
            "{}",
            first.origin_uri
        );
        assert!(session.next_put().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener, &[]).expect("second");
        let second = session.next_put().expect("put").expect("one");
        assert_eq!(second.bytes, long, "a mebibyte across segments");
        assert!(session.next_put().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener, &[]).expect("third");
        let third = session.next_put().expect("put").expect("one");
        assert!(third.bytes.is_empty());
        assert!(session.next_put().expect("closed").is_none());
        assert_eq!(session.queue("ORDERS"), vec![Vec::<u8>::new()]);
        sender.join().expect("thread").expect("sending");
    }

    #[test]
    fn a_receive_gets_every_message_oldest_first_until_the_queue_is_empty() {
        let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            IbmMqTransport::new(address, "QM1", "ORDERS")
                .timing_out_after(secs(2))
                .receive()
        });
        let mut session = far_end
            .accept_one(&listener, &[b"first", b"\x00second\xff"])
            .expect("accepting");
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("event") {
            events.push(event);
        }
        assert_eq!(
            events.iter().filter(|e| matches!(e, Event::Got(_))).count(),
            3,
            "two, then 2033"
        );
        assert_eq!(events.last(), Some(&Event::Disconnected));
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"first");
        assert_eq!(arrived[1].bytes, b"\x00second\xff");
        assert!(arrived[0].origin_uri.starts_with("ibm-mq://127.0.0.1:"));
        assert!(arrived[0].origin_uri.contains("/QM1/ORDERS?msgid=414d5120"));
        assert!(session.queue("ORDERS").is_empty(), "got is gone");
    }

    #[test]
    fn the_wrong_queue_manager_an_unknown_queue_and_too_big_a_message_are_refused() {
        let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near =
                IbmMqTransport::new(address.clone(), "QM1", "ORDERS").timing_out_after(secs(2));
            let wrong = near.send("QM9/ORDERS", b"x");
            let unknown = near.send("NOWHERE", b"x");
            let big = QueueManager::new("QM1");
            drop(big);
            let mut client = near.connect()?;
            let handle = client.open("ORDERS", descriptor::OPEN_OUTPUT)?;
            let too_big = client.put(handle, &[0u8; 101]);
            client.disconnect()?;
            Ok::<_, TransportError>((wrong, unknown, too_big, near.send("only/three/parts", b"")))
        });
        for _ in 0..2 {
            let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
            while session.next_event().expect("event").is_some() {}
        }
        let mut session = QueueManager::new("QM1")
            .with_queue("ORDERS")
            .carrying_at_most(100)
            .accept(&listener, Some(secs(2)))
            .expect("small");
        while session.next_event().expect("event").is_some() {}
        let (wrong, unknown, too_big, target) = sender.join().expect("thread").expect("ran");
        assert!(wrong.expect_err("2058").message.contains("MQRC 2058"));
        assert!(unknown.expect_err("2085").message.contains("MQRC 2085"));
        let too_big = too_big.expect_err("2031");
        assert!(too_big.message.contains("MQRC 2031"), "{too_big}");
        assert!(!too_big.retryable);
        assert!(!target.expect_err("not a queue").retryable);
    }

    #[test]
    fn the_transport_names_itself_and_claims_nothing() {
        let mq = IbmMqTransport::new("host:1414", "QM1", "ORDERS").on_channel("XMIP.SVRCONN");
        assert_eq!(mq.name(), "ibm-mq");
        assert_eq!(mq.directions(), Directions::BOTH);
        assert!(mq.claims().is_none(), "a got message is gone");
        assert_eq!(mq.channel, "XMIP.SVRCONN");
        assert_eq!(
            mq.resolve("mq://other:1414/QM2/Q").expect("full"),
            ("other:1414", "QM2", "Q")
        );
        assert_eq!(
            mq.resolve("other:1414/QM2/Q").expect("bare"),
            ("other:1414", "QM2", "Q")
        );
        assert_eq!(mq.resolve("Q").expect("queue"), ("host:1414", "QM1", "Q"));
    }
}
