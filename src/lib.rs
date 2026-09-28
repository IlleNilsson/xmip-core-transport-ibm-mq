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
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! queue manager up at the server address and takes the one put.

pub mod client;
pub mod descriptor;
pub mod manager;
pub mod segment;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, DEFAULT_CHANNEL, DEFAULT_MAX_MESSAGE};
pub use manager::{Event, QueueManager, Session};
use net::Target;
use transport::error::{Result, TransportError, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, Pool, Transport};
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct IbmMqTransport {
    server: String,
    queue_manager: String,
    queue: String,
    channel: String,
    timeout: Option<Duration>,
    /// The connections a receive gets on and a send puts on, connected once
    /// per server and queue manager and kept.
    connections: Pool<Client>,
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
            connections: Pool::new(),
        }
    }

    /// Connect on `channel` rather than `SYSTEM.DEF.SVRCONN`.
    #[must_use]
    fn on_channel(mut self, channel: impl Into<String>) -> Self {
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
        let (server, path) = Target::naming_server(&["ibm-mq", "mq"], target)
            .map_or((self.server.as_str(), target), |named| {
                (named.authority(), named.path())
            });
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

    /// Get every message waiting on the queue, oldest first, on the
    /// connection kept for the queue manager and made on the first receive.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let origin = Self::origin(&self.server, &self.queue_manager, &self.queue);
        self.connections.exchange(
            &format!("{}/{}", self.server, self.queue_manager),
            || self.connect(),
            |client| {
                let handle = client.held(
                    &self.queue,
                    descriptor::OPEN_INPUT | descriptor::OPEN_FAIL_IF_QUIESCING,
                )?;
                let mut arrived = Vec::new();
                while let Some((id, bytes)) = client.get(handle)? {
                    arrived.push(Arrived::new(
                        format!("{origin}{}", codec::hex::encode(&id)),
                        bytes,
                    ));
                }
                Ok(arrived)
            },
        )
    }

    /// Put the bytes as one message on the queue the target names, on the
    /// connection kept for its queue manager and made on the first send.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, queue_manager, queue) = self.resolve(target)?;
        self.connections.exchange(
            &format!("{server}/{queue_manager}"),
            || self.connect_to(server, queue_manager),
            |client| {
                let handle = client.held(
                    queue,
                    descriptor::OPEN_OUTPUT | descriptor::OPEN_FAIL_IF_QUIESCING,
                )?;
                client.put(handle, bytes).map(|_| ())
            },
        )
    }
}

impl Configured for IbmMqTransport {
    /// The address is the queue manager's listener, host and port: where a
    /// Location connects.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "queue_manager",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The queue manager a Location connects to unless the target names \
                          another.",
                applies: Applies::Both,
            },
            Setting {
                name: "queue",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The queue a Location gets every waiting message from.",
                applies: Applies::Receive,
            },
            Setting {
                name: "channel",
                kind: Kind::Text,
                presence: Presence::Default(Fixed::Text(DEFAULT_CHANNEL)),
                meaning: "The server-connection channel a Location connects on.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a queue manager that stops mid-reply is waited on; \
                          unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    /// A Send Location puts on the queue its target names, so it has no
    /// queue of its own.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let transport = Self::new(
            address,
            settings.text("queue_manager"),
            settings.optional_text("queue").unwrap_or_default(),
        )
        .on_channel(settings.text("channel"));
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl IbmMqTransport {
    /// Both ends on this machine: the queue manager stands up on an
    /// ephemeral local port with one queue, the loopback timeout on both
    /// sides.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for IbmMqTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener, &[])?;
        let put = session
            .next_put()?
            .ok_or_else(|| protocol_error("connected, but nothing was put"))?;
        // The client's send is not over until its close and disconnect are
        // answered; a queue manager that hangs up after the put aborts them.
        while session.next_event()?.is_some() {}
        Ok(put)
    }
}

impl Loopback for IbmMqTransport {
    /// `MAXMSGL` as MQ ships it: four mebibytes, what the two sides agree
    /// on when neither says otherwise.
    fn ceiling(&self) -> Option<usize> {
        Some(DEFAULT_MAX_MESSAGE as usize)
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self::new(address, &self.queue_manager, &self.queue).on_channel(&self.channel);
        match self.timeout {
            Some(timeout) => near.timing_out_after(timeout),
            None => near,
        }
        .send(&self.queue, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::edge_payloads;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn ibm_mq_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(IbmMqTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            ("queue_manager".to_string(), Given::Text("QM1".to_string())),
            ("queue".to_string(), Given::Text("ORDERS".to_string())),
            ("timeout".to_string(), Given::Text("15s".to_string())),
        ];
        let built =
            IbmMqTransport::open("mq.example:1414", Applies::Receive, &given).expect("built");
        assert_eq!(built.queue_manager, "QM1");
        assert_eq!(built.queue, "ORDERS");
        assert_eq!(built.channel, DEFAULT_CHANNEL);
        assert_eq!(built.timeout, Some(secs(15)));
        let Err(refused) = IbmMqTransport::open("mq.example:1414", Applies::Send, &given[1..2])
        else {
            panic!("a Send Location's queue manager is required, and it names no queue");
        };
        assert!(
            refused.message.contains("\"queue_manager\""),
            "{}",
            refused.message
        );
        assert!(refused.message.contains("\"queue\""), "{}", refused.message);
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
            near.send("ORDERS", b"")?;
            Ok::<_, TransportError>(near.connections.opened())
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
        // One queue manager, so one connection for all three puts.
        let second = session.next_put().expect("put").expect("one");
        assert_eq!(second.bytes, long, "a mebibyte across segments");
        let third = session.next_put().expect("put").expect("one");
        assert!(third.bytes.is_empty());
        assert!(session.next_put().expect("closed").is_none());
        let opened = sender.join().expect("thread").expect("sending");
        assert_eq!(opened, 1);
        assert_eq!(session.queue("ORDERS").len(), 3);
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
        assert!(
            matches!(events.last(), Some(Event::Got(_))),
            "the connection and the queue kept open: {events:?}"
        );
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"first");
        assert_eq!(arrived[1].bytes, b"\x00second\xff");
        assert!(arrived[0].origin_uri.starts_with("ibm-mq://127.0.0.1:"));
        assert!(arrived[0].origin_uri.contains("/QM1/ORDERS?msgid=414d5120"));
        assert!(session.queue("ORDERS").is_empty(), "got is gone");
    }

    #[test]
    fn a_thousand_receives_connect_once_and_a_connection_the_manager_closed_is_replaced() {
        const RECEIVES: usize = 1000;
        let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = IbmMqTransport::new(address, "QM1", "ORDERS").timing_out_after(secs(5));
        let receiving = near.clone();
        let receiver = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for _ in 0..RECEIVES {
                assert!(receiving.receive()?.is_empty());
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a receive.
            assert!(took < Duration::from_millis(RECEIVES as u64), "{took:?}");
            receiving.receive()
        });
        // Served until `receives` gets found the queue empty, one a
        // receive; how many connects and opens.
        let serve = |session: &mut Session, receives: usize| {
            let (mut connected, mut opened, mut emptied) = (0, 0, 0);
            while emptied < receives {
                match session.next_event().expect("serving").expect("one") {
                    Event::Connected => connected += 1,
                    Event::Opened(_) => opened += 1,
                    Event::Got(_) => emptied += 1,
                    _ => {}
                }
            }
            (connected, opened)
        };
        let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
        let once = serve(&mut session, RECEIVES);
        assert_eq!(once, (1, 1), "one connect and one open for every receive");
        drop(session);
        let mut again = far_end
            .accept_one(&listener, &[b"after"])
            .expect("a new connection");
        // The one message, then the get that finds the queue empty.
        serve(&mut again, 2);
        let arrived = receiver.join().expect("thread").expect("received");
        assert_eq!(arrived[0].bytes, b"after");
        assert_eq!(near.connections.opened(), 2);
    }

    #[test]
    fn a_thousand_puts_connect_once_and_a_connection_the_manager_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = IbmMqTransport::new(address, "QM1", "ORDERS").timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("ORDERS", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a put.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("ORDERS", b"after the close")
        });
        // One MQCONN for every put: one connection accepted.
        let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
        for n in 0..SENDS {
            let put = session.next_put().expect("put").expect("one");
            assert_eq!(put.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end
            .accept_one(&listener, &[])
            .expect("a new connection");
        let last = again.next_put().expect("put").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.connections.opened(), 2);
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

    /// The payloads a message must carry whole, and one at `MAXMSGL`.
    fn payloads() -> Vec<(&'static str, Vec<u8>)> {
        let mut payloads = edge_payloads();
        payloads.extend([("the brim", vec![b'q'; DEFAULT_MAX_MESSAGE as usize])]);
        payloads
    }

    #[test]
    fn a_loopback_round_puts_one_message_and_takes_it_at_the_queue_manager() {
        let mq = IbmMqTransport::loopback();
        let arrived = mq.round(b"order\0\xff").expect("round");
        assert_eq!(arrived.bytes, b"order\0\xff");
        assert!(
            arrived
                .origin_uri
                .starts_with("ibm-mq://QM1/ORDERS?msgid=414d5120"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(mq.name(), "ibm-mq");
        assert!(mq.refuses(&[0, 0xff]).is_none(), "bytes are bytes");
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let mq = IbmMqTransport::loopback();
        assert_eq!(mq.ceiling(), Some(4 * 1024 * 1024));
        for (name, payload) in payloads() {
            let arrived = mq.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'q'; DEFAULT_MAX_MESSAGE as usize + 1];
        let failure = mq.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("MQRC 2031"), "{failure}");
    }
}
