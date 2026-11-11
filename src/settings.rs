//! What an IBM MQ Location is configured with, declared once and read
//! through (ADR-0064, amendment 2026-09-26).

use transport::Configured;
use transport::destination;
use transport::error::Result;
use xcore::settings::{Applies, Fixed, Kind, Presence, Read, Setting, Settings};

use crate::{DEFAULT_CHANNEL, DEFAULT_WAIT, IbmMqTransport};

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
                meaning: "The queue a Receive Location gets every waiting message from, or a \
                          Send Location puts on.",
                applies: Applies::Both,
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
            Setting {
                name: "wait",
                kind: Kind::Duration,
                presence: Presence::Default(Fixed::Duration(DEFAULT_WAIT)),
                meaning: "How long a receive waits on an empty queue for a message to be put \
                          (MQGMO_WAIT); zero answers at once.",
                applies: Applies::Receive,
            },
        ],
    };

    /// The schemes a Location's address is entered with to choose this technology.
    const SCHEMES: &'static [&'static str] = &["ibm-mq", "mq"];

    /// The queue manager and queue settings: the address names only the
    /// listener.
    fn own_target(_address: &str) -> &str {
        destination::OWN
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let transport = Self::new(
            address,
            settings.text("queue_manager"),
            settings.text("queue"),
        )
        .on_channel(settings.text("channel"))
        .waiting(settings.optional_duration("wait").unwrap_or(DEFAULT_WAIT));
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueueManager;
    use std::time::Duration;
    use transport::{Transport, socket};
    use xcore::settings::Given;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn text(name: &str, value: &str) -> (String, Given) {
        (name.to_string(), Given::Text(value.to_string()))
    }

    #[test]
    fn ibm_mq_declares_its_settings_and_reads_through_them() {
        assert_eq!(IbmMqTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            text("queue_manager", "QM1"),
            text("queue", "ORDERS"),
            text("timeout", "15s"),
        ];
        let built =
            IbmMqTransport::open("mq.example:1414", Applies::Receive, &given).expect("built");
        assert_eq!(built.queue_manager, "QM1");
        assert_eq!(built.queue, "ORDERS");
        assert_eq!(built.channel, DEFAULT_CHANNEL);
        assert_eq!(built.timeout, Some(secs(15)));
        assert_eq!(built.wait, DEFAULT_WAIT);
        let given = [given[0].clone(), given[1].clone(), text("wait", "250ms")];
        let waiting = IbmMqTransport::open("mq.example:1414", Applies::Receive, &given);
        assert_eq!(waiting.expect("built").wait, Duration::from_millis(250));
        let Err(refused) = IbmMqTransport::open("mq.example:1414", Applies::Send, &given[1..2])
        else {
            panic!("a Send Location's queue manager is required");
        };
        assert!(
            refused.message.contains("\"queue_manager\""),
            "{}",
            refused.message
        );
        let Err(refused) = IbmMqTransport::open("mq.example:1414", Applies::Send, &given[..1])
        else {
            panic!("a Send Location's queue is required");
        };
        assert!(refused.message.contains("\"queue\""), "{}", refused.message);
    }

    #[test]
    fn a_send_location_puts_on_its_queue_and_a_target_naming_another_wins() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("binding");
        let location = format!("ibm-mq://{address}");
        let near = std::thread::spawn(move || {
            let given = [
                text("queue_manager", "QM1"),
                text("queue", "ORDERS"),
                text("timeout", "2s"),
            ];
            let near = IbmMqTransport::open(&location, Applies::Send, &given)?;
            near.send(IbmMqTransport::own_target(&location), b"own")?;
            near.send("INVOICES", b"other")
        });
        let mut session = QueueManager::new("QM1")
            .with_queue("ORDERS")
            .with_queue("INVOICES")
            .accept(&listener, Some(secs(2)))
            .expect("accepting");
        let own = session.next_put().expect("own").expect("put");
        assert!(
            own.origin_uri.starts_with("ibm-mq://QM1/ORDERS?"),
            "{}",
            own.origin_uri
        );
        let other = session.next_put().expect("other").expect("put");
        assert!(
            other.origin_uri.starts_with("ibm-mq://QM1/INVOICES?"),
            "{}",
            other.origin_uri
        );
        near.join().expect("thread").expect("putting");
        assert_eq!(session.queue("ORDERS"), [b"own".to_vec()]);
        assert_eq!(session.queue("INVOICES"), [b"other".to_vec()]);
    }
}
