//! An IBM MQ Location built from what it is configured with, its
//! declaration read through (`declaration.rs`; ADR-0064, amendments
//! 2026-09-26 and 2026-11-26).

use transport::Configured;
use transport::destination;
use transport::error::Result;
use xcore::settings::{Read, Settings};

use crate::IbmMqTransport;
use crate::declaration::{self, DEFAULT_WAIT};

impl Configured for IbmMqTransport {
    const SETTINGS: &'static Settings = declaration::SETTINGS;
    const SCHEMES: &'static [&'static str] = declaration::SCHEMES;

    /// The queue manager and queue settings: the address names only the
    /// listener.
    fn own_target(_address: &str) -> &str {
        destination::OWN
    }

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        Ok(Self::new(
            address,
            settings.text("queue_manager"),
            settings.text("queue"),
        )
        .on_channel(settings.text("channel"))
        .waiting(settings.optional_duration("wait").unwrap_or(DEFAULT_WAIT)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueueManager;
    use crate::declaration::DEFAULT_CHANNEL;
    use std::time::Duration;
    use transport::{Transport, socket};
    use xcore::settings::{Applies, Given};

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
