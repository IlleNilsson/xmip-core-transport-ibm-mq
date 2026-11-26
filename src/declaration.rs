//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-09-26,
//! 2026-10-22 and 2026-11-26), for the library the surfaces load to carry
//! (open problem 48). The channel and the wait a Location takes unless
//! told otherwise are declared here, and the code takes them from here.

use std::time::Duration;

use xcore::settings::{Applies, Fixed, Kind, Presence, Setting, Settings};

/// The channel a client connects on unless told otherwise.
pub const DEFAULT_CHANNEL: &str = "SYSTEM.DEF.SVRCONN";

/// How long a receive's get waits on an empty queue when the Location
/// does not say (`wait`): a message put meanwhile is got at once, so the
/// wait adds nothing to a message's latency, and a Location stopping is
/// heard within it.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(1);

/// Every setting a Location takes beyond its address, the queue manager's
/// listener, host and port: where a Location connects.
pub const SETTINGS: &Settings = &Settings {
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
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a queue manager that stops mid-reply is waited on; \
             unbounded when left out.",
        ),
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
pub const SCHEMES: &[&str] = &["ibm-mq", "mq"];
