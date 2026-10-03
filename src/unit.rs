//! One message as the unit of work it is on the queue manager: got under
//! syncpoint on the receiving connection, alone, and settled by its own
//! verdict.
//!
//! MQ commits and backs out a connection's unit of work whole, so a unit
//! holds one message: its verdict settles it and nothing else — `MQCMIT`
//! where the cycle accepted it or refused it, `MQBACK` where the cycle
//! failed, which puts that message, and only it, back on the queue to be
//! got again (at least once, never a loss). No accepted message is ever
//! backed out beside a failed one.
//!
//! MQ has no rejection of a message: a backout requeue queue (`BOQNAME`,
//! with `BOTHRESH`) is a convention an application keeps by putting a
//! message there itself. So a refused message is committed — taken off the
//! queue and not got again; the runtime audited the refusal, and from
//! Message creation on the Stream is kept in Xmip (ADR-0013).

use transport::{Acknowledgement, Pool, Verdict};

use crate::client::Client;

/// The acknowledgement of the one message got on the connection
/// `receivers` keeps under `key`: `MQCMIT` on [`Verdict::Accepted`] and
/// [`Verdict::Refused`], `MQBACK` on [`Verdict::Failed`], on the connection
/// that got it — one call and its reply. Where the queue manager closed
/// that connection meanwhile, none is opened: it backed the unit out
/// itself, and the message is got again. Where the acknowledgement is
/// dropped without a verdict nothing is settled here: the next receive
/// backs the unit out first.
#[must_use]
pub fn acknowledgement(receivers: &Pool<Client>, key: &str) -> Acknowledgement {
    let receivers = receivers.clone();
    let key = key.to_string();
    Acknowledgement::deferred(move |verdict| {
        receivers.kept(
            key.as_str(),
            "the connection that got the message is closed; \
             the queue manager backed it out, and it is got again",
            |client| match verdict {
                Verdict::Accepted | Verdict::Refused(_) => client.commit(),
                Verdict::Failed => client.back_out(),
            },
        )
    })
}
