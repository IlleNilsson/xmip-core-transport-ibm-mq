//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand receives on one connection take a millisecond each at most;
//! - a thousand messages, each its own unit, commit under a millisecond
//!   each;
//! - a thousand puts on one connection take a millisecond each at most;
//! - an empty get gives up soon after its wait, and a put meanwhile ends
//!   the next get's wait before its interval.
//!
//! Each of the thousands the millisecond beyond the round's wakes, round by
//! round beside a plain wake, the fastest of five
//! (`xcore::latency::Rounds`).

use std::time::{Duration, Instant};

use transport::arrived::next_arrival;
use transport::{Transport, TransportError};
use xcore::fastest::FIVE;
use xcore::latency::{Rounds, spread};
use xmip_core_transport_ibm_mq::{Event, IbmMqTransport};

/// What a get, a commit or a put wakes where a plain wake wakes one: the
/// queue manager at the call, and this end at its reply.
const WAKES: u32 = 2;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_thousand_receives_take_a_millisecond_each_at_most() {
    const RECEIVES: usize = 1000;
    let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = IbmMqTransport::new(address, "QM1", "ORDERS")
        .timing_out_after(secs(5))
        .waiting(Duration::ZERO);
    let receiver = std::thread::spawn(move || {
        let mut rounds = Rounds::crossing(WAKES);
        for _ in 0..RECEIVES {
            assert!(rounds.round(|| near.receive())?.is_empty());
        }
        Ok::<_, TransportError>(rounds)
    });
    // Served until every receive's get found the queue empty.
    let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
    let mut emptied = 0;
    while emptied < RECEIVES {
        if let Event::Got(_) = session.next_event().expect("serving").expect("one") {
            emptied += 1;
        }
    }
    let rounds = receiver.join().expect("thread").expect("receiving");
    rounds.hold("a receive, the queue empty");
}

#[test]
fn a_thousand_messages_each_its_own_unit_commit_under_a_millisecond_each() {
    const MESSAGES: usize = 1000;
    let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let receiver = std::thread::spawn(move || {
        let near = IbmMqTransport::new(address, "QM1", "ORDERS")
            .timing_out_after(secs(5))
            .waiting(Duration::ZERO);
        let mut committing = Rounds::crossing(WAKES);
        for _ in 0..MESSAGES {
            let (_, _, acknowledgement) = next_arrival(near.receive()?, "one")?.into_parts();
            committing.round(|| acknowledgement.acknowledge(transport::Verdict::Accepted))?;
        }
        Ok::<_, TransportError>(committing)
    });
    let messages = vec![&b"m"[..]; MESSAGES];
    let mut session = far_end.accept_one(&listener, &messages).expect("accepting");
    let mut commits = 0;
    while commits < MESSAGES {
        if session.next_event().expect("event") == Some(Event::Committed) {
            commits += 1;
        }
    }
    let committing = receiver.join().expect("thread").expect("receiving");
    // The commit each message's own unit adds: one MQCMIT and its reply,
    // a quarter of a millisecond on loopback, held to the millisecond
    // beyond its wakes. Until 2026-11-15 the thousand were summed.
    committing.hold("a message's own unit committed");
    assert_eq!(session.queue("ORDERS"), [] as [Vec<u8>; 0]);
}

#[test]
fn a_thousand_puts_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = IbmMqTransport::new(address, "QM1", "ORDERS")
        .timing_out_after(secs(5))
        .waiting(Duration::ZERO);
    let sender = std::thread::spawn(move || {
        let mut rounds = Rounds::crossing(WAKES);
        for n in 0..SENDS {
            rounds.round(|| near.send("ORDERS", n.to_string().as_bytes()))?;
        }
        Ok::<_, TransportError>(rounds)
    });
    let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
    for n in 0..SENDS {
        let put = session.next_put().expect("put").expect("one");
        assert_eq!(put.bytes, n.to_string().as_bytes());
    }
    let rounds = sender.join().expect("thread").expect("sending");
    rounds.hold("a put");
}

/// The interval a get waits on an empty queue.
const WAIT: Duration = Duration::from_millis(300);

/// One round over a far end of its own: how long a get on the empty queue
/// waited before it gave up, and how long the next get took once a message
/// was put while it waited. What each does is `src/lib.rs`'s test of the
/// same.
fn waited_and_put_meanwhile() -> (Duration, Duration) {
    let far_end = IbmMqTransport::new("127.0.0.1:0", "QM1", "ORDERS").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let (asked, waiting) = std::sync::mpsc::channel();
    let receiver = std::thread::spawn(move || {
        let near = IbmMqTransport::new(address, "QM1", "ORDERS")
            .timing_out_after(secs(5))
            .waiting(WAIT);
        let began = Instant::now();
        assert!(near.receive()?.is_empty(), "nothing was put");
        let waited = began.elapsed();
        asked.send(()).expect("told");
        let began = Instant::now();
        next_arrival(near.receive()?, "put while waiting")?.taken()?;
        Ok::<_, TransportError>((waited, began.elapsed()))
    });
    let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
    let putting = session.putting();
    let putter = std::thread::spawn(move || {
        waiting.recv().expect("asked");
        putting
            .send(("ORDERS".to_string(), b"C1".to_vec()))
            .expect("put");
    });
    while session.next_event().expect("event").is_some() {}
    putter.join().expect("putter");
    receiver.join().expect("thread").expect("receiving")
}

/// An empty get gives up soon after its wait, and a put meanwhile ends the
/// next get's wait before its interval: each the fastest of five rounds
/// (`xcore::latency::spread`). Each bound is the interval's, not the
/// millisecond `Rounds` holds a wake to, so no plain wake is measured
/// beside it. Until 2026-11-15 one round was held, among the crate's other
/// tests.
#[test]
fn an_empty_get_gives_up_after_its_wait_and_a_put_meanwhile_ends_the_next_at_once() {
    let (waited, took): (Vec<Duration>, Vec<Duration>) =
        (0..FIVE).map(|_| waited_and_put_meanwhile()).unzip();
    let waited = spread("an empty get, given up after its wait", waited);
    let took = spread("a get a put meanwhile ended", took);
    assert!(
        waited.fastest_of_five < WAIT * 3,
        "and no longer: {waited:?}"
    );
    assert!(took.fastest_of_five < WAIT, "a put ends the wait: {took:?}");
}
