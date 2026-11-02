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
//! - a thousand puts on one connection take a millisecond each at most.

use std::time::{Duration, Instant};

use transport::arrived::next_arrival;
use transport::{Transport, TransportError};
use xmip_core_transport_ibm_mq::{Event, IbmMqTransport};

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
        let began = Instant::now();
        for _ in 0..RECEIVES {
            assert!(near.receive()?.is_empty());
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a receive.
        assert!(took < Duration::from_millis(RECEIVES as u64), "{took:?}");
        Ok::<_, TransportError>(())
    });
    // Served until every receive's get found the queue empty.
    let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
    let mut emptied = 0;
    while emptied < RECEIVES {
        if let Event::Got(_) = session.next_event().expect("serving").expect("one") {
            emptied += 1;
        }
    }
    receiver.join().expect("thread").expect("receiving");
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
        let mut committing = Duration::ZERO;
        for _ in 0..MESSAGES {
            let (_, _, acknowledgement) = next_arrival(near.receive()?, "one")?.into_parts();
            let began = Instant::now();
            acknowledgement.acknowledge(transport::Verdict::Accepted)?;
            committing += began.elapsed();
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
    // a quarter of a millisecond on loopback; generous for a debug build
    // under load, a millisecond each.
    assert!(
        committing < Duration::from_millis(MESSAGES as u64),
        "{committing:?}"
    );
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
        let began = Instant::now();
        for n in 0..SENDS {
            near.send("ORDERS", n.to_string().as_bytes())?;
        }
        let took = began.elapsed();
        // Generous for a debug build under load: a millisecond a put.
        assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
        Ok::<_, TransportError>(())
    });
    let mut session = far_end.accept_one(&listener, &[]).expect("accepting");
    for n in 0..SENDS {
        let put = session.next_put().expect("put").expect("one");
        assert_eq!(put.bytes, n.to_string().as_bytes());
    }
    sender.join().expect("thread").expect("sending");
}
