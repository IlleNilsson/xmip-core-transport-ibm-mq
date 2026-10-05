//! A keyed send carries its deduplication key as the message descriptor's
//! `MsgId`, the same on every attempt of one Journey; an unkeyed send
//! carries none, and the queue manager assigns one.

use std::thread;

use transport::Transport;
use xmip_core_transport_ibm_mq::IbmMqTransport;
use xmip_core_transport_ibm_mq::descriptor::message_id_of;

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_put_carries_the_journey_id_as_its_msg_id_on_every_attempt() {
    let far_end = IbmMqTransport::loopback();
    let (listener, address) = far_end.bind().expect("bound");
    let sender = thread::spawn(move || {
        let near = IbmMqTransport::new(address, "QM1", "ORDERS");
        near.send_keyed("ORDERS", b"order", KEY)?;
        near.send_keyed("ORDERS", b"order", KEY)?;
        near.send("ORDERS", b"order")
    });
    let mut session = far_end.accept_one(&listener, &[]).expect("accepted");
    let heard: Vec<String> = (0..3)
        .map(|_| {
            let put = session.next_put().expect("read").expect("put");
            assert_eq!(put.bytes, b"order");
            put.origin_uri
        })
        .collect();
    sender.join().expect("sender").expect("sent");
    let keyed = "ibm-mq://QM1/ORDERS?msgid=0b6f5a527c1e4d0a9a4e3f1d2c8b9e700000000000000000";
    assert_eq!(heard[..2], [keyed, keyed]);
    assert!(
        heard[2].contains("msgid=414d5120"),
        "assigned: {}",
        heard[2]
    );
}

#[test]
fn a_key_that_is_no_uuid_is_its_octets_cut_or_padded_to_twenty_four() {
    let mut short = [0u8; 24];
    short[..5].copy_from_slice(b"order");
    assert_eq!(message_id_of("order"), short);
    let long = "a key longer than twenty-four octets";
    assert_eq!(&message_id_of(long), &long.as_bytes()[..24]);
}
