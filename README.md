# xmip-core-transport-ibm-mq

IBM MQ transport: one message on a queue is one Stream — MQCONN, MQOPEN, MQPUT and MQGET on the MQ wire over TCP, against a queue manager or the in-process one this crate carries. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Receive Location gets, and a Send Location puts, on a connection made once per server and queue manager and kept (`transport::Pool`), each queue opened once on it and its handle kept (`Client::held`); one the queue manager closed is replaced. Until 2026-09-28 every receive and every put connected, opened, closed and disconnected.

A receive gets one message under syncpoint and waits on an empty queue: `MQGMO_WAIT` with the `wait` setting as the wait interval (one second unless the Location says otherwise; zero answers at once), so a message put meanwhile is got at once and adds nothing to its latency, and `2033` after the interval is an empty receive — an idle Location never spins, and a stopping one is heard within the interval. The reply is read for the interval beyond the connection's `timeout`. The in-process queue manager waits the same way, and a test puts while a get waits through `Session::putting`. Until 2026-10-02 a get on an empty queue answered at once, and the receive loop asked again at once. A receive gets one message under syncpoint, on a connection of its own (no put ever takes it), and that message is its own unit of work: MQ settles a connection's unit whole, so one message a unit is what lets each verdict settle its own message and nothing else. Nothing leaves the queue until the runtime's verdict after the whole receive cycle (`unit::acknowledgement`): Accepted commits it (`MQCMIT`); Refused commits it too — MQ has no rejection of a message, and a backout requeue queue (`BOQNAME`) is a convention an application keeps by putting a message there itself, so a refused message is taken off the queue and not got again, the runtime having audited the refusal and kept the Stream (ADR-0013); Failed backs it out (`MQBACK`), which puts that message, and only it, back on the queue to be got again (at least once, never a loss). No accepted message is ever backed out beside a failed one. A unit an earlier receive left without its verdict is backed out before the next get, and one whose connection broke is backed out by the queue manager. The settling is one call and its reply per message: about a quarter of a millisecond on loopback, held under a millisecond each by `a_thousand_messages_each_its_own_unit_commit_under_a_millisecond_each`. Until 2026-10-02 (the owner's ruling) a receive got every waiting message into one unit, and one failed verdict backed them all out. The in-process queue manager models it: a get under syncpoint is held in the conversation's unit, `MQCMIT` and `MQDISC` let it go, `MQBACK` and a broken connection put it back (`Event::Committed`, `Event::BackedOut`). Until 2026-10-02 a get took the message off the queue as it was got.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
