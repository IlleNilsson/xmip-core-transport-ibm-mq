# xmip-core-transport-ibm-mq

IBM MQ transport: one message on a queue is one Stream — MQCONN, MQOPEN, MQPUT and MQGET on the MQ wire over TCP, against a queue manager or the in-process one this crate carries. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Receive Location gets, and a Send Location puts, on a connection made once per server and queue manager and kept (`transport::Pool`), each queue opened once on it and its handle kept (`Client::held`); one the queue manager closed is replaced. Until 2026-09-28 every receive and every put connected, opened, closed and disconnected.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
