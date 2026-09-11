# xmip-core-transport-ibm-mq

IBM MQ transport: one message on a queue is one Stream — MQCONN, MQOPEN, MQPUT and MQGET on the MQ wire over TCP, against a queue manager or the in-process one this crate carries. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
