//! Xmip's side of one conversation with a queue manager: connect, open,
//! put, get, close, disconnect.
//!
//! Each call is one segment out and one reply back on the same
//! conversation, the request id counting up. The initial data exchange
//! settles the largest message the two sides will carry, and a put over
//! that is refused here with MQ's own reason, `MQRC_MSG_TOO_BIG_FOR_Q_MGR`,
//! before a byte of it is written.

use std::io::BufReader;
use std::net::TcpStream;
use std::time::Duration;

use transport::ceiling;
use transport::error::{Result, TransportError, protocol_error};
use transport::socket;

use crate::descriptor::{
    InitialData, MQGMO_LENGTH, MessageDescriptor, decode_options, encode_get_options,
    encode_object, encode_put_options, fixed,
};
use crate::segment::{
    ApiHeader, INITIAL_DATA, MQCLOSE, MQCONN, MQDISC, MQGET, MQOPEN, MQPUT, REPLY, Segment,
    read_segment, write_segment,
};

/// The largest message a queue manager carries unless told otherwise:
/// four mebibytes, `MAXMSGL` as MQ ships it.
pub const DEFAULT_MAX_MESSAGE: u32 = 4 * 1024 * 1024;
/// The channel a client connects on unless told otherwise.
pub const DEFAULT_CHANNEL: &str = "SYSTEM.DEF.SVRCONN";

/// `MQRC_NO_MSG_AVAILABLE`.
pub const NO_MESSAGE: u32 = 2033;
/// `MQRC_MSG_TOO_BIG_FOR_Q_MGR`.
pub const TOO_BIG: u32 = 2031;
/// `MQRC_Q_MGR_NAME_ERROR`.
pub const QUEUE_MANAGER_NAME_ERROR: u32 = 2058;
/// `MQRC_UNKNOWN_OBJECT_NAME`.
pub const UNKNOWN_OBJECT: u32 = 2085;
/// `MQRC_HOBJ_ERROR`.
pub const HANDLE_ERROR: u32 = 2019;

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    request: u32,
    max_message: u32,
    queue_manager: String,
}

impl Client {
    /// Connect to the queue manager `queue_manager` at `address` on
    /// `channel`, as application `application`.
    ///
    /// # Errors
    /// Where the address could not be reached, the initial data was not
    /// answered, or the queue manager refused the connection.
    pub fn connect(
        address: &str,
        queue_manager: &str,
        channel: &str,
        application: &str,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let stream = socket::connect_tcp(address, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            request: 0,
            max_message: DEFAULT_MAX_MESSAGE,
            queue_manager: queue_manager.to_string(),
        };
        let mine = InitialData {
            max_message: DEFAULT_MAX_MESSAGE,
            channel: channel.to_string(),
            queue_manager: queue_manager.to_string(),
        };
        let answer = client.call(INITIAL_DATA, mine.encode())?;
        let theirs = InitialData::decode(&answer.body)?;
        client.max_message = client.max_message.min(theirs.max_message);
        let mut body = ApiHeader::call(0).encode().to_vec();
        body.extend_from_slice(&fixed(queue_manager, 48));
        body.extend_from_slice(&fixed(application, 28));
        body.extend_from_slice(&application_type().to_be_bytes());
        body.extend_from_slice(&[0u8; 32]);
        body.extend_from_slice(&0u32.to_be_bytes());
        let reply = client.call(MQCONN, body)?;
        let (header, _) = ApiHeader::decode(&reply.body)?;
        judge(&header)?;
        Ok(client)
    }

    /// The largest message the two sides agreed to carry.
    #[must_use]
    pub const fn max_message(&self) -> u32 {
        self.max_message
    }

    /// The queue manager this client is connected to.
    #[must_use]
    pub fn queue_manager(&self) -> &str {
        &self.queue_manager
    }

    /// Open `queue` with `options`, and take its handle.
    ///
    /// # Errors
    /// Where the queue manager refused: an unknown queue is `MQRC 2085`.
    pub fn open(&mut self, queue: &str, options: u32) -> Result<u32> {
        let mut body = ApiHeader::call(0).encode().to_vec();
        body.extend_from_slice(&encode_object(queue));
        body.extend_from_slice(&options.to_be_bytes());
        let reply = self.call(MQOPEN, body)?;
        let (header, _) = ApiHeader::decode(&reply.body)?;
        judge(&header)?;
        Ok(header.handle)
    }

    /// Put `bytes` as one message on `handle`, and take the id assigned.
    ///
    /// # Errors
    /// Where the message is over what was agreed, or the queue manager
    /// refused.
    pub fn put(&mut self, handle: u32, bytes: &[u8]) -> Result<[u8; 24]> {
        ceiling::within(
            bytes.len(),
            self.max_message as usize,
            "the queue manager carries",
        )
        .map_err(|refused| reason_error(TOO_BIG, &refused.message))?;
        let mut body = ApiHeader::call(handle).encode().to_vec();
        body.extend_from_slice(&MessageDescriptor::datagram("xmip").encode());
        body.extend_from_slice(&encode_put_options("", bytes.len()));
        body.extend_from_slice(bytes);
        let reply = self.call(MQPUT, body)?;
        let (header, rest) = ApiHeader::decode(&reply.body)?;
        judge(&header)?;
        let (descriptor, _) = MessageDescriptor::decode(rest)?;
        Ok(descriptor.message_id)
    }

    /// Get the next message on `handle`: its id and its bytes, or `None`
    /// where the queue is empty.
    ///
    /// # Errors
    /// Where the queue manager refused for a reason other than an empty
    /// queue.
    pub fn get(&mut self, handle: u32) -> Result<Option<([u8; 24], Vec<u8>)>> {
        let mut body = ApiHeader::call(handle).encode().to_vec();
        body.extend_from_slice(&MessageDescriptor::datagram("").encode());
        body.extend_from_slice(&encode_get_options("", self.max_message as usize));
        let reply = self.call(MQGET, body)?;
        let (header, rest) = ApiHeader::decode(&reply.body)?;
        if header.reason == NO_MESSAGE {
            return Ok(None);
        }
        judge(&header)?;
        let (descriptor, rest) = MessageDescriptor::decode(rest)?;
        let (length, data) = decode_options(rest, b"GMO ", MQGMO_LENGTH)?;
        let data = data
            .get(..length)
            .ok_or_else(|| protocol_error("a got message shorter than its length"))?;
        Ok(Some((descriptor.message_id, data.to_vec())))
    }

    /// Close `handle`.
    ///
    /// # Errors
    /// Where the queue manager refused.
    pub fn close(&mut self, handle: u32) -> Result<()> {
        let mut body = ApiHeader::call(handle).encode().to_vec();
        body.extend_from_slice(&0u32.to_be_bytes());
        let reply = self.call(MQCLOSE, body)?;
        judge(&ApiHeader::decode(&reply.body)?.0)
    }

    /// Disconnect, and let the connection go.
    ///
    /// # Errors
    /// Where the connection broke before the reply.
    pub fn disconnect(mut self) -> Result<()> {
        let body = ApiHeader::call(0).encode().to_vec();
        let reply = self.call(MQDISC, body)?;
        judge(&ApiHeader::decode(&reply.body)?.0)
    }

    fn call(&mut self, kind: u8, body: Vec<u8>) -> Result<Segment> {
        self.request += 1;
        write_segment(&mut self.writer, &Segment::new(kind, 1, self.request, body))?;
        let reply = read_segment(&mut self.reader)?
            .ok_or_else(|| protocol_error("the queue manager closed without replying"))?;
        let expected = if kind == INITIAL_DATA {
            kind
        } else {
            kind | REPLY
        };
        if reply.kind != expected && reply.kind != kind {
            return Err(protocol_error(format!(
                "a {:#04x} segment where a {expected:#04x} reply was due",
                reply.kind
            )));
        }
        Ok(reply)
    }
}

/// `MQAT_WINDOWS_NT` or `MQAT_UNIX`, whichever this build is.
fn application_type() -> u32 {
    if cfg!(windows) { 11 } else { 6 }
}

fn judge(header: &ApiHeader) -> Result<()> {
    if header.completion == 0 {
        Ok(())
    } else {
        Err(reason_error(header.reason, "the queue manager refused"))
    }
}

/// What a reason code means to resilience: a queue manager not available,
/// a broken connection or a full queue will clear; the rest will not.
#[must_use]
pub fn reason_error(reason: u32, context: &str) -> TransportError {
    let retryable = matches!(reason, 2009 | 2053 | 2059 | 2161 | 2162 | 2195);
    TransportError {
        message: format!("{context}: MQRC {reason}"),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reason_is_judged_for_resilience() {
        assert!(
            reason_error(2059, "connecting").retryable,
            "not available yet"
        );
        assert!(reason_error(2053, "putting").retryable, "queue full");
        assert!(!reason_error(UNKNOWN_OBJECT, "opening").retryable);
        assert!(!reason_error(2035, "opening").retryable, "not authorised");
        assert!(reason_error(2085, "x").message.contains("MQRC 2085"));
    }

    #[test]
    fn a_queue_manager_that_is_not_there_is_retryable() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        drop(listener);
        let error = Client::connect(&address, "QM1", DEFAULT_CHANNEL, "xmip", None)
            .err()
            .expect("refused");
        assert!(error.retryable, "{error}");
    }
}
