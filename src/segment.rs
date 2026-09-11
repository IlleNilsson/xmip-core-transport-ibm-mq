//! The transmission segment: what every MQ exchange on a socket is cut
//! into.
//!
//! Every message between a client and a queue manager travels as one or
//! more segments, each opening with a transmission segment header — the
//! `TSHM` of a multiplexed conversation: eyecatcher, length, conversation
//! and request ids, byte order, segment type, control flags, the logical
//! unit of work, encoding and CCSID — thirty-six bytes before anything
//! else. An API call follows it with a sixteen-byte API header — reply
//! length, completion code, reason code, object handle — and then the
//! structures the call carries. A message longer than the transmission
//! size the two sides agreed is cut across several segments, the first
//! flagged first and the last flagged last, and the reader here puts it
//! back together before anyone above sees it.

use std::io::{Read, Write};

use transport::error::{Result, classify, protocol_error};

/// The transmission segment header's length: `TSHM`.
pub const TSHM_LENGTH: usize = 36;
/// The API header's length.
pub const API_LENGTH: usize = 16;
/// The transmission size every segment stays under, as MQ negotiates it.
pub const MAX_TRANSMISSION: usize = 32_766;
/// The most the reader will put back together from segments.
pub const MAX_MESSAGE: usize = transport::wire::MAX_BODY;

/// Segment types, MQ's own values.
pub const INITIAL_DATA: u8 = 0x01;
pub const MQCONN: u8 = 0x81;
pub const MQDISC: u8 = 0x82;
pub const MQOPEN: u8 = 0x83;
pub const MQCLOSE: u8 = 0x84;
pub const MQGET: u8 = 0x85;
pub const MQPUT: u8 = 0x86;
/// A reply is its call's type with the reply bit.
pub const REPLY: u8 = 0x10;

const FIRST: u8 = 0x10;
const LAST: u8 = 0x20;
/// Integers, decimals and floats as the network carries them.
pub const ENCODING: u32 = 0x0000_0111;
/// UTF-8.
pub const CCSID: u16 = 1208;

/// One whole segment, as it left or as it was put back together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub kind: u8,
    pub conversation: u32,
    pub request: u32,
    pub body: Vec<u8>,
}

impl Segment {
    #[must_use]
    pub fn new(kind: u8, conversation: u32, request: u32, body: Vec<u8>) -> Self {
        Self {
            kind,
            conversation,
            request,
            body,
        }
    }

    /// The reply to this segment, the same conversation and request.
    #[must_use]
    pub fn reply(&self, body: Vec<u8>) -> Self {
        Self::new(self.kind | REPLY, self.conversation, self.request, body)
    }
}

/// The API header an API call opens with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApiHeader {
    pub reply_length: u32,
    pub completion: u32,
    pub reason: u32,
    pub handle: u32,
}

impl ApiHeader {
    /// A call on `handle`, nothing wrong yet.
    #[must_use]
    pub const fn call(handle: u32) -> Self {
        Self {
            reply_length: 0,
            completion: 0,
            reason: 0,
            handle,
        }
    }

    /// A failed reply with `reason`.
    #[must_use]
    pub const fn failed(reason: u32, handle: u32) -> Self {
        Self {
            reply_length: 0,
            completion: 2,
            reason,
            handle,
        }
    }

    #[must_use]
    pub fn encode(&self) -> [u8; API_LENGTH] {
        let mut out = [0u8; API_LENGTH];
        out[0..4].copy_from_slice(&self.reply_length.to_be_bytes());
        out[4..8].copy_from_slice(&self.completion.to_be_bytes());
        out[8..12].copy_from_slice(&self.reason.to_be_bytes());
        out[12..16].copy_from_slice(&self.handle.to_be_bytes());
        out
    }

    /// The header at the start of `body`, and what follows it.
    ///
    /// # Errors
    /// Where the body is shorter than a header.
    pub fn decode(body: &[u8]) -> Result<(Self, &[u8])> {
        if body.len() < API_LENGTH {
            return Err(protocol_error("a segment too short for an API header"));
        }
        Ok((
            Self {
                reply_length: be32(&body[0..4]),
                completion: be32(&body[4..8]),
                reason: be32(&body[8..12]),
                handle: be32(&body[12..16]),
            },
            &body[API_LENGTH..],
        ))
    }
}

/// Write `segment`, cut across as many transmissions as its body needs.
///
/// # Errors
/// Where the connection could not be written.
pub fn write_segment(writer: &mut impl Write, segment: &Segment) -> Result<()> {
    let room = MAX_TRANSMISSION - TSHM_LENGTH;
    let pieces = segment.body.len().div_ceil(room).max(1);
    for (n, piece) in segment
        .body
        .chunks(room)
        .chain(std::iter::once(&[][..]).take(usize::from(segment.body.is_empty())))
        .enumerate()
    {
        let mut flags = 0;
        if n == 0 {
            flags |= FIRST;
        }
        if n + 1 == pieces {
            flags |= LAST;
        }
        let mut out = Vec::with_capacity(TSHM_LENGTH + piece.len());
        out.extend_from_slice(b"TSHM");
        out.extend_from_slice(
            &u32::try_from(TSHM_LENGTH + piece.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        out.extend_from_slice(&segment.conversation.to_be_bytes());
        out.extend_from_slice(&segment.request.to_be_bytes());
        out.push(1);
        out.push(segment.kind);
        out.push(flags);
        out.push(0);
        out.extend_from_slice(&[0u8; 8]);
        out.extend_from_slice(&ENCODING.to_be_bytes());
        out.extend_from_slice(&CCSID.to_be_bytes());
        out.extend_from_slice(&[0u8; 2]);
        out.extend_from_slice(piece);
        writer
            .write_all(&out)
            .map_err(|e| classify("writing a segment", &e))?;
    }
    writer
        .flush()
        .map_err(|e| classify("flushing a segment", &e))
}

/// Read one whole segment, putting a cut one back together; `None` where
/// the peer closed between segments.
///
/// # Errors
/// Where the connection broke, the eyecatcher is not `TSHM`, the segment
/// is a big-endian one this reader does not take, or the pieces exceed
/// [`MAX_MESSAGE`].
pub fn read_segment(reader: &mut impl Read) -> Result<Option<Segment>> {
    let mut whole: Option<Segment> = None;
    loop {
        let mut head = [0u8; TSHM_LENGTH];
        match reader.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if whole.is_none() && e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(classify("reading a segment header", &e)),
        }
        if &head[0..4] != b"TSHM" {
            return Err(protocol_error(format!(
                "not a transmission segment: {:?}",
                String::from_utf8_lossy(&head[0..4])
            )));
        }
        let length = be32(&head[4..8]) as usize;
        if !(TSHM_LENGTH..=MAX_TRANSMISSION).contains(&length) {
            return Err(protocol_error(format!("a segment of {length} bytes")));
        }
        if head[16] != 1 {
            return Err(protocol_error(
                "a little-endian segment, which this reader does not take",
            ));
        }
        let mut piece = vec![0u8; length - TSHM_LENGTH];
        reader
            .read_exact(&mut piece)
            .map_err(|e| classify("reading a segment", &e))?;
        let flags = head[18];
        let segment = whole.get_or_insert_with(|| {
            Segment::new(
                head[17],
                be32(&head[8..12]),
                be32(&head[12..16]),
                Vec::new(),
            )
        });
        if segment.body.len() + piece.len() > MAX_MESSAGE {
            return Err(protocol_error("more segments than Xmip will put together"));
        }
        segment.body.extend_from_slice(&piece);
        if flags & LAST != 0 {
            return Ok(whole);
        }
    }
}

/// A big-endian integer from exactly four bytes.
#[must_use]
pub fn be32(bytes: &[u8]) -> u32 {
    let mut out = [0u8; 4];
    out.copy_from_slice(&bytes[..4]);
    u32::from_be_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_segment_round_trips_and_a_long_one_is_cut_and_put_together() {
        let long: Vec<u8> = (0..100_000u32)
            .map(|n| u8::try_from(n % 253).unwrap_or(0))
            .collect();
        let segment = Segment::new(MQPUT, 3, 7, long);
        let mut wire = Vec::new();
        write_segment(&mut wire, &segment).expect("writing");
        assert_eq!(wire.len(), 100_000 + 4 * TSHM_LENGTH, "four transmissions");
        assert_eq!(&wire[0..4], b"TSHM");
        assert_eq!(wire[18], FIRST, "the first is flagged first");
        let back = read_segment(&mut &wire[..]).expect("reading").expect("one");
        assert_eq!(back, segment);
        let empty = Segment::new(MQDISC, 3, 8, Vec::new());
        let mut wire = Vec::new();
        write_segment(&mut wire, &empty).expect("writing");
        assert_eq!(wire.len(), TSHM_LENGTH);
        assert_eq!(wire[18], FIRST | LAST);
        assert_eq!(read_segment(&mut &wire[..]).expect("reading"), Some(empty));
        assert_eq!(read_segment(&mut &b""[..]).expect("closed"), None);
    }

    #[test]
    fn an_api_header_encodes_and_a_wrong_eyecatcher_is_refused() {
        let header = ApiHeader::failed(2085, 2);
        let mut body = header.encode().to_vec();
        body.extend_from_slice(b"rest");
        let (back, rest) = ApiHeader::decode(&body).expect("decoding");
        assert_eq!(back, header);
        assert_eq!(rest, b"rest");
        assert!(ApiHeader::decode(b"short").is_err());
        let header = b"TSH \0\0\0\x24............................";
        let error = read_segment(&mut &header[..]).expect_err("TSH");
        assert!(!error.retryable);
    }
}
