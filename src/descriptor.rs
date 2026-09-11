//! The structures an MQ call carries: the message descriptor, the put and
//! get options, the object descriptor, and the initial data two sides
//! open with.
//!
//! Each is MQ's own version-1 layout to the byte — `MQMD` is 324 bytes,
//! `MQPMO` 128, `MQGMO` 72, `MQOD` 168 — with text fields space-padded to
//! their width as the C headers declare them. What Xmip reads of each is
//! little: a name, a handle's options, a message id, a length. The rest is
//! carried so a queue manager sees what it expects.

use transport::error::{Result, protocol_error};

use crate::segment::be32;

pub const MQMD_LENGTH: usize = 324;
pub const MQPMO_LENGTH: usize = 128;
pub const MQGMO_LENGTH: usize = 72;
pub const MQOD_LENGTH: usize = 168;
pub const ID_LENGTH: usize = 96;

/// `MQOO_INPUT_AS_Q_DEF`.
pub const OPEN_INPUT: u32 = 0x0000_0001;
/// `MQOO_OUTPUT`.
pub const OPEN_OUTPUT: u32 = 0x0000_0010;
/// `MQOO_FAIL_IF_QUIESCING`.
pub const OPEN_FAIL_IF_QUIESCING: u32 = 0x0000_2000;
/// `MQMT_DATAGRAM`.
const DATAGRAM: u32 = 8;
/// The level of the format and protocol this crate speaks.
pub const FAP_LEVEL: u8 = 10;

/// `text` in a field `width` wide, space-padded or cut.
#[must_use]
pub fn fixed(text: &str, width: usize) -> Vec<u8> {
    let mut out = text.as_bytes().to_vec();
    out.resize(width, b' ');
    out
}

/// The text of a fixed field, its padding and any NUL gone.
#[must_use]
pub fn text_of(field: &[u8]) -> String {
    String::from_utf8_lossy(field)
        .trim_end_matches([' ', '\0'])
        .to_string()
}

/// A field of `width` at `at`, or the refusal.
fn field(bytes: &[u8], at: usize, width: usize) -> Result<&[u8]> {
    bytes
        .get(at..at + width)
        .ok_or_else(|| protocol_error(format!("a structure cut short at byte {at}")))
}

fn eyecatcher(bytes: &[u8], expected: [u8; 4], length: usize) -> Result<()> {
    if bytes.len() < length || bytes[0..4] != expected {
        return Err(protocol_error(format!(
            "not {:?}: {:?}",
            String::from_utf8_lossy(&expected),
            String::from_utf8_lossy(&bytes[..bytes.len().min(4)])
        )));
    }
    Ok(())
}

/// The message descriptor, `MQMD` version 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageDescriptor {
    pub message_id: [u8; 24],
    pub correlation_id: [u8; 24],
    pub format: String,
    pub put_application: String,
}

impl MessageDescriptor {
    /// A datagram with no id yet, the queue manager assigns one.
    #[must_use]
    pub fn datagram(put_application: &str) -> Self {
        Self {
            message_id: [0; 24],
            correlation_id: [0; 24],
            format: String::new(),
            put_application: put_application.to_string(),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(MQMD_LENGTH);
        out.extend_from_slice(b"MD  ");
        out.extend_from_slice(&1u32.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&DATAGRAM.to_be_bytes());
        out.extend_from_slice(&u32::MAX.to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&crate::segment::ENCODING.to_be_bytes());
        out.extend_from_slice(&u32::from(crate::segment::CCSID).to_be_bytes());
        out.extend_from_slice(&fixed(&self.format, 8));
        out.extend_from_slice(&u32::MAX.to_be_bytes());
        out.extend_from_slice(&2u32.to_be_bytes());
        out.extend_from_slice(&self.message_id);
        out.extend_from_slice(&self.correlation_id);
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&fixed("", 48));
        out.extend_from_slice(&fixed("", 48));
        out.extend_from_slice(&fixed("", 12));
        out.extend_from_slice(&[0u8; 32]);
        out.extend_from_slice(&fixed("", 32));
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&fixed(&self.put_application, 28));
        out.extend_from_slice(&fixed("", 8));
        out.extend_from_slice(&fixed("", 8));
        out.extend_from_slice(&fixed("", 4));
        debug_assert_eq!(out.len(), MQMD_LENGTH);
        out
    }

    /// The descriptor at the start of `bytes`, and what follows it.
    ///
    /// # Errors
    /// Where the bytes are not an `MQMD`.
    pub fn decode(bytes: &[u8]) -> Result<(Self, &[u8])> {
        eyecatcher(bytes, *b"MD  ", MQMD_LENGTH)?;
        let mut message_id = [0u8; 24];
        message_id.copy_from_slice(field(bytes, 48, 24)?);
        let mut correlation_id = [0u8; 24];
        correlation_id.copy_from_slice(field(bytes, 72, 24)?);
        Ok((
            Self {
                message_id,
                correlation_id,
                format: text_of(field(bytes, 32, 8)?),
                put_application: text_of(field(bytes, 276, 28)?),
            },
            &bytes[MQMD_LENGTH..],
        ))
    }
}

/// The put message options, `MQPMO` version 1, and the data length that
/// follows them on the wire.
#[must_use]
pub fn encode_put_options(resolved_queue: &str, data_length: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(MQPMO_LENGTH + 4);
    out.extend_from_slice(b"PMO ");
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&0x0000_2000u32.to_be_bytes());
    for _ in 0..5 {
        out.extend_from_slice(&0u32.to_be_bytes());
    }
    out.extend_from_slice(&fixed(resolved_queue, 48));
    out.extend_from_slice(&fixed("", 48));
    out.extend_from_slice(&u32::try_from(data_length).unwrap_or(u32::MAX).to_be_bytes());
    out
}

/// The get message options, `MQGMO` version 1, and the buffer length that
/// follows them on the wire.
#[must_use]
pub fn encode_get_options(resolved_queue: &str, buffer_length: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(MQGMO_LENGTH + 4);
    out.extend_from_slice(b"GMO ");
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&0x0000_2000u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&fixed(resolved_queue, 48));
    out.extend_from_slice(
        &u32::try_from(buffer_length)
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out
}

/// Past the put or get options at the start of `bytes`: the length after
/// them, and what follows.
///
/// # Errors
/// Where the bytes are not the options named by `eyecatcher`.
pub fn decode_options<'a>(
    bytes: &'a [u8],
    catcher: &[u8; 4],
    length: usize,
) -> Result<(usize, &'a [u8])> {
    eyecatcher(bytes, *catcher, length)?;
    let declared = be32(field(bytes, length, 4)?) as usize;
    Ok((declared, &bytes[length + 4..]))
}

/// The object descriptor, `MQOD` version 1: a queue by name.
#[must_use]
pub fn encode_object(queue: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(MQOD_LENGTH);
    out.extend_from_slice(b"OD  ");
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&1u32.to_be_bytes());
    out.extend_from_slice(&fixed(queue, 48));
    out.extend_from_slice(&fixed("", 48));
    out.extend_from_slice(&fixed("", 48));
    out.extend_from_slice(&fixed("", 12));
    out
}

/// The queue an object descriptor names, and what follows it.
///
/// # Errors
/// Where the bytes are not an `MQOD` naming a queue.
pub fn decode_object(bytes: &[u8]) -> Result<(String, &[u8])> {
    eyecatcher(bytes, *b"OD  ", MQOD_LENGTH)?;
    if be32(&bytes[8..12]) != 1 {
        return Err(protocol_error("an object that is not a queue"));
    }
    Ok((text_of(&bytes[12..60]), &bytes[MQOD_LENGTH..]))
}

/// The initial data each side opens with: level, sizes, channel and
/// queue manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialData {
    pub max_message: u32,
    pub channel: String,
    pub queue_manager: String,
}

impl InitialData {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(ID_LENGTH);
        out.extend_from_slice(b"ID  ");
        out.push(FAP_LEVEL);
        out.extend_from_slice(&[0, 0, 0, 0]);
        out.extend_from_slice(&50u16.to_be_bytes());
        out.extend_from_slice(
            &u32::try_from(crate::segment::MAX_TRANSMISSION)
                .unwrap_or(0)
                .to_be_bytes(),
        );
        out.extend_from_slice(&self.max_message.to_be_bytes());
        out.extend_from_slice(&999_999_999u32.to_be_bytes());
        out.extend_from_slice(&fixed(&self.channel, 20));
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&crate::segment::CCSID.to_be_bytes());
        out.extend_from_slice(&fixed(&self.queue_manager, 48));
        out.push(0);
        debug_assert_eq!(out.len(), ID_LENGTH);
        out
    }

    /// # Errors
    /// Where the bytes are not initial data.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        eyecatcher(bytes, *b"ID  ", ID_LENGTH)?;
        Ok(Self {
            max_message: be32(&bytes[15..19]),
            channel: text_of(&bytes[23..43]),
            queue_manager: text_of(&bytes[47..95]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_structure_is_its_declared_length_and_reads_back() {
        let mut md = MessageDescriptor::datagram("xmip");
        md.message_id[0] = 0x41;
        md.format = "MQSTR".to_string();
        let encoded = md.encode();
        assert_eq!(encoded.len(), MQMD_LENGTH);
        let trailing = [encoded, b"x".to_vec()].concat();
        let (back, rest) = MessageDescriptor::decode(&trailing).expect("md");
        assert_eq!(back, md);
        assert_eq!(rest, b"x");
        let pmo = encode_put_options("ORDERS", 5);
        assert_eq!(pmo.len(), MQPMO_LENGTH + 4);
        assert_eq!(
            decode_options(&pmo, b"PMO ", MQPMO_LENGTH).expect("pmo").0,
            5
        );
        let gmo = encode_get_options("", 4096);
        assert_eq!(gmo.len(), MQGMO_LENGTH + 4);
        assert_eq!(
            decode_options(&gmo, b"GMO ", MQGMO_LENGTH).expect("gmo").0,
            4096
        );
        let od = encode_object("ORDERS");
        assert_eq!(od.len(), MQOD_LENGTH);
        assert_eq!(decode_object(&od).expect("od").0, "ORDERS");
        let id = InitialData {
            max_message: 4_194_304,
            channel: "SYSTEM.DEF.SVRCONN".to_string(),
            queue_manager: "QM1".to_string(),
        };
        assert_eq!(id.encode().len(), ID_LENGTH);
        assert_eq!(InitialData::decode(&id.encode()).expect("id"), id);
    }

    #[test]
    fn a_wrong_eyecatcher_or_a_short_structure_is_refused() {
        assert!(MessageDescriptor::decode(&encode_object("Q")).is_err());
        assert!(decode_object(&[b"OD  ".to_vec(), vec![0; 10]].concat()).is_err());
        assert!(InitialData::decode(b"ID  ").is_err());
        assert_eq!(text_of(b"ORDERS  \0"), "ORDERS");
        assert_eq!(fixed("Q", 3), b"Q  ");
    }
}
