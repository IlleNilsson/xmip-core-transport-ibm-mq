//! The queue manager's side of one conversation: what a test puts at the
//! far end, and what the playground drives.
//!
//! Not MQ. One queue manager holds named queues in memory, answers the
//! initial data with its own, checks its name on `MQCONN`, hands out
//! handles on `MQOPEN`, appends on `MQPUT` and takes from the front on
//! `MQGET`, each reply with MQ's completion and reason codes — `2058` for
//! the wrong queue manager, `2085` for a queue it does not have, `2033`
//! for an empty one. Persistence, channels, security exits and clustering
//! are a queue manager's.

use std::collections::{BTreeMap, VecDeque};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use codec::hex;
use transport::Arrived;
use transport::error::{Result, protocol_error};
use transport::socket;

use crate::client::{
    DEFAULT_MAX_MESSAGE, HANDLE_ERROR, NO_MESSAGE, QUEUE_MANAGER_NAME_ERROR, TOO_BIG,
    UNKNOWN_OBJECT,
};
use crate::descriptor::{
    InitialData, MQPMO_LENGTH, MessageDescriptor, decode_object, decode_options,
    encode_get_options, fixed, text_of,
};
use crate::segment::{
    ApiHeader, INITIAL_DATA, MQCLOSE, MQCONN, MQDISC, MQGET, MQOPEN, MQPUT, Segment, read_segment,
    write_segment,
};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client connected as this queue manager.
    Connected,
    /// The client opened this queue.
    Opened(String),
    /// The client put one message; here is the Stream.
    Put(Arrived),
    /// The client got from this queue, or found it empty.
    Got(String),
    /// The client closed a handle.
    Closed,
    /// The client disconnected.
    Disconnected,
    /// The client was refused with this reason.
    Refused(u32),
}

/// A queue manager by name, holding its queues, ready to accept.
#[derive(Clone, Debug)]
pub struct QueueManager {
    name: String,
    queues: BTreeMap<String, VecDeque<Vec<u8>>>,
    max_message: u32,
}

impl QueueManager {
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            queues: BTreeMap::new(),
            max_message: DEFAULT_MAX_MESSAGE,
        }
    }

    /// Define `queue`, empty.
    #[must_use]
    pub fn with_queue(mut self, queue: &str) -> Self {
        self.queues.entry(queue.to_string()).or_default();
        self
    }

    /// Define `queue` holding `messages`, oldest first.
    #[must_use]
    pub fn holding(mut self, queue: &str, messages: &[&[u8]]) -> Self {
        let held = self.queues.entry(queue.to_string()).or_default();
        held.extend(messages.iter().map(|m| m.to_vec()));
        self
    }

    /// The largest message this queue manager carries.
    #[must_use]
    pub const fn carrying_at_most(mut self, max_message: u32) -> Self {
        self.max_message = max_message;
        self
    }

    /// Accept one client on `listener` and answer its initial data.
    ///
    /// # Errors
    /// Where the connection could not be accepted, or the client opened
    /// with something other than initial data.
    pub fn accept(self, listener: &TcpListener, timeout: Option<Duration>) -> Result<Session> {
        let (stream, _) = socket::accept_tcp(listener, timeout)?;
        let (mut reader, mut writer) = socket::split(stream)?;
        let opening = read_segment(&mut reader)?
            .ok_or_else(|| protocol_error("a client that sent nothing"))?;
        if opening.kind != INITIAL_DATA {
            return Err(protocol_error(
                "a client that did not open with initial data",
            ));
        }
        let theirs = InitialData::decode(&opening.body)?;
        let mine = InitialData {
            max_message: self.max_message,
            channel: theirs.channel,
            queue_manager: self.name.clone(),
        };
        write_segment(
            &mut writer,
            &Segment::new(
                INITIAL_DATA,
                opening.conversation,
                opening.request,
                mine.encode(),
            ),
        )?;
        Ok(Session {
            manager: self,
            reader,
            writer,
            handles: BTreeMap::new(),
            next_handle: 2,
            next_id: 1,
        })
    }
}

pub struct Session {
    manager: QueueManager,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    handles: BTreeMap<u32, String>,
    next_handle: u32,
    next_id: u32,
}

impl Session {
    /// What `queue` holds now, oldest first.
    #[must_use]
    pub fn queue(&self, queue: &str) -> Vec<Vec<u8>> {
        self.manager
            .queues
            .get(queue)
            .map(|held| held.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The next message the client put, or `None` when it disconnected.
    ///
    /// # Errors
    /// As [`Session::next_event`].
    pub fn next_put(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_event()? {
                Some(Event::Put(arrived)) => return Ok(Some(arrived)),
                Some(Event::Disconnected) | None => return Ok(None),
                Some(_) => {}
            }
        }
    }

    /// The next call the client made, answered, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke or the client sent what a queue manager
    /// does not take.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        let Some(segment) = read_segment(&mut self.reader)? else {
            return Ok(None);
        };
        let (header, rest) = ApiHeader::decode(&segment.body)?;
        let (reply, event) = match segment.kind {
            MQCONN => self.connect(rest),
            MQOPEN => self.open(rest),
            MQPUT => self.put(header.handle, rest),
            MQGET => self.get(header.handle),
            MQCLOSE => {
                self.handles.remove(&header.handle);
                (
                    ApiHeader::call(header.handle).encode().to_vec(),
                    Event::Closed,
                )
            }
            MQDISC => (ApiHeader::call(0).encode().to_vec(), Event::Disconnected),
            other => return Err(protocol_error(format!("a {other:#04x} segment"))),
        };
        write_segment(&mut self.writer, &segment.reply(reply))?;
        Ok(Some(event))
    }

    fn connect(&self, body: &[u8]) -> (Vec<u8>, Event) {
        let asked = body.get(..48).map(text_of).unwrap_or_default();
        if !asked.is_empty() && asked != self.manager.name {
            return refused(QUEUE_MANAGER_NAME_ERROR, 0);
        }
        let mut reply = ApiHeader::call(0).encode().to_vec();
        reply.extend_from_slice(&fixed(&self.manager.name, 48));
        reply.extend_from_slice(body.get(48..).unwrap_or_default());
        (reply, Event::Connected)
    }

    fn open(&mut self, body: &[u8]) -> (Vec<u8>, Event) {
        let Ok((queue, _)) = decode_object(body) else {
            return refused(UNKNOWN_OBJECT, 0);
        };
        if !self.manager.queues.contains_key(&queue) {
            return refused(UNKNOWN_OBJECT, 0);
        }
        let handle = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(handle, queue.clone());
        let mut reply = ApiHeader::call(handle).encode().to_vec();
        reply.extend_from_slice(body);
        (reply, Event::Opened(queue))
    }

    fn put(&mut self, handle: u32, body: &[u8]) -> (Vec<u8>, Event) {
        let Some(queue) = self.handles.get(&handle).cloned() else {
            return refused(HANDLE_ERROR, handle);
        };
        let Ok((mut descriptor, rest)) = MessageDescriptor::decode(body) else {
            return refused(HANDLE_ERROR, handle);
        };
        let Ok((length, data)) = decode_options(rest, b"PMO ", MQPMO_LENGTH) else {
            return refused(HANDLE_ERROR, handle);
        };
        if length > self.manager.max_message as usize || data.len() < length {
            return refused(TOO_BIG, handle);
        }
        descriptor.message_id = self.message_id();
        let bytes = data[..length].to_vec();
        if let Some(held) = self.manager.queues.get_mut(&queue) {
            held.push_back(bytes.clone());
        }
        let origin = format!(
            "ibm-mq://{}/{queue}?msgid={}",
            self.manager.name,
            hex::encode(&descriptor.message_id)
        );
        let mut reply = ApiHeader::call(handle).encode().to_vec();
        reply.extend_from_slice(&descriptor.encode());
        (reply, Event::Put(Arrived::new(origin, bytes)))
    }

    fn get(&mut self, handle: u32) -> (Vec<u8>, Event) {
        let Some(queue) = self.handles.get(&handle).cloned() else {
            return refused(HANDLE_ERROR, handle);
        };
        let Some(bytes) = self
            .manager
            .queues
            .get_mut(&queue)
            .and_then(VecDeque::pop_front)
        else {
            return (
                ApiHeader::failed(NO_MESSAGE, handle).encode().to_vec(),
                Event::Got(queue),
            );
        };
        let mut descriptor = MessageDescriptor::datagram("xmip");
        descriptor.message_id = self.message_id();
        let mut reply = ApiHeader::call(handle).encode().to_vec();
        reply.extend_from_slice(&descriptor.encode());
        reply.extend_from_slice(&encode_get_options(&queue, bytes.len()));
        reply.extend_from_slice(&bytes);
        (reply, Event::Got(queue))
    }

    /// `AMQ `, the queue manager's name, and a number no message here
    /// shares.
    fn message_id(&mut self) -> [u8; 24] {
        let mut id = [0u8; 24];
        id[0..4].copy_from_slice(b"AMQ ");
        id[4..12].copy_from_slice(&fixed(&self.manager.name, 8));
        id[20..24].copy_from_slice(&self.next_id.to_be_bytes());
        self.next_id += 1;
        id
    }
}

fn refused(reason: u32, handle: u32) -> (Vec<u8>, Event) {
    (
        ApiHeader::failed(reason, handle).encode().to_vec(),
        Event::Refused(reason),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_queue_manager_is_defined_with_its_queues_and_what_they_hold() {
        let manager = QueueManager::new("QM1")
            .with_queue("ORDERS")
            .holding("INVOICES", &[b"one", b"two"])
            .carrying_at_most(100);
        assert_eq!(manager.queues.len(), 2);
        assert_eq!(manager.queues["INVOICES"].len(), 2);
        assert_eq!(manager.max_message, 100);
    }
}
