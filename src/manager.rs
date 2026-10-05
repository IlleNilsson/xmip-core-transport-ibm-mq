//! The queue manager's side of one conversation: what a test puts at the
//! far end, and what the playground drives.
//!
//! Not MQ. One queue manager holds named queues in memory, answers the
//! initial data with its own, checks its name on `MQCONN`, hands out
//! handles on `MQOPEN`, appends on `MQPUT` and takes from the front on
//! `MQGET`, each reply with MQ's completion and reason codes — `2058` for
//! the wrong queue manager, `2085` for a queue it does not have, `2033`
//! for an empty one — at once, or under `MQGMO_WAIT` once its wait
//! interval is over, a message put meanwhile through [`Session::putting`]
//! answering it at once. A get under syncpoint is held in the conversation's
//! unit of work: `MQCMIT` and `MQDISC` let it go, `MQBACK` and a
//! connection that breaks put it back at the front of its queue.
//! Persistence, channels, security exits and clustering are a queue
//! manager's.

use std::collections::{BTreeMap, VecDeque};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;

use codec::hex;
use transport::Taken;
use transport::error::{Result, protocol_error};
use transport::socket;

use crate::client::{
    DEFAULT_MAX_MESSAGE, HANDLE_ERROR, NO_MESSAGE, QUEUE_MANAGER_NAME_ERROR, TOO_BIG,
    UNKNOWN_OBJECT,
};
use crate::descriptor::{
    GET_SYNCPOINT, GET_WAIT, InitialData, MQPMO_LENGTH, MessageDescriptor, decode_object,
    decode_options, encode_get_options, fixed, get_options_of, text_of,
};
use crate::segment::{
    ApiHeader, INITIAL_DATA, MQBACK, MQCLOSE, MQCMIT, MQCONN, MQDISC, MQGET, MQOPEN, MQPUT,
    Segment, read_segment, write_segment,
};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client connected as this queue manager.
    Connected,
    /// The client opened this queue.
    Opened(String),
    /// The client put one message; here is what it put.
    Put(Taken),
    /// The client got from this queue, or found it empty.
    Got(String),
    /// The client committed its unit of work: what it got is gone.
    Committed,
    /// The client backed out its unit of work, or its connection broke:
    /// what it got is back on its queue.
    BackedOut,
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
        let (putting, arriving) = channel();
        Ok(Session {
            manager: self,
            putting,
            arriving,
            reader,
            writer,
            handles: BTreeMap::new(),
            next_handle: 2,
            next_id: 1,
            unit: Vec::new(),
        })
    }
}

pub struct Session {
    manager: QueueManager,
    /// Messages put by another application while this conversation is
    /// served ([`Session::putting`]): taken onto their queue before a get,
    /// and what a waiting get waits on.
    putting: Sender<(String, Vec<u8>)>,
    arriving: Receiver<(String, Vec<u8>)>,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    handles: BTreeMap<u32, String>,
    next_handle: u32,
    next_id: u32,
    /// What was got under syncpoint and not yet committed: each queue and
    /// message, oldest first.
    unit: Vec<(String, Vec<u8>)>,
}

impl Session {
    /// Where another application puts a message — a queue and its bytes —
    /// while this conversation is served: a get waiting on an empty queue
    /// (`MQGMO_WAIT`) is answered with it at once.
    #[must_use]
    pub fn putting(&self) -> Sender<(String, Vec<u8>)> {
        self.putting.clone()
    }

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
    pub fn next_put(&mut self) -> Result<Option<Taken>> {
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
            // A connection that ends with a unit of work open backs it out.
            self.back_out();
            return Ok(None);
        };
        let (header, rest) = ApiHeader::decode(&segment.body)?;
        let (reply, event) = match segment.kind {
            MQCONN => self.connect(rest),
            MQOPEN => self.open(rest),
            MQPUT => self.put(header.handle, rest),
            MQGET => self.get(header.handle, rest),
            MQCMIT => {
                self.unit.clear();
                (ApiHeader::call(0).encode().to_vec(), Event::Committed)
            }
            MQBACK => {
                self.back_out();
                (ApiHeader::call(0).encode().to_vec(), Event::BackedOut)
            }
            MQCLOSE => {
                self.handles.remove(&header.handle);
                (
                    ApiHeader::call(header.handle).encode().to_vec(),
                    Event::Closed,
                )
            }
            MQDISC => {
                // A disconnect commits, as MQ does off z/OS.
                self.unit.clear();
                (ApiHeader::call(0).encode().to_vec(), Event::Disconnected)
            }
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
        // A put's own id is kept, as a queue manager keeps it without
        // MQPMO_NEW_MSG_ID; one assigned where it named none.
        if descriptor.message_id == [0; 24] {
            descriptor.message_id = self.message_id();
        }
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
        (reply, Event::Put(Taken::new(origin, bytes)))
    }

    fn get(&mut self, handle: u32, body: &[u8]) -> (Vec<u8>, Event) {
        let Some(queue) = self.handles.get(&handle).cloned() else {
            return refused(HANDLE_ERROR, handle);
        };
        let Ok((options, wait)) =
            MessageDescriptor::decode(body).and_then(|(_, rest)| get_options_of(rest))
        else {
            return refused(HANDLE_ERROR, handle);
        };
        while let Ok(put) = self.arriving.try_recv() {
            self.arrived(put);
        }
        if options & GET_WAIT != 0 && self.queue(&queue).is_empty() {
            // Waited on until a message is put or the interval is over.
            let interval = Duration::from_millis(u64::from(wait));
            if let Ok(put) = self.arriving.recv_timeout(interval) {
                self.arrived(put);
            }
        }
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
        reply.extend_from_slice(&encode_get_options(&queue, options, wait, bytes.len()));
        reply.extend_from_slice(&bytes);
        if options & GET_SYNCPOINT != 0 {
            self.unit.push((queue.clone(), bytes));
        }
        (reply, Event::Got(queue))
    }

    /// A message another application put, at the back of its queue.
    fn arrived(&mut self, (queue, bytes): (String, Vec<u8>)) {
        if let Some(held) = self.manager.queues.get_mut(&queue) {
            held.push_back(bytes);
        }
    }

    /// Every message of the open unit of work back at the front of its
    /// queue, in the order it was got.
    fn back_out(&mut self) {
        for (queue, bytes) in self.unit.drain(..).rev() {
            if let Some(held) = self.manager.queues.get_mut(&queue) {
                held.push_front(bytes);
            }
        }
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
