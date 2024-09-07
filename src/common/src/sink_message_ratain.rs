use std::{
    collections::VecDeque,
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};

use message::MessageBatch;
use types::MessageRetain;

pub trait SinkMessageRetain: Debug + Sync + Send {
    fn push(&mut self, mb: MessageBatch);
    fn pop(&mut self) -> Option<MessageBatch>;
}

pub fn new(mr: &MessageRetain) -> Box<dyn SinkMessageRetain> {
    match mr.typ {
        types::MessageRetainType::All => Box::new(SinkMessageRetainAll {
            mbs: VecDeque::new(),
        }),
        types::MessageRetainType::None => Box::new(SinkMessageRetainNone {}),
        types::MessageRetainType::LatestCount => Box::new(SinkMessageRetainCount {
            count: *mr.count.as_ref().unwrap(),
            mbs: VecDeque::new(),
        }),
        types::MessageRetainType::LatestTime => Box::new(SinkMessageRetainTime {
            duration: *mr.time.as_ref().unwrap(),
            mbs: VecDeque::new(),
        }),
    }
}

#[derive(Debug)]
pub struct SinkMessageRetainAll {
    mbs: VecDeque<MessageBatch>,
}

impl SinkMessageRetain for SinkMessageRetainAll {
    fn push(&mut self, mb: MessageBatch) {
        self.mbs.push_back(mb);
    }

    fn pop(&mut self) -> Option<MessageBatch> {
        self.mbs.pop_front()
    }
}

#[derive(Debug)]
pub struct SinkMessageRetainNone {}

impl SinkMessageRetain for SinkMessageRetainNone {
    fn push(&mut self, _mb: MessageBatch) {}

    fn pop(&mut self) -> Option<MessageBatch> {
        None
    }
}

#[derive(Debug)]
pub struct SinkMessageRetainCount {
    count: usize,
    mbs: VecDeque<MessageBatch>,
}

impl SinkMessageRetain for SinkMessageRetainCount {
    fn push(&mut self, mb: MessageBatch) {
        self.mbs.push_back(mb);
        if self.mbs.len() > self.count {
            self.mbs.pop_front();
        }
    }

    fn pop(&mut self) -> Option<MessageBatch> {
        self.mbs.pop_front()
    }
}

#[derive(Debug)]
struct SinkMessageRetainTime {
    // 秒
    duration: u64,
    mbs: VecDeque<MessageBatch>,
}

impl SinkMessageRetainTime {
    fn remove_expire_mbs(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.mbs.retain(|mb| mb.get_ts() + self.duration < now);
    }
}

impl SinkMessageRetain for SinkMessageRetainTime {
    fn push(&mut self, mb: MessageBatch) {
        self.mbs.push_back(mb);
        self.remove_expire_mbs();
    }

    fn pop(&mut self) -> Option<MessageBatch> {
        self.remove_expire_mbs();
        self.mbs.pop_front()
    }
}
