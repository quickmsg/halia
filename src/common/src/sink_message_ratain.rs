use std::{
    collections::VecDeque,
    time::{SystemTime, UNIX_EPOCH},
};

use message::MessageBatch;
use types::MessageRetain;

pub struct SinkMessageRetain {
    strategy: Strategy,
    mbs: VecDeque<MessageBatch>,
}

impl SinkMessageRetain {
    pub fn new(mr: &MessageRetain) -> Self {
        match mr.typ {
            types::MessageRetainType::All => Self {
                strategy: Strategy::All,
                mbs: VecDeque::new(),
            },
            types::MessageRetainType::LatestCount => {
                // 创建时验证
                let count = mr.count.as_ref().unwrap();
                Self {
                    strategy: Strategy::Count(*count),
                    mbs: VecDeque::new(),
                }
            }
            types::MessageRetainType::LatestTime => {
                let time = mr.time.as_ref().unwrap();
                Self {
                    strategy: Strategy::Time(*time),
                    mbs: VecDeque::new(),
                }
            }
        }
    }

    pub fn push(&mut self, mb: MessageBatch) {
        match self.strategy {
            Strategy::All => self.mbs.push_back(mb),
            Strategy::Count(n) => {
                self.mbs.push_back(mb);
                if self.mbs.len() > n {
                    self.mbs.pop_front();
                }
            }
            Strategy::Time(duration) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                self.mbs.retain(|mb| mb.get_ts() + duration < now);
            }
        }
    }

    pub fn pop(&mut self) -> Option<MessageBatch> {
        match self.strategy {
            Strategy::Time(duration) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                loop {
                    let mb = self.mbs.pop_front();
                    match mb {
                        Some(mb) => {
                            if mb.get_ts() + duration > now {
                                return Some(mb);
                            }
                        }
                        None => return None,
                    }
                }
            }
            _ => self.mbs.pop_front(),
        }
    }
}

pub enum Strategy {
    // 所有消息保留
    All,
    // 保留最近的n条数据，参数必须大于0
    Count(usize),
    // 保留时间内的数据，消息到达时间 + time <= 现在时间
    Time(u64),
}
