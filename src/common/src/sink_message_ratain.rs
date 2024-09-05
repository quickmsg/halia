use message::MessageBatch;
use types::MessageRetain;

pub struct SinkMessageRetain {
    strategy: Strategy,
    mbs: Vec<MessageBatch>,
}

impl SinkMessageRetain {
    pub fn new(mr: &MessageRetain) -> Self {
        match mr.typ {
            types::MessageRetainType::All => Self {
                strategy: Strategy::All,
                mbs: vec![],
            },
            types::MessageRetainType::LatestCount => {
                // 创建时验证
                let count = mr.count.as_ref().unwrap();
                Self {
                    strategy: Strategy::Count(*count),
                    mbs: vec![],
                }
            }
            types::MessageRetainType::LatestTime => {
                let time = mr.count.as_ref().unwrap();
                Self {
                    strategy: Strategy::Time(*time),
                    mbs: vec![],
                }
            }
        }
    }

    pub fn push(&mut self, mb: MessageBatch) {
        match self.strategy {
            Strategy::All => self.mbs.push(mb),
            Strategy::Count(n) => {
                if self.mbs.len() < n {
                    self.mbs.push(mb);
                } else {
                    self.mbs.remove(0);
                    self.mbs.push(mb);
                }
            }
            Strategy::Time(_) => todo!(),
        }
    }

    pub fn get() -> MessageBatch {
        todo!()
    }
}

pub enum Strategy {
    // 所有消息保留
    All,
    // 保留最近的n条数据
    Count(usize),
    // 保留时间内的数据，消息到达时间 + time <= 现在时间
    Time(usize),
}
