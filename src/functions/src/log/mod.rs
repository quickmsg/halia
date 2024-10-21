use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use message::MessageBatch;
use tokio::sync::mpsc;

use crate::Function;

struct Log {
    name: String,
    tx: mpsc::Sender<String>,
}

pub fn new(name: String, tx: mpsc::Sender<String>) -> Box<dyn Function> {
    Box::new(Log { name, tx })
}

#[async_trait]
impl Function for Log {
    async fn call(&self, message_batch: &mut MessageBatch) -> bool {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let utc_now: DateTime<Utc> = Utc::now();
        let datetime_in_fixed_zone: DateTime<FixedOffset> = utc_now.with_timezone(&offset);
        let s = format!(
            "{} name:{}, {:?}\n",
            datetime_in_fixed_zone, self.name, message_batch
        );
        self.tx.send(s).await.unwrap();
        true
    }
}
