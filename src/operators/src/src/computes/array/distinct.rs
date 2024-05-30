use anyhow::Result;
use message::MessageBatch;

pub struct Distinct {
    field: String,
}

// TODO
impl Distinct {
    pub fn new(field: String) -> Result<Distinct> {
        Ok(Distinct { field })
    }

    pub fn operate(&self, mb: &mut MessageBatch) {
        let messages = mb.get_messages_mut();
        for message in messages {
            message.get_array_mut(&self.field).map(|array| {
                // TODO maybe arc
                // array.extend(self.values.clone());
            });
        }
    }
}
