use message::MessageBatch;
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug)]
pub struct RefInfo {
    ref_rules: Vec<(Uuid, bool)>,
    active_cnt: usize,
    tx: Option<broadcast::Sender<MessageBatch>>,
}

impl RefInfo {
    pub fn new() -> Self {
        Self {
            ref_rules: vec![],
            active_cnt: 0,
            tx: None,
        }
    }

    pub fn get_tx(&self) -> &Option<broadcast::Sender<MessageBatch>> {
        &self.tx
    }

    pub fn get_ref_rules(&self) -> Vec<(Uuid, bool)> {
        self.ref_rules.clone()
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_rules.push((rule_id.clone(), false));
    }

    pub fn subscribe(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        match self.ref_rules.iter_mut().find(|(id, _)| id == rule_id) {
            Some((_, active)) => {
                self.active_cnt += 1;
                *active = true;
                match &self.tx {
                    Some(tx) => tx.subscribe(),
                    None => {
                        let (tx, rx) = broadcast::channel(16);
                        self.tx = Some(tx);
                        rx
                    }
                }
            }
            None => unreachable!(),
        }
    }

    pub fn unsubscribe(&mut self, rule_id: &Uuid) {
        match self.ref_rules.iter_mut().find(|(id, _)| id == rule_id) {
            Some((_, active)) => {
                *active = false;
                self.active_cnt -= 1;
                if self.active_cnt == 0 {
                    self.tx = None;
                }
            }
            None => unreachable!(),
        }
    }

    pub fn remove_ref(&mut self, rule_id: &Uuid) {
        self.ref_rules.retain(|(id, _)| id != rule_id);
    }

    pub fn can_stop(&self) -> bool {
        self.active_cnt == 0
    }

    pub fn can_delete(&self) -> bool {
        self.ref_rules.len() == 0
    }
}
