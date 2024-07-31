use uuid::Uuid;

#[derive(Debug)]
pub struct RefInfo {
    ref_rules: Vec<(Uuid, bool)>,
}

impl RefInfo {
    pub fn new() -> Self {
        Self { ref_rules: vec![] }
    }

    pub fn ref_rules(&self) -> Vec<(Uuid, bool)> {
        self.ref_rules.clone()
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_rules.push((rule_id.clone(), false));
    }

    pub fn active_ref(&mut self, rule_id: &Uuid) {
        todo!()
    }

    pub fn deactive_ref(&mut self, rule_id: &Uuid) {
        todo!()
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_rules.retain(|(id, _)| id != rule_id);
    }

    pub fn can_stop(&self) -> bool {
        todo!()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_rules.len() == 0
    }
}
