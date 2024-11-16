use tracing::debug;
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
        match self.ref_rules.iter_mut().find(|(id, _)| id == rule_id) {
            Some((_, s)) => *s = true,
            None => debug!("未找到待激活的引用"),
        }
    }

    pub fn deactive_ref(&mut self, rule_id: &Uuid) {
        match self.ref_rules.iter_mut().find(|(id, _)| id == rule_id) {
            Some((_, s)) => *s = true,
            None => debug!("未找到待关闭激活的引用"),
        }
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_rules.retain(|(id, _)| id != rule_id);
    }

    pub fn can_stop(&self) -> bool {
        self.ref_rules
            .iter()
            .find(|(_, status)| *status == true)
            .is_none()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_rules.len() == 0
    }

    pub fn ref_cnt(&self) -> usize {
        self.ref_rules.len()
    }

    pub fn active_ref_cnt(&self) -> usize {
        self.ref_rules.iter().fold(0, |acc, (_, s)| match s {
            true => acc + 1,
            false => acc,
        })
    }

    pub fn get_rule_ref(&self) -> RuleRef {
        RuleRef {
            rule_ref_cnt: self.ref_cnt(),
            rule_active_ref_cnt: self.active_ref_cnt(),
        }
    }
}
