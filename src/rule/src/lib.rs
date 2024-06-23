#![feature(lazy_cell)]
use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use rule::Rule;
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::RwLock;
use tracing::error;
use types::rule::{CreateRuleReq, ListRuleResp};
use uuid::Uuid;

mod rule;
mod stream;

pub static GLOBAL_RULE_MANAGER: LazyLock<RuleManager> = LazyLock::new(|| RuleManager {
    rules: RwLock::new(HashMap::new()),
});

pub struct RuleManager {
    rules: RwLock<HashMap<Uuid, Rule>>,
}

impl RuleManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateRuleReq) -> HaliaResult<()> {
        let (id, persistence) = match id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };

        let rule = Rule::create(id, &req).await.unwrap();
        if persistence {
            if let Err(e) =
                persistence::rule::insert(id, serde_json::to_string(&req).unwrap()).await
            {
                error!("write rule to file err: {}", e);
            }
        }
        self.rules.write().await.insert(id, rule);

        Ok(())
    }

    pub async fn list(&self) -> HaliaResult<Vec<ListRuleResp>> {
        Ok(self
            .rules
            .read()
            .await
            .iter()
            .map(|(id, rule)| ListRuleResp {
                id: id.clone(),
                name: rule.req.name.clone(),
            })
            .collect())
    }

    pub async fn read(&self, id: Uuid) -> HaliaResult<CreateRuleReq> {
        match self.rules.read().await.get(&id) {
            Some(rule) => Ok(rule.req.clone()),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn start(&self, id: Uuid) -> HaliaResult<()> {
        match self.rules.write().await.get_mut(&id) {
            Some(rule) => match rule.start().await {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("rule send err :{}", e);
                    return Err(HaliaError::Existed);
                }
            },
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn stop(&self, id: Uuid) -> HaliaResult<()> {
        match self.rules.write().await.get_mut(&id) {
            Some(rule) => Ok(rule.stop()),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete(&self, id: Uuid) -> HaliaResult<()> {
        match self.rules.write().await.get_mut(&id) {
            Some(rule) => {
                rule.stop();
                self.rules.write().await.remove(&id);
                Ok(())
            }
            None => return Err(HaliaError::NotFound),
        }
    }
}

impl RuleManager {
    pub async fn recover(&self) -> Result<()> {
        Ok(())
    } 
}