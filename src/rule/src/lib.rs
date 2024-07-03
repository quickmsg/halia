use anyhow::Result;
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
                persistence::rule::insert(&id, &serde_json::to_string(&req).unwrap()).await
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
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::rule::read().await {
            Ok(rules) => {
                for (id, status, data) in rules {
                    let req: CreateRuleReq = serde_json::from_str(&data)?;
                    self.create(Some(id), req).await?;
                    match status {
                        persistence::Status::Stopped => {}
                        persistence::Status::Runing => {
                            // TODO start this rule
                        }
                    }
                }
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    if let Err(e) = persistence::rule::init().await {
                        error!("{e}");
                        return Err(e.into());
                    }
                }
                std::io::ErrorKind::PermissionDenied => todo!(),
                std::io::ErrorKind::ConnectionRefused => todo!(),
                std::io::ErrorKind::ConnectionReset => todo!(),
                std::io::ErrorKind::ConnectionAborted => todo!(),
                std::io::ErrorKind::NotConnected => todo!(),
                std::io::ErrorKind::AddrInUse => todo!(),
                std::io::ErrorKind::AddrNotAvailable => todo!(),
                std::io::ErrorKind::BrokenPipe => todo!(),
                std::io::ErrorKind::AlreadyExists => todo!(),
                std::io::ErrorKind::WouldBlock => todo!(),
                std::io::ErrorKind::InvalidInput => todo!(),
                std::io::ErrorKind::InvalidData => todo!(),
                std::io::ErrorKind::TimedOut => todo!(),
                std::io::ErrorKind::WriteZero => todo!(),
                std::io::ErrorKind::Interrupted => todo!(),
                std::io::ErrorKind::Unsupported => todo!(),
                std::io::ErrorKind::UnexpectedEof => todo!(),
                std::io::ErrorKind::OutOfMemory => todo!(),
                std::io::ErrorKind::Other => todo!(),
                _ => todo!(),
            },
        }
        Ok(())
    }
}
