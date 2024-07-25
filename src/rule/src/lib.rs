use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use rule::Rule;
use std::sync::LazyLock;
use tokio::sync::RwLock;
use tracing::error;
use types::rules::{CreateUpdateRuleReq, SearchRulesResp};
use uuid::Uuid;

mod rule;
mod stream;

pub static GLOBAL_RULE_MANAGER: LazyLock<RuleManager> = LazyLock::new(|| RuleManager {
    rules: RwLock::new(vec![]),
});

pub struct RuleManager {
    rules: RwLock<Vec<Rule>>,
}

impl RuleManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateUpdateRuleReq) -> HaliaResult<()> {
        match Rule::new(id, req).await {
            Ok(rule) => {
                self.rules.write().await.push(rule);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn search(&self, page: usize, size: usize) -> HaliaResult<SearchRulesResp> {
        let mut data = vec![];
        for rule in self.rules.read().await.iter().rev().skip((page - 1) * size) {
            data.push(rule.search());
            if data.len() == size {
                break;
            }
        }
        Ok(SearchRulesResp {
            total: self.rules.read().await.len(),
            data,
        })
    }

    pub async fn start(&self, id: Uuid) -> HaliaResult<()> {
        match self
            .rules
            .write()
            .await
            .iter_mut()
            .find(|rule| rule.id == id)
        {
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
        match self
            .rules
            .write()
            .await
            .iter_mut()
            .find(|rule| rule.id == id)
        {
            Some(rule) => Ok(rule.stop()),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn update(&self, id: Uuid, req: CreateUpdateRuleReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&self, id: Uuid) -> HaliaResult<()> {
        match self
            .rules
            .write()
            .await
            .iter_mut()
            .find(|rule| rule.id == id)
        {
            Some(rule) => {
                rule.stop();
                // TODO delete
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
                // for (id, status, data) in rules {
                //     let req: CreateRuleReq = serde_json::from_str(&data)?;
                //     self.do_create_rule(id, &req).await?;
                //     match status {
                //         persistence::Status::Stopped => {}
                //         persistence::Status::Runing => {
                //             // TODO start this rule
                //         }
                //     }
                // }
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