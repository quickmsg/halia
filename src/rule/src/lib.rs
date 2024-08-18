use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use rule::Rule;
use std::{str::FromStr, sync::LazyLock};
use tokio::sync::RwLock;
use tracing::error;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

mod rule;
mod segment;

fn rule_not_find_err(id: Uuid) -> HaliaError {
    HaliaError::NotFound("规则".to_owned(), id)
}

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
    pub async fn get_summary(&self) -> Summary {
        let mut total = 0;
        let mut running_cnt = 0;
        let mut off_cnt = 0;

        for rule in self.rules.read().await.iter() {
            let resp = rule.search();
            total += 1;
            if resp.on {
                running_cnt += 1;
            } else {
                off_cnt += 1;
            }
        }

        Summary {
            total,
            running_cnt,
            off_cnt,
        }
    }

    pub async fn search(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchRulesResp {
        let mut i = 0;
        let mut total = 0;
        let mut data = vec![];

        for rule in self.rules.read().await.iter().rev() {
            let rule = rule.search();
            if let Some(query_name) = &query_params.name {
                if !rule.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if let Some(on) = &query_params.on {
                if rule.on != *on {
                    continue;
                }
            }

            if i >= (pagination.page - 1) * pagination.size && i < pagination.page * pagination.size
            {
                data.push(rule);
            }

            i += 1;
            total += 1;
        }

        SearchRulesResp { total, data }
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
                Err(e) => Err(HaliaError::Common(e.to_string())),
            },
            None => Err(rule_not_find_err(id)),
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
            Some(rule) => rule.stop(),
            None => Err(rule_not_find_err(id)),
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
                _ = rule.stop();
                // TODO delete
                Ok(())
            }
            None => Err(rule_not_find_err(id)),
        }
    }
}

impl RuleManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::rule::read().await {
            Ok(rule_datas) => {
                for rule_data in rule_datas {
                    if rule_data.len() == 0 {
                        continue;
                    }
                    let items = rule_data
                        .split(persistence::DELIMITER)
                        .collect::<Vec<&str>>();
                    assert_eq!(items.len(), 3);

                    let rule_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateRuleReq = serde_json::from_str(&items[2])?;
                    self.create(Some(rule_id), req).await?;
                    match items[1] {
                        "0" => {}
                        "1" => GLOBAL_RULE_MANAGER.start(rule_id).await.unwrap(),
                        _ => unreachable!(),
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
