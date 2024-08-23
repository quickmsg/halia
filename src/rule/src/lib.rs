use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use rule::Rule;
use std::{str::FromStr, sync::LazyLock};
use tokio::sync::RwLock;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

mod rule;
mod segment;

macro_rules! rule_not_fonnd_err {
    () => {
        Err(HaliaError::NotFound("规则".to_owned()))
    };
}

pub static GLOBAL_RULE_MANAGER: LazyLock<RuleManager> = LazyLock::new(|| RuleManager {
    rules: RwLock::new(vec![]),
});

pub struct RuleManager {
    rules: RwLock<Vec<Rule>>,
}

impl RuleManager {
    pub async fn create(
        &self,
        id: Uuid,
        req: CreateUpdateRuleReq,
        persist: bool,
    ) -> HaliaResult<()> {
        let data = serde_json::to_string(&req)?;
        let rule = Rule::new(id, req).await?;
        if persist {
            persistence::create_rule(&id, &data).await?;
        }
        self.rules.write().await.push(rule);
        Ok(())
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

            if pagination.check(total) {
                data.push(rule);
            }

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
            Some(rule) => {
                if let Err(e) = rule.start().await {
                    return Err(HaliaError::Common(e.to_string()));
                }
                persistence::update_rule_status(&rule.id, persistence::Status::Runing).await?;
                Ok(())
            }
            None => rule_not_fonnd_err!(),
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
            Some(rule) => {
                rule.stop()?;
                persistence::update_rule_status(&rule.id, persistence::Status::Stopped).await?;
                Ok(())
            }
            None => rule_not_fonnd_err!(),
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
                rule.stop()?;
                persistence::delete_rule(&id).await?;
                Ok(())
            }
            None => rule_not_fonnd_err!(),
        }
    }
}

impl RuleManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::read_rules().await {
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
                    self.create(rule_id, req, false).await?;
                    match items[1] {
                        "0" => {}
                        "1" => GLOBAL_RULE_MANAGER.start(rule_id).await.unwrap(),
                        _ => panic!("文件损坏"),
                    }
                }

                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    persistence::init_rules().await?;
                    Ok(())
                }
                _ => Err(e.into()),
            },
        }
    }
}
