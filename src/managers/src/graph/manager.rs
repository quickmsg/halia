use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::RwLock;
use types::rule::{CreateGraph, ListRuleResp, Status};
use uuid::Uuid;

pub struct RuleManager {
    rules: RwLock<HashMap<Uuid, Graph>>,
}

pub struct Graph {
    pub status: Status,
    pub create_graph: CreateGraph,
    pub graph: super::graph::Graph,
}

pub static GLOBAL_GRAPH_MANAGER: LazyLock<RuleManager> = LazyLock::new(|| RuleManager {
    rules: RwLock::new(HashMap::new()),
});

pub fn stop(name: &str) -> Result<()> {
    Ok(())
}

impl RuleManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateGraph) -> Result<()> {
        let id = match id {
            Some(id) => id,
            None => Uuid::new_v4(),
        };

        let graph_name = req.name.clone();

        let graph = Graph {
            status: Status::Stopped,
            graph: super::graph::new(&req),
            create_graph: req,
        };

        self.rules.write().await.insert(id, graph);

        Ok(())
    }

    pub async fn list(&self) -> HaliaResult<Vec<ListRuleResp>> {
        Ok(self
            .rules
            .read()
            .await
            .iter()
            .map(|(id, g)| ListRuleResp {
                id: id.clone(),
                name: "todo".to_string(),
            })
            .collect())
    }

    pub async fn start(&self, id: Uuid) -> HaliaResult<()> {
        match self.rules.read().await.get(&id) {
            Some(graph) => match graph.graph.run().await {
                Ok(_) => Ok(()),
                Err(_) => todo!(),
            },
            None => return Err(HaliaError::NotFound),
        }
    }
}
