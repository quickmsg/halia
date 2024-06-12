use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};
use types::rule::{CreateGraph, Status};

pub struct Manager {
    pub graphs: HashMap<String, Graph>,
}

pub struct Graph {
    pub status: Status,
    pub create_graph: CreateGraph,
    pub graph: super::graph::Graph,
}

pub static GRAPH_MANAGER: LazyLock<Mutex<Manager>> = LazyLock::new(|| {
    Mutex::new(Manager {
        graphs: HashMap::new(),
    })
});

pub fn stop(name: &str) -> Result<()> {
    Ok(())
}

impl Manager {
    pub fn register(&mut self, create_graph: CreateGraph) -> Result<()> {
        self.check_duplicate(&create_graph.name)?;

        let graph_name = create_graph.name.clone();

        let graph = Graph {
            status: Status::Stopped,
            graph: super::graph::new(&create_graph),
            create_graph: create_graph,
        };

        self.graphs.insert(graph_name, graph);

        Ok(())
    }

    fn check_duplicate(&self, name: &str) -> Result<()> {
        if self.graphs.contains_key(name) {
            bail!("已存在");
        }
        Ok(())
    }

    pub async fn run(&self, name: String) -> HaliaResult<()> {
        let graph = self.graphs.get(&name);
        match graph {
            Some(graph) => match graph.graph.run().await {
                Ok(_) => Ok(()),
                Err(_) => todo!(),
            },
            None => return Err(HaliaError::NotFound),
        }
    }
}
