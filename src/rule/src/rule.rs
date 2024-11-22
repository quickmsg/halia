use std::collections::HashMap;

use common::{
    error::{HaliaError, HaliaResult},
    log::Logger,
};
use functions::{aggregation, computes, filter, merge::merge, window};
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{debug, error};
use types::rules::{
    AppSinkNode, AppSourceNode, Conf, DataboardNode, DeviceSinkNode, DeviceSourceNode, NodeType,
    ReadRuleNodeResp, ReadRuleResp,
};

use crate::{
    graph::Graph,
    segment::{start_segment, BlackHole},
};

pub struct Rule {
    id: String,
    stop_signal_tx: broadcast::Sender<()>,
    logger: Logger,
}

impl Rule {
    pub async fn new(id: String, conf: &Conf) -> HaliaResult<Self> {
        let (stop_signal_tx, _) = broadcast::channel(1);
        let mut rule = Self {
            id: id,
            stop_signal_tx: stop_signal_tx.clone(),
            logger: Logger::new(),
        };
        rule.start(conf).await?;

        Ok(rule)
    }

    pub fn get_log_status(&self) -> bool {
        self.logger.status()
    }

    async fn start(&mut self, conf: &Conf) -> HaliaResult<()> {
        let mut node_map = HashMap::new();
        for node in conf.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut graph = Graph::new(&conf);

        let source_indexes = graph.take_source_indexes();
        let mut receivers = HashMap::new();
        for index in source_indexes {
            let node = conf.nodes.iter().find(|node| node.index == index).unwrap();
            let cnt = graph.get_output_cnt_by_index(index);
            let rxs = match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    devices::get_source_rxs(&source_node.device_id, &source_node.source_id, cnt)
                        .await?
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    apps::get_source_rxs(&source_node.app_id, &source_node.source_id, cnt).await?
                }
                _ => return Err(HaliaError::Common(format!("{:?} 不是源节点", node))),
            };
            receivers.insert(index, rxs);
        }

        let sink_indexes = graph.take_sink_indexes();
        let mut senders = HashMap::new();
        for index in sink_indexes {
            let node = conf.nodes.iter().find(|node| node.index == index).unwrap();
            let cnt = graph.get_input_cnt_by_index(index);
            let txs = match node.node_type {
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    devices::get_sink_txs(&sink_node.device_id, &sink_node.sink_id, cnt).await?
                }
                NodeType::AppSink => {
                    let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                    apps::get_sink_txs(&sink_node.app_id, &sink_node.sink_id, cnt).await?
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    databoard::get_data_txs(
                        &databoard_node.databoard_id,
                        &databoard_node.data_id,
                        cnt,
                    )
                    .await?
                }
                NodeType::BlackHole => {
                    let mut black_hole = BlackHole::new(self.logger.get_logger_item());
                    let txs = black_hole.get_txs(cnt);
                    black_hole.run(self.stop_signal_tx.subscribe());
                    txs
                }

                _ => unreachable!(),
            };
            senders.insert(node.index, txs);
        }

        let segments = graph.get_segments();
        for segment in segments {
            let mut functions = vec![];
            let mut indexes = vec![];
            for index in segment {
                let node = node_map.get(&index).unwrap();
                match node.node_type {
                    NodeType::Merge => {
                        let rxs = graph.get_rxs(&index, &mut receivers, &mut senders);
                        let txs = graph.get_txs(&index, &mut receivers, &mut senders);
                        merge::run(rxs, txs, self.stop_signal_tx.subscribe());
                        break;
                    }
                    NodeType::Window => {
                        let rxs = graph.get_rxs(&index, &mut receivers, &mut senders);
                        let txs = graph.get_txs(&index, &mut receivers, &mut senders);
                        let conf: types::rules::functions::window::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        window::run(conf, rxs, txs, self.stop_signal_tx.subscribe()).unwrap();
                        break;
                    }
                    NodeType::Filter => {
                        let conf: types::rules::functions::filter::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        functions.push(filter::new(conf, self.logger.get_logger_item())?);
                        indexes.push(index);
                    }
                    NodeType::Computer => {
                        let conf: types::rules::functions::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        functions.push(computes::new(conf)?);
                        indexes.push(index);
                    }
                    NodeType::Aggregation => {
                        let conf: types::rules::functions::aggregation::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        functions.push(aggregation::new(conf)?);
                        indexes.push(index);
                    }
                    _ => {
                        debug!("{:?}", node);
                    }
                }
            }

            // merge、window 节点的indexes长度为0
            if indexes.len() > 0 {
                let rxs = graph.get_rxs(&indexes.first().unwrap(), &mut receivers, &mut senders);
                let txs = graph.get_txs(indexes.last().unwrap(), &mut receivers, &mut senders);

                start_segment(rxs, functions, txs, self.stop_signal_tx.subscribe());
            }
        }

        for (index, senders) in senders {
            if senders.len() == 0 {
                continue;
            }

            for sender in senders {
                let index = graph.get_remain_previous_ids(index)[0];
                let rx = receivers.get_mut(&index).unwrap().pop().unwrap();
                run_direct_link(sender, rx, self.stop_signal_tx.subscribe());
            }
        }

        Ok(())
    }

    pub async fn start_log(&mut self) {
        self.logger.start(&self.id).await;
    }

    pub async fn stop_log(&mut self) {
        self.logger.stop().await;
    }

    pub async fn read(db_rule: storage::rule::Rule) -> HaliaResult<ReadRuleResp> {
        let mut nodes = Vec::with_capacity(db_rule.conf.nodes.len());
        for node in db_rule.conf.nodes.into_iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = devices::get_rule_info(types::devices::QueryRuleInfoParams {
                        device_id: source_node.device_id,
                        source_id: Some(source_node.source_id),
                        sink_id: None,
                    })
                    .await?;
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::DeviceSource,
                        data: Some(serde_json::to_value(rule_info).unwrap()),
                    });
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = apps::get_rule_info(types::apps::QueryRuleInfo {
                        app_id: source_node.app_id,
                        source_id: Some(source_node.source_id),
                        sink_id: None,
                    })
                    .await?;
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::AppSource,
                        data: Some(serde_json::to_value(rule_info).unwrap()),
                    });
                }
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = devices::get_rule_info(types::devices::QueryRuleInfoParams {
                        device_id: sink_node.device_id,
                        source_id: None,
                        sink_id: Some(sink_node.sink_id),
                    })
                    .await?;
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::DeviceSink,
                        data: Some(serde_json::to_value(rule_info).unwrap()),
                    });
                }
                NodeType::AppSink => {
                    let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = apps::get_rule_info(types::apps::QueryRuleInfo {
                        app_id: sink_node.app_id,
                        source_id: None,
                        sink_id: Some(sink_node.sink_id),
                    })
                    .await?;
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::AppSink,
                        data: Some(serde_json::to_value(rule_info).unwrap()),
                    });
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = databoard::get_rule_info(types::databoard::QueryRuleInfo {
                        databoard_id: databoard_node.databoard_id,
                        data_id: databoard_node.data_id,
                    })
                    .await?;
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::Databoard,
                        data: Some(serde_json::to_value(rule_info).unwrap()),
                    });
                }
                NodeType::Merge => {
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::Merge,
                        data: None,
                    });
                }
                NodeType::Window => {
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::Window,
                        data: Some(node.conf),
                    });
                }
                NodeType::Filter => {
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::Filter,
                        data: Some(node.conf),
                    });
                }
                NodeType::Computer => {
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::Computer,
                        data: Some(node.conf),
                    });
                }
                NodeType::BlackHole => {
                    nodes.push(ReadRuleNodeResp {
                        index: node.index,
                        node_type: NodeType::BlackHole,
                        data: None,
                    });
                }
                NodeType::Aggregation => nodes.push(ReadRuleNodeResp {
                    index: node.index,
                    node_type: NodeType::Aggregation,
                    data: Some(node.conf),
                }),
            }
        }

        Ok(ReadRuleResp {
            id: db_rule.id,
            name: db_rule.name,
            status: db_rule.status,
            nodes,
            edges: db_rule.conf.edges,
        })
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if let Err(e) = self.stop_signal_tx.send(()) {
            error!("rule stop send signal err:{}", e);
        }

        storage::rule::reference::deactive(&self.id).await?;

        Ok(())
    }

    pub async fn update(&mut self, _old_conf: Conf, new_conf: Conf) -> HaliaResult<()> {
        self.stop().await?;
        self.start(&new_conf).await?;
        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        storage::rule::reference::delete_many_by_rule_id(&self.id).await?;

        Ok(())
    }
}

fn run_direct_link(
    tx: UnboundedSender<RuleMessageBatch>,
    mut rx: UnboundedReceiver<RuleMessageBatch>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            select! {
                Some(rmb) = rx.recv() => {
                    _ = tx.send(rmb);
                }

                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });
}
