use std::collections::HashMap;

use common::{
    error::{HaliaError, HaliaResult},
    log::Logger,
};
use functions::{computes, filter, merge::merge};
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    },
};
use tracing::{debug, error};
use types::rules::{
    AppSinkNode, AppSourceNode, DataboardNode, DeviceSinkNode, DeviceSourceNode, Node, NodeType,
    ReadRuleNodeResp, RuleConf,
};

use crate::segment::{get_segments, start_segment, take_sink_ids, take_source_ids, BlackHole};

pub struct Rule {
    id: String,
    stop_signal_tx: broadcast::Sender<()>,

    logger: Logger,
}

impl Rule {
    pub async fn new(id: String, conf: &RuleConf) -> HaliaResult<Self> {
        let (stop_signal_tx, _) = broadcast::channel(1);
        let mut rule = Self {
            id: id,
            stop_signal_tx: stop_signal_tx.clone(),
            logger: Logger::new(),
        };
        rule.start(conf).await?;

        Ok(rule)
    }

    async fn start(&mut self, conf: &RuleConf) -> HaliaResult<()> {
        let (incoming_edges, outgoing_edges) = conf.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::new();
        for node in conf.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = conf.nodes.iter().map(|node| node.index).collect();

        let source_ids =
            take_source_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);
        debug!("{:?}", source_ids);
        let mut receivers =
            Self::get_source_rxs(source_ids, &node_map, &tmp_outgoing_edges).await?;

        let sink_ids = take_sink_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);
        debug!("{:?}", sink_ids);
        let mut senders = self
            .get_sink_txs(sink_ids, &node_map, &incoming_edges)
            .await?;

        debug!("{:?}", ids);

        // if let Some(e) = error {
        //     storage::rule::reference::deactive(&self.id).await?;
        //     return Err(e.into());
        // } else {
        //     storage::rule::reference::active(&self.id).await?;
        // }

        let segments = get_segments(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        );

        debug!("{:?}", receivers);
        debug!("segments:{:?}", segments);
        debug!("{:?}", senders);

        for segment in segments {
            let mut functions = vec![];
            let mut indexes = vec![];
            for index in segment {
                let node = node_map.get(&index).unwrap();
                match node.node_type {
                    NodeType::Merge => {
                        let source_ids = incoming_edges.get(&index).unwrap();
                        let mut rxs = vec![];
                        for source_id in source_ids {
                            rxs.push(receivers.get_mut(source_id).unwrap().pop().unwrap());
                        }
                        let mut n_rxs = vec![];
                        let mut n_txs = vec![];
                        let cnt = outgoing_edges.get(&index).unwrap().len();
                        for _ in 0..cnt {
                            let (tx, rx) = unbounded_channel();
                            n_rxs.push(rx);
                            n_txs.push(tx);
                        }

                        receivers.insert(index, n_rxs);
                        merge::run(rxs, n_txs, self.stop_signal_tx.subscribe());
                        break;
                    }
                    // NodeType::Window => {
                    //     let source_ids = incoming_edges.get(&id).unwrap();
                    //     let source_id = source_ids[0];
                    //     let rx = receivers.get_mut(&source_id).unwrap().pop().unwrap();
                    //     let (tx, _) = broadcast::channel::<MessageBatch>(16);
                    //     let mut rxs = vec![];
                    //     let cnt = outgoing_edges.get(&id).unwrap().len();
                    //     for _ in 0..cnt {
                    //         rxs.push(tx.subscribe());
                    //     }
                    //     receivers.insert(id, rxs);
                    //     let window_conf: WindowConf =
                    //         serde_json::from_value(node.conf.clone())?;
                    //     window::run(window_conf, rx, tx, self.stop_signal_tx.subscribe())
                    //         .unwrap();
                    // }
                    NodeType::Filter => {
                        let conf: types::rules::functions::filter::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        functions.push(filter::new(conf, self.logger.get_logger_item())?);
                        indexes.push(index);
                    }
                    NodeType::Computer => {
                        let conf: types::rules::functions::computer::Conf =
                            serde_json::from_value(node.conf.clone())?;
                        functions.push(computes::new(conf)?);
                        indexes.push(index);
                    }
                    NodeType::Operator => todo!(),
                    _ => {
                        debug!("{:?}", node);
                    }
                }
            }

            // merge 节点的indexes长度为0
            if indexes.len() > 0 {
                let mut segment_rxs = vec![];
                let mut segment_txs = vec![];
                debug!("{:?}", indexes);
                let source_ids = incoming_edges.get(&indexes.first().unwrap()).unwrap();
                for source_id in source_ids {
                    debug!("{}", source_id);
                    match receivers.get_mut(source_id) {
                        Some(receiver_rxs) => {
                            debug!("{:?}", receiver_rxs);
                            segment_rxs.push(receiver_rxs.pop().unwrap());
                        }
                        None => {
                            let (tx, rx) = unbounded_channel::<RuleMessageBatch>();
                            match senders.get_mut(source_id) {
                                Some(sender_txs) => {
                                    sender_txs.push(tx);
                                }
                                None => {
                                    senders.insert(*source_id, vec![tx]);
                                }
                            }
                            segment_rxs.push(rx);
                        }
                    }
                }

                let sink_ids = outgoing_edges.get(&indexes.last().unwrap()).unwrap();
                for sink_id in sink_ids {
                    match senders.get_mut(sink_id) {
                        Some(sender_txs) => {
                            segment_txs.push(sender_txs.pop().unwrap());
                        }
                        None => {
                            let (tx, rx) = unbounded_channel::<RuleMessageBatch>();
                            match receivers.get_mut(sink_id) {
                                Some(receiver_rxs) => {
                                    receiver_rxs.push(rx);
                                }
                                None => {
                                    receivers.insert(*sink_id, vec![rx]);
                                }
                            }
                            segment_txs.push(tx);
                        }
                    }
                }
                start_segment(
                    segment_rxs,
                    functions,
                    segment_txs,
                    self.stop_signal_tx.subscribe(),
                );
            }
        }

        // for (index, mut senders) in senders {
        //     let index = incoming_edges.get_mut(&index).unwrap().pop().unwrap();
        //     let rx = receivers.get_mut(&index).unwrap().pop().unwrap();
        //     run_direct_link(senders.pop().unwrap(), rx, self.stop_signal_tx.subscribe());
        // }

        Ok(())
    }

    pub async fn start_log(&mut self) {
        self.logger.start(&self.id).await;
    }

    pub async fn stop_log(&mut self) {
        self.logger.stop().await;
    }

    pub async fn read(conf: RuleConf) -> HaliaResult<Vec<ReadRuleNodeResp>> {
        let mut read_rule_node_resp = vec![];
        for node in conf.nodes.iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info =
                        devices::get_rule_info(types::devices::device::QueryRuleInfoParams {
                            device_id: source_node.device_id,
                            source_id: Some(source_node.source_id),
                            sink_id: None,
                        })
                        .await?;
                    read_rule_node_resp.push(ReadRuleNodeResp {
                        index: node.index,
                        data: serde_json::to_value(rule_info).unwrap(),
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
                    read_rule_node_resp.push(ReadRuleNodeResp {
                        index: node.index,
                        data: serde_json::to_value(rule_info).unwrap(),
                    });
                }
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info =
                        devices::get_rule_info(types::devices::device::QueryRuleInfoParams {
                            device_id: sink_node.device_id,
                            source_id: None,
                            sink_id: Some(sink_node.sink_id),
                        })
                        .await?;
                    read_rule_node_resp.push(ReadRuleNodeResp {
                        index: node.index,
                        data: serde_json::to_value(rule_info).unwrap(),
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
                    read_rule_node_resp.push(ReadRuleNodeResp {
                        index: node.index,
                        data: serde_json::to_value(rule_info).unwrap(),
                    });
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = databoard::get_rule_info(types::databoard::QueryRuleInfo {
                        databoard_id: databoard_node.databoard_id,
                        data_id: databoard_node.data_id,
                    })
                    .await?;
                    read_rule_node_resp.push(ReadRuleNodeResp {
                        index: node.index,
                        data: serde_json::to_value(rule_info).unwrap(),
                    });
                }
                NodeType::Merge
                | NodeType::Window
                | NodeType::Filter
                | NodeType::Operator
                | NodeType::Computer
                | NodeType::BlackHole => {}
            }
        }

        Ok(read_rule_node_resp)
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if let Err(e) = self.stop_signal_tx.send(()) {
            error!("rule stop send signal err:{}", e);
        }

        storage::rule::reference::deactive(&self.id).await?;

        Ok(())
    }

    pub async fn update(&mut self, _old_conf: RuleConf, new_conf: RuleConf) -> HaliaResult<()> {
        self.stop().await?;
        self.start(&new_conf).await?;
        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        storage::rule::reference::delete_many_by_rule_id(&self.id).await?;

        Ok(())
    }

    // 获取所有输入源的rx
    async fn get_source_rxs(
        source_ids: Vec<usize>,
        node_map: &HashMap<usize, Node>,
        outgoing_edges: &HashMap<usize, Vec<usize>>,
    ) -> HaliaResult<HashMap<usize, Vec<UnboundedReceiver<RuleMessageBatch>>>> {
        let mut receivers = HashMap::new();
        for source_id in source_ids {
            let node = node_map.get(&source_id).unwrap();
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        rxs.push(
                            devices::get_source_rx(&source_node.device_id, &source_node.source_id)
                                .await?,
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        rxs.push(
                            apps::get_source_rx(&source_node.app_id, &source_node.source_id)
                                .await?,
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                _ => return Err(HaliaError::Common(format!("{:?} 不是源节点", node))),
            }
        }
        Ok(receivers)
    }

    // 获取所有输出动作的tx
    async fn get_sink_txs(
        &mut self,
        sink_ids: Vec<usize>,
        node_map: &HashMap<usize, Node>,
        incoming_edges: &HashMap<usize, Vec<usize>>,
    ) -> HaliaResult<HashMap<usize, Vec<UnboundedSender<RuleMessageBatch>>>> {
        let mut senders = HashMap::new();
        for sink_id in sink_ids {
            let node = node_map.get(&sink_id).unwrap();
            let txs = match node.node_type {
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = incoming_edges.get(&sink_id).unwrap().len();
                    let mut txs = vec![];
                    for _ in 0..cnt {
                        txs.push(
                            devices::get_sink_tx(&sink_node.device_id, &sink_node.sink_id).await?,
                        )
                    }
                    txs
                }
                NodeType::AppSink => {
                    let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = incoming_edges.get(&sink_id).unwrap().len();
                    let mut txs = vec![];
                    for _ in 0..cnt {
                        txs.push(apps::get_sink_tx(&sink_node.app_id, &sink_node.sink_id).await?)
                    }
                    txs
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = incoming_edges.get(&sink_id).unwrap().len();
                    let mut txs = vec![];
                    for _ in 0..cnt {
                        txs.push(
                            databoard::get_data_tx(
                                &databoard_node.databoard_id,
                                &databoard_node.data_id,
                            )
                            .await?,
                        )
                    }
                    txs
                }
                NodeType::BlackHole => {
                    let mut black_hole = BlackHole::new();
                    let cnt = incoming_edges.get(&sink_id).unwrap().len();
                    let mut txs = vec![];
                    for _ in 0..cnt {
                        txs.push(black_hole.get_tx());
                    }
                    black_hole.run(self.stop_signal_tx.subscribe());
                    txs
                }

                _ => unreachable!(),
            };
            senders.insert(node.index, txs);
        }
        Ok(senders)
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
