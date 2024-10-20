use std::collections::HashMap;

use common::{
    constants::CHANNEL_SIZE,
    error::{HaliaError, HaliaResult},
};
use functions::{computes, filter, merge::merge, metadata, window};
use message::{MessageBatch, MessageValue};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error};
use types::rules::{
    functions::{ComputerConf, FilterConf, WindowConf},
    AppSinkNode, AppSourceNode, DataboardNode, DeviceSinkNode, DeviceSourceNode, LogNode, Node,
    NodeType, ReadRuleNodeResp, RuleConf,
};

use crate::{
    log::Logger,
    segment::{get_3d_ids, start_segment, take_source_ids},
};

pub struct Rule {
    id: String,
    stop_signal_tx: broadcast::Sender<()>,
    logger: Option<Logger>,
}

impl Rule {
    pub async fn new(id: String, conf: &RuleConf) -> HaliaResult<Self> {
        let (stop_signal_tx, _) = broadcast::channel(1);
        let mut rule = Self {
            id: id,
            stop_signal_tx: stop_signal_tx.clone(),
            logger: None,
        };
        rule.start(conf).await?;

        Ok(rule)
    }

    async fn start(&mut self, conf: &RuleConf) -> HaliaResult<()> {
        let (incoming_edges, outgoing_edges) = conf.get_edges();
        debug!("{:?}", incoming_edges);
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::<usize, Node>::new();
        for node in conf.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = conf.nodes.iter().map(|node| node.index).collect();

        let mut receivers = HashMap::new();

        let source_ids =
            take_source_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);

        let mut error = None;
        let mut device_sink_active_ref_nodes = vec![];
        let mut app_sink_active_ref_nodes = vec![];
        let mut databoard_active_ref_nodes = vec![];
        for source_id in source_ids {
            let node = node_map.get(&source_id).unwrap();
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        rxs.push(
                            match devices::get_source_rx(
                                &source_node.device_id,
                                &source_node.source_id,
                            )
                            .await
                            {
                                Ok(rx) => rx,
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            },
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        rxs.push(
                            match apps::get_source_rx(&source_node.app_id, &source_node.source_id)
                                .await
                            {
                                Ok(rx) => {
                                    // app_source_active_ref_nodes
                                    //     .push((source_node.app_id, source_node.source_id));
                                    rx
                                }
                                Err(e) => return Err(e.into()),
                            },
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                _ => unreachable!(),
            }
        }

        if let Some(e) = error {
            storage::rule::reference::deactive(&self.id).await?;
            return Err(e.into());
        } else {
            storage::rule::reference::active(&self.id).await?;
        }

        let threed_ids = get_3d_ids(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        )?;

        for twod_ids in threed_ids {
            for oned_ids in twod_ids {
                let mut functions = vec![];
                let mut ids = vec![];
                let mut mpsc_tx: Option<mpsc::Sender<MessageBatch>> = None;
                let mut broadcast_tx: Option<broadcast::Sender<MessageBatch>> = None;

                for id in oned_ids {
                    let node = node_map.get(&id).unwrap();
                    match node.node_type {
                        NodeType::DeviceSource | NodeType::AppSource => {}
                        NodeType::Merge => {
                            let source_ids = incoming_edges.get(&id).unwrap();
                            let mut rxs = vec![];
                            for source_id in source_ids {
                                rxs.push(receivers.get_mut(source_id).unwrap().pop().unwrap());
                            }
                            let (tx, _) = broadcast::channel::<MessageBatch>(16);
                            let mut n_rxs = vec![];
                            let cnt = outgoing_edges.get(&id).unwrap().len();
                            for _ in 0..cnt {
                                n_rxs.push(tx.subscribe());
                            }
                            receivers.insert(id, n_rxs);
                            merge::run(rxs, tx, self.stop_signal_tx.subscribe());
                            debug!("here");
                            break;
                        }
                        NodeType::Window => {
                            let source_ids = incoming_edges.get(&id).unwrap();
                            let source_id = source_ids[0];
                            let rx = receivers.get_mut(&source_id).unwrap().pop().unwrap();
                            let (tx, _) = broadcast::channel::<MessageBatch>(16);
                            let mut rxs = vec![];
                            let cnt = outgoing_edges.get(&id).unwrap().len();
                            for _ in 0..cnt {
                                rxs.push(tx.subscribe());
                            }
                            receivers.insert(id, rxs);
                            let window_conf: WindowConf =
                                serde_json::from_value(node.conf.clone())?;
                            window::run(window_conf, rx, tx, self.stop_signal_tx.subscribe())
                                .unwrap();
                        }
                        NodeType::Filter => {
                            debug!("{:?}", node.conf);
                            let conf: FilterConf = serde_json::from_value(node.conf.clone())?;
                            functions.push(filter::new(conf)?);
                            ids.push(id);
                        }
                        NodeType::Computer => {
                            let conf: ComputerConf = serde_json::from_value(node.conf.clone())?;
                            functions.push(computes::new(conf)?);
                            ids.push(id);
                        }
                        NodeType::DeviceSink => {
                            ids.push(id);
                            let sink_node: DeviceSinkNode =
                                serde_json::from_value(node.conf.clone())?;
                            let tx = match devices::get_sink_tx(
                                &sink_node.device_id,
                                &sink_node.sink_id,
                            )
                            .await
                            {
                                Ok(tx) => {
                                    device_sink_active_ref_nodes
                                        .push((sink_node.device_id, sink_node.sink_id));
                                    tx
                                }
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            };
                            mpsc_tx = Some(tx);
                        }
                        NodeType::AppSink => {
                            ids.push(id);
                            let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                            let tx = match apps::get_sink_tx(&sink_node.app_id, &sink_node.sink_id)
                                .await
                            {
                                Ok(tx) => {
                                    app_sink_active_ref_nodes
                                        .push((sink_node.app_id, sink_node.sink_id));
                                    tx
                                }
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            };
                            mpsc_tx = Some(tx);
                        }
                        NodeType::Databoard => {
                            ids.push(id);
                            let databoard_node: DataboardNode =
                                serde_json::from_value(node.conf.clone())?;
                            let tx = match databoard::get_data_tx(
                                &databoard_node.databoard_id,
                                &databoard_node.data_id,
                            )
                            .await
                            {
                                Ok(tx) => {
                                    databoard_active_ref_nodes.push((
                                        databoard_node.databoard_id,
                                        databoard_node.data_id,
                                    ));
                                    tx
                                }
                                Err(e) => {
                                    error = Some(e);
                                    break;
                                }
                            };
                            mpsc_tx = Some(tx);
                        }
                        NodeType::Operator => todo!(),
                        NodeType::Log => {
                            ids.push(id);
                            let log_node: LogNode = serde_json::from_value(node.conf.clone())?;
                            let tx = match &self.logger {
                                Some(logger) => logger.get_mb_tx(),
                                None => {
                                    let logger =
                                        Logger::new(&self.id, self.stop_signal_tx.subscribe())
                                            .await?;
                                    let tx = logger.get_mb_tx();
                                    self.logger = Some(logger);
                                    tx
                                }
                            };
                            functions.push(metadata::new_add_metadata(
                                "log".to_string(),
                                MessageValue::String(log_node.name),
                            ));
                            broadcast_tx = Some(tx);
                        }
                    }
                }

                if let Some(err) = error {
                    return Err(err);
                }

                if ids.len() > 0 {
                    let source_ids = incoming_edges.get(&ids[0]).unwrap();
                    let source_id = source_ids[0];
                    let rx = receivers.get_mut(&source_id).unwrap().pop().unwrap();
                    if mpsc_tx.is_none() {
                        let (tx, _) = broadcast::channel::<MessageBatch>(16);
                        let mut rxs = vec![];
                        let cnt = outgoing_edges.get(&ids.last().unwrap()).unwrap().len();
                        for _ in 0..cnt {
                            rxs.push(tx.subscribe());
                        }
                        receivers.insert(*ids.last().unwrap(), rxs);
                        broadcast_tx = Some(tx);
                    }
                    // TODO
                    start_segment(
                        rx,
                        functions,
                        mpsc_tx,
                        broadcast_tx,
                        self.stop_signal_tx.subscribe(),
                    );
                }
            }
        }

        match error {
            Some(error) => {
                storage::rule::reference::deactive(&self.id).await?;
                return Err(error);
            }
            None => {}
        }

        Ok(())
    }

    pub async fn read(conf: RuleConf) -> HaliaResult<Vec<ReadRuleNodeResp>> {
        let mut read_rule_node_resp = vec![];
        // let mut read_rule_resp = ReadRuleResp { nodes: vec![] };
        for node in conf.nodes.iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let rule_info = devices::get_rule_info(types::devices::QueryRuleInfo {
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
                    let rule_info = devices::get_rule_info(types::devices::QueryRuleInfo {
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
                | NodeType::Log => {}
            }
        }

        Ok(read_rule_node_resp)
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if let Err(e) = self.stop_signal_tx.send(()) {
            error!("rule stop send signal err:{}", e);
        }

        self.logger = None;

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

    pub fn tail_log(&self) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match &self.logger {
            Some(logger) => Ok(logger.mb_tx.subscribe()),
            None => Err(HaliaError::Common("logger为空".to_owned())),
        }
    }

    // pub async fn download_log(&self) {
    //     let file = File::open("xxx").await.unwrap();
    //     let (mut tx, rx) = mpsc::channel(16);

    //     tokio::spawn(async move {
    //         let mut reader = BufReader::new(file).lines();
    //         while let Some(line) = reader.next_line().await.unwrap() {
    //             if tx.send(Ok(line)).await.is_err() {
    //                 break;
    //             }
    //         }
    //         drop(tx);
    //     });

    //     let mut response = HttpResponse::build(StatusCode::OK)
    //         .content_type("text/plain")
    //         .streaming(rx.map(|result| match result {
    //             Ok(line) => Ok(Bytes::from(line + "\n")),
    //             Err(_) => Err(error::ErrorInternalServerError("Error reading log file")),
    //         }));

    //     response.headers_mut().insert(
    //         header::TRANSFER_ENCODING,
    //         header::HeaderValue::from_static("chunked"),
    //     );

    //     Ok(response)
    // }

    pub async fn get_log_filename(&self) -> String {
        // let file = match tokio::fs::File::open("download_file.txt").await {
        //     Ok(file) => file,
        //     Err(err) => return Err((StatusCode::NOT_FOUND, format!("File not found: {}", err))),
        // };
        // let stream = ReaderStream::new(file);
        // let body = Body::from_stream(stream);

        // let mut headers = HeaderMap::new();
        // headers.insert(
        //     header::CONTENT_TYPE,
        //     "text/plain; charset=utf-8".parse().unwrap(),
        // );
        // headers.insert(
        //     header::CONTENT_DISPOSITION,
        //     "attachment; filename=\"download_file.txt\""
        //         .parse()
        //         .unwrap(),
        // );

        // Ok((headers, body))
        todo!()
    }

    pub async fn delete_log(&self) {}
}
