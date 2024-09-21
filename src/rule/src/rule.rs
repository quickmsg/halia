use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use common::{
    error::HaliaResult,
    storage::{self, rule_ref},
};
use functions::{computes, filter, merge::merge::Merge, metadata, window};
use message::{MessageBatch, MessageValue};
use tokio::sync::{broadcast, mpsc};
use tracing::error;
use types::rules::{
    functions::{ComputerConf, FilterConf, WindowConf},
    AppSinkNode, AppSourceNode, CreateUpdateRuleReq, DataboardNode, DeviceSinkNode,
    DeviceSourceNode, LogNode, Node, NodeType, ReadRuleNodeResp, SearchRulesItemResp,
};

use crate::{
    add_rule_on_count,
    log::Logger,
    segment::{get_3d_ids, start_segment, take_source_ids},
    sub_rule_on_count,
};

pub struct Rule {
    pub id: String,
    conf: CreateUpdateRuleReq,
    pub on: bool,
    pub stop_signal_tx: Option<broadcast::Sender<()>>,
    logger: Option<Logger>,
}

impl Rule {
    pub async fn new(rule_id: String, req: CreateUpdateRuleReq) -> HaliaResult<Self> {
        for node in req.ext.nodes.iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    storage::rule_ref::insert(
                        &rule_id,
                        &source_node.device_id,
                        &source_node.source_id,
                    )
                    .await?;
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    storage::rule_ref::insert(
                        &rule_id,
                        &source_node.app_id,
                        &source_node.source_id,
                    )
                    .await?;
                }
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    storage::rule_ref::insert(&rule_id, &sink_node.device_id, &sink_node.sink_id)
                        .await?;
                }
                NodeType::AppSink => {
                    let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                    storage::rule_ref::insert(&rule_id, &sink_node.app_id, &sink_node.sink_id)
                        .await?;
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    storage::rule_ref::insert(
                        &rule_id,
                        &databoard_node.databoard_id,
                        &databoard_node.data_id,
                    )
                    .await?;
                }
                _ => {}
            }
        }

        Ok(Self {
            on: false,
            id: rule_id,
            conf: req,
            stop_signal_tx: None,
            logger: None,
        })
    }

    pub fn search(&self) -> SearchRulesItemResp {
        SearchRulesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            on: self.on,
        }
    }

    pub async fn read(&self) -> HaliaResult<Vec<ReadRuleNodeResp>> {
        let mut read_rule_node_resp = vec![];
        // let mut read_rule_resp = ReadRuleResp { nodes: vec![] };
        for node in self.conf.ext.nodes.iter() {
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

    pub async fn start(&mut self) -> Result<()> {
        add_rule_on_count();

        let (stop_signal_tx, _) = broadcast::channel(1);

        let (incoming_edges, outgoing_edges) = self.conf.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::<usize, Node>::new();
        for node in self.conf.ext.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = self.conf.ext.nodes.iter().map(|node| node.index).collect();

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
            self.on = false;
            rule_ref::deactive(&self.id).await?;
            return Err(e.into());
        } else {
            rule_ref::active(&self.id).await?;
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
                            match Merge::new(rxs, tx) {
                                Ok(mut merge) => {
                                    merge.run().await;
                                }
                                Err(e) => error!("create merge err:{}", e),
                            }
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
                            window::run(window_conf, rx, tx, stop_signal_tx.subscribe()).unwrap();
                        }
                        NodeType::Filter => {
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
                                        Logger::new(&self.id, stop_signal_tx.subscribe()).await?;
                                    let tx = logger.get_mb_tx();
                                    self.logger = Some(logger);
                                    tx
                                }
                            };
                            functions.push(metadata::new_add_metadata(
                                "log".to_string(),
                                MessageValue::String(log_node.name),
                            ));
                            mpsc_tx = Some(tx);
                        }
                    }
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
                        stop_signal_tx.subscribe(),
                    );
                }
            }
        }

        self.stop_signal_tx = Some(stop_signal_tx);
        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        sub_rule_on_count();

        if let Err(e) = self.stop_signal_tx.as_ref().unwrap().send(()) {
            error!("rule stop send signal err:{}", e);
        }

        storage::rule_ref::deactive(&self.id).await?;

        Ok(())
    }

    pub async fn update(&mut self, req: CreateUpdateRuleReq) -> HaliaResult<()> {
        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if self.on && restart {
            self.stop().await?;
            self.start().await?;
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        storage::rule_ref::delete_many_by_rule_id(&self.id).await?;

        Ok(())
    }

    pub async fn tail_log(&self) -> Result<()> {
        // let (tx, rx) = tokio::sync::mpsc::channel(16);

        // let file = File::open("xxx").await?;
        // let mut reader = BufReader::new(file).lines();

        // while let Some(line) = reader.next_line().await? {
        //     tx.send(line).await?;
        // }

        // tokio::spawn(async move {
        //     loop {
        //         if let Some(line) = reader.next_line().await? {
        //             tx.send(line).await?;
        //         } else {
        //             time::sleep(Duration::from_millis(30)).await;
        //         }
        //     }
        // });

        // Body::from_stream();
        // StreamBody::new(rx);

        // StreamBody::new(rx);

        Ok(())
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
