use std::sync::Arc;

use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use influxdb::{Client, InfluxDbWriteable as _, Timestamp, Type};
use log::debug;
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::influxdb_v1::{Conf, SinkConf};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub conf: SinkConf,
    pub influxdb_conf: Arc<Conf>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(conf: SinkConf, influxdb_conf: Arc<Conf>) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            influxdb_conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
        };
        let join_handle = Self::event_loop(join_handle_data);

        Sink {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let influxdb_client =
            new_influxdb_client(&join_handle_data.influxdb_conf, &join_handle_data.conf);

        tokio::spawn(async move {
            let app_err = false;
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(rmb) = join_handle_data.mb_rx.recv() => {
                        if !app_err {
                            Self::send_msg_to_influxdb(&influxdb_client, &join_handle_data.conf, rmb).await;
                        } else {
                            join_handle_data.message_retainer.push(rmb.take_mb());
                        }
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(
        influxdb_client: &Client,
        conf: &SinkConf,
        rmb: RuleMessageBatch,
    ) {
        let mb = rmb.take_mb();
        let mut querys = vec![];
        for msg in mb.get_messages() {
            let timestamp = match &conf.precision {
                types::apps::influxdb_v1::Precision::Nanoseconds => Timestamp::Nanoseconds(0),
                types::apps::influxdb_v1::Precision::Microseconds => Timestamp::Microseconds(0),
                types::apps::influxdb_v1::Precision::Milliseconds => Timestamp::Milliseconds(0),
                types::apps::influxdb_v1::Precision::Seconds => Timestamp::Seconds(0),
                types::apps::influxdb_v1::Precision::Minutes => Timestamp::Minutes(0),
                types::apps::influxdb_v1::Precision::Hours => Timestamp::Hours(0),
            };
            let mut query = timestamp.into_query(&conf.mesaurement);
            for (field, field_value) in &conf.fields {
                let value = match get_dynamic_value_from_json(field_value) {
                    common::DynamicValue::Const(value) => value,
                    common::DynamicValue::Field(s) => match msg.get(&s) {
                        Some(value) => value.clone().into(),
                        None => serde_json::Value::Null,
                    },
                };

                match value {
                    serde_json::Value::Bool(b) => query = query.add_field(field, Type::Boolean(b)),
                    serde_json::Value::Number(number) => {
                        if let Some(i) = number.as_i64() {
                            query = query.add_field(field, Type::SignedInteger(i));
                        } else if let Some(u) = number.as_u64() {
                            query = query.add_field(field, Type::UnsignedInteger(u));
                        } else if let Some(f) = number.as_f64() {
                            query = query.add_field(field, Type::Float(f));
                        }
                    }
                    serde_json::Value::String(s) => {
                        query = query.add_field(field, Type::Text(s));
                    }
                    _ => {}
                }
            }

            if let Some(tags) = &conf.tags {
                for (tag, tag_value) in tags {
                    let value = match get_dynamic_value_from_json(tag_value) {
                        common::DynamicValue::Const(value) => value,
                        common::DynamicValue::Field(s) => match msg.get(&s) {
                            Some(value) => value.clone().into(),
                            None => serde_json::Value::Null,
                        },
                    };

                    match value {
                        serde_json::Value::Bool(b) => query = query.add_tag(tag, Type::Boolean(b)),
                        serde_json::Value::Number(number) => {
                            if let Some(i) = number.as_i64() {
                                query = query.add_tag(tag, Type::SignedInteger(i));
                            } else if let Some(u) = number.as_u64() {
                                query = query.add_tag(tag, Type::UnsignedInteger(u));
                            } else if let Some(f) = number.as_f64() {
                                query = query.add_tag(tag, Type::Float(f));
                            }
                        }
                        serde_json::Value::String(s) => {
                            query = query.add_tag(tag, Type::Text(s));
                        }
                        _ => {}
                    }
                }
            }
            querys.push(query);
        }
        if let Err(e) = influxdb_client.query(querys).await {
            debug!("{}", e);
        }
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf(&mut self, conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = conf;
        self.join_handle = Some(Self::event_loop(join_handle_data));
    }

    pub async fn update_influxdb_client(&mut self, influxdb_conf: Arc<Conf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.influxdb_conf = influxdb_conf;
        self.join_handle = Some(Self::event_loop(join_handle_data));
    }
}

fn new_influxdb_client(influxdb_conf: &Arc<Conf>, sink_conf: &SinkConf) -> Client {
    let schema = match &influxdb_conf.ssl_enable {
        true => "https",
        false => "http",
    };
    let client = Client::new(
        format!(
            "{}://{}:{}",
            schema, &influxdb_conf.host, influxdb_conf.port
        ),
        &sink_conf.database,
    );

    // match &sink_conf.auth_method {
    //     types::apps::influxdb_v1::AuthMethod::None => match &influxdb_conf.auth_method {
    //         types::apps::influxdb_v1::InfluxdbAuthMethod::None => {}
    //         types::apps::influxdb_v1::InfluxdbAuthMethod::BasicAuthentication => {
    //             client = client.with_auth(
    //                 influxdb_conf
    //                     .conf_v1
    //                     .as_ref()
    //                     .unwrap()
    //                     .auth
    //                     .username
    //                     .as_ref()
    //                     .unwrap(),
    //                 influxdb_conf
    //                     .conf_v1
    //                     .as_ref()
    //                     .unwrap()
    //                     .auth
    //                     .password
    //                     .as_ref()
    //                     .unwrap(),
    //             );
    //         }
    //         types::apps::influxdb_v1::InfluxdbAuthMethod::ApiToken => {
    //             client = client.with_token(
    //                 influxdb_conf
    //                     .conf_v1
    //                     .as_ref()
    //                     .unwrap()
    //                     .auth
    //                     .api_token
    //                     .as_ref()
    //                     .unwrap(),
    //             );
    //         }
    //     },
    //     types::apps::influxdb_v1::InfluxdbAuthMethod::BasicAuthentication => {
    //         client = client.with_auth(
    //             sink_conf
    //                 .conf_v1
    //                 .as_ref()
    //                 .unwrap()
    //                 .auth
    //                 .username
    //                 .as_ref()
    //                 .unwrap(),
    //             sink_conf
    //                 .conf_v1
    //                 .as_ref()
    //                 .unwrap()
    //                 .auth
    //                 .password
    //                 .as_ref()
    //                 .unwrap(),
    //         );
    //     }
    //     types::apps::influxdb_v1::InfluxdbAuthMethod::ApiToken => {
    //         client = client.with_token(
    //             sink_conf
    //                 .conf_v1
    //                 .as_ref()
    //                 .unwrap()
    //                 .auth
    //                 .api_token
    //                 .as_ref()
    //                 .unwrap(),
    //         );
    //     }
    // }

    client
}
