use std::{io, sync::Arc, time::Duration};

use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch, RuleMessageBatch};
use modbus_protocol::Context;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        watch,
    },
    task::JoinHandle,
    time,
};
use tracing::warn;
use types::devices::{
    device::modbus::{Area, DeviceConf, SourceConf},
    source_sink_template::modbus::{SourceCustomizeConf, SourceTemplateConf},
};

pub struct Source {
    pub source_conf: SourceConf,
    quantity: u16,

    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    err_info: Option<String>,

    pub mb_txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
}

pub struct TaskLoop {
    id: Arc<String>,
    stop_signal_rx: watch::Receiver<()>,
    read_tx: mpsc::UnboundedSender<Arc<String>>,
    device_err_rx: broadcast::Receiver<bool>,
}

impl TaskLoop {
    fn new(
        id: String,
        stop_signal_rx: watch::Receiver<()>,
        read_tx: UnboundedSender<Arc<String>>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        Self {
            id: Arc::new(id),
            stop_signal_rx,
            read_tx,
            device_err_rx,
        }
    }

    fn start(mut self, source_conf: &SourceConf) -> JoinHandle<Self> {
        let mut interval = time::interval(Duration::from_millis(source_conf.interval));
        let mut device_err = false;
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    _ = interval.tick() => {
                        if !device_err {
                            _ = self.read_tx.send(self.id.clone());
                        }
                    }

                    Ok(err) = self.device_err_rx.recv() => {
                        device_err = err;
                    }
                }
            }
        })
    }
}

impl Source {
    pub fn new(
        id: String,
        source_conf: SourceConf,
        read_tx: UnboundedSender<Arc<String>>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());

        let task_loop = TaskLoop::new(id, stop_signal_rx, read_tx, device_err_rx);

        let join_handle = task_loop.start(&source_conf);

        let quantity = source_conf.data_type.get_quantity();
        Self {
            source_conf,
            quantity,
            stop_signal_tx,
            join_handle: Some(join_handle),
            err_info: None,
            mb_txs: vec![],
        }
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if conf.interval == 0 {
            return Err(HaliaError::Common("点位频率必须大于0".to_owned()));
        }

        match &conf.data_type.typ {
            types::devices::device::modbus::Type::Bool => match &conf.area {
                Area::InputRegisters | Area::HoldingRegisters => {
                    if conf.data_type.pos.is_none() {
                        return Err(HaliaError::Common("必须填写位置！".to_owned()));
                    }
                }
                _ => {}
            },
            types::devices::device::modbus::Type::Int8
            | types::devices::device::modbus::Type::Uint8 => match &conf.data_type.coder_type {
                Some(coder_type) => match coder_type {
                    types::devices::device::modbus::CoderType::A
                    | types::devices::device::modbus::CoderType::B => {}
                    _ => return Err(HaliaError::Common("不支持的编码类型！".to_owned())),
                },
                None => return Err(HaliaError::Common("必须填写编码类型！".to_owned())),
            },
            types::devices::device::modbus::Type::Int16
            | types::devices::device::modbus::Type::Uint16 => match &conf.data_type.coder_type {
                Some(coder_type) => match coder_type {
                    types::devices::device::modbus::CoderType::A
                    | types::devices::device::modbus::CoderType::B
                    | types::devices::device::modbus::CoderType::AB
                    | types::devices::device::modbus::CoderType::BA => {}
                    _ => return Err(HaliaError::Common("不支持的编码类型！".to_owned())),
                },
                None => return Err(HaliaError::Common("必须填写编码类型！".to_owned())),
            },
            types::devices::device::modbus::Type::Int32
            | types::devices::device::modbus::Type::Uint32
            | types::devices::device::modbus::Type::Float32 => match &conf.data_type.coder_type {
                Some(coder_type) => match coder_type {
                    types::devices::device::modbus::CoderType::ABCD
                    | types::devices::device::modbus::CoderType::BADC
                    | types::devices::device::modbus::CoderType::CDAB
                    | types::devices::device::modbus::CoderType::DCBA => {}
                    _ => return Err(HaliaError::Common("不支持的编码类型！".to_owned())),
                },
                None => return Err(HaliaError::Common("必须填写编码类型！".to_owned())),
            },
            types::devices::device::modbus::Type::Int64
            | types::devices::device::modbus::Type::Uint64
            | types::devices::device::modbus::Type::Float64 => {
                match &conf.data_type.coder_type.unwrap() {
                    types::devices::device::modbus::CoderType::ABCDEFGH
                    | types::devices::device::modbus::CoderType::BADCFEHG
                    | types::devices::device::modbus::CoderType::GHEFCDAB
                    | types::devices::device::modbus::CoderType::HGFEDCBA => {}
                    _ => return Err(HaliaError::Common("不支持的编码类型！".to_owned())),
                }
            }

            types::devices::device::modbus::Type::String => {
                match conf.data_type.coder_type {
                    Some(coder_type) => match coder_type {
                        types::devices::device::modbus::CoderType::A
                        | types::devices::device::modbus::CoderType::B
                        | types::devices::device::modbus::CoderType::AB
                        | types::devices::device::modbus::CoderType::BA => {}
                        _ => return Err(HaliaError::Common("不支持的编码类型！".to_owned())),
                    },
                    None => return Err(HaliaError::Common("必须填写编码类型！".to_owned())),
                }
                if conf.data_type.len.is_none() {
                    return Err(HaliaError::Common("必须填写长度配置！".to_owned()));
                }
            }
            types::devices::device::modbus::Type::Bytes => {
                if conf.data_type.len.is_none() {
                    return Err(HaliaError::Common("必须填写长度配置！".to_owned()));
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> TaskLoop {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update(&mut self, conf: serde_json::Value) -> HaliaResult<()> {
        let task_loop = self.stop().await;
        let conf: SourceConf = serde_json::from_value(conf)?;
        self.source_conf = conf;
        let join_handle = task_loop.start(&self.source_conf);
        self.join_handle = Some(join_handle);

        Ok(())
    }

    pub async fn read(
        &mut self,
        ctx: &mut Box<dyn Context>,
        device_conf: &DeviceConf,
    ) -> io::Result<()> {
        let res = match self.source_conf.area {
            Area::InputDiscrete => {
                ctx.read_discrete_inputs(
                    self.source_conf.slave,
                    self.source_conf.address,
                    self.quantity,
                )
                .await
            }
            Area::Coils => {
                ctx.read_coils(
                    self.source_conf.slave,
                    self.source_conf.address,
                    self.quantity,
                )
                .await
            }
            Area::InputRegisters => {
                ctx.read_input_registers(
                    self.source_conf.slave,
                    self.source_conf.address,
                    self.quantity,
                )
                .await
            }
            Area::HoldingRegisters => {
                ctx.read_holding_registers(
                    self.source_conf.slave,
                    self.source_conf.address,
                    self.quantity,
                )
                .await
            }
        };

        match res {
            Ok(mut data) => {
                let value = self.source_conf.data_type.decode(&mut data);
                let mut message = Message::default();
                // todo 考虑field的共享，避免clone
                message.add(self.source_conf.field.clone(), value);

                if let Some(metadatas) = &device_conf.metadatas {
                    message.insert_raw_metadatas(metadatas.clone());
                }
                if let Some(metadatas) = &self.source_conf.metadatas {
                    message.insert_raw_metadatas(metadatas.clone());
                }

                let mut message_batch = MessageBatch::default();
                message_batch.push_message(message);

                // todo 没有receiver时不请求
                // 删除关闭的channel
                match self.mb_txs.len() {
                    0 => {}
                    1 => {
                        let mb = RuleMessageBatch::Owned(message_batch);
                        if let Err(_) = self.mb_txs[0].send(mb) {
                            self.mb_txs.remove(0);
                        }
                    }
                    _ => {
                        let mb = RuleMessageBatch::Arc(Arc::new(message_batch));
                        self.mb_txs.retain(|tx| tx.send(mb.clone()).is_ok());
                    }
                }

                Ok(())
            }
            Err(e) => match e {
                modbus_protocol::ModbusError::Transport(e) => Err(e),
                modbus_protocol::ModbusError::Protocol(e) => {
                    warn!("{}", e);
                    self.err_info = Some(e.to_string());
                    Ok(())
                }
                modbus_protocol::ModbusError::Exception(e) => {
                    self.err_info = Some(e.to_string());
                    warn!("{}", e);
                    Ok(())
                }
            },
        }
    }

    pub fn get_rxs(&mut self, cnt: usize) -> Vec<mpsc::UnboundedReceiver<RuleMessageBatch>> {
        let mut rxs = vec![];
        for _ in 0..cnt {
            let (tx, rx) = mpsc::unbounded_channel();
            self.mb_txs.push(tx);
            rxs.push(rx);
        }
        rxs
    }
}

pub fn get_source_conf(
    customize_conf: serde_json::Value,
    template_conf: serde_json::Value,
) -> HaliaResult<SourceConf> {
    let customize_conf: SourceCustomizeConf = serde_json::from_value(customize_conf)?;
    let template_conf: SourceTemplateConf = serde_json::from_value(template_conf)?;
    Ok(SourceConf {
        slave: customize_conf.slave,
        field: template_conf.field,
        data_type: template_conf.data_type,
        area: template_conf.area,
        address: template_conf.address,
        interval: template_conf.interval,
        metadatas: customize_conf.metadatas,
    })
}

pub fn update_customize_conf(
    source_conf: &mut SourceConf,
    customize_conf: serde_json::Value,
) -> HaliaResult<()> {
    let customize_conf: SourceCustomizeConf = serde_json::from_value(customize_conf)?;
    source_conf.slave = customize_conf.slave;
    source_conf.metadatas = customize_conf.metadatas;

    Ok(())
}

pub fn update_template_conf(
    source_conf: &mut SourceConf,
    template_conf: serde_json::Value,
) -> HaliaResult<()> {
    let template_conf: SourceTemplateConf = serde_json::from_value(template_conf)?;
    source_conf.field = template_conf.field;
    source_conf.data_type = template_conf.data_type;
    source_conf.area = template_conf.area;
    source_conf.address = template_conf.address;
    source_conf.interval = template_conf.interval;
    Ok(())
}
