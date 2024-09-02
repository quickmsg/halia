use std::{io, sync::Arc, time::Duration};

use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::{Message, MessageBatch};
use protocol::modbus::Context;
use serde_json::Value;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::warn;
use types::{
    devices::modbus::{Area, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct Source {
    pub id: Uuid,

    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,
    quantity: u16,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,
    value: serde_json::Value,
    err_info: Option<String>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        let quantity = ext_conf.data_type.get_quantity();
        Ok(Self {
            id,
            base_conf,
            ext_conf,
            quantity,
            value: Value::Null,
            stop_signal_tx: None,
            mb_tx: None,
            join_handle: None,
            err_info: None,
        })
    }

    fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if conf.interval == 0 {
            return Err(HaliaError::Common("点位频率必须大于0".to_owned()));
        }

        match &conf.data_type.typ {
            types::devices::modbus::Type::Bool => match &conf.area {
                Area::InputRegisters | Area::HoldingRegisters => {
                    if conf.data_type.pos.is_none() {
                        return Err(HaliaError::Common("必须填写位置！".to_owned()));
                    }
                }
                _ => {}
            },
            types::devices::modbus::Type::Int8
            | types::devices::modbus::Type::Uint8
            | types::devices::modbus::Type::Int16
            | types::devices::modbus::Type::Uint16 => {
                if conf.data_type.single_endian.is_none() {
                    return Err(HaliaError::Common("必须填写单字节序配置！".to_owned()));
                }
            }
            types::devices::modbus::Type::Int32
            | types::devices::modbus::Type::Uint32
            | types::devices::modbus::Type::Int64
            | types::devices::modbus::Type::Uint64
            | types::devices::modbus::Type::Float32
            | types::devices::modbus::Type::Float64 => {
                if conf.data_type.single_endian.is_none() {
                    return Err(HaliaError::Common("必须填写单字节序配置！".to_owned()));
                }

                if conf.data_type.double_endian.is_none() {
                    return Err(HaliaError::Common("必须填写双字节序配置！".to_owned()));
                }
            }

            types::devices::modbus::Type::String => {
                if conf.data_type.single_endian.is_none() {
                    return Err(HaliaError::Common("必须填写单字节序配置！".to_owned()));
                }

                if conf.data_type.double_endian.is_none() {
                    return Err(HaliaError::Common("必须填写双字节序配置！".to_owned()));
                }

                if conf.data_type.len.is_none() {
                    return Err(HaliaError::Common("必须填写长度配置！".to_owned()));
                }
            }
            types::devices::modbus::Type::Bytes => {
                if conf.data_type.len.is_none() {
                    return Err(HaliaError::Common("必须填写长度配置！".to_owned()));
                }
            }
        }

        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        if self.ext_conf.data_type == ext_conf.data_type
            && self.ext_conf.slave == ext_conf.slave
            && self.ext_conf.area == ext_conf.area
            && self.ext_conf.address == ext_conf.address
        {
            return Err(HaliaError::AddressExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn start(
        &mut self,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
            .await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let point_id = self.id.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx, device_err);
                    }

                    _ = interval.tick() => {
                        if device_err.read().await.is_none() {
                            _ = read_tx.send(point_id).await;
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.quantity = ext_conf.data_type.get_quantity();
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, read_tx, device_err) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
                .await;
        }

        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Box<dyn Context>) -> io::Result<()> {
        let res = match self.ext_conf.area {
            Area::InputDiscrete => {
                ctx.read_discrete_inputs(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::Coils => {
                ctx.read_coils(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::InputRegisters => {
                ctx.read_input_registers(self.ext_conf.slave, self.ext_conf.address, self.quantity)
                    .await
            }
            Area::HoldingRegisters => {
                ctx.read_holding_registers(
                    self.ext_conf.slave,
                    self.ext_conf.address,
                    self.quantity,
                )
                .await
            }
        };

        match res {
            Ok(mut data) => {
                let value = self.ext_conf.data_type.decode(&mut data);
                match &value {
                    message::MessageValue::Bytes(bytes) => {
                        let str = BASE64_STANDARD.encode(bytes);
                        self.value = serde_json::Value::String(str);
                    }
                    _ => self.value = value.clone().into(),
                }

                match &self.mb_tx {
                    Some(tx) => {
                        if tx.receiver_count() == 0 {
                            return Ok(());
                        }
                        let mut message = Message::default();
                        message.add(self.base_conf.name.clone(), value);
                        let mut message_batch = MessageBatch::default();
                        message_batch.push_message(message);
                        _ = tx.send(message_batch);
                    }
                    None => {}
                }
                Ok(())
            }
            Err(e) => match e {
                protocol::modbus::ModbusError::Transport(e) => Err(e),
                protocol::modbus::ModbusError::Protocol(e) => {
                    warn!("{}", e);
                    self.err_info = Some(e.to_string());
                    Ok(())
                }
                protocol::modbus::ModbusError::Exception(e) => {
                    self.err_info = Some(e.to_string());
                    warn!("{}", e);
                    Ok(())
                }
            },
        }
    }
}
