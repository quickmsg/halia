use std::{io, time::Duration};

use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch};
use protocol::modbus::Context;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::warn;
use types::{
    devices::modbus::{Area, SourceConf},
    SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct Source {
    pub id: Uuid,

    pub conf: SourceConf,
    quantity: u16,

    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            broadcast::Receiver<bool>,
        )>,
    >,
    err_info: Option<String>,

    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn new(
        id: Uuid,
        conf: SourceConf,
        read_tx: mpsc::Sender<Uuid>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        // Self::validate_conf(&ext_conf)?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, _) = broadcast::channel(16);

        let quantity = conf.data_type.get_quantity();
        let mut source = Self {
            id,
            conf,
            quantity,
            stop_signal_tx,
            mb_tx,
            join_handle: None,
            err_info: None,
        };

        source.event_loop(stop_signal_rx, read_tx, device_err_rx);

        source
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
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

    // pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SourceConf) -> HaliaResult<()> {
    //     if self.base_conf.name == base_conf.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     if self.ext_conf.data_type == ext_conf.data_type
    //         && self.ext_conf.slave == ext_conf.slave
    //         && self.ext_conf.area == ext_conf.area
    //         && self.ext_conf.address == ext_conf.address
    //     {
    //         return Err(HaliaError::AddressExists);
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        // get_search_sources_or_sinks_info_resp!(self)
        todo!()
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
        mut device_err_rx: broadcast::Receiver<bool>,
    ) {
        let interval = self.conf.interval;
        let point_id = self.id.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            let mut device_err = false;
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx, device_err_rx);
                    }

                    _ = interval.tick() => {
                        if !device_err {
                            _ = read_tx.send(point_id).await;
                        }
                    }

                    err = device_err_rx.recv() => {
                        match err {
                            Ok(err) => device_err = err,
                            Err(e) => warn!("{}", e),
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).await.unwrap();
    }

    pub async fn update(&mut self, old_conf: String, new_conf: SourceConf) -> HaliaResult<()> {
        // Self::validate_conf(&ext_conf)?;

        let old_conf = serde_json::from_str::<SourceConf>(&old_conf).unwrap();
        if old_conf.data_type != new_conf.data_type {
            self.quantity = new_conf.data_type.get_quantity();
        }

        if old_conf.interval != new_conf.interval {
            self.stop_signal_tx.send(()).await.unwrap();

            let (stop_signal_rx, read_tx, device_err_rx) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(stop_signal_rx, read_tx, device_err_rx)
                .await;
        }

        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Box<dyn Context>) -> io::Result<()> {
        let res = match self.conf.area {
            Area::InputDiscrete => {
                ctx.read_discrete_inputs(self.conf.slave, self.conf.address, self.quantity)
                    .await
            }
            Area::Coils => {
                ctx.read_coils(self.conf.slave, self.conf.address, self.quantity)
                    .await
            }
            Area::InputRegisters => {
                ctx.read_input_registers(self.conf.slave, self.conf.address, self.quantity)
                    .await
            }
            Area::HoldingRegisters => {
                ctx.read_holding_registers(self.conf.slave, self.conf.address, self.quantity)
                    .await
            }
        };

        match res {
            Ok(mut data) => {
                let value = self.conf.data_type.decode(&mut data);

                if self.mb_tx.receiver_count() == 0 {
                    return Ok(());
                }
                let mut message = Message::default();
                // todo 考虑field的共享，避免clone
                message.add(self.conf.field.clone(), value);
                let mut message_batch = MessageBatch::default();
                message_batch.push_message(message);
                _ = self.mb_tx.send(message_batch);
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
