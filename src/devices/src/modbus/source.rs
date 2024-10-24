use std::{io, sync::Arc, time::Duration};

use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch, RuleMessageBatch};
use protocol::modbus::Context;
use tokio::{
    select,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
    time,
};
use tracing::warn;
use types::devices::{device::modbus::SourceConf, modbus::Area};

pub struct Source {
    pub conf: SourceConf,
    quantity: u16,

    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    err_info: Option<String>,

    pub mb_txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
}

pub struct JoinHandleData {
    pub id: String,
    pub stop_signal_rx: watch::Receiver<()>,
    pub read_tx: mpsc::Sender<String>,
    pub device_err_rx: broadcast::Receiver<bool>,
}

impl Source {
    pub fn new(
        id: String,
        conf: SourceConf,
        read_tx: mpsc::Sender<String>,
        device_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());

        let join_handle_data = JoinHandleData {
            id,
            stop_signal_rx,
            read_tx,
            device_err_rx,
        };
        let join_handle = Self::event_loop(join_handle_data, &conf);

        let quantity = conf.data_type.get_quantity();
        Self {
            conf,
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

    fn event_loop(
        mut join_handle_data: JoinHandleData,
        conf: &SourceConf,
    ) -> JoinHandle<JoinHandleData> {
        let mut interval = time::interval(Duration::from_millis(conf.interval));
        let mut device_err = false;
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    _ = interval.tick() => {
                        if !device_err {
                            _ = join_handle_data.read_tx.send(join_handle_data.id.clone()).await;
                        }
                    }

                    err = join_handle_data.device_err_rx.recv() => {
                        match err {
                            Ok(err) => device_err = err,
                            Err(e) => warn!("{}", e),
                        }
                    }
                }
            }
        })
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update(&mut self, conf: SourceConf) {
        let join_handle_data = self.stop().await;
        self.conf = conf;
        let join_handle = Self::event_loop(join_handle_data, &self.conf);
        self.join_handle = Some(join_handle);
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
                let mut message = Message::default();
                // todo 考虑field的共享，避免clone
                message.add(self.conf.field.clone(), value);
                let mut message_batch = MessageBatch::default();
                message_batch.push_message(message);

                // todo 没有receiver时不请求
                // 删除关闭的channel
                let mb;
                match self.mb_txs.len() {
                    0 => {}
                    1 => {
                        mb = RuleMessageBatch::Owned(message_batch);
                        if let Err(_) = self.mb_txs[0].send(mb) {
                            self.mb_txs.remove(0);
                        }
                    }
                    _ => {
                        mb = RuleMessageBatch::Arc(Arc::new(message_batch));
                        self.mb_txs.retain(|tx| tx.send(mb.clone()).is_ok());
                    }
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

    pub fn get_rx(&mut self) -> mpsc::UnboundedReceiver<RuleMessageBatch> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.mb_txs.push(tx);
        rx
    }
}
