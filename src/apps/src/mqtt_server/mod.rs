use common::{check_and_set_on_false, check_and_set_on_true, error::HaliaResult};
use sink::Sink;
use srouce::Source;
use tokio::sync::mpsc;
use uuid::Uuid;

mod sink;
mod srouce;

pub struct MqttServer {
    pub id: Uuid,

    on: bool,
    err: Option<String>,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    sources: Vec<Source>,
    sinks: Vec<Sink>,
}

impl MqttServer {
    pub fn new(app_id: Option<Uuid>) -> HaliaResult<()> {
        todo!()
    }

    pub fn search(&self) -> HaliaResult<()> {
        todo!()
    }

    pub fn update(&mut self) -> HaliaResult<()> {
        todo!()
    }

    pub fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }

    pub fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        Ok(())
    }

    pub fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);

        Ok(())
    }
}
