use std::sync::Arc;

use futures::lock::BiLock;

pub enum ResourceType {
    Device,
    DeviceSource,
    DeviceSink,
    App,
    AppSource,
    AppSink,
}

pub struct ErrorManager {
    resource_type: ResourceType,
    resource_id: String,
    resource_err: BiLock<Option<Arc<String>>>,
    last_err: Option<Arc<String>>,
}

impl ErrorManager {
    pub fn new(
        resource_type: ResourceType,
        resource_id: String,
        resource_err: BiLock<Option<Arc<String>>>,
    ) -> Self {
        Self {
            resource_type,
            resource_id,
            resource_err,
            last_err: None,
        }
    }

    // 状态是否切换
    pub async fn put_err(&mut self, err: Arc<String>) -> bool {
        match &self.last_err {
            Some(last_err) => {
                if *last_err != err {
                    self.last_err = Some(err.clone());
                    *self.resource_err.lock().await = Some(err);
                }
                false
            }
            None => {
                self.last_err = Some(err.clone());
                *self.resource_err.lock().await = Some(err);
                match self.resource_type {
                    ResourceType::Device => todo!(),
                    ResourceType::DeviceSource => todo!(),
                    ResourceType::DeviceSink => todo!(),
                    ResourceType::App => {
                        let _ =
                            storage::app::update_status(&self.resource_id, types::Status::Error)
                                .await;
                    }
                    ResourceType::AppSource => {
                        let _ = storage::app::source_sink::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                    }
                    ResourceType::AppSink => todo!(),
                }
                true
            }
        }
    }

    pub async fn set_ok(&mut self) -> bool {
        if self.last_err.is_some() {
            self.last_err = None;
            *self.resource_err.lock().await = None;
            let _ = storage::app::update_status(&self.resource_id, types::Status::Running).await;
            true
        } else {
            false
        }
    }
}
