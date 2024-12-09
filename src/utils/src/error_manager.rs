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
    inited: bool,
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
            inited: false,
            resource_type,
            resource_id,
            resource_err,
            last_err: None,
        }
    }

    // 状态是否切换
    pub async fn set_err(&mut self, err: Arc<String>) -> bool {
        if !self.inited {
            self.inited = true;
        }
        match &self.last_err {
            Some(last_err) => {
                if *last_err != err {
                    self.last_err = Some(err.clone());
                    *self.resource_err.lock().await = Some(err);
                }
                false
            }
            None => {
                // let err = (*err).clone();
                self.last_err = Some(err.clone());
                *self.resource_err.lock().await = Some(err);
                match self.resource_type {
                    ResourceType::Device => {
                        let _ = storage::device::device::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                        events::insert_connect_failed(
                            types::events::ResourceType::Device,
                            &self.resource_id,
                            (**self.last_err.as_ref().unwrap()).clone(),
                        )
                        .await;
                    }
                    ResourceType::DeviceSource => {
                        let _ = storage::device::source_sink::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                    }
                    ResourceType::DeviceSink => {
                        let _ = storage::device::source_sink::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                    }
                    ResourceType::App => {
                        let _ =
                            storage::app::update_status(&self.resource_id, types::Status::Error)
                                .await;
                        events::insert_connect_failed(
                            types::events::ResourceType::App,
                            &self.resource_id,
                            (**self.last_err.as_ref().unwrap()).clone(),
                        )
                        .await;
                    }
                    ResourceType::AppSource => {
                        let _ = storage::app::source_sink::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                    }
                    ResourceType::AppSink => {
                        let _ = storage::app::source_sink::update_status(
                            &self.resource_id,
                            types::Status::Error,
                        )
                        .await;
                    }
                }
                true
            }
        }
    }

    pub async fn set_ok(&mut self) -> bool {
        if self.last_err.is_none() {
            if self.inited {
                return false;
            } else {
                self.inited = true;
            }
        }

        match self.resource_type {
            ResourceType::Device => todo!(),
            ResourceType::DeviceSource => todo!(),
            ResourceType::DeviceSink => todo!(),
            ResourceType::App => {
                let _ =
                    storage::app::update_status(&self.resource_id, types::Status::Running).await;
                events::insert_connect_succeed(types::events::ResourceType::App, &self.resource_id)
                    .await;
            }
            ResourceType::AppSource => {
                let _ = storage::app::source_sink::update_status(
                    &self.resource_id,
                    types::Status::Running,
                )
                .await;
            }
            ResourceType::AppSink => todo!(),
        }

        if self.last_err.is_some() {
            self.last_err = None;
            *self.resource_err.lock().await = None;
        }

        true
    }
}
