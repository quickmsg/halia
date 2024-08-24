#[macro_export]
macro_rules! check_and_set_on_true {
    ($self:expr) => {
        match $self.on {
            true => return Ok(()),
            false => $self.on = true,
        }
    };
}

#[macro_export]
macro_rules! check_and_set_on_false {
    ($self:expr) => {
        match $self.on {
            true => $self.on = false,
            false => return Ok(()),
        }
    };
}

#[macro_export]
macro_rules! get_search_sources_or_sinks_info_resp {
    ($self:expr) => {
        SearchSourcesOrSinksInfoResp {
            id: $self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: $self.base_conf.clone(),
                ext: serde_json::to_value(&$self.ext_conf).unwrap(),
            },
            value: None,
        }
    };
}
