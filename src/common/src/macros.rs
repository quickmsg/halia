#[macro_export]
macro_rules! get_search_sources_or_sinks_info_resp {
    ($self:expr) => {
        SearchSourcesOrSinksInfoResp {
            id: $self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: $self.base_conf.clone(),
                ext: serde_json::to_value(&$self.ext_conf).unwrap(),
            },
        }
    };
}

#[macro_export]
macro_rules! check_stop_all {
    ($self:expr, $item:ident) => {
        paste! {
            if $self
            .[<$item _ref_infos>]
            .iter()
            .any(|(_, ref_info)| !ref_info.can_stop())
        {
            return Err(HaliaError::StopActiveRefing);
        }
        }
    };
}

#[macro_export]
macro_rules! check_delete_all {
    ($self:expr, $item:ident) => {
        paste! {
            if $self
                .[<$item _ref_infos>]
                .iter()
                .any(|(_, ref_info)| !ref_info.can_delete())
            {
                return Err(HaliaError::DeleteRefing);
            }
        }
    };
}

#[macro_export]
macro_rules! check_delete {
    ($self:expr, $item:ident, $item_id:ident) => {
        use paste::paste;
        paste! {
            match $self.[<$item _ref_infos>].iter().find(|(id, _)| *id == $item_id) {
                Some((_, ref_info)) => {
                    if !ref_info.can_delete() {
                        return Err(HaliaError::DeleteRefing);
                    }
                }
                None =>  return Err(HaliaError::DeleteRefing),
            }
        }
    };
}