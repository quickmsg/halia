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

#[macro_export]
macro_rules! add_ref {
    ($self:expr, $item:ident, $item_id:ident, $rule_id:ident) => {
        paste! {
            match $self.[<$item _ref_infos>].iter_mut()
            .find(|(id, _)| id == $item_id)
        {
            Some((_, ref_info)) => Ok(ref_info.add_ref($rule_id)),
            None => return Err(HaliaError::NotFound),
        }
        }
    };
}

#[macro_export]
macro_rules! active_ref {
    ($self:expr, $item:ident, $item_id:ident, $rule_id:ident) => {
        paste! {
            match $self
            .[<$item _ref_infos>]
            .iter_mut()
            .find(|(id, _)| id == $item_id)
        {
            Some((_, ref_info)) => ref_info.active_ref($rule_id),
            None => return Err(HaliaError::NotFound),
        }
        }
    };
}

#[macro_export]
macro_rules! deactive_ref {
    ($self:expr, $item:ident, $item_id:ident, $rule_id:ident) => {
        paste! {
            match $self
            .[<$item _ref_infos>]
            .iter_mut()
            .find(|(id, _)| id == $item_id)
            {
            Some((_, ref_info)) => Ok(ref_info.deactive_ref($rule_id)),
            None => return Err(HaliaError::NotFound),
         }
        }
    };
}

#[macro_export]
macro_rules! del_ref {
    ($self:expr, $item:ident, $item_id:ident, $rule_id:ident) => {
        paste! {
            match $self
            .[<$item _ref_infos>]
            .iter_mut()
            .find(|(id, _)| id == $item_id)
        {
            Some((_, ref_info)) => Ok(ref_info.del_ref($rule_id)),
            None => return Err(HaliaError::NotFound),
        }
        }
    };
}
