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

#[macro_export]
macro_rules! check_stop {
    ($self:expr, $ref_infos:ident) => {
        if $self
            .$ref_infos
            .iter()
            .any(|(_, ref_info)| !ref_info.can_stop())
        {
            return Err(HaliaError::StopActiveRefing);
        }
    };
}

#[macro_export]
macro_rules! check_delete {
    ($self:expr, $ref_infos:ident) => {
        if $self
            .$ref_infos
            .iter()
            .any(|(_, ref_info)| !ref_info.can_delete())
        {
            return Err(HaliaError::DeleteRefing);
        }
    };
}

#[macro_export]
macro_rules! check_delete_item {
    ($self:expr, $ref_infos:ident, $source_id:ident) => {
        match $self.$ref_infos.iter().find(|(id, _)| *id == $source_id) {
            Some((_, ref_info)) => {
                if !ref_info.can_delete() {
                    return Err(HaliaError::DeleteRefing);
                }
            }
            None => return source_not_found_err!(),
        }
    };
}

#[macro_export]
macro_rules! find_source_add_ref {
    ($self:expr, $source_id:expr, $rule_id:expr) => {
        match $self
            .sources_ref_infos
            .iter_mut()
            .find(|(id, _)| id == $source_id)
        {
            Some((_, ref_info)) => Ok(ref_info.add_ref($rule_id)),
            None => source_not_found_err!(),
        }
    };
}

#[macro_export]
macro_rules! get_source_rx {
    ($self:expr, $source_id:expr, $rule_id:expr) => {
        $self.check_on()?;

        match $self
            .sources_ref_infos
            .iter_mut()
            .find(|(id, _)| id == $source_id)
        {
            Some((_, ref_info)) => ref_info.active_ref($rule_id),
            None => return source_not_found_err!(),
        }

        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => unreachable!(),
        }
    };
}

#[macro_export]
macro_rules! find_sink_add_ref {
    ($self:expr, $sink_id:expr, $rule_id:expr) => {
        match $self
            .sinks_ref_infos
            .iter_mut()
            .find(|(id, _)| id == $sink_id)
        {
            Some((_, ref_info)) => Ok(ref_info.add_ref($rule_id)),
            None => sink_not_found_err!(),
        }
    };
}
