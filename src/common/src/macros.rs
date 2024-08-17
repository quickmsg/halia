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
macro_rules! get_mb_rx {
    ($self:expr, $rule_id:expr) => {{
        $self.ref_info.active_ref($rule_id);
        match &$self.mb_tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(16);
                $self.mb_tx = Some(tx);
                rx
            }
        }
    }};
}

#[macro_export]
macro_rules! del_mb_rx {
    ($self:expr, $rule_id:expr) => {{
        $self.ref_info.deactive_ref($rule_id);
        if $self.ref_info.can_stop() {
            $self.mb_tx = None;
        }
    }};
}
