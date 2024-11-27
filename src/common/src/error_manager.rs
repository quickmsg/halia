use std::sync::Arc;

pub struct ErrorManager {
    last_err: Option<Arc<String>>,
}

impl Default for ErrorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorManager {
    pub fn new() -> Self {
        Self { last_err: None }
    }

    // 第一个返回值代表是否应该更新外部的错误信息
    // 第二个返回值代表是否应该更新外部的状态信息 running <-> error
    pub fn put_err(&mut self, err: Arc<String>) -> (bool, bool) {
        match &self.last_err {
            Some(last_err) => {
                if *last_err == err {
                    (false, false)
                } else {
                    self.last_err = Some(err);
                    (true, false)
                }
            }
            None => {
                self.last_err = Some(err);
                (true, true)
            }
        }
    }

    // 第一个返回值代表是否应该更新外部的错误信息
    // 第二个返回值代表是否应该更新外部的状态信息 running <-> error
    pub fn put_ok(&mut self) -> (bool, bool) {
        if self.last_err.is_some() {
            self.last_err = None;
            (true, true)
        } else {
            (false, false)
        }
    }
}
