use super::FunctionCode;

pub struct TCPContext {
    transcation_id: u16,
}

impl TCPContext {
    pub fn new() -> Self {
        TCPContext { transcation_id: 0 }
    }

    pub fn encode(&mut self, slave: u8, addr: u16, function_code: FunctionCode, quantity: u16) {
        // TODO
        let mut req: Vec<u8> = vec![];
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }

        req[0] = (self.transcation_id >> 8) as u8;
        req[1] = (self.transcation_id & 0x00ff) as u8;

        req[2] = 0;
        req[3] = 0;

        // 4-5 len

        req[6] = slave;

        req[7] = function_code.into();

        req[8] = (addr >> 8) as u8;
        req[9] = (addr & 0x00ff) as u8;

        // req[10] = xx;
        // req[11] = xx;
    }
}