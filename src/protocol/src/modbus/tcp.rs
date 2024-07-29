use tracing::debug;

use super::{Context, FunctionCode, ProtocolError};

struct TcpContext {
    transcation_id: u16,
    function_code: FunctionCode,
    unit_id: u8,
    buffer: [u8; 255],
}

impl TcpContext {
    fn update_transcation_id(&mut self) {
        if self.transcation_id < u16::MAX {
            self.transcation_id += 1;
        } else {
            self.transcation_id = 0;
        }
    }
}

pub fn new() -> impl Context {
    TcpContext {
        function_code: FunctionCode::MaskWriteRegister,
        transcation_id: 0,
        unit_id: 0,
        buffer: [0; 255],
    }
}

impl Context for TcpContext {
    fn get_buf(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    fn encode_read(
        &mut self,
        slave: u8,
        addr: u16,
        function_code: FunctionCode,
        quantity: u16,
    ) -> &[u8] {
        self.update_transcation_id();

        self.buffer[0] = (self.transcation_id >> 8) as u8;
        self.buffer[1] = (self.transcation_id & 0x00ff) as u8;

        self.buffer[2] = 0;
        self.buffer[3] = 0;

        self.buffer[4] = 0;
        self.buffer[5] = 6;

        self.buffer[6] = slave;
        self.unit_id = slave;

        self.function_code = function_code.clone();
        self.buffer[7] = function_code.into();

        self.buffer[8] = (addr >> 8) as u8;
        self.buffer[9] = (addr & 0x00ff) as u8;

        self.buffer[10] = (quantity >> 8) as u8;
        self.buffer[11] = (quantity & 0x00ff) as u8;
        debug!("{:?}", &self.buffer[..12]);
        &self.buffer[..12]
    }

    fn decode_read(&mut self, n: usize) -> Result<&mut [u8], ProtocolError> {
        if n == 0 {
            return Err(ProtocolError::EmptyResp);
        }
        if n < 9 {
            return Err(ProtocolError::DataTooSmall);
        }
        let transcation_id = ((self.buffer[0] as u16) << 8) | (self.buffer[1] as u16);
        if transcation_id != self.transcation_id {
            return Err(ProtocolError::TranscationIdMismatch);
        }

        if self.buffer[2] != 0 || self.buffer[3] != 0 {
            return Err(ProtocolError::ProtocolIdErr);
        }

        let len = ((self.buffer[4] as u16) << 8) | (self.buffer[5] as u16);

        let unit_id = self.buffer[6];
        if self.unit_id != unit_id {
            return Err(ProtocolError::UnitIdMismatch);
        }

        let function_code = self.buffer[7];
        // if FunctionCode::from(function_code) != self.function_code {
        //     todo!()
        // }

        let byte_cnt = self.buffer[8];

        Ok(&mut self.buffer[9..(9 + byte_cnt as usize)])
    }
    fn encode_write(
        &mut self,
        slave: u8,
        addr: u16,
        function_code: FunctionCode,
        value: &[u8],
    ) -> &[u8] {
        self.update_transcation_id();
        self.buffer[0] = (self.transcation_id >> 8) as u8;
        self.buffer[1] = (self.transcation_id & 0x00ff) as u8;

        self.buffer[2] = 0;
        self.buffer[3] = 0;

        // len
        self.buffer[4] = 0;
        self.buffer[5] = 6;

        self.buffer[6] = slave;

        self.buffer[7] = function_code.into();

        self.buffer[8] = (addr >> 8) as u8;
        self.buffer[9] = (addr & 0x00ff) as u8;

        let quantity = value.len();
        self.buffer[10] = (quantity >> 8) as u8;
        self.buffer[11] = (quantity & 0x00ff) as u8;

        &self.buffer[..12]
    }
}
