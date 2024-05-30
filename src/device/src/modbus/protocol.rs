use anyhow::{bail, Result};
use bytes::{Buf, BufMut, BytesMut};
use tracing::warn;

pub struct TcpContext {
    function_code: u8,
    id: u16,
    slave: u8,
    buf: BytesMut,
}

impl TcpContext {
    pub fn new() -> Self {
        Self {
            function_code: 0,
            id: 0,
            slave: 0,
            buf: BytesMut::with_capacity(260),
        }
    }

    pub fn clear_buf(&mut self) {
        self.buf.clear();
    }

    pub fn get_buf(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn encode_read(&mut self, area: u8, slave: u8, addr: u16, cnt: u16) {
        self.id = self.id.wrapping_add(1);
        self.buf.clear();
        self.buf.put_u16(self.id);
        self.buf.put_u16(0);
        self.buf.put_u16(6);

        self.buf.put_u8(slave);
        self.slave = slave;

        match area {
            0 => {
                self.buf.put_u8(0x01);
                self.function_code = 0x01;
            }
            1 => {
                self.buf.put_u8(0x02);
                self.function_code = 0x02;
            }
            4 => {
                self.buf.put_u8(0x04);
                self.function_code = 0x04;
            }
            3 => {
                self.buf.put_u8(0x03);
                self.function_code = 0x03;
            }
            _ => unreachable!(),
        }
        self.buf.put_u16(addr);
        self.buf.put_u16(cnt);
    }

    fn write_single_coil(&mut self, addr: u16, coil: bool) {}

    /// Write a single holding register (0x06)
    fn write_single_register(&mut self, addr: u16, word: u16) {}

    /// Write multiple coils (0x0F)
    fn write_multiple_coils(&mut self, addr: u16, coils: u16) {}

    /// Write multiple holding registers (0x10)
    fn write_multiple_registers(&mut self, addr: u16, words: u16) {}

    /// Set or clear individual bits of a holding register (0x16)
    fn masked_write_register(&mut self, addr: u16, and_mask: u16, or_mask: u16) {}

    pub fn decode(&mut self) -> Result<()> {
        if self.buf.len() < 8 {
            bail!("too short,something is wrong")
        }
        warn!("decode:{:?}", self.buf);
        if self.buf.get_u16() != self.id {
            bail!("请求id不一致")
        }
        if self.buf.get_u16() != 0 {
            bail!("协议错误，TCP下应该为0")
        }
        let len = self.buf.get_u16();
        if self.buf.get_u8() != self.slave {
            bail!("slave id 不一致")
        }

        let req_code = self.function_code;
        let resp_code = self.buf.get_u8();
        if req_code != resp_code {
            // if self.buf.get_u8() != self.function_code {
            println!("{},{}", req_code, resp_code);
            bail!("指令码不一致")
        }
        let data_len = self.buf.get_u8() as usize;
        if data_len != self.buf.len() {
            bail!("长度不一致")
        }

        Ok(())
    }
}

pub struct RtuContext {
    function_code: u8,
    slave: u8,
    pub buf: BytesMut,
}

impl RtuContext {
    pub fn new() -> Self {
        Self {
            function_code: 0,
            slave: 0,
            buf: BytesMut::with_capacity(256),
        }
    }

    pub fn encode_read(&mut self, area: u8, slave: u8, addr: u16, cnt: u16) {
        self.buf.clear();

        self.buf.put_u8(slave);
        self.slave = slave;

        match area {
            0 => {
                self.buf.put_u8(0x01);
                self.function_code = 0x01;
            }
            1 => {
                self.buf.put_u8(0x02);
                self.function_code = 0x02;
            }
            4 => {
                self.buf.put_u8(0x04);
                self.function_code = 0x04;
            }
            3 => {
                self.buf.put_u8(0x03);
                self.function_code = 0x03;
            }
            _ => unreachable!(),
        }

        self.buf.put_u16(addr);
        self.buf.put_u16(cnt);
        self.buf.put_u16(calc_crc(&self.buf))
    }

    fn read_write_multiple_registers(
        &mut self,
        read_addr: u16,
        read_count: u16,
        write_addr: u16,
        write_data: &[u8],
    ) {
    }

    fn write_single_coil(&mut self, addr: u16, coil: bool) {}

    /// Write a single holding register (0x06)
    fn write_single_register(&mut self, addr: u16, word: u16) {}

    /// Write multiple coils (0x0F)
    fn write_multiple_coils(&mut self, addr: u16, coils: u16) {}

    /// Write multiple holding registers (0x10)
    fn write_multiple_registers(&mut self, addr: u16, words: u16) {}

    /// Set or clear individual bits of a holding register (0x16)
    fn masked_write_register(&mut self, addr: u16, and_mask: u16, or_mask: u16) {}

    pub fn decode(&mut self) -> Result<()> {
        // if self.buf.len() < 8 {
        //     bail!("too short,something is wrong")
        if self.buf.get_u8() != self.slave {
            bail!("slave id 不一致")
        }

        let req_code = self.function_code;
        let resp_code = self.buf.get_u8();
        if req_code != resp_code {
            // if self.buf.get_u8() != self.function_code {
            println!("{},{}", req_code, resp_code);
            bail!("指令码不一致")
        }
        let data_len = self.buf.get_u8() as usize;
        if data_len != self.buf.len() {
            bail!("长度不一致")
        }

        // TODO 校验码0

        Ok(())
    }
}

fn calc_crc(data: &[u8]) -> u16 {
    let mut crc = 0xFFFF;
    for x in data {
        crc ^= u16::from(*x);
        for _ in 0..8 {
            let crc_odd = (crc & 0x0001) != 0;
            crc >>= 1;
            if crc_odd {
                crc ^= 0xA001;
            }
        }
    }
    crc << 8 | crc >> 8
}

fn check_crc(adu_data: &[u8], expected_crc: u16) -> Result<()> {
    let actual_crc = calc_crc(adu_data);
    if expected_crc != actual_crc {
        bail!("crc校验失败")
    }
    Ok(())
}
