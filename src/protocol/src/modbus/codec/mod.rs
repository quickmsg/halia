use std::{
    convert::TryFrom,
    io::{Cursor, Error, ErrorKind},
};

use byteorder::{BigEndian, ReadBytesExt as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;

use super::{
    frame::{RequestPdu, ResponsePdu},
    Exception, ExceptionResponse, FunctionCode, Request, Response,
};

pub(crate) mod rtu;
pub(crate) mod tcp;

#[allow(clippy::cast_possible_truncation)]
fn u16_len(len: usize) -> u16 {
    // This type conversion should always be safe, because either
    // the caller is responsible to pass a valid usize or the
    // possible values are limited by the protocol.
    debug_assert!(len <= u16::MAX.into());
    len as u16
}

#[allow(clippy::cast_possible_truncation)]
fn u8_len(len: usize) -> u8 {
    // This type conversion should always be safe, because either
    // the caller is responsible to pass a valid usize or the
    // possible values are limited by the protocol.
    debug_assert!(len <= u8::MAX.into());
    len as u8
}

impl<'a> TryFrom<Request<'a>> for Bytes {
    type Error = Error;

    #[allow(clippy::panic_in_result_fn)] // Intentional unreachable!()
    fn try_from(req: Request<'a>) -> Result<Bytes, Self::Error> {
        use super::frame::Request::*;
        let cnt = request_byte_count(&req);
        let mut data = BytesMut::with_capacity(cnt);
        data.put_u8(req.function_code().value());
        match req {
            ReadCoils(address, quantity)
            | ReadDiscreteInputs(address, quantity)
            | ReadInputRegisters(address, quantity)
            | ReadHoldingRegisters(address, quantity) => {
                data.put_u16(address);
                data.put_u16(quantity);
            }
            WriteSingleCoil(address, state) => {
                data.put_u16(address);
                data.put_u16(u8_to_coil(state));
            }
            WriteMultipleCoils(address, coils) => {
                data.put_u16(address);
                let len = coils.len();
                data.put_u16(u16_len(len));
                let packed_coils = pack_coils(&coils);
                data.put_u8(u8_len(packed_coils.len()));
                for b in packed_coils {
                    data.put_u8(b);
                }
            }
            WriteSingleRegister(address, word) => {
                data.put_u16(address);
                data.put_slice(&word);
            }
            WriteMultipleRegisters(address, words) => {
                data.put_u16(address);
                data.put_u16((words.len() / 2) as u16);
                data.put_u8(words.len() as u8);
                for w in &*words {
                    data.put_u8(*w);
                }
            }
            MaskWriteRegister(address, and_mask, or_mask) => {
                data.put_u16(address);
                data.put_u16(and_mask);
                data.put_u16(or_mask);
            }
            ReadWriteMultipleRegisters(read_address, quantity, write_address, words) => {
                data.put_u16(read_address);
                data.put_u16(quantity);
                data.put_u16(write_address);
                let n = words.len();
                data.put_u16(u16_len(n));
                data.put_u8(u8_len(n * 2));
                for w in &*words {
                    data.put_u8(*w);
                }
            }
            Custom(_, custom_data) => {
                for d in &*custom_data {
                    data.put_u8(*d);
                }
            }
            Disconnect => unreachable!(),
        }
        Ok(data.freeze())
    }
}

impl<'a> TryFrom<RequestPdu<'a>> for Bytes {
    type Error = Error;

    fn try_from(pdu: RequestPdu<'a>) -> Result<Bytes, Self::Error> {
        pdu.0.try_into()
    }
}

impl From<Response> for Bytes {
    fn from(rsp: Response) -> Bytes {
        use super::frame::Response::*;
        let cnt = response_byte_count(&rsp);
        let mut data = BytesMut::with_capacity(cnt);
        data.put_u8(rsp.function_code().value());
        match rsp {
            ReadCoils(coils) | ReadDiscreteInputs(coils) => {
                let packed_coils = pack_coils(&coils);
                data.put_u8(u8_len(packed_coils.len()));
                for b in packed_coils {
                    data.put_u8(b);
                }
            }
            ReadInputRegisters(registers)
            | ReadHoldingRegisters(registers)
            | ReadWriteMultipleRegisters(registers) => {
                data.put_u8(u8_len(registers.len() * 2));
                for r in registers {
                    data.put_u8(r);
                }
            }
            WriteSingleCoil(address, state) => {
                data.put_u16(address);
                debug!("{}", state);
                data.put_u16(bool_to_coil(state));
            }
            WriteMultipleCoils(address, quantity) | WriteMultipleRegisters(address, quantity) => {
                data.put_u16(address);
                data.put_u16(quantity);
            }
            WriteSingleRegister(address, word) => {
                data.put_u16(address);
                data.put_u16(word);
            }
            MaskWriteRegister(address, and_mask, or_mask) => {
                data.put_u16(address);
                data.put_u16(and_mask);
                data.put_u16(or_mask);
            }
            Custom(_, custom_data) => {
                for d in custom_data {
                    data.put_u8(d);
                }
            }
        }
        data.freeze()
    }
}

impl From<ExceptionResponse> for Bytes {
    fn from(ex: ExceptionResponse) -> Bytes {
        let mut data = BytesMut::with_capacity(2);
        debug_assert!(ex.function.value() < 0x80);
        data.put_u8(ex.function.value() + 0x80);
        data.put_u8(ex.exception.into());
        data.freeze()
    }
}

impl From<ResponsePdu> for Bytes {
    fn from(pdu: ResponsePdu) -> Bytes {
        pdu.0.map_or_else(Into::into, Into::into)
    }
}

impl TryFrom<Bytes> for Request<'static> {
    type Error = Error;

    // TODO get will panic if not have enough data
    fn try_from(mut bytes: Bytes) -> Result<Self, Self::Error> {
        use super::frame::Request::*;
        let fn_code = bytes.get_u8();
        let req = match fn_code {
            0x01 => ReadCoils(bytes.get_u16(), bytes.get_u16()),
            0x02 => {
                debug!("bytes is {:?}", bytes);
                ReadDiscreteInputs(bytes.get_u16(), bytes.get_u16())
            }
            0x05 => {
                debug!("bytes is {:?}", bytes);

                WriteSingleCoil(bytes.get_u16(), bytes.get_u8())
            }
            0x0F => {
                let address = bytes.get_u16();
                let byte_count = bytes.get_u8();
                if bytes.len() < 6 + usize::from(byte_count) {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid byte count"));
                }
                let x = &bytes[6..];
                WriteMultipleCoils(address, Vec::from(x).into())
            }
            0x04 => ReadInputRegisters(bytes.get_u16(), bytes.get_u16()),
            0x03 => ReadHoldingRegisters(bytes.get_u16(), bytes.get_u16()),
            0x06 => {
                let address = bytes.get_u16();
                let mut data = Vec::with_capacity(2);
                for _ in 0..2 {
                    data.push(bytes.get_u8());
                }
                WriteSingleRegister(address, data.into())
            }
            0x10 => {
                let address = bytes.get_u16();
                let quantity = bytes.get_u16() * 2;
                let byte_count = bytes.get_u8();
                if bytes.len() < 6 + usize::from(byte_count) {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid byte count"));
                }
                let mut data = Vec::with_capacity(quantity.into());
                for _ in 0..quantity {
                    data.push(bytes.get_u8());
                }
                WriteMultipleRegisters(address, data.into())
            }
            0x16 => {
                let address = bytes.get_u16();
                let and_mask = bytes.get_u16();
                let or_mask = bytes.get_u16();
                MaskWriteRegister(address, and_mask, or_mask)
            }
            0x17 => {
                let read_address = bytes.get_u16();
                let read_quantity = bytes.get_u16();
                let write_address = bytes.get_u16();
                let write_quantity = bytes.get_u16();
                let write_count = bytes.get_u8();
                if bytes.len() < 10 + usize::from(write_count) {
                    return Err(Error::new(ErrorKind::InvalidData, "Invalid byte count"));
                }
                let mut data = Vec::with_capacity(write_quantity.into());
                for _ in 0..write_quantity {
                    data.push(bytes.get_u8());
                }
                ReadWriteMultipleRegisters(read_address, read_quantity, write_address, data.into())
            }
            fn_code if fn_code < 0x80 => Custom(fn_code, bytes[1..].to_vec().into()),
            fn_code => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid function code: 0x{fn_code:0>2X}"),
                ));
            }
        };
        Ok(req)
    }
}

impl TryFrom<Bytes> for RequestPdu<'static> {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let pdu = Request::try_from(bytes)?.into();
        Ok(pdu)
    }
}

impl TryFrom<Bytes> for Response {
    type Error = Error;

    fn try_from(mut bytes: Bytes) -> Result<Self, Self::Error> {
        use super::frame::Response::*;
        let fn_code = bytes.get_u8();
        let rsp = match fn_code {
            0x01 => {
                let byte_count = bytes.get_u8();
                let quantity = u16::from(byte_count) * 8;
                ReadDiscreteInputs(unpack_coils(&bytes, quantity))
            }
            0x02 => {
                let byte_count = bytes.get_u8();
                let quantity = u16::from(byte_count) * 8;
                ReadDiscreteInputs(unpack_coils(&bytes, quantity))
            }
            0x05 => WriteSingleCoil(bytes.get_u16(), bytes.get_u8()),
            0x0F => WriteMultipleCoils(bytes.get_u16(), bytes.get_u16()),
            0x04 => {
                let byte_count = bytes.get_u8();
                let mut data = Vec::with_capacity(byte_count.into());
                for _ in 0..byte_count {
                    data.push(bytes.get_u8());
                }
                ReadInputRegisters(data)
            }
            0x03 => {
                let byte_count = bytes.get_u8();
                let mut data = Vec::with_capacity(byte_count.into());
                for _ in 0..byte_count {
                    data.push(bytes.get_u8());
                }
                ReadHoldingRegisters(data)
            }
            0x06 => WriteSingleRegister(bytes.get_u16(), bytes.get_u16()),

            0x10 => WriteMultipleRegisters(bytes.get_u16(), bytes.get_u16()),
            0x16 => {
                let address = bytes.get_u16();
                let and_mask = bytes.get_u16();
                let or_mask = bytes.get_u16();
                MaskWriteRegister(address, and_mask, or_mask)
            }
            0x17 => {
                let byte_count = bytes.get_u8();
                let mut data = Vec::with_capacity(byte_count.into());
                for _ in 0..byte_count {
                    data.push(bytes.get_u8());
                }
                ReadWriteMultipleRegisters(data)
            }
            _ => {
                let mut bytes = bytes;
                Custom(fn_code, bytes.split_off(1))
            }
        };
        Ok(rsp)
    }
}

impl TryFrom<Bytes> for ExceptionResponse {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let mut rdr = Cursor::new(&bytes);
        let fn_err_code = rdr.read_u8()?;
        if fn_err_code < 0x80 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid exception function code",
            ));
        }
        let function = fn_err_code - 0x80;
        let exception = Exception::try_from(rdr.read_u8()?)?;
        Ok(ExceptionResponse {
            function: FunctionCode::new(function),
            exception,
        })
    }
}

impl TryFrom<u8> for Exception {
    type Error = Error;

    fn try_from(code: u8) -> Result<Self, Self::Error> {
        use super::frame::Exception::*;
        let ex = match code {
            0x01 => IllegalFunction,
            0x02 => IllegalDataAddress,
            0x03 => IllegalDataValue,
            0x04 => ServerDeviceFailure,
            0x05 => Acknowledge,
            0x06 => ServerDeviceBusy,
            0x08 => MemoryParityError,
            0x0A => GatewayPathUnavailable,
            0x0B => GatewayTargetDevice,
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, "Invalid exception code"));
            }
        };
        Ok(ex)
    }
}

impl TryFrom<Bytes> for ResponsePdu {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let fn_code = Cursor::new(&bytes).read_u8()?;
        let pdu = if fn_code < 0x80 {
            Response::try_from(bytes)?.into()
        } else {
            ExceptionResponse::try_from(bytes)?.into()
        };
        Ok(pdu)
    }
}

fn bool_to_coil(state: u8) -> u16 {
    if state == 1 {
        0xFF00
    } else {
        0x0000
    }
}

fn packed_coils_len(bitcount: usize) -> usize {
    (bitcount + 7) / 8
}

fn pack_coils(coils: &[u8]) -> Vec<u8> {
    let packed_size = packed_coils_len(coils.len());
    let mut res = vec![0; packed_size];
    for (i, b) in coils.iter().enumerate() {
        let v = u8::from(*b); // 0 or 1
        res[i / 8] |= v << (i % 8);
    }
    res
}

fn request_byte_count(req: &Request<'_>) -> usize {
    use super::frame::Request::*;
    match *req {
        ReadCoils(_, _)
        | ReadDiscreteInputs(_, _)
        | ReadInputRegisters(_, _)
        | ReadHoldingRegisters(_, _)
        | WriteSingleRegister(_, _)
        | WriteSingleCoil(_, _) => 5,
        // TODO
        WriteMultipleCoils(_, ref coils) => 6 + packed_coils_len(coils.len()),
        WriteMultipleRegisters(_, ref data) => 6 + data.len(),
        MaskWriteRegister(_, _, _) => 7,
        ReadWriteMultipleRegisters(_, _, _, ref data) => 10 + data.len(),
        Custom(_, ref data) => 1 + data.len(),
        Disconnect => unreachable!(),
    }
}

fn response_byte_count(rsp: &Response) -> usize {
    use super::frame::Response::*;
    match *rsp {
        ReadCoils(ref coils) | ReadDiscreteInputs(ref coils) => 2 + packed_coils_len(coils.len()),
        WriteSingleCoil(_, _)
        | WriteMultipleCoils(_, _)
        | WriteMultipleRegisters(_, _)
        | WriteSingleRegister(_, _) => 5,
        ReadInputRegisters(ref data)
        | ReadHoldingRegisters(ref data)
        | ReadWriteMultipleRegisters(ref data) => 2 + data.len() * 2,
        MaskWriteRegister(_, _, _) => 7,
        Custom(_, ref data) => 1 + data.len(),
    }
}

fn u8_to_coil(state: u8) -> u16 {
    if state == 1 {
        0xFF00
    } else {
        0x0000
    }
}

fn unpack_coils(bytes: &Bytes, count: u16) -> Vec<u8> {
    let mut res = Vec::with_capacity(count.into());
    for i in 0usize..count.into() {
        match (bytes[i / 8] >> (i % 8)) & 0b1 > 0 {
            true => res.push(1),
            false => res.push(0),
        }
    }
    res
}
