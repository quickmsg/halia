use std::io::{Error, ErrorKind, Result};

use byteorder::{BigEndian, ByteOrder};
use tokio_util::codec::{Decoder, Encoder};

use crate::modbus_bak::frame::tcp::{Header, RequestAdu, ResponseAdu};

use super::*;

const HEADER_LEN: usize = 7;

const PROTOCOL_ID: u16 = 0x0000; // TCP

#[derive(Debug, PartialEq)]
pub(crate) struct AduDecoder;

#[derive(Debug, PartialEq)]
pub(crate) struct ClientCodec {
    pub(crate) decoder: AduDecoder,
}

impl Default for ClientCodec {
    fn default() -> Self {
        Self {
            decoder: AduDecoder,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct ServerCodec {
    pub(crate) decoder: AduDecoder,
}

impl Default for ServerCodec {
    fn default() -> Self {
        Self {
            decoder: AduDecoder,
        }
    }
}

impl Decoder for AduDecoder {
    type Item = (Header, Bytes);
    type Error = Error;

    #[allow(clippy::assertions_on_constants)]
    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<(Header, Bytes)>> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }

        debug_assert!(HEADER_LEN >= 6);
        let len = usize::from(BigEndian::read_u16(&buf[4..6]));
        let pdu_len = if len > 0 {
            // len = bytes of PDU + one byte (unit ID)
            len - 1
        } else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid data length: {len}"),
            ));
        };
        if buf.len() < HEADER_LEN + pdu_len {
            return Ok(None);
        }

        let header_data = buf.split_to(HEADER_LEN);

        debug_assert!(HEADER_LEN >= 4);
        let protocol_id = BigEndian::read_u16(&header_data[2..4]);
        if protocol_id != PROTOCOL_ID {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Invalid protocol identifier: expected = {PROTOCOL_ID}, actual = {protocol_id}"
                ),
            ));
        }

        debug_assert!(HEADER_LEN >= 2);
        let transaction_id = BigEndian::read_u16(&header_data[0..2]);

        debug_assert!(HEADER_LEN > 6);
        let unit_id = header_data[6];

        let header = Header {
            transaction_id,
            unit_id,
        };

        let pdu_data = buf.split_to(pdu_len).freeze();

        Ok(Some((header, pdu_data)))
    }
}

impl Decoder for ClientCodec {
    type Item = ResponseAdu;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<ResponseAdu>> {
        if let Some((hdr, pdu_data)) = self.decoder.decode(buf)? {
            let pdu = ResponsePdu::try_from(pdu_data)?;
            Ok(Some(ResponseAdu { hdr, pdu }))
        } else {
            Ok(None)
        }
    }
}

impl Decoder for ServerCodec {
    type Item = RequestAdu<'static>;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<RequestAdu<'static>>> {
        if let Some((hdr, pdu_data)) = self.decoder.decode(buf)? {
            let pdu = RequestPdu::try_from(pdu_data)?;
            Ok(Some(RequestAdu {
                hdr,
                pdu,
                disconnect: false,
            }))
        } else {
            Ok(None)
        }
    }
}

impl<'a> Encoder<RequestAdu<'a>> for ClientCodec {
    type Error = Error;

    fn encode(&mut self, adu: RequestAdu<'a>, buf: &mut BytesMut) -> Result<()> {
        if adu.disconnect {
            // The disconnect happens implicitly after letting this request
            // fail by returning an error. This will drop the attached
            // transport, e.g. for terminating an open connection.
            return Err(Error::new(
                ErrorKind::NotConnected,
                "Disconnecting - not an error",
            ));
        }
        let RequestAdu { hdr, pdu, .. } = adu;
        let pdu_data: Bytes = pdu.try_into()?;
        buf.reserve(pdu_data.len() + 7);
        buf.put_u16(hdr.transaction_id);
        buf.put_u16(PROTOCOL_ID);
        buf.put_u16((pdu_data.len() + 1) as u16);
        buf.put_u8(hdr.unit_id);
        buf.put_slice(&pdu_data);
        Ok(())
    }
}

impl Encoder<ResponseAdu> for ServerCodec {
    type Error = Error;

    fn encode(&mut self, adu: ResponseAdu, buf: &mut BytesMut) -> Result<()> {
        let ResponseAdu { hdr, pdu } = adu;
        let pdu_data: Bytes = pdu.into();
        buf.reserve(pdu_data.len() + 7);
        buf.put_u16(hdr.transaction_id);
        buf.put_u16(PROTOCOL_ID);
        buf.put_u16((pdu_data.len() + 1) as u16);
        buf.put_u8(hdr.unit_id);
        buf.put_slice(&pdu_data);
        Ok(())
    }
}
