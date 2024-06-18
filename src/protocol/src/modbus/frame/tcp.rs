use super::*;

pub(crate) type TransactionId = u16;
pub(crate) type UnitId = u8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) transaction_id: TransactionId,
    pub(crate) unit_id: UnitId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAdu<'a> {
    pub(crate) hdr: Header,
    pub(crate) pdu: RequestPdu<'a>,
    pub(crate) disconnect: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResponseAdu {
    pub(crate) hdr: Header,
    pub(crate) pdu: ResponsePdu,
}

impl<'a> From<RequestAdu<'a>> for Request<'a> {
    fn from(from: RequestAdu<'a>) -> Self {
        from.pdu.into()
    }
}
