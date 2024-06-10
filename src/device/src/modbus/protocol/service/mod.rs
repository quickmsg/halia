// SPDX-FileCopyrightText: Copyright (c) 2017-2024 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

pub(crate) mod rtu;

pub(crate) mod tcp;

fn verify_response_header<H: Eq + std::fmt::Debug>(req_hdr: &H, rsp_hdr: &H) -> Result<(), String> {
    if req_hdr != rsp_hdr {
        return Err(format!(
            "expected/request = {req_hdr:?}, actual/response = {rsp_hdr:?}"
        ));
    }
    Ok(())
}
