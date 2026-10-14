//! A native extension that echoes its arguments: the smallest library that
//! speaks the elri C ABI. The core's tests load it.

#![allow(unsafe_code)]

use std::ffi::c_int;

/// Handles one call. `req` is JSON `{"function", "args", "space"}`; the
/// reply is JSON the caller releases with [`elri_ext_free`].
///
/// # Safety
/// `req` must point to `req_len` readable bytes; `out` and `out_len` must
/// be valid for writes.
#[no_mangle]
pub unsafe extern "C" fn elri_ext_call(
    req: *const u8,
    req_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    let bytes = unsafe { std::slice::from_raw_parts(req, req_len) };
    let reply = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(v) => {
            let function = v.get("function").and_then(|f| f.as_str()).unwrap_or("");
            if function == "echo" {
                serde_json::json!({ "echo": v.get("args").cloned().unwrap_or(serde_json::Value::Null), "space": v.get("space") })
            } else {
                serde_json::json!({ "error": format!("no function {function}") })
            }
        }
        Err(e) => serde_json::json!({ "error": e.to_string() }),
    };
    let mut buf = reply.to_string().into_bytes().into_boxed_slice();
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    unsafe {
        *out = ptr;
        *out_len = len;
    }
    0
}

/// Releases a reply buffer.
///
/// # Safety
/// `ptr` and `len` must come from [`elri_ext_call`].
#[no_mangle]
pub unsafe extern "C" fn elri_ext_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) });
    }
}
