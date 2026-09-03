use std::io;
use std::os::windows::io::AsRawHandle;
use std::ptr::null_mut;

use windows_sys::Win32::Foundation::LocalFree;
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use super::owned_handle;

pub(crate) struct PrivateSecurity(*mut core::ffi::c_void);

impl PrivateSecurity {
    pub fn new() -> io::Result<Self> {
        let sid = current_user_sid()?;
        // Protected DACL: only the current user, no inherited Everyone/network
        // grants. Explicit owner also avoids inheriting an Administrators owner.
        let sddl = format!("O:{sid}D:P(A;;GA;;;{sid})\0")
            .encode_utf16()
            .collect::<Vec<_>>();
        let mut descriptor = null_mut();
        // SAFETY: valid NUL-terminated SDDL; successful allocation uses LocalFree.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(descriptor))
    }

    pub fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }
}

impl Drop for PrivateSecurity {
    fn drop(&mut self) {
        // SAFETY: this is the unique descriptor allocation returned by conversion.
        unsafe {
            LocalFree(self.0);
        }
    }
}

fn current_user_sid() -> io::Result<String> {
    let mut token = null_mut();
    // SAFETY: query the current process token; successful handle is adopted below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = owned_handle(token)?;
    let mut length = 0;
    // SAFETY: first call only determines the variable-sized token information buffer.
    unsafe {
        GetTokenInformation(token.as_raw_handle(), TokenUser, null_mut(), 0, &mut length);
    }
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    // usize provides the alignment TOKEN_USER requires, unlike Vec<u8>.
    let mut buffer = vec![0_usize; (length as usize).div_ceil(size_of::<usize>())];
    // SAFETY: buffer is aligned, large enough, and retained through SID conversion.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            buffer.as_mut_ptr().cast(),
            length,
            &mut length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful TokenUser query initializes a TOKEN_USER at this address.
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut text = null_mut();
    // SAFETY: the queried token owns a valid SID; returned string uses LocalFree.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &mut text) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: conversion returns a NUL-terminated wide string allocated by Windows.
    let sid = unsafe {
        let mut length = 0;
        while *text.add(length) != 0 {
            length += 1;
        }
        let sid = String::from_utf16_lossy(std::slice::from_raw_parts(text, length));
        LocalFree(text.cast());
        sid
    };
    Ok(sid)
}
