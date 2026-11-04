use std::fs::File;

pub(crate) fn close_file(file:File)->Result<(),String>{
    use std::os::fd::IntoRawFd;
    unsafe extern "C"{fn close(fd:std::os::raw::c_int)->std::os::raw::c_int;}
    if unsafe{close(file.into_raw_fd())}==0{Ok(())}else{Err(format!("close report file: {}",std::io::Error::last_os_error()))}
}
