//! C-compatible adapter for the optional rapidgzip input backend.
//!
//! The C++ side owns one rapidgzip reader and one bounded decoded chunk.
//! Rust keeps only the opaque handle and exposes the same available/refill/
//! consume seam used by the existing zlib reader.

use std::ffi::{CStr, CString};
use std::io;
use std::os::raw::{c_char, c_uchar};
use std::ptr;

const ERROR_SIZE: usize = 1024;

#[repr(C)]
struct RapidGzipHandle {
    _private: [u8; 0],
}

extern "C" {
    fn soapnuke_rapidgzip_open(
        path: *const c_char,
        decoder_threads: usize,
        error: *mut c_char,
        error_size: usize,
    ) -> *mut RapidGzipHandle;
    fn soapnuke_rapidgzip_available(
        reader: *mut RapidGzipHandle,
        data: *mut *const c_uchar,
        size: *mut usize,
        error: *mut c_char,
        error_size: usize,
    ) -> i32;
    fn soapnuke_rapidgzip_refill(
        reader: *mut RapidGzipHandle,
        error: *mut c_char,
        error_size: usize,
    ) -> i32;
    fn soapnuke_rapidgzip_consume(
        reader: *mut RapidGzipHandle,
        size: usize,
        error: *mut c_char,
        error_size: usize,
    ) -> i32;
    fn soapnuke_rapidgzip_cancel(reader: *mut RapidGzipHandle);
    fn soapnuke_rapidgzip_close(reader: *mut RapidGzipHandle);
    fn soapnuke_rapidgzip_decoder_threads(reader: *const RapidGzipHandle) -> usize;
    fn soapnuke_rapidgzip_parallel_reader_active(reader: *const RapidGzipHandle) -> i32;
}

pub struct RapidGzipReader {
    handle: *mut RapidGzipHandle,
    path: String,
}

// A handle is consumed by exactly one source lane. Refill invokes the C++
// decoder synchronously; rapidgzip's internal helpers, when requested, stay
// within the reserved decoder allocation.
unsafe impl Send for RapidGzipReader {}

impl RapidGzipReader {
    pub fn open(path: &str, decoder_threads: usize) -> io::Result<RapidGzipReader> {
        let c_path = CString::new(path).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "gzip input path contains NUL")
        })?;
        let mut error = [0 as c_char; ERROR_SIZE];
        let handle = unsafe {
            soapnuke_rapidgzip_open(
                c_path.as_ptr(),
                decoder_threads,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if handle.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                error_message(&error, "cannot open rapidgzip reader"),
            ));
        }
        Ok(RapidGzipReader {
            handle,
            path: path.to_string(),
        })
    }

    pub(crate) fn dedup_backend(&self)->crate::decoder::Decoder{crate::decoder::Decoder{backend:"rapidgzip",parallelism:unsafe{soapnuke_rapidgzip_decoder_threads(self.handle)},active:Some(unsafe{soapnuke_rapidgzip_parallel_reader_active(self.handle)}!=0),rapid:true}}
    pub fn backend_facts(&self) -> serde_json::Value {
        serde_json::json!({"backend":"rapidgzip","decoder_parallelism":unsafe{soapnuke_rapidgzip_decoder_threads(self.handle)},"parallel_reader_active":unsafe{soapnuke_rapidgzip_parallel_reader_active(self.handle)}!=0,
            "proof":"live native handle fields; serial GzipReader selected at parallelism1; OS thread observation is separate","native_helper_reservation":0})
    }
    pub fn available_without_refill(&mut self) -> io::Result<&[u8]> {
        loop {
            let mut data = ptr::null();
            let mut size = 0usize;
            let mut error = [0 as c_char; ERROR_SIZE];
            let result = unsafe {
                soapnuke_rapidgzip_available(
                    self.handle,
                    &mut data,
                    &mut size,
                    error.as_mut_ptr(),
                    error.len(),
                )
            };
            match result {
                1 => {
                    if data.is_null() || size == 0 {
                        return Err(self.invalid_data("rapidgzip returned an empty decoded span"));
                    }
                    return Ok(unsafe { std::slice::from_raw_parts(data, size) });
                }
                2 => {
                    let refill_result = self.refill_once()?;
                    match refill_result {
                        1 => continue,
                        0 => return Ok(&[]),
                        _ => unreachable!("rapidgzip refill_once only returns 0, 1, or an error"),
                    }
                }
                0 => return Ok(&[]),
                _ => {
                    return Err(self.invalid_data(&error_message(&error, "rapidgzip decoder error")))
                }
            }
        }
    }

    fn refill_once(&mut self) -> io::Result<i32> {
        let mut error = [0 as c_char; ERROR_SIZE];
        let result =
            unsafe { soapnuke_rapidgzip_refill(self.handle, error.as_mut_ptr(), error.len()) };
        match result {
            1 | 0 => Ok(result),
            _ => Err(self.invalid_data(&error_message(&error, "rapidgzip decoder error"))),
        }
    }

    pub fn refill_input(&mut self) -> io::Result<bool> {
        Err(self.invalid_data(
            "direct refill is unreachable; available_without_refill performs synchronous refills",
        ))
    }

    pub fn consume(&mut self, size: usize) -> io::Result<()> {
        let mut error = [0 as c_char; ERROR_SIZE];
        let result = unsafe {
            soapnuke_rapidgzip_consume(self.handle, size, error.as_mut_ptr(), error.len())
        };
        if result == 0 {
            Ok(())
        } else {
            Err(self.invalid_data(&format!(
                "rapidgzip consume failed: {}",
                error_message(&error, "invalid decoded span")
            )))
        }
    }

    fn invalid_data(&self, message: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("rapidgzip input {}: {}", self.path, message),
        )
    }
}

impl Drop for RapidGzipReader {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                soapnuke_rapidgzip_cancel(self.handle);
                soapnuke_rapidgzip_close(self.handle);
            }
            self.handle = ptr::null_mut();
        }
    }
}

fn error_message(buffer: &[c_char], fallback: &str) -> String {
    if buffer.is_empty() {
        return fallback.to_string();
    }
    let message = unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_string_lossy()
        .trim()
        .to_string();
    if message.is_empty() {
        fallback.to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::RapidGzipReader;
    use crate::cpu_budget::{CpuBudget, Stage};
    use std::fs;
    use std::path::PathBuf;

    fn fixture_path(name: &str) -> PathBuf {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&directory).unwrap();
        directory.join(format!("{name}.gz"))
    }

    #[test]
    fn invalid_consume_is_reported_as_io_error() {
        let path = fixture_path("rapidgzip-invalid-consume");
        fs::write(
            &path,
            [
                0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xcb, 0x48, 0xcd, 0xc9,
                0xc9, 0xe7, 0x02, 0x00, 0x20, 0x30, 0x3a, 0x36, 0x06, 0x00, 0x00, 0x00,
            ],
        )
        .unwrap();
        let mut reader = RapidGzipReader::open(path.to_str().unwrap(), 1).unwrap();
        let available = reader
            .available_without_refill()
            .expect("valid gzip failed before invalid consume")
            .len();
        assert!(available > 0);
        let error = reader.consume(available + 1).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("consume"));
        drop(reader);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn synchronous_refill_is_inside_source_permit() {
        let path = fixture_path("rapidgzip-source-permit");
        fs::write(
            &path,
            [
                0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xcb, 0x48, 0xcd, 0xc9,
                0xc9, 0xe7, 0x02, 0x00, 0x20, 0x30, 0x3a, 0x36, 0x06, 0x00, 0x00, 0x00,
            ],
        )
        .unwrap();
        let budget = CpuBudget::new(1);
        let mut reader = RapidGzipReader::open(path.to_str().unwrap(), 1).unwrap();
        let permit = budget.acquire(Stage::Source);
        let bytes = reader
            .available_without_refill()
            .expect("synchronous rapidgzip refill failed");
        assert!(!bytes.is_empty());
        drop(permit);
        let metrics = budget.snapshot();
        assert_eq!(metrics.peak_active, 1);
        assert!(metrics.active_cpu_nanos[Stage::Source as usize] > 0);
        drop(reader);
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
#[path = "native_refill_tests.rs"]
mod refill_tests;
