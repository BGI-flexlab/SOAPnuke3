//! System-zlib gzip reader used by the production FASTQ source.
//!
//! Low-level `inflate()` is used instead of `gzread()` so every gzip member
//! is validated and trailing garbage or a truncated final member is rejected.

use std::ffi::CStr;
use std::fs::File;
use std::io::{self, Read};
use std::os::raw::{c_char, c_int, c_uint, c_ulong, c_void};

const INPUT_BUFFER_SIZE: usize = 1 << 20;
const OUTPUT_BUFFER_SIZE: usize = 1 << 20;
const Z_NO_FLUSH: c_int = 0;
const Z_OK: c_int = 0;
const Z_STREAM_END: c_int = 1;
const Z_BUF_ERROR: c_int = -5;

#[repr(C)]
struct ZStream {
    next_in: *mut u8,
    avail_in: c_uint,
    total_in: c_ulong,
    next_out: *mut u8,
    avail_out: c_uint,
    total_out: c_ulong,
    msg: *const c_char,
    state: *mut c_void,
    zalloc: Option<unsafe extern "C" fn(*mut c_void, c_uint, c_uint) -> *mut c_void>,
    zfree: Option<unsafe extern "C" fn(*mut c_void, *mut c_void)>,
    opaque: *mut c_void,
    data_type: c_int,
    adler: c_ulong,
    reserved: c_ulong,
}

extern "C" {
    fn zlibVersion() -> *const c_char;
    fn inflateInit2_(
        stream: *mut ZStream,
        window_bits: c_int,
        version: *const c_char,
        stream_size: c_int,
    ) -> c_int;
    fn inflate(stream: *mut ZStream, flush: c_int) -> c_int;
    fn inflateEnd(stream: *mut ZStream) -> c_int;
}

pub struct ZlibReader {
    file: crate::native_io::InputReader,
    input: Vec<u8>,
    input_off: usize,
    input_len: usize,
    eof: bool,
    stream: Option<Box<ZStream>>,
    output: Vec<u8>,
    output_start: usize,
    output_end: usize,
    members_seen: usize,
    done: bool,
    error: Option<String>,
    path: String,
}

// A reader is created and consumed by exactly one lane thread. The zlib
// stream never crosses that owner boundary while it is active.
unsafe impl Send for ZlibReader {}

impl ZlibReader {
    pub fn open(path: &str) -> io::Result<ZlibReader> {
        Ok(ZlibReader {
            file: crate::native_io::InputReader::File(File::open(path)?),
            input: vec![0u8; INPUT_BUFFER_SIZE],
            input_off: 0,
            input_len: 0,
            eof: false,
            stream: None,
            output: vec![0u8; OUTPUT_BUFFER_SIZE],
            output_start: 0,
            output_end: 0,
            members_seen: 0,
            done: false,
            error: None,
            path: path.to_string(),
        })
    }

    pub(crate) fn from_reader(reader: Box<dyn Read + Send>, label: &str) -> Self {
        Self { file: crate::native_io::InputReader::Owned(reader), input: vec![0u8; INPUT_BUFFER_SIZE], input_off: 0, input_len: 0, eof: false, stream: None, output: vec![0u8; OUTPUT_BUFFER_SIZE], output_start: 0, output_end: 0, members_seen: 0, done: false, error: None, path: label.into() }
    }
    fn fail(&mut self, message: impl Into<String>) -> io::Error {
        let message = message.into();
        self.error = Some(message.clone());
        io::Error::new(io::ErrorKind::InvalidData, message)
    }

    fn refill_raw(&mut self, allow_read: bool) -> io::Result<bool> {
        if self.input_off < self.input_len {
            return Ok(true);
        }
        if self.eof {
            return Ok(false);
        }
        if !allow_read {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "zlib reader needs compressed input",
            ));
        }
        self.input_off = 0;
        self.input_len = self.file.read(&mut self.input)?;
        if self.input_len == 0 {
            self.eof = true;
            return Ok(false);
        }
        Ok(true)
    }

    fn start_member(&mut self) -> io::Result<()> {
        let version = unsafe { zlibVersion() };
        if version.is_null() {
            return Err(self.fail("system zlib did not provide a version"));
        }
        let mut stream = Box::new(unsafe { std::mem::zeroed::<ZStream>() });
        // 31 selects the gzip wrapper only. A zlib-wrapped stream is not a
        // valid .gz input and must not be accepted at this seam.
        let result = unsafe {
            inflateInit2_(
                &mut *stream,
                31,
                version,
                std::mem::size_of::<ZStream>() as c_int,
            )
        };
        if result != Z_OK {
            return Err(self.fail(format!(
                "cannot initialize system zlib for {} (status {})",
                self.path, result
            )));
        }
        self.stream = Some(stream);
        Ok(())
    }

    fn end_member(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            unsafe { inflateEnd(&mut *stream) };
            self.members_seen += 1;
        }
    }

    fn zlib_message(&self, status: c_int) -> String {
        let detail = self
            .stream
            .as_ref()
            .and_then(|stream| {
                if stream.msg.is_null() {
                    None
                } else {
                    Some(unsafe { CStr::from_ptr(stream.msg) }.to_string_lossy().into_owned())
                }
            })
            .unwrap_or_else(|| "invalid gzip stream".to_string());
        format!(
            "system zlib error for {} (status {}): {}",
            self.path, status, detail
        )
    }

    fn produce(&mut self, allow_read: bool) -> io::Result<bool> {
        if self.output_start < self.output_end {
            return Ok(true);
        }
        self.output_start = 0;
        self.output_end = 0;
        if self.done {
            return Ok(false);
        }
        loop {
            if self.stream.is_none() {
                if !self.refill_raw(allow_read)? {
                    if self.members_seen == 0 {
                        return Err(self.fail("invalid gzip input: no gzip member"));
                    }
                    self.done = true;
                    return Ok(false);
                }
                self.start_member()?;
            }
            let stream = self.stream.as_mut().expect("stream initialized");
            if self.input_off < self.input_len {
                stream.next_in = self.input[self.input_off..].as_ptr() as *mut u8;
                stream.avail_in = (self.input_len - self.input_off) as c_uint;
            } else {
                stream.next_in = std::ptr::null_mut();
                stream.avail_in = 0;
            }
            stream.next_out = self.output.as_mut_ptr();
            stream.avail_out = self.output.len() as c_uint;
            let before_input = stream.avail_in as usize;
            let status = unsafe { inflate(&mut **stream, Z_NO_FLUSH) };
            let consumed = before_input - stream.avail_in as usize;
            self.input_off += consumed;
            let produced = self.output.len() - stream.avail_out as usize;
            if produced > 0 {
                self.output_end = produced;
                if status == Z_STREAM_END {
                    self.end_member();
                } else if status != Z_OK {
                    return Err(self.fail(self.zlib_message(status)));
                }
                return Ok(true);
            }
            match status {
                Z_STREAM_END => self.end_member(),
                Z_OK | Z_BUF_ERROR => {
                    if stream.avail_in == 0 {
                        if !self.refill_raw(allow_read)? {
                            return Err(self.fail("truncated gzip deflate stream"));
                        }
                    } else if status == Z_BUF_ERROR {
                        return Err(self.fail(self.zlib_message(status)));
                    }
                }
                _ => return Err(self.fail(self.zlib_message(status))),
            }
        }
    }

    pub fn available_without_refill(&mut self) -> io::Result<&[u8]> {
        if self.output_start >= self.output_end && !self.produce(false)? {
            return Ok(&[]);
        }
        Ok(&self.output[self.output_start..self.output_end])
    }

    pub fn refill_input(&mut self) -> io::Result<bool> {
        self.refill_raw(true)
    }

    pub fn consume(&mut self, n: usize) {
        assert!(n <= self.output_end.saturating_sub(self.output_start));
        self.output_start += n;
        if self.output_start == self.output_end {
            self.output_start = 0;
            self.output_end = 0;
        }
    }

    #[cfg(test)]
    pub fn available(&mut self) -> io::Result<&[u8]> {
        loop {
            if self.output_start < self.output_end {
                return Ok(&self.output[self.output_start..self.output_end]);
            }
            match self.produce(false) {
                Ok(true) => continue,
                Ok(false) => return Ok(&[]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.refill_input()?;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for ZlibReader {
    fn drop(&mut self) {
        self.end_member();
    }
}

struct ActiveStream {
    stream: Box<ZStream>,
}

impl Drop for ActiveStream {
    fn drop(&mut self) {
        unsafe { inflateEnd(&mut *self.stream) };
    }
}

fn stream_error(stream: &ZStream, status: c_int) -> String {
    let detail = if stream.msg.is_null() {
        "invalid gzip stream".to_string()
    } else {
        unsafe { CStr::from_ptr(stream.msg) }
            .to_string_lossy()
            .into_owned()
    };
    if detail.contains("length check") {
        return "gzip ISIZE mismatch".to_string();
    }
    if detail.contains("data check") || detail.contains("CRC") {
        return "gzip CRC mismatch".to_string();
    }
    format!("system zlib error (status {}): {}", status, detail)
}

fn inflate_member(bytes: &[u8], start: usize) -> Result<(usize, Vec<u8>), String> {
    if bytes.len().saturating_sub(start) < 10
        || bytes[start] != 0x1f
        || bytes[start + 1] != 0x8b
        || bytes[start + 2] != 8
    {
        return Err("invalid gzip member header".to_string());
    }
    let version = unsafe { zlibVersion() };
    if version.is_null() {
        return Err("system zlib did not provide a version".to_string());
    }
    let mut stream = Box::new(unsafe { std::mem::zeroed::<ZStream>() });
    let result = unsafe {
        inflateInit2_(
            &mut *stream,
            31,
            version,
            std::mem::size_of::<ZStream>() as c_int,
        )
    };
    if result != Z_OK {
        return Err(format!("cannot initialize system zlib (status {})", result));
    }
    let mut active = ActiveStream { stream };
    let mut input_offset = start;
    let mut member = Vec::new();
    let mut out_buf = vec![0u8; OUTPUT_BUFFER_SIZE];
    loop {
        if input_offset >= bytes.len() {
            return Err("truncated gzip deflate stream".to_string());
        }
        let input_len = (bytes.len() - input_offset).min(c_uint::MAX as usize);
        active.stream.next_in = bytes[input_offset..input_offset + input_len].as_ptr() as *mut u8;
        active.stream.avail_in = input_len as c_uint;
        active.stream.next_out = out_buf.as_mut_ptr();
        active.stream.avail_out = out_buf.len() as c_uint;
        let before_input = active.stream.avail_in as usize;
        let status = unsafe { inflate(&mut *active.stream, Z_NO_FLUSH) };
        let consumed = before_input - active.stream.avail_in as usize;
        input_offset += consumed;
        let produced = out_buf.len() - active.stream.avail_out as usize;
        member.extend_from_slice(&out_buf[..produced]);
        if status == Z_STREAM_END {
            break;
        }
        if status != Z_OK {
            return Err(stream_error(&active.stream, status));
        }
        if consumed == 0 && produced == 0 {
            return Err("system zlib made no progress".to_string());
        }
    }
    // zlib's gzip wrapper consumes and validates the eight-byte trailer before
    // returning Z_STREAM_END. The returned input offset therefore points at
    // the next concatenated member, if any.
    Ok((input_offset, member))
}

/// Decompress and validate one or more concatenated gzip members held in memory.
/// This is used only when a split/head sink must inspect a closed output member.
pub fn inflate_gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut cursor = 0usize;
    let mut members = 0usize;
    let mut output = Vec::new();
    while cursor < bytes.len() {
        let (next, member) = inflate_member(bytes, cursor)?;
        cursor = next;
        output.extend_from_slice(&member);
        members += 1;
    }
    if members == 0 {
        return Err("invalid gzip input: no gzip member".to_string());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{inflate_gzip_bytes, ZlibReader};
    use crate::ffi::Compressor;
    use std::fs;
    use std::io;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&directory).unwrap();
        directory.join(format!("soapnuke-zlib-{label}-{}-{nonce}.gz", std::process::id()))
    }

    fn read_all(reader: &mut ZlibReader) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        loop {
            let length;
            {
                let bytes = reader.available()?;
                if bytes.is_empty() {
                    break;
                }
                length = bytes.len();
                output.extend_from_slice(bytes);
            }
            reader.consume(length);
        }
        Ok(output)
    }

    #[test]
    fn rejects_zlib_wrapper_at_gzip_input_seam() {
        let path = fixture_path("zlib-wrapper");
        fs::write(&path, bytes_from_hex("789ccb48cdc9c90700062c0215")).unwrap();
        let mut reader = ZlibReader::open(path.to_str().unwrap()).unwrap();
        let error = read_all(&mut reader).expect_err("zlib wrapper was accepted as gzip");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_gzip_member_followed_by_zlib_wrapper() {
        let path = fixture_path("gzip-then-zlib");
        let compressor = Compressor::new(2).unwrap();
        let mut bytes = compressor.gzip_compress_alloc(b"@read/1\nACGT\n+\nIIII\n");
        bytes.extend_from_slice(&bytes_from_hex("789ccb48cdc9c90700062c0215"));
        fs::write(&path, bytes).unwrap();
        let mut reader = ZlibReader::open(path.to_str().unwrap()).unwrap();
        assert!(read_all(&mut reader).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn accepts_empty_and_concatenated_gzip_members() {
        let path = fixture_path("empty-concatenated");
        let compressor = Compressor::new(2).unwrap();
        let mut bytes = compressor.gzip_compress_alloc(b"");
        bytes.extend_from_slice(&compressor.gzip_compress_alloc(b"second member\n"));
        fs::write(&path, bytes).unwrap();
        let mut reader = ZlibReader::open(path.to_str().unwrap()).unwrap();
        assert_eq!(read_all(&mut reader).unwrap(), b"second member\n");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn in_memory_decoder_rejects_zlib_wrapper() {
        let error = inflate_gzip_bytes(&bytes_from_hex("789ccb48cdc9c90700062c0215"))
            .expect_err("zlib wrapper was accepted by in-memory gzip decoder");
        assert!(error.contains("gzip member header"));
    }

    fn bytes_from_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }
}
