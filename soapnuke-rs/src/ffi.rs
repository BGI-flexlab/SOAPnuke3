// libdeflate FFI: CRC32 and gzip compression (used for output shards).
use std::os::raw::c_int;

pub struct Compressor {
    ptr: *mut std::ffi::c_void,
}

unsafe impl Send for Compressor {}

impl Compressor {
    pub fn new(level: c_int) -> Result<Compressor, String> {
        let ptr = unsafe { libdeflate_alloc_compressor(level) };
        if ptr.is_null() {
            return Err("libdeflate_alloc_compressor failed".to_string());
        }
        Ok(Compressor { ptr })
    }

    // Compress into a gzip member (header + deflate + trailer). Returns bytes
    // written to output; output must be large enough (caller retries with a
    // bigger buffer if needed).
    pub fn gzip_compress(&self, input: &[u8], output: &mut [u8]) -> usize {
        unsafe {
            libdeflate_gzip_compress(
                self.ptr,
                input.as_ptr(),
                input.len(),
                output.as_mut_ptr(),
                output.len(),
            )
        }
    }

    // Convenience: compress with a dynamically sized buffer.
    pub fn gzip_compress_alloc(&self, input: &[u8]) -> Vec<u8> {
        let cap = unsafe { libdeflate_gzip_compress_bound(self.ptr, input.len()) };
        let mut out = vec![0u8; cap];
        let n = self.gzip_compress(input, &mut out);
        out.truncate(n);
        out
    }
}

impl Drop for Compressor {
    fn drop(&mut self) {
        unsafe { libdeflate_free_compressor(self.ptr) }
    }
}

pub fn crc32(seed: u32, data: &[u8]) -> u32 {
    unsafe { libdeflate_crc32(seed, data.as_ptr(), data.len()) }
}

#[link(name = "deflate")]
extern "C" {
    fn libdeflate_alloc_compressor(level: c_int) -> *mut std::ffi::c_void;
    fn libdeflate_gzip_compress(
        c: *mut std::ffi::c_void,
        in_data: *const u8,
        in_nbytes: usize,
        out_data: *mut u8,
        out_nbytes_avail: usize,
    ) -> usize;
    fn libdeflate_gzip_compress_bound(c: *mut std::ffi::c_void, input_size: usize) -> usize;
    fn libdeflate_free_compressor(c: *mut std::ffi::c_void);
    fn libdeflate_crc32(seed: u32, data: *const u8, len: usize) -> u32;
}
