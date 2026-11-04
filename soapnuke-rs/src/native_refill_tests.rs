use super::*;
const LIMIT: usize = 1024 * 1024;
fn fixture(name: &str) -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/native-refill/fixtures")
        .join(name)
        .to_str()
        .unwrap()
        .to_owned()
}
fn drain(name: &str, length: usize, expect_error: bool) -> Vec<usize> {
    let reader = RapidGzipReader::open(&fixture(name), 1).unwrap();
    let mut error = [0 as c_char; ERROR_SIZE];
    let mut offset = 0;
    let mut chunk = 0;
    let mut chunks = Vec::new();
    let mut step = 0;
    let mut failed = false;
    loop {
        let mut data = ptr::null();
        let mut size = 0;
        let available = unsafe {
            soapnuke_rapidgzip_available(
                reader.handle,
                &mut data,
                &mut size,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if available == 1 {
            assert!(!data.is_null() && size > 0);
            let take = size.min([1, 97, 8191, usize::MAX][step % 4]);
            let bytes = unsafe { std::slice::from_raw_parts(data, take) };
            assert!(bytes
                .iter()
                .enumerate()
                .all(|(j, b)| *b == ((offset + j) % 251) as u8));
            assert_eq!(
                unsafe {
                    soapnuke_rapidgzip_consume(reader.handle, take, error.as_mut_ptr(), error.len())
                },
                0
            );
            offset += take;
            chunk += take;
            step += 1;
            assert!(
                chunk <= LIMIT,
                "a refill offered more than the independent 1MiB limit"
            );
        } else if available == 2 {
            if chunk > 0 {
                chunks.push(chunk);
                chunk = 0;
            }
            let result = unsafe {
                soapnuke_rapidgzip_refill(reader.handle, error.as_mut_ptr(), error.len())
            };
            if result < 0 {
                failed = true;
                break;
            }
            if result == 0 {
                break;
            }
            assert_eq!(result, 1);
        } else {
            assert_eq!(available, 0);
            break;
        }
    }
    if chunk > 0 {
        chunks.push(chunk);
    }
    assert_eq!(
        failed,
        expect_error,
        "{}",
        error_message(&error, "unexpected EOF")
    );
    if !expect_error {
        assert_eq!(offset, length);
    } else {
        assert!(offset <= length);
    }
    chunks
}
#[test]
fn offered_refill_limit_and_partial_consumption_match_literal_bytes() {
    for length in [LIMIT - 1, LIMIT, LIMIT + 1, 2 * LIMIT + 37] {
        let chunks = drain(&format!("bytes-{length}.gz"), length, false);
        assert_eq!(chunks[0], length.min(LIMIT));
        assert_eq!(chunks.iter().sum::<usize>(), length);
        assert_eq!(chunks.len(), length.div_ceil(LIMIT));
    }
}
#[test]
fn members_footer_crc_and_truncation_cross_refill_boundaries() {
    let length = 2 * LIMIT + 37;
    drain("concat-empty.gz", length, false);
    drain("bad-crc.gz", length, true);
    drain("truncated.gz", length, true);
}
#[test]
fn cancellation_at_exhausted_chunk_does_not_refill_more_payload() {
    let reader = RapidGzipReader::open(&fixture("bytes-2097189.gz"), 1).unwrap();
    let mut error = [0 as c_char; ERROR_SIZE];
    assert_eq!(
        unsafe { soapnuke_rapidgzip_refill(reader.handle, error.as_mut_ptr(), error.len()) },
        1
    );
    let mut consumed = 0;
    loop {
        let mut data = ptr::null();
        let mut size = 0;
        let status = unsafe {
            soapnuke_rapidgzip_available(
                reader.handle,
                &mut data,
                &mut size,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if status == 2 {
            break;
        }
        assert_eq!(status, 1);
        consumed += size;
        assert_eq!(
            unsafe {
                soapnuke_rapidgzip_consume(reader.handle, size, error.as_mut_ptr(), error.len())
            },
            0
        );
    }
    assert_eq!(consumed, LIMIT);
    unsafe { soapnuke_rapidgzip_cancel(reader.handle) };
    assert_eq!(
        unsafe { soapnuke_rapidgzip_refill(reader.handle, error.as_mut_ptr(), error.len()) },
        0
    );
}
