use serde::ser::{Serialize, Serializer, SerializeMap};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputBackend {
    Zlib,
    Rapidgzip,
}

#[derive(Clone, Copy, Debug)]
pub struct DecoderChoice {
    pub backend: InputBackend,
    pub threads_per_lane: usize,
    pub reserved_credits: usize,
}

pub const RAPIDGZIP_CROSSOVER: usize = 14;

pub fn decoder_choice(
    total_threads: usize,
    path1: &str,
    path2: Option<&str>,
) -> Result<DecoderChoice, String> {
    let requested = std::env::var("SOAPNUKE_INPUT_DECODER").unwrap_or_else(|_| {
        if total_threads >= RAPIDGZIP_CROSSOVER {
            "rapidgzip".into()
        } else {
            "zlib".into()
        }
    });
    let backend = match requested.as_str() {
        "zlib" => InputBackend::Zlib,
        "rapidgzip" => InputBackend::Rapidgzip,
        _ => return Err("SOAPNUKE_INPUT_DECODER must be zlib or rapidgzip".into()),
    };
    let gzip_lanes = usize::from(path1.ends_with(".gz"))
        + usize::from(path2.is_some_and(|path| path.ends_with(".gz")));
    let threads_per_lane = if backend == InputBackend::Rapidgzip && gzip_lanes > 0 {
        std::env::var("SOAPNUKE_RAPIDGZIP_THREADS")
            .ok()
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| "SOAPNUKE_RAPIDGZIP_THREADS must be 1".to_string())
            })
            .transpose()?
            .unwrap_or(1)
    } else {
        1
    };
    if threads_per_lane != 1 {
        return Err(
            "SOAPNUKE_RAPIDGZIP_THREADS must be 1; multi-worker decoding is not qualified".into(),
        );
    }
    let reserved_credits = if threads_per_lane > 1 {
        gzip_lanes * threads_per_lane
    } else {
        0
    };
    if reserved_credits >= total_threads {
        return Err("rapidgzip helper threads exceed the CPU budget".into());
    }
    Ok(DecoderChoice {
        backend,
        threads_per_lane,
        reserved_credits,
    })
}

#[derive(Clone,Copy,Debug)]
pub(crate) struct Decoder {pub backend:&'static str,pub parallelism:usize,pub active:Option<bool>,pub rapid:bool}
#[derive(Clone,Copy,Debug)]
pub(crate) struct Snapshot {pub lanes:[Option<Decoder>;2],pub current:Option<usize>,pub scope:&'static str}
impl Snapshot {pub(crate) const fn empty(scope:&'static str)->Self{Self{lanes:[None,None],current:None,scope}}}
impl Serialize for Decoder{fn serialize<S:Serializer>(&self,s:S)->Result<S::Ok,S::Error>{let mut m=s.serialize_map(None)?;m.serialize_entry("backend",self.backend)?;m.serialize_entry("decoder_parallelism",&self.parallelism)?;if let Some(v)=self.active{m.serialize_entry("parallel_reader_active",&v)?;}if self.rapid{m.serialize_entry("proof","live native handle fields; serial GzipReader selected at parallelism1; OS thread observation is separate")?;}m.serialize_entry("native_helper_reservation",&0usize)?;m.end()}}
