use crate::failure::{SourceError, Failure, Reason};
// FASTQ source: two parallel lane readers (one per file). Lanes only inflate
// and split the stream at record boundaries into raw byte chunks; record
// parsing and pair validation are deferred to the filter workers (P4: the
// lane hot path no longer builds records, and the single-threaded producer
// no longer touches every record).
use crate::cpu_budget::{CpuBudget, Permit, Stage};
use crate::rapidgzip_reader::RapidGzipReader;
use crate::zlib_reader::ZlibReader;
use std::fs::File;
use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const LANE_CHUNK: usize = 2500;

// Plain (uncompressed) reader with the same interface as ZlibReader.
pub struct PlainReader {
    file: crate::native_io::InputReader,
    buf: Vec<u8>,
    start: usize,
    end: usize,
    eof: bool,
}

impl PlainReader {
    pub fn open(path: &str) -> io::Result<PlainReader> {
        Ok(PlainReader {
            file: crate::native_io::InputReader::File(File::open(path)?),
            buf: vec![0u8; 8 << 20],
            start: 0,
            end: 0,
            eof: false,
        })
    }
    pub(crate) fn from_reader(reader: Box<dyn Read + Send>) -> Self {
        Self { file: crate::native_io::InputReader::Owned(reader), buf: vec![0u8; 8 << 20], start: 0, end: 0, eof: false }
    }
    fn available_without_refill(&mut self) -> io::Result<&[u8]> {
        if self.start < self.end {
            return Ok(&self.buf[self.start..self.end]);
        }
        if self.eof {
            return Ok(&[]);
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "plain reader needs input",
        ))
    }

    fn refill_input(&mut self) -> io::Result<bool> {
        if self.start < self.end {
            return Ok(true);
        }
        self.start = 0;
        self.end = 0;
        let n = self.file.read(&mut self.buf)?;
        if n == 0 {
            self.eof = true;
            return Ok(false);
        }
        self.end = n;
        Ok(true)
    }
    fn consume(&mut self, n: usize) {
        self.start += n;
        if self.start >= self.end {
            self.start = 0;
            self.end = 0;
        }
    }
}

enum LaneReader {
    Gz(ZlibReader),
    Rapid(RapidGzipReader),
    Plain(PlainReader),
}

pub use crate::decoder::{InputBackend, DecoderChoice, decoder_choice, RAPIDGZIP_CROSSOVER};

impl LaneReader {
    fn open(path: &str, choice: DecoderChoice) -> io::Result<LaneReader> {
        if path.len() >= 3 && &path[path.len() - 3..] == ".gz" {
            return match choice.backend {
                InputBackend::Zlib => Ok(LaneReader::Gz(ZlibReader::open(path)?)),
                InputBackend::Rapidgzip => Ok(LaneReader::Rapid(RapidGzipReader::open(
                    path,
                    choice.threads_per_lane,
                )?)),
            };
        } else {
            Ok(LaneReader::Plain(PlainReader::open(path)?))
        }
    }
    fn open_owned(io:&crate::native_io::OwnedIo,decl:&crate::job_api::InputDecl,choice:DecoderChoice)->io::Result<LaneReader>{
        if crate::job_api::input_codec(decl)=="gzip" {
            if let crate::job_api::Location::Path(path)=&decl.location {if !io.has_override(decl)&&choice.backend==InputBackend::Rapidgzip {return Ok(LaneReader::Rapid(RapidGzipReader::open(&path.to_string_lossy(),1)?));}}
            Ok(LaneReader::Gz(ZlibReader::from_reader(io.reader(decl)?,&decl.channel)))
        } else {Ok(LaneReader::Plain(PlainReader::from_reader(io.reader(decl)?)))}
    }
    fn open_stream(gzip: bool) -> io::Result<LaneReader> {
        if gzip {
            Ok(LaneReader::Gz(ZlibReader::open("/dev/stdin")?))
        } else {
            Ok(LaneReader::Plain(PlainReader::open("/dev/stdin")?))
        }
    }
    fn available_without_refill(&mut self) -> io::Result<&[u8]> {
        match self {
            LaneReader::Gz(g) => g.available_without_refill(),
            LaneReader::Rapid(g) => g.available_without_refill(),
            LaneReader::Plain(p) => p.available_without_refill(),
        }
    }

    fn refill_input(&mut self) -> io::Result<bool> {
        match self {
            LaneReader::Gz(g) => g.refill_input(),
            LaneReader::Rapid(g) => g.refill_input(),
            LaneReader::Plain(p) => p.refill_input(),
        }
    }
    fn consume(&mut self, n: usize) -> io::Result<()> {
        match self {
            LaneReader::Gz(g) => {
                g.consume(n);
                Ok(())
            }
            LaneReader::Rapid(g) => g.consume(n),
            LaneReader::Plain(p) => {
                p.consume(n);
                Ok(())
            }
        }
    }
}

pub struct DemuxInput {pub indices:Vec<Vec<u8>>,pub error:Option<crate::failure::UnitError>}
pub struct InputBatch {
    pub demux: Option<DemuxInput>,
    pub lane_index: Option<usize>,
    pub id: u64,
    pub first_record_index: u64,
    pub record_count: usize,
    pub reads1: Vec<u8>,
    pub reads2: Option<Vec<u8>>,
}

pub struct LaneChunk {
    pub bytes: Vec<u8>,
    pub record_count: usize,
}

enum LaneMessage {
    Data(LaneChunk),
    End,
    Error(SourceError),
}

// A lane worker inflates one file and splits the decompressed stream at
// record boundaries into raw chunks of the requested patch size. Parsing and
// pair validation are deferred to the filter workers.
struct Carry {
    chunk: LaneChunk,
    offset: usize,
    remaining: usize,
}

#[derive(Clone, Debug)]
pub struct InputLane {
    pub id: String,
    pub r1: String,
    pub r2: Option<String>,
}
struct NativeLayout {
    api_io: Option<Arc<crate::native_io::OwnedIo>>,
    demux:bool,
    dedup:bool,
    demux_done:bool,
    index_readers:Vec<LaneReader>,
    index_opened:bool,
    _index_buffers:Option<crate::native_execution::FeatureTicket>,
    specs: Vec<InputLane>,
    next_spec: usize,
    readers: Option<(LaneReader, Option<LaneReader>)>,
    interleaved: bool,
    stdin_gzip: bool,
    limit: Option<u64>,
    units: u64,
    pending: Option<(Vec<u8>, Option<Vec<u8>>)>,
    group_ended: bool,
    _index_reservation: crate::native_execution::FeatureTicket,
}
impl NativeLayout {
    fn record(reader: &mut LaneReader, stop: &AtomicBool) -> Result<Option<Vec<u8>>, SourceError> {
        let mut bytes = Vec::with_capacity(512);
        let mut lines = 0;
        loop {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceError::synthetic_cancel("FASTQ source cancelled"));
            }
            let slice = match reader.available_without_refill() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    reader
                        .refill_input()
                        .map_err(|e| format!("[input] stream: {e}"))?;
                    continue;
                }
                Err(e) => return Err(format!("[input] stream: {e}").into()),
            };
            if slice.is_empty() {
                if bytes.is_empty() {
                    return Ok(None);
                }
                if lines != 3 {
                    return Err("[input] truncated FASTQ record".into());
                }
                if bytes.len() == crate::native_execution::RECORD_BYTES {
                    return Err("[input] FASTQ record exceeds framing limit".into());
                }
                bytes.reserve_exact(1);
                bytes.push(b'\n');
                return Ok(Some(bytes));
            }
            let mut end = slice.len();
            for i in memchr::memchr_iter(b'\n', slice) {
                lines += 1;
                if lines == 4 {
                    end = i + 1;
                    break;
                }
            }
            let needed = bytes
                .len()
                .checked_add(end)
                .ok_or("[input] record length overflow")?;
            if needed > crate::native_execution::RECORD_BYTES {
                return Err("[input] FASTQ record exceeds framing limit".into());
            }
            if needed > bytes.capacity() {
                bytes.reserve_exact(needed - bytes.len());
            }
            bytes.extend_from_slice(&slice[..end]);
            reader
                .consume(end)
                .map_err(|e| format!("[input] consume: {e}"))?;
            if lines == 4 {
                return Ok(Some(bytes));
            }
        }
    }
    fn open_next(&mut self, choice: DecoderChoice) -> Result<bool, SourceError> {
        if self.next_spec == self.specs.len() {
            return Ok(false);
        }
        let spec = &self.specs[self.next_spec];
        let first = if let Some(io) = &self.api_io {
            let decl=io.spec.lanes[self.next_spec].inputs.iter().find(|d|d.channel=="R1").unwrap();
            LaneReader::open_owned(io,decl,choice)
        } else if spec.r1 == "-" {
            LaneReader::open_stream(self.stdin_gzip)
        } else {
            LaneReader::open(&spec.r1, choice)
        }
        .map_err(|e| format!("[input] lane {} R1: {e}", spec.id))?;
        let second = if let Some(io) = &self.api_io {
            io.spec.lanes[self.next_spec].inputs.iter().find(|d|d.channel=="R2").map(|decl| {
                LaneReader::open_owned(io,decl,choice)
            }).transpose()
        } else { spec
            .r2
            .as_ref()
            .map(|p| LaneReader::open(p, choice))
            .transpose()
        }
            .map_err(|e:io::Error| format!("[input] lane {} R2: {e}", spec.id))?;
        self.next_spec += 1;
        self.readers = Some((first, second));
        self.group_ended = false;
        Ok(true)
    }
    fn pair(&mut self, stop: &AtomicBool) -> Result<Option<(Vec<u8>, Option<Vec<u8>>)>, SourceError> {
        if let Some(pair) = self.pending.take() {
            return Ok(Some(pair));
        }
        if self.api_io.as_ref().is_some_and(|io|io.cancel.is_cancelled()){return Err(SourceError::api_cancelled());}
        let (first, second) = self.readers.as_mut().unwrap();
        let a = Self::record(first, stop)?;
        let b = if self.interleaved {
            if a.is_none() {
                return Ok(None);
            }
            Some(Self::record(first, stop)?.ok_or("[input] odd interleaved EOF: missing mate2")?)
        } else if let Some(r2) = second {
            let b = Self::record(r2, stop)?;
            if a.is_none() != b.is_none() {
                return Err(SourceError(Failure::input("[input] paired read counts differ at EOF").because(Reason::PairCount)));
            }
            b
        } else {
            None
        };
        Ok(a.map(|a| (a, b)))
    }
}

pub struct Source {
    layout: Option<NativeLayout>,
    native: bool,
    backend_facts: serde_json::Value,
    dedup_backend:Option<crate::decoder::Snapshot>,
    carry1: Option<Carry>,
    carry2: Option<Carry>,
    lane1_rx: Receiver<LaneMessage>,
    lane2_rx: Option<Receiver<LaneMessage>>,
    done1: bool,
    done2: bool,
    batch_id: u64,
    next_record_index: u64,
    error: Option<SourceError>,
    stop: Arc<AtomicBool>,
    lane_handles: Option<(
        thread::JoinHandle<Result<(), SourceError>>,
        Option<thread::JoinHandle<Result<(), SourceError>>>,
    )>,
    budget: Arc<CpuBudget>,
    pub patch_size: usize,
    pair_count: u64,
}

fn send_lane_message(
    tx: &SyncSender<LaneMessage>,
    mut message: LaneMessage,
    stop: &AtomicBool,
) -> bool {
    loop {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        match tx.try_send(message) {
            Ok(()) => return true,
            Err(TrySendError::Full(returned)) => {
                message = returned;
                thread::sleep(Duration::from_micros(100));
            }
            Err(TrySendError::Disconnected(_)) => return false,
        }
    }
}

fn inflate_lane_loop(
    mut reader: LaneReader,
    path: &str,
    tx: SyncSender<LaneMessage>,
    label: &'static str,
    stop: Arc<AtomicBool>,
    chunk_size: usize,
    budget: Arc<CpuBudget>,
) -> Result<(), SourceError> {
    // pending carries the partial record tail (< 1 record) from the previous
    // window; completed records are appended straight from the window slice.
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk: Vec<u8> = Vec::with_capacity(chunk_size * 260);
    let mut chunk_records = 0usize;
    let mut err: Option<SourceError> = None;
    let mut eof = false;
    'outer: loop {
        if stop.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        // Keep compressed input decoding and record framing under one source
        // permit. Queue admission remains outside the permit so blocked lane
        // workers do not consume a compute credit.
        let (slice, mut source_permit): (&[u8], Option<Permit>) = loop {
            let permit = budget.acquire(Stage::Source);
            match reader.available_without_refill() {
                Ok(slice) => break (slice, Some(permit)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    drop(permit);
                    match reader.refill_input() {
                        Ok(true) => continue,
                        Ok(false) => {
                            eof = true;
                            // Gzip must see the EOF state again so it can
                            // finalize DEFLATE and validate CRC/ISIZE. Plain
                            // input returns an empty slice on the retry.
                            continue;
                        }
                        Err(error) => {
                            err = Some(format!(
                                "[input] error reading FASTQ input {}: {}",
                                path, error
                            ).into());
                            break (&[], None);
                        }
                    }
                }
                Err(error) => {
                    err = Some(format!(
                        "[input] error reading FASTQ input {}: {}",
                        path, error
                    ).into());
                    break (&[], None);
                }
            }
        };
        if slice.is_empty() {
            eof = true;
            drop(source_permit.take());
            break 'outer;
        }
        let consume_len = slice.len();
        let tail_off;
        if pending.is_empty() {
            // Process the whole window: split complete records into the chunk
            // and flush whenever it fills; only a partial trailing record is
            // left for the next window (in `pending`).
            let mut pos = 0usize;
            loop {
                if chunk_records >= chunk_size {
                    let completed = std::mem::take(&mut chunk);
                    let completed_records = chunk_records;
                    drop(source_permit.take());
                    if !send_lane_message(
                        &tx,
                        LaneMessage::Data(LaneChunk {
                            bytes: completed,
                            record_count: completed_records,
                        }),
                        &stop,
                    ) {
                        return Ok(());
                    }
                    source_permit = Some(budget.acquire(Stage::Source));
                    chunk = Vec::with_capacity(chunk_size * 260);
                    chunk_records = 0;
                }
                let (t, nrec) = split_window(&slice[pos..], &mut chunk, chunk_size - chunk_records);
                chunk_records += nrec;
                pos += t;
                if t == 0 && nrec == 0 {
                    break;
                }
                if pos >= consume_len {
                    break;
                }
            }
            tail_off = pos;
        } else {
            // Complete the pending partial record with the first bytes of this
            // window (up to its 4th newline).
            let need = 4 - count_newlines(&pending);
            let mut take = 0usize;
            let mut cnt = 0usize;
            for (i, &b) in slice.iter().enumerate() {
                if b == b'\n' {
                    cnt += 1;
                    if cnt == need {
                        take = i + 1;
                        break;
                    }
                }
            }
            if take == 0 {
                pending.extend_from_slice(slice);
                if let Err(error) = reader.consume(consume_len) {
                    err = Some(format!(
                        "[input] error reading FASTQ input {}: {}",
                        path, error
                    ).into());
                    drop(source_permit.take());
                    break 'outer;
                }
                drop(source_permit.take());
                continue;
            }
            pending.extend_from_slice(&slice[..take]);
            chunk.extend_from_slice(&pending);
            chunk_records += 1;
            pending.clear();
            let mut pos = take;
            loop {
                if chunk_records >= chunk_size {
                    let completed = std::mem::take(&mut chunk);
                    let completed_records = chunk_records;
                    drop(source_permit.take());
                    if !send_lane_message(
                        &tx,
                        LaneMessage::Data(LaneChunk {
                            bytes: completed,
                            record_count: completed_records,
                        }),
                        &stop,
                    ) {
                        return Ok(());
                    }
                    source_permit = Some(budget.acquire(Stage::Source));
                    chunk = Vec::with_capacity(chunk_size * 260);
                    chunk_records = 0;
                }
                let (t, nrec) = split_window(&slice[pos..], &mut chunk, chunk_size - chunk_records);
                chunk_records += nrec;
                pos += t;
                if t == 0 && nrec == 0 {
                    break;
                }
                if pos >= consume_len {
                    break;
                }
            }
            tail_off = pos;
        }
        if tail_off < consume_len {
            pending.extend_from_slice(&slice[tail_off..]);
        }
        if let Err(error) = reader.consume(consume_len) {
            err = Some(format!(
                "[input] error reading FASTQ input {}: {}",
                path, error
            ).into());
            drop(source_permit.take());
            break 'outer;
        }
        drop(source_permit.take());
    }
    if err.is_none() && eof && !pending.is_empty() {
        // A FASTQ file may end immediately after the final quality line. The
        // framing stream normally uses newlines as record delimiters, so add
        // one internal delimiter only when the tail has exactly the three
        // separators preceding a non-empty fourth line. The worker parser
        // still validates the sequence/quality lengths; genuinely partial
        // tails therefore remain errors.
        if count_newlines(&pending) == 3 && pending.last() != Some(&b'\n') {
            chunk.extend_from_slice(&pending);
            chunk.push(b'\n');
            chunk_records += 1;
            pending.clear();
        } else {
            err = Some(format!("[input] truncated FASTQ record in {} input", label).into());
        }
    }
    if chunk_records > 0 {
        let completed = std::mem::take(&mut chunk);
        let completed_records = chunk_records;
        if !send_lane_message(
            &tx,
            LaneMessage::Data(LaneChunk {
                bytes: completed,
                record_count: completed_records,
            }),
            &stop,
        ) {
            return Ok(());
        }
    }
    match err {
        Some(e) => {
            let _ = send_lane_message(&tx, LaneMessage::Error(e.clone()), &stop);
            Err(e)
        }
        None => {
            let _ = send_lane_message(&tx, LaneMessage::End, &stop);
            Ok(())
        }
    }
}

fn record_end(bytes: &[u8], start: usize) -> Result<usize, SourceError> {
    memchr::memchr_iter(b'\n', &bytes[start..])
        .nth(3)
        .map(|offset| start + offset + 1)
        .ok_or_else(|| "[input] native framing lost a complete record".into())
}

fn inflate_native_lane(
    mut reader: LaneReader,
    path: &str,
    tx: SyncSender<LaneMessage>,
    stop: Arc<AtomicBool>,
    chunk_units: usize,
    lane_target_bytes: usize,
    budget: Arc<CpuBudget>,
) -> Result<(), SourceError> {
    use crate::native_execution::{LANE_BYTES, RECORD_BYTES};
    let result: Result<(), SourceError> = (|| {
        let mut pending = Vec::with_capacity(RECORD_BYTES);
        let mut chunk = Vec::with_capacity(LANE_BYTES);
        let mut records = 0;
        let mut lines = 0;
        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            let mut permit = Some(budget.acquire_cancellable(Stage::Source, &stop)?);
            let slice = match reader.available_without_refill() {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    reader
                        .refill_input()
                        .map_err(|e| format!("[input] {path}: {e}"))?;
                    drop(permit);
                    continue;
                }
                Err(e) => return Err(format!("[input] {path}: {e}").into()),
            };
            let length = slice.len();
            if length == 0 {
                drop(permit);
                if !pending.is_empty() {
                    if lines != 3 {
                        return Err(format!("[input] truncated FASTQ record in {path}").into());
                    }
                    if pending.len() >= RECORD_BYTES {
                        return Err(format!(
                            "[input] FASTQ record exceeds {RECORD_BYTES} bytes in {path}"
                        ).into());
                    }
                    pending.push(b'\n');
                    if records > 0
                        && (records == chunk_units
                            || chunk.len() + pending.len() > lane_target_bytes)
                    {
                        let bytes = std::mem::take(&mut chunk);
                        if !send_lane_message(
                            &tx,
                            LaneMessage::Data(LaneChunk {
                                bytes,
                                record_count: records,
                            }),
                            &stop,
                        ) {
                            return Ok(());
                        }
                        chunk = Vec::with_capacity(LANE_BYTES);
                        records = 0;
                    }
                    chunk.extend_from_slice(&pending);
                    records += 1;
                }
                if records > 0 {
                    let _ = send_lane_message(
                        &tx,
                        LaneMessage::Data(LaneChunk {
                            bytes: chunk,
                            record_count: records,
                        }),
                        &stop,
                    );
                }
                let _ = send_lane_message(&tx, LaneMessage::End, &stop);
                return Ok(());
            }
            let mut begin = 0;
            for i in memchr::memchr_iter(b'\n', slice) {
                lines += 1;
                if lines != 4 {
                    continue;
                }
                let part = &slice[begin..i + 1];
                if pending.len() + part.len() > RECORD_BYTES {
                    return Err(format!(
                        "[input] FASTQ record exceeds {RECORD_BYTES} bytes in {path}"
                    ).into());
                }
                let record_bytes = pending.len() + part.len();
                if records > 0
                    && (records == chunk_units || chunk.len() + record_bytes > lane_target_bytes)
                {
                    drop(permit.take());
                    let bytes = std::mem::take(&mut chunk);
                    if !send_lane_message(
                        &tx,
                        LaneMessage::Data(LaneChunk {
                            bytes,
                            record_count: records,
                        }),
                        &stop,
                    ) {
                        return Ok(());
                    }
                    chunk = Vec::with_capacity(LANE_BYTES);
                    records = 0;
                    permit = Some(budget.acquire_cancellable(Stage::Source, &stop)?);
                }
                chunk.extend_from_slice(&pending);
                chunk.extend_from_slice(part);
                pending.clear();
                records += 1;
                lines = 0;
                begin = i + 1;
            }
            let tail = &slice[begin..];
            if pending.len() + tail.len() > RECORD_BYTES {
                return Err(format!(
                    "[input] FASTQ record exceeds {RECORD_BYTES} bytes in {path}"
                ).into());
            }
            pending.extend_from_slice(tail);
            reader
                .consume(length)
                .map_err(|e| format!("[input] {path}: {e}"))?;
            drop(permit);
        }
    })();
    if let Err(e) = &result {
        let _ = send_lane_message(&tx, LaneMessage::Error(e.clone()), &stop);
    }
    result
}

// Count newlines in a (small) buffer.
fn count_newlines(b: &[u8]) -> usize {
    let mut c = 0usize;
    for &x in b {
        if x == b'\n' {
            c += 1;
        }
    }
    c
}

// Split `slice` at complete FASTQ record boundaries, appending the bytes of
// up to `max_records` records to `chunk`. Returns (offset after the last
// complete record, records appended).
fn split_window(slice: &[u8], chunk: &mut Vec<u8>, max_records: usize) -> (usize, usize) {
    let mut pos = 0usize;
    let mut added = 0usize;
    loop {
        if added >= max_records {
            return (pos, added);
        }
        let start = pos;
        let mut found = 0usize;
        let mut nl = [0usize; 4];
        let mut i = start;
        while i < slice.len() {
            if slice[i] == b'\n' {
                nl[found] = i;
                found += 1;
                if found == 4 {
                    break;
                }
            }
            i += 1;
        }
        if found < 4 {
            return (start, added);
        }
        chunk.extend_from_slice(&slice[start..nl[3] + 1]);
        added += 1;
        pos = nl[3] + 1;
        if pos >= slice.len() {
            return (pos, added);
        }
    }
}

impl Source {
    pub fn open_layout(
        gp: &crate::params::GlobalParams,
        stop: Arc<AtomicBool>,
        budget: Arc<CpuBudget>,
        execution: &Arc<crate::native_execution::Execution>,
    ) -> Result<Source, SourceError> {
        let count = gp.native_io.as_ref().map(|io|io.spec.lanes.len()).unwrap_or_else(|| gp
            .native_lane_manifest
            .as_ref()
            .map_or(1, |s| s.lines().skip(1).count()));
        let text_bytes = gp.native_io.as_ref().map(|io|io.spec.lanes.iter().map(|l|l.id.len()+l.inputs.iter().map(|d|match &d.location{crate::job_api::Location::Path(p)=>p.as_os_str().len(),crate::job_api::Location::Handle(h)=>h.len()}).sum::<usize>()).sum()).unwrap_or_else(|| gp
            .native_lane_manifest
            .as_ref()
            .map_or(gp.fq1_path.len() + gp.fq2_path.len(), |s| s.len()));
        let header = gp
            .native_lane_parent
            .len()
            .checked_mul(4)
            .and_then(|n| n.checked_add(std::mem::size_of::<InputLane>() + 128))
            .ok_or_else(||SourceError::worker("[worker] lane index capacity overflow"))?;
        let index_bound = count
            .checked_mul(header)
            .and_then(|n| text_bytes.checked_mul(8).and_then(|m| n.checked_add(m)))
            .ok_or_else(||SourceError::worker("[worker] lane index capacity overflow"))?;
        let reservation = execution.reserve_persistent_feature(index_bound)?;
        let mut specs = Vec::with_capacity(count);
        if let Some(io)=&gp.native_io {
            for lane in &io.spec.lanes {
                let label=|d:&crate::job_api::InputDecl| match &d.location {crate::job_api::Location::Path(p)=>p.to_string_lossy().into_owned(),crate::job_api::Location::Handle(h)=>format!("handle:{h}")};
                specs.push(InputLane{id:lane.id.clone(),r1:label(&lane.inputs[0]),r2:lane.inputs.iter().find(|d|d.channel=="R2").map(label)});
            }
        } else if let Some(text) = &gp.native_lane_manifest {
            for line in text.lines().skip(1) {
                let mut f = line.split('\t');
                let id = f.next().unwrap().to_string();
                let a = f.next().unwrap();
                let b = f.next().unwrap();
                let path = |s: &str| {
                    std::path::Path::new(&gp.native_lane_parent)
                        .join(s)
                        .to_string_lossy()
                        .into_owned()
                };
                specs.push(InputLane {
                    id,
                    r1: path(a),
                    r2: if b.is_empty() { None } else { Some(path(b)) },
                });
            }
        } else {
            specs.push(InputLane {
                id: "input".into(),
                r1: gp.fq1_path.clone(),
                r2: if gp.fq2_path.is_empty() {
                    None
                } else {
                    Some(gp.fq2_path.clone())
                },
            });
        }
        let actual = specs.capacity() * std::mem::size_of::<InputLane>()
            + specs
                .iter()
                .map(|s| {
                    s.id.capacity() + s.r1.capacity() + s.r2.as_ref().map_or(0, String::capacity)
                })
                .sum::<usize>();
        if actual > index_bound {
            return Err(SourceError::worker("[worker] lane index exceeded reserved capacity"));
        }
        let (tx, rx) = sync_channel(1);
        drop(tx);
        Ok(Source {
            native: true,
            backend_facts: if gp.native_dedup.is_some(){serde_json::Value::Null}else{serde_json::json!({"lanes":[],"scope":"no input reader opened yet"})},
            dedup_backend:gp.native_dedup.as_ref().map(|_|crate::decoder::Snapshot::empty("no input reader opened yet")),
            carry1: None,
            carry2: None,
            lane1_rx: rx,
            lane2_rx: None,
            done1: false,
            done2: false,
            batch_id: 0,
            next_record_index: 0,
            error: None,
            stop,
            lane_handles: None,
            budget,
            patch_size: gp.patch_size.min(crate::native_execution::CHUNK_UNITS),
            pair_count: 0,
            layout: Some(NativeLayout {
                api_io: gp.native_io.clone(),
                dedup:gp.native_dedup.is_some(),demux:gp.native_demux.is_some()||gp.native_dedup.is_some(),demux_done:false,index_readers:Vec::new(),index_opened:false,
                _index_buffers:if gp.native_demux.is_some(){let n=gp.native_io.as_ref().unwrap().spec.lanes[0].inputs.iter().filter(|d|d.channel=="I1"||d.channel=="I2").count();Some(execution.reserve_persistent_feature(n.checked_mul(crate::native_execution::PREFETCH_PER_LANE).ok_or_else(||SourceError::worker("[worker] index reservation overflow"))?)?)}else{None},
                specs,
                next_spec: 0,
                readers: None,
                interleaved: gp.native_interleaved,
                stdin_gzip: gp.native_stdin_gzip.unwrap_or(false),
                limit: gp.native_limit_units,
                units: 0,
                pending: None,
                group_ended: false,
                _index_reservation: reservation,
            }),
        })
    }
    fn next_layout_batch(
        &mut self,
        layout: &mut NativeLayout,
    ) -> Result<Option<InputBatch>, SourceError> {
        if layout.demux_done{return Ok(None);}
        if layout.limit.is_some_and(|n| layout.units >= n) {
            layout.readers = None;
            return Ok(None);
        }
        let mut a = Vec::with_capacity(crate::native_execution::LANE_BYTES);
        let paired = layout.interleaved || layout.specs.first().is_some_and(|s| s.r2.is_some());
        let mut b = if paired {
            Some(Vec::with_capacity(crate::native_execution::LANE_BYTES))
        } else {
            None
        };
        let mut count = 0;
        let _permit = self.budget.acquire_cancellable(Stage::Source, &self.stop)?;
        let index_decls=if layout.demux {layout.api_io.as_ref().unwrap().spec.lanes[0].inputs.iter().filter(|d|d.channel=="I1"||d.channel=="I2").cloned().collect::<Vec<_>>()}else{Vec::new()};
        if !layout.index_opened{
            for d in &index_decls{let io=layout.api_io.as_ref().unwrap();layout.index_readers.push(LaneReader::open_owned(io,d,io.choice(self.budget.limit())).map_err(|e|format!("[input] index: {e}"))?);}layout.index_opened=true;
        }
        let mut demux=if layout.demux{Some(DemuxInput{indices:index_decls.iter().map(|_|Vec::with_capacity(crate::native_execution::LANE_BYTES)).collect(),error:None})}else{None};
        loop {
            if layout
                .limit
                .is_some_and(|n| layout.units + count as u64 >= n)
            {
                break;
            }
            if layout.group_ended {
                layout.readers = None;
                if count > 0 {
                    break;
                }
            }
            if layout.readers.is_none() {
                let choice = if let Some(io)=&layout.api_io{io.choice(self.budget.limit())}else{decoder_choice(
                    self.budget.limit(),
                    &layout
                        .specs
                        .get(layout.next_spec)
                        .map_or("", |s| s.r1.as_str()),
                    layout
                        .specs
                        .get(layout.next_spec)
                        .and_then(|s| s.r2.as_deref()),
                ).map_err(|e|SourceError::from(crate::failure::Failure::write(e)))?
                };
                if !layout.open_next(choice)? {
                    break;
                }
                if layout.dedup {
                    let fact=|reader:&LaneReader|match reader{LaneReader::Plain(_)=>crate::decoder::Decoder{backend:"plain",parallelism:0,active:None,rapid:false},LaneReader::Gz(_)=>crate::decoder::Decoder{backend:"zlib",parallelism:1,active:Some(false),rapid:false},LaneReader::Rapid(r)=>r.dedup_backend()};let(r1,r2)=layout.readers.as_ref().unwrap();self.dedup_backend=Some(crate::decoder::Snapshot{lanes:[Some(fact(r1)),r2.as_ref().map(fact)],current:Some(layout.next_spec-1),scope:""});
                }else{
                let fact = |reader: &LaneReader| match reader {
                    LaneReader::Plain(_) => {
                        serde_json::json!({"backend":"plain","decoder_parallelism":0,"native_helper_reservation":0})
                    }
                    LaneReader::Gz(_) => {
                        serde_json::json!({"backend":"zlib","decoder_parallelism":1,"parallel_reader_active":false,"native_helper_reservation":0})
                    }
                    LaneReader::Rapid(r) => r.backend_facts(),
                };
                let (r1, r2) = layout.readers.as_ref().unwrap();
                let mut facts = vec![fact(r1)];
                if let Some(r2) = r2 {
                    facts.push(fact(r2));
                }
                self.backend_facts = serde_json::json!({"lanes":facts,"current_lane_id":layout.specs[layout.next_spec-1].id,"opened_lane_groups":layout.next_spec,"layout_source":"bounded existing readers on producer; decode/framing under source CPU credit"});
                }
            }
            let pair=match layout.pair(&self.stop){Ok(v)=>v,Err(e)if layout.demux=>{demux.as_mut().unwrap().error=Some(crate::failure::UnitError{ordinal:self.next_record_index+count as u64,stage:"input",message:e.to_string(),code:3});layout.demux_done=true;break;},Err(e)=>return Err(e)};
            let Some((r1,r2))=pair else{layout.group_ended=true;continue;};
            let bytes = a.len()
                + b.as_ref().map_or(0, Vec::len)
                + r1.len()
                + r2.as_ref().map_or(0, Vec::len);
            if count > 0
                && (bytes > crate::native_execution::TARGET_BYTES || count == self.patch_size)
            {
                layout.pending = Some((r1, r2));
                break;
            }
            if bytes > crate::native_execution::ASSEMBLY_BYTES {
                return Err("[input] layout pair exceeds assembly bound".into());
            }
            if let Some(d)=&mut demux{
                for (i,reader) in layout.index_readers.iter_mut().enumerate(){
                    match NativeLayout::record(reader,&self.stop){Ok(Some(raw))=>{let needed=d.indices[i].len().checked_add(raw.len()).ok_or_else(||SourceError::worker("[worker] index buffer overflow"))?;if needed>d.indices[i].capacity(){let additional=needed-d.indices[i].len();d.indices[i].reserve_exact(additional);}d.indices[i].extend_from_slice(&raw)},Ok(None)=>{d.error=Some(crate::failure::UnitError{ordinal:self.next_record_index+count as u64,stage:"index-count",message:"[input] index association mismatch: index-count missing index record".into(),code:3});break;},Err(e)=>{d.error=Some(crate::failure::UnitError{ordinal:self.next_record_index+count as u64,stage:"index-association",message:format!("[input] index-association: {e}"),code:3});break;}}
                }
            }
            a.extend_from_slice(&r1);
            if let (Some(out), Some(r2)) = (&mut b, r2) {
                out.extend_from_slice(&r2);
            }
            count += 1;
            if demux.as_ref().is_some_and(|d|d.error.is_some()){layout.demux_done=true;break;}
            if demux.as_ref().is_some_and(|d|d.indices.iter().any(|b|b.len()>=crate::native_execution::TARGET_BYTES)){break;}
            if count == self.patch_size {
                break;
            }
        }
        if count==0 && layout.next_spec==layout.specs.len() && layout.group_ended && layout.demux && !layout.demux_done{
            for reader in &mut layout.index_readers{match NativeLayout::record(reader,&self.stop){Ok(None)=>{},Ok(Some(_))=>{demux.as_mut().unwrap().error=Some(crate::failure::UnitError{ordinal:self.next_record_index,stage:"index-count",message:"[input] index association mismatch: index-count extra index record".into(),code:3});break;},Err(e)=>{demux.as_mut().unwrap().error=Some(crate::failure::UnitError{ordinal:self.next_record_index,stage:"input",message:e.to_string(),code:3});break;}}}layout.demux_done=true;
        }
        if count==0 && demux.as_ref().is_none_or(|d|d.error.is_none()){return Ok(None);}
        assert!(
            a.capacity() <= 2 * crate::native_execution::LANE_BYTES
                && b.as_ref()
                    .is_none_or(|b| b.capacity() <= 2 * crate::native_execution::LANE_BYTES)
        );
        let result = InputBatch {
            demux,
            lane_index: Some(layout.next_spec - 1),
            id: self.batch_id,
            first_record_index: self.next_record_index,
            record_count: count,
            reads1: a,
            reads2: b,
        };
        self.batch_id += 1;
        self.next_record_index = self
            .next_record_index
            .checked_add(count as u64)
            .ok_or("[input] source ordinal overflow")?;
        self.pair_count += count as u64;
        layout.units += count as u64;
        Ok(Some(result))
    }

    #[cfg(test)]
    pub fn open(
        path1: &str,
        path2: &str,
        patch_size: usize,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Source, SourceError> {
        Self::open_with_budget(path1, path2, patch_size, stop, CpuBudget::new(usize::MAX))
    }

    #[cfg(test)]
    pub fn open_single(
        path1: &str,
        patch_size: usize,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Source, SourceError> {
        Self::open_single_with_budget(path1, patch_size, stop, CpuBudget::new(usize::MAX))
    }

    pub fn open_with_budget(
        path1: &str,
        path2: &str,
        patch_size: usize,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        budget: Arc<CpuBudget>,
    ) -> Result<Source, SourceError> {
        Self::open_internal(path1, Some(path2), patch_size, stop, budget, false)
    }

    pub fn open_single_with_budget(
        path1: &str,
        patch_size: usize,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        budget: Arc<CpuBudget>,
    ) -> Result<Source, SourceError> {
        Self::open_internal(path1, None, patch_size, stop, budget, false)
    }

    pub fn open_native(
        path1: &str,
        path2: Option<&str>,
        patch_size: usize,
        stop: Arc<AtomicBool>,
        budget: Arc<CpuBudget>,
    ) -> Result<Source, SourceError> {
        Self::open_internal(
            path1,
            path2,
            patch_size.min(crate::native_execution::CHUNK_UNITS),
            stop,
            budget,
            true,
        )
    }

    pub fn open_native_stream(
        gzip: bool,
        patch_size: usize,
        stop: Arc<AtomicBool>,
        budget: Arc<CpuBudget>,
    ) -> Result<Source, SourceError> {
        Self::open_internal_mode(
            "-",
            None,
            patch_size.min(crate::native_execution::CHUNK_UNITS),
            stop,
            budget,
            true,
            Some(gzip),
        )
    }
    fn open_internal(
        path1: &str,
        path2: Option<&str>,
        patch_size: usize,
        stop: Arc<AtomicBool>,
        budget: Arc<CpuBudget>,
        native: bool,
    ) -> Result<Source, SourceError> {
        Self::open_internal_mode(path1, path2, patch_size, stop, budget, native, None)
    }
    fn open_internal_mode(
        path1: &str,
        path2: Option<&str>,
        patch_size: usize,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        budget: Arc<CpuBudget>,
        native: bool,
        stdin_gzip: Option<bool>,
    ) -> Result<Source, SourceError> {
        if patch_size == 0 {
            return Err("patch size must be greater than zero".into());
        }
        // Open the physical readers before starting any lane or worker. File
        // open and gzip-header setup are initialization/I/O, not parallel
        // compute, so this keeps reader construction outside the shared
        // permit budget without allowing it to race worker initialization.
        let choice = decoder_choice(budget.limit(), path1, path2).map_err(|e|SourceError::from(crate::failure::Failure::write(e)))?;
        let reader1 = match stdin_gzip {
            Some(gzip) => LaneReader::open_stream(gzip),
            None => LaneReader::open(path1, choice),
        }
        .map_err(|error| format!("[input] cannot open read1 input {}: {}", path1, error))?;
        let reader2 = path2
            .map(|path| {
                LaneReader::open(path, choice)
                    .map(|reader| (reader, path.to_string()))
                    .map_err(|error| format!("[input] cannot open read2 input {}: {}", path, error))
            })
            .transpose()?;
        let fact = |reader: &LaneReader| match reader {
            LaneReader::Plain(_) => {
                serde_json::json!({"backend":"plain","decoder_parallelism":0,"native_helper_reservation":0})
            }
            LaneReader::Gz(_) => {
                serde_json::json!({"backend":"zlib","decoder_parallelism":1,"parallel_reader_active":false,"native_helper_reservation":0,"proof":"synchronous zlib inflate in owning source lane"})
            }
            LaneReader::Rapid(r) => r.backend_facts(),
        };
        let mut lanes = vec![fact(&reader1)];
        if let Some((r, _)) = &reader2 {
            lanes.push(fact(r));
        }
        let backend_facts = serde_json::json!({"lanes":lanes,"automatic_crossover":RAPIDGZIP_CROSSOVER,"cpu_scope":"source decode/framing under application CPU permit; synchronous calls may include internal local file I/O; no qualified parallel native helper mode"});
        let lane_target_bytes = if reader2.is_some() {
            crate::native_execution::LANE_TARGET
        } else {
            crate::native_execution::TARGET_BYTES
        };
        let (tx1, rx1) = sync_channel::<LaneMessage>(if native { 1 } else { 4 });
        let p1 = path1.to_string();
        let s1 = stop.clone();
        let b1 = budget.clone();
        #[cfg(test)]
        let fail_lane = std::env::var("SOAPNUKE_TEST_FAIL_LANE_SPAWN").ok();
        #[cfg(not(test))]
        let fail_lane: Option<String> = None;
        let h1 = if fail_lane.as_deref() == Some("1") {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "injected lane1 resource exhaustion",
            ))
        } else {
            thread::Builder::new().name("lane1".into()).spawn(move || {
                if native {
                    inflate_native_lane(reader1, &p1, tx1, s1, patch_size, lane_target_bytes, b1)
                } else {
                    inflate_lane_loop(reader1, &p1, tx1, "read1", s1, patch_size, b1)
                }
            })
        }
        .map_err(|e| SourceError::worker(format!("[worker] lane1 spawn: {e}")))?;
        let (lane2_rx, lane2_handle) = if let Some((reader2, p2)) = reader2 {
            let (tx2, rx2) = sync_channel::<LaneMessage>(if native { 1 } else { 4 });
            let s2 = stop.clone();
            let b2 = budget.clone();
            let lane2_result = if fail_lane.as_deref() == Some("2") {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "injected lane2 resource exhaustion",
                ))
            } else {
                thread::Builder::new().name("lane2".into()).spawn(move || {
                    if native {
                        inflate_native_lane(
                            reader2,
                            &p2,
                            tx2,
                            s2,
                            patch_size,
                            lane_target_bytes,
                            b2,
                        )
                    } else {
                        inflate_lane_loop(reader2, &p2, tx2, "read2", s2, patch_size, b2)
                    }
                })
            };
            let h2 = match lane2_result {
                Ok(handle) => handle,
                Err(error) => {
                    stop.store(true, Ordering::Relaxed);
                    let _ = h1.join();
                    return Err(SourceError::worker(format!("[worker] lane2 spawn: {error}")));
                }
            };
            (Some(rx2), Some(h2))
        } else {
            (None, None)
        };
        Ok(Source {
            layout: None,
            native,
            backend_facts,
            dedup_backend:None,
            carry1: None,
            carry2: None,
            lane1_rx: rx1,
            lane2_rx,
            done1: false,
            done2: false,
            batch_id: 0,
            next_record_index: 0,
            error: None,
            stop,
            lane_handles: Some((h1, lane2_handle)),
            budget,
            patch_size,
            pair_count: 0,
        })
    }

    fn join_lanes(&mut self) -> Result<(), SourceError> {
        let handles = match self.lane_handles.take() {
            Some(handles) => handles,
            None => return Ok(()),
        };
        let first = handles
            .0
            .join()
            .map_err(|_| SourceError::worker("read1 lane thread panicked"));
        let second = handles.1.map(|handle| {
            handle
                .join()
                .map_err(|_| SourceError::worker("read2 lane thread panicked"))
        });
        let mut error = None;
        for result in [Some(first), second] {
            let Some(result) = result else { continue };
            match result {
                Ok(Ok(())) => {}
                Ok(Err(message)) => {
                    if error.is_none() {
                        error = Some(message);
                    }
                }
                Err(message) => {
                    if error.is_none() {
                        error = Some(message);
                    }
                }
            }
        }
        match error {
            Some(message) => Err(message),
            None => Ok(()),
        }
    }

    fn fail<T>(&mut self, message: SourceError) -> Result<T, SourceError> {
        self.error = Some(message.clone());
        self.stop.store(true, Ordering::Relaxed);
        self.budget.wake_cancelled();
        if let Err(join_error) = self.join_lanes() {
            let combined=format!("{}; {}", message, join_error);
            self.error = Some(message.with_message(combined));
        }
        Err(self.error.clone().unwrap())
    }

    // Load the next chunk from a lane; Ok(None) is an explicit clean end.
    fn next_chunk(
        rx: &Receiver<LaneMessage>,
        done: &mut bool,
        stop: &AtomicBool,
    ) -> Result<Option<LaneChunk>, SourceError> {
        if *done {
            return Ok(None);
        }
        loop {
            if stop.load(Ordering::Relaxed) {
                return Err(SourceError::synthetic_cancel("FASTQ source cancelled"));
            }
            match rx.recv_timeout(Duration::from_millis(10)) {
                Ok(LaneMessage::Data(chunk)) => return Ok(Some(chunk)),
                Ok(LaneMessage::End) => {
                    *done = true;
                    return Ok(None);
                }
                Ok(LaneMessage::Error(error)) => {
                    *done = true;
                    return Err(error);
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    *done = true;
                    // A lane exits without a terminal message when the
                    // shared cancellation flag is raised.  That is an
                    // expected cancellation path, not a new source error;
                    // reporting it would hide the worker/sink failure that
                    // caused cancellation.
                    if stop.load(Ordering::Relaxed) {
                        return Err(SourceError::synthetic_cancel("FASTQ source cancelled"));
                    }
                    return Err("FASTQ lane terminated without an End or Error message".into());
                }
            }
        }
    }

    // Get the next batch. Returns Ok(None) when both lanes are exhausted.
    pub(crate) fn exchange_native_patch_size(&mut self, units: usize) -> usize {
        assert!(self.native && units > 0 && units <= crate::native_execution::CHUNK_UNITS);
        std::mem::replace(&mut self.patch_size, units)
    }
    pub fn next_batch(&mut self) -> Result<Option<InputBatch>, SourceError> {
        if let Some(mut layout) = self.layout.take() {
            let result = self.next_layout_batch(&mut layout);
            self.layout = Some(layout);
            return result;
        }
        if self.native {
            return self.next_native_batch();
        }
        if let Some(e) = self.error.clone() {
            return Err(e);
        }
        let mut batch = InputBatch {
            demux:None,
            lane_index: None,
            id: self.batch_id,
            first_record_index: self.next_record_index,
            record_count: 0,
            reads1: Vec::new(),
            reads2: self.lane2_rx.as_ref().map(|_| Vec::new()),
        };
        let mut any = false;
        while batch.record_count < self.patch_size {
            let more1 = match Self::next_chunk(&self.lane1_rx, &mut self.done1, &self.stop) {
                Ok(chunk) => chunk,
                Err(error) => return self.fail(error),
            };
            let more2 = match self.lane2_rx.as_ref() {
                Some(rx) => match Self::next_chunk(rx, &mut self.done2, &self.stop) {
                    Ok(chunk) => chunk,
                    Err(error) => return self.fail(error),
                },
                None => None,
            };
            match (more1, more2) {
                (None, None) => break,
                (Some(_), None) if self.lane2_rx.is_some() => {
                    return self
                        .fail(SourceError(Failure::input("the number of reads in fastq1 and fastq2 is not equal").because(Reason::PairCount)));
                }
                (None, Some(_)) => {
                    return self
                        .fail(SourceError(Failure::input("the number of reads in fastq1 and fastq2 is not equal").because(Reason::PairCount)));
                }
                (Some(c1), None) => {
                    let n = c1.record_count;
                    let _permit = self.budget.acquire(Stage::Producer);
                    batch.reads1.extend_from_slice(&c1.bytes);
                    batch.record_count += n;
                    self.pair_count += n as u64;
                    any = true;
                }
                (Some(c1), Some(c2)) => {
                    if c1.record_count != c2.record_count {
                        return self.fail(
                            SourceError(Failure::input("the number of reads in fastq1 and fastq2 is not equal").because(Reason::PairCount)),
                        );
                    }
                    let n = c1.record_count;
                    {
                        let _permit = self.budget.acquire(Stage::Producer);
                        batch.reads1.extend_from_slice(&c1.bytes);
                        batch
                            .reads2
                            .as_mut()
                            .expect("paired source has read2 storage")
                            .extend_from_slice(&c2.bytes);
                        batch.record_count += n;
                        self.pair_count += n as u64;
                    }
                    any = true;
                }
            }
        }
        self.next_record_index += batch.record_count as u64;
        self.batch_id += 1;
        if !any && batch.record_count == 0 {
            self.join_lanes()?;
            return Ok(None);
        }
        Ok(Some(batch))
    }

    pub(crate) fn dedup_backend_facts(&self)->crate::decoder::Snapshot{self.dedup_backend.expect("Dedup source metadata initialized")}
    pub fn backend_facts(&self) -> serde_json::Value {
        self.backend_facts.clone()
    }

    fn next_native_batch(&mut self) -> Result<Option<InputBatch>, SourceError> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        if self.carry1.is_none() {
            self.carry1 = match Self::next_chunk(&self.lane1_rx, &mut self.done1, &self.stop) {
                Ok(c) => c.map(|chunk| Carry {
                    remaining: chunk.record_count,
                    chunk,
                    offset: 0,
                }),
                Err(e) => return self.fail(e),
            };
        }
        if let Some(rx) = &self.lane2_rx {
            if self.carry2.is_none() {
                self.carry2 = match Self::next_chunk(rx, &mut self.done2, &self.stop) {
                    Ok(c) => c.map(|chunk| Carry {
                        remaining: chunk.record_count,
                        chunk,
                        offset: 0,
                    }),
                    Err(e) => return self.fail(e),
                };
            }
            if self.carry1.is_none() != self.carry2.is_none() {
                return self.fail(crate::failure::Failure::input("the number of reads in fastq1 and fastq2 is not equal").because(crate::failure::Reason::PairCount).into());
            }
        }
        let Some(c1) = &self.carry1 else {
            self.join_lanes()?;
            return Ok(None);
        };
        let _permit = self
            .budget
            .acquire_cancellable(Stage::Producer, &self.stop)?;
        let max = c1
            .remaining
            .min(self.carry2.as_ref().map_or(usize::MAX, |c| c.remaining))
            .min(self.patch_size);
        // Carried vectors keep their prefetch slots until admission. Their excess
        // capacity is then charged to the complete-lifetime execution ticket.
        let intact = |c: &Carry| {
            c.offset == 0
                && c.remaining == c.chunk.record_count
                && c.chunk.bytes.capacity() <= crate::native_execution::LANE_BYTES
        };
        let raw = c1
            .chunk
            .bytes
            .len()
            .checked_add(self.carry2.as_ref().map_or(0, |c| c.chunk.bytes.len()));
        if max > 0
            && max == c1.remaining
            && intact(c1)
            && self
                .carry2
                .as_ref()
                .is_none_or(|c| c.remaining == max && intact(c))
            && raw.is_some_and(|bytes| {
                bytes <= crate::native_execution::TARGET_BYTES
                    || (max == 1 && bytes <= crate::native_execution::ASSEMBLY_BYTES)
            })
        {
            let batch = InputBatch {
            demux:None,
                lane_index: None,
                id: self.batch_id,
                first_record_index: self.next_record_index,
                record_count: max,
                reads1: self.carry1.take().unwrap().chunk.bytes,
                reads2: self.carry2.take().map(|c| c.chunk.bytes),
            };
            self.batch_id += 1;
            self.next_record_index += max as u64;
            self.pair_count += max as u64;
            return Ok(Some(batch));
        }
        let mut end1 = c1.offset;
        let mut end2 = self.carry2.as_ref().map_or(0, |c| c.offset);
        let mut count = 0;
        for _ in 0..max {
            let n1 = record_end(&c1.chunk.bytes, end1)?;
            let n2 = match &self.carry2 {
                Some(c) => record_end(&c.chunk.bytes, end2)?,
                None => 0,
            };
            let total = n1 - c1.offset + self.carry2.as_ref().map_or(0, |c| n2 - c.offset);
            if count > 0 && total > crate::native_execution::TARGET_BYTES {
                break;
            }
            end1 = n1;
            end2 = n2;
            count += 1;
        }
        let reads1 = c1.chunk.bytes[c1.offset..end1].to_vec();
        let reads2 = self
            .carry2
            .as_ref()
            .map(|c| c.chunk.bytes[c.offset..end2].to_vec());
        let advance = |carry: &mut Option<Carry>, end| {
            let c = carry.as_mut().unwrap();
            c.offset = end;
            c.remaining -= count;
            if c.remaining == 0 {
                *carry = None;
            }
        };
        advance(&mut self.carry1, end1);
        if self.carry2.is_some() {
            advance(&mut self.carry2, end2);
        }
        let batch = InputBatch {
            demux:None,
            lane_index: None,
            id: self.batch_id,
            first_record_index: self.next_record_index,
            record_count: count,
            reads1,
            reads2,
        };
        self.batch_id += 1;
        self.next_record_index += count as u64;
        self.pair_count += count as u64;
        Ok(Some(batch))
    }

    /// Join lanes after the producer has reached a clean terminal message.
    /// This must not set `stop`: downstream workers may still be draining
    /// batches that were already published.
    pub fn finish(&mut self) -> Result<(), SourceError> {
        self.join_lanes()
    }

    /// Cancel lane production and join both lane threads. This is used for a
    /// source/worker failure or during drop, never for ordinary EOF.
    pub fn cancel(&mut self) -> Result<(), SourceError> {
        self.stop.store(true, Ordering::Relaxed);
        self.budget.wake_cancelled();
        self.join_lanes()
    }

    #[cfg(test)]
    pub fn pair_count(&self) -> u64 {
        self.pair_count
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::Source;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.scratch/tests");
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!(
            "soapnuke-source-{label}-{}-{nonce}.fastq",
            std::process::id()
        ))
    }

    fn fastq_records(count: usize, mate: usize) -> String {
        let mut out = String::new();
        for i in 0..count {
            out.push_str(&format!("@read{i}/{mate}\nACGT\n+\nIIII\n"));
        }
        out
    }

    fn count_records(bytes: &[u8]) -> usize {
        bytes.iter().filter(|&&byte| byte == b'\n').count() / 4
    }

    fn assert_source_error(result: Result<Option<super::InputBatch>, crate::failure::SourceError>, needle: &str) {
        match result {
            Err(error) => assert!(error.contains(needle), "unexpected source error: {error}"),
            Ok(_) => panic!("source unexpectedly returned a batch or clean EOF"),
        }
    }

    #[test]
    fn native_layout_maximum_frame_capacity_and_cancel_before_open() {
        use crate::{
            cpu_budget::CpuBudget,
            native_execution::{Execution, RECORD_BYTES},
            params::GlobalParams,
        };
        let path = fixture_path("maximum-header");
        let overhead = "@\nA\n+\nI\n".len();
        let text = format!("@{}\nA\n+\nI\n", "h".repeat(RECORD_BYTES - overhead));
        assert_eq!(text.len(), RECORD_BYTES);
        fs::write(&path, text).unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.native_limit_units = Some(1);
        gp.fq1_path = path.to_string_lossy().into();
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 1, stop.clone()).unwrap();
        let mut source = Source::open_layout(&gp, stop, CpuBudget::new(1), &e).unwrap();
        let batch = source.next_batch().unwrap().unwrap();
        assert_eq!(batch.reads1.len(), RECORD_BYTES);
        assert!(batch.reads1.capacity() <= crate::native_execution::LANE_BYTES);
        assert!(source.next_batch().unwrap().is_none());
        drop(source);
        gp.fq1_path = "missing-must-not-open-after-cancel".into();
        gp.native_limit_units = None;
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 1, stop.clone()).unwrap();
        let mut source = Source::open_layout(&gp, stop, CpuBudget::new(1), &e).unwrap();
        e.cancel();
        assert!(source.next_batch().err().unwrap().contains("cancelled"));
        drop(source);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
    #[test]
    fn native_layout_global_indices_survive_lanes_and_filtering() {
        use crate::{cpu_budget::CpuBudget, native_execution::Execution, params::GlobalParams};
        for threads in [1, 2, 16, 32] {
            for patch in [1, 2, 2500] {
                let a = fixture_path("layout-a1");
                let b = fixture_path("layout-a2");
                let c = fixture_path("layout-b1");
                let d = fixture_path("layout-b2");
                fs::write(&a, "@left/1\nA\n+\nI\n").unwrap();
                fs::write(&b, "@left/2\nT\n+\nI\n").unwrap();
                fs::write(&c, "@right0/1\nCC\n+\nII\n@right1/1\nGG\n+\nII\n").unwrap();
                fs::write(&d, "@right0/2\nGG\n+\nII\n@right1/2\nCC\n+\nII\n").unwrap();
                let mut gp = GlobalParams::new();
                gp.native_m1 = true;
                gp.threads_num = threads;
                gp.patch_size = patch;
                gp.fq1_path = a.to_string_lossy().into();
                gp.fq2_path = b.to_string_lossy().into();
                gp.native_lane_manifest = Some(Arc::new(format!(
                    "lane_id\tr1\tr2\nL0\t{}\t{}\nL1\t{}\t{}\n",
                    a.display(),
                    b.display(),
                    c.display(),
                    d.display()
                )));
                Arc::get_mut(&mut gp.native_process).unwrap().min_length = 2;
                let stop = Arc::new(AtomicBool::new(false));
                let execution = Execution::new(threads, 2, stop.clone()).unwrap();
                let mut source =
                    Source::open_layout(&gp, stop, CpuBudget::new(threads), &execution).unwrap();
                assert!(
                    execution.facts()["memory_budget"]["current_feature_reserved_bytes"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
                let mut stats = crate::stats::PeLocalStats::new(&gp);
                let mut seen = Vec::new();
                let mut accepted = Vec::new();
                while let Some(batch) = source.next_batch().unwrap() {
                    let r1 = crate::pipeline::qualification_parse_at(
                        &batch.reads1,
                        "read1",
                        batch.first_record_index,
                        batch.record_count,
                    )
                    .unwrap();
                    let r2 = crate::pipeline::qualification_parse_at(
                        batch.reads2.as_ref().unwrap(),
                        "read2",
                        batch.first_record_index,
                        batch.record_count,
                    )
                    .unwrap();
                    seen.extend(r1.iter().map(|r| r.source_index));
                    assert_eq!(r1[0].source_index, r2[0].source_index);
                    let (a, _) =
                        crate::native_kernel::apply(r1, Some(r2), &mut stats, &gp).unwrap();
                    accepted.extend(a.iter().map(|r| r.source_index));
                }
                assert_eq!(seen, [0, 1, 2]);
                assert_eq!(accepted, [1, 2]);
                assert_eq!(stats.raw1.gs.reads_number, 3);
                source.finish().unwrap();
                drop(source);
                assert_eq!(
                    execution.facts()["memory_budget"]["current_feature_reserved_bytes"],
                    0
                );
            }
        }
    }
    #[test]
    fn native_prefix_stops_before_malformed_tail_and_missing_next_lane() {
        use crate::{cpu_budget::CpuBudget, native_execution::Execution, params::GlobalParams};
        let a = fixture_path("prefix-a");
        let b = fixture_path("prefix-b");
        fs::write(&a, "@p/1\nAC\n+\nII\nmalformed later tail").unwrap();
        fs::write(&b, "@p/2\nGT\n+\nII\nmalformed later tail").unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.patch_size = 2500;
        gp.native_limit_units = Some(1);
        gp.fq1_path = a.to_string_lossy().into();
        gp.fq2_path = b.to_string_lossy().into();
        gp.native_lane_manifest = Some(Arc::new(format!(
            "lane_id\tr1\tr2\nL0\t{}\t{}\nL1\tmissing-untouched1\tmissing-untouched2\n",
            a.display(),
            b.display()
        )));
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 2, stop.clone()).unwrap();
        let mut source = Source::open_layout(&gp, stop, CpuBudget::new(1), &e).unwrap();
        let first = source.next_batch().unwrap().unwrap();
        assert_eq!(first.record_count, 1);
        assert_eq!(first.first_record_index, 0);
        assert!(source.next_batch().unwrap().is_none());
        assert_eq!(source.backend_facts()["opened_lane_groups"], 1);
    }
    #[test]
    fn native_interleaved_pair_boundaries_eof_and_ownership() {
        use crate::{
            cpu_budget::CpuBudget,
            native_execution::{
                Execution, ASSEMBLY_BYTES, LANE_BYTES, PREFETCH_PER_LANE, RECORD_BYTES,
            },
            params::GlobalParams,
        };
        let path = fixture_path("interleaved");
        fs::write(&path, "@p/1\nAC\n+\nII\n@p/2\nGT\n+\nII\n@odd/1\nA\n+\nI\n").unwrap();
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        gp.native_interleaved = true;
        gp.patch_size = 1;
        gp.fq1_path = path.to_string_lossy().into();
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 2, stop.clone()).unwrap();
        let mut source = Source::open_layout(&gp, stop, CpuBudget::new(1), &e).unwrap();
        let pair = source.next_batch().unwrap().unwrap();
        assert_eq!(pair.record_count, 1);
        assert!(pair.reads2.is_some());
        assert_eq!(pair.first_record_index, 0);
        assert!(source
            .next_batch()
            .err()
            .unwrap()
            .contains("odd interleaved EOF"));
        drop(source);
        // Two physical-reader reservations conservatively cover the single-reader
        // interleaved bridge: readers, overlapping frame growth, pending pair and outputs.
        let bridge = 2 * (8 << 20) + 3 * RECORD_BYTES + 2 * RECORD_BYTES + 2 * LANE_BYTES;
        assert!(bridge <= 2 * PREFETCH_PER_LANE + ASSEMBLY_BYTES);
        gp.native_limit_units = Some(1);
        let stop = Arc::new(AtomicBool::new(false));
        let e = Execution::new(1, 2, stop.clone()).unwrap();
        let mut source = Source::open_layout(&gp, stop, CpuBudget::new(1), &e).unwrap();
        assert!(source.next_batch().unwrap().is_some());
        assert!(source.next_batch().unwrap().is_none());
    }
    #[test]
    fn patch_size_counts_records_not_serialized_bytes() {
        let path1 = fixture_path("r1");
        let path2 = fixture_path("r2");
        fs::write(&path1, fastq_records(3, 1)).unwrap();
        fs::write(&path2, fastq_records(3, 2)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 2, stop).unwrap();

        let first = source.next_batch().unwrap().unwrap();
        assert_eq!(first.id, 0);
        assert_eq!(first.first_record_index, 0);
        assert_eq!(first.record_count, 2);
        assert_eq!(count_records(&first.reads1), 2);
        assert_eq!(count_records(first.reads2.as_ref().unwrap()), 2);
        let second = source.next_batch().unwrap().unwrap();
        assert_eq!(second.id, 1);
        assert_eq!(second.first_record_index, 2);
        assert_eq!(second.record_count, 1);
        assert_eq!(count_records(&second.reads1), 1);
        assert_eq!(count_records(second.reads2.as_ref().unwrap()), 1);
        assert!(source.next_batch().unwrap().is_none());

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn patch_sizes_keep_record_boundaries_and_final_partial_batch() {
        const TOTAL: usize = 10_006;
        for patch in [1usize, 625, 2500, 2501, 10_000] {
            let path1 = fixture_path(&format!("patch-{patch}-r1"));
            let path2 = fixture_path(&format!("patch-{patch}-r2"));
            fs::write(&path1, fastq_records(TOTAL, 1)).unwrap();
            fs::write(&path2, fastq_records(TOTAL, 2)).unwrap();

            let stop = Arc::new(AtomicBool::new(false));
            let mut source = Source::open(
                path1.to_str().unwrap(),
                path2.to_str().unwrap(),
                patch,
                stop,
            )
            .unwrap();
            let mut batches = Vec::new();
            while let Some(batch) = source.next_batch().unwrap() {
                batches.push((batch.id, batch.first_record_index, batch.record_count));
            }
            let expected_batches = (TOTAL + patch - 1) / patch;
            assert_eq!(batches.len(), expected_batches);
            for (batch_number, &(id, first, count)) in batches.iter().enumerate() {
                let expected_first = batch_number * patch;
                let expected_count = patch.min(TOTAL - expected_first);
                assert_eq!(id, batch_number as u64);
                assert_eq!(first, expected_first as u64);
                assert_eq!(count, expected_count);
            }
            assert_eq!(source.pair_count(), TOTAL as u64);

            fs::remove_file(path1).unwrap();
            fs::remove_file(path2).unwrap();
        }
    }

    #[test]
    fn native_unequal_mate_counts_preserve_pair_reason_and_complete_prefix() {
        use crate::failure::{Category, Reason};
        for (n1, n2) in [(1, 0), (0, 1), (2, 1), (1, 2)] {
            let path1 = fixture_path("native-short-r1");
            let path2 = fixture_path("native-short-r2");
            fs::write(&path1, fastq_records(n1, 1)).unwrap();
            fs::write(&path2, fastq_records(n2, 2)).unwrap();
            let mut source = Source::open_native(
                path1.to_str().unwrap(), Some(path2.to_str().unwrap()), 2500,
                Arc::new(AtomicBool::new(false)), crate::cpu_budget::CpuBudget::new(2),
            ).unwrap();
            let mut first = Vec::new();
            let mut second = Vec::new();
            let error = loop {
                match source.next_batch() {
                    Ok(Some(batch)) => {
                        first.extend(batch.reads1);
                        second.extend(batch.reads2.unwrap());
                    }
                    Err(error) => break error.0,
                    Ok(None) => panic!("unequal native mates reported clean EOF"),
                }
            };
            assert_eq!((error.category, error.category.exit(), error.stage, error.reason),
                (Category::Input, 3, "input", Reason::PairCount));
            assert_eq!(error.message, "the number of reads in fastq1 and fastq2 is not equal");
            assert_eq!(error.process_message(), "pair.unequal_eof: the number of reads in fastq1 and fastq2 is not equal");
            assert_eq!(error.with_message("opaque").process_message(), "pair.unequal_eof: opaque");
            assert_eq!(first, fastq_records(n1.min(n2), 1).as_bytes());
            assert_eq!(second, fastq_records(n1.min(n2), 2).as_bytes());
            drop(source);
            fs::remove_file(path1).unwrap();
            fs::remove_file(path2).unwrap();
        }
    }

    #[test]
    fn unequal_mate_counts_are_a_source_error() {
        let path1 = fixture_path("unequal-r1");
        let path2 = fixture_path("unequal-r2");
        fs::write(&path1, fastq_records(2, 1)).unwrap();
        fs::write(&path2, fastq_records(1, 2)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 2500, stop).unwrap();
        assert_source_error(
            source.next_batch(),
            "the number of reads in fastq1 and fastq2 is not equal",
        );

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn lane_error_is_not_reported_as_clean_eof() {
        let path1 = fixture_path("truncated-r1");
        let path2 = fixture_path("valid-r2");
        fs::write(&path1, b"@read0/1\nACGT\n+\n").unwrap();
        fs::write(&path2, fastq_records(1, 2)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 1, stop).unwrap();
        assert_source_error(source.next_batch(), "truncated FASTQ record");

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn either_lane_error_cancels_the_pair() {
        let path1 = fixture_path("valid-r1");
        let path2 = fixture_path("truncated-r2");
        fs::write(&path1, fastq_records(1, 1)).unwrap();
        fs::write(&path2, b"@read0/2\nACGT\n+\n").unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 1, stop).unwrap();
        assert_source_error(source.next_batch(), "truncated FASTQ record");

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn cancellation_joins_lanes_blocked_on_full_channels() {
        let path1 = fixture_path("cancel-r1");
        let path2 = fixture_path("cancel-r2");
        fs::write(&path1, fastq_records(20_000, 1)).unwrap();
        fs::write(&path2, fastq_records(20_000, 2)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 1, stop).unwrap();
        source.cancel().unwrap();

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn zero_patch_size_is_rejected_before_lanes_start() {
        let path1 = fixture_path("zero-r1");
        let path2 = fixture_path("zero-r2");
        fs::write(&path1, fastq_records(1, 1)).unwrap();
        fs::write(&path2, fastq_records(1, 2)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let error = match Source::open(path1.to_str().unwrap(), path2.to_str().unwrap(), 0, stop) {
            Ok(_) => panic!("zero patch size unexpectedly opened a source"),
            Err(error) => error,
        };
        assert!(error.contains("patch size"));

        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }

    #[test]
    fn single_end_source_uses_one_lane_and_preserves_record_batches() {
        let path = fixture_path("single-end");
        fs::write(&path, fastq_records(3, 1)).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let mut source = Source::open_single(path.to_str().unwrap(), 2, stop).unwrap();
        let first = source.next_batch().unwrap().unwrap();
        assert_eq!(first.record_count, 2);
        assert!(first.reads2.is_none());
        let second = source.next_batch().unwrap().unwrap();
        assert_eq!(second.record_count, 1);
        assert!(second.reads2.is_none());
        assert!(source.next_batch().unwrap().is_none());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn accepts_complete_final_record_without_trailing_newline_for_pe_and_se() {
        let pe1 = fixture_path("final-no-newline-pe-r1");
        let pe2 = fixture_path("final-no-newline-pe-r2");
        fs::write(&pe1, b"@read0/1\nACGT\n+\nIIII").unwrap();
        fs::write(&pe2, b"@read0/2\nACGT\n+\nIIII").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut pe = Source::open(pe1.to_str().unwrap(), pe2.to_str().unwrap(), 1, stop).unwrap();
        let batch = pe.next_batch().unwrap().unwrap();
        assert_eq!(batch.record_count, 1);
        assert!(batch.reads1.ends_with(b"IIII\n"));
        assert!(batch.reads2.as_ref().unwrap().ends_with(b"IIII\n"));
        assert!(pe.next_batch().unwrap().is_none());
        fs::remove_file(pe1).unwrap();
        fs::remove_file(pe2).unwrap();

        let se = fixture_path("final-no-newline-se");
        fs::write(&se, b"@read0/1\nACGT\n+\nIIII").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut source = Source::open_single(se.to_str().unwrap(), 1, stop).unwrap();
        let batch = source.next_batch().unwrap().unwrap();
        assert_eq!(batch.record_count, 1);
        assert!(batch.reads1.ends_with(b"IIII\n"));
        assert!(source.next_batch().unwrap().is_none());
        fs::remove_file(se).unwrap();
    }

    #[test]
    fn final_quality_line_without_newline_still_reaches_parser_validation() {
        let pe1 = fixture_path("partial-final-pe-r1");
        let pe2 = fixture_path("partial-final-pe-r2");
        fs::write(&pe1, b"@read0/1\nACGT\n+\nIII").unwrap();
        fs::write(&pe2, b"@read0/2\nACGT\n+\nIIII").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut source =
            Source::open(pe1.to_str().unwrap(), pe2.to_str().unwrap(), 1, stop).unwrap();
        let batch = source.next_batch().unwrap().unwrap();
        assert_eq!(batch.record_count, 1);
        assert!(batch.reads1.ends_with(b"III\n"));
        fs::remove_file(pe1).unwrap();
        fs::remove_file(pe2).unwrap();
    }
}

#[cfg(test)]
mod native_bounded_tests {
    use super::*;
    fn input(label: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = std::path::PathBuf::from(std::env::var("TMPDIR").unwrap()).join(format!(
            "m4-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&p, bytes).unwrap();
        p
    }
    #[test]
    fn intact_chunks_transfer_actual_capacity_for_se_and_pe() {
        for paired in [false, true] {
            let a = b"@x/1 full comment\nAC\n+\nII\n@y/1\nGT\n+\nII";
            let b = b"@x/2 comment\nTG\n+\nII\n@y/2\nCA\n+\nII";
            let p1 = input("intact1", a);
            let p2 = paired.then(|| input("intact2", b));
            let mut source = Source::open_native(
                p1.to_str().unwrap(),
                p2.as_ref().map(|p| p.to_str().unwrap()),
                4096,
                Arc::new(AtomicBool::new(false)),
                CpuBudget::new(4),
            )
            .unwrap();
            source.carry1 = Source::next_chunk(&source.lane1_rx, &mut source.done1, &source.stop)
                .unwrap()
                .map(|chunk| Carry {
                    remaining: chunk.record_count,
                    chunk,
                    offset: 0,
                });
            if let Some(rx) = &source.lane2_rx {
                source.carry2 = Source::next_chunk(rx, &mut source.done2, &source.stop)
                    .unwrap()
                    .map(|chunk| Carry {
                        remaining: chunk.record_count,
                        chunk,
                        offset: 0,
                    });
            }
            let before = &source.carry1.as_ref().unwrap().chunk.bytes;
            let ptr = before.as_ptr();
            let capacity = before.capacity();
            assert!(capacity > before.len());
            let second = source
                .carry2
                .as_ref()
                .map(|c| (c.chunk.bytes.as_ptr(), c.chunk.bytes.capacity()));
            let batch = source.next_batch().unwrap().unwrap();
            assert_eq!(batch.reads1.as_ptr(), ptr);
            assert_eq!(batch.reads1.capacity(), capacity);
            let mut normalized = a.to_vec();
            normalized.push(b'\n');
            assert_eq!(batch.reads1, normalized);
            if let Some((ptr, capacity)) = second {
                let reads = batch.reads2.unwrap();
                assert_eq!(reads.as_ptr(), ptr);
                assert_eq!(reads.capacity(), capacity);
                let mut normalized = b.to_vec();
                normalized.push(b'\n');
                assert_eq!(reads, normalized);
            }
            assert_eq!(
                (batch.id, batch.first_record_index, batch.record_count),
                (0, 0, 2)
            );
            assert!(source.next_batch().unwrap().is_none());
        }
    }
    #[test]
    fn partial_or_excess_capacity_chunks_use_copied_fallback() {
        for excessive_capacity in [false, true] {
            let text = b"@x\nAC\n+\nII\n@y\nGT\n+\nII\n";
            let p = input("fallback", text);
            let mut source = Source::open_native(
                p.to_str().unwrap(),
                None,
                4096,
                Arc::new(AtomicBool::new(false)),
                CpuBudget::new(2),
            )
            .unwrap();
            let mut chunk = Source::next_chunk(&source.lane1_rx, &mut source.done1, &source.stop)
                .unwrap()
                .unwrap();
            if excessive_capacity {
                chunk.bytes.reserve(crate::native_execution::LANE_BYTES + 1);
            }
            let ptr = chunk.bytes.as_ptr();
            source.carry1 = Some(Carry {
                remaining: chunk.record_count,
                chunk,
                offset: 0,
            });
            if !excessive_capacity {
                source.patch_size = 1;
            }
            let first = source.next_batch().unwrap().unwrap();
            assert_ne!(first.reads1.as_ptr(), ptr);
            assert_eq!(first.reads1.capacity(), first.reads1.len());
            let mut actual = first.reads1;
            while let Some(batch) = source.next_batch().unwrap() {
                actual.extend(batch.reads1);
            }
            assert_eq!(actual, text);
        }
    }
    #[test]
    fn asymmetric_mate_chunk_boundaries_keep_units_and_all_bytes() {
        let a = (0..40)
            .map(|i| format!("@pair{i}/1 {}\nAC\n+\nII\n", "x".repeat(40000)))
            .collect::<String>();
        let b = (0..40)
            .map(|i| format!("@pair{i}/2\nGT\n+\nII\n"))
            .collect::<String>();
        let p1 = input("asym1", a.as_bytes());
        let p2 = input("asym2", b.as_bytes());
        let mut source = Source::open_native(
            p1.to_str().unwrap(),
            Some(p2.to_str().unwrap()),
            4096,
            Arc::new(AtomicBool::new(false)),
            CpuBudget::new(4),
        )
        .unwrap();
        let mut x = Vec::new();
        let mut y = Vec::new();
        let mut n = 0;
        while let Some(batch) = source.next_batch().unwrap() {
            assert!(
                batch.reads1.len() + batch.reads2.as_ref().unwrap().len()
                    <= crate::native_execution::ASSEMBLY_BYTES
            );
            assert_eq!(batch.first_record_index, n);
            n += batch.record_count as u64;
            x.extend(batch.reads1);
            y.extend(batch.reads2.unwrap());
        }
        assert_eq!(n, 40);
        assert_eq!(x, a.as_bytes());
        assert_eq!(y, b.as_bytes());
        source.finish().unwrap();
    }
    #[test]
    fn oversized_framing_fails_before_growth_and_lane_cancel_wakes_cpu() {
        let text = format!(
            "@{}\nA\n+\nI\n",
            "x".repeat(crate::native_execution::RECORD_BYTES)
        );
        let p = input("oversized", text.as_bytes());
        let mut source = Source::open_native(
            p.to_str().unwrap(),
            None,
            4096,
            Arc::new(AtomicBool::new(false)),
            CpuBudget::new(1),
        )
        .unwrap();
        assert!(source.next_batch().err().unwrap().contains("exceeds"));
        let p = input("cancel", b"@x\nAC\n+\nII\n");
        let budget = CpuBudget::new(1);
        let held = budget.acquire(Stage::Worker);
        let mut source = Source::open_native(
            p.to_str().unwrap(),
            None,
            4096,
            Arc::new(AtomicBool::new(false)),
            budget,
        )
        .unwrap();
        let start = std::time::Instant::now();
        let _ = source.cancel();
        assert!(start.elapsed() < Duration::from_secs(5));
        drop(held);
    }
}

#[cfg(test)]
mod record_end_equivalence_tests {
    use super::record_end;
    fn scalar(bytes: &[u8], start: usize) -> Result<usize, String> {
        let mut count = 0;
        for (i, &b) in bytes[start..].iter().enumerate() {
            if b == b'\n' {
                count += 1;
                if count == 4 {
                    return Ok(start + i + 1);
                }
            }
        }
        Err("[input] native framing lost a complete record".into())
    }
    #[test]
    fn fourth_newline_literal_offsets_errors_and_trailing_bytes() {
        let cases: &[(&[u8], usize, Option<usize>)] = &[
            (b"", 0, None),
            (b"abc", 3, None),
            (b"\n\n\n", 0, None),
            (b"\n\n\n\n", 0, Some(4)),
            (b"xx\n\n\n\nrest\n", 2, Some(6)),
            (b"@x\nA\n+\nI\nextra", 0, Some(9)),
            (b"@x\nA\n+\nI", 0, None),
            (b"\xff\n\x00\n\xfe\n\x80\n", 0, Some(8)),
        ];
        for &(bytes, start, expected) in cases {
            let actual = record_end(bytes, start);
            assert_eq!(actual.clone().map_err(String::from), scalar(bytes, start));
            assert_eq!(actual.ok(), expected);
        }
    }
    #[test]
    fn fourth_newline_matches_scalar_across_generated_offsets_and_fragments() {
        for initial in [20261022u64, 20261023u64] {
            let mut seed = initial;
            for case in 0..512usize {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let len = (seed >> 32) as usize % 4097;
                let mut bytes = Vec::with_capacity(len);
                for _ in 0..len {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    bytes.push(if seed & 15 == 0 {
                        b'\n'
                    } else {
                        (seed >> 32) as u8
                    });
                }
                for start in [0, len / 4, len / 2, len.saturating_sub(1), len] {
                    assert_eq!(
                        record_end(&bytes, start).map_err(String::from),
                        scalar(&bytes, start),
                        "seed={initial} case={case} start={start}"
                    );
                }
                let width = [1, 3, 7, 64, 4096][case % 5];
                let joined: Vec<u8> = bytes.chunks(width).flatten().copied().collect();
                assert_eq!(joined, bytes);
                assert_eq!(record_end(&joined, 0).map_err(String::from), scalar(&bytes, 0));
            }
        }
    }
}

#[cfg(test)]
mod native_newline_iteration_tests {
    fn scalar(bytes: &[u8]) -> Vec<usize> {
        bytes
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| (b == b'\n').then_some(i))
            .collect()
    }
    fn check(bytes: &[u8]) {
        let reference = scalar(bytes);
        assert_eq!(
            memchr::memchr_iter(b'\n', bytes).collect::<Vec<_>>(),
            reference
        );
        for width in [1, 2, 3, 4, 7, 31, 64, 255, 4096] {
            let positions: Vec<_> = bytes
                .chunks(width)
                .enumerate()
                .flat_map(|(block, part)| {
                    memchr::memchr_iter(b'\n', part).map(move |i| block * width + i)
                })
                .collect();
            assert_eq!(positions, reference);
            assert_eq!(
                positions.iter().skip(3).step_by(4).collect::<Vec<_>>(),
                reference.iter().skip(3).step_by(4).collect::<Vec<_>>()
            );
        }
    }
    #[test]
    fn native_framer_newlines_match_scalar_and_fragmented_positions() {
        for value in [
            b"".as_slice(),
            b"no newline",
            b"\n\n\n\n\n",
            b"@x\r\nAC\r\n+\r\nII\r\n",
            b"@x\nAC\n+\nII",
            b"\xff\x00\n\xfe\n+\n\x93\n",
        ] {
            check(value);
        }
        for size in [4095, 4096, 4097, 65535, 65536, 65537] {
            let mut value = vec![b'X'; size];
            for i in [0, 1, 3, size / 2, size - 1] {
                value[i] = b'\n';
            }
            check(&value);
        }
        for mut seed in [2026102307u64, 2026102308u64] {
            for case in 0..1000 {
                let len = case % 513;
                let mut value = Vec::with_capacity(len);
                for _ in 0..len {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    value.push(if seed % 7 == 0 {
                        b'\n'
                    } else {
                        (seed >> 8) as u8
                    });
                }
                check(&value);
            }
        }
    }
}

#[cfg(test)]
mod native_layout_target_tests {
    use super::*;
    use crate::native_execution::{
        ASSEMBLY_BYTES, LANE_BYTES, LANE_TARGET, RECORD_BYTES, TARGET_BYTES,
    };
    fn fixture(label: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = std::path::PathBuf::from(std::env::var("TMPDIR").unwrap()).join(format!(
            "r37-layout-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&p, bytes).unwrap();
        p
    }
    #[test]
    fn layout_targets_keep_all_bytes_units_and_actual_capacity() {
        let record = format!(
            "@same full header\n{}\n+\n{}\n",
            "T".repeat(150),
            "I".repeat(150)
        );
        let text = record.repeat(2501);
        assert!(record.len() * 2500 > LANE_TARGET && record.len() * 2500 < TARGET_BYTES);
        for paired in [false, true] {
            let p1 = fixture("normal-r1", text.as_bytes());
            let p2 = paired.then(|| fixture("normal-r2", text.as_bytes()));
            let mut source = Source::open_native(
                p1.to_str().unwrap(),
                p2.as_ref().map(|p| p.to_str().unwrap()),
                2500,
                Arc::new(AtomicBool::new(false)),
                CpuBudget::new(4),
            )
            .unwrap();
            let target = if paired { LANE_TARGET } else { TARGET_BYTES };
            let expected_first = if paired {
                (LANE_TARGET / record.len()).min(2500)
            } else {
                2500
            };
            let mut units = 0;
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut batches = 0;
            while let Some(batch) = source.next_batch().unwrap() {
                assert_eq!(batch.id, batches);
                assert_eq!(batch.first_record_index, units);
                assert!(batch.reads1.len() <= target && batch.reads1.capacity() <= LANE_BYTES);
                if batches == 0 {
                    assert_eq!(batch.record_count, expected_first);
                    assert_eq!(batch.reads1.capacity(), LANE_BYTES);
                    if !paired {
                        assert!(batch.reads1.len() > LANE_TARGET);
                    }
                }
                let other = batch.reads2.as_ref().map_or(0, |v| v.len());
                assert!(batch.reads1.len() + other <= TARGET_BYTES);
                units += batch.record_count as u64;
                batches += 1;
                a.extend(batch.reads1);
                if let Some(v) = batch.reads2 {
                    assert!(v.len() <= LANE_TARGET && v.capacity() <= LANE_BYTES);
                    b.extend(v);
                }
            }
            assert_eq!(units, 2501);
            assert_eq!(a, text.as_bytes());
            if paired {
                assert_eq!(b, text.as_bytes());
            } else {
                assert_eq!(batches, 2);
            }
            source.finish().unwrap();
        }
    }
    #[test]
    fn oversized_single_record_stays_inside_existing_capacity_for_both_layouts() {
        let text = format!("@{}\nA\n+\nI\n", "x".repeat(LANE_TARGET + 100));
        assert!(text.len() > LANE_TARGET && text.len() < RECORD_BYTES);
        for paired in [false, true] {
            let p1 = fixture("large-r1", text.as_bytes());
            let p2 = paired.then(|| fixture("large-r2", text.as_bytes()));
            let mut source = Source::open_native(
                p1.to_str().unwrap(),
                p2.as_ref().map(|p| p.to_str().unwrap()),
                2500,
                Arc::new(AtomicBool::new(false)),
                CpuBudget::new(4),
            )
            .unwrap();
            let batch = source.next_batch().unwrap().unwrap();
            assert_eq!(batch.record_count, 1);
            assert_eq!(batch.reads1, text.as_bytes());
            assert!(batch.reads1.capacity() <= LANE_BYTES);
            if !paired {
                assert_eq!(batch.reads1.capacity(), LANE_BYTES);
            }
            let total = batch.reads1.len() + batch.reads2.as_ref().map_or(0, |v| v.len());
            assert!(total <= ASSEMBLY_BYTES);
            if let Some(v) = batch.reads2 {
                assert_eq!(v, text.as_bytes());
                assert!(v.capacity() <= LANE_BYTES);
            }
            assert!(source.next_batch().unwrap().is_none());
            source.finish().unwrap();
        }
    }
}

#[cfg(test)]
mod owned_reader_tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::AtomicUsize;
    const DATA: &[u8] = b"@first full comment\nAC\n+\nII\n@second\nTG\n+\n55\n";
    struct Owned { bytes: Cursor<Vec<u8>>, drops: Arc<AtomicUsize> }
    impl Read for Owned { fn read(&mut self, out: &mut [u8]) -> io::Result<usize> { let n=out.len().min(1); self.bytes.read(&mut out[..n]) } }
    impl Drop for Owned { fn drop(&mut self) { self.drops.fetch_add(1,Ordering::SeqCst); } }
    #[test]
    fn owned_plain_uses_existing_framing_and_drops_once() {
        let drops=Arc::new(AtomicUsize::new(0));
        let input=Owned{bytes:Cursor::new(DATA.to_vec()),drops:drops.clone()};
        let mut reader=LaneReader::Plain(PlainReader::from_reader(Box::new(input)));
        let stop=AtomicBool::new(false);
        assert_eq!(NativeLayout::record(&mut reader,&stop).unwrap().unwrap(),b"@first full comment\nAC\n+\nII\n");
        assert_eq!(NativeLayout::record(&mut reader,&stop).unwrap().unwrap(),b"@second\nTG\n+\n55\n");
        assert!(NativeLayout::record(&mut reader,&stop).unwrap().is_none());
        drop(reader);assert_eq!(drops.load(Ordering::SeqCst),1);
    }
    #[test]
    fn owned_gzip_uses_existing_crc_and_truncation_checks() {
        let zip=crate::ffi::Compressor::new(2).unwrap().gzip_compress_alloc(DATA);
        let stop=AtomicBool::new(false);
        for truncated in [false,true] {
            let bytes=if truncated{zip[..zip.len()-5].to_vec()}else{zip.clone()};
            let mut reader=LaneReader::Gz(crate::zlib_reader::ZlibReader::from_reader(Box::new(Cursor::new(bytes)),"owned gzip"));
            let mut records=Vec::new();
            let terminal=loop { match NativeLayout::record(&mut reader,&stop) { Ok(Some(record))=>records.push(record), other=>break other } };
            if truncated {assert!(terminal.is_err());} else {assert!(terminal.unwrap().is_none());assert_eq!(records.concat(),DATA);}
        }
    }
}

// Reuse the production framing/record parser for saved-report artifact association.
pub(crate) fn verify_saved_artifacts(paths:&[std::path::PathBuf],encodings:&[u64],counts:&[u64],layout:&str)->Result<(),String>{
    let stop=AtomicBool::new(false);let choice=DecoderChoice{backend:InputBackend::Zlib,threads_per_lane:1,reserved_credits:0};
    let mut a=LaneReader::open(paths[0].to_str().ok_or("non-UTF8 artifact path")?,choice).map_err(|e|e.to_string())?;
    let mut b=if paths.len()==2{Some(LaneReader::open(paths[1].to_str().ok_or("non-UTF8 artifact path")?,choice).map_err(|e|e.to_string())?)}else{None};
    fn next(r:&mut LaneReader,stop:&AtomicBool,encoding:u64)->Result<Option<crate::record::FastqRecord>,String>{let Some(raw)=NativeLayout::record(r,stop)?else{return Ok(None)};let mut records=crate::pipeline::parse_chunk_records_with_capacity(&raw,"saved artifact",0,true,1)?;let record=records.pop().ok_or("missing artifact record")?;if ![33,64].contains(&encoding)||record.qual_seq.bytes().any(|q|u64::from(q)<encoding||u64::from(q)>encoding+93)||record.sequence.bytes().any(|b|!b"ACGTNacgtn".contains(&b)){return Err("artifact encoding/alphabet mismatch".into());}Ok(Some(record))}
    let mut seen=[0u64;2];loop{let x=next(&mut a,&stop,encodings[0])?;if layout=="split-PE"{let y=next(b.as_mut().unwrap(),&stop,encodings[1])?;match(x,y){(None,None)=>break,(Some(x),Some(y))=>{if !crate::record::pair_ids_match(&x.seq_id,&y.seq_id){return Err("mate association mismatch".into());}seen[0]+=1;seen[1]+=1;},_=>return Err("mate artifact count mismatch".into())}}else{let Some(x)=x else{break};seen[0]+=1;if layout=="interleaved-PE"{let y=next(&mut a,&stop,encodings[0])?.ok_or("odd interleaved artifact")?;seen[0]+=1;if !crate::record::pair_ids_match(&x.seq_id,&y.seq_id){return Err("interleaved mate association mismatch".into());}}}}
    if seen[..paths.len()]!=*counts{return Err("artifact record count mismatch".into());}Ok(())
}
