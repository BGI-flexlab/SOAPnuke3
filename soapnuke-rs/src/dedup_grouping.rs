//! Bounded native full-key grouping; no FASTQ framing or job wrapper.
use crate::native_execution::{Execution, FeatureTicket};
use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};
#[derive(Debug)]
struct Entry {
    key: Vec<u8>,
    ordinal: u64,
}
struct Reader {
    file: Option<File>,
    path: PathBuf,
    head: Option<Entry>,
    header: [u8; 12],
}
struct Writer {
    file: Option<File>,
    path: PathBuf,
    header: [u8; 12],
}
enum Phase {
    Resident(Vec<Entry>),
    Merge {
        readers: [Reader; 2],
        writer: Writer,
        previous: Vec<u8>,
    },
}
struct Core {
    phase: Phase,
    directory: PathBuf,
    _ticket: FeatureTicket,
    limit: usize,
    peak: usize,
    units: u64,
    runs: u64,
    spill_bytes: u64,
}
pub(crate) fn fixed_owner_bytes() -> usize {
    std::mem::size_of::<Collector>()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compiled_owner_layout() {
        let v = serde_json::json!({"core_fixed_bytes":fixed_owner_bytes(),"entry_bytes":std::mem::size_of::<Entry>(),"reader_bytes":std::mem::size_of::<Reader>(),"writer_bytes":std::mem::size_of::<Writer>(),"phase_bytes":std::mem::size_of::<Phase>(),"ticket_bytes":std::mem::size_of::<FeatureTicket>(),"scope":"Rust object storage only; dynamic capacities/paths and overlapping owners additionally required; not a universal algorithm minimum"});
        println!("{}", v);
        if let Ok(p) = std::env::var("R83_LAYOUT_CAPTURE") {
            std::fs::write(p, v.to_string()).unwrap();
        }
    }
}
#[derive(Debug)]
pub(crate) enum Failure {
    Nonfit { minimum: usize, limit: usize },
    Io(std::io::Error),
    Invalid,
    Cancelled,
}
impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
#[derive(Debug)]
pub(crate) struct Facts {
    pub fixed: usize,
    pub reserved: usize,
    pub proven_upper: usize,
    pub measured_peak: usize,
    pub runs: u64,
    pub spill_bytes: u64,
    pub units: u64,
    pub budget_flushes: u64,
    pub boundary_flushes: u64,
}
fn path(root: &std::path::Path, pass: u64, id: u64) -> PathBuf {
    root.join(format!("run-{pass:03}-{id:020}.bin"))
}
fn key_size(parts: &[&str]) -> Option<usize> {
    parts
        .iter()
        .try_fold(1usize, |n, s| n.checked_add(8)?.checked_add(s.len()))
}
fn key(parts: &[&str], maximum: usize) -> Result<Vec<u8>, Failure> {
    let n = key_size(parts).ok_or(Failure::Invalid)?;
    if n > maximum || parts.len() > 2 || parts.is_empty() {
        return Err(Failure::Invalid);
    }
    let mut b = Vec::new();
    b.try_reserve_exact(n).map_err(|_| Failure::Nonfit {
        minimum: n,
        limit: maximum,
    })?;
    if b.capacity() != n {
        return Err(Failure::Invalid);
    }
    b.push(parts.len() as u8);
    for s in parts {
        let n = u64::try_from(s.len()).map_err(|_| Failure::Invalid)?;
        b.extend_from_slice(&n.to_le_bytes());
        b.extend(s.bytes().map(|c| c.to_ascii_uppercase()));
    }
    Ok(b)
}
impl Reader {
    fn empty() -> Self {
        Self {
            file: None,
            path: PathBuf::new(),
            head: None,
            header: [0; 12],
        }
    }
    fn open(p: PathBuf, maximum: usize) -> Result<Self, Failure> {
        let f = File::open(&p)?;
        let mut r = Self {
            file: Some(f),
            path: p,
            head: None,
            header: [0; 12],
        };
        r.advance(maximum)?;
        Ok(r)
    }
    fn advance(&mut self, maximum: usize) -> Result<(), Failure> {
        let f = self.file.as_mut().unwrap();
        let n = f.read(&mut self.header[..1])?;
        if n == 0 {
            self.head = None;
            return Ok(());
        }
        f.read_exact(&mut self.header[1..])?;
        let len = u32::from_le_bytes(self.header[..4].try_into().unwrap()) as usize;
        let ordinal = u64::from_le_bytes(self.header[4..].try_into().unwrap());
        if len > maximum {
            return Err(Failure::Invalid);
        }
        let mut b = Vec::new();
        b.try_reserve_exact(len).map_err(|_| Failure::Nonfit {
            minimum: len,
            limit: maximum,
        })?;
        if b.capacity() != len {
            return Err(Failure::Invalid);
        }
        b.resize(len, 0);
        f.read_exact(&mut b)?;
        self.head = Some(Entry { key: b, ordinal });
        Ok(())
    }
    fn dynamic(&self) -> usize {
        self.path.capacity() + self.head.as_ref().map_or(0, |e| e.key.capacity())
    }
}
impl Writer {
    fn create(p: PathBuf) -> Result<Self, Failure> {
        let f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&p)?;
        Ok(Self {
            file: Some(f),
            path: p,
            header: [0; 12],
        })
    }
    fn entry(&mut self, e: &Entry) -> Result<usize, Failure> {
        self.header[..4].copy_from_slice(&(e.key.len() as u32).to_le_bytes());
        self.header[4..].copy_from_slice(&e.ordinal.to_le_bytes());
        let f = self.file.as_mut().unwrap();
        storage_write(f,&self.header)?;
        storage_write(f,&e.key)?;
        Ok(12 + e.key.len())
    }
    fn close(&mut self) -> Result<(), Failure> {
        if let Some(mut f) = self.file.take() {
            storage_flush(&mut f)?;
            f.sync_all()?;
            storage_close(f)?;
        }
        Ok(())
    }
}
#[cfg(test)]
thread_local!{pub(crate) static STORAGE_FAULT:std::cell::Cell<u8>=const{std::cell::Cell::new(0)};}
fn storage_write(f:&mut File,bytes:&[u8])->std::io::Result<()>{
 #[cfg(test)]{
  let mode=STORAGE_FAULT.with(|v|v.get());if mode==1||mode==2{
   let n=bytes.len().min(if mode==1{4}else{2});f.write_all(&bytes[..n])?;
   return Err(std::io::Error::from_raw_os_error(if mode==1{28}else{5}));
  }
 }
 f.write_all(bytes)
}
fn storage_flush(f:&mut File)->std::io::Result<()>{f.flush()?;#[cfg(test)]if STORAGE_FAULT.with(|v|v.get()==3){return Err(std::io::Error::from_raw_os_error(5));}Ok(())}
fn storage_close(f:File)->std::io::Result<()>{crate::checked_io::close_file(f).map_err(std::io::Error::other)?;#[cfg(test)]if STORAGE_FAULT.with(|v|v.get()==4){return Err(std::io::Error::from_raw_os_error(5));}Ok(())}
impl Core {
    fn measured(&mut self, temporary: usize) -> Result<(), Failure> {
        let dynamic = self.directory.capacity()
            + match &self.phase {
                Phase::Resident(v) => {
                    v.capacity() * std::mem::size_of::<Entry>()
                        + v.iter().map(|e| e.key.capacity()).sum::<usize>()
                }
                Phase::Merge {
                    readers,
                    writer,
                    previous,
                } => {
                    readers.iter().map(Reader::dynamic).sum::<usize>()
                        + writer.path.capacity()
                        + previous.capacity()
                }
            };
        let n = fixed_owner_bytes() + dynamic + temporary;
        self.peak = self.peak.max(n);
        if n > self.limit {
            return Err(Failure::Nonfit {
                minimum: n,
                limit: self.limit,
            });
        }
        Ok(())
    }
    fn flush(&mut self, run: u64) -> Result<(), Failure> {
        let Phase::Resident(v) = &mut self.phase else {
            return Err(Failure::Invalid);
        };
        v.sort_unstable_by(|a, b| (&a.key, a.ordinal).cmp(&(&b.key, b.ordinal)));
        let mut w = Writer::create(path(&self.directory, 0, run))?;
        let extra = std::mem::size_of::<Writer>() + w.path.capacity();
        self.measured(extra)?;
        let Phase::Resident(v) = &self.phase else {
            unreachable!()
        };
        for e in v {
            self.spill_bytes += w.entry(e)? as u64;
        }
        w.close()?;
        let Phase::Resident(v) = &mut self.phase else {
            unreachable!()
        };
        v.clear();
        self.runs += 1;
        Ok(())
    }
}
pub(crate) struct Collector {
    core: Core,
    maximum: usize,
    admission_peak: usize,
    budget_flushes: u64,
    boundary_flushes: u64,
}
impl Collector {
    pub(crate) fn new(
        limit: usize,
        directory: &std::path::Path,
        execution: &Arc<Execution>,
    ) -> Result<Self, Failure> {
        let fixed = fixed_owner_bytes();
        if limit < fixed {
            return Err(Failure::Nonfit {
                minimum: fixed,
                limit,
            });
        }
        let floor = Self::headroom(directory, 0)?;
        if floor > limit {
            return Err(Failure::Nonfit {
                minimum: floor,
                limit,
            });
        }
        let ticket = execution
            .reserve_preparation_feature(limit)
            .map_err(|_| Failure::Nonfit {
                minimum: limit,
                limit,
            })?;
        let mut result = Self {
            core: Core {
                phase: Phase::Resident(Vec::new()),
                directory: directory.to_path_buf(),
                _ticket: ticket,
                limit,
                peak: 0,
                units: 0,
                runs: 0,
                spill_bytes: 0,
            },
            maximum: 0,
            admission_peak: floor,
            budget_flushes: 0,
            boundary_flushes: 0,
        };
        result.core.measured(0)?;
        Ok(result)
    }
    fn headroom(directory: &std::path::Path, maximum: usize) -> Result<usize, Failure> {
        let name = directory
            .as_os_str()
            .len()
            .checked_add(48)
            .ok_or(Failure::Invalid)?;
        let temporary = 2 * std::mem::size_of::<Reader>()
            + std::mem::size_of::<Writer>()
            + std::mem::size_of::<Phase>()
            + 2 * std::mem::size_of::<Entry>()
            + 2 * std::mem::size_of::<Vec<u8>>()
            + 2 * std::mem::size_of::<PathBuf>()
            + 2 * std::mem::size_of::<File>()
            + 16 * std::mem::size_of::<usize>();
        fixed_owner_bytes()
            .checked_add(
                std::mem::size_of::<Entry>()
                    .checked_add(maximum)
                    .and_then(|n| n.checked_mul(6))
                    .ok_or(Failure::Invalid)?,
            )
            .and_then(|n| n.checked_add(name.checked_mul(14)?))
            .and_then(|n| n.checked_add(temporary))
            .ok_or(Failure::Invalid)
    }
    pub(crate) fn push(&mut self, parts: &[&str]) -> Result<(), Failure> {
        let length = key_size(parts).ok_or(Failure::Invalid)?;
        if parts.is_empty() || parts.len() > 2 {
            return Err(Failure::Invalid);
        }
        let maximum = self.maximum.max(length);
        let base = Self::headroom(&self.core.directory, maximum)?;
        let minimum = base
            .checked_add(std::mem::size_of::<Entry>() + length)
            .ok_or(Failure::Invalid)?;
        if minimum > self.core.limit {
            return Err(Failure::Nonfit {
                minimum,
                limit: self.core.limit,
            });
        }
        loop {
            let Phase::Resident(v) = &self.core.phase else {
                return Err(Failure::Invalid);
            };
            let old = v.capacity();
            let grow = v.len() == old;
            let target = if grow {
                old.max(1)
                    .checked_mul(if old == 0 { 1 } else { 2 })
                    .ok_or(Failure::Invalid)?
            } else {
                old
            };
            let slots = (if grow {
                old.checked_add(target).ok_or(Failure::Invalid)?
            } else {
                old
            })
            .checked_mul(std::mem::size_of::<Entry>())
            .ok_or(Failure::Invalid)?;
            let keys = v.iter().map(|e| e.key.capacity()).sum::<usize>();
            let forecast = base
                .checked_add(slots)
                .and_then(|n| n.checked_add(keys))
                .and_then(|n| n.checked_add(length))
                .ok_or(Failure::Invalid)?;
            if forecast > self.core.limit {
                if !v.is_empty() {
                    let run = self.core.runs;
                    self.core.flush(run)?;
                    self.budget_flushes += 1;
                    continue;
                }
                if old > 0 {
                    drop(std::mem::replace(
                        &mut self.core.phase,
                        Phase::Resident(Vec::new()),
                    ));
                    continue;
                }
                return Err(Failure::Nonfit {
                    minimum: forecast,
                    limit: self.core.limit,
                });
            }
            self.admission_peak = self.admission_peak.max(forecast);
            if grow {
                let Phase::Resident(v) = &mut self.core.phase else {
                    unreachable!()
                };
                v.try_reserve_exact(target - v.len())
                    .map_err(|_| Failure::Nonfit {
                        minimum: forecast,
                        limit: self.core.limit,
                    })?;
                if v.capacity() != target {
                    return Err(Failure::Invalid);
                }
            }
            let e = Entry {
                key: key(parts, maximum)?,
                ordinal: self.core.units,
            };
            self.core
                .measured(std::mem::size_of::<Entry>() + e.key.capacity())?;
            let Phase::Resident(v) = &mut self.core.phase else {
                unreachable!()
            };
            v.push(e);
            self.core.units = self.core.units.checked_add(1).ok_or(Failure::Invalid)?;
            self.maximum = maximum;
            return Ok(());
        }
    }
    pub(crate) fn boundary(&mut self) -> Result<(), Failure> {
        if matches!(&self.core.phase,Phase::Resident(v)if !v.is_empty()) {
            let run = self.core.runs;
            self.core.flush(run)?;
            self.boundary_flushes += 1;
        }
        Ok(())
    }
    pub(crate) fn finish(self, emit: impl FnMut(u64, bool)) -> Result<Facts, Failure> {
        self.finish_cancellable(emit, || false)
    }
    pub(crate) fn finish_cancellable(
        self,
        mut emit: impl FnMut(u64, bool),
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<Facts, Failure> {
        if cancelled() {
            return Err(Failure::Cancelled);
        }
        let mut c = self.core;
        let maximum = self.maximum;
        let admission_peak = self.admission_peak;
        let budget_flushes = self.budget_flushes;
        let boundary_flushes = self.boundary_flushes;
        if matches!(&c.phase,Phase::Resident(v)if !v.is_empty()) {
            let run = c.runs;
            c.flush(run)?;
        }
        let initial_runs = c.runs;
        let mut count = c.runs;
        let mut pass = 0u64;
        drop(std::mem::replace(&mut c.phase, Phase::Resident(Vec::new())));
        while count > 1 {
            let mut output = 0;
            let mut i = 0;
            while i < count {
                if cancelled() {
                    return Err(Failure::Cancelled);
                }
                let a = Reader::open(path(&c.directory, pass, i), maximum)?;
                let b = if i + 1 < count {
                    Reader::open(path(&c.directory, pass, i + 1), maximum)?
                } else {
                    Reader::empty()
                };
                let w = Writer::create(path(&c.directory, pass + 1, output))?;
                c.measured(
                    std::mem::size_of::<Phase>() + a.dynamic() + b.dynamic() + w.path.capacity(),
                )?;
                c.phase = Phase::Merge {
                    readers: [a, b],
                    writer: w,
                    previous: Vec::new(),
                };
                c.measured(0)?;
                loop {
                    if cancelled() {
                        return Err(Failure::Cancelled);
                    }
                    let Phase::Merge {
                        readers, writer, ..
                    } = &mut c.phase
                    else {
                        unreachable!()
                    };
                    let which = match (&readers[0].head, &readers[1].head) {
                        (None, None) => break,
                        (Some(_), None) => 0,
                        (None, Some(_)) => 1,
                        (Some(a), Some(b)) => {
                            if (&a.key, a.ordinal) <= (&b.key, b.ordinal) {
                                0
                            } else {
                                1
                            }
                        }
                    };
                    let e = readers[which].head.take().unwrap();
                    c.spill_bytes += writer.entry(&e)? as u64;
                    drop(e);
                    readers[which].advance(maximum)?;
                    c.measured(0)?;
                }
                if let Phase::Merge { writer, .. } = &mut c.phase {
                    writer.close()?;
                }
                drop(std::mem::replace(&mut c.phase, Phase::Resident(Vec::new())));
                i += 2;
                output += 1;
            }
            pass += 1;
            count = output;
        }
        let flags = c.directory.join("classification.bin");
        let a = if count == 0 {
            Reader::empty()
        } else {
            Reader::open(path(&c.directory, pass, 0), maximum)?
        };
        let w = Writer::create(flags)?;
        c.measured(std::mem::size_of::<Phase>() + a.dynamic() + w.path.capacity() + maximum)?;
        c.phase = Phase::Merge {
            readers: [a, Reader::empty()],
            writer: w,
            previous: Vec::with_capacity(maximum),
        };
        let mut first = true;
        loop {
            if cancelled() {
                return Err(Failure::Cancelled);
            }
            let Phase::Merge {
                readers,
                writer,
                previous,
            } = &mut c.phase
            else {
                unreachable!()
            };
            let Some(e) = readers[0].head.take() else {
                break;
            };
            if e.ordinal >= c.units {
                return Err(Failure::Invalid);
            }
            let representative = first || *previous != e.key;
            first = false;
            if representative {
                previous.clear();
                previous.extend_from_slice(&e.key);
            }
            use std::io::{Seek, SeekFrom};
            let f = writer.file.as_mut().unwrap();
            f.seek(SeekFrom::Start(e.ordinal))?;
            f.write_all(&[if representative { 1 } else { 2 }])?;
            c.spill_bytes += 1;
            drop(e);
            readers[0].advance(maximum)?;
            c.measured(0)?;
        }
        if let Phase::Merge { writer, .. } = &mut c.phase {
            writer.close()?;
        }
        drop(std::mem::replace(&mut c.phase, Phase::Resident(Vec::new())));
        let mut decisions = File::open(c.directory.join("classification.bin"))?;
        c.measured(std::mem::size_of::<File>() + 1)?;
        let mut byte = [0];
        for ordinal in 0..c.units {
            if cancelled() {
                return Err(Failure::Cancelled);
            }
            decisions.read_exact(&mut byte)?;
            match byte[0] {
                1 => emit(ordinal, true),
                2 => emit(ordinal, false),
                _ => return Err(Failure::Invalid),
            }
        }
        let mut extra = [0];
        if decisions.read(&mut extra)? != 0 {
            return Err(Failure::Invalid);
        }
        drop(decisions);
        let facts = Facts {
            fixed: fixed_owner_bytes(),
            reserved: c.limit,
            proven_upper: admission_peak,
            measured_peak: c.peak,
            runs: initial_runs,
            spill_bytes: c.spill_bytes,
            units: c.units,
            budget_flushes,
            boundary_flushes,
        };
        drop(c);
        Ok(facts)
    }
}

pub(crate) fn group(
    parts: &[Vec<String>],
    limit: usize,
    partitions: usize,
    directory: &std::path::Path,
    execution: &Arc<Execution>,
    mut emit: impl FnMut(u64, bool),
) -> Result<Facts, Failure> {
    if partitions == 0 || partitions > 64 {
        return Err(Failure::Invalid);
    }
    let width = parts.len().div_ceil(partitions).max(1);
    let mut stream = Collector::new(limit, directory, execution)?;
    for (i, p) in parts.iter().enumerate() {
        if p.is_empty() || p.len() > 2 {
            return Err(Failure::Invalid);
        }
        let view = [p[0].as_str(), p.get(1).map_or("", String::as_str)];
        stream.push(&view[..p.len()])?;
        if partitions > 1 && (i + 1) % width == 0 {
            stream.boundary()?;
        }
    }
    stream.finish(&mut emit)
}

#[cfg(test)]
mod probe {
    use super::*;
    #[derive(PartialEq, Debug)]
    struct Record {
        name: String,
        seq: String,
        q: Vec<u8>,
    }
    struct Input {
        units: Vec<Vec<Record>>,
        representatives: Vec<usize>,
        duplicates: Vec<usize>,
        representative_records: Vec<Vec<Record>>,
        duplicate_records: Vec<Vec<Record>>,
    }
    fn record_bytes(v: &Vec<Vec<Record>>) -> usize {
        v.capacity() * std::mem::size_of::<Vec<Record>>()
            + v.iter()
                .map(|u| {
                    u.capacity() * std::mem::size_of::<Record>()
                        + u.iter()
                            .map(|r| r.name.capacity() + r.seq.capacity() + r.q.capacity())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
    #[test]
    fn real_core_control() {
        control("control-spill.json", "small", 1);
        for (fixture, mode) in [("control-inmemory.json", "large"), ("control-spill.json", "adaptive"), ("control-order.json", "adaptive")] {
            for boundary in [1, 17, 64] { control(fixture, mode, boundary); }
        }
    }
    fn control(fixture: &str, mode: &str, partitions: usize) {
        let root = crate::test_support::workspace("grouping").join(format!("{fixture}-{mode}-{partitions}"));
        std::fs::create_dir(&root).unwrap();
        let limit = if mode == "small" {
            256
        } else if mode == "adaptive" {
            8192
        } else {
            1 << 20
        };

        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        let feature = e.reserve_preparation_feature(2 << 20).unwrap();
        let text = std::fs::read(crate::test_support::fixtures().join(fixture)).unwrap();
        assert!(text.len() < 256 << 10);
        let value: serde_json::Value = serde_json::from_slice(&text).unwrap();
        let records = |v: &serde_json::Value| {
            v.as_array()
                .unwrap()
                .iter()
                .map(|u| {
                    u.as_array()
                        .unwrap()
                        .iter()
                        .map(|r| Record {
                            name: r["name"].as_str().unwrap().into(),
                            seq: r["seq"].as_str().unwrap().into(),
                            q: r["q"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|v| v.as_u64().unwrap() as u8)
                                .collect(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let input = Input {
            units: records(&value["units"]),
            representatives: value["representatives"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as usize)
                .collect(),
            duplicates: value["duplicates"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as usize)
                .collect(),
            representative_records: records(&value["representative_records"]),
            duplicate_records: records(&value["duplicate_records"]),
        };
        drop(value);
        assert_eq!(
            (
                input.units.len(),
                input.representatives.len(),
                input.duplicates.len()
            ),
            (343, 257, 86)
        );
        let parts = input
            .units
            .iter()
            .map(|u| u.iter().map(|r| r.seq.clone()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let caller_bytes = text.capacity()
            + record_bytes(&input.units)
            + record_bytes(&input.representative_records)
            + record_bytes(&input.duplicate_records)
            + input.representatives.capacity() * std::mem::size_of::<usize>()
            + input.duplicates.capacity() * std::mem::size_of::<usize>()
            + parts.capacity() * std::mem::size_of::<Vec<String>>()
            + parts
                .iter()
                .map(|v| {
                    v.capacity() * std::mem::size_of::<String>()
                        + v.iter().map(String::capacity).sum::<usize>()
                })
                .sum::<usize>();
        assert!(caller_bytes < 1 << 20);
        let mut classifications = Vec::with_capacity(input.units.len());
        let mut group_during_emit = 0u64;
        let stage = root.join(format!("{mode}-spill"));
        std::fs::create_dir(&stage).unwrap();
        let before = e.facts()["memory_budget"]["current_feature_reserved_bytes"]
            .as_u64()
            .unwrap();
        assert_eq!(before, 2 << 20);
        let result = group(&parts, limit, partitions, &stage, &e, |o, r| {
            classifications.push((o as usize, r));
            if group_during_emit == 0 {
                group_during_emit = e.facts()["memory_budget"]["current_feature_reserved_bytes"]
                    .as_u64()
                    .unwrap()
                    - before;
            }
        });
        let after = e.facts()["memory_budget"]["current_feature_reserved_bytes"]
            .as_u64()
            .unwrap();
        assert_eq!(after, before, "grouping ticket leaked");
        let mut output = serde_json::json!({"mode":mode,"limit":limit,"caller_input_result_reservation":2<<20,"caller_input_capacity_bytes":caller_bytes,"caller_classification_capacity_bytes":classifications.capacity()*std::mem::size_of::<(usize,bool)>(),"fixed_core_bytes":fixed_owner_bytes(),"group_feature_after":after-before,"group_feature_during_emit":group_during_emit,"native_process_calls":0,"real_core_calls":1});
        match result {
            Err(Failure::Nonfit { minimum, limit }) => {
                assert_eq!(mode, "small");
                assert_eq!(minimum, fixed_owner_bytes());
                assert!(minimum > limit);
                assert!(classifications.is_empty());
                assert_eq!(std::fs::read_dir(&stage).unwrap().count(), 0);
                output["status"] = serde_json::json!("PERMANENT_NONFIT");
                output["representation_lower_bound_bytes"] = serde_json::json!(minimum);
                output["minimum_scope"]=serde_json::json!("compiled fixed Core object only; dynamic working minimum is larger, no universal impossibility claim");
            }
            Err(e) => panic!("{e:?}"),
            Ok(f) => {
                assert!(mode == "large" || mode == "adaptive");
                let reps = classifications
                    .iter()
                    .filter(|(_, r)| *r)
                    .map(|(o, _)| *o)
                    .collect::<Vec<_>>();
                let dup = classifications
                    .iter()
                    .filter(|(_, r)| !*r)
                    .map(|(o, _)| *o)
                    .collect::<Vec<_>>();
                assert_eq!(reps, input.representatives);
                assert_eq!(dup, input.duplicates);
                assert_eq!(
                    reps.iter().map(|i| &input.units[*i]).collect::<Vec<_>>(),
                    input.representative_records.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    dup.iter().map(|i| &input.units[*i]).collect::<Vec<_>>(),
                    input.duplicate_records.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    classifications.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
                    (0..343).collect::<Vec<_>>()
                );
                assert_eq!(group_during_emit, limit as u64);
                assert!(
                    f.runs >= 1
                        && f.spill_bytes > 0
                        && f.measured_peak <= f.proven_upper
                        && f.proven_upper <= f.reserved
                );
                if limit == 8192 && partitions == 1 {
                    assert!(f.budget_flushes > 0);
                    assert_eq!(f.boundary_flushes, 0);
                }
                let physical = std::fs::read_dir(&stage)
                    .unwrap()
                    .map(|v| v.unwrap().metadata().unwrap().len())
                    .sum::<u64>();
                assert_eq!(physical, f.spill_bytes);
                output["status"] = serde_json::json!("SCIENTIFIC_CONTROL_PASS");
                output["facts"] = serde_json::json!({"fixed":f.fixed,"proven_owner_upper":f.proven_upper,"measured_named_owner_peak":f.measured_peak,"reserved":f.reserved,"initial_runs":f.runs,"budget_flushes":f.budget_flushes,"boundary_flushes":f.boundary_flushes,"boundary_setting":partitions,"spill_bytes":f.spill_bytes,"retained_physical_spill_bytes":physical,"units":f.units});
                output["representatives"] = serde_json::json!(reps);
                output["duplicates"] = serde_json::json!(dup);
            }
        }
        drop(classifications);
        drop(parts);
        drop(input);
        drop(text);
        drop(feature);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        output["terminal_feature_bytes"] = serde_json::json!(0);
        std::fs::write(
            root.join(format!("{mode}-result.json")),
            serde_json::to_vec_pretty(&output).unwrap(),
        )
        .unwrap();
        println!("{}", output);
    }
}
#[cfg(test)]
mod admission_tests {
    use super::*;
    fn stage(name: &str) -> PathBuf {
        let p = crate::test_support::workspace("direct").join(name);
        std::fs::create_dir(&p).unwrap();
        p
    }
    fn descriptors(p: &std::path::Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|v| std::fs::read_link(v.ok()?.path()).ok())
            .filter(|v| v.starts_with(p))
            .count()
    }
    #[test]
    fn permanent_nonfit_overflow_and_drop() {
        let p = stage("nonfit");
        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        assert!(Collector::headroom(&p, usize::MAX).is_err());
        println!("R84_CORE_ENTER256");
        match Collector::new(256, &p, &e) {
            Err(Failure::Nonfit { minimum, limit }) => {
                assert!(minimum > limit);
                assert_eq!(minimum, fixed_owner_bytes());
            }
            _ => panic!("fixed-owner nonfit not enforced"),
        };
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let caller = e.reserve_preparation_feature(4096).unwrap();
        let input = vec!["A".repeat(1000)];
        println!("R84_CORE_ENTER8192oversize");
        let mut c = Collector::new(8192, &p, &e).unwrap();
        assert!(matches!(
            c.push(&[input[0].as_str()]),
            Err(Failure::Nonfit { .. })
        ));
        drop(c);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            4096
        );
        drop(input);
        drop(caller);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert_eq!(std::fs::read_dir(&p).unwrap().count(), 0);
        assert_eq!(descriptors(&p), 0);
    }
    #[test]
    fn full_lengths_and_tuple_identity() {
        let p = stage("long");
        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        let caller = e.reserve_preparation_feature(1 << 20).unwrap();
        let data = vec![
            vec!["A".repeat(70000)],
            vec!["C".repeat(70000)],
            vec!["a".repeat(70000)],
        ];
        let mut got = Vec::new();
        println!("R84_CORE_ENTERlong");
        let f = group(&data, 1 << 20, 1, &p, &e, |o, r| got.push((o, r))).unwrap();
        assert_eq!(got, vec![(0, true), (1, true), (2, false)]);
        assert!(f.measured_peak <= f.proven_upper && f.proven_upper <= f.reserved);
        assert_eq!(descriptors(&p), 0);
        let q = stage("tuples");
        let data = vec![
            vec!["AC".into(), "G".into()],
            vec!["A".into(), "CG".into()],
            vec!["ac".into(), "g".into()],
        ];
        let mut got = Vec::new();
        println!("R84_CORE_ENTERtuples");
        group(&data, 8192, 1, &q, &e, |o, r| got.push((o, r))).unwrap();
        assert_eq!(got, vec![(0, true), (1, true), (2, false)]);
        assert_eq!(descriptors(&q), 0);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            1 << 20
        );
        drop(caller);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }

    #[test]
    fn persistent_group_owner_rejects_impossible_chunk_without_wait() {
        let p = stage("persistent");
        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        println!("R84_CORE_ENTERpersistent");
        let c = Collector::new(8192, &p, &e).unwrap();
        let facts = e.facts();
        let persistent = facts["memory_budget"]["current_persistent_feature_reserved_bytes"]
            .as_u64()
            .unwrap() as usize;
        assert_eq!(
            persistent, 8192,
            "group owner must participate in permanent-fit admission"
        );
        let available = facts["chunk_capacity_bytes"].as_u64().unwrap() as usize;
        let base = Execution::reservation(0, 0).unwrap();
        let feature = available - persistent + 1 - base;
        assert!(base + feature <= available);
        let child = e.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let result = child.admit_with_feature(0, 0, 0, feature).map(|_| ());
            tx.send(result).unwrap();
        });
        let got = rx.recv_timeout(std::time::Duration::from_millis(200));
        if got.is_err() {
            e.cancel();
        }
        drop(c);
        h.join().unwrap();
        assert!(
            got.is_ok(),
            "self-held grouping storage blocked an impossible chunk"
        );
        assert!(got
            .unwrap()
            .unwrap_err()
            .contains("persistent reservations"));
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        assert_eq!(
            e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"],
            0
        );
        assert_eq!(descriptors(&p), 0);
        assert_eq!(std::fs::read_dir(&p).unwrap().count(), 0);
    }

    #[test]
    fn cancellation_during_merge_releases_group_owners() {
        let p = stage("cancel");
        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        let caller = e.reserve_preparation_feature(4096).unwrap();
        println!("R84_CORE_ENTERcancel");
        let mut c = Collector::new(8192, &p, &e).unwrap();
        c.push(&["ACGT"]).unwrap();
        c.boundary().unwrap();
        c.push(&["TT"]).unwrap();
        c.boundary().unwrap();
        let mut checks = 0;
        let result = c.finish_cancellable(
            |_, _| panic!("cancelled grouping emitted classification"),
            || {
                checks += 1;
                checks >= 4
            },
        );
        assert!(matches!(result, Err(Failure::Cancelled)));
        assert_eq!(checks, 4);
        assert!(!p.join("classification.bin").exists());
        assert_eq!(descriptors(&p), 0);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            4096
        );
        drop(caller);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
    #[test]
    fn real_storage_create_and_write_errors_release() {
        let p = stage("disk-fault");
        let e = Execution::new(1, 1, Arc::new(std::sync::atomic::AtomicBool::new(false))).unwrap();
        let existing = path(&p, 0, 0);
        std::fs::write(&existing, b"preserved sentinel").unwrap();
        println!("R84_CORE_ENTERdiskfault");
        let mut c = Collector::new(8192, &p, &e).unwrap();
        c.push(&["AC"]).unwrap();
        assert!(matches!(c.boundary(), Err(Failure::Io(_))));
        drop(c);
        assert_eq!(std::fs::read(&existing).unwrap(), b"preserved sentinel");
        assert_eq!(descriptors(&p), 0);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
        let ticket = e.reserve_preparation_feature(2048).unwrap();
        let mut w = Writer {
            file: Some(File::open(&existing).unwrap()),
            path: existing.clone(),
            header: [0; 12],
        };
        let entry = Entry {
            key: key(&["AC"], 11).unwrap(),
            ordinal: 0,
        };
        assert!(
            std::mem::size_of::<Writer>()
                + w.path.capacity()
                + std::mem::size_of::<Entry>()
                + entry.key.capacity()
                < 2048
        );
        assert!(matches!(w.entry(&entry), Err(Failure::Io(_))));
        drop(entry);
        drop(w);
        drop(ticket);
        assert_eq!(std::fs::read(&existing).unwrap(), b"preserved sentinel");
        assert_eq!(descriptors(&p), 0);
        assert_eq!(
            e.facts()["memory_budget"]["current_feature_reserved_bytes"],
            0
        );
    }
}
