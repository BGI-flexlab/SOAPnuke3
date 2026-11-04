use std::collections::BTreeMap;
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq)]
pub enum Layout { Se, SplitPe, InterleavedPe }
#[derive(Clone, Debug, PartialEq)]
pub enum Format { Auto, Fastq, Gzip }
#[derive(Clone, Debug, PartialEq)]
pub enum Location { Path(PathBuf), Handle(String) }
#[derive(Clone, Debug, PartialEq)]
pub struct InputDecl { pub channel: String, pub location: Location, pub format: Format, pub phred: u8, pub seekable: bool }
#[derive(Clone, Debug, PartialEq)]
pub struct OutputDecl { pub channel: String, pub location: Location, pub format: Format, pub phred: u8 }
#[derive(Clone, Debug, PartialEq)]
pub struct LaneDecl { pub id: String, pub inputs: Vec<InputDecl> }
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}
pub enum InputHandle { Stream(Box<dyn Read + Send>), Seekable(Box<dyn ReadSeek + Send>) }
impl Read for InputHandle {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        match self { Self::Stream(r) => r.read(b), Self::Seekable(r) => r.read(b) }
    }
}
pub trait ReportWriter: Write + Send { fn close(&mut self) -> io::Result<()>; }
pub struct JobIo { pub inputs: BTreeMap<String, InputHandle>, pub outputs: BTreeMap<String, Box<dyn ReportWriter>> }
