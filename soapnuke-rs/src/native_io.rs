use std::fs::File;
use std::io::{self, Read};

pub(crate) enum InputReader { File(File), Owned(Box<dyn Read + Send>) }
impl Read for InputReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self { Self::File(file) => file.read(bytes), Self::Owned(reader) => reader.read(bytes) }
    }
}

use crate::job_api::{CancellationToken, Format, InputDecl, InputHandle, JobIo, JobSpec, Location, ReportWriter};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

#[derive(Clone,Copy,Debug,PartialEq)]
pub(crate) enum InputCapability { PathFile, OwnedStream, OwnedSeekable, UnavailablePath }
pub(crate) struct OwnedIo {
    capabilities: Vec<Vec<InputCapability>>,
    owned_outputs: Vec<bool>,
    pub spec: JobSpec,
    pub cancel: CancellationToken,
    pub execution: std::sync::Arc<crate::native_execution::Execution>,
    pub stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    inputs: Mutex<BTreeMap<String, InputHandle>>,
    outputs: Mutex<BTreeMap<String, Box<dyn ReportWriter>>>,
    pub bytes: Mutex<BTreeMap<String,u64>>,
    closed: Mutex<bool>,
    staged_files:Mutex<Vec<(String,std::path::PathBuf)>>,
    demux_outputs:std::sync::atomic::AtomicBool,
    _metadata: crate::native_execution::FeatureTicket,

}
struct CancelReader { reader: InputHandle, cancel: CancellationToken }
impl Read for CancelReader {
    fn read(&mut self, out:&mut[u8])->io::Result<usize>{
        if self.cancel.is_cancelled(){return Err(io::Error::other("API cancelled"));}
        self.reader.read(out)
    }
}
struct FileWriter(File);
impl io::Write for FileWriter {
    fn write(&mut self,b:&[u8])->io::Result<usize>{self.0.write(b)}
    fn flush(&mut self)->io::Result<()>{self.0.flush()}
}
impl ReportWriter for FileWriter {fn close(&mut self)->io::Result<()>{self.0.sync_all()}}
use std::io::Write;
impl OwnedIo {
    pub fn new(spec:JobSpec, io:JobIo, cancel:CancellationToken, execution:std::sync::Arc<crate::native_execution::Execution>, stop:std::sync::Arc<std::sync::atomic::AtomicBool>, reservation:crate::native_execution::FeatureTicket)->Self{
        let capabilities=spec.lanes.iter().map(|l|l.inputs.iter().map(|d|{
            let key=match &d.location{Location::Handle(h)=>h,_=>&d.channel};
            match io.inputs.get(key){Some(InputHandle::Stream(_))=>InputCapability::OwnedStream,Some(InputHandle::Seekable(_))=>InputCapability::OwnedSeekable,None=>match &d.location{Location::Path(p)if d.seekable&&p.is_file()=>InputCapability::PathFile,_=>InputCapability::UnavailablePath}}
        }).collect()).collect();
        let owned_outputs=spec.outputs.iter().map(|d|io.outputs.contains_key(match &d.location{Location::Handle(h)=>h,_=>&d.channel})).collect();
        Self{spec,capabilities,owned_outputs,cancel,execution,stop,_metadata:reservation,inputs:Mutex::new(io.inputs),outputs:Mutex::new(io.outputs),bytes:Mutex::new(BTreeMap::new()),closed:Mutex::new(false),staged_files:Mutex::new(Vec::new()),demux_outputs:std::sync::atomic::AtomicBool::new(false)}
    }
    pub fn reader(&self, decl:&InputDecl)->io::Result<Box<dyn Read+Send>>{
        let value=self.inputs.lock().unwrap().remove(match &decl.location{Location::Handle(h)=>h,_=>&decl.channel});
        let reader=match (value,&decl.location){
            (Some(r),_)=>r,
            (None,Location::Path(p))=>InputHandle::Seekable(Box::new(File::open(p)?)),
            (None,Location::Handle(_))=>return Err(io::Error::other("missing input binding")),
        };
        Ok(Box::new(CancelReader{reader,cancel:self.cancel.clone()}))
    }
    pub fn prepare(&self,stage:&Path,names:&[(String,String)])->io::Result<()>{
        let mut outputs=self.outputs.lock().unwrap();
        for (channel,name)in names{
            let key=self.spec.outputs.iter().find(|d|&d.channel==channel).map(|d|match &d.location{Location::Handle(h)=>h.as_str(),_=>d.channel.as_str()}).unwrap_or(channel);
            if !outputs.contains_key(key){
                let decl=self.spec.outputs.iter().find(|d|&d.channel==channel).ok_or_else(||io::Error::other("undeclared output"))?;
                if matches!(decl.location,Location::Handle(_)){return Err(io::Error::other("missing output binding"));}
                let f=std::fs::OpenOptions::new().write(true).create_new(true).open(stage.join(name))?;
                outputs.insert(key.to_string(),Box::new(FileWriter(f)));
            }
        }
        Ok(())
    }
    pub fn prepare_demux(&self,stage:&Path,names:&[(String,String)])->io::Result<()>{
        self.demux_outputs.store(true,std::sync::atomic::Ordering::Relaxed);
        let mut files=self.staged_files.lock().unwrap();files.try_reserve_exact(names.len()).map_err(io::Error::other)?;
        for(channel,name)in names{if self.owned_output_key(channel).is_none(){let path=stage.join(name);if channel.starts_with("sample/"){let file=std::fs::OpenOptions::new().write(true).create_new(true).open(&path)?;crate::checked_io::close_file(file).map_err(io::Error::other)?;}files.push((channel.clone(),path));}}Ok(())
    }
    pub fn owned_output_key(&self,channel:&str)->Option<&str>{
        self.spec.outputs.iter().enumerate().find(|(_,d)|(if channel.starts_with("sample/"){d.channel==channel}else{d.channel.eq_ignore_ascii_case(channel)})||crate::job_api::main_channel(&d.channel).is_some_and(|n|Some(n)==crate::job_api::main_channel(channel))).and_then(|(i,d)|if self.owned_outputs[i]{Some(match &d.location{Location::Handle(h)=>h.as_str(),_=>d.channel.as_str()})}else{None})
    }
    pub fn write(&self,channel:&str,bytes:&[u8])->io::Result<()>{
        let mut outputs=self.outputs.lock().unwrap();
        let decl=self.spec.outputs.iter().find(|d|(if channel.starts_with("sample/"){d.channel==channel}else{d.channel.eq_ignore_ascii_case(channel)})||crate::job_api::main_channel(&d.channel).is_some_and(|n|Some(n)==crate::job_api::main_channel(channel))).ok_or_else(||io::Error::other("undeclared output channel"))?;
        let key=match &decl.location{Location::Handle(h)=>h,_=>&decl.channel};
        if let Some(writer)=outputs.get_mut(key){writer.write_all(bytes)?;}else{
            drop(outputs);let path=self.staged_files.lock().unwrap().iter().find(|(c,_)|c==&decl.channel).map(|(_,p)|p.clone()).ok_or_else(||io::Error::other("missing output channel"))?;let mut file=std::fs::OpenOptions::new().append(true).create(true).open(path)?;file.write_all(bytes)?;crate::checked_io::close_file(file).map_err(io::Error::other)?;
        }
        *self.bytes.lock().unwrap().entry(decl.channel.clone()).or_default()+=bytes.len()as u64;
        Ok(())
    }
    pub fn finish_completed(&self,level:i32,split:bool)->Result<(),String>{
        for d in &self.spec.outputs {
            if split&&crate::job_api::main_channel(&d.channel).is_some(){continue;}
            if self.demux_outputs.load(std::sync::atomic::Ordering::Relaxed) && !d.channel.starts_with("sample/") && !self.bytes.lock().unwrap().contains_key(&d.channel){continue;}
            if crate::job_api::output_codec(d)=="gzip" && !self.bytes.lock().unwrap().contains_key(&d.channel) {
                let bytes=crate::ffi::Compressor::new(level)?.gzip_compress_alloc(&[]);
                if bytes.is_empty(){return Err("empty owned gzip failed".into());}
                self.write(&d.channel,&bytes).map_err(|e|e.to_string())?;
            }
        }
        self.finish().map_err(|e|e.to_string())
    }
    pub fn finish(&self)->io::Result<()>{
        let mut closed=self.closed.lock().unwrap();if *closed{return Ok(());}*closed=true;
        let mut error=None;let mut outputs=self.outputs.lock().unwrap();
        for writer in outputs.values_mut(){if let Err(e)=writer.flush(){if error.is_none(){error=Some(e);}}if let Err(e)=writer.close(){if error.is_none(){error=Some(e);}}}
        if self.demux_outputs.load(std::sync::atomic::Ordering::Relaxed){for(_,path)in self.staged_files.lock().unwrap().iter(){if path.is_file(){match std::fs::OpenOptions::new().write(true).open(path){Ok(file)=>{if let Err(e)=file.sync_all(){if error.is_none(){error=Some(e);}}if let Err(e)=crate::checked_io::close_file(file){if error.is_none(){error=Some(io::Error::other(e));}}},Err(e)=>{if error.is_none(){error=Some(e);}}}}}}
        match error{Some(e)=>Err(e),None=>Ok(())}
    }
    pub fn output_phred(&self,channel:&str)->i32{self.spec.outputs.iter().find(|d|(if channel.starts_with("sample/"){d.channel==channel}else{d.channel.eq_ignore_ascii_case(channel)})).map_or(33,|d|d.phred as i32)}
}
impl Drop for OwnedIo{fn drop(&mut self){let _=self.finish();}}

impl OwnedIo {
    pub fn has_override(&self,d:&InputDecl)->bool{self.inputs.lock().unwrap().contains_key(match &d.location{Location::Handle(h)=>h,_=>&d.channel})}
    pub fn choice(&self,threads:usize)->crate::source::DecoderChoice{
        let requested=self.spec.supplied.get("decoder").or_else(||self.spec.configured.get("decoder")).and_then(|v|if let crate::job_api::Setting::Text(s)=v{Some(s.as_str())}else{None}).unwrap_or("auto");
        let path_gzip=self.spec.lanes.iter().enumerate().any(|(i,l)|l.inputs.iter().enumerate().any(|(j,d)|self.capabilities[i][j]==InputCapability::PathFile&&crate::job_api::input_codec(d)=="gzip"));
        crate::source::DecoderChoice{backend:if requested=="rapidgzip" || (requested=="auto"&&threads>=14&&path_gzip){crate::source::InputBackend::Rapidgzip}else{crate::source::InputBackend::Zlib},threads_per_lane:1,reserved_credits:0}
    }
}

impl OwnedIo {
    pub(crate) fn input_capability(&self,lane:usize,input:usize)->InputCapability{self.capabilities[lane][input]}
    pub fn unsupported_rapid(&self)->Option<InputCapability>{self.capabilities.iter().flatten().copied().find(|c|*c!=InputCapability::PathFile)}
    pub fn decoder_bindings(&self,threads:usize)->serde_json::Value {
        let choice=self.choice(threads);
        let requested=self.spec.supplied.get("decoder").or_else(||self.spec.configured.get("decoder")).and_then(|v|if let crate::job_api::Setting::Text(s)=v{Some(s.as_str())}else{None}).unwrap_or("auto");
        serde_json::json!({"rapidgzip_interface":"seekable regular path with no owned override; opaque ReadSeek is not a FileReader ABI","bindings":self.spec.lanes.iter().enumerate().map(|(i,l)|serde_json::json!({"lane":l.id,"inputs":l.inputs.iter().enumerate().map(|(j,d)|{
            let cap=self.capabilities[i][j];let gzip=crate::job_api::input_codec(d)=="gzip";
            serde_json::json!({"channel":d.channel,"declared_seekable":d.seekable,"actual_binding_capability":format!("{cap:?}"),"actual_seekable":match cap{InputCapability::PathFile|InputCapability::OwnedSeekable=>Some(true),InputCapability::OwnedStream=>Some(false),InputCapability::UnavailablePath=>None},"requested_decoder":requested,"selected_backend":if requested=="rapidgzip"&&cap!=InputCapability::PathFile{None}else{Some(if !gzip{"plain"}else if cap==InputCapability::PathFile&&choice.backend==crate::source::InputBackend::Rapidgzip{"rapidgzip"}else{"zlib"})},"selection_reason":if matches!(cap,InputCapability::OwnedStream|InputCapability::OwnedSeekable){"opaque owned IO uses existing plain/zlib interface; explicit rapidgzip is rejected before Read/Seek"}else{"native regular-path decoder policy"}})
        }).collect::<Vec<_>>()})).collect::<Vec<_>>(),"scope":"binding and selection at run start; actual opened backends appear in execution.decoders"})
    }
}
