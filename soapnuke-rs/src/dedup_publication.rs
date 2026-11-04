use crate::native_execution::{Execution,FeatureTicket};
use std::{fs::{self,File,OpenOptions,Metadata},io::Write,path::{Path,PathBuf},sync::Arc,os::unix::fs::MetadataExt};

#[derive(Clone,Copy,PartialEq,Eq)]
struct Identity{device:u64,inode:u64}
impl Identity{fn of(m:Metadata)->Self{Self{device:m.dev(),inode:m.ino()}}}
#[derive(Clone,Copy,Debug)]
pub(crate) struct Error{pub stage:&'static str,pub cleanup_failed:bool,pub cleanup_stage:Option<&'static str>}
pub(crate) struct Publication<'a>{
    target:&'a Path,
    stage:PathBuf,
    stage_owner:Option<Identity>,
    claim_owner:Option<Identity>,
    stage_created:bool,
    claim_created:bool,
    finished:bool,
    last_error:Option<Error>,
    _ticket:FeatureTicket,
}
#[cfg(test)]
thread_local!{pub(crate) static FAULT:std::cell::Cell<u8>=const{std::cell::Cell::new(0)};pub(crate) static INSPECTION_ERROR:std::cell::Cell<bool>=const{std::cell::Cell::new(false)};}
impl<'a> Publication<'a>{
 pub(crate) fn new(target:&'a Path,e:&Arc<Execution>)->Result<Self,String>{
    use std::os::unix::ffi::OsStrExt;
    let path=target.as_os_str().as_bytes().len();
    // Two live paths, join growth/filename, and the two std IO path conversions.
    let bound=path.checked_add(128).and_then(|n|n.checked_mul(8)).and_then(|n|n.checked_add(std::mem::size_of::<Self>()+512)).ok_or("report publication path overflow")?;
    let ticket=e.reserve_preparation_feature(bound).map_err(|e| e.to_string())?;
    let stage=target.parent().unwrap_or(Path::new(".")).join(format!(".soapnuke-dedup-report-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|"report stage clock")?.as_nanos()));
    if stage.capacity()>2*(path+128){return Err("report stage path capacity exceeds reservation".into());}
    Ok(Self{target,stage,stage_owner:None,claim_owner:None,stage_created:false,claim_created:false,finished:false,last_error:None,_ticket:ticket})
 }
 fn ownership(path:&Path,owner:Identity)->std::io::Result<bool>{
    #[cfg(test)] if INSPECTION_ERROR.with(|v|v.get()){return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));}
    match fs::symlink_metadata(path){Ok(m)=>Ok(m.is_file()&&Identity::of(m)==owner),Err(e)if e.kind()==std::io::ErrorKind::NotFound=>Ok(false),Err(e)=>Err(e)}
 }
 fn remove_owned(path:&Path,owner:&mut Option<Identity>,inspection:&'static str,unlink:&'static str)->Result<(),&'static str>{
    if let Some(id)=owner.take(){if Self::ownership(path,id).map_err(|_|inspection)?{fs::remove_file(path).map_err(|_|unlink)?;}}
    Ok(())
 }
 fn abort(&mut self)->Option<&'static str>{
    if self.finished{return self.last_error.and_then(|e|e.cleanup_stage);}
    self.finished=true;
    let unknown=if self.stage_created&&self.stage_owner.is_none(){Some("stage-identity-unavailable")}else if self.claim_created&&self.claim_owner.is_none(){Some("claim-identity-unavailable")}else{None};
    let stage=Self::remove_owned(&self.stage,&mut self.stage_owner,"stage-inspection","stage-unlink");
    let claim=Self::remove_owned(self.target,&mut self.claim_owner,"claim-inspection","claim-unlink");
    unknown.or(stage.err()).or(claim.err())
 }
 pub(crate) fn can_attempt(&self)->bool{!self.finished}
 pub(crate) fn failed(&self)->bool{self.last_error.is_some()}
 pub(crate) fn cleanup_stage(&self)->Option<&'static str>{self.last_error.and_then(|e|e.cleanup_stage)}
 pub(crate) fn commit(&mut self,text:&str)->Result<(),Error>{
    if self.finished{return Err(Error{stage:"publication-attempt-finished",cleanup_failed:false,cleanup_stage:None});}
    let result=(||->Result<(),&'static str>{
      let mut file=OpenOptions::new().write(true).create_new(true).open(&self.stage).map_err(|_|"stage-create")?;
      self.stage_created=true;
      self.stage_owner=Some(Identity::of(file.metadata().map_err(|_|"stage-identity")?));
      file.write_all(text.as_bytes()).map_err(|_|"stage-write")?;file.sync_all().map_err(|_|"stage-sync")?;
      crate::checked_io::close_file(file).map_err(|_|"stage-close")?;
      let mut claim=OpenOptions::new().write(true).create_new(true).open(self.target).map_err(|_|"claim-existing-or-unavailable")?;
      self.claim_created=true;
      self.claim_owner=Some(Identity::of(claim.metadata().map_err(|_|"claim-identity")?));
      claim.write_all(b"SOAPnuke report publication incomplete\n").map_err(|_|"claim-write")?;claim.sync_all().map_err(|_|"claim-sync")?;
      crate::checked_io::close_file(claim).map_err(|_|"claim-close")?;
      #[cfg(test)] let _pin=if FAULT.with(|v|v.get()==1){
          let pin=File::open(self.target).map_err(|_|"test-pin")?;fs::remove_file(self.target).map_err(|_|"test-replace")?;fs::write(self.target,b"foreign replacement").map_err(|_|"test-replace")?;Some(pin)
      }else{None};
      if !self.stage_owner.map_or(Ok(false),|id|Self::ownership(&self.stage,id)).map_err(|_|"stage-inspection")?{return Err("stage-ownership-lost");}
      if !self.claim_owner.map_or(Ok(false),|id|Self::ownership(self.target,id)).map_err(|_|"claim-inspection")?{return Err("claim-ownership-lost");}
      #[cfg(test)] if FAULT.with(|v|v.get()==2){fs::remove_file(&self.stage).map_err(|_|"test-stage-remove")?;}
      #[cfg(test)] if FAULT.with(|v|v.get()==3){INSPECTION_ERROR.with(|v|v.set(true));return Err("commit-injected");}
      // This replaces our exclusive reservation, not an arbitrary existing destination.
      // An external replacement after the checks is outside this ownership contract.
      fs::rename(&self.stage,self.target).map_err(|_|"commit")?;
      self.stage_owner=None;self.claim_owner=None;self.finished=true;Ok(())
    })();
    result.map_err(|stage|{let cleanup_stage=self.abort();let err=Error{stage,cleanup_failed:cleanup_stage.is_some(),cleanup_stage};self.last_error=Some(err);err})
 }
}
impl Drop for Publication<'_>{fn drop(&mut self){self.abort();}}

#[cfg(test)]
pub(crate) fn proof(root:&Path){
 use std::sync::atomic::AtomicBool;
 fs::create_dir(root).unwrap();let e=Execution::new(1,1,Arc::new(AtomicBool::new(false))).unwrap();let caller=e.reserve_preparation_feature(1<<20).unwrap();
 let expected=b"{\"status\":\"complete\",\"literal\":1}\n";let mut rows=Vec::new();
 for (name,mode) in [("absent",0),("existing",0),("lost",1),("commit-failure",2)]{
  let target=root.join(name);if name=="existing"{fs::write(&target,b"existing bytes").unwrap();}
  let mut p=Publication::new(&target,&e).unwrap();let stage=p.stage.clone();FAULT.with(|v|v.set(mode));let result=p.commit(std::str::from_utf8(expected).unwrap());FAULT.with(|v|v.set(0));
  match name{
   "absent"=>{assert!(result.is_ok());assert_eq!(fs::read(&target).unwrap(),expected);},
   "existing"=>{assert_eq!(result.unwrap_err().stage,"claim-existing-or-unavailable");assert_eq!(fs::read(&target).unwrap(),b"existing bytes");},
   "lost"=>{assert_eq!(result.unwrap_err().stage,"claim-ownership-lost");assert_eq!(fs::read(&target).unwrap(),b"foreign replacement");},
   _=>{assert_eq!(result.unwrap_err().stage,"commit");assert!(!target.exists());},
  }
  assert!(!stage.exists());rows.push((name,result.is_ok()));drop(p);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],1<<20);
 }
 fs::write(root.join("receipt.json"),serde_json::to_string(&rows).unwrap()).unwrap();drop(caller);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
}

#[cfg(test)]pub(crate) fn r4_inspection_proof(root:&Path){
 use std::sync::atomic::AtomicBool;fs::create_dir(root).unwrap();let e=Execution::new(1,1,Arc::new(AtomicBool::new(false))).unwrap();let caller=e.reserve_preparation_feature(1<<20).unwrap();let mut rows=Vec::new();
 {let target=root.join("uninspectable.json");let mut p=Publication::new(&target,&e).unwrap();FAULT.with(|v|v.set(3));let err=p.commit("{\"status\":\"complete\"}").unwrap_err();FAULT.with(|v|v.set(0));INSPECTION_ERROR.with(|v|v.set(false));assert!(err.cleanup_failed);assert_eq!(err.cleanup_stage,Some("stage-inspection"));assert_eq!(fs::read(&target).unwrap(),b"SOAPnuke report publication incomplete\n");assert_eq!(fs::read(&p.stage).unwrap(),b"{\"status\":\"complete\"}");rows.push(("inspection-error",err.cleanup_stage));}
 for foreign in [false,true]{let target=root.join(if foreign{"foreign"}else{"absent"});let mut p=Publication::new(&target,&e).unwrap();let f=OpenOptions::new().read(true).write(true).create_new(true).open(&p.stage).unwrap();p.stage_created=true;p.stage_owner=Some(Identity::of(f.metadata().unwrap()));fs::remove_file(&p.stage).unwrap();if foreign{fs::write(&p.stage,b"foreign bytes").unwrap();}assert_eq!(p.abort(),None);if foreign{assert_eq!(fs::read(&p.stage).unwrap(),b"foreign bytes");}else{assert!(!p.stage.exists());}drop(f);rows.push((if foreign{"foreign"}else{"absent"},None));}
 fs::write(root.join("receipt.json"),serde_json::to_string(&rows).unwrap()).unwrap();assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],1<<20);drop(caller);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
}
