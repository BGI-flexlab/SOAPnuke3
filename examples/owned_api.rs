extern crate soapnuke_api;
use soapnuke_api::*;
use std::collections::BTreeMap;
use std::io::{self,Read,Cursor,Write};
use std::path::PathBuf;
use std::sync::{Arc,Mutex};
struct Reader(Cursor<Vec<u8>>,Arc<Mutex<usize>>);
impl Read for Reader {fn read(&mut self,b:&mut[u8])->io::Result<usize>{*self.1.lock().unwrap()+=1;self.0.read(b)}}
struct Sink(Arc<Mutex<Vec<u8>>>,Arc<Mutex<(usize,usize)>>);
impl Write for Sink{fn write(&mut self,b:&[u8])->io::Result<usize>{self.0.lock().unwrap().extend_from_slice(b);Ok(b.len())}fn flush(&mut self)->io::Result<()>{Ok(())}}
impl ReportWriter for Sink{fn close(&mut self)->io::Result<()>{self.1.lock().unwrap().0+=1;Ok(())}}
impl Drop for Sink{fn drop(&mut self){self.1.lock().unwrap().1+=1;}}
fn main(){
 let args:Vec<_>=std::env::args().collect();let name=&args[1];let root=PathBuf::from(&args[2]);let fixture=PathBuf::from(&args[3]);
 let paired=name=="merge-mixed"||name=="correct-mixed";let merging=name=="merge-mixed";
 let mut supplied=BTreeMap::from([("sample-id".into(),Setting::Text("r93-api".into())),("threads".into(),Setting::Int(2))]);
 if paired{supplied.extend([("overlap-min".into(),Setting::Int(4)),("overlap-max-error".into(),Setting::Float(0.)),("correct-overlap".into(),Setting::Bool(true))]);if merging{supplied.insert("merge".into(),Setting::Bool(true));supplied.insert("unmerged-mode".into(),Setting::Text("discard".into()));}}
 let mut resolver=ResourceResolver::default();
 if name.starts_with("reference") {supplied.extend([("screen-method".into(),Setting::Text("kmer-exact".into())),("screen-k".into(),Setting::Int(3)),("screen-min-hits".into(),Setting::Int(1)),("screen-reference".into(),Setting::List(vec![if name=="reference-id"{"id:ref-lower".into()}else{fixture.join(if name=="reference-ambiguity"{"ref-ambiguous.fa"}else{"ref-lower.fa"}).to_str().unwrap().into()}]))]);}
 if name.starts_with("adapter") {supplied.extend([("adapter-file".into(),Setting::List(vec![if name=="adapter-lower-id"{"id:adapter-lower".into()}else if name=="adapter-N"{"id:adapter-N".into()}else{fixture.join("adapter-n.fa").to_str().unwrap().into()}])),("adapter-min-overlap".into(),Setting::Int(4)),("adapter-max-error".into(),Setting::Float(0.))]);}
 if name.ends_with("-id")||name=="adapter-N"{resolver.catalogue=Some(fixture.join("catalogue-expanded.json"));}
 let readcalls=Arc::new(Mutex::new(0usize));let mut readers=BTreeMap::new();let mut inputs=vec![];
 for i in 0..if paired{2}else{1}{let ch=if i==0{"R1"}else{"R2"};let seq=if paired{if i==0{"AcGt"}else{"ACGT"}}else{"ttt"};let phred=if i==0{33}else{64};let q=if i==0{40}else{20};let data=format!("@case/{}\n{seq}\n+\n{}\n",i+1,char::from(phred+q).to_string().repeat(seq.len())).into_bytes();readers.insert(ch.into(),InputHandle::Stream(Box::new(Reader(Cursor::new(data),readcalls.clone()))));inputs.push(InputDecl{channel:ch.into(),location:Location::Handle(ch.into()),format:Format::Fastq,phred,seekable:false});}
 let channels=if merging{vec![("merged-out",64)]}else if paired{vec![("R1",33),("R2",64)]}else{vec![("R1",33)]};let mut writers:BTreeMap<String,Box<dyn ReportWriter>>=BTreeMap::new();let mut outputs=vec![];let mut captures=vec![];
 for(ch,p)in channels {let bytes=Arc::new(Mutex::new(Vec::new()));let life=Arc::new(Mutex::new((0,0)));writers.insert(ch.into(),Box::new(Sink(bytes.clone(),life.clone())));outputs.push(OutputDecl{channel:ch.into(),location:Location::Handle(ch.into()),format:Format::Fastq,phred:p});captures.push((ch,bytes,life));}
 let config=JobConfig::Process(JobSpec{configured:BTreeMap::new(),supplied,layout:if paired{Layout::SplitPe}else{Layout::Se},lanes:vec![LaneDecl{id:"L1".into(),inputs}],outputs,json_report:None});
 eprintln!("API_VALIDATE_ENTER");let job=validate(config,resolver);assert_eq!(*readcalls.lock().unwrap(),0);
 if name=="adapter-N"||name=="adapter-n" {let error=job.unwrap_err();std::fs::write(root.join("validation-error.txt"),format!("{}\t{}\n",error.code,error.field)).unwrap();assert!(matches!(error.code,"resource-fasta"|"config"));return;}
 let job=job.unwrap();
 assert!(matches!(job.configuration(),JobConfig::Process(_)));
 assert!(matches!(job.setting("threads"),Some(Setting::Int(2))));
 assert!(std::ptr::eq(job.setting("threads").unwrap(),job.effective_settings().get("threads").unwrap()));
 assert_eq!(job.provenance().get("threads"),Some(&"api"));
 let ids=job.resolved_resources().iter().map(|r|format!("{}\t{}\t{}\n",r.catalogue_id.as_deref().unwrap_or("-"),r.kind,r.sha256)).collect::<String>();std::fs::write(root.join("validated-identities.txt"),ids).unwrap();
 eprintln!("API_RUN_ENTER");let result=run(job,JobIo{inputs:readers,outputs:writers},None,&CancellationToken::default()).unwrap();assert_eq!(result.report.status,"complete");assert_eq!((result.report.input_units,result.report.accepted_units,result.report.written_units),(1,Some(1),Some(1)));assert!(*readcalls.lock().unwrap()>0);
 std::fs::write(root.join("report.json"),&result.report.json).unwrap();
 for(ch,bytes,life)in captures {assert_eq!(*life.lock().unwrap(),(1,1));std::fs::write(root.join(format!("{ch}.fq")),&*bytes.lock().unwrap()).unwrap();}
 std::fs::write(root.join("result.txt"),format!("complete\t1\t1\t1\treads={}\tclose_drop=1,1\n",*readcalls.lock().unwrap())).unwrap();
}
