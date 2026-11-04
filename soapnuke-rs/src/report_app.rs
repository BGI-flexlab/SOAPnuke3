use crate::job_api::ReportFormat;
use serde::de::{Deserialize,DeserializeSeed,MapAccess,Visitor};
use serde_json::value::RawValue;
use std::{collections::BTreeMap,fmt,fs::{self,OpenOptions},io::{Read,Write,BufWriter},path::{Path,PathBuf}};

pub(crate) struct Working{execution:std::sync::Arc<crate::native_execution::Execution>,_ticket:crate::native_execution::FeatureTicket,documents:std::cell::Cell<usize>}
impl Working{pub(crate) fn new()->Result<std::sync::Arc<Self>,String>{let execution=crate::native_execution::Execution::new(1,1,std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false))).map_err(|e|e.to_string())?;let ticket=execution.reserve_preparation_feature(128*1024*1024).map_err(|e| e.to_string())?;Ok(std::sync::Arc::new(Self{execution,_ticket:ticket,documents:std::cell::Cell::new(0)}))}}
pub(crate) const DOCUMENT_LIMIT:usize=4*1024*1024;
const SCALAR_LIMIT:usize=64*1024;
fn deserializer(bytes:&[u8])->Result<serde_json::Deserializer<serde_json::de::SliceRead<'_>>,String>{
    if bytes.len()>DOCUMENT_LIMIT{return Err("report document exceeds 4MiB limit".into());}
    let mut d=serde_json::Deserializer::from_slice(bytes);
    d.soapnuke_reserve_scratch(bytes.len()).map_err(|e|e.to_string())?;
    Ok(d)
}
struct IsKey<'a>(&'a str);
impl<'de> DeserializeSeed<'de> for IsKey<'_>{type Value=bool;fn deserialize<D:serde::Deserializer<'de>>(self,d:D)->Result<bool,D::Error>{struct K<'a>(&'a str);impl Visitor<'_> for K<'_>{type Value=bool;fn expecting(&self,f:&mut fmt::Formatter)->fmt::Result{f.write_str("object key")}fn visit_str<E:serde::de::Error>(self,v:&str)->Result<bool,E>{Ok(v==self.0)}}d.deserialize_str(K(self.0))}}
struct Member<'a>(&'a str);
impl<'de> Visitor<'de> for Member<'_>{type Value=Option<&'de RawValue>;fn expecting(&self,f:&mut fmt::Formatter)->fmt::Result{f.write_str("JSON object")}fn visit_map<M:MapAccess<'de>>(self,mut m:M)->Result<Self::Value,M::Error>{let mut found=None;while let Some(matches)=m.next_key_seed(IsKey(self.0))?{let v=m.next_value::<&'de RawValue>()?;if matches{if found.is_some(){return Err(serde::de::Error::custom("duplicate selected report key"));}found=Some(v);}}Ok(found)}}
pub(crate) struct Document<'a>{raw:&'a RawValue,original:&'a [u8],pub(crate) working:std::sync::Arc<Working>,root:bool}
impl Drop for Document<'_>{fn drop(&mut self){if self.root{self.working.documents.set(self.working.documents.get()-1);}}}
impl<'a> Document<'a>{
    pub(crate) fn child(&self,raw:&'a RawValue)->Document<'a>{Document{raw,original:raw.get().as_bytes(),working:self.working.clone(),root:false}}
    pub(crate) fn each<F:FnMut(&'a RawValue)->Result<(),String>>(&self,path:&[&str],mut f:F)->Result<(),String>{
      struct Items<'f,F>(&'f mut F);impl<'de,F:FnMut(&'de RawValue)->Result<(),String>> Visitor<'de> for Items<'_,F>{type Value=();fn expecting(&self,f:&mut fmt::Formatter)->fmt::Result{f.write_str("JSON array")}fn visit_seq<S:serde::de::SeqAccess<'de>>(self,mut a:S)->Result<(),S::Error>{while let Some(raw)=a.next_element::<&'de RawValue>()?{(self.0)(raw).map_err(serde::de::Error::custom)?;}Ok(())}}
      let raw=self.get(path)?.ok_or("missing array")?;let mut d=deserializer(raw.get().as_bytes())?;serde::de::Deserializer::deserialize_seq(&mut d,Items(&mut f)).map_err(|e|e.to_string())?;d.end().map_err(|e|e.to_string())
    }
    pub(crate) fn bytes(&self)->&'a [u8]{self.original}
    pub(crate) fn raw(&self)->&'a str{self.raw.get()}
    pub(crate) fn get(&self,path:&[&str])->Result<Option<&'a RawValue>,String>{let mut raw=self.raw;for key in path{if raw.get()=="null"{return Ok(None);}let mut d=deserializer(raw.get().as_bytes())?;let found=serde::de::Deserializer::deserialize_map(&mut d,Member(key)).map_err(|e|e.to_string())?;d.end().map_err(|e|e.to_string())?;match found{Some(v)=>raw=v,None=>return Ok(None)}}Ok(Some(raw))}
    pub(crate) fn text(&self,path:&[&str])->Result<Option<String>,String>{match self.get(path)?{None=>Ok(None),Some(v)if v.get()=="null"=>Ok(None),Some(v)=>{if v.get().len()>SCALAR_LIMIT{return Err("decoded report scalar exceeds 64KiB limit".into());}let mut d=deserializer(v.get().as_bytes())?;let s=String::deserialize(&mut d).map_err(|e|e.to_string())?;d.end().map_err(|e|e.to_string())?;Ok(Some(s))}}}
    pub(crate) fn number(&self,path:&[&str])->Result<Option<u64>,String>{match self.get(path)?{None=>Ok(None),Some(v)if v.get()=="null"=>Ok(None),Some(v)=>serde_json::from_str(v.get()).map(Some).map_err(|e|e.to_string())}}
}
pub(crate) fn document<'a>(bytes:&'a [u8],working:&std::sync::Arc<Working>)->Result<Document<'a>,String>{if working.documents.get()>=3{return Err("report permits at most three simultaneous documents".into());}let mut d=deserializer(bytes)?;let raw=<&RawValue>::deserialize(&mut d).map_err(|e|format!("invalid JSON: {e}"))?;d.end().map_err(|e|e.to_string())?;working.documents.set(working.documents.get()+1);Ok(Document{raw,original:bytes,working:working.clone(),root:true})}
pub(crate) fn parse<'a>(bytes:&'a [u8],working:&std::sync::Arc<Working>)->Result<Document<'a>,String>{
    let r=document(bytes,working)?;
    if r.text(&["schema"])?.as_deref()!=Some("soapnuke-v3.report/2")||!matches!(r.text(&["status"])?.as_deref(),Some("complete"|"failed"|"incomplete")){return Err("unsupported native report schema/status".into());}
    if r.get(&["phase2"])?.is_some() || r.get(&["phase3"])?.is_some(){return Err("legacy report fields are unsupported".into());}
    if !r.get(&["run"])?.is_some_and(|v|v.get().starts_with('{')){return Err("invalid native run".into());}
    for stage in ["input","accepted","written"]{let p=r.get(&["populations",stage])?.ok_or("missing native population")?;if p.get()!="null"{r.number(&["populations",stage,"source_units"])?;if r.number(&["populations",stage,"read_records"])?.is_none()||r.number(&["populations",stage,"bases"])?.is_none(){return Err("invalid native population".into());}}}
    Ok(r)
}
fn cell<W:Write>(out:&mut W,r:&Document<'_>,path:&[&str])->Result<(),String>{
    let Some(v)=r.get(path)? else{return Ok(())};let raw=v.get();if raw=="null"{return Ok(())}if raw.starts_with('"'){let text=r.text(path)?.unwrap();let quote=text.contains(['\t','\r','\n','"']);if quote{out.write_all(b"\"").map_err(|e|e.to_string())?;}for part in text.split_inclusive('"'){out.write_all(part.as_bytes()).map_err(|e|e.to_string())?;if part.ends_with('"'){out.write_all(b"\"").map_err(|e|e.to_string())?;}}if quote{out.write_all(b"\"").map_err(|e|e.to_string())?;}Ok(())}else if raw.starts_with(['{','[']){Err("non-scalar report cell".into())}else{out.write_all(raw.as_bytes()).map_err(|e|e.to_string())}}
pub(crate) fn write<W:Write>(out:&mut W,r:&Document<'_>,format:&ReportFormat)->Result<(),String>{match format{
ReportFormat::Html=>crate::native_html::write_saved(out,r).map_err(|e|e.to_string()),
ReportFormat::Tsv=>{out.write_all(b"sample\tjob\tstatus\tstage\tchannel\tsource_units\treads\tbases\tmin_length\tmax_length\tmean_length\tmedian_length\tn_bases\tgc_canonical_fraction\tq20_bases\tq30_bases\n").map_err(|e|e.to_string())?;for stage in ["input","accepted","written"]{for(i,path)in [vec!["run","sample_id"],vec!["run","job"],vec!["status"]].iter().enumerate(){if i>0{out.write_all(b"\t").map_err(|e|e.to_string())?;}cell(out,r,path)?;}write!(out,"\t{stage}\tall").map_err(|e|e.to_string())?;for path in [vec!["source_units"],vec!["read_records"],vec!["bases"],vec!["length","minimum"],vec!["length","maximum"],vec!["length","mean","value"],vec!["length","median"],vec!["n_bases"],vec!["gc_canonical_fraction","value"],vec!["q20_bases"],vec!["q30_bases"]]{out.write_all(b"\t").map_err(|e|e.to_string())?;let mut full=vec!["populations",stage];full.extend(path);cell(out,r,&full)?;}out.write_all(b"\n").map_err(|e|e.to_string())?;}Ok(())},_=>Err("named consumer renderer not implemented yet".into())}}
pub(crate) fn read(path:&Path)->Result<Vec<u8>,String>{let mut f=fs::File::open(path).map_err(|e|e.to_string())?;let size=f.metadata().map_err(|e|e.to_string())?.len();if size>DOCUMENT_LIMIT as u64{return Err("report document exceeds 4MiB limit".into());}let mut bytes=Vec::new();bytes.try_reserve_exact(size as usize).map_err(|e|e.to_string())?;if bytes.capacity()>size as usize{return Err("raw report capacity exceeds reservation".into());}bytes.resize(size as usize,0);f.read_exact(&mut bytes).map_err(|e|e.to_string())?;let mut extra=[0u8;1];if f.read(&mut extra).map_err(|e|e.to_string())?!=0{return Err("input report changed during read".into());}Ok(bytes)}
fn run(args:&[String])->Result<(),(i32,String)>{
    let mut values=BTreeMap::new();let mut i=0;
    while i<args.len(){let arg=args[i].strip_prefix("--").ok_or((2,"invalid report option".into()))?;let(key,inline)=arg.split_once('=').map_or((arg,None),|(k,v)|(k,Some(v)));if !crate::option_metadata::lookup(crate::option_metadata::Job::Report,key).is_some(){return Err((2,format!("unknown report option {key}")));}let value=if let Some(v)=inline{v.to_string()}else if key=="allow-unqualified"{"true".into()}else{i+=1;args.get(i).ok_or((2,format!("missing {key}")))?.clone()};if values.insert(key,value).is_some(){return Err((2,format!("duplicate {key}")));}i+=1;}
    let input=PathBuf::from(values.get("input-report").ok_or((2,"input-report required".into()))?.as_str());let input=fs::canonicalize(input).map_err(|e|(2,e.to_string()))?;
    let working=Working::new().map_err(|e|(2,e))?;let bytes=read(&input).map_err(|e|(2,e))?;let report=parse(&bytes,&working).map_err(|e|(2,e))?;
    let mut outputs=Vec::with_capacity(4);for(key,kind)in [("html-report",0u8),("tsv",1)]{if let Some(p)=values.get(key){outputs.push((crate::job_api::path_identity(Path::new(p)).map_err(|e|(2,e))?,kind));}}
    let named_format=if let Some(kind)=values.get("export"){let f=match kind.as_str(){"multiqc"=>ReportFormat::Multiqc,"mgnify"=>ReportFormat::Mgnify,_=>return Err((2,format!("unsupported export format {kind}")))};let p=crate::job_api::path_identity(Path::new(values.get("export-out").ok_or((2,"export output required".into()))?)).map_err(|e|(2,e))?;if matches!(f,ReportFormat::Multiqc)&&!p.to_string_lossy().ends_with("_mqc.json"){return Err((2,"MultiQC custom-content export requires _mqc.json extension".into()));}if matches!(f,ReportFormat::Mgnify){outputs.push((PathBuf::from(format!("{}.samplesheet.csv",p.to_string_lossy())),4));outputs.push((p,3));}else{outputs.push((p,2));}Some(f)}else{if ["export-out","consumer-version","consumer-contract","allow-unqualified"].iter().any(|k|values.contains_key(k)){return Err((2,"consumer options require export".into()));}None};
    if outputs.is_empty(){return Err((2,"at least one report output required".into()));}for(i,(p,_))in outputs.iter().enumerate(){if p==&input||p.exists()||outputs[..i].iter().any(|(q,_)|q==p){return Err((2,"input/output alias or existing output".into()));}}
    let output_paths=outputs.iter().map(|(p,_)|p.clone()).collect::<Vec<_>>();let named=if let Some(ref format)=named_format{let path=PathBuf::from(values.get("consumer-contract").ok_or((2,"consumer-contract required".into()))?);let version=values.get("consumer-version").ok_or((2,"consumer-version required".into()))?.clone();let allow=match values.get("allow-unqualified").map(String::as_str){Some("true")=>true,None|Some("false")=>false,_=>return Err((2,"allow-unqualified expects true/false".into()))};let name=if matches!(format,ReportFormat::Multiqc){"multiqc-custom-content"}else{"mgnify-rawr-input"};let contract=crate::job_api::ConsumerContract{name:name.into(),version,manifest_sha256:crate::native_screen::sha256(path.to_str().ok_or((2,"non-UTF8 contract path".into()))?).map_err(|e|(2,e))?,manifest_path:path,allow_unqualified:allow};Some(crate::consumer_export::Named::new(&report,format,&contract,input.parent(),&output_paths,"cli/1").map_err(|e|(2,e))?)}else{None};
    let mut staged=Vec::with_capacity(4);
    for(dest,kind)in outputs{let stage=dest.with_file_name(format!(".{}.r81-{}",dest.file_name().unwrap().to_string_lossy(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));let file=OpenOptions::new().write(true).create_new(true).open(&stage).map_err(|e|(4,e.to_string()))?;let mut out=BufWriter::with_capacity(8192,file);match kind{0=>write(&mut out,&report,&ReportFormat::Html),1=>write(&mut out,&report,&ReportFormat::Tsv),2=>named.as_ref().unwrap().write_multiqc(&mut out,&report),3=>named.as_ref().unwrap().write_manifest(&mut out,&format!("{}.samplesheet.csv",dest.file_name().unwrap().to_string_lossy())),4=>named.as_ref().unwrap().write_csv(&mut out),_=>unreachable!()}.map_err(|e|(4,e))?;out.flush().map_err(|e|(4,e.to_string()))?;let file=out.into_inner().map_err(|e|(4,e.to_string()))?;file.sync_all().map_err(|e|(4,e.to_string()))?;crate::checked_io::close_file(file).map_err(|e|(4,e))?;staged.push((stage,dest));}
    for(stage,dest)in staged{fs::hard_link(&stage,&dest).map_err(|e|(4,format!("publish report: {e}")))?;fs::remove_file(stage).map_err(|e|(4,e.to_string()))?;}Ok(())
}
pub fn entry(args:&[String])->i32{if args.len()==1&&matches!(args[0].as_str(),"--help"|"-h"|"--version"|"-v"){println!("SOAPnuke {} report - Render or export a saved QC report",env!("CARGO_PKG_VERSION"));if matches!(args[0].as_str(),"--help"|"-h"){print!("{}",include_str!("report_help.txt"));}return 0;}match run(args){Ok(())=>0,Err((code,e))=>{eprintln!("report: {e}");code}}}

#[cfg(test)] mod tests{
use super::*;
#[test] fn report_borrowed_capacity_nested_escaped_limits_release(){
 let w=Working::new().unwrap();let e=w.execution.clone();
 let bytes=br#"{"unknown":[[[{"escaped\u006bey":"\uD83D\uDE00\n</script>"}]]],"target":"literal"}"#;
 let d=document(bytes,&w).unwrap();assert!(std::ptr::eq(d.raw().as_ptr(),bytes.as_ptr()));assert_eq!(d.text(&["target"]).unwrap().as_deref(),Some("literal"));assert_eq!(d.get(&["unknown"]).unwrap().unwrap().get(),r#"[[[{"escaped\u006bey":"\uD83D\uDE00\n</script>"}]]]"#);
 let d2=document(bytes,&w).unwrap();let d3=document(bytes,&w).unwrap();assert!(document(bytes,&w).is_err());drop((d,d2,d3));assert_eq!(w.documents.get(),0);
 let deep=format!("{}0{}","[".repeat(4096),"]".repeat(4096));assert!(document(deep.as_bytes(),&w).is_ok());let over=vec![b' ';DOCUMENT_LIMIT+1];assert!(document(&over,&w).is_err());assert!(document(b"{broken",&w).is_err());
 drop(w);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
}
#[test] fn saved_count_cells_validate_types_preserve_nullable_u64(){
 let w=Working::new().unwrap();
 let base=serde_json::json!({"schema":"soapnuke-v3.report/2","status":"complete","run":{"sample_id":"literal","job":"process"},"populations":{"input":{"source_units":1,"read_records":1,"bases":2},"accepted":{"source_units":1,"read_records":1,"bases":2},"written":{"source_units":1,"read_records":1,"bases":2}}});
 let mut old=base.clone();old["schema"]=serde_json::json!("soapnuke-v3.report/1");assert!(parse(&serde_json::to_vec(&old).unwrap(),&w).is_err());
 for key in ["phase2","phase3"]{let mut old=base.clone();old[key]=serde_json::json!({});assert!(parse(&serde_json::to_vec(&old).unwrap(),&w).is_err());}
 for stage in ["input","accepted","written"]{for key in ["source_units","read_records","bases"]{for wrong in [serde_json::json!("</td><script>literal</script>"),serde_json::json!(false),serde_json::json!([]),serde_json::json!({}),serde_json::json!(-1),serde_json::json!(1.5)]{let mut value=base.clone();value["populations"][stage][key]=wrong;let bytes=serde_json::to_vec(&value).unwrap();assert!(parse(&bytes,&w).is_err(),"{stage}.{key}");}}}
 let mut value=base.clone();for key in ["source_units","read_records","bases"]{value["populations"]["input"][key]=serde_json::json!(u64::MAX);}value["populations"]["accepted"]["source_units"]=serde_json::Value::Null;value["populations"]["written"].as_object_mut().unwrap().remove("source_units");let bytes=serde_json::to_vec(&value).unwrap();let report=parse(&bytes,&w).unwrap();let mut html=Vec::new();write(&mut html,&report,&ReportFormat::Html).unwrap();let html=String::from_utf8(html).unwrap();assert!(html.contains("<tr><th>input</th><td>18446744073709551615</td><td>18446744073709551615</td><td>18446744073709551615</td>"));for stage in ["accepted","written"]{assert!(html.contains(&format!("<tr><th>{stage}</th><td>null</td><td>1</td><td>2</td>")));}assert_eq!(report.number(&["populations","accepted","source_units"]).unwrap(),None);assert_eq!(report.number(&["populations","written","source_units"]).unwrap(),None);
}

}
