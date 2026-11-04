use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const LIMIT: usize = 512 << 20;
#[derive(Debug)]
pub struct Error { pub code: &'static str, pub field: String, pub message: String }
fn err(code: &'static str, field: &str, message: impl ToString) -> Error { Error { code, field: field.into(), message: message.to_string() } }
pub struct Catalogue { pub path: PathBuf, pub sha256: String, pub value: Value, reserved: usize }
fn bytes(path: &Path, multiplier: usize, available: usize, field: &str) -> Result<Vec<u8>, Error> {
    let size = std::fs::metadata(path).map_err(|e| err("resource-io",field,e))?.len();
    let size = usize::try_from(size).map_err(|_| err("resource-memory",field,"length overflow"))?;
    let bound = size.checked_mul(multiplier).and_then(|n| n.checked_add(65536)).ok_or_else(||err("resource-memory",field,"capacity overflow"))?;
    if bound > available { return Err(err("resource-memory",field,"resource does not fit managed memory budget")); }
    // Bound the read even if the file grows after metadata inspection.
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|e|err("resource-io",field,e))?;
    let mut data = Vec::new();
    data.try_reserve_exact(size).map_err(|e|err("resource-memory",field,e))?;
    file.take(size as u64 + 1).read_to_end(&mut data).map_err(|e|err("resource-io",field,e))?;
    if data.len() != size { return Err(err("resource-io",field,"resource changed during read")); }
    Ok(data)
}
impl Catalogue {
    pub fn load(path: &Path) -> Result<Self,Error> {
        let field="resources.catalogue";
        let data=bytes(path,128,LIMIT,field).map_err(|e|if e.code=="resource-io"{err("resource-catalogue-io",field,e.message)}else{e})?;
        let value:Value=serde_json::from_slice(&data).map_err(|e|err("resource-catalogue-schema",field,e))?;
        if value["schema"]!="soapnuke-v3.resources/1" || !value["resources"].is_array() { return Err(err("resource-catalogue-schema",field,"invalid catalogue schema/resources")); }
        let mut ids=BTreeSet::new();
        for row in value["resources"].as_array().unwrap() {
            for key in ["id","kind","path","sha256"] { if row[key].as_str().is_none_or(str::is_empty) { return Err(err("resource-catalogue-schema",field,format!("missing resource {key}"))); } }
            if !ids.insert(row["id"].as_str().unwrap()) { return Err(err("resource-catalogue-schema",field,"duplicate resource ID")); }
            if !matches!(row["kind"].as_str(),Some("adapter"|"reference")) {return Err(err("resource-kind",field,"invalid resource kind"));}
            let hash=row["sha256"].as_str().unwrap();
            if hash.len()!=64 || !hash.bytes().all(|b|b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {return Err(err("resource-catalogue-schema",field,"invalid sha256"));}
            if let Some(description)=row.get("description") { if !description.is_null() && !description.is_string(){return Err(err("resource-catalogue-schema",field,"invalid description"));} }
        }
        if let Some(version)=value.get("catalogue_version") {if !version.is_null() && !version.is_string(){return Err(err("resource-catalogue-schema",field,"invalid catalogue_version"));}}
        let reserved=data.len()*128+65536;
        let sha256=hash(path,field)?;
        Ok(Self{path:path.into(),sha256,value,reserved})
    }
    pub fn row(&self,id:&str)->Result<&Value,Error>{self.value["resources"].as_array().unwrap().iter().find(|r|r["id"]==id).ok_or_else(||err("resource-id","resources","unknown resource ID"))}
    pub fn resolve(&self,id:&str,kind:&str,field:&str)->Result<(PathBuf,String),Error>{
        let row=self.row(id).map_err(|e|err(e.code,field,e.message))?;
        if row["kind"]!=kind{return Err(err("resource-kind",field,"resource kind mismatch"));}
        let path=self.path.parent().unwrap_or(Path::new(".")).join(row["path"].as_str().unwrap());
        verify(&path,kind,field,Some(row["sha256"].as_str().unwrap()),LIMIT-self.reserved)
    }
    fn output(&self,rows:Vec<Value>)->Value{
        let rows:Vec<Value>=rows.into_iter().map(|mut row|{if row.get("description").is_none(){row["description"]=Value::Null;row["description_unavailable_reason"]=json!("absent in supplied catalogue");}row}).collect();
        let mut result=json!({"catalogue_schema":self.value["schema"],"catalogue_version":self.value.get("catalogue_version").unwrap_or(&Value::Null),"resources":rows});
        if self.value.get("catalogue_version").is_none(){result["catalogue_version_unavailable_reason"]=json!("absent in supplied catalogue");}
        result
    }
}
pub fn hash(path:&Path,field:&str)->Result<String,Error>{crate::native_screen::sha256(&path.to_string_lossy()).map_err(|e|err("resource-io",field,e))}
pub fn verify(path:&Path,kind:&str,field:&str,expected:Option<&str>,available:usize)->Result<(PathBuf,String),Error>{
    if !matches!(kind,"adapter"|"reference"){return Err(err("resource-kind",field,"invalid resource kind"));}
    let path=std::fs::canonicalize(path).map_err(|e|err("resource-io",field,e))?;
    let data=bytes(&path,2,available,field)?;
    let digest=hash(&path,field)?;
    if expected.is_some_and(|h|h!=digest){return Err(err("resource-hash",field,"resource hash mismatch"));}
    let text=std::str::from_utf8(&data).map_err(|_|err("resource-fasta",field,"invalid FASTA encoding"))?;
    let mut seen=false;let mut length=0;
    for line in text.lines(){
        if let Some(header)=line.strip_prefix('>'){
            if (seen && length==0)||header.is_empty(){return Err(err("resource-fasta",field,"empty FASTA record"));}
            if kind=="adapter"{
                let parts:Vec<_>=header.split(';').collect();
                if parts.len()!=3 || parts[0].is_empty() || parts[0].bytes().any(|b|b.is_ascii_whitespace()) || !matches!(parts[1],"mate=R1"|"mate=R2") || !matches!(parts[2],"end=3"|"end=5") {return Err(err("resource-fasta",field,"adapter FASTA requires name;mate=R1|R2;end=3|5"));}
            }
            seen=true;length=0;
        }else{
            if !seen || line.is_empty() || !line.bytes().all(|b|if kind=="adapter"{b"ACGT".contains(&b.to_ascii_uppercase())}else{b"ACGTRYKMSWBDHVN".contains(&b.to_ascii_uppercase())}) {return Err(err("resource-fasta",field,if kind=="adapter"{"adapter FASTA sequence must contain only A/C/G/T (case-insensitive); N and other ambiguity codes are unsupported"}else{"invalid FASTA sequence"}));}
            length+=line.len();
        }
    }
    if !seen||length==0{return Err(err("resource-fasta",field,"empty FASTA"));}
    Ok((path,digest))
}
fn run(args:&[String])->Result<Value,Error>{
    let mut operation=None;let mut id=None;let mut catalog=None;let mut i=0;
    while i<args.len(){match args[i].as_str(){a if a.strip_prefix("--").is_some_and(|k|crate::option_metadata::lookup(crate::option_metadata::Job::Resources,k).is_some())=>{i+=1;catalog=Some(args.get(i).ok_or_else(||err("config","resource-catalog","missing catalogue path"))?);},a if a.starts_with('-')=>return Err(err("config",a,"unknown resource option")),a=>{if operation.is_none(){operation=Some(a);}else if id.is_none(){id=Some(a);}else{return Err(err("config","resources","unexpected resource argument"));}}}i+=1;}
    let operation=operation.ok_or_else(||err("config","resources","missing operation list|show|verify"))?;
    if !matches!(operation,"list"|"show"|"verify") {return Err(err("config","resources","invalid operation"));}
    if operation=="list"&&id.is_some() || operation!="list"&&id.is_none(){return Err(err("config","resources","list does not accept an ID; show/verify require an ID"));}
    let catalog=Catalogue::load(Path::new(catalog.ok_or_else(||err("config","resource-catalog","missing catalogue"))?))?;
    let rows=if let Some(id)=id{let row=catalog.row(id)?.clone();if operation=="verify"{catalog.resolve(id,row["kind"].as_str().unwrap(),"resources")?;}vec![row]}else{catalog.value["resources"].as_array().unwrap().clone()};
    Ok(catalog.output(rows))
}
pub fn entry(args:&[String])->i32{
    if args.len()==1 && matches!(args[0].as_str(),"-h"|"--help"|"-v"|"--version") {println!("SOAPnuke {} resources - Inspect and verify local adapter/reference files",env!("CARGO_PKG_VERSION"));if matches!(args[0].as_str(),"-h"|"--help"){print!("{}",include_str!("resources_help.txt"));}return 0;}
    match run(args){Ok(v)=>{println!("{}",v);0},Err(e)=>{eprintln!("{}: {}: {}",e.code,e.field,e.message);if matches!(e.code,"resource-hash"|"resource-io"|"resource-memory"){4}else{2}}}
}
#[cfg(test)]mod tests{
    use super::*;
    #[test]fn resource_capacity_checked_before_open(){let result=bytes(Path::new("/nonexistent"),128,0,"test");assert!(result.is_err());}
    #[test]fn exact_operation_arity(){assert!(run(&["list".into(),"extra".into()]).is_err());assert!(run(&["show".into()]).is_err());}
}

#[cfg(test)]
mod r93_alphabet_tests {
    use super::*;
    #[test]
    fn adapter_canonical_only_reference_iupac_unchanged() {
        let d=Path::new(env!("CARGO_MANIFEST_DIR")).join("../.scratch/r93-resources");std::fs::create_dir_all(&d).unwrap();let p=d.join("a.fa");
        for seq in ["ACGT","acgt","AcGt"] {std::fs::write(&p,format!(">a;mate=R1;end=3\n{seq}\n")).unwrap();let digest=hash(&p,"test").unwrap();assert!(verify(&p,"adapter","test",Some(&digest),LIMIT).is_ok());}
        for seq in ["ACGN","ACGn","ACGR"] {std::fs::write(&p,format!(">a;mate=R1;end=3\n{seq}\n")).unwrap();let v=verify(&p,"adapter","test",None,LIMIT).err().unwrap();assert_eq!(v.code,"resource-fasta");assert!(v.message.contains("only A/C/G/T"));}
        std::fs::write(&p,">r\nacgtrykmswbdhvn\n").unwrap();assert!(verify(&p,"reference","test",None,LIMIT).is_ok());
    }
}
