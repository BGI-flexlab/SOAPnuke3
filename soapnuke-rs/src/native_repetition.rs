use crate::failure::BudgetError;
use crate::native_execution::{Execution,FeatureTicket,Ticket};
use crate::record::FastqRecord;
use serde_json::{Value,json};
use std::sync::{Arc,Mutex};

struct Key {sequences:[String;2],_reservation:FeatureTicket}
pub struct Estimator {
    requested:usize,target:usize,mates:usize,threshold:Option<f64>,execution:Arc<Execution>,
    slots:Mutex<Vec<Option<Key>>>,_metadata:FeatureTicket,
}
impl Estimator {
    pub fn new(gp:&crate::params::GlobalParams,execution:&Arc<Execution>)->Result<Arc<Self>,BudgetError>{
        let requested=gp.native_process.repetition_units;
        let target=requested.min(usize::try_from(gp.native_limit_units.unwrap_or(u64::MAX)).unwrap_or(usize::MAX));
        // Slots and all possible report entries are bounded before input; key
        // payload/report copies are charged separately from live chunk capacity.
        let bytes=target.checked_mul(2048+std::mem::size_of::<Option<Key>>()+4*std::mem::size_of::<usize>()).and_then(|n|n.checked_add(65536)).ok_or("[worker] repetition metadata overflow")?;
        let metadata=execution.reserve_preparation_feature(bytes)?;
        let mut slots=Vec::new();slots.try_reserve_exact(target).map_err(|e|format!("[worker] repetition slots: {e}"))?;
        if slots.capacity()!=target{return Err("[worker] repetition slot capacity exceeded reservation".into());}
        slots.resize_with(target,||None);
        Ok(Arc::new(Self{requested,target,mates:if gp.paired(){2}else{1},threshold:gp.native_process.overrep_fraction,execution:execution.clone(),slots:Mutex::new(slots),_metadata:metadata}))
    }
    pub fn observe(&self,r1:&FastqRecord,r2:Option<&FastqRecord>,ticket:Option<&mut Ticket>)->Result<(),BudgetError>{
        let ordinal=usize::try_from(r1.source_index).map_err(|_|"[worker] repetition ordinal overflow")?;
        if ordinal>=self.target{return Ok(());}
        if r2.is_some()!= (self.mates==2){return Err("[worker] repetition mate association mismatch".into());}
        let mut slots=self.slots.lock().unwrap_or_else(|p|p.into_inner());
        if let Some(key)=&slots[ordinal]{
            if !key.sequences[0].bytes().eq(r1.sequence.bytes().map(|b|b.to_ascii_uppercase()))||r2.is_some_and(|r|!key.sequences[1].bytes().eq(r.sequence.bytes().map(|b|b.to_ascii_uppercase()))){return Err("[worker] replayed original repetition key differs".into());}
            return Ok(());
        }
        let payload=r1.sequence.len().checked_add(r2.map_or(0,|r|r.sequence.len())).ok_or("[worker] repetition payload overflow")?;
        let bytes=payload.checked_mul(2).ok_or("[worker] repetition report-copy overflow")?;
        let reservation=if let Some(ticket)=ticket{ticket.retain_feature(bytes)?}else{self.execution.reserve_preparation_feature(bytes)?};
        let sequences=[r1.sequence.to_ascii_uppercase(),r2.map_or_else(String::new,|r|r.sequence.to_ascii_uppercase())];
        if sequences.iter().map(String::capacity).sum::<usize>()>payload{return Err("[worker] repetition key capacity exceeded reservation".into());}
        slots[ordinal]=Some(Key{sequences,_reservation:reservation});Ok(())
    }
    pub fn report(&self,input:u64,job_complete:bool)->Value{
        let slots=self.slots.lock().unwrap_or_else(|p|p.into_inner());let n=slots.iter().take_while(|v|v.is_some()).count();let observed=slots.iter().filter(|v|v.is_some()).count();
        let mut order:Vec<usize>=(0..n).collect();order.sort_unstable_by(|a,b|slots[*a].as_ref().unwrap().sequences.cmp(&slots[*b].as_ref().unwrap().sequences));
        let mut groups:Vec<(usize,u64)>=Vec::with_capacity(n);
        for i in order{if let Some((prior,count))=groups.last_mut(){if slots[*prior].as_ref().unwrap().sequences==slots[i].as_ref().unwrap().sequences{*count+=1;continue;}}groups.push((i,1));}
        let distinct=groups.len();let mut report=json!({"population":"input-prefix","stage":"original","requested_units":self.requested,"effective_target_units":self.target,"sample_units":n,"distinct_keys":distinct,"repeated_units":n-distinct,"sample_tail_excluded":input.saturating_sub(n as u64),"observed_prefix_slots":observed,"later_observed_slots_excluded_from_estimator":observed-n,"prefix_end":if n==self.target{"target"}else if job_complete{"input-eof"}else{"error-or-cancellation"},"key_definition":"complete uppercased original SE sequence or ordered PE sequence tuple; no quality/header/UMI; complete-byte equality, not digest identity","repeated_fraction":{"numerator":n-distinct,"denominator":n,"value":if n==0{None}else{Some((n-distinct)as f64/n as f64)},"unit":"source units","stage":"original-input-prefix","unavailable_reason":if n==0{Some("no observed contiguous sample units")}else{None}},"scope":"exact observed contiguous original-input prefix; not full-input prevalence or PCR-specific diagnosis; no removal"});
        if let Some(threshold)=self.threshold {
            let mut entries=Vec::new();
            for(index,count)in groups{if above_fraction(count,n as u64,threshold){let mut entry=serde_json::Map::new();entry.insert("key".into(),Value::Array(slots[index].as_ref().unwrap().sequences.iter().take(self.mates).map(|s|Value::String(s.clone())).collect()));entry.insert("count".into(),json!(count));entries.push(Value::Object(entry));}}
            report["overrepresented"]=Value::Array(entries);report["overrep_fraction"]=json!(threshold);report["overrep_comparison"]=json!("count/sample_units strictly exceeds the configured binary64 fraction; exact integer floor of fraction*denominator");
        }
        report
    }
}
pub fn above_fraction(count:u64,total:u64,p:f64)->bool{
    if total==0{return false;}
    let bits=p.to_bits();let exponent=((bits>>52)&0x7ff)as u32;let mantissa=(bits&((1u64<<52)-1))|if exponent==0{0}else{1u64<<52};let shift=if exponent==0{1074}else{1075-exponent};let product=mantissa as u128*total as u128;let floor=if shift>=128{0}else{product>>shift};count as u128>floor
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc,Layout,System};
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool,Ordering};
    thread_local! {static TRACK:Cell<Option<usize>>=const{Cell::new(None)};}
    struct AllocationCounter;
    #[global_allocator]
    static ALLOCATOR:AllocationCounter=AllocationCounter;
    unsafe impl GlobalAlloc for AllocationCounter {
        unsafe fn alloc(&self,l:Layout)->*mut u8 {TRACK.with(|s|if let Some(n)=s.get(){s.set(Some(n+l.size()));});System.alloc(l)}
        unsafe fn alloc_zeroed(&self,l:Layout)->*mut u8 {TRACK.with(|s|if let Some(n)=s.get(){s.set(Some(n+l.size()));});System.alloc_zeroed(l)}
        unsafe fn dealloc(&self,p:*mut u8,l:Layout){System.dealloc(p,l)}
        unsafe fn realloc(&self,p:*mut u8,l:Layout,n:usize)->*mut u8{TRACK.with(|s|if let Some(old)=s.get(){s.set(Some(old+n));});System.realloc(p,l,n)}
    }
    fn params(n:usize,paired:bool)->crate::params::GlobalParams{
        let mut g=crate::params::GlobalParams::new();let p=Arc::make_mut(&mut g.native_process);p.repetition=true;p.repetition_units=n;p.overrep_fraction=Some(0.0);if paired{g.fq2_path="test-mate".into();}g
    }
    fn record(i:u64,s:&str)->FastqRecord{let mut r=FastqRecord::new(1);r.source_index=i;r.sequence=s.into();r.qual_seq="I".repeat(s.len());r}
    #[test]
    fn repetition_handoff_contiguous_prefix_replay_release_and_nonfit(){
        let stop=Arc::new(AtomicBool::new(false));let e=Execution::new(2,2,stop.clone()).unwrap();let g=params(4,true);let est=Estimator::new(&g,&e).unwrap();
        let mut ticket=e.admit_with_feature(100,4,100,1024).unwrap();let before=e.facts()["memory_budget"]["current_total_reserved_bytes"].clone();
        let a=record(2,"acgt");let b=record(2,"NN");est.observe(&a,Some(&b),Some(&mut ticket)).unwrap();
        assert_eq!(before,e.facts()["memory_budget"]["current_total_reserved_bytes"]);assert!(e.reserve_preparation_feature(1).is_err());
        let v=est.report(3,false);assert_eq!(v["sample_units"],0);assert_eq!(v["later_observed_slots_excluded_from_estimator"],1);assert!(v["repeated_fraction"]["value"].is_null());
        let a0=record(0,"ACGT");let b0=record(0,"NN");est.observe(&a0,Some(&b0),Some(&mut ticket)).unwrap();
        let retained=e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"].clone();est.observe(&a0,Some(&b0),Some(&mut ticket)).unwrap();assert_eq!(retained,e.facts()["memory_budget"]["current_persistent_feature_reserved_bytes"]);
        assert!(est.observe(&record(0,"AAAA"),Some(&b0),Some(&mut ticket)).is_err());
        est.observe(&record(1,"acgt"),Some(&record(1,"nn")),Some(&mut ticket)).unwrap();
        let v=est.report(3,false);assert_eq!(v["sample_units"],3);assert_eq!(v["distinct_keys"],1);assert_eq!(v["repeated_units"],2);assert_eq!(v["overrepresented"][0]["key"],json!(["ACGT","NN"]));
        drop(ticket);assert_eq!(e.facts()["active"],0);drop(est);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
        assert!(Estimator::new(&params(usize::MAX,false),&e).is_err());assert!(e.reserve_preparation_feature(usize::MAX).is_err());stop.store(true,Ordering::Relaxed);assert!(e.reserve_preparation_feature(1).is_err());
    }
    #[test]
    fn repetition_exact_binary64_thresholds(){
        for n in 1..100 {assert!(!above_fraction(n/2,n,0.5));assert_eq!(above_fraction(n/2+1,n,0.5),n/2+1>n/2);assert!(!above_fraction(n,n,1.0));assert!(above_fraction(1,n,0.0));}
        assert!(!above_fraction(0,0,0.0));assert!(!above_fraction(1,2,0.5));assert!(above_fraction(1,2,f64::from_bits(0.5f64.to_bits()-1)));assert!(!above_fraction(1,2,f64::from_bits(0.5f64.to_bits()+1)));assert!(above_fraction(1,u64::MAX,f64::from_bits(1)));
    }
    #[test]
    fn repetition_actual_metadata_report_growth_bound(){
        // Count total requested allocation bytes, including every old/new realloc
        // request. This overbounds live overlap without relying on allocator RSS.
        for n in [0,1,2,3,4,7,8,9,15,16,17,31,32,33,127,128,129,4095,4096,4097,100000]{
            let e=Execution::new(1,2,Arc::new(AtomicBool::new(false))).unwrap();let mut g=params(n.max(1),true);g.native_limit_units=Some(n as u64);
            let records:Vec<_>=(0..n).map(|i|record(i as u64,&format!("ACGT{i:016X}"))).collect();
            TRACK.with(|s|s.set(Some(0)));let est=Estimator::new(&g,&e).unwrap();
            for r in &records{est.observe(r,Some(r),None).unwrap();}let v=est.report(n as u64,true);let actual=TRACK.with(|s|s.replace(None).unwrap());
            let reserved=e.facts()["memory_budget"]["current_feature_reserved_bytes"].as_u64().unwrap()as usize;
            assert_eq!(v["distinct_keys"],n);assert!(actual<=reserved,"n={n} allocation_requests={actual} reserved={reserved}");
            println!("REPETITION_CAPACITY n={n} cumulative_requested={actual} reserved={reserved}");drop(v);drop(est);assert_eq!(e.facts()["memory_budget"]["current_feature_reserved_bytes"],0);
        }
    }
}
