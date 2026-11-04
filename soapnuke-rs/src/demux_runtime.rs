use crate::failure::Failure;
use crate::{
    native_qc::Qc,
    demux::{Destination, Settings, Table},
    record::FastqRecord,
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
#[derive(Clone, Debug)]
pub(crate) struct Channel {
    pub name: String,
    pub path: PathBuf,
    pub codec: String,
    pub phred: i32,
}
pub(crate) use crate::failure::UnitError as Error;
pub(crate) struct Unit {
    pub raw: Vec<FastqRecord>,
    pub routed: Vec<FastqRecord>,
    pub destination: Result<Destination, Error>,
    pub indices:Vec<FastqRecord>,
    pub lane_index:Option<usize>,
    pub payload: Vec<(usize, Vec<u8>)>,
    pub output_error: Option<Error>,
}
pub(crate) struct State {
    pub input: Qc,
    pub accepted: Qc,
    pub written: Qc,
    pub channels: Vec<Qc>,
    pub indices:[Qc;2],
    pub error_lane:Option<usize>,
    pub counts: [u64; 5],
    pub error: Option<Error>,
}
pub(crate) struct Runtime {
    pub table: Arc<Table>,
    pub settings: Settings,
    pub channels: Vec<Channel>,
    pub mates: usize,
    pub index_source: bool,
    pub unassigned: Option<usize>,
    pub ambiguous: Option<usize>,
    pub stage: PathBuf,
    pub level: i32,
    pub state: Mutex<Option<State>>,
    pub(crate) metadata: Mutex<Option<crate::native_execution::FeatureTicket>>,
}
impl Runtime {
    pub(crate) fn statistics_bound(channels: usize) -> Result<usize, String> {
        channels.checked_add(5).ok_or("[worker] demux statistics overflow")?
            .checked_mul(Qc::maximum_heap_bytes() + std::mem::size_of::<Qc>())
            .ok_or("[worker] demux statistics overflow".into())
    }
    pub(crate) fn metadata_bound(table:&Table,channels:&Vec<Channel>,stage:&std::path::Path)->Result<usize,String>{
        let bytes = channels.iter()
            .map(|c| c.name.capacity() + c.path.as_os_str().len() * 4 + 2048)
            .sum::<usize>()
            + table.samples
                .iter()
                .map(|s| {
                    s.name.capacity()
                        + s.barcodes.capacity() * std::mem::size_of::<String>()
                        + s.barcodes.iter().map(String::capacity).sum::<usize>()
                        + std::mem::size_of::<crate::demux::Sample>()
                })
                .sum::<usize>()
            + channels.len() * (stage.as_os_str().len()*2+64)
            + channels.capacity() * std::mem::size_of::<Channel>() * 4
            + 65536;
        Ok(bytes)
    }
    pub(crate) fn feature_bound(raw:usize,units:usize)->Result<usize,String>{
        let slots=8*std::mem::size_of::<FastqRecord>()+std::mem::size_of::<Unit>()+2*std::mem::size_of::<(usize,Vec<u8>)>()+256;
        raw.checked_mul(8).and_then(|n|units.checked_mul(slots).and_then(|m|n.checked_add(m))).and_then(|n|n.checked_add(65536)).ok_or("[worker] demux chunk representation overflow".into())
    }
    pub(crate) fn prepare(
        &self,
        e: &Arc<crate::native_execution::Execution>,
        io: &Arc<crate::native_io::OwnedIo>,
    ) -> Result<(), String> {
        if self.metadata.lock().unwrap().is_none(){return Err("[worker] demux metadata lacks pre-allocation reservation".into());}
        *self.state.lock().unwrap() = Some(State {
            input: Qc::default(),
            accepted: Qc::default(),
            written: Qc::default(),
            channels: (0..self.channels.len()).map(|_| Qc::default()).collect(),
            counts: [0; 5],
            indices:Default::default(),error_lane:None,
            error: None,
        });
        let names = self
            .channels
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.name.clone(),
                    format!(
                        "demux-{i:06}.fq{}",
                        if c.codec == "gzip" { ".gz" } else { "" }
                    ),
                )
            })
            .collect::<Vec<_>>();
        io.prepare_demux(&self.stage, &names)
            .map_err(|e| format!("[write] demux preparation: {e}"))
    }
    pub(crate) fn append(
        &self,
        gp: &crate::params::GlobalParams,
        units: &mut Vec<Unit>,
        cpu: &Arc<crate::cpu_budget::CpuBudget>,
    ) -> Result<(), Failure> {
        let io = gp.native_io.as_ref().unwrap();
        let mut state = self.state.lock().unwrap();
        let state = state.as_mut().unwrap();
        for unit in units.drain(..) {
            let ordinal = unit.raw.first().map_or(state.counts[0], |r| r.source_index);
            for(i,r)in unit.indices.iter().enumerate(){state.indices[i].observe_validated(r.sequence.as_bytes(),r.qual_seq.as_bytes(),33);}
            if unit.destination.is_err()||unit.output_error.is_some(){state.error_lane=unit.lane_index;}
            {
                let _permit = cpu.acquire(crate::cpu_budget::Stage::Sink);
                if !unit.raw.is_empty() {
                    for r in &unit.raw {
                        state.input.observe_validated(
                            r.sequence.as_bytes(),
                            r.qual_seq.as_bytes(),
                            33,
                        );
                    }
                    state.counts[0] += 1;
                }
                gp.native_runtime.as_ref().unwrap().processed(u64::from(!unit.raw.is_empty()),u64::from(matches!(unit.destination,Ok(Destination::Sample(_)))));
                match &unit.destination {
                    Ok(Destination::Sample(_)) => {
                        state.counts[1] += 1;
                        for r in &unit.routed {
                            state.accepted.observe_validated(
                                r.sequence.as_bytes(),
                                r.qual_seq.as_bytes(),
                                33,
                            );
                        }
                    }
                    Ok(Destination::Ambiguous) => state.counts[2] += 1,
                    Ok(Destination::Unassigned(_)) => state.counts[3] += 1,
                    Err(error) => {
                        state.error = Some(error.clone());
                        return Err(error.failure(format!(
                            "{} {} ordinal {}: {}",
                            if error.code == 3 {
                                "[input]"
                            } else {
                                "[worker]"
                            },
                            error.stage,
                            error.ordinal,
                            error.message
                        )));
                    }
                }
            }
            if let Some(error)=&unit.output_error{state.error=Some(error.clone());return Err(error.failure(format!("[input] {} ordinal {}: {}",error.stage,error.ordinal,error.message)));}
            let mut written = false;
            for (index, bytes) in unit.payload {
                if let Err(error) = io.write(&self.channels[index].name, &bytes) {
                    state.error_lane=unit.lane_index;
                    state.error = Some(Error {
                        ordinal,
                        stage: "write",
                        message: error.to_string(),
                        code: 4,
                    });
                    return Err(Failure::write(format!("[write] {error}")));
                }
                let _permit = cpu.acquire(crate::cpu_budget::Stage::Sink);
                let mate = index % self.mates;
                let record = &unit.routed[mate];
                state.channels[index].observe_validated(
                    record.sequence.as_bytes(),
                    record.qual_seq.as_bytes(),
                    33,
                );
                state.written.observe_validated(
                    record.sequence.as_bytes(),
                    record.qual_seq.as_bytes(),
                    33,
                );
                gp.native_runtime.as_ref().unwrap().appended_records(mate,1);
                if !written {
                    state.counts[4] += 1;
                    written = true;
                }
            }

        }
        Ok(())
    }
    pub(crate) fn process(
        &self,
        mut first: Vec<FastqRecord>,
        mut second: Option<Vec<FastqRecord>>,
        mut indices:Vec<Vec<FastqRecord>>,
        terminal:Option<Error>,
        gp: &crate::params::GlobalParams,
        comp: &crate::ffi::Compressor,
    ) -> Vec<Unit> {
        let mut result = Vec::with_capacity(first.len()+1);
        for (i, mut r1) in first.drain(..).enumerate() {
            let mut reads = vec![r1.clone()];
            if let Some(second) = &mut second {
                reads.push(std::mem::take(&mut second[i]));
            }
            for r in &mut reads {
                if r.input_phred.is_none() {
                    r.input_phred = Some(gp.quality_phred);
                }
            }
            let ordinal = r1.source_index;
            let checked = reads
                .iter_mut()
                .try_for_each(|r| crate::native_kernel::validate_normalize(r, 33));
            let mut routed = reads.clone();
            let mut validated_indices=Vec::new();
            let mut destination = match checked {
                Ok(()) => {
                    routed = reads.clone();
                    if reads.len() == 2
                        && !crate::record::pair_ids_match(&reads[0].seq_id, &reads[1].seq_id)
                    {
                        Err(Error {
                            ordinal,
                            stage: "pair-association",
                            message: "input association mismatch: paired identifiers differ".into(),
                            code: 3,
                        })
                    } else if terminal.as_ref().is_some_and(|e|e.ordinal==ordinal){Err(terminal.as_ref().unwrap().clone())} else {
                        validated_indices=Vec::with_capacity(indices.len());
                        let checked=(||->Result<(),Error>{
                            for column in &mut indices{let Some(index)=column.get_mut(i)else{return Err(Error{ordinal,stage:"index-count",message:"index association mismatch: missing record".into(),code:3});};
                                crate::native_kernel::validate_normalize(index,33).map_err(|message|Error{ordinal,stage:"index-association",message,code:3})?;
                                validated_indices.push(index.clone());
                                if !crate::record::pair_ids_match(&reads[0].seq_id,&index.seq_id){return Err(Error{ordinal,stage:"index-association",message:"index association mismatch".into(),code:3});}
                            }Ok(())})();
                        checked.and_then(|_|self.table.route(&mut routed,if self.index_source{Some(&validated_indices)}else{None},&self.settings,33).map_err(|message|Error{ordinal,stage:if self.index_source{"index-association"}else{"barcode-metadata"},message,code:if self.index_source{3}else{5}}))
                    }
                }
                Err(message) => {
                    reads.clear();
                    Err(Error {
                        ordinal,
                        stage: "input",
                        message,
                        code: 3,
                    })
                }
            };
            let channel = match &destination {
                Ok(Destination::Sample(i)) => Some(i * self.mates),
                Ok(Destination::Ambiguous) => self.ambiguous,
                Ok(Destination::Unassigned(_)) => self.unassigned,
                _ => None,
            };
            let mut payload = Vec::with_capacity(self.mates);
            let mut output_error=None;
            if let Some(channel) = channel {
                for (mate, read) in routed.iter().enumerate() {
                    let index = channel + mate;
                    let mut raw = Vec::with_capacity(
                        read.seq_id.len() + read.sequence.len() + read.qual_seq.len() + 8,
                    );
                    if let Err(message) = crate::output::serialize_native_record(
                        read,
                        false,
                        gp,
                        &self.channels[index].name,
                        &mut raw,
                    ) {
                        output_error = Some(Error {
                            ordinal,
                            stage: "quality-output",
                            message,
                            code: 3,
                        });
                        payload.clear();
                        break;
                    }
                    let bytes = if self.channels[index].codec == "gzip" {
                        comp.gzip_compress_alloc(&raw)
                    } else {
                        raw
                    };
                    payload.push((index, bytes));
                }
            }
            let failed = destination.is_err() || output_error.is_some();
            result.push(Unit {
                raw: reads,
                indices:validated_indices,lane_index:None,
                routed,
                destination,
                payload,
                output_error,
            });
            if failed {
                break;
            }
        }
        if let Some(error)=terminal{if result.last().is_none_or(|u|u.raw.first().is_none_or(|r|r.source_index<error.ordinal)){
            result.push(Unit{raw:Vec::new(),indices:Vec::new(),lane_index:None,routed:Vec::new(),destination:Err(error),payload:Vec::new(),output_error:None});
        }}
        result
    }
}

#[cfg(test)]mod tests{
use super::*;
fn row(i:usize)->FastqRecord{let mut r=FastqRecord::new(1);r.seq_id=format!("@u{i}");r.sequence="AC".into();r.qual_seq="II".into();r.input_phred=Some(33);r.source_index=i as u64;r}
fn payload(r:&FastqRecord)->usize{r.seq_id.capacity()+r.sequence.capacity()+r.qual_seq.capacity()}
#[test]fn retained_units_fit_explicit_slots_at_growth_boundaries(){
 let table=Arc::new(Table::validate(vec![crate::demux::Sample{name:"S".into(),barcodes:vec!["A".into()]}]).unwrap());
 let d=Runtime{table,settings:Settings{ranges:vec![[0,1]],errors:0,min_q:None,max_n:0,remove:true},channels:vec![Channel{name:"sample/S.R1".into(),path:"sample.fastq".into(),codec:"plain".into(),phred:33}],mates:1,index_source:false,unassigned:None,ambiguous:None,stage:PathBuf::new(),level:4,state:Mutex::new(None),metadata:Mutex::new(None)};
 let mut gp=crate::params::GlobalParams::new();gp.quality_phred=33;gp.output_quality_phred=33;let comp=crate::ffi::Compressor::new(4).unwrap();
 for n in [0,1,2,3,7,8,9,31,32,33]{let input=(0..n).map(row).collect::<Vec<_>>();let source_slots=input.capacity()*std::mem::size_of::<FastqRecord>();let raw=input.iter().map(|r|r.seq_id.len()+r.sequence.len()+r.qual_seq.len()+6).sum::<usize>();let out=d.process(input,None,Vec::new(),None,&gp,&comp);
 let retained=out.capacity()*std::mem::size_of::<Unit>()+out.iter().map(|u|u.raw.capacity()*std::mem::size_of::<FastqRecord>()+u.routed.capacity()*std::mem::size_of::<FastqRecord>()+u.indices.capacity()*std::mem::size_of::<FastqRecord>()+u.payload.capacity()*std::mem::size_of::<(usize,Vec<u8>)>()+u.raw.iter().chain(&u.routed).chain(&u.indices).map(payload).sum::<usize>()+u.payload.iter().map(|(_,p)|p.capacity()).sum::<usize>()).sum::<usize>();
 let extra=Runtime::feature_bound(raw,n).unwrap();let base=crate::native_execution::Execution::reservation(raw,n).unwrap();assert!(retained+source_slots<base+extra,"{n}: {retained}+{source_slots} vs {base}+{extra}");assert_eq!(out.len(),n);for u in out{assert_eq!(u.routed[0].sequence,"C");assert!(u.routed[0].seq_id.ends_with("BC:Z:A"));}
 }
 assert!(Runtime::feature_bound(usize::MAX,1).is_err());assert!(Runtime::feature_bound(0,usize::MAX).is_err());assert!(Runtime::statistics_bound(usize::MAX).is_err());
}
#[test]fn aggregate_statistics_exact_capacities_and_fanout_admission(){
 let mut qs=(0..69).map(|_|Qc::default()).collect::<Vec<_>>();for q in &mut qs{q.observe_validated(b"A",b"I",33);let a=q.cycles.as_ptr();let b=q.lengths.as_ptr();q.observe_validated(&vec![b'N';1000],&vec![b'I';1000],33);assert_eq!(a,q.cycles.as_ptr());assert_eq!(b,q.lengths.as_ptr());}
 assert!(qs.iter().map(Qc::allocated_bytes).sum::<usize>()<=Runtime::statistics_bound(64).unwrap());
 assert!(crate::native_execution::Execution::validate_budget_with_statistics(32,1,Some(256<<20),Runtime::statistics_bound(64).unwrap()).is_ok());
 assert!(crate::native_execution::Execution::validate_budget_with_statistics(32,1,None,Runtime::statistics_bound(200).unwrap()).is_err());
}
}
