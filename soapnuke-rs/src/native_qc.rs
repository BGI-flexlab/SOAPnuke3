use crate::stats::GeneralStat;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Cycle {
    pub bases: [u64; 6],
    pub qualities: [u64; 94],
}
impl Default for Cycle {
    fn default() -> Self {
        Self {
            bases: [0; 6],
            qualities: [0; 94],
        }
    }
}
pub struct Qc {
    pub reads: u64,
    pub bases: [u64; 6],
    pub qualities: [u64; 94],
    pub lengths: Vec<u64>,
    pub cycles: Vec<Cycle>,
    batch_maximum: Option<usize>,
}
impl Default for Qc {
    fn default() -> Self {
        Self {
            reads: 0,
            bases: [0; 6],
            qualities: [0; 94],
            lengths: Vec::new(),
            cycles: Vec::new(),
            batch_maximum: None,
        }
    }
}
impl Clone for Qc {
    fn clone(&self) -> Self {
        let mut copy = Self::default();
        copy.reads = self.reads;
        copy.bases = self.bases;
        copy.qualities = self.qualities;
        copy.batch_maximum = self.batch_maximum;
        if !self.lengths.is_empty() {
            copy.reserve(self.lengths.len() - 1, self.cycles.len());
        }
        copy.lengths.extend_from_slice(&self.lengths);
        copy.cycles.extend_from_slice(&self.cycles);
        copy
    }
}
impl Qc {
    pub fn batch() -> Self {
        Self {
            batch_maximum: Some(crate::params::READ_MAX_LEN),
            ..Self::default()
        }
    }
    pub fn merged() -> Self {
        Self {
            batch_maximum: Some(crate::native_overlap::MERGED_MAX_LEN),
            ..Self::default()
        }
    }
    pub fn set_merged_maximum(&mut self, maximum: usize) {
        assert!(self.reads == 0 && self.lengths.capacity() == 0 && self.cycles.capacity() == 0);
        assert!(maximum <= crate::native_overlap::MERGED_MAX_LEN);
        self.batch_maximum = Some(maximum);
    }
    pub const fn merged_maximum_heap_bytes() -> usize {
        (crate::native_overlap::MERGED_MAX_LEN + 1) * std::mem::size_of::<u64>()
            + crate::native_overlap::MERGED_MAX_LEN * std::mem::size_of::<Cycle>()
    }
    pub fn set_batch_maximum(&mut self, maximum: usize) {
        if self.batch_maximum.is_some() {
            assert!(self.reads == 0 && self.lengths.capacity() == 0 && self.cycles.capacity() == 0);
            self.batch_maximum = Some(maximum.min(crate::params::READ_MAX_LEN));
        }
    }
    pub const fn maximum_heap_bytes() -> usize {
        (crate::params::READ_MAX_LEN + 1) * std::mem::size_of::<u64>()
            + crate::params::READ_MAX_LEN * std::mem::size_of::<Cycle>()
    }
    pub fn allocated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.lengths.capacity() * std::mem::size_of::<u64>()
            + self.cycles.capacity() * std::mem::size_of::<Cycle>()
    }
    fn reserve(&mut self, length: usize, cycles: usize) {
        // Reserve the supported capacity once, before observing/merging counters.
        // Subsequent growth never overlaps an old and new backing allocation.
        let maximum = self.batch_maximum.unwrap_or(crate::params::READ_MAX_LEN);
        assert!(length <= maximum && cycles <= maximum);
        if self.lengths.capacity() == 0 {
            self.lengths.reserve_exact(maximum + 1);
        }
        if cycles > 0 && self.cycles.capacity() == 0 {
            self.cycles.reserve_exact(maximum);
        }
    }
    pub fn observe_validated(&mut self, sequence: &[u8], quality: &[u8], phred: i32) {
        let length = sequence.len();
        self.reserve(length, length);
        if self.lengths.len() <= length {
            self.lengths.resize(length + 1, 0);
        }
        if self.cycles.len() < length {
            self.cycles.resize_with(length, Cycle::default);
        }
        self.reads += 1;
        self.lengths[length] += 1;
        for (cycle, (&base, &encoded)) in self.cycles.iter_mut().zip(sequence.iter().zip(quality)) {
            let base = match base.to_ascii_uppercase() {
                b'A' => 0,
                b'C' => 1,
                b'G' => 2,
                b'T' => 3,
                b'N' => 4,
                _ => 5,
            };
            let q = (encoded as i32 - phred) as usize;
            self.bases[base] += 1;
            self.qualities[q] += 1;
            cycle.bases[base] += 1;
            cycle.qualities[q] += 1;
        }
    }
    pub(crate) fn reset_counts(&mut self) {
        self.reads = 0;
        self.bases.fill(0);
        self.qualities.fill(0);
        self.lengths.clear();
        self.cycles.clear();
    }
    pub fn merge(&mut self, other: &Self) {
        if !other.lengths.is_empty() {
            self.reserve(other.lengths.len() - 1, other.cycles.len());
        }
        self.reads += other.reads;
        for (a, b) in self.bases.iter_mut().zip(other.bases) {
            *a += b;
        }
        for (a, b) in self.qualities.iter_mut().zip(other.qualities) {
            *a += b;
        }
        if self.lengths.len() < other.lengths.len() {
            self.lengths.resize(other.lengths.len(), 0);
        }
        for (a, b) in self.lengths.iter_mut().zip(&other.lengths) {
            *a += b;
        }
        if self.cycles.len() < other.cycles.len() {
            self.cycles.resize_with(other.cycles.len(), Cycle::default);
        }
        for (a, b) in self.cycles.iter_mut().zip(&other.cycles) {
            for (x, y) in a.bases.iter_mut().zip(b.bases) {
                *x += y;
            }
            for (x, y) in a.qualities.iter_mut().zip(b.qualities) {
                *x += y;
            }
        }
    }
    pub fn general(&self) -> GeneralStat {
        let max = self.lengths.iter().rposition(|&n| n > 0).unwrap_or(0) as u64;
        GeneralStat {
            reads_number: self.reads,
            base_number: self.bases.iter().sum(),
            read_length: max,
            read_max_length: max,
            a_number: self.bases[0],
            c_number: self.bases[1],
            g_number: self.bases[2],
            t_number: self.bases[3],
            n_number: self.bases[4],
            q20_num: self.qualities[20..].iter().sum(),
            q30_num: self.qualities[30..].iter().sum(),
        }
    }
    pub fn metrics(&self, stage: &str, channel: &str, units: u64, scope: &str) -> Value {
        let bases: u64 = self.bases.iter().sum();
        let canonical: u64 = self.bases[..4].iter().sum();
        let minimum = self.lengths.iter().position(|&n| n > 0);
        let maximum = self.lengths.iter().rposition(|&n| n > 0);
        let at_rank = |rank: u64| -> usize {
            let mut sum = 0;
            for (length, &count) in self.lengths.iter().enumerate() {
                sum += count;
                if sum > rank {
                    return length;
                }
            }
            0
        };
        let median = if self.reads == 0 {
            None
        } else {
            Some((at_rank((self.reads - 1) / 2) as f64 + at_rank(self.reads / 2) as f64) / 2.0)
        };
        let cycles: Vec<_>=self.cycles.iter().enumerate().map(|(i,cycle)|{
            let denominator: u64=cycle.bases.iter().sum();let sum:u64=cycle.qualities.iter().enumerate().map(|(q,&n)|q as u64*n).sum();
            json!({"cycle":i+1,"read_records":denominator,"base_counts":base_counts(cycle.bases),"quality_histogram":cycle.qualities.as_slice(),"mean_quality":fraction(sum,denominator,"Phred score per reaching record",stage,channel),"q20_fraction":fraction(cycle.qualities[20..].iter().sum(),denominator,"bases",stage,channel),"q30_fraction":fraction(cycle.qualities[30..].iter().sum(),denominator,"bases",stage,channel)})
        }).collect();
        json!({"source_units":units,"read_records":self.reads,"bases":bases,"n_bases":self.bases[4],"q20_bases":self.qualities[20..].iter().sum::<u64>(),"q30_bases":self.qualities[30..].iter().sum::<u64>(),"scope":scope,"stage":stage,"channel":channel,"base_counts":base_counts(self.bases),"n_fraction":fraction(self.bases[4],bases,"bases",stage,channel),"gc_canonical_fraction":fraction(self.bases[1]+self.bases[2],canonical,"canonical bases",stage,channel),"q20_fraction":fraction(self.qualities[20..].iter().sum(),bases,"bases",stage,channel),"q30_fraction":fraction(self.qualities[30..].iter().sum(),bases,"bases",stage,channel),"quality_histogram":self.qualities.as_slice(),"length":{"histogram":self.lengths,"minimum":minimum,"maximum":maximum,"mean":fraction(bases,self.reads,"bases per read record",stage,channel),"median":median,"median_unavailable_reason":if median.is_none(){Some("no observed records")}else{None}},"cycles":cycles,"observation":"complete validated records only; zero other does not describe unseen or invalid records"})
    }
}
fn base_counts(b: [u64; 6]) -> Value {
    json!({"A":b[0],"C":b[1],"G":b[2],"T":b[3],"N":b[4],"other":b[5]})
}
pub fn fraction(numerator: u64, denominator: u64, unit: &str, stage: &str, channel: &str) -> Value {
    json!({"numerator":numerator,"denominator":denominator,"unit":unit,"stage":stage,"channel":channel,"value":if denominator==0{None}else{Some(numerator as f64/denominator as f64)},"unavailable_reason":if denominator==0{Some("zero denominator")}else{None}})
}

pub fn population(qcs: &[&Qc], stage: &str, units: u64, scope: &str, written: bool) -> Value {
    let mut total = if qcs
        .iter()
        .any(|q| q.cycles.len() > crate::params::READ_MAX_LEN)
    {
        Qc::merged()
    } else {
        Qc::default()
    };
    let mut channels = Vec::new();
    for (i, qc) in qcs.iter().enumerate() {
        total.merge(qc);
        channels.push(qc.metrics(stage, ["r1", "r2", "merged"][i], qc.reads, scope));
    }
    let mut value = total.metrics(stage, "all", units, scope);
    value[if written { "channels" } else { "mates" }] = Value::Array(channels);
    value
}
pub fn written_population(qcs: &[&Qc], units: u64, scope: &str, interleaved: bool) -> Value {
    let mut pop = population(qcs, "written", units, scope, true);
    if interleaved {
        let mut channel = pop.clone();
        channel.as_object_mut().unwrap().remove("channels");
        fn rename(value: &mut Value) {
            match value {
                Value::Object(map) => {
                    for (key, value) in map {
                        if key == "channel" && value.as_str() == Some("all") {
                            *value = json!("r1");
                        } else {
                            rename(value);
                        }
                    }
                }
                Value::Array(a) => {
                    for v in a {
                        rename(v)
                    }
                }
                _ => {}
            }
        }
        rename(&mut channel);
        pop["channels"] = json!([channel]);
    }
    pop
}
pub fn summary(sample: &str, populations: &Value) -> String {
    let mut text=format!("SOAPnuke QC sample={sample:?}; observations only; output-set completion requires the native report marker\n");
    for stage in ["input", "accepted", "written"] {
        let v = &populations[stage];
        if v.is_null(){text.push_str(&format!("{stage}\tstatus=not-applicable\treason=QC-only input inspection\n"));continue;}
        text.push_str(&format!("{stage}\treads={}\tbases={}\tlength_min={}\tlength_max={}\tlength_mean={}\tlength_median={}\tN={}\tQ20={}\tQ30={}\n",v["read_records"],v["bases"],v["length"]["minimum"],v["length"]["maximum"],v["length"]["mean"]["value"],v["length"]["median"],v["n_bases"],v["q20_bases"],v["q30_bases"]));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn interleaved_written_channel_retains_pair_unit_and_numeric_qc_meanings() {
        let mut a = Qc::default();
        let mut b = Qc::default();
        for _ in 0..2 {
            a.observe_validated(b"AC", b"II", 33);
            b.observe_validated(b"GT", b"II", 33);
        }
        let p = written_population(&[&a, &b], 2, "complete", true);
        assert_eq!(p["source_units"], 2);
        assert_eq!(p["read_records"], 4);
        assert_eq!(p["bases"], 8);
        assert_eq!(p["channels"].as_array().unwrap().len(), 1);
        assert_eq!(p["channels"][0]["source_units"], 2);
        assert_eq!(p["channels"][0]["read_records"], 4);
        assert_eq!(p["channels"][0]["quality_histogram"][40], 8);
        assert_eq!(p["channels"][0]["q20_fraction"]["channel"], "r1");
    }
    #[test]
    fn maximum_capacities_do_not_reallocate_during_growth_or_merge() {
        let mut qc = Qc::default();
        assert_eq!(qc.allocated_bytes(), std::mem::size_of::<Qc>());
        qc.observe_validated(b"A", b"!", 33);
        let length_ptr = qc.lengths.as_ptr();
        let cycle_ptr = qc.cycles.as_ptr();
        for length in [0, 2, 93, 127, 999, 1000] {
            let sequence = vec![b'N'; length];
            let quality: Vec<_> = (0..length).map(|i| 33 + (i % 94) as u8).collect();
            qc.observe_validated(&sequence, &quality, 33);
            assert_eq!(qc.lengths.as_ptr(), length_ptr);
            assert_eq!(qc.cycles.as_ptr(), cycle_ptr);
        }
        assert_eq!(qc.lengths.capacity(), 1001);
        assert_eq!(qc.cycles.capacity(), 1000);
        assert_eq!(
            qc.allocated_bytes(),
            std::mem::size_of::<Qc>() + Qc::maximum_heap_bytes()
        );
        let snapshot = qc.clone();
        qc.merge(&snapshot);
        assert_eq!(qc.lengths.as_ptr(), length_ptr);
        assert_eq!(qc.cycles.as_ptr(), cycle_ptr);
        assert_eq!(qc.reads, 14);
        assert_eq!(snapshot.lengths.capacity(), 1001);
        assert_eq!(snapshot.cycles.capacity(), 1000);
        assert_eq!(Qc::default().clone().lengths.capacity(), 0);
    }

    #[test]
    fn batch_qc_pointer_stability_respects_its_fixed_capacity_and_empty_records() {
        for maximum in [0usize, 150, 1000] {
            let mut qc = Qc::batch();
            qc.set_batch_maximum(maximum);
            qc.observe_validated(b"", b"", 33);
            let lengths = qc.lengths.as_ptr();
            if maximum > 0 {
                qc.observe_validated(b"A", b"!", 33);
                let cycles = qc.cycles.as_ptr();
                let sequence = vec![b'A'; maximum];
                let quality: Vec<_> = (0..maximum).map(|i| 33 + (i % 94) as u8).collect();
                qc.observe_validated(&sequence, &quality, 33);
                assert_eq!(qc.cycles.as_ptr(), cycles);
                assert_eq!(qc.cycles.capacity(), maximum);
            }
            assert_eq!(qc.lengths.as_ptr(), lengths);
            assert_eq!(qc.lengths.capacity(), maximum + 1);
            let copy = qc.clone();
            assert_eq!(copy.lengths.capacity(), maximum + 1);
            assert_eq!(copy.cycles.capacity(), maximum);
            assert!(qc.allocated_bytes() <= crate::native_execution::QC_BYTES);
        }
        let mut persistent = Qc::default();
        persistent.set_batch_maximum(150);
        persistent.observe_validated(b"A", b"I", 33);
        assert_eq!(persistent.cycles.capacity(), 1000);
    }

    #[test]
    fn deterministic_merge_and_cycle_length_reference() {
        for seed in [
            2026102301u64,
            2026102302,
            2026102303,
            2026102304,
            2026102305,
        ] {
            let mut state = seed;
            for _ in 0..10_000 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let mut direct = Qc::default();
                let mut parts = [Qc::default(), Qc::default()];
                let mut lengths = Vec::new();
                let mut reference_bases = [0u64; 6];
                let mut reference_quality = [0u64; 94];
                for i in 0..(state as usize % 9) {
                    let length = (state.rotate_left(i as u32) as usize) % 31;
                    let seq: Vec<_> = (0..length).map(|j| b"ACGTN"[(j + i) % 5]).collect();
                    let qualities: Vec<_> = (0..length)
                        .map(|j| ((state >> (j % 32)) % 94) as u8 + 33)
                        .collect();
                    direct.observe_validated(&seq, &qualities, 33);
                    parts[i % 2].observe_validated(&seq, &qualities, 33);
                    lengths.push(length);
                    for (b, q) in seq.into_iter().zip(qualities) {
                        reference_bases[b"ACGTN".iter().position(|v| *v == b).unwrap()] += 1;
                        reference_quality[(q - 33) as usize] += 1;
                    }
                }
                parts[0].merge(&parts[1].clone());
                assert_eq!(direct.bases, reference_bases);
                assert_eq!(direct.qualities, reference_quality);
                assert_eq!(
                    direct.metrics("input", "r1", direct.reads, "complete"),
                    parts[0].metrics("input", "r1", direct.reads, "complete")
                );
                for (length, &frequency) in direct.lengths.iter().enumerate() {
                    assert_eq!(
                        frequency,
                        lengths.iter().filter(|&&v| v == length).count() as u64
                    );
                }
                for (cycle, counts) in direct.cycles.iter().enumerate() {
                    assert_eq!(
                        counts.bases.iter().sum::<u64>(),
                        lengths.iter().filter(|&&n| n > cycle).count() as u64
                    );
                }
            }
        }
    }
    #[test]
    fn literal_cycles_lengths_and_empty_denominators() {
        let mut q = Qc::default();
        q.observe_validated(b"AC", b"!I", 33);
        q.observe_validated(b"N", b"5", 33);
        q.observe_validated(b"", b"", 33);
        let m = q.metrics("input", "r1", 3, "complete");
        assert_eq!(m["length"]["histogram"], json!([1, 1, 1]));
        assert_eq!(m["length"]["median"], 1.0);
        assert_eq!(m["length"]["mean"]["value"], 1.0);
        assert_eq!(m["cycles"][0]["read_records"], 2);
        assert_eq!(m["cycles"][1]["read_records"], 1);
        assert_eq!(m["cycles"][0]["quality_histogram"][20], 1);
        assert_eq!(m["gc_canonical_fraction"]["numerator"], 1);
        assert_eq!(m["gc_canonical_fraction"]["denominator"], 2);
        let mut empty = Qc::default();
        assert!(empty.metrics("input", "r1", 0, "complete")["length"]["median"].is_null());
        empty.observe_validated(b"", b"", 33);
        let m = empty.metrics("input", "r1", 1, "complete");
        assert_eq!(m["length"]["median"], 0.0);
        assert!(m["n_fraction"]["value"].is_null());
        assert_eq!(m["length"]["mean"]["value"], 0.0);
    }
    #[test]
    fn integer_counts_above_two_to_53_and_even_median() {
        let mut q = Qc::default();
        q.reads = (1 << 53) + 1;
        q.lengths = vec![0, q.reads];
        q.bases[0] = q.reads;
        q.qualities[40] = q.reads;
        let m = q.metrics("input", "r1", q.reads, "complete");
        let bytes = serde_json::to_string(&m).unwrap();
        assert!(bytes.contains("9007199254740993"));
        assert_eq!(m["read_records"].as_u64(), Some(q.reads));
        let mut q = Qc::default();
        q.observe_validated(b"A", b"I", 33);
        q.observe_validated(b"AAA", b"III", 33);
        assert_eq!(
            q.metrics("accepted", "r1", 2, "complete")["length"]["median"],
            2.0
        );
    }
}

#[cfg(test)]
mod r66_tests {
    use super::*;
    #[test]
    fn r66_reset_retains_capacity_and_zeroes_every_counter() {
        for maximum in [0,150,1000] {
            let mut q=Qc::batch();q.set_batch_maximum(maximum);q.observe_validated(&vec![b'A';maximum],&vec![b'I';maximum],33);
            let pointers=(q.lengths.as_ptr(),q.cycles.as_ptr());let allocated=q.allocated_bytes();let capacities=(q.lengths.capacity(),q.cycles.capacity());
            q.reset_counts();assert_eq!(q.reads,0);assert_eq!(q.bases,[0;6]);assert_eq!(q.qualities,[0;94]);assert!(q.lengths.is_empty()&&q.cycles.is_empty());assert_eq!(q.allocated_bytes(),allocated);assert_eq!((q.lengths.capacity(),q.cycles.capacity()),capacities);
            q.observe_validated(&vec![b'G';maximum],&vec![b'5';maximum],33);assert_eq!(q.reads,1);assert_eq!(q.bases[2],maximum as u64);assert_eq!(q.qualities[20],maximum as u64);assert_eq!(q.lengths[maximum],1);assert_eq!((q.lengths.as_ptr(),q.cycles.as_ptr()),pointers);
            for cycle in &q.cycles {assert_eq!(cycle.bases,[0,0,1,0,0,0]);assert_eq!(cycle.qualities[20],1);assert_eq!(cycle.qualities[40],0);}
        }
    }
}
