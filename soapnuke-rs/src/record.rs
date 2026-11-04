// FastqRecord: exact equivalent of C_fastq in the v2 codebase.
pub const UNASSIGNED_SOURCE_INDEX: u64 = u64::MAX;

#[derive(Clone)]
pub struct FastqRecord {
    pub(crate) input_phred: Option<i32>,
    pub source_index: u64,
    pub seq_id: String,
    pub sequence: String,
    pub qual_seq: String,
    pub adapter_orientation: String,
    pub adapter_orientation2: String,
    pub head_hdcut: i32,
    pub head_lqcut: i32,
    pub tail_hdcut: i32,
    pub tail_lqcut: i32,
    pub adacut_pos: i32,
    pub contam_pos: i32,
    pub global_contam_5pos: i32,
    pub global_contam_3pos: i32,
    pub raw_length: i32,
    pub pair_pos: i32,
}

impl Default for FastqRecord {
    fn default() -> FastqRecord {
        FastqRecord::new(1)
    }
}

impl FastqRecord {
    pub fn new(pair_position: i32) -> FastqRecord {
        FastqRecord {
            input_phred: None,
            source_index: UNASSIGNED_SOURCE_INDEX,
            seq_id: String::new(),
            sequence: String::new(),
            qual_seq: String::new(),
            adapter_orientation: String::new(),
            adapter_orientation2: String::new(),
            head_hdcut: -1,
            head_lqcut: -1,
            tail_hdcut: -1,
            tail_lqcut: -1,
            adacut_pos: -1,
            contam_pos: -1,
            global_contam_5pos: -1,
            global_contam_3pos: -1,
            raw_length: 0,
            pair_pos: pair_position,
        }
    }

    pub fn to_string(&self) -> String {
        let mut s = String::with_capacity(
            self.seq_id.len() + self.sequence.len() + self.qual_seq.len() + 4,
        );
        s.push_str(&self.seq_id);
        s.push('\n');
        s.push_str(&self.sequence);
        s.push('\n');
        s.push('+');
        s.push('\n');
        s.push_str(&self.qual_seq);
        s.push('\n');
        s
    }
}

// A slice-based record view for parsing (zero-copy until owned conversion).
pub fn pair_id_token_length(id: &str) -> usize {
    let mut length = id.len();
    for (idx, ch) in id.char_indices() {
        if ch == ' ' || ch == '\t' {
            length = idx;
            break;
        }
    }
    let bytes = id.as_bytes();
    if length >= 2
        && bytes[length - 2] == b'/'
        && (bytes[length - 1] == b'1' || bytes[length - 1] == b'2')
    {
        length -= 2;
    }
    length
}

pub fn pair_ids_match(read1: &str, read2: &str) -> bool {
    let l1 = pair_id_token_length(read1);
    let l2 = pair_id_token_length(read2);
    l1 == l2 && read1.as_bytes()[..l1] == read2.as_bytes()[..l2]
}
