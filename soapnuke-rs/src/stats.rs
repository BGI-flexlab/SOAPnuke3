// Statistics structures, exact equivalents of C_fastq_file_stat / C_filter_stat.
use crate::params::GlobalParams;
use crate::params::READ_MAX_LEN;

#[derive(Clone, Default)]
pub struct GeneralStat {
    pub reads_number: u64,
    pub base_number: u64,
    pub read_length: u64,
    pub read_max_length: u64,
    pub a_number: u64,
    pub c_number: u64,
    pub g_number: u64,
    pub t_number: u64,
    pub n_number: u64,
    pub q20_num: u64,
    pub q30_num: u64,
}

#[derive(Clone, Default)]
pub struct PosBaseStat {
    pub position_acgt_content: Vec<[u64; 5]>,
}

impl PosBaseStat {
    pub fn new() -> PosBaseStat {
        PosBaseStat {
            position_acgt_content: vec![[0; 5]; READ_MAX_LEN],
        }
    }
}

#[derive(Clone)]
pub struct PosQualStat {
    pub quality_bins: usize,
    pub position_qual: Vec<u64>,
}

impl PosQualStat {
    pub fn new(gp: &GlobalParams) -> PosQualStat {
        PosQualStat {
            quality_bins: gp.quality_bins(),
            position_qual: if gp.native_m1 {
                Vec::new()
            } else {
                vec![0u64; READ_MAX_LEN * gp.quality_bins()]
            },
        }
    }
    #[inline]
    pub fn at(&mut self, i: usize, j: usize) -> &mut u64 {
        &mut self.position_qual[i * self.quality_bins + j]
    }
    #[inline]
    pub fn row_data(&self, i: usize) -> &[u64] {
        &self.position_qual[i * self.quality_bins..(i + 1) * self.quality_bins]
    }
}

#[derive(Clone, Default)]
pub struct TrimStat {
    pub ht: Vec<u64>,
    pub hlq: Vec<u64>,
    pub tt: Vec<u64>,
    pub tlq: Vec<u64>,
    pub ta: Vec<u64>,
}

impl TrimStat {
    pub fn new() -> TrimStat {
        TrimStat {
            ht: vec![0; READ_MAX_LEN + 1],
            hlq: vec![0; READ_MAX_LEN + 1],
            tt: vec![0; READ_MAX_LEN + 1],
            tlq: vec![0; READ_MAX_LEN + 1],
            ta: vec![0; READ_MAX_LEN + 1],
        }
    }
}

#[derive(Clone)]
pub struct FileStat {
    pub native_qc: Option<Box<crate::native_qc::Qc>>,
    pub gs: GeneralStat,
    pub bs: PosBaseStat,
    pub qs: PosQualStat,
    pub ts: TrimStat,
}

impl FileStat {
    pub fn new(gp: &GlobalParams) -> FileStat {
        FileStat {
            native_qc: if gp.native_m1 {
                Some(Box::new(crate::native_qc::Qc::default()))
            } else {
                None
            },
            gs: GeneralStat::default(),
            bs: if gp.native_m1 {
                PosBaseStat::default()
            } else {
                PosBaseStat::new()
            },
            qs: PosQualStat::new(gp),
            ts: if gp.native_m1 {
                TrimStat::default()
            } else {
                TrimStat::new()
            },
        }
    }
}

#[derive(Clone, Default)]
pub struct FilterStat {
    pub native: crate::native_processing::FateStats,
    pub in_adapter_list_num: u64,
    pub include_adapter_seq_num: u64,
    pub include_adapter_seq_num1: u64,
    pub include_adapter_seq_num2: u64,
    pub include_adapter_seq_num_overlap: u64,
    pub n_ratio_num: u64,
    pub n_ratio_num1: u64,
    pub n_ratio_num2: u64,
    pub n_ratio_num_overlap: u64,
    pub high_a_num: u64,
    pub high_a_num1: u64,
    pub high_a_num2: u64,
    pub high_a_num_overlap: u64,
    pub poly_x_num: u64,
    pub poly_x_num1: u64,
    pub poly_x_num2: u64,
    pub poly_x_num_overlap: u64,
    pub tile_num: u64,
    pub fov_num: u64,
    pub over_lapped_num: u64,
    pub low_qual_base_ratio_num: u64,
    pub low_qual_base_ratio_num1: u64,
    pub low_qual_base_ratio_num2: u64,
    pub low_qual_base_ratio_num_overlap: u64,
    pub mean_quality_num: u64,
    pub mean_quality_num1: u64,
    pub mean_quality_num2: u64,
    pub mean_quality_num_overlap: u64,
    pub short_len_num: u64,
    pub short_len_num1: u64,
    pub short_len_num2: u64,
    pub short_len_num_overlap: u64,
    pub long_len_num: u64,
    pub long_len_num1: u64,
    pub long_len_num2: u64,
    pub long_len_num_overlap: u64,
    pub include_contam_seq_num: u64,
    pub include_contam_seq_num1: u64,
    pub include_contam_seq_num2: u64,
    pub include_contam_seq_num_overlap: u64,
    pub include_global_contam_seq_num: u64,
    pub include_global_contam_seq_num1: u64,
    pub include_global_contam_seq_num2: u64,
    pub include_global_contam_seq_num_overlap: u64,
    pub filtered_base_num1: u64,
    pub filtered_base_num2: u64,
    pub no_3_adapter_num: u64,
    pub int_insert_null_num: u64,
}

// Per-worker local stats bundle (PE).
#[derive(Clone)]
pub struct PeLocalStats {
    pub raw1: FileStat,
    pub raw2: FileStat,
    pub trim1: FileStat,
    pub trim2: FileStat,
    pub clean1: FileStat,
    pub clean2: FileStat,
    pub fs: FilterStat,
}

impl PeLocalStats {
    pub fn new(gp: &GlobalParams) -> PeLocalStats {
        PeLocalStats {
            raw1: FileStat::new(gp),
            raw2: FileStat::new(gp),
            trim1: FileStat::new(gp),
            trim2: FileStat::new(gp),
            clean1: FileStat::new(gp),
            clean2: FileStat::new(gp),
            fs: FilterStat::default(),
        }
    }
}

// Global stats container (gv equivalent).
pub struct GlobalStats {
    pub raw1: FileStat,
    pub raw2: FileStat,
    pub trim1: FileStat,
    pub trim2: FileStat,
    pub clean1: FileStat,
    pub clean2: FileStat,
    pub fs: FilterStat,
}

impl GlobalStats {
    pub fn new(gp: &GlobalParams) -> GlobalStats {
        GlobalStats {
            raw1: FileStat::new(gp),
            raw2: FileStat::new(gp),
            trim1: FileStat::new(gp),
            trim2: FileStat::new(gp),
            clean1: FileStat::new(gp),
            clean2: FileStat::new(gp),
            fs: FilterStat::default(),
        }
    }
}

fn merge_file_stat_arrays(dst: &mut FileStat, src: &FileStat, gp: &GlobalParams) {
    if gp.native_m1 {
        return;
    }
    // base position content
    for i in 0..src.gs.read_max_length as usize {
        let row = &mut dst.bs.position_acgt_content[i];
        let srow = &src.bs.position_acgt_content[i];
        for j in 0..5 {
            row[j] += srow[j];
        }
    }
    // trim stat
    for i in 0..=READ_MAX_LEN {
        dst.ts.ht[i] += src.ts.ht[i];
        dst.ts.hlq[i] += src.ts.hlq[i];
        dst.ts.tt[i] += src.ts.tt[i];
        dst.ts.tlq[i] += src.ts.tlq[i];
        dst.ts.ta[i] += src.ts.ta[i];
    }
    // quality position content
    for i in 0..src.gs.read_max_length as usize {
        let dstart = i * gp.quality_bins();
        let sstart = i * gp.quality_bins();
        for j in 0..=gp.max_base_quality as usize {
            dst.qs.position_qual[dstart + j] += src.qs.position_qual[sstart + j];
        }
    }
}

fn merge_fs(dst: &mut FilterStat, src: &FilterStat) {
    dst.native.merge(&src.native);
    dst.in_adapter_list_num += src.in_adapter_list_num;
    dst.include_adapter_seq_num += src.include_adapter_seq_num;
    dst.include_adapter_seq_num1 += src.include_adapter_seq_num1;
    dst.include_adapter_seq_num2 += src.include_adapter_seq_num2;
    dst.include_adapter_seq_num_overlap += src.include_adapter_seq_num_overlap;
    dst.n_ratio_num += src.n_ratio_num;
    dst.n_ratio_num1 += src.n_ratio_num1;
    dst.n_ratio_num2 += src.n_ratio_num2;
    dst.n_ratio_num_overlap += src.n_ratio_num_overlap;
    dst.high_a_num += src.high_a_num;
    dst.high_a_num1 += src.high_a_num1;
    dst.high_a_num2 += src.high_a_num2;
    dst.high_a_num_overlap += src.high_a_num_overlap;
    dst.poly_x_num += src.poly_x_num;
    dst.poly_x_num1 += src.poly_x_num1;
    dst.poly_x_num2 += src.poly_x_num2;
    dst.poly_x_num_overlap += src.poly_x_num_overlap;
    dst.tile_num += src.tile_num;
    dst.fov_num += src.fov_num;
    dst.over_lapped_num += src.over_lapped_num;
    dst.low_qual_base_ratio_num += src.low_qual_base_ratio_num;
    dst.low_qual_base_ratio_num1 += src.low_qual_base_ratio_num1;
    dst.low_qual_base_ratio_num2 += src.low_qual_base_ratio_num2;
    dst.low_qual_base_ratio_num_overlap += src.low_qual_base_ratio_num_overlap;
    dst.mean_quality_num += src.mean_quality_num;
    dst.mean_quality_num1 += src.mean_quality_num1;
    dst.mean_quality_num2 += src.mean_quality_num2;
    dst.mean_quality_num_overlap += src.mean_quality_num_overlap;
    dst.short_len_num += src.short_len_num;
    dst.short_len_num1 += src.short_len_num1;
    dst.short_len_num2 += src.short_len_num2;
    dst.short_len_num_overlap += src.short_len_num_overlap;
    dst.long_len_num += src.long_len_num;
    dst.long_len_num1 += src.long_len_num1;
    dst.long_len_num2 += src.long_len_num2;
    dst.long_len_num_overlap += src.long_len_num_overlap;
    dst.include_contam_seq_num += src.include_contam_seq_num;
    dst.include_contam_seq_num1 += src.include_contam_seq_num1;
    dst.include_contam_seq_num2 += src.include_contam_seq_num2;
    dst.include_contam_seq_num_overlap += src.include_contam_seq_num_overlap;
    dst.include_global_contam_seq_num += src.include_global_contam_seq_num;
    dst.include_global_contam_seq_num1 += src.include_global_contam_seq_num1;
    dst.include_global_contam_seq_num2 += src.include_global_contam_seq_num2;
    dst.include_global_contam_seq_num_overlap += src.include_global_contam_seq_num_overlap;
    dst.filtered_base_num1 += src.filtered_base_num1;
    dst.filtered_base_num2 += src.filtered_base_num2;
}

fn merge_local_file_stat(
    dst: &mut FileStat,
    src: &FileStat,
    gp: &GlobalParams,
    average_read_length: bool,
) {
    if let (Some(dst), Some(src)) = (&mut dst.native_qc, &src.native_qc) {
        dst.merge(src);
    }
    dst.gs.reads_number += src.gs.reads_number;
    dst.gs.base_number += src.gs.base_number;
    dst.gs.a_number += src.gs.a_number;
    dst.gs.c_number += src.gs.c_number;
    dst.gs.g_number += src.gs.g_number;
    dst.gs.t_number += src.gs.t_number;
    dst.gs.n_number += src.gs.n_number;
    dst.gs.q20_num += src.gs.q20_num;
    dst.gs.q30_num += src.gs.q30_num;
    if average_read_length {
        dst.gs.read_length = if dst.gs.reads_number == 0 {
            0
        } else {
            dst.gs.base_number / dst.gs.reads_number
        };
    } else {
        dst.gs.read_length = dst.gs.read_length.max(src.gs.read_length);
    }
    dst.gs.read_max_length = dst.gs.read_max_length.max(src.gs.read_max_length);
    merge_file_stat_arrays(dst, src, gp);
}

/// Add one batch-local accumulator to a worker accumulator. Streaming needs
/// both values: the batch is sent to the ordered stdout sink, while the
/// worker's cumulative value is retained for the normal report files.
pub fn merge_pe_local(
    dst: &mut PeLocalStats,
    src: &PeLocalStats,
    gp: &GlobalParams,
    trim_enabled: bool,
) {
    merge_local_file_stat(&mut dst.raw1, &src.raw1, gp, false);
    merge_local_file_stat(&mut dst.raw2, &src.raw2, gp, false);
    if trim_enabled {
        merge_local_file_stat(&mut dst.trim1, &src.trim1, gp, true);
        merge_local_file_stat(&mut dst.trim2, &src.trim2, gp, true);
    }
    merge_local_file_stat(&mut dst.clean1, &src.clean1, gp, true);
    merge_local_file_stat(&mut dst.clean2, &src.clean2, gp, true);
    merge_fs(&mut dst.fs, &src.fs);
}

pub fn merge_pe_stat(
    gv: &mut GlobalStats,
    local: &PeLocalStats,
    gp: &GlobalParams,
    trim_enabled: bool,
) {
    for (dst, src) in [
        (&mut gv.raw1, &local.raw1),
        (&mut gv.raw2, &local.raw2),
        (&mut gv.clean1, &local.clean1),
        (&mut gv.clean2, &local.clean2),
    ] {
        if let (Some(dst), Some(src)) = (&mut dst.native_qc, &src.native_qc) {
            dst.merge(src);
        }
    }
    // raw
    {
        let dst = &mut gv.raw1;
        let src = &local.raw1;
        if dst.gs.read_length < src.gs.read_length {
            dst.gs.read_length = src.gs.read_length;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
        let dst = &mut gv.raw2;
        let src = &local.raw2;
        if dst.gs.read_length < src.gs.read_length {
            dst.gs.read_length = src.gs.read_length;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
    }
    // trim
    if trim_enabled {
        let dst = &mut gv.trim1;
        let src = &local.trim1;
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        if dst.gs.reads_number == 0 {
            dst.gs.read_length = src.gs.read_length;
        } else {
            dst.gs.read_length = dst.gs.base_number / dst.gs.reads_number;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
        let dst = &mut gv.trim2;
        let src = &local.trim2;
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        if dst.gs.reads_number == 0 {
            dst.gs.read_length = src.gs.read_length;
        } else {
            dst.gs.read_length = dst.gs.base_number / dst.gs.reads_number;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
    }
    // clean
    {
        let dst = &mut gv.clean1;
        let src = &local.clean1;
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        if dst.gs.reads_number == 0 {
            dst.gs.read_length = src.gs.read_length;
        } else {
            dst.gs.read_length = dst.gs.base_number / dst.gs.reads_number;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
        let dst = &mut gv.clean2;
        let src = &local.clean2;
        dst.gs.reads_number += src.gs.reads_number;
        dst.gs.base_number += src.gs.base_number;
        if dst.gs.reads_number == 0 {
            dst.gs.read_length = src.gs.read_length;
        } else {
            dst.gs.read_length = dst.gs.base_number / dst.gs.reads_number;
        }
        if dst.gs.read_max_length < src.gs.read_max_length {
            dst.gs.read_max_length = src.gs.read_max_length;
        }
        dst.gs.a_number += src.gs.a_number;
        dst.gs.c_number += src.gs.c_number;
        dst.gs.g_number += src.gs.g_number;
        dst.gs.t_number += src.gs.t_number;
        dst.gs.n_number += src.gs.n_number;
        dst.gs.q20_num += src.gs.q20_num;
        dst.gs.q30_num += src.gs.q30_num;
        merge_file_stat_arrays(dst, src, gp);
    }
    merge_fs(&mut gv.fs, &local.fs);
}

pub fn merge_se_stat(
    gv: &mut GlobalStats,
    local: &PeLocalStats,
    gp: &GlobalParams,
    trim_enabled: bool,
) {
    merge_local_file_stat(&mut gv.raw1, &local.raw1, gp, false);
    if trim_enabled {
        merge_local_file_stat(&mut gv.trim1, &local.trim1, gp, true);
        if gv.trim1.gs.reads_number == 0 {
            gv.trim1.gs.read_length = local.trim1.gs.read_length;
        }
    }
    merge_local_file_stat(&mut gv.clean1, &local.clean1, gp, true);
    if gv.clean1.gs.reads_number == 0 {
        gv.clean1.gs.read_length = local.clean1.gs.read_length;
    }
    merge_fs(&mut gv.fs, &local.fs);
}

#[cfg(test)]
mod tests {
    use super::{merge_pe_local, PeLocalStats};
    use crate::params::GlobalParams;

    #[test]
    fn legacy_storage_native_tables_are_empty_before_processing() {
        let mut gp = GlobalParams::new();
        gp.native_m1 = true;
        let local = PeLocalStats::new(&gp);
        for stat in [
            &local.raw1,
            &local.raw2,
            &local.trim1,
            &local.trim2,
            &local.clean1,
            &local.clean2,
        ] {
            assert_eq!(stat.bs.position_acgt_content.len(), 0);
            assert_eq!(stat.bs.position_acgt_content.capacity(), 0);
            for bins in [
                &stat.ts.ht[..],
                &stat.ts.hlq[..],
                &stat.ts.tt[..],
                &stat.ts.tlq[..],
                &stat.ts.ta[..],
            ] {
                assert!(bins.is_empty());
            }
        }
        assert!(std::mem::size_of::<PeLocalStats>() < 8192);
        println!(
            "NATIVE_LEGACY_EMPTY bundle_bytes={} old_bundle_bytes=481976 saving={}",
            std::mem::size_of::<PeLocalStats>(),
            481976 - std::mem::size_of::<PeLocalStats>()
        );
    }

    #[test]
    fn legacy_storage_full_clone_and_merge_keep_boundary_values() {
        let gp = GlobalParams::new();
        let mut first = Box::new(PeLocalStats::new(&gp));
        for (i, stat) in [
            &mut first.raw1,
            &mut first.raw2,
            &mut first.trim1,
            &mut first.trim2,
            &mut first.clean1,
            &mut first.clean2,
        ]
        .into_iter()
        .enumerate()
        {
            let value = (i + 1) as u64;
            assert_eq!(stat.bs.position_acgt_content.len(), 1000);
            for bins in [
                &stat.ts.ht[..],
                &stat.ts.hlq[..],
                &stat.ts.tt[..],
                &stat.ts.tlq[..],
                &stat.ts.ta[..],
            ] {
                assert_eq!(bins.len(), 1001);
                assert!(bins.iter().all(|&n| n == 0));
            }
            stat.gs.reads_number = 1;
            stat.gs.base_number = 1000;
            stat.gs.read_max_length = 1000;
            stat.bs.position_acgt_content[0][0] = value;
            stat.bs.position_acgt_content[999][4] = value + 1;
            for bins in [
                &mut stat.ts.ht[..],
                &mut stat.ts.hlq[..],
                &mut stat.ts.tt[..],
                &mut stat.ts.tlq[..],
                &mut stat.ts.ta[..],
            ] {
                bins[0] = value + 2;
                bins[1000] = value + 3;
            }
            *stat.qs.at(999, 42) = value + 4;
        }
        let copied = Box::new(first.as_ref().clone());
        let mut global = Box::new(super::GlobalStats::new(&gp));
        super::merge_pe_stat(&mut global, &first, &gp, true);
        super::merge_pe_stat(&mut global, &copied, &gp, true);
        for (i, stat) in [
            &global.raw1,
            &global.raw2,
            &global.trim1,
            &global.trim2,
            &global.clean1,
            &global.clean2,
        ]
        .into_iter()
        .enumerate()
        {
            let value = (i + 1) as u64;
            assert_eq!(stat.bs.position_acgt_content[0][0], 2 * value);
            assert_eq!(stat.bs.position_acgt_content[999][4], 2 * (value + 1));
            for bins in [
                &stat.ts.ht[..],
                &stat.ts.hlq[..],
                &stat.ts.tt[..],
                &stat.ts.tlq[..],
                &stat.ts.ta[..],
            ] {
                assert_eq!(bins[0], 2 * (value + 2));
                assert_eq!(bins[1000], 2 * (value + 3));
            }
            assert_eq!(stat.qs.row_data(999)[42], 2 * (value + 4));
        }
    }

    #[test]
    fn batch_local_merge_is_cumulative_without_mutating_the_snapshot() {
        let gp = GlobalParams::new();
        let mut first = PeLocalStats::new(&gp);
        first.raw1.gs.reads_number = 1;
        first.raw1.gs.base_number = 4;
        first.raw1.gs.read_length = 4;
        first.raw1.gs.read_max_length = 4;
        first.clean1.gs.reads_number = 1;
        first.clean1.gs.base_number = 4;
        first.clean1.gs.read_max_length = 4;
        first.fs.n_ratio_num = 2;

        let mut cumulative = PeLocalStats::new(&gp);
        merge_pe_local(&mut cumulative, &first, &gp, false);
        assert_eq!(cumulative.raw1.gs.reads_number, 1);
        assert_eq!(cumulative.clean1.gs.reads_number, 1);
        assert_eq!(cumulative.fs.n_ratio_num, 2);
        assert_eq!(first.raw1.gs.reads_number, 1);

        let mut second = PeLocalStats::new(&gp);
        second.raw1.gs.reads_number = 2;
        second.raw1.gs.base_number = 8;
        second.raw1.gs.read_length = 4;
        second.raw1.gs.read_max_length = 4;
        second.clean1.gs.reads_number = 2;
        second.clean1.gs.base_number = 8;
        second.clean1.gs.read_max_length = 4;
        second.fs.n_ratio_num = 3;
        merge_pe_local(&mut cumulative, &second, &gp, false);
        assert_eq!(cumulative.raw1.gs.reads_number, 3);
        assert_eq!(cumulative.clean1.gs.reads_number, 3);
        assert_eq!(cumulative.clean1.gs.read_length, 4);
        assert_eq!(cumulative.fs.n_ratio_num, 5);
        assert_eq!(second.raw1.gs.reads_number, 2);
    }
}
