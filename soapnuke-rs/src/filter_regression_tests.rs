use super::{
    bq_check, cal_quar_from_array, check_tile_or_fov, filter_pe_fqs, filter_se_fqs, pe_discard,
    stat_read, whether_over_overlapped, ReadResult,
};
use crate::params::GlobalParams;
use crate::record::FastqRecord;
use crate::stats::{FileStat, FilterStat};

#[test]
fn exact_overlap_at_threshold_is_detected_and_discarded() {
    let mut gp = GlobalParams::new();
    gp.overlap_length = 4;
    gp.pe_mismatch_ratio = 0.0;
    gp.min_read_length = -1;

    // reverse-complement(R2) starts with CCCC, matching the R1 suffix.
    let overlapped = whether_over_overlapped("AAAACCCC", "TTTTGGGG", &gp);
    assert!(overlapped);

    let r1 = ReadResult {
        over_lapped: overlapped,
        ..ReadResult::default()
    };
    let r2 = ReadResult::default();
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &r1, &r2, 8, 8), Some(1));
    assert_eq!(fs.over_lapped_num, 1);
}

#[test]
fn overlap_mismatch_budget_and_n_are_inclusive() {
    let mut gp = GlobalParams::new();
    gp.overlap_length = 4;
    gp.pe_mismatch_ratio = 0.25;

    assert!(whether_over_overlapped("AAAA", "GTTT", &gp));

    gp.pe_mismatch_ratio = 0.0;
    assert!(!whether_over_overlapped("AAAA", "GTTT", &gp));
    assert!(!whether_over_overlapped("AAAA", "GGTT", &gp));

    // N contributes one mismatch, so it is accepted only when the one-error
    // budget is available.
    gp.pe_mismatch_ratio = 0.25;
    assert!(whether_over_overlapped("AAAA", "NTTT", &gp));
    gp.pe_mismatch_ratio = 0.0;
    assert!(!whether_over_overlapped("AAAA", "NTTT", &gp));
}

#[test]
fn disabled_overlap_never_discards() {
    let mut gp = GlobalParams::new();
    gp.overlap_length = -1;
    gp.min_read_length = -1;
    let r1 = ReadResult {
        over_lapped: true,
        ..ReadResult::default()
    };
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &r1, &ReadResult::default(), 4, 4), None);
    assert_eq!(fs.over_lapped_num, 0);
}

#[test]
fn quality_check_uses_the_first_populated_batch() {
    let gp = GlobalParams::new();
    let mut read = FastqRecord::new(1);
    read.qual_seq = "IIII".to_string();
    let stat = FileStat::new(&gp);

    // The local stat is still empty before the first batch is accumulated.
    // The quality check must derive its denominator from the reads it checks.
    assert!(bq_check(&[read], &stat, &gp).is_ok());
}

#[test]
fn tile_and_fov_parameters_match_exact_lists_and_ranges() {
    assert!(check_tile_or_fov("1100", "1100"));
    assert!(check_tile_or_fov("1100", "1000,1100"));
    assert!(check_tile_or_fov("1100", "1000-1200"));
    assert!(check_tile_or_fov("1000", "1000-1100"));
    assert!(check_tile_or_fov("1100", "1000-1100"));
    assert!(!check_tile_or_fov("1300", "1000-1200"));
    assert!(!check_tile_or_fov("1101", "1000-1100"));
    assert!(!check_tile_or_fov("1100", "1000-invalid"));
    assert!(!check_tile_or_fov("1100", "1000-1200-1300"));

    assert!(check_tile_or_fov("C001R003", "C001R003,C003R004"));
    assert!(!check_tile_or_fov("C002R003", "C001R003,C003R004"));
}

#[test]
fn tile_filter_does_not_discard_when_parsed_value_is_missing() {
    let mut gp = GlobalParams::new();
    gp.tile = "1100".to_string();
    gp.min_read_length = -1;
    let mut fs = FilterStat::default();

    // A missing parsed value must not match a requested tile. The current
    // implementation passes an empty placeholder and discards unconditionally.
    assert_eq!(
        pe_discard(&mut fs, &gp, &ReadResult::default(), &ReadResult::default(), 4, 4),
        None
    );
    assert_eq!(fs.tile_num, 0);
}

#[test]
fn parsed_tile_and_fov_values_drive_pair_filtering() {
    let mut gp = GlobalParams::new();
    gp.min_read_length = -1;
    gp.tile = "1000,1100-1200".to_string();

    let mut tile_read = FastqRecord::new(1);
    tile_read.seq_id = "instrument:run:1100:0".to_string();
    tile_read.sequence = "AAAA".to_string();
    tile_read.qual_seq = "IIII".to_string();
    let tile_result = stat_read(&mut tile_read, &gp, None).unwrap();
    assert_eq!(tile_result.read_tile, "1100");
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &tile_result, &ReadResult::default(), 4, 4), Some(1));
    assert_eq!(fs.tile_num, 1);

    gp.tile = "1300".to_string();
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &tile_result, &ReadResult::default(), 4, 4), None);
    assert_eq!(fs.tile_num, 0);

    gp.tile.clear();
    gp.fov = "C001R003,C003R004".to_string();
    let mut fov_read = FastqRecord::new(1);
    fov_read.seq_id = "instrument:C001R003".to_string();
    fov_read.sequence = "AAAA".to_string();
    fov_read.qual_seq = "IIII".to_string();
    let fov_result = stat_read(&mut fov_read, &gp, None).unwrap();
    assert_eq!(fov_result.read_fov, "C001R003");
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &fov_result, &ReadResult::default(), 4, 4), Some(1));
    assert_eq!(fs.fov_num, 1);

    gp.fov = "C002R003".to_string();
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &fov_result, &ReadResult::default(), 4, 4), None);
    assert_eq!(fs.fov_num, 0);

    gp.fov = "C001R003-invalid".to_string();
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &fov_result, &ReadResult::default(), 4, 4), None);
    assert_eq!(fs.fov_num, 0);

    let mut missing_read = FastqRecord::new(1);
    missing_read.seq_id = "instrument:run:1100:0".to_string();
    missing_read.sequence = "AAAA".to_string();
    missing_read.qual_seq = "IIII".to_string();
    gp.fov = "C001R003".to_string();
    let missing_result = stat_read(&mut missing_read, &gp, None).unwrap();
    assert!(missing_result.read_fov.is_empty());
    let mut fs = FilterStat::default();
    assert_eq!(pe_discard(&mut fs, &gp, &missing_result, &ReadResult::default(), 4, 4), None);
    assert_eq!(fs.fov_num, 0);
}

#[test]
fn single_end_filter_reports_only_the_first_lane() {
    let mut gp = GlobalParams::new();
    gp.clean_fq1 = "clean.fastq".to_string();
    gp.min_read_length = -1;
    let mut reads = Vec::new();
    for (id, sequence) in [("kept", "AAAA"), ("filtered", "NNNN")] {
        let mut read = FastqRecord::new(1);
        read.seq_id = format!("@{id}");
        read.sequence = sequence.to_string();
        read.qual_seq = "IIII".to_string();
        reads.push(read);
    }
    let mut fs = FilterStat::default();
    let (_, clean) = filter_se_fqs(&mut reads, &mut fs, &gp, false, false, None).unwrap();
    assert_eq!(clean.len(), 1);
    assert_eq!(fs.n_ratio_num, 1);
    assert_eq!(fs.n_ratio_num1, 1);
    assert_eq!(fs.n_ratio_num2, 0);
    assert_eq!(fs.n_ratio_num_overlap, 0);
}

#[test]
fn single_end_length_filter_never_creates_mate2_or_overlap_counts() {
    let mut gp = GlobalParams::new();
    gp.clean_fq1 = "clean.fastq".to_string();
    gp.min_read_length = 5;
    let mut reads = vec![{
        let mut read = FastqRecord::new(1);
        read.seq_id = "@short".to_string();
        read.sequence = "AAAA".to_string();
        read.qual_seq = "IIII".to_string();
        read
    }];
    let mut fs = FilterStat::default();
    filter_se_fqs(&mut reads, &mut fs, &gp, false, false, None).unwrap();
    assert_eq!(fs.short_len_num, 1);
    assert_eq!(fs.short_len_num1, 1);
    assert_eq!(fs.short_len_num2, 0);
    assert_eq!(fs.short_len_num_overlap, 0);
    assert_eq!(fs.over_lapped_num, 0);
}

#[test]
fn single_end_hard_trim_uses_cli_head_and_tail_values() {
    let mut gp = GlobalParams::new();
    gp.clean_fq1 = "clean.fastq".to_string();
    gp.trim = "1,2".to_string();
    gp.min_read_length = 1;
    let mut reads = vec![{
        let mut read = FastqRecord::new(1);
        read.seq_id = "@hard-trim".to_string();
        read.sequence = "ACGTAC".to_string();
        read.qual_seq = "IIIIII".to_string();
        read
    }];

    let mut fs = FilterStat::default();
    let (_, clean) = filter_se_fqs(&mut reads, &mut fs, &gp, true, false, None).unwrap();
    assert_eq!(clean.len(), 1);
    assert_eq!(clean[0].sequence, "CGT");
    assert_eq!(clean[0].qual_seq, "III");
    assert_eq!(clean[0].head_hdcut, 1);
    assert_eq!(clean[0].tail_hdcut, 2);
}

#[test]
fn paired_end_hard_trim_uses_mate_specific_values() {
    let mut gp = GlobalParams::new();
    gp.clean_fq1 = "clean-r1.fastq".to_string();
    gp.clean_fq2 = "clean-r2.fastq".to_string();
    gp.trim = "1,2,2,1".to_string();
    gp.min_read_length = 1;
    let mut reads1 = vec![{
        let mut read = FastqRecord::new(1);
        read.seq_id = "@hard-trim/1".to_string();
        read.sequence = "ACGTAC".to_string();
        read.qual_seq = "IIIIII".to_string();
        read
    }];
    let mut reads2 = vec![{
        let mut read = FastqRecord::new(2);
        read.seq_id = "@hard-trim/2".to_string();
        read.sequence = "TTGGCC".to_string();
        read.qual_seq = "JJJJJJ".to_string();
        read
    }];

    let mut fs = FilterStat::default();
    let (_, _, clean1, clean2) =
        filter_pe_fqs(&mut reads1, &mut reads2, &mut fs, &gp, true, false, None, None).unwrap();
    assert_eq!(clean1[0].sequence, "CGT");
    assert_eq!(clean1[0].qual_seq, "III");
    assert_eq!(clean2[0].sequence, "GGC");
    assert_eq!(clean2[0].qual_seq, "JJJ");
}

fn concentrated_quality_histogram(total: u64, quality: usize) -> Vec<u64> {
    let mut histogram = vec![0; quality + 1];
    histogram[quality] = total;
    histogram
}

#[test]
fn percentile_p90_does_not_wrap_at_signed_32_bit_product_boundary() {
    for total in [
        238_609_294_u64,
        238_609_295,
        430_952_721,
        2_147_483_646,
        2_147_483_647,
        2_147_483_648,
        4_294_967_296,
    ] {
        let histogram = concentrated_quality_histogram(total, 40);
        let quartile = cal_quar_from_array(&histogram, histogram.len());
        assert_eq!(quartile.last10_quar, 40.0, "total={total}");
    }
}

#[test]
fn percentile_upper_quartile_preserves_large_count_boundaries() {
    for total in [715_827_882_u64, 715_827_883, 4_294_967_295] {
        let histogram = concentrated_quality_histogram(total, 41);
        let quartile = cal_quar_from_array(&histogram, histogram.len());
        assert_eq!(quartile.upper_quar, 41.0, "total={total}");
    }

    let near_max = concentrated_quality_histogram(u64::MAX - 1, 1);
    let quartile = cal_quar_from_array(&near_max, near_max.len());
    assert_eq!(quartile.mean, 1.0);
    assert_eq!(quartile.upper_quar, 1.0);
    assert_eq!(quartile.last10_quar, 1.0);
}

#[test]
fn percentile_handles_empty_small_gapped_and_max_quality_histograms() {
    let empty = cal_quar_from_array(&[], 0);
    assert_eq!(empty.mean, 0.0);
    assert_eq!(empty.last10_quar, 0.0);

    let all_zero = cal_quar_from_array(&[0; 41], 41);
    assert_eq!(all_zero.mean, 0.0);
    // Preserve the existing inclusive-boundary behavior for an allocated
    // all-zero row; percentile arithmetic is not allowed to redefine it.
    assert_eq!(all_zero.last10_quar, 40.0);

    let single = cal_quar_from_array(&concentrated_quality_histogram(7, 5), 6);
    assert_eq!(single.mean, 5.0);
    assert_eq!(single.median, 5.0);
    assert_eq!(single.lower_quar, 5.0);
    assert_eq!(single.upper_quar, 5.0);
    assert_eq!(single.first10_quar, 5.0);
    assert_eq!(single.last10_quar, 5.0);

    let mut mixed = vec![0_u64; 43];
    mixed[0] = 2;
    mixed[10] = 3;
    mixed[42] = 5;
    let quartile = cal_quar_from_array(&mixed, mixed.len());
    assert_eq!(quartile.mean, 24.0);
    assert_eq!(quartile.last10_quar, 42.0);
}
