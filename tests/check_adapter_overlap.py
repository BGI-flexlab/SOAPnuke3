#!/usr/bin/env python3
"""End-to-end known-insert and abstention cases for optional PE adapter clipping."""

import argparse
import csv
import json
from pathlib import Path
import subprocess


def rc(sequence):
    return sequence.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def write_fastq(path, records):
    with path.open("w") as out:
        for record in records:
            name, sequence = record[:2]
            quality = record[2] if len(record) == 3 else "I" * len(sequence)
            out.write(f"@{name}\n{sequence}\n+\n{quality}\n")


def read_fastq(path):
    rows = path.read_text().splitlines()
    assert len(rows) % 4 == 0
    return [(rows[i][1:], rows[i + 1], rows[i + 3]) for i in range(0, len(rows), 4)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True)
    adapter = "AGATCGGAAGAG"
    insert = "ACGTTGCATCAGTCCGATGCTACGTAGCTGACTAACGCTAGTCG"
    cases = [
        ("OneTail", insert + "A", rc(insert) + "A", insert, rc(insert)),
        ("TwoTail", insert + "AG", rc(insert) + "AG", insert, rc(insert)),
        ("Unequal", insert + "AG", rc(insert) + "AGATC", insert, rc(insert)),
        ("NoAdapter", insert + "T", rc(insert) + "A", insert + "T", rc(insert) + "A"),
        ("OneMateTail", insert + "A", rc(insert), insert + "A", rc(insert)),
        ("Repeat", "AT" * 25 + "A", "AT" * 25 + "A", "AT" * 25 + "A", "AT" * 25 + "A"),
    ]
    write_fastq(args.out / "R1.fq", [(name + "/1", r1) for name, r1, _, _, _ in cases])
    write_fastq(args.out / "R2.fq", [(name + "/2", r2) for name, _, r2, _, _ in cases])

    def run(label, options, expected_exit=0):
        folder = args.out / label
        folder.mkdir()
        command = [str(args.binary.resolve()), "process", "--r1", str(args.out / "R1.fq"), "--r2", str(args.out / "R2.fq"),
                   "--out1", str(folder / "clean1.fq"), "--out2", str(folder / "clean2.fq"), "--report", str(folder / "report.json"),
                   "--adapter3-r1", adapter, "--adapter3-r2", adapter, "--threads", "2", *options]
        (folder / "command.json").write_text(json.dumps(command, indent=2) + "\n")
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        (folder / "stdout.txt").write_text(result.stdout)
        (folder / "stderr.txt").write_text(result.stderr)
        assert result.returncode == expected_exit, (label, result.returncode, result.stderr)
        return folder

    enabled = run("enabled", ["--adapter-overlap", "--match-report", str(args.out / "enabled" / "matches.tsv"), "--match-limit", "1000"])
    r1 = read_fastq(enabled / "clean1.fq")
    r2 = read_fastq(enabled / "clean2.fq")
    assert [(x[0].split("/")[0], x[1], y[1]) for x, y in zip(r1, r2)] == [(name, expected1, expected2) for name, _, _, expected1, expected2 in cases]
    assert all(len(a[1]) == len(a[2]) and len(b[1]) == len(b[2]) for a, b in zip(r1, r2))
    report = json.loads((enabled / "report.json").read_text())
    assert report["status"] == "complete"
    assert report["processing"]["adapter_overlap"]["evaluated_pairs"] == len(cases)
    assert report["processing"]["adapter_overlap"]["clipped_pairs"] == 3
    assert report["processing"]["adapter_overlap"]["ambiguous_pairs"] >= 1
    assert report["processing"]["adapter_overlap"]["abstained_pairs"] == 3
    assert report["disposition"]["accepted_units"] == len(cases)
    rows = list(csv.DictReader((enabled / "matches.tsv").open(), delimiter="\t"))
    overlap_rows = [row for row in rows if row["adapter"].startswith("overlap:")]
    assert len(overlap_rows) == 6
    assert {int(row["unit"]) for row in overlap_rows} == {0, 1, 2}
    assert {int(row["start"]) for row in overlap_rows} == {len(insert)}

    disabled = run("disabled", ["--adapter-overlap=false"])
    assert read_fastq(disabled / "clean1.fq")[0][1] == insert + "A"
    assert "adapter_overlap" not in json.loads((disabled / "report.json").read_text()).get("processing", {})
    order = run("reordered", ["--adapter-overlap", "--stage-order", "adapters,tails,quality", "--quality-mode", "ends", "--quality-q", "20"])
    assert read_fastq(order / "clean1.fq")[0][1] == insert

    fasta = args.out / "adapters.fa"
    fasta.write_text(f">R1;mate=R1;end=3\n{adapter}\n>R2;mate=R2;end=3\n{adapter}\n")
    file_job = args.out / "fasta"
    file_job.mkdir()
    command = [str(args.binary.resolve()), "process", "--r1", str(args.out / "R1.fq"), "--r2", str(args.out / "R2.fq"),
               "--out1", str(file_job / "clean1.fq"), "--out2", str(file_job / "clean2.fq"), "--report", str(file_job / "report.json"),
               "--adapter-file", str(fasta), "--adapter-overlap", "--threads", "2"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert read_fastq(file_job / "clean1.fq") == r1

    earlier_insert = insert[:12] + adapter + insert[12 + len(adapter):]
    early = args.out / "earlier-match"
    early.mkdir()
    write_fastq(early / "R1.fq", [("Earlier/1", earlier_insert + "A")])
    write_fastq(early / "R2.fq", [("Earlier/2", rc(earlier_insert) + "A")])
    earlier_outputs = []
    for enabled_flag in ["false", "true"]:
        directory = early / enabled_flag
        directory.mkdir()
        command = [str(args.binary.resolve()), "process", "--r1", str(early / "R1.fq"), "--r2", str(early / "R2.fq"),
                   "--out1", str(directory / "R1.fq"), "--out2", str(directory / "R2.fq"), "--report", str(directory / "report.json"),
                   "--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter-overlap=" + enabled_flag,
                   "--match-report", str(directory / "matches.tsv"), "--threads", "2"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stderr
        earlier_outputs.append((read_fastq(directory / "R1.fq"), read_fastq(directory / "R2.fq")))
    assert len(earlier_outputs[0][0][0][1]) < len(earlier_insert)
    assert earlier_outputs[0] == earlier_outputs[1], "Adapter-overlap clipping must not undo an earlier supplied-adapter cut"
    early_audit = list(csv.DictReader((early / "true" / "matches.tsv").open(), delimiter="\t"))
    assert early_audit and all(not row["adapter"].startswith("overlap:") for row in early_audit)

    prior = args.out / "prior-tail-cut"
    prior.mkdir()
    write_fastq(prior / "R1.fq", [("PriorTail/1", insert + "AG")])
    write_fastq(prior / "R2.fq", [("PriorTail/2", rc(insert) + "AG")])
    command = [str(args.binary.resolve()), "process", "--r1", str(prior / "R1.fq"), "--r2", str(prior / "R2.fq"),
               "--out1", str(prior / "clean1.fq"), "--out2", str(prior / "clean2.fq"), "--report", str(prior / "report.json"),
               "--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter-overlap", "--cut-r1", "0,1", "--cut-r2", "0,1", "--threads", "2"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert read_fastq(prior / "clean1.fq") == [("PriorTail/1", insert, "I" * len(insert))]
    assert read_fastq(prior / "clean2.fq") == [("PriorTail/2", rc(insert), "I" * len(insert))]
    assert json.loads((prior / "report.json").read_text())["processing"]["adapter_overlap"]["clipped_pairs"] == 1

    ordered = args.out / "active-stage-order"
    ordered.mkdir()
    write_fastq(ordered / "R1.fq", [("StageOrder/1", insert + "A")])
    write_fastq(ordered / "R2.fq", [("StageOrder/2", rc(insert) + "A")])
    for label, stage_order, overlap_count in [("tails-first", "tails,quality,adapters", 0),
                                           ("adapters-first", "adapters,tails,quality", 1)]:
        directory = ordered / label
        directory.mkdir()
        command = [str(args.binary.resolve()), "process", "--r1", str(ordered / "R1.fq"), "--r2", str(ordered / "R2.fq"),
                   "--out1", str(directory / "clean1.fq"), "--out2", str(directory / "clean2.fq"), "--report", str(directory / "report.json"),
                   "--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter-overlap", "--tail-bases", "A", "--tail-min", "1",
                   "--stage-order", stage_order, "--threads", "2"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, result.stderr
        assert read_fastq(directory / "clean1.fq") == [("StageOrder/1", insert, "I" * len(insert))]
        assert read_fastq(directory / "clean2.fq") == [("StageOrder/2", rc(insert), "I" * len(insert))]
        assert json.loads((directory / "report.json").read_text())["processing"]["adapter_overlap"]["clipped_pairs"] == overlap_count

    combo = args.out / "overlap-correction-merge"
    combo.mkdir()
    long_insert = insert + "ACGTTGCA"
    changed = "C" if long_insert[10] != "C" else "A"
    changed_r1 = long_insert[:10] + changed + long_insert[11:]
    r1_quality = "I" * 10 + "+" + "I" * (len(changed_r1) - 11)
    write_fastq(combo / "R1.fq", [("Combo/1", changed_r1 + "A", r1_quality + "I")])
    write_fastq(combo / "R2.fq", [("Combo/2", rc(long_insert) + "A")])
    command = [str(args.binary.resolve()), "process", "--r1", str(combo / "R1.fq"), "--r2", str(combo / "R2.fq"),
               "--out1", str(combo / "clean1.fq"), "--out2", str(combo / "clean2.fq"), "--report", str(combo / "report.json"),
               "--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter-overlap", "--correct-overlap", "--merge",
               "--merged-out", str(combo / "merged.fq"), "--threads", "2"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    merged = read_fastq(combo / "merged.fq")
    assert len(merged) == 1 and merged[0][1:] == (long_insert, "I" * len(long_insert))
    assert json.loads((combo / "report.json").read_text())["processing"]["adapter_overlap"]["clipped_pairs"] == 1

    base = ["--adapter-overlap", "--threads", "2", "--r1", str(args.out / "R1.fq"), "--r2", str(args.out / "R2.fq"),
            "--out1", str(args.out / "reject1.fq"), "--out2", str(args.out / "reject2.fq"), "--report", str(args.out / "reject.json")]
    for name, extra in [
        ("missing-adapter", ["--adapter3-r1", adapter]),
        ("fixed-head", ["--adapter3-r1", adapter, "--adapter3-r2", adapter, "--cut-r1", "5,0"]),
        ("quality-head", ["--adapter3-r1", adapter, "--adapter3-r2", adapter, "--quality-mode", "ends", "--quality-q", "20"]),
        ("primer-head", ["--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter5-r1", "GGG"]),
        ("discard", ["--adapter3-r1", adapter, "--adapter3-r2", adapter, "--adapter-action", "discard"]),
    ]:
        result = subprocess.run([str(args.binary.resolve()), "process", *base, *extra], capture_output=True, text=True, timeout=30)
        assert result.returncode == 2 and "adapter-overlap" in result.stderr, (name, result.returncode, result.stderr)
    (args.out / "summary.json").write_text(json.dumps({"cases": len(cases), "clipped": 3, "abstained": 3, "audit_rows": len(overlap_rows)}, indent=2) + "\n")
    print("Adapter-overlap clipping integration: known pairs, active prior tail cut, correction/merge, audit, reordered stage, FASTA, disabled parity, invalid configurations passed")


if __name__ == "__main__":
    main()
