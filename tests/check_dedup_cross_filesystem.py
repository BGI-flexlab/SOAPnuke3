"""Run Dedup with two writable workspace directories on distinct filesystems."""

import argparse
import gzip
import json
from pathlib import Path
import subprocess
import tempfile


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
parser.add_argument('--spill-parent', type=Path, required=True)
parser.add_argument('--output-parent', type=Path, required=True)
args = parser.parse_args()
if args.spill_parent.stat().st_dev == args.output_parent.stat().st_dev:
    parser.error('spill and output parents must be on different filesystems')

spill = Path(tempfile.mkdtemp(prefix='dedup-spill-', dir=args.spill_parent.resolve()))
output = Path(tempfile.mkdtemp(prefix='dedup-output-', dir=args.output_parent.resolve()))
binary = args.binary.resolve(strict=True)
input_path = spill / 'input.fq'
representatives = b'@first\nACGT\n+\nIIII\n@distinct\nTTAA\n+\nHHHH\n'
duplicate = b'@same-sequence\nacgt\n+\nGGGG\n'
input_path.write_bytes(representatives + duplicate)

for name in ('all-cross-device', 'mixed-destinations', 'existing-output'):
    case = spill / name
    case.mkdir()
    scratch = case / 'scratch'
    scratch.mkdir()
    destination = output / name
    destination.mkdir()
    representative_path = (case if name == 'mixed-destinations' else destination) / 'rep.fq.gz'
    duplicates_path = destination / 'duplicates.fq'
    report_path = case / 'report.json'
    if name == 'existing-output':
        duplicates_path.write_bytes(b'keep this existing file\n')
    command = [str(binary), 'dedup', '--r1', str(input_path), '--threads', '1',
               '--spill-dir', str(scratch), '--out1', str(representative_path),
               '--duplicates1', str(duplicates_path), '--report', str(report_path)]
    (case / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
    result = subprocess.run(command, capture_output=True, timeout=30)
    (case / 'stdout.txt').write_bytes(result.stdout)
    (case / 'stderr.txt').write_bytes(result.stderr)
    if name == 'existing-output':
        assert result.returncode != 0
        assert duplicates_path.read_bytes() == b'keep this existing file\n'
        assert not representative_path.exists()
    else:
        assert result.returncode == 0, result.stderr.decode()
        assert gzip.decompress(representative_path.read_bytes()) == representatives
        assert duplicates_path.read_bytes() == duplicate
        report = json.loads(report_path.read_text())
        assert report['status'] == 'complete'
        assert report['disposition']['input_units'] == 3
        assert report['disposition']['accepted_units'] == 2
        assert [item['state'] for item in report['outputs']] == ['committed', 'committed']
    assert not list(scratch.iterdir())
    assert not list(destination.glob('.soapnuke-dedup-*'))

print(f'PASS: inputs, reports and logs in {spill}, cross-device outputs in {output}')
