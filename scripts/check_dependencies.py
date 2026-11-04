#!/usr/bin/env python3
"""Check the reviewed locked/vendor/native integration contract without building."""
import argparse
import hashlib
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--contract', type=Path, default=root/'tests/dependency-contract.json')
args = parser.parse_args()
contract = json.loads(args.contract.read_text())
errors = []
for name, expected in contract['integration_files'].items():
    path = root/name
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        errors.append('integration hash mismatch: '+name)
locked = {}
for block in (root/'soapnuke-rs/Cargo.lock').read_text().split('[[package]]')[1:]:
    fields = dict(re.findall(r'^(name|version|checksum) = "([^"\n]+)"', block, re.M))
    if 'checksum' in fields:
        locked[fields['name']] = fields
if locked != contract['locked_packages']:
    errors.append('locked registry package identities differ')
checked = 0
for name, package in locked.items():
    folder = root/'vendor'/name
    checksum = json.loads((folder/'.cargo-checksum.json').read_text())
    if checksum['package'] != package['checksum']:
        errors.append('upstream package checksum differs: '+name)
    for relative, expected in checksum['files'].items():
        path = folder/relative
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            errors.append('vendored file mismatch: '+name+'/'+relative)
        checked += 1
patch = contract['local_patch']
source = (root/patch['path']).read_text()
start = source.find('    /// Local SOAPnuke bounded-report hook:')
end = source.find('    /// Create a JSON deserializer', start)
if start < 0 or end < 0 or hashlib.sha256(source[start:end].encode()).hexdigest() != patch['hook_sha256']:
    errors.append('bounded raw_value scratch hook differs or is absent')
if 'features = ["raw_value"]' not in (root/'soapnuke-rs/Cargo.toml').read_text():
    errors.append('serde_json raw_value feature is not enabled')
native = json.loads((root/contract['dependency_manifest']).read_text())
actual = {str(p.relative_to(root)) for folder in ['vendor', 'soapnuke-rs/third_party'] for p in (root/folder).rglob('*') if p.is_file()}
if actual != set(native):
    errors.append('dependency payload file set differs')
for relative, expected in native.items():
    path = root/relative
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        errors.append('locked dependency payload mismatch: '+relative)
result = {'status': 'FAIL' if errors else 'VALIDATED', 'registry_packages': len(locked),
          'vendored_files': checked, 'dependency_payload_files': len(native),
          'local_patch': patch['upstream_package'], 'errors': errors}
print(json.dumps(result, indent=2))
raise SystemExit(2 if errors else 0)
