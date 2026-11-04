import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

work = Path(__file__).resolve().parents[1]
from common import read_records, write_records

ap = argparse.ArgumentParser()
ap.add_argument('--out', required=True)
ap.add_argument('--binary', required=True)
ap.add_argument('--threads', type=int, default=2)
opts = ap.parse_args()
out = Path(opts.out).resolve()
out.mkdir()
binary = Path(opts.binary).resolve()
tmp = work / '.scratch/tmp'
tmp.mkdir(parents=True, exist_ok=True)
env = {**os.environ, 'TMPDIR': str(tmp)}
rows = []

def run(name, args, executable=binary, code=0, stdin=None, folder=None):
    folder = folder or out / name
    folder.mkdir(exist_ok=True)
    args = list(args)
    for i, arg in enumerate(args):
        if i and args[i-1] in ('-T', '--threads', '--thread') and arg == '16':
            args[i] = str(opts.threads)
        elif arg in ('-T16', '-T=16'):
            args[i] = '-T' + str(opts.threads)
    command = [str(executable), *args]
    (folder / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
    p = subprocess.run(command, input=stdin, capture_output=True, timeout=30,
                       cwd=folder, env=env)
    (folder / 'stdout.txt').write_bytes(p.stdout)
    (folder / 'stderr.txt').write_bytes(p.stderr)
    (folder / 'terminal.json').write_text(json.dumps({'exit': p.returncode}) + '\n')
    assert p.returncode == code, (name, p.returncode, p.stderr.decode())
    rows.append({'name': name, 'exit': p.returncode, 'binary_sha256': hashlib.sha256(executable.read_bytes()).hexdigest()})
    return p

fixture = out / 'inputs'
fixture.mkdir()
names = ['quality-equal', 'fraction-equal', 'fraction-above', 'n-equal', 'n-above', 'short']
seqs = ['A'*40, 'A'*40, 'A'*40, 'NN'+'A'*38, 'NNN'+'A'*37, 'A'*29]
quals = [[20]*40, [19]*20+[30]*20, [19]*21+[30]*19, [30]*40, [30]*40, [30]*29]
r1 = [{'name': n+'/1', 'seq': s, 'q': q} for n,s,q in zip(names,seqs,quals)]
r2 = [{'name': n+'/2', 'seq': 'G'*len(s), 'q': [40]*len(s)} for n,s in zip(names,seqs)]
write_records(fixture/'r1.fq.gz',r1)
write_records(fixture/'r2.fq.gz',r2)
write_records(fixture/'phred64.fq',r1,64)
adapter = 'AGATCGGAAGAGC'
adapters = [{'name':'adapter/1','seq':'C'*60+adapter,'q':[40]*(60+len(adapter))}]
write_records(fixture/'adapter.fq.gz', adapters)
(out/'expected.json').write_text(json.dumps({'boundary_pass_names':[names[i] for i in (0,1,3)],'adapter_sequence':'C'*60,'default_reads':len(r1)},indent=2)+'\n')

for label, args in [('top',[]),('top-long',['--help']),('top-short',['-h']),
                    ('help',['process']),('help-long',['process','--help']),('help-short',['process','-h']),
                    ('version',['--version']),('short-version',['-v']),('process-version',['process','-v'])]:
    p=run(label,args)
    assert not p.stderr
    if 'version' in label:
        assert p.stdout.decode().splitlines()[0] == 'SOAPnuke 3.0.0'
    if label.startswith(('help','top')):assert max(map(len,p.stdout.decode().splitlines()))<=79
top=(out/'top/stdout.txt').read_text()
help_text=(out/'help/stdout.txt').read_text()
assert '  filter ' not in top and '  process ' in top
assert all((out/p/'stdout.txt').read_text()==top for p in ('top-long','top-short'))
assert all((out/p/'stdout.txt').read_text()==help_text for p in ('help-long','help-short'))
metadata_source=(work/'soapnuke-rs/src/option_metadata.rs').read_text()
allowed={key for key,jobs in re.findall(r'OptionSpec \{ name: "([a-z0-9-]+)", jobs: ([0-9]+)',metadata_source) if int(jobs)&1}
assert allowed, 'Process option metadata inventory absent'
assert all(re.search(r'--'+re.escape(key)+r'\b',help_text) for key in allowed), allowed

canonical=['--r1',str(fixture/'r1.fq.gz'),'--r2',str(fixture/'r2.fq.gz'),
           '--out1','clean1.fq.gz','--out2','clean2.fq.gz','--report','SOAPnuke.json',
           '--threads','16','--low-q','20','--max-low-q-fraction','0.5',
           '--max-n-fraction','0.05','--min-length','30','--min-mean-q','20']
for name,args in [
    ('boundary-short',['-1'+str(fixture/'r1.fq.gz'),'-2',str(fixture/'r2.fq.gz'),'-Cclean1.fq.gz','-D','clean2.fq.gz','-o.','-T=16','-l20','-q0.5','-n0.05','-4','30','-m20']),
    ('boundary-long',['--fq1='+str(fixture/'r1.fq.gz'),'--fq2',str(fixture/'r2.fq.gz'),'--cleanFq1','clean1.fq.gz','--cleanFq2=clean2.fq.gz','--outDir','.','--thread','16','--lowQual','20','--qualRate','0.5','--nRate','0.05','--minReadLen','30','--mean','20']),
    ('boundary-native',canonical),
]:
    run(name,['process',*args])
    report=json.loads((out/name/'SOAPnuke.json').read_text())
    assert ('report' in report['settings']['supplied_keys']) == (name == 'boundary-native')
    assert report['settings']['provenance']['resolved_options']['report']['explicit'] == (name == 'boundary-native')
    for mate,records in [(1,r1),(2,r2)]:
        actual=read_records(out/name/f'clean{mate}.fq.gz')
        assert actual==[records[i] for i in (0,1,3)],name

for name,extra in [('defaults',[]),('max-length',['--minLen','1','--maxReadLen','39'])]:
    run(name,['process','-1',str(fixture/'r1.fq.gz'),'-C','clean.fq.gz','--out-dir','.','-T1',*extra])
    assert read_records(out/name/'clean.fq.gz')==(r1 if name=='defaults' else [r1[-1]])

for name,extra in [('adapter-short',['-f',adapter,'-J']),('adapter-long',['--adapter1',adapter,'--ada_trim']),('adapter-v1-trim',['--adapter3-r1',adapter,'-E']),('adapter-long-v1-trim',['--adapter1',adapter,'--cutAdaptor']),('adapter-repeat',['-f','TTTTTTTT','--adapter3-r1',adapter])]:
    run(name,['process','-1',str(fixture/'adapter.fq.gz'),'-C','clean.fq.gz','-o','.','-T1',*extra])
    assert read_records(out/name/'clean.fq.gz')==[{'name':'adapter/1','seq':'C'*60,'q':[40]*60}]

run('fixed-pe',['process','-1',str(fixture/'r1.fq.gz'),'-2',str(fixture/'r2.fq.gz'),'-C','a.fq','-D','b.fq','-o.','-T1','-t1,2,3,4'])
for file,records,head,tail in [('a.fq',r1,1,2),('b.fq',r2,3,4)]:
    assert read_records(out/'fixed-pe'/file)==[{**r,'seq':r['seq'][head:-tail],'q':r['q'][head:-tail]} for r in records]
run('phred64',['process','-1',str(fixture/'phred64.fq'),'-Q1','-G','-Cclean.fq','-o.','-T1'])
assert read_records(out/'phred64/clean.fq')==r1
run('phred-long',['process','--fq1',str(fixture/'r1.fq.gz'),'--qualSys','2','--outQualSys','1','-Cclean.fq','-o.','-T1'])
assert read_records(out/'phred-long/clean.fq',64)==r1
for name,flag in [('split-short','-w'),('split-long','--cleanOutSplit'),('split-old-long','--output_clean')]:
    run(name,['process','-1',str(fixture/'r1.fq.gz'),'-C','clean.{chunk}.fq','-o.','-T1',flag,'2'])
    records=[]
    for p in sorted((out/name).glob('clean.*.fq')):records.extend(read_records(p))
    assert records==r1 and len(list((out/name).glob('clean.*.fq')))==3

run('qc',['process','-1',str(fixture/'r1.fq.gz'),'--qc-only','-T1','-o.','--html-report','qc.html'])
assert (out/'qc/qc.html').is_file() and not list((out/'qc').glob('clean*'))
assert json.loads((out/'qc/SOAPnuke.json').read_text())['populations']['input']['read_records']==6
run('stdout',['process','-1',str(fixture/'r1.fq.gz'),'-C-','--output-format','fastq','-o.','-T1'])
assert (out/'stdout/stdout.txt').read_bytes()==gzip.decompress((fixture/'r1.fq.gz').read_bytes())
run('stdin',['process','-1-','--sample-id','stream','-Cclean.fq','-o.','-T1'],stdin=gzip.decompress((fixture/'r1.fq.gz').read_bytes()))
assert read_records(out/'stdin/clean.fq')==r1

config=out/'config.json'
config.write_text(json.dumps({'job':'process','options':{'r1':str(fixture/'r1.fq.gz'),'out1':'clean.fq','out-dir':str(out/'qc'),'report':'custom.json','threads':1}}))
run('config-override',['process','--config',str(config),'-o.','-T2'])
report=json.loads((out/'config-override/custom.json').read_text())
assert report['settings']['effective']['threads']==2
assert read_records(out/'config-override/clean.fq')==r1
assert not (out/'qc/custom.json').exists()
assert report['settings']['provenance']['resolved_options']['out-dir']['origin']=='cli'
assert 'report' in report['settings']['supplied_keys']
assert report['settings']['provenance']['resolved_options']['report']['origin']=='config'
absolute=out/'absolute-clean.fq'
run('absolute-output',['process','-1',str(fixture/'r1.fq.gz'),'-C',str(absolute),'-o.','-T1'])
assert read_records(absolute)==r1

for name,args,needle in [
    ('removed-filter',['filter','-1','missing'],'no longer a separate command'),
    ('duplicate-input',['process','-1','a','--r1','b'],'duplicate option --r1'),
    ('duplicate-thread',['process','-T1','--threads','2'],'duplicate option --threads'),
    ('duplicate-trim',['process','-t1,2','--cut-r1','0,0'],'duplicate option --cut-r1'),
    ('duplicate-encoding',['process','-Q2','--input-phred','33'],'duplicate option --input-phred'),
    ('ambiguous-c',['process','-c','legacy.cfg'],'different meanings'),
    ('ambiguous-L',['process','-L','100'],'different meanings'),
    ('invalid-trim',['process','-t1,2,3'],'requires HEAD,TAIL'),
    ('invalid-Q',['process','-Q33'],'requires 1'),
    ('v1-J-conflict',['process','-J0.2'],'takes no value'),
    ('old-disabled-sentinel',['process','-1',str(fixture/'r1.fq.gz'),'-n-1'],'max-n-fraction'),
    ('missing-directory',['process','-1',str(fixture/'r1.fq.gz'),'-Cclean.fq','-o',str(out/'absent-directory')],'directory must exist'),
    ('missing-report',['process','-1',str(fixture/'r1.fq.gz'),'-Cclean.fq'],'--report'),
    ('alias-qc-conflict',['process','-1',str(fixture/'r1.fq.gz'),'--qc-only','-l20','-q0.5','-o.'],'qc-only rejects'),
]:
    p=run(name,args,code=2)
    assert needle in p.stderr.decode(),(name,p.stderr.decode())
    assert not list((out/name).glob('*.fq')) and not (out/name/'SOAPnuke.json').exists()
assert not (out/'absent-directory').exists()
collision=out/'existing-report';collision.mkdir();(collision/'SOAPnuke.json').write_text('keep this evidence\n')
run('existing-report',['process','-1',str(fixture/'r1.fq.gz'),'-Cclean.fq','-o.'],code=2,folder=collision)
assert (collision/'SOAPnuke.json').read_text()=='keep this evidence\n' and not (collision/'clean.fq').exists()

commands=[];pending=''
for line in help_text.split('Examples:\n',1)[1].splitlines():
    line=line.strip()
    if pending or line.startswith('SOAPnuke process '):
        pending+=line.removesuffix('\\').rstrip()+' '
        if not line.endswith('\\'):
            commands.append(shlex.split(pending)[1:]);pending=''
assert len(commands)==4
for n,args in enumerate(commands,1):
    folder=out/f'printed-example-{n}';folder.mkdir()
    (folder/'results').mkdir();(folder/'filtered').mkdir()
    for name,target in [('reads_R1.fq.gz','r1.fq.gz'),('reads_R2.fq.gz','r2.fq.gz'),('reads.fq.gz','adapter.fq.gz')]:
        (folder/name).symlink_to(fixture/target)
    run(f'printed-example-{n}',args,folder=folder)
    reports=[p for p in folder.rglob('*.json') if p.name not in ('command.json','terminal.json')]
    assert len(reports)==1 and json.loads(reports[0].read_text())['status']=='complete'

manifest={str(p.relative_to(fixture)):hashlib.sha256(p.read_bytes()).hexdigest() for p in fixture.iterdir()}
(out/'fixture-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
(out/'summary.json').write_text(json.dumps({'status':'PASS','invocations':len(rows),'canonical_options_documented':len(allowed),'runs':rows},indent=2)+'\n')
print(json.dumps({'status':'PASS','invocations':len(rows),'canonical_options_documented':len(allowed)}))
