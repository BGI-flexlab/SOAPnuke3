"""Small portable CLI checks; fixtures/oracles do not import SOAPnuke code."""
import argparse,csv,hashlib,json,os,re,shlex,subprocess,sys
from pathlib import Path
from common import contains,metric,read_records,write_records
if sys.flags.optimize:raise SystemExit('run checks without Python optimization')
a=argparse.ArgumentParser();a.add_argument('--binary',required=True);a.add_argument('--out',required=True);a.add_argument('--threads',type=int,default=2);args=a.parse_args()
repo=Path(__file__).resolve().parents[1];pack=repo/'tests';fixture=repo/'examples/case-fixtures';out=Path(args.out).resolve();out.mkdir(parents=True);binary=Path(args.binary).resolve();rows=[];env={**os.environ,'TMPDIR':str(out)}
def run(name,argv,code=0):
 d=out/name;d.mkdir();cmd=[str(binary),*argv];(d/'command.json').write_text(json.dumps(cmd,indent=2)+'\n');z=subprocess.run(cmd,cwd=d,capture_output=True,timeout=30,env=env);(d/'terminal.json').write_text(json.dumps({'exit':z.returncode}));(d/'stdout').write_bytes(z.stdout);(d/'stderr').write_bytes(z.stderr);assert z.returncode==code,(name,z.returncode,z.stderr.decode());rows.append({'name':name,'exit':z.returncode,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()});return d,z

def qc(report,pop,records):contains(report['populations'][pop],metric([{**r,'seq':r['seq'].upper()}for r in records],pop,'all'))
def process(name,inputs,extra=(),expected=None):
 d=out/name;argv=['process','--r1',str(inputs[0]),'--report',str(d/'report.json'),'--threads',str(args.threads),*extra]
 if len(inputs)==2:argv+=['--r2',str(inputs[1])]
 if '--merge'not in extra:argv+=['--out1',str(d/'r1.fq')]+(['--out2',str(d/'r2.fq')]if len(inputs)==2 else[])
 d,z=run(name,argv);v=json.loads((d/'report.json').read_text());assert v['status']=='complete';original=sum((read_records(p)for p in inputs),[]);qc(v,'input',original)
 if expected is not None:
  actual=[]
  for file,records in expected.items():assert read_records(d/file)==records,(name,file);actual+=records
  qc(v,'written',actual)
 return d,v
for name,argv in [('version',['--version']),('top',['--help']),*[(n,[n,'--help'])for n in ['process','demux','dedup','report','resources']]]:
 d,z=run('help-'+name,argv)
 if name=='version':assert z.stdout.splitlines()[0]==b'SOAPnuke 3.0.0'
 if name=='process':assert b'--resource-catalog JSON'in z.stdout
se=read_records(fixture/'query-lower.fq');saved,v=process('ordinary-SE',[fixture/'query-lower.fq'],expected={'r1.fq':se});qc(v,'accepted',se)
p1=repo/'examples/reads_R1.fq.gz';p2=repo/'examples/reads_R2.fq.gz';all1=read_records(p1);all2=read_records(p2);names={'quality-equal','fraction-equal','n-equal'};expected={'r1.fq':[r for r in all1 if r['name'].split('/')[0]in names],'r2.fq':[r for r in all2 if r['name'].split('/')[0]in names]}
for n,extra in [('canonical',['--low-q','20','--max-low-q-fraction','0.5','--max-n-fraction','0.05','--min-length','30']),('aliases',['-l','20','-q','0.5','-n','0.05','-4','30'])]:
 d,v=process('ordinary-PE-'+n,[p1,p2],extra,expected);qc(v,'accepted',sum(expected.values(),[]))
merged=[{'name':'case/1','seq':'AcGt','q':[40]*4}];d=out/'overlap-case';d,v=process('overlap-case',[fixture/'mixed.R1.fq',fixture/'mixed.R2.fq'],['--correct-overlap','--merge','--unmerged-mode','discard','--overlap-min','4','--overlap-max-error','0','--merged-out',str(d/'merged.fq')],{'merged.fq':merged});assert v['processing']['overlap']['corrected_bases']==0 and v['processing']['overlap']['merged_units']==1
catalog=fixture/'catalogue-expanded.json'
for mode in ['id','path']:
 d,v=process('reference-'+mode,[fixture/'query-lower.fq'],['--screen-reference','id:ref-lower'if mode=='id'else str(fixture/'ref-lower.fa'),'--resource-catalog',str(catalog),'--screen-method','kmer-exact','--screen-k','3','--screen-min-hits','1'],{'r1.fq':se});assert v['processing']['reference']['mate_states']['H']==1;assert v['processing']['reference']['resources'][0]['sha256']==hashlib.sha256((fixture/'ref-lower.fa').read_bytes()).hexdigest()
for operation in ['list','show','verify']:run('resources-'+operation,['resources',operation,*(['adapter-lower']if operation!='list'else[]),'--resource-catalog',str(catalog)])
d,z=run('verify-N',['resources','verify','adapter-N','--resource-catalog',str(catalog)],2);assert b'only A/C/G/T'in z.stderr
bad=out/'compile-N';bad.mkdir();os.mkfifo(bad/'unfed.fq');argv=['process','--r1',str(bad/'unfed.fq'),'--out1',str(bad/'out.fq'),'--report',str(bad/'report.json'),'--adapter-file','id:adapter-N','--resource-catalog',str(catalog),'--threads',str(args.threads)];z=subprocess.run([str(binary),*argv],capture_output=True,timeout=10,env=env);(bad/'terminal.json').write_text(json.dumps({'exit':z.returncode}));(bad/'stderr').write_bytes(z.stderr);assert z.returncode==2 and b'only A/C/G/T'in z.stderr and not(bad/'out.fq').exists()and not(bad/'report.json').exists();rows.append({'name':'compile-N','exit':2})
hit=[{'name':'hit','seq':'Ttt','q':[40]*3}];d,v=process('adapter-hit',[fixture/'adapter-hit.fq'],['--adapter-file','id:adapter-lower','--resource-catalog',str(catalog),'--adapter-min-overlap','4','--adapter-max-error','0'],{'r1.fq':hit});qc(v,'accepted',hit)
for case in json.loads((pack/'module-cases.json').read_text()):
 name=case['id'];d=out/name;replace={'{PACK}':str(pack),'{FIXTURE}':str(pack/'fixtures'/name),'{OUT}':str(d),'{THREADS}':str(args.threads)};argv=[]
 for value in case['argv']:
  for key,new in replace.items():value=value.replace(key,new)
  argv.append(value)
 d,z=run(name,argv);expected=json.loads((pack/case['golden']).read_text());v=json.loads((d/'report.json').read_text());contains(v,expected['report'])
 for channel in expected['channels'].values():assert read_records(d/channel['path'],channel['phred'])==channel['records']
original=hashlib.sha256((saved/'report.json').read_bytes()).hexdigest();d=out/'report';d,z=run('report',['report','--input-report',str(saved/'report.json'),'--html-report',str(d/'qc.html'),'--tsv',str(d/'qc.tsv')]);assert hashlib.sha256((saved/'report.json').read_bytes()).hexdigest()==original;assert '<html'in(d/'qc.html').read_text().lower();table=list(csv.DictReader((d/'qc.tsv').open(),delimiter='\t'));assert len(table)==3 and {r['stage']for r in table}=={'input','accepted','written'}

# Execute the example printed by Dedup help; verify complete alternating records.
help_text=(out/'help-dedup/stdout').read_text()
assert '--out-interleaved FILE' in help_text and '--out-interleaved BOOL' not in help_text
examples=help_text.replace('\\\n',' ').splitlines()
example=next(x for x in examples if x.strip().startswith('SOAPnuke dedup') and '--out-interleaved NEW_rep.interleaved.fq.gz' in x)
d=out/'dedup-help-example';d.mkdir();(d/'scratch').mkdir()
left=[{'name':n+'/1','seq':seq,'q':[40]*len(seq)} for n,seq in [('first','ACGT'),('copy','ACGT'),('distinct','TGCA')]]
right=[{'name':n+'/2','seq':seq,'q':[30]*len(seq)} for n,seq in [('first','CCAA'),('copy','CCAA'),('distinct','GGTT')]]
write_records(d/'r1.fq.gz',left);write_records(d/'r2.fq.gz',right)
cmd=[str(binary),*shlex.split(example)[1:]];(d/'command.json').write_text(json.dumps(cmd,indent=2)+'\n')
z=subprocess.run(cmd,cwd=d,capture_output=True,timeout=30,env=env)
(d/'stdout').write_bytes(z.stdout);(d/'stderr').write_bytes(z.stderr);(d/'terminal.json').write_text(json.dumps({'exit':z.returncode}))
assert z.returncode==0,z.stderr.decode()
assert read_records(d/'NEW_rep.interleaved.fq.gz')==[left[0],right[0],left[2],right[2]]
assert json.loads((d/'NEW_interleaved.json').read_text())['status']=='complete'
rows.append({'name':'dedup-help-example','exit':0,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()})
bad=out/'dedup-interleaved-conflict'
_,z=run('dedup-interleaved-conflict',['dedup','--r1',str(d/'r1.fq.gz'),'--r2',str(d/'r2.fq.gz'),'--out1',str(bad/'wrong.fq'),'--out-interleaved',str(bad/'interleaved.fq'),'--spill-dir',str(d/'scratch'),'--report',str(bad/'report.json')],2)
assert b'out-interleaved excludes out1/out2' in z.stderr
assert not (bad/'wrong.fq').exists() and not (bad/'interleaved.fq').exists()

(out/'receipt.json').write_text(json.dumps({'status':'VALIDATED','calls':rows,'scope':'portable authored CLI/module/case smoke; not full qualification'},indent=2)+'\n');print(json.dumps({'status':'VALIDATED','CLI_calls_including_help':len(rows),'help_calls':7,'product_calls':len(rows)-7}))
