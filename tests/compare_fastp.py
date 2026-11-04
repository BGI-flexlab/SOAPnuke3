#!/usr/bin/env python3
"""Differential tests against the pinned fastp executable; artifacts are retained."""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import random
import subprocess

ap=argparse.ArgumentParser()
ap.add_argument('--soap',required=True)
ap.add_argument('--fastp',required=True)
ap.add_argument('--out',required=True)
a=ap.parse_args()
soap,fastp=Path(a.soap).resolve(),Path(a.fastp).resolve()
out=Path(a.out).resolve();out.mkdir(parents=True)
env={**os.environ,'TMPDIR':str(out)}
rows=[]
version=subprocess.run([str(fastp),'--version'],capture_output=True,text=True,check=True)
assert (version.stdout+version.stderr).strip()=='fastp 1.4.0','This comparison requires the pinned fastp 1.4.0 reference'

def run(folder,cmd):
    folder.mkdir(parents=True,exist_ok=True)
    (folder/'command.json').write_text(json.dumps([str(x) for x in cmd],indent=2)+'\n')
    with (folder/'stdout.txt').open('w') as stdout,(folder/'stderr.txt').open('w') as stderr:
        p=subprocess.run([str(x) for x in cmd],stdout=stdout,stderr=stderr,env=env,timeout=120)
    (folder/'exit.json').write_text(json.dumps({'exit':p.returncode})+'\n')
    assert p.returncode==0,(folder,(folder/'stderr.txt').read_text()[-4000:])

def write(path,records,phred=33):
    path.write_text(''.join('@'+n+'\n'+s+'\n+\n'+''.join(chr(q+phred) for q in qs)+'\n' for n,s,qs in records))

def read(path):
    if not path.exists():return []
    lines=path.read_text().splitlines()
    assert len(lines)%4==0,path
    return [(lines[i][1:].split()[0],lines[i+1],lines[i+3]) for i in range(0,len(lines),4)]

def compare(label,r1,r2=None,soap_options=(),fastp_options=(),phred=33,expected=None):
    d=out/label;d.mkdir()
    write(d/'r1.fq',r1,phred)
    if r2 is not None:write(d/'r2.fq',r2,phred)
    merged='--merge' in soap_options
    for tool,exe,options in [('soap',soap,soap_options),('fastp',fastp,fastp_options)]:
        f=d/tool;f.mkdir()
        if tool=='soap':
            cmd=[exe,'process','--r1',d/'r1.fq','--out1',f/'r1.fq','--report',f/'report.json','--threads','2','--input-phred',str(phred),'--output-phred','33']
            if r2 is not None:cmd+=['--r2',d/'r2.fq','--out2',f/'r2.fq','--overlap-min','30','--overlap-max-error','0.2']
            if merged:cmd+=['--merged-out',f/'merged.fq']
        else:
            cmd=[exe,'-i',d/'r1.fq','-o',f/'r1.fq','-j',f/'report.json','-h',f/'report.html','-w','1','-A','-G','-Q','-L','--dont_eval_duplication']
            if phred==64:cmd+=['--phred64']
            if r2 is not None:cmd+=['-I',d/'r2.fq','-O',f/'r2.fq','--overlap_len_require','30','--overlap_diff_limit','5','--overlap_diff_percent_limit','20']
            if merged:cmd+=['--merged_out',f/'merged.fq']
        run(f,cmd+list(options))
    channels=['r1.fq']+(['r2.fq'] if r2 is not None else [])+(['merged.fq'] if merged else [])
    counts={}
    for channel in channels:
        actual,reference=read(d/'soap'/channel),read(d/'fastp'/channel)
        assert actual==reference,(label,channel,next(((x,y) for x,y in itertools.zip_longest(actual,reference) if x!=y),None))
        counts[channel]=len(actual)
    if expected is not None:
        assert [x[0] for x in read(d/'soap/r1.fq')]==expected,(label,'independent boundary truth')
    rows.append({'case':label,'input_units':len(r1),'output_records':counts,'status':'PASS'})
    (out/'results.json').write_text(json.dumps(rows,indent=2)+'\n')
    print(label,'PASS',counts,flush=True)

rng=random.Random(20261104)
def rc(s):return s.translate(str.maketrans('ACGTN','TGCAN'))[::-1]
complexity_seqs=['A','N','AA','NN','AN','AaAa','AAAAANAAAAA','ACGTN','NNNNNN','AAAAC','AAGT']
complexity_seqs+=[''.join(rng.choices('ACGTN',k=rng.choice([2,5,11,50,151]))) for _ in range(200)]
complexity=[(f'complexity{i}',s,[40]*len(s)) for i,s in enumerate(complexity_seqs)]
for pct in [0,20,21,30,60,100]:
    expected=[n for n,s,q in complexity if len(s)>1 and sum(x!=y for x,y in zip(s,s[1:]))/(len(s)-1)>=pct/100]
    compare('complexity_'+str(pct),complexity,soap_options=['--min-complexity',str(pct/100)],fastp_options=['-y','-Y',str(pct)],expected=expected)

left=[];right=[];truth=[]
for geometry in ['positive','zero','negative','contained']:
  for q1,q2 in itertools.product([0,14,15,29,30,40],repeat=2):
    insert=''.join(rng.choices('ACGT',k={'positive':100,'zero':80,'negative':60,'contained':90}[geometry]))
    if geometry=='negative':s1=insert+'AGATCGGAAGAGCACACGTC';s2=rc(insert)+'AGATCGGAAGAGCGTCGTGT';i,j=25,34
    elif geometry=='contained':s1=insert;s2=rc(insert[-60:]);i,j=45,44
    else:s1=insert[:80];s2=rc(insert[-80:]);i=45;j=len(insert)-1-i
    s2=s2[:j]+next(b for b in 'ACGT' if b!=s2[j])+s2[j+1:]
    qa=[40]*len(s1);qb=[40]*len(s2);qa[i]=q1;qb[j]=q2
    n=f'{geometry}_q{q1}_{q2}'
    left.append((n+'/1',s1,qa));right.append((n+'/2',s2,qb))
    truth.append({'name':n,'mismatch_r1_index':i,'mismatch_r2_index':j,'correct':q1>=30 and q2<=14 or q2>=30 and q1<=14})
# Agreement keeps the lower R1 quality during merging, plus unmergeable routing.
s=''.join(rng.choices('ACGT',k=80))
left += [('agreement/1',s,[10]*80),('unmergeable/1','A'*80,[40]*80)]
right += [('agreement/2',rc(s),[40]*80),('unmergeable/2','A'*80,[40]*80)]
(out/'boundary-truth.json').write_text(json.dumps(truth,indent=2)+'\n')
for phred in [33,64]:
  for mode,so,fo in [('correct',['--correct-overlap'],['--correction']),('merge',['--merge'],['--merge']),('correct_merge',['--correct-overlap','--merge'],['--correction','--merge'])]:
    compare(mode+'_phred'+str(phred),left,right,so,fo,phred)
    d=out/(mode+'_phred'+str(phred))/'soap'
    if mode=='correct':
      a1={n:(s,q) for n,s,q in read(d/'r1.fq')};a2={n:(s,q) for n,s,q in read(d/'r2.fq')}
      for l,r,t in zip(left,right,truth):
        n,s,q=l;m,v,w=r;i,j=t['mismatch_r1_index'],t['mismatch_r2_index']
        ls,lq=a1[n];rs,rq=a2[m]
        if q[i]>=30 and w[j]<=14:assert rs[j]==rc(s[i]) and ord(rq[j])-33==q[i]
        elif w[j]>=30 and q[i]<=14:assert ls[i]==rc(v[j]) and ord(lq[i])-33==w[j]
        else:assert ls==s and rs==v and lq==''.join(chr(x+33) for x in q) and rq==''.join(chr(x+33) for x in w)
    if mode=='merge':
      agreement=next(x for x in read(d/'merged.fq') if x[0]=='agreement/1')
      assert agreement[2]=='+'*80

for syntax in [['--correction-delta','5'],['--correction-delta=5']]:
  p=subprocess.run([str(soap),'process',*syntax],capture_output=True,text=True,env=env)
  assert p.returncode!=0 and 'was removed' in p.stderr and 'Q>=30 / Q<=14' in p.stderr
  rows.append({'case':'removed '+syntax[0],'status':'PASS'})
config=out/'removed.json';config.write_text(json.dumps({'job':'process','options':{'correction-delta':5}}))
p=subprocess.run([str(soap),'process','--config',str(config)],capture_output=True,text=True,env=env)
assert p.returncode!=0 and 'was removed' in p.stderr
rows.append({'case':'removed config option','status':'PASS'})
(out/'results.json').write_text(json.dumps({'reference':'fastp 1.4.0','soap_sha256':hashlib.sha256(soap.read_bytes()).hexdigest(),'fastp_sha256':hashlib.sha256(fastp.read_bytes()).hexdigest(),'rows':rows},indent=2)+'\n')
print('ALL PASS',len(rows),flush=True)
