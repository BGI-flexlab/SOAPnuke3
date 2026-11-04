"""Test-pack record/QC comparison; no production transform imports."""
import gzip
import json
import math
from collections import Counter
from pathlib import Path

PACK=Path(__file__).resolve().parent

def read_records(path,phred=33):
    op=gzip.open if str(path).endswith('.gz') else open
    with op(path,'rt') as f: lines=f.read().splitlines()
    assert len(lines)%4==0, 'FASTQ framing'
    result=[]
    for i in range(0,len(lines),4):
        assert lines[i].startswith('@') and lines[i+2].startswith('+')
        row={'name':lines[i][1:],'seq':lines[i+1],'q':[ord(c)-phred for c in lines[i+3]]}
        assert len(row['seq'])==len(row['q']) and all(0<=q<=93 for q in row['q'])
        result.append(row)
    return result

def write_records(path,rows,phred=33):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    text=''.join('@'+r['name']+'\n'+r['seq']+'\n+\n'+''.join(chr(q+phred) for q in r['q'])+'\n' for r in rows)
    if str(path).endswith('.gz'):
        with path.open('wb') as f:
            with gzip.GzipFile(fileobj=f,mode='wb',mtime=0) as z:z.write(text.encode())
    else:path.write_text(text)


def contains(actual,expected,path='root'):
    if isinstance(expected,dict):
        assert isinstance(actual,dict),(path,'object missing')
        for key,value in expected.items():
            assert key in actual,(path,key,'missing')
            contains(actual[key],value,path+'.'+key)
    elif isinstance(expected,list):
        assert isinstance(actual,list) and len(actual)==len(expected),(path,'length')
        for i,(a,e) in enumerate(zip(actual,expected)):contains(a,e,path+f'[{i}]')
    elif isinstance(expected,float):assert isinstance(actual,(int,float)) and math.isclose(actual,expected,rel_tol=1e-12,abs_tol=1e-12),(path,actual,expected)
    else:
        if isinstance(expected,(bool,int)):assert type(actual) is type(expected),(path,'numeric/bool type')
        assert actual==expected,(path,actual,expected)

# Scalar QC reference reused from test-only run_qc_cases.py; SHA256 3c23bb160e7c5c9eb11b4ea4a9120e8586438ab88cd57fc37f8499ad3ac9bbc6

def ratio(n,d): return {'numerator':n,'denominator':d,'value':None if d==0 else n/d,'unavailable_reason':'zero denominator' if d==0 else None}

def metric(rows, stage, channel):
    seqs=[r['seq'] for r in rows]; qs=[r['q'] for r in rows]
    counts={b:0 for b in 'ACGTN'}; counts['other']=0
    qhist=[0]*94; lengths=Counter(); maxlen=max(map(len,seqs),default=-1); cycles=[]
    q20=q30=nb=bases=0
    for s,q in zip(seqs,qs):
        lengths[len(s)]+=1
        for b,v in zip(s,q):
            counts[b if b in counts else 'other']+=1; bases+=1; nb+=b=='N'; q20+=v>=20; q30+=v>=30
            if not 0<=v<=93: raise ValueError(f'Q outside supported range: {v}')
            qhist[v]+=1
    for i in range(maxlen):
        part=[(r['seq'][i],r['q'][i]) for r in rows if len(r['seq'])>i]
        bc={b:0 for b in 'ACGTN'}; bc['other']=0; h=[0]*94
        for b,q in part: bc[b if b in bc else 'other']+=1; h[q]+=1
        n=len(part); q20c=sum(q>=20 for _,q in part); q30c=sum(q>=30 for _,q in part)
        cycles.append({'cycle':i+1,'read_records':n,'base_counts':bc,'quality_histogram':h,
          'mean_quality':ratio(sum(q for _,q in part),n),'q20_fraction':ratio(q20c,n),'q30_fraction':ratio(q30c,n)})
    lens=sorted(len(s) for s in seqs); n=len(lens); med=None if not n else (lens[(n-1)//2]+lens[n//2])/2
    return {'read_records':n,'bases':bases,'base_counts':counts,'n_bases':nb,'q20_bases':q20,'q30_bases':q30,
      'quality_histogram':qhist,'length':{'histogram':[lengths[i] for i in range(maxlen+1)] if n else [],'minimum':min(lens) if n else None,'maximum':max(lens) if n else None,'mean':ratio(bases,n),'median':med},
      'n_fraction':ratio(nb,bases),'gc_canonical_fraction':ratio(counts['G']+counts['C'],sum(counts[x] for x in 'ACGT')),
      'q20_fraction':ratio(q20,bases),'q30_fraction':ratio(q30,bases),'cycles':cycles}
