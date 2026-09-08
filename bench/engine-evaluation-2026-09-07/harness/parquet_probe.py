"""Same Go-written Parquet file, projected SQL vs current whole-day read pattern."""
import collections,hashlib,json,statistics,time,traceback
from run import ROOT,Engine,http,load_data

def main():
 rows=load_data();by=collections.defaultdict(list)
 for d,m,t,v in rows:by[(d,m)].append((t,v))
 (driver,metric),series=max(by.items(),key=lambda x:len(x[1]))
 expected={'all':len(rows),'one_series':len(series)}
 for rep in range(1,4):
  for kind in ['sqlite-full','duckdb']:
   tag=f'parquet-{kind}-r{rep}';e=Engine(kind,tag,256);r={'kind':kind,'memory_mib':256,'file_sha256':hashlib.sha256((ROOT/'snapshots/day.parquet').read_bytes()).hexdigest(),'queries':{}}
   try:
    e.start();e.snapshot_stats('idle')
    for name in expected:
     where='' if name=='all' else f" WHERE driver='{driver}' AND metric='{metric}'"
     query="SELECT count(*) AS n,sum(value) AS total FROM read_parquet('/eval/snapshots/day.parquet')"+where
     path='/parquet-scan'+('' if name=='all' else f'?driver={driver}&metric={metric}')
     times=[]
     for _ in range(6):
      t=time.perf_counter()
      result=e.query(query)[0] if kind=='duckdb' else json.loads(http(e.port,path))
      times.append((time.perf_counter()-t)*1000)
      assert int(result['n'])==expected[name],result
     r['queries'][name]={'first_ms':times[0],'warm_ms':times[1:],'median_warm_ms':statistics.median(times[1:]),'result':result}
    if kind=='duckdb' and rep==1:
     compared=0;bad=0
     for (d,m),want in by.items():
      got=e.query(f"SELECT ts_ms,value FROM read_parquet('/eval/snapshots/day.parquet') WHERE driver='{d}' AND metric='{m}' ORDER BY ts_ms")
      assert len(got)==len(want)
      for a,b in zip(got,sorted(want)):
       compared+=1;bad+=(a['ts_ms'],a['value'])!=b
     r['parquet_readback']={'compared':compared,'value_or_timestamp_differences':bad}
    r['ok']=True
   except Exception as x:r.update(ok=False,error=str(x),traceback=traceback.format_exc())
   finally:
    try:e.snapshot_stats('final')
    except Exception:pass
    r['stats']=e.stats;e.stop();e.save_logs(tag)
    (ROOT/'results'/(tag+'.json')).write_text(json.dumps(r,indent=2))
   print(json.dumps({k:v for k,v in r.items() if k not in ('stats','traceback')}),flush=True)

if __name__=='__main__':main()
