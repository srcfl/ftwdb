"""Small functional checks, not throughput benchmarks or power-cut tests."""
import json, pathlib, time, traceback
from run import ROOT,Engine,encode,http,docker,BASE

def one(kind):
 tag='semantics-'+kind;e=Engine(kind,tag,512)
 rows=[('sentinel','watts',BASE+i*1000,123456.123456789+i) for i in range(50)]
 r={'kind':kind,'sent_points':len(rows),'scope':'process SIGKILL only; no VM reboot, fsync fault injection, or physical power cut'}
 try:
  e.start();r['command']=e.command
  e.write(encode(rows,e.basekind));ack=time.monotonic()
  docker('kill','--signal','KILL',e.name);r['ack_to_kill_return_ms']=(time.monotonic()-ack)*1000
  r['killed_state']=json.loads(docker('inspect',e.name))[0]['State']
  docker('start',e.name);e.ready()
  if e.basekind=='vm': http(e.port,'/internal/force_flush',b'')
  got=e.raw('sentinel','watts');r['after_restart_rows']=len(got)
  r['after_restart_matching_values']=sum(a==b for a,b in zip(got,[(t,v) for _,_,t,v in rows]))
  # Retry/correction exercise on a separate series; no implicit idempotency claim.
  point=[('revision','watts',BASE+1234,1.25)]
  r['retry_responses']=[]
  for batch in [point,point,[('revision','watts',BASE+1234,2.5)]]:
   try:e.write(encode(batch,e.basekind));r['retry_responses'].append('accepted')
   except Exception as x:r['retry_responses'].append(str(x))
  if e.basekind=='vm':http(e.port,'/internal/force_flush',b'')
  r['same_identity_readback']=e.raw('revision','watts')
  r['stats']=e.snapshot_stats('complete');r['ok']=True
 except Exception as x:r.update(ok=False,error=str(x),traceback=traceback.format_exc())
 finally:
  e.stop();e.save_logs(tag)
  (ROOT/'results'/(tag+'.json')).write_text(json.dumps(r,indent=2))
 print(json.dumps({k:v for k,v in r.items() if k not in ('stats','command','traceback')}),flush=True)

if __name__=='__main__':
 for kind in ['sqlite-full','sqlite-normal','duckdb','vm','influx','clickhouse']:one(kind)
