"""Reproducible, local-only engine evaluation. No production connections.

Run one engine at a time after coordination has freed the Docker VM.
All processes/volumes use the dedicated ftwdb-eval prefix. Retain volumes.
"""
import argparse, collections, csv, gzip, hashlib, json, math
import http.client as http_client
import os, pathlib, statistics, struct, subprocess, time, traceback, urllib.parse

ROOT=pathlib.Path(__file__).resolve().parent.parent
FIXTURE=pathlib.Path(os.environ.get('FTWDB_EVAL_FIXTURE', pathlib.Path(__file__).resolve().parents[2] / 'fixtures/ftw-real-v1/points.csv.gz'))
BASE=1767225600000
NETWORK='ftwdb-eval-20260907'
IMAGES={
 'sqlite':'alpine@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40',
 'duckdb':'python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36',
 'vm':'victoriametrics/victoria-metrics@sha256:6d164540a04f49ba4e696cbdb70f9fee78be1e94b8f2a1292743a0b1ab8275bd',
 'influx':'influxdb@sha256:b3e577f38c19963597170d8850a3a7f77af8f0cfa866c64cd13e5de0f238e114',
 'clickhouse':'clickhouse/clickhouse-server@sha256:456063a689194186633bb3db0862283068f4c2ee538852d3aaffa7eb66f1f841'}

def docker(*args,check=True):
 p=subprocess.run(['docker',*args],text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 if check and p.returncode: raise RuntimeError('docker '+str(args)+': '+p.stderr[-3000:])
 return p.stdout.strip()

def http(port,path,body=None,method=None,timeout=90):
 c=http_client.HTTPConnection('127.0.0.1',port,timeout=timeout)
 try:
  c.request(method or ('POST' if body is not None else 'GET'),path,body,{'Content-Type':'text/plain'})
  r=c.getresponse();b=r.read()
  if r.status>=300: raise RuntimeError(f'HTTP {r.status} {path}: '+b.decode(errors='replace')[:1800])
  return b
 finally: c.close()

def load_data(limit=0):
 rows=[]
 with gzip.open(FIXTURE,'rt') as f:
  for r in csv.DictReader(f):
   rows.append(('d'+r['driver_id'],'s'+r['series_id'],BASE+int(r['offset_ms']),float(r['value'])))
   if limit and len(rows)>=limit: break
 return rows

def encode(rows,engine):
 if engine=='clickhouse' and os.environ.get('FTWDB_EVAL_CH_BINARY')=='1':
  out=bytearray()
  for d,m,t,v in rows:
   for value in [d,m]:
    b=value.encode();n=len(b)
    while n>=128:out.append((n&127)|128);n>>=7
    out.append(n);out.extend(b)
   out.extend(struct.pack('<qd',t,v))
  return bytes(out)
 if engine=='influx':
  return ''.join(f'samples,driver={d},metric={m} value={v!r} {t}\n' for d,m,t,v in rows).encode()
 return ''.join(f'{d},{m},{t},{v!r}\n' for d,m,t,v in rows).encode()

def pct(v,p):
 s=sorted(v);return s[max(0,math.ceil(len(s)*p)-1)]

class Engine:
 def __init__(self,kind,tag,memory=512):
  self.kind=kind;self.basekind='sqlite' if kind.startswith('sqlite') else kind
  self.name='ftwdb-eval-'+tag;self.vol=self.name+'-data';self.port=18421
  self.memory=memory;self.command=None;self.stats={}
 def start(self):
  if not json.loads(docker('network','inspect',NETWORK,check=False) or '[]'):
   docker('network','create','--internal','--label','purpose=ftwdb-evaluation',NETWORK)
  proxy=NETWORK+'-proxy'
  if not docker('container','ls','--filter','name=^/'+proxy+'$','--format','{{.Names}}'):
   docker('run','-d','--name',proxy,'--label','purpose=ftwdb-evaluation','--network','bridge','--memory','256m','--pids-limit','64','--mount',f'type=bind,source={ROOT},target=/eval,readonly','-p','127.0.0.1:18421:8080',IMAGES['duckdb'],'python','/eval/harness/proxy.py')
   docker('network','connect',NETWORK,proxy)
  if docker('container','ls','-a','--filter','name=^/'+self.name+'$','--format','{{.Names}}'): raise RuntimeError('container already exists '+self.name)
  docker('volume','create','--label','purpose=ftwdb-evaluation',self.vol)
  kind=self.basekind
  if kind=='influx':
   docker('run','--rm','--network','none','--mount',f'type=volume,source={self.vol},target=/data',IMAGES['sqlite'],'chown','1500:1500','/data')
  args=['run','-d','--name',self.name,'--label','purpose=ftwdb-evaluation','--network',NETWORK,'--network-alias','engine',
    '--cpus','1','--memory',f'{self.memory}m','--memory-swap',f'{self.memory}m','--pids-limit','256',
    '--mount',f'type=volume,source={self.vol},target=/data',
    '--mount',f'type=bind,source={ROOT},target=/eval,readonly']
  image=IMAGES[kind]
  if kind=='sqlite': args+=['-e','GOMAXPROCS=1','-e','GOMEMLIMIT=192MiB','-e','SQLITE_SYNC='+('NORMAL' if 'normal' in self.kind else 'FULL'),image,'/eval/bin/sqlite-http']
  elif kind=='duckdb': args+=['-e','PYTHONPATH=/eval/bin/python-site',image,'python','/eval/harness/duck_server.py']
  elif kind=='vm': args+=['-e','GOMAXPROCS=1',image,'-httpListenAddr=:8080','-storageDataPath=/data','-retentionPeriod=100y','-memory.allowedBytes=64MiB','-search.maxMemoryPerQuery=64MiB','-search.maxConcurrentRequests=1']
  elif kind=='influx': args+=[image,'influxdb3','serve','--http-bind=0.0.0.0:8080','--node-id=eval','--object-store=file','--data-dir=/data','--without-auth','--wal-flush-interval=100ms','--exec-mem-pool-bytes=67108864','--force-snapshot-mem-threshold=100663296','--parquet-mem-cache-size=33554432','--datafusion-num-threads=1','--wal-replay-concurrency-limit=1']
  elif kind=='clickhouse':
   config='/eval/harness/clickhouse.xml'
   if self.memory<512:
    content=(ROOT/'harness/clickhouse.xml').read_text().replace('402653184',str(self.memory*1024*1024*3//4)).replace('134217728',str(self.memory*1024*1024//4))
    (ROOT/'results'/(self.name+'.xml')).write_text(content);config='/eval/results/'+self.name+'.xml'
   args+=['--entrypoint','clickhouse',image,'server','--config-file='+config]
  self.command=['docker',*args];docker(*args);self.ready()
  if kind=='clickhouse': self.query("CREATE TABLE samples(driver LowCardinality(String),metric LowCardinality(String),ts_ms Int64,value Float64) ENGINE=MergeTree ORDER BY (driver,metric,ts_ms) SETTINGS fsync_after_insert=1,fsync_part_directory=1")
 def ready(self):
  path={'vm':'/health','influx':'/health','clickhouse':'/ping'}.get(self.basekind,'/health')
  end=time.monotonic()+60
  while time.monotonic()<end:
   try: return http(self.port,path,timeout=1).decode()
   except Exception:
    state=json.loads(docker('inspect',self.name))[0]['State']
    if not state['Running']: raise RuntimeError('startup failed '+json.dumps(state)+' '+docker('logs','--tail','25',self.name,check=False))
    time.sleep(.2)
  raise RuntimeError('not ready after 60s')
 def query(self,sql):
  kind=self.basekind
  if kind=='influx':
   b=http(self.port,'/api/v3/query_sql?'+urllib.parse.urlencode({'db':'eval','q':sql,'format':'json'}))
  elif kind=='clickhouse':
   if sql.lstrip().upper().startswith(('SELECT','WITH')): sql+=' FORMAT JSONEachRow'
   b=http(self.port,'/?output_format_json_quote_64bit_integers=0',sql.encode())
   return [json.loads(l) for l in b.splitlines() if l]
  else: b=http(self.port,'/query',sql.encode())
  return json.loads(b or b'[]')
 def write(self,body):
  path={'vm':'/api/v1/import/csv?format=1:label:driver,2:label:metric,3:time:unix_ms,4:metric:sample',
        'influx':'/api/v3/write_lp?db=eval&precision=millisecond&no_sync=false',
        'clickhouse':'/?query=INSERT%20INTO%20samples%20FORMAT%20'+('RowBinary' if os.environ.get('FTWDB_EVAL_CH_BINARY')=='1' else 'CSV')}.get(self.basekind,'/write')
  return http(self.port,path,body)
 def raw(self,d,m):
  if self.basekind=='vm':
   p='/api/v1/export?'+urllib.parse.urlencode({'match[]':f'sample{{driver="{d}",metric="{m}"}}','start':BASE/1000,'end':(BASE+86400000)/1000})
   data=http(self.port,p);out=[]
   for l in data.splitlines():
    obj=json.loads(l);out.extend(zip(obj['timestamps'],obj['values']))
   return sorted(out)
  t='CAST(time AS BIGINT)/1000000' if self.basekind=='influx' else 'ts_ms'
  return [(int(r['ts_ms']),r['value']) for r in self.query(f"SELECT {t} AS ts_ms,value FROM samples WHERE driver='{d}' AND metric='{m}' ORDER BY ts_ms")]
 def snapshot_stats(self,name):
  s=json.loads(docker('exec',self.name,'/eval/bin/sqlite-http','--stats'));self.stats[name]=s;return s
 def stop(self):
  docker('stop','-t','30',self.name,check=False)
 def save_logs(self,tag):
  p=subprocess.run(['docker','logs',self.name],capture_output=True)
  (ROOT/'logs'/f'{tag}.log').write_bytes(p.stdout+p.stderr)

def run_trial(kind,tag,rows,memory):
 e=Engine(kind,tag,memory);r={'kind':kind,'tag':tag,'memory_mib':memory,'cpus':1,'points':len(rows),'image':IMAGES[e.basekind],'clickhouse_binary':os.environ.get('FTWDB_EVAL_CH_BINARY')=='1','started_at':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
 by=collections.defaultdict(list)
 for d,m,t,v in rows: by[(d,m)].append((t,v))
 batches=[encode(rows[i:i+10000],e.basekind) for i in range(0,len(rows),10000)]
 try:
  e.start();r['command']=e.command;r['health']=e.ready();e.snapshot_stats('idle')
  times=[];started=time.perf_counter()
  for b in batches:
   if (ROOT/'PAUSE').exists(): raise RuntimeError('PAUSED for release runtime; exclude this incomplete trial')
   t=time.perf_counter();e.write(b);times.append((time.perf_counter()-t)*1000)
  r['submit_seconds']=time.perf_counter()-started;r['batch_latency_ms']={'p50':statistics.median(times),'p95':pct(times,.95),'samples':times}
  e.snapshot_stats('after_ingest')
  if e.basekind=='vm':
   # This forces query visibility only; it is not a per-request durability ACK.
   t=time.perf_counter();http(e.port,'/internal/force_flush',b'');r['force_visibility_seconds']=time.perf_counter()-t
  expected_count=len(rows);bad=0;missing=0;extra=0;duplicates=0;maxerr=0.;examples=[];compared=0
  t=time.perf_counter()
  for (d,m),want in sorted(by.items()):
   got=e.raw(d,m);want=sorted(want)
   wm=dict(want);gm=dict(got);missing+=len(set(wm)-set(gm));extra+=len(set(gm)-set(wm));duplicates+=len(got)-len(gm)
   for ts in wm.keys()&gm.keys():
    a,b=wm[ts],gm[ts];compared+=1
    if struct.pack('>d',a)!=struct.pack('>d',b):
     bad+=1;maxerr=max(maxerr,abs(a-b))
     if len(examples)<5:examples.append({'driver':d,'metric':m,'ts_ms':ts,'expected':a,'actual':b})
  r['validation']={'compared':compared,'expected':expected_count,'missing':missing,'extra':extra,'duplicates':duplicates,'f64_bit_differences':bad,'max_absolute_error':maxerr,'examples':examples,'seconds':time.perf_counter()-t}
  r['queries']={}
  if e.basekind!='vm':
   tcol='CAST(time AS BIGINT)/1000000' if e.basekind=='influx' else 'ts_ms'
   qs={'day_aggregate':f'SELECT driver,metric,count(*) AS n,min(value) AS lo,max(value) AS hi,sum(value) AS total FROM samples GROUP BY driver,metric ORDER BY driver,metric',
      'five_minute':f'SELECT driver,metric,CAST(floor(({tcol}-{BASE})/300000.0) AS BIGINT) AS bucket,count(*) AS n,sum(value) AS total FROM samples GROUP BY driver,metric,bucket ORDER BY driver,metric,bucket'}
   for name,q in qs.items():
    elapsed=[];answer=None
    for _ in range(5):
     t=time.perf_counter();answer=e.query(q);elapsed.append((time.perf_counter()-t)*1000)
    r['queries'][name]={'ms':elapsed,'median_ms':statistics.median(elapsed),'result_rows':len(answer),'count_sum':sum(int(a['n']) for a in answer)}
    if sum(int(a['n']) for a in answer)!=len(rows):raise AssertionError('aggregate count mismatch')
  else:
   q='sum(count_over_time(sample[24h]))'
   elapsed=[];answer=None
   for _ in range(5):
    t=time.perf_counter();answer=json.loads(http(e.port,'/api/v1/query?'+urllib.parse.urlencode({'query':q,'time':(BASE+86400000)/1000,'nocache':'1'})));elapsed.append((time.perf_counter()-t)*1000)
   r['queries']['count_over_24h']={'ms':elapsed,'median_ms':statistics.median(elapsed),'answer':answer}
  e.snapshot_stats('after_query')
  if kind=='sqlite-full' and tag.endswith('-r1'):
   t=time.perf_counter();r['parquet_write']=json.loads(http(e.port,'/parquet-write',b''));r['parquet_write_seconds']=time.perf_counter()-t
   docker('cp',e.name+':/data/day.parquet',str(ROOT/'snapshots/day.parquet'))
  e.snapshot_stats('final');r['stats']=e.stats
  r['state']=json.loads(docker('inspect',e.name))[0]['State'];r['ok']=True
 except Exception as x:
  r['error']=str(x);r['traceback']=traceback.format_exc();r['ok']=False;r['stats']=e.stats
  try:r['state']=json.loads(docker('inspect',e.name))[0]['State']
  except Exception:pass
 finally:
  e.stop();e.save_logs(tag)
  r['volume']=e.vol
  (ROOT/'results'/f'{tag}.json').write_text(json.dumps(r,indent=2))
 return r

def main():
 p=argparse.ArgumentParser();p.add_argument('--engines',default='sqlite-full,sqlite-normal,duckdb,vm,influx,clickhouse');p.add_argument('--repeats',type=int,default=3);p.add_argument('--limit',type=int,default=0);p.add_argument('--memory',type=int,default=512);p.add_argument('--prefix',default='main');a=p.parse_args()
 rows=load_data(a.limit)
 (ROOT/'results'/'fixture.json').write_text(json.dumps({'file':str(FIXTURE),'sha256':hashlib.sha256(FIXTURE.read_bytes()).hexdigest(),'base_ms':BASE,'rows':len(rows),'series':len({(r[0],r[1]) for r in rows}),'ordering':'offset_ms then series_id','batch_points':10000},indent=2))
 for rep in range(1,a.repeats+1):
  # Rotate order across repetitions to reduce systematic order effects.
  engines=a.engines.split(',');engines=engines[rep-1:]+engines[:rep-1]
  for k in engines:
   if (ROOT/'PAUSE').exists(): print('PAUSED',flush=True);return
   tag=f'{a.prefix}-{k}-r{rep}';print('START',tag,flush=True);r=run_trial(k,tag,rows,a.memory)
   print(json.dumps({x:r.get(x) for x in ('tag','ok','submit_seconds','validation','queries','error')}),flush=True)

if __name__=='__main__':main()
