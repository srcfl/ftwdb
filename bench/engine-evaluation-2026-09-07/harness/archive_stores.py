"""Archive only the evaluation's own stopped stores and record their size."""
import json, pathlib,subprocess
from run import ROOT,IMAGES,docker

SELECTION={'sqlite-full':'main-sqlite-full-r3','sqlite-normal':'main-sqlite-normal-r3','duckdb':'main-duckdb-r3','vm':'main-vm-r3','influx':'main-influx-r3','clickhouse':'binary-clickhouse-r3'}

def main():
 args=['run','--rm','--network','none','--cpus','1','--memory','128m','--mount',f'type=bind,source={ROOT},target=/eval']
 for kind,tag in SELECTION.items():
  r=json.loads((ROOT/'results'/(tag+'.json')).read_text());assert r['ok']
  state=json.loads(docker('inspect','ftwdb-eval-'+tag))[0]['State'];assert not state['Running']
  args+=['--mount',f"type=volume,source={r['volume']},target=/vols/{kind},readonly"]
 code='''
import pathlib,json,tarfile,hashlib
out={}
for root in pathlib.Path('/vols').iterdir():
 files=[p for p in root.rglob('*') if p.is_file()]
 entries=[{'path':str(p.relative_to(root)),'bytes':p.stat().st_size,'allocated_bytes':p.stat().st_blocks*512} for p in files]
 target=pathlib.Path('/eval/snapshots')/('store-'+root.name+'.tar')
 with tarfile.open(target,'w') as tar:tar.add(root,arcname='data')
 with tarfile.open(target,'r') as tar:assert sum(m.isfile() for m in tar.getmembers())==len(files)
 out[root.name]={'logical_bytes':sum(p['bytes'] for p in entries),'allocated_bytes':sum(p['allocated_bytes'] for p in entries),'files':entries,'archive_sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
pathlib.Path('/eval/results/store-footprints.json').write_text(json.dumps(out,indent=2))
print(json.dumps({k:{'bytes':v['logical_bytes'],'files':len(v['files'])} for k,v in out.items()}))
'''
 args+=[IMAGES['duckdb'],'python','-c',code];print(docker(*args))

if __name__=='__main__':main()
