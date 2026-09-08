"""Offline archive restore, full readback, on new disposable volumes."""
import collections,json
from run import ROOT,IMAGES,Engine,docker,load_data

def main():
 by=collections.defaultdict(list)
 for d,m,t,v in load_data():by[(d,m)].append((t,v))
 results=[]
 for kind in ['sqlite-full','duckdb']:
  e=Engine(kind,'restore-'+kind,256)
  assert not docker('volume','ls','--filter','name=^'+e.vol+'$','--format','{{.Name}}')
  docker('volume','create','--label','purpose=ftwdb-evaluation',e.vol)
  docker('run','--rm','--network','none','--mount',f'type=volume,source={e.vol},target=/target','--mount',f'type=bind,source={ROOT},target=/eval,readonly',IMAGES['sqlite'],'tar','-xf','/eval/snapshots/store-'+kind+'.tar','-C','/target','--strip-components=1')
  r={'kind':kind,'scope':'offline archive; new volume; full readback'}
  try:
   e.start();n=0
   for (d,m),want in by.items():
    got=e.raw(d,m);assert got==sorted(want),(kind,d,m);n+=len(got)
   r['matching_points']=n
   if kind=='sqlite-full':r['integrity_check']=e.query('PRAGMA integrity_check')
   r['ok']=True
  except Exception as x:r.update(ok=False,error=str(x))
  finally:e.stop();e.save_logs('restore-'+kind)
  results.append(r);print(json.dumps(r),flush=True)
 (ROOT/'results/restore.json').write_text(json.dumps(results,indent=2))

if __name__=='__main__':main()
