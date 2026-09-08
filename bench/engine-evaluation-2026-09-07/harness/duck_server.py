"""Isolated HTTP adapter for the pinned embedded DuckDB library."""
import http.server, json, os, pathlib, signal, sys
import duckdb

db=duckdb.connect('/data/eval.duckdb',config={
 'threads':'1','memory_limit':'128MiB','temp_directory':'/data/tmp',
 'enable_external_access':'true','autoinstall_known_extensions':'false',
 'autoload_known_extensions':'false'})
db.execute('CREATE TABLE IF NOT EXISTS samples(driver VARCHAR,metric VARCHAR,ts_ms BIGINT,value DOUBLE,PRIMARY KEY(driver,metric,ts_ms))')
def shutdown(*args):
 db.close();sys.exit(0)
signal.signal(signal.SIGTERM,shutdown)

class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_GET(self): self.handle_request()
 def do_POST(self): self.handle_request()
 def handle_request(self):
  try:
   path=self.path.split('?')[0]
   if path=='/health': result={'duckdb':duckdb.__version__}
   elif path=='/write':
    body=self.rfile.read(int(self.headers.get('Content-Length','0')))
    pathlib.Path('/data/batch.csv').write_bytes(body)
    db.execute('BEGIN')
    try:
     db.execute("COPY samples FROM '/data/batch.csv' (FORMAT CSV,HEADER false)")
     db.execute('COMMIT')
    except Exception:
     db.execute('ROLLBACK');raise
    os.unlink('/data/batch.csv');result={'rows':body.count(b'\n')}
   elif path=='/query':
    sql=self.rfile.read(int(self.headers.get('Content-Length','0'))).decode()
    c=db.execute(sql);cols=[c[0] for c in c.description]
    result=[dict(zip(cols,row)) for row in c.fetchall()]
   else: raise ValueError('unknown path')
   body=json.dumps(result,allow_nan=False).encode();self.send_response(200)
  except Exception as e:
   body=json.dumps({'error':str(e)}).encode();self.send_response(500)
  self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)

http.server.HTTPServer(('0.0.0.0',8080),Handler).serve_forever()
