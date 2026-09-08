"""Uniform local benchmark transport; database network has no egress route."""
import http.client,http.server

class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a): pass
 def do_GET(self): self.forward()
 def do_POST(self): self.forward()
 def forward(self):
  c=None
  try:
   body=self.rfile.read(int(self.headers.get('Content-Length','0')))
   c=http.client.HTTPConnection('engine',8080,timeout=90)
   c.request(self.command,self.path,body,{'Content-Type':self.headers.get('Content-Type','text/plain')})
   r=c.getresponse();data=r.read();self.send_response(r.status)
  except Exception as e: data=str(e).encode();self.send_response(502)
  finally:
   if c: c.close()
  self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
http.server.ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
