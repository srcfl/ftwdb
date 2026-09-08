// Isolated evaluation adapter. Not FTW production code.
package main

import (
 "database/sql"
 "encoding/csv"
 "encoding/json"
 "fmt"
 "io"
 "log"
 "net/http"
 "os"
 "path/filepath"
 "strconv"
 "strings"

 "github.com/parquet-go/parquet-go"
 "github.com/parquet-go/parquet-go/compress/zstd"
 _ "modernc.org/sqlite"
)

type Sample struct {
 TsMs int64 `parquet:"ts_ms"`
 Driver string `parquet:"driver,dict,zstd"`
 Metric string `parquet:"metric,dict,zstd"`
 Value float64 `parquet:"value,zstd"`
}

func stats() map[string]any {
 out:=map[string]any{}
 for _,p:=range []string{"/sys/fs/cgroup/memory.current","/sys/fs/cgroup/memory.peak","/sys/fs/cgroup/memory.stat","/sys/fs/cgroup/memory.events","/sys/fs/cgroup/cpu.stat","/sys/fs/cgroup/io.stat","/proc/1/status"} {
  b,e:=os.ReadFile(p); if e==nil {out[p]=string(b)} else {out[p]=e.Error()}
 }
 procs:=map[string]string{};paths,_:=filepath.Glob("/proc/[0-9]*/status");for _,p:=range paths{if p==fmt.Sprintf("/proc/%d/status",os.Getpid()){continue};b,e:=os.ReadFile(p);if e==nil{procs[p]=string(b)}};out["processes"]=procs
 return out
}
func main(){
 if len(os.Args)>1 && os.Args[1]=="--stats" {json.NewEncoder(os.Stdout).Encode(stats());return}
 mode:=os.Getenv("SQLITE_SYNC");if mode==""{mode="FULL"};if mode!="FULL"&&mode!="NORMAL"{log.Fatal("bad mode")}
 db,e:=sql.Open("sqlite","/data/eval.sqlite?_pragma=journal_mode(WAL)&_pragma=synchronous("+mode+")&_pragma=cache_size(-32768)&_pragma=busy_timeout(5000)");must(e)
 db.SetMaxOpenConns(1)
 _,e=db.Exec(`CREATE TABLE IF NOT EXISTS samples(driver TEXT, metric TEXT, ts_ms INTEGER, value REAL, PRIMARY KEY(driver,metric,ts_ms)) WITHOUT ROWID`);must(e)
 handle:=func(path string,fn func(http.ResponseWriter,*http.Request)error){http.HandleFunc(path,func(w http.ResponseWriter,r *http.Request){if e:=fn(w,r);e!=nil{http.Error(w,e.Error(),500)}})}
 handle("/health",func(w http.ResponseWriter,r *http.Request)error{var v string; e:=db.QueryRow("SELECT sqlite_version()").Scan(&v);if e!=nil{return e};return json.NewEncoder(w).Encode(map[string]any{"sqlite":v,"sync":mode})})
 handle("/stats",func(w http.ResponseWriter,r *http.Request)error{return json.NewEncoder(w).Encode(stats())})
 handle("/write",func(w http.ResponseWriter,r *http.Request)error{
  tx,e:=db.Begin();if e!=nil{return e};defer tx.Rollback()
  st,e:=tx.Prepare("INSERT OR REPLACE INTO samples VALUES(?,?,?,?)");if e!=nil{return e};defer st.Close()
  cr:=csv.NewReader(io.LimitReader(r.Body,16<<20));n:=0
  for {row,e:=cr.Read();if e==io.EOF{break};if e!=nil{return e};if len(row)!=4{return fmt.Errorf("need four columns")}
   ts,e:=strconv.ParseInt(row[2],10,64);if e!=nil{return e};v,e:=strconv.ParseFloat(row[3],64);if e!=nil{return e}
   if _,e=st.Exec(row[0],row[1],ts,v);e!=nil{return e};n++
  };if e=tx.Commit();e!=nil{return e};return json.NewEncoder(w).Encode(map[string]any{"rows":n})
 })
 handle("/query",func(w http.ResponseWriter,r *http.Request)error{
  q,e:=io.ReadAll(io.LimitReader(r.Body,1<<20));if e!=nil{return e}
  rows,e:=db.QueryContext(r.Context(),string(q));if e!=nil{return e};defer rows.Close();cols,e:=rows.Columns();if e!=nil{return e}
  out:=[]map[string]any{};for rows.Next(){v:=make([]any,len(cols));p:=make([]any,len(cols));for i:=range p{p[i]=&v[i]};if e=rows.Scan(p...);e!=nil{return e};m:=map[string]any{};for i,c:=range cols{m[c]=v[i]};out=append(out,m)}
  if e=rows.Err();e!=nil{return e};return json.NewEncoder(w).Encode(out)
 })
 handle("/parquet-write",func(w http.ResponseWriter,r *http.Request)error{
  rows,e:=db.Query("SELECT ts_ms,driver,metric,value FROM samples ORDER BY ts_ms,driver,metric");if e!=nil{return e};defer rows.Close()
  tmp:="/data/day.parquet.tmp";f,e:=os.Create(tmp);if e!=nil{return e};defer f.Close()
  pw:=parquet.NewGenericWriter[Sample](f,parquet.Compression(&zstd.Codec{Level:zstd.DefaultLevel}))
  buf:=make([]Sample,0,8192);n:=0
  for rows.Next(){var sm Sample;if e=rows.Scan(&sm.TsMs,&sm.Driver,&sm.Metric,&sm.Value);e!=nil{return e};buf=append(buf,sm);n++;if len(buf)==cap(buf){if _,e=pw.Write(buf);e!=nil{return e};buf=buf[:0]}}
  if e=rows.Err();e!=nil{return e};if _,e=pw.Write(buf);e!=nil{return e};if e=pw.Close();e!=nil{return e};if e=f.Sync();e!=nil{return e};if e=f.Close();e!=nil{return e};if e=os.Rename(tmp,"/data/day.parquet");e!=nil{return e};d,e:=os.Open("/data");if e!=nil{return e};defer d.Close();if e=d.Sync();e!=nil{return e};return json.NewEncoder(w).Encode(map[string]any{"rows":n})
 })
 handle("/parquet-scan",func(w http.ResponseWriter,r *http.Request)error{
  // Matches Core's current whole-day materialisation pattern, not its full API.
  path:="/eval/snapshots/day.parquet";if r.URL.Query().Get("own")=="1"{path="/data/day.parquet"}
  f,e:=os.Open(filepath.Clean(path));if e!=nil{return e};defer f.Close();st,e:=f.Stat();if e!=nil{return e};pf,e:=parquet.OpenFile(f,st.Size());if e!=nil{return e};pr:=parquet.NewGenericReader[Sample](pf);defer pr.Close()
  all:=make([]Sample,0,1024);buf:=make([]Sample,1024);for{n,e:=pr.Read(buf);all=append(all,buf[:n]...);if e==io.EOF{break};if e!=nil{return e}}
  n:=0;sum:=0.0;driver:=r.URL.Query().Get("driver");metric:=r.URL.Query().Get("metric");for _,s:=range all{if (driver==""||s.Driver==driver)&&(metric==""||s.Metric==metric){n++;sum+=s.Value}}
  return json.NewEncoder(w).Encode(map[string]any{"n":n,"sum":sum,"scanned":len(all)})
 })
 log.Printf("sqlite adapter sync=%s",strings.ToUpper(mode));log.Fatal(http.ListenAndServe(":8080",nil))
}
func must(e error){if e!=nil{log.Fatal(e)}}
