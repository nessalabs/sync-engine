#!/usr/bin/env python3
"""Owned620-entry development-loopback catalogue lab; not a session transport."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import statistics
import subprocess
import tempfile
import threading
import time
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('owned_catalogue_link',HERE/'verify-session-e2e.py')
link=importlib.util.module_from_spec(spec);spec.loader.exec_module(link)
class CountHop(link.Forwarder):
 def __init__(self,port):self.bytes=0;self.statuses=Counter();super().__init__(port,'loopback',framed=True)
 def pump(self,source,target,up):
  try:
   while not self.stop.is_set():
    data=link.frame(source)
    with self.lock:
     self.bytes+=len(data)
     if up:self.requests[str(data[4])]+=1
     else:self.statuses[f'{data[4]}:{data[5] if len(data)>5 else 0}:{len(data)}']+=1
    self.send(target,data)
  except (OSError,EOFError,ValueError):pass
  finally:
   for sock in (source,target):
    try:sock.shutdown(socket.SHUT_RDWR)
    except OSError:pass
 def stats(self):
  with self.lock:return self.bytes,dict(self.requests)
def run(binary,*args):
 start=time.perf_counter();r=subprocess.run([str(binary),*map(str,args)],text=True,capture_output=True,check=True,timeout=120)
 return json.loads(r.stdout),round((time.perf_counter()-start)*1000,3)
def connect(port,child):
 deadline=time.monotonic()+5
 while time.monotonic()<deadline:
  if child.poll() is not None:raise AssertionError('owned server stopped')
  try:
   with socket.create_connection(('127.0.0.1',port),timeout=.2):return
  except OSError:time.sleep(.02)
 raise AssertionError('owned server not ready')
def rows(path,source=False):
 with sqlite3.connect(path) as db:
  table='catalogue_source_entries' if source else 'catalogue_entries'
  return db.execute(f'SELECT entry_id,creation,revision,deleted,payload FROM {table} ORDER BY creation,entry_id').fetchall()
def progress(path):
 with sqlite3.connect(path) as db:return db.execute('SELECT completed,active_boundary,cursor_creation,cursor_id FROM catalogue_progress').fetchone()
def native(root,folder,n):
 binary=root/'target/debug/examples/catalogue_apps';source=folder/'source.db';unused=folder/'unused.db';result=[]
 for phase,command in [('seed620',['seed','620']),('upsert',['upsert','entry-0000','changed']),('delete',['delete','entry-0619'])]:
  report,elapsed=run(binary,'transcript',source,unused,*command)
  expected={'seed620':620,'upsert':621,'delete':622}[phase]
  assert report.get('seeded',report.get('revision'))==expected
  with sqlite3.connect(source) as db:
   head=db.execute('SELECT head FROM catalogue_source_meta').fetchone()[0];count=db.execute('SELECT count(*) FROM catalogue_source_entries').fetchone()[0]
  assert head==expected and count==620
  result.append({'phase':phase,'repetition':n,'elapsed_ms':elapsed,'head':head,'entries':count})
 final=rows(source,True);assert final[0][1:]==(1,621,0,b'changed') and final[-1][2:]==(622,1,b'')
 return result
def wire(root,folder,path,n,dialect):
 local=root/'target/debug/examples/catalogue_apps';network=root/'target/debug/examples/catalogue_network';source=folder/'source.db';unused=folder/'unused.db'
 assert run(local,'transcript',source,unused,'seed','620')[0]=={'seeded':620}
 with socket.socket() as listener:listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
 child=subprocess.Popen([str(network),'serve',str(source),str(port),'reference-read','reference-write'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
 hops=[]
 try:
  connect(port,child)
  if path=='relay':
   opaque=link.Forwarder(port,'loopback');hops.append(opaque);port=opaque.port
  counted=CountHop(port);hops.append(counted);results=[];full=folder/'full.db';resume=folder/'resume.db'
  phases=[('bootstrap',full,40,637),('partial',resume,1,42),('restart_resume',resume,40,596 if dialect=='capabilities' else 595),('churn',full,40,4),('unchanged_recheck',full,40,1)]
  for phase,cache,pages,expected_exchanges in phases:
   if phase=='churn':
    run(local,'transcript',source,unused,'upsert','entry-0000','changed');run(local,'transcript',source,unused,'delete','entry-0619')
   before_bytes,before_requests=counted.stats();report,elapsed=run(network,'cap-pass' if dialect=='capabilities' else 'pass',counted.port,cache,'phone','reference-read',pages)
   after_bytes,after_requests=counted.stats();requests=dict(Counter(after_requests)-Counter(before_requests));exchanges=sum(requests.values());total=after_bytes-before_bytes
   assert exchanges==expected_exchanges,(phase,requests)
   assert report['protocol_bytes']+report['manifest_bytes']+report['payload_bytes']==total
   data=rows(cache);actual=rows(source,True);saved=progress(cache)
   if phase=='partial':assert data==actual[:40] and saved[:3]==(0,620,40) and report['active']
   else:assert data==actual and saved[0]==(622 if phase in ['churn','unchanged_recheck'] else 620) and not report['active']
   assert report['duplicate_bytes']==0
   results.append({'path':path,'phase':phase,'repetition':n,'elapsed_ms':elapsed,'exchanges':exchanges,'observed_operations':requests,'wire_total_bytes':total,'protocol_overhead_bytes':report['protocol_bytes'],'manifest_bytes':report['manifest_bytes'],'opaque_payload_bytes':report['payload_bytes'],'cache_entries':len(data),'completed':saved[0],'active_boundary':saved[1],'fresh_process_rows_match':True})
  return results
 finally:
  for hop in reversed(hops):hop.close()
  child.terminate();child.wait(timeout=5);child.stderr.close()
def summary(cases,fields):
 result=[]
 for key in sorted({tuple(c[f] for f in fields) for c in cases}):
  group=[c for c in cases if tuple(c[f] for f in fields)==key];values=[c['elapsed_ms'] for c in group]
  row={f:v for f,v in zip(fields,key)};row.update(n=len(group),elapsed_ms_min=min(values),elapsed_ms_median=round(statistics.median(values),3),elapsed_ms_max=max(values))
  for f in ['exchanges','wire_total_bytes','protocol_overhead_bytes','manifest_bytes','opaque_payload_bytes']:
   if f in group[0]:assert len({g[f] for g in group})==1;row[f]=group[0][f]
  result.append(row)
 return result
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=HERE.parent);p.add_argument('--client-dialect',choices=['legacy','capabilities'],default='legacy');p.add_argument('--repetitions',type=int,default=1);p.add_argument('--native-repetitions',type=int,default=5);p.add_argument('--output',type=Path);args=p.parse_args();root=args.root.resolve()
 if not 1<=args.repetitions<=5 or not 1<=args.native_repetitions<=10:p.error('bounded repetitions required')
 measured=[*root.glob('src/**/*.rs'),root/'Cargo.toml',root/'Cargo.lock',root/'examples/catalogue_apps.rs',root/'examples/catalogue_network.rs']
 def hashes():return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
 fingerprints=hashes()
 subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_apps','--example','catalogue_network'],cwd=root,check=True,capture_output=True)
 native_cases=[];wire_cases=[]
 with tempfile.TemporaryDirectory(prefix='owned-catalogue-measure-') as temp:
  for n in range(args.native_repetitions):
   folder=Path(temp)/f'native-{n}';folder.mkdir();native_cases.extend(native(root,folder,n+1))
  for n in range(args.repetitions):
   for path in ['direct','relay']:
    folder=Path(temp)/f'wire-{n}-{path}';folder.mkdir();wire_cases.extend(wire(root,folder,path,n+1,args.client_dialect))
 assert hashes()==fingerprints
 report={'client_dialect':args.client_dialect,'scope':'native publication and620-entry development-loopback wire; no session catalogue or secure channel claim','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'source_sha256':fingerprints,'harness_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__).resolve(),HERE/'verify-session-e2e.py']},'command':f'python3 scripts/measure-catalogue-publication.py --root {root} --repetitions {args.repetitions} --native-repetitions {args.native_repetitions} --client-dialect {args.client_dialect}', 'conditions':['unshaped debug builds; timings have no CI threshold or performance gain claim','direct includes counted framed TCP hop; relay adds an opaque raw TCP forwarding hop','fixture plaintext tokens; no production authentication, encryption, session or weak-link claim','wire total includes length prefixes/tokens, manifest descriptors and opaque payload; protocol overhead excludes both manifest and opaque payload bytes','wire timer includes process start, connections and receiver commit; excludes producer churn and fresh-process verification','native seed620 includes file create/migrate/open and620 separate WAL/FULL upserts; update/delete include process/open/one commit','legacy active resume skips initial head; capability resume rechecks metadata before returning a pass; explicit unchanged_recheck is measured separately','fresh SQLite connections compare every cached entry/key/revision/deletion/payload against source, plus saved progress'],'native_cases':native_cases,'native_summary':summary(native_cases,['phase']),'wire_cases':wire_cases,'wire_summary':summary(wire_cases,['path','phase']),'full_issue47_ready':False}
 if args.output:args.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
