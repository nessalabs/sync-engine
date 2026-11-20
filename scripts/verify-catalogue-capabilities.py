#!/usr/bin/env python3
"""Owned real-TCP typed catalogue refusal/restart lab, no production channel claim."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('owned_catalogue_cap_link',ROOT/'scripts/measure-catalogue-publication.py');lab=importlib.util.module_from_spec(spec);spec.loader.exec_module(lab)
READ='reference-read';WRITE='reference-write'
def snapshot(path):
 with sqlite3.connect(path) as db:return db.execute('SELECT * FROM catalogue_progress').fetchall(),db.execute('SELECT * FROM catalogue_entries ORDER BY entry_id').fetchall()
def start(source,fault=None):
 with socket.socket() as listener:listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
 args=[str(ROOT/'target/debug/examples/catalogue_network'),'serve',str(source),str(port),READ,WRITE]
 if fault:args.append(fault)
 child=subprocess.Popen(args,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);lab.connect(port,child);return child,port
def close(child,hops):
 for hop in reversed(hops):hop.close()
 child.terminate();child.wait(timeout=5);child.stderr.close()
def hops(port,path):
 owned=[]
 if path=='relay':
  opaque=lab.link.Forwarder(port,'loopback');owned.append(opaque);port=opaque.port
 counted=lab.CountHop(port);owned.append(counted);return owned,counted
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path);args=p.parse_args()
 subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_apps','--example','catalogue_network'],cwd=ROOT,check=True,capture_output=True)
 subprocess.run(['cargo','test','--locked','--all-features','--lib','catalogue_capabilities'],cwd=ROOT,check=True,capture_output=True)
 measured=[*ROOT.glob('src/**/*.rs'),ROOT/'examples/catalogue_network.rs',ROOT/'examples/catalogue_apps.rs',Path(__file__).resolve(),ROOT/'scripts/measure-catalogue-publication.py',ROOT/'scripts/verify-session-e2e.py',ROOT/'Cargo.toml',ROOT/'Cargo.lock']
 def hashes():return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
 fingerprint=hashes();cases=[];faults=[]
 with tempfile.TemporaryDirectory(prefix='owned-catalogue-capability-') as folder:
  local=ROOT/'target/debug/examples/catalogue_apps';network=ROOT/'target/debug/examples/catalogue_network'
  for path in ['direct','relay']:
   f=Path(folder)/path;f.mkdir();source=f/'source.db';partial=f/'partial.db';full=f/'full.db';cold=f/'cold.db'
   assert lab.run(local,'transcript',source,partial,'seed','620')[0]=={'seeded':620}
   child,port=start(source);owned,counted=hops(port,path)
   try:
    first,_=lab.run(network,'cap-pass',counted.port,partial,'phone',READ,'1');assert first['count']==40 and first['completed']==0 and first['active'];saved_partial=snapshot(partial)
    with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET writer_required=?',(((1<<64)-1).to_bytes(8,'big'),))
    writer,_=lab.run(network,'cap-pass',counted.port,full,'phone',READ,'40');assert writer['completed']==writer['count']==620;saved_full=snapshot(full)
    with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET reader_required=?',(bytes.fromhex('0000000000000001'),))
    for phase,cache,saved in [('resumed',partial,saved_partial),('completed',full,saved_full),('cold',cold,([],[]))]:
     before_bytes,before_ops=counted.stats()
     with counted.lock:before_status=dict(counted.statuses)
     report,elapsed=lab.run(network,'cap-pass',counted.port,cache,'phone',READ,'40')
     after_bytes,after_ops=counted.stats()
     with counted.lock:statuses=dict(Counter(counted.statuses)-Counter(before_status))
     operations=dict(Counter(after_ops)-Counter(before_ops));wire=after_bytes-before_bytes
     assert report=={'typed_incompatible':True,'exchanges':1,'protocol_bytes':wire,'payload_bytes':0}
     assert operations=={'17':1} and statuses=={'8:17:25':1},(operations,statuses)
     assert snapshot(cache)==saved
     cases.append({'path':path,'phase':phase,'elapsed_ms':elapsed,'wire_total_bytes':wire,'protocol_bytes':wire,'opaque_payload_bytes':0,'exchanges':1,'observed_operations':operations,'observed_status_operation_frame_bytes':statuses,'cache_and_progress_unchanged_after_reopen':True})
   finally:close(child,owned)
   # Separate zero-requirement owned source for ordinary disconnect recovery.
   for fault in ['drop','truncate']:
    source=f/f'{fault}.db';cache=f/f'{fault}-cache.db';assert lab.run(local,'transcript',source,cache,'seed','620')[0]=={'seeded':620}
    child,port=start(source);owned,counted=hops(port,path)
    try:assert lab.run(network,'cap-pass',counted.port,cache,'phone',READ,'1')[0]['count']==40
    finally:close(child,owned)
    saved=snapshot(cache);child,port=start(source,fault);owned,counted=hops(port,path)
    try:
     result=subprocess.run([str(network),'cap-pass',str(counted.port),str(cache),'phone',READ,'40'],text=True,capture_output=True,timeout=60);assert result.returncode!=0;assert snapshot(cache)==saved
     recovered,_=lab.run(network,'cap-pass',counted.port,cache,'phone',READ,'40');assert recovered['completed']==recovered['count']==620;assert lab.rows(cache)==lab.rows(source,True)
     faults.append({'path':path,'fault':fault,'failed_call_preserved_exact_active_cache':True,'explicit_retry_fresh_admission_recovered620':True})
    finally:close(child,owned)
 assert hashes()==fingerprint
 report={'scope':'version1 catalogue operations on development loopback real direct/opaque-relay TCP','full_issue47_ready':False,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'source_sha256':fingerprint,'command':'python3 scripts/verify-catalogue-capabilities.py','conditions':['owned plaintext fixture tokens, no session/production-security qualification','620 entries, active40-entry cursor; writer-only read completes620, reader requirement then changes0→1 monotonically','ordinary explicit retry after disconnect; no implicit downgrade/retry in client','refusal timings are unshaped debug single samples, not performance statistics or CI thresholds'],'cases':cases,'disconnect_recovery':faults}
 if args.output:args.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
