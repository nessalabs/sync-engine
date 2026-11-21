#!/usr/bin/env python3
"""Owned real-TCP v2 catalogue coverage, collection/reset and mixed-version lab."""
import argparse,hashlib,importlib.util,io,json,socket,sqlite3,statistics,subprocess,tarfile,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BASE='8596fd36e922af25fe1da2d2524a6e65673ee25b'
spec=importlib.util.spec_from_file_location('owned_coverage_wire',ROOT/'scripts/measure-catalogue-publication.py');lab=importlib.util.module_from_spec(spec);spec.loader.exec_module(lab)
READ='reference-read';WRITE='reference-write'
def snapshot(path):
 with sqlite3.connect(path) as c:return {t:c.execute(f'SELECT * FROM {t} ORDER BY 1,2,3,4').fetchall() for t in ['catalogue_entries','catalogue_progress','catalogue_candidates']}
def current_rows_match(source,cache):
 with sqlite3.connect(source) as s,sqlite3.connect(cache) as d:
  assert s.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries ORDER BY entry_id').fetchall()==d.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_entries ORDER BY entry_id').fetchall()
def exact(source,cache):
 with sqlite3.connect(source) as s,sqlite3.connect(cache) as d:
  assert s.execute("SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries UNION ALL SELECT entry_id,creation,revision,1,x'' FROM catalogue_retired_proof ORDER BY entry_id").fetchall()==d.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_entries ORDER BY entry_id').fetchall()
  assert d.execute('SELECT count(*),sum(deleted) FROM catalogue_entries').fetchone()==(620,160)
  assert d.execute('SELECT completed,active_boundary,reset_phase FROM catalogue_progress').fetchone()==(780,None,0)
  assert d.execute('SELECT count(*) FROM catalogue_candidates').fetchone()==(0,)
def start(binary,source,fault=None):
 with socket.socket() as l:l.bind(('127.0.0.1',0));port=l.getsockname()[1]
 args=[str(binary),'serve',str(source),str(port),READ,WRITE]
 if fault:args.append(fault)
 child=subprocess.Popen(args,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);lab.connect(port,child);return child,port
class HookHop(lab.CountHop):
 def __init__(self,port):self.hook=None;self.head_hook=None;self.value_number=0;self.response_values=0;self.cut_second_value=False;self.responses=0;self.partial_bytes=0;super().__init__(port)
 def pump(self,source,target,up):
  try:
   while not self.stop.is_set():
    data=lab.link.frame(source);trigger=None;cut=False
    with self.lock:
     if up:
      self.requests[str(data[4])]+=1
      if data[4]==22:
       self.value_number+=1
       if self.value_number==2 and self.hook:trigger=self.hook;self.hook=None
     else:
      self.statuses[f'{data[4]}:{data[5] if len(data)>5 else 0}:{len(data)}']+=1
      if len(data)>5 and data[4]==0 and data[5]==20 and self.head_hook:trigger=self.head_hook;self.head_hook=None
      if len(data)>5 and data[4]==0 and data[5]==22:
       self.response_values+=1
       if self.cut_second_value and self.response_values==2:cut=True;self.cut_second_value=False
      if cut:data=data[:-1];self.partial_bytes+=len(data)
      else:self.responses+=1
     self.bytes+=len(data)
    if trigger:trigger()
    self.send(target,data)
    if cut:raise EOFError('owned interrupted value fixture')
  except (OSError,EOFError,ValueError):pass
  finally:
   for sock in (source,target):
    try:sock.shutdown(socket.SHUT_RDWR)
    except OSError:pass

def hops(port,path):
 result=[]
 if path=='relay':raw=lab.link.Forwarder(port,'loopback');result.append(raw);port=raw.port
 counted=HookHop(port);result.append(counted);return result,counted

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--baseline-root',type=Path);p.add_argument('--repetitions',type=int,default=1);p.add_argument('--output',type=Path);a=p.parse_args();assert 1<=a.repetitions<=5;root=a.root.resolve();cases=[]
 files=[*root.glob('src/**/*.rs'),root/'Cargo.toml',root/'Cargo.lock',root/'examples/catalogue_horizon_network.rs',root/'examples/catalogue_network.rs',root/'examples/catalogue_apps.rs']
 def hashes():return {str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(files)}
 fingerprint=hashes();subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_apps','--example','catalogue_network','--example','catalogue_horizon_network'],cwd=root,check=True,capture_output=True)
 with tempfile.TemporaryDirectory(prefix='owned-coverage-wire-') as temp:
  folder=Path(temp);baseline=a.baseline_root
  if baseline is None:
   baseline=folder/'old';baseline.mkdir();archive=subprocess.check_output(['git','archive',BASE],cwd=root)
   with tarfile.open(fileobj=io.BytesIO(archive)) as archive:archive.extractall(baseline,filter='data')
  baseline=baseline.resolve()
  if a.baseline_root:
   assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=baseline,text=True).strip()==BASE
   subprocess.run(['git','diff','--exit-code'],cwd=baseline,check=True,capture_output=True)
  baseline_fingerprints={str(p.relative_to(baseline)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([*baseline.glob('src/**/*.rs'),baseline/'Cargo.toml',baseline/'Cargo.lock',baseline/'examples/catalogue_network.rs',baseline/'examples/catalogue_apps.rs'])}
  subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_network','--example','catalogue_apps'],cwd=baseline,check=True,capture_output=True)
  old_local=baseline/'target/debug/examples/catalogue_apps';old=baseline/'target/debug/examples/catalogue_network';network=root/'target/debug/examples/catalogue_network';v2=root/'target/debug/examples/catalogue_horizon_network';local=root/'target/debug/examples/catalogue_apps'
  def producer(source,cache,*args):return lab.run(local,'transcript',source,cache,*args)[0]
  def old_producer(source,cache,*args):return lab.run(old_local,'transcript',source,cache,*args)[0]
  def invoke(source,*args):return lab.run(v2,*args)[0]
  for rep in range(a.repetitions):
   for path in ['direct','relay']:
    f=folder/f'{rep}-{path}';f.mkdir();source=f/'baseline-source';full=f/'baseline-full';partial=f/'baseline-partial';cold=f/'cold';old_producer(source,full,'seed','620');child,port=start(old,source);owned,hop=hops(port,path)
    with sqlite3.connect(source) as db:assert db.execute('PRAGMA user_version').fetchone()==(4,)
    def phase(name,cache,command='pass',limit='40',token=READ,dialect='v2',epoch='epoch-1'):
     before_bytes,before=hop.stats();start_time=time.perf_counter()
     with hop.lock:before_responses=hop.responses;before_partial=hop.partial_bytes
     args=['cap-pass' if dialect=='v1' else command,str(hop.port),str(cache),'phone',token]
     if command=='head':args=['head',str(hop.port),token,epoch]
     if command=='pass':args.append(limit)
     raw=subprocess.run([str(old if dialect=='v1' else v2),*args],cwd=root,text=True,capture_output=True,timeout=30);assert raw.returncode==0,(name,raw.stderr);r=json.loads(raw.stdout);total,after=hop.stats();ops={k:after.get(k,0)-before.get(k,0) for k in set(before)|set(after)};ops={k:v for k,v in ops.items() if v};attempts=sum(ops.values())
     with hop.lock:responses=hop.responses-before_responses;partial_bytes=hop.partial_bytes-before_partial
     r.setdefault('exchanges',responses);r.setdefault('outcome','ok');r.setdefault('work_ms',None);assert r['exchanges']<=attempts and r['exchanges']==responses
     row={'phase':name,'path':path,'repetition':rep+1,**r,'process_ms':(time.perf_counter()-start_time)*1000,'wire_total_bytes':total-before_bytes,'attempted_requests':attempts,'unanswered_requests':attempts-r['exchanges'],'operations':ops,'incomplete_reply_bytes':partial_bytes};assert row['wire_total_bytes']==r['protocol_bytes']+r['manifest_bytes']+r['payload_bytes']+partial_bytes;cases.append(row);return r
    try:
     assert phase('v1_bootstrap',f/'v1-full',dialect='v1')['completed']==620;current_rows_match(source,f/'v1-full')
     assert phase('v1_partial',f/'v1-partial',limit='1',dialect='v1')['active']
     assert phase('v1_restart_resume',f/'v1-partial',dialect='v1')['completed']==620;current_rows_match(source,f/'v1-partial')
     assert phase('v1_unchanged_recheck',f/'v1-full',limit='0',dialect='v1')['completed']==620
     old_producer(source,full,'delete','entry-0000');old_producer(source,full,'upsert','entry-0001','newer-live')
     assert phase('v1_churn',f/'v1-full',dialect='v1')['completed']==622;current_rows_match(source,f/'v1-full')
     assert phase('v1_post_churn_recheck',f/'v1-full',limit='0',dialect='v1')['completed']==622
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
    source=f/'source';full=f/'full';partial=f/'partial';producer(source,full,'seed','620');child,port=start(network,source);owned,hop=hops(port,path)
    with sqlite3.connect(source) as db:assert db.execute('PRAGMA user_version').fetchone()==(6,)
    try:
     with sqlite3.connect(source) as fresh,sqlite3.connect(f/'v1-partial') as previous:
      assert fresh.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries ORDER BY entry_id').fetchall()==previous.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_entries ORDER BY entry_id').fetchall()
     assert phase('bootstrap',full)['completed']==620;current_rows_match(source,full)
     assert phase('old_client_new_server_zero',full,limit='0',dialect='v1')['completed']==620
     assert phase('partial',partial,limit='1')['active']
     # Ordinary future-format fixtures: requirements only, never cutoffs.
     with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET writer_required=?',(((1<<64)-1).to_bytes(8,'big'),))
     assert phase('writer_only_check',full,limit='0')['outcome']=='ok'
     with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET reader_required=?',((1).to_bytes(8,'big'),))
     saved=snapshot(full);assert phase('denied_before_format',full,limit='0',token='wrong')['outcome']=='denied' and snapshot(full)==saved
     assert phase('identity_before_format',full,command='head',epoch='other-epoch')['outcome']=='identity'
     for name,cache in [('resumed',partial),('completed',full)]:
      saved=snapshot(cache);r=phase('reader_refused_'+name,cache,limit='0');assert r['outcome']=='incompatible' and r['payload_bytes']==r['manifest_bytes']==0 and snapshot(cache)==saved
     r=phase('reader_refused_cold',f/'compat-cold',limit='0');assert r['outcome']=='incompatible' and r['payload_bytes']==0
     with sqlite3.connect(f/'compat-cold') as db:assert db.execute('SELECT count(*) FROM catalogue_progress').fetchone()==(0,)
     with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET reader_required=?,writer_required=?',(bytes(8),bytes(8)))
     assert phase('restart_resume',partial)['completed']==620;current_rows_match(source,partial)
     assert phase('unchanged_recheck',full,limit='0')['completed']==620
     producer(source,full,'delete','entry-0000')
     producer(source,full,'upsert','entry-0001','newer-live')
     assert phase('churn',full)['completed']==622;current_rows_match(source,full)
     assert phase('post_churn_recheck',full,limit='0')['completed']==622
     # Restore a separate matching deletion workload while preserving the above
     # ordinary churn evidence; no source metadata/membership is injected.
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
    # Fresh620 workload: explicit equality, interrupted C and actual collection.
    source=f/'coverage-source';full=f/'coverage-full';partial=f/'coverage-partial';producer(source,full,'seed','620');child,port=start(network,source);owned,hop=hops(port,path)
    try:
     assert phase('coverage_seed',full)['completed']==620
     assert phase('coverage_seed_partial',partial)['completed']==620
     invoke(source,'retire',source,'160');activation=invoke(source,'collect',source,'620');assert activation=={'head':780,'horizon':620,'hidden':0}
     assert phase('equal_completed_floor',full)['completed']==780;exact(source,full)
     assert phase('equal_floor_fresh_check',full,limit='0')['completed']==780
     assert phase('interrupted_cutoff',partial,limit='1')['active'];saved=snapshot(partial)
     assert invoke(source,'pin-fixture',source,partial)=={'protected_completed':620,'active_boundary':780,'horizon':620,'manual_refused':True};assert snapshot(partial)==saved
     # Hold a genuine owned head response while another authorized process
     # publishes/observes higher coverage. No recorded response is replayed.
     nested=[]
     def advance_observe():
      activation=invoke(source,'collect-retention',source,'780','10');assert activation=={'head':780,'horizon':780,'hidden':159}
      assert invoke(source,'compact',source,'256')=={'moved':159}
      observed,_=lab.run(v2,'pass',hop.port,partial,'phone',READ,'0');assert observed['outcome']=='expired';nested.append(observed)
     before_bytes,before_ops=hop.stats();hop.head_hook=advance_observe;timer=time.perf_counter();outer,process_ms=lab.run(v2,'pass',hop.port,partial,'phone',READ,'0');assert outer['outcome']=='regressed' and snapshot(partial)==saved
     total,after_ops=hop.stats();ops={k:after_ops.get(k,0)-before_ops.get(k,0) for k in set(after_ops)|set(before_ops)};ops={k:v for k,v in ops.items() if v};assert ops=={'20':2} and len(nested)==1
     protocol=outer['protocol_bytes']+nested[0]['protocol_bytes'];assert total-before_bytes==protocol
     cases.append({'phase':'delayed_lower_head','path':path,'repetition':rep+1,**outer,'exchanges':2,'attempted_requests':2,'unanswered_requests':0,'wire_total_bytes':protocol,'protocol_bytes':protocol,'manifest_bytes':0,'payload_bytes':0,'incomplete_reply_bytes':0,'operations':ops,'process_ms':process_ms,'concurrent_observer':nested[0]})
     with sqlite3.connect(partial) as c:assert c.execute('SELECT horizon FROM catalogue_horizons').fetchone()==(780,)
     for command in ['saved-page','pass']:
      assert phase('expired_'+command,partial,command=command,limit='0')['outcome']=='expired';assert snapshot(partial)==saved
     assert phase('denied_reset',partial,command='reset-start',token='wrong')['outcome']=='denied';assert snapshot(partial)==saved
     assert phase('reset_start',partial,command='reset-start')['outcome']=='ok'
     for _ in range(64):
      assert phase('reset_step',partial,command='reset-step')['outcome']=='ok'
      with sqlite3.connect(partial) as c:state=c.execute('SELECT completed,reset_phase FROM catalogue_progress').fetchone()
      if state==(780,0):break
     else:raise AssertionError('finite reset did not finish')
     exact(source,partial);assert phase('reset_fresh_check',partial,limit='0')['completed']==780
     assert phase('cold_expired',cold,limit='0')['outcome']=='expired'
     with sqlite3.connect(cold) as c:assert c.execute('SELECT count(*) FROM catalogue_progress').fetchone()==(0,)
     assert phase('cold_reset_start',cold,command='reset-start')['outcome']=='ok'
     for _ in range(64):
      assert phase('cold_reset_step',cold,command='reset-step')['outcome']=='ok'
      with sqlite3.connect(cold) as c:state=c.execute('SELECT completed,reset_phase FROM catalogue_progress').fetchone()
      if state==(780,0):break
     else:raise AssertionError('finite cold reset did not finish')
     with sqlite3.connect(cold) as c:
      assert c.execute('SELECT count(*),sum(deleted) FROM catalogue_entries').fetchone()==(461,1)
      assert c.execute('SELECT count(*) FROM catalogue_candidates').fetchone()==(0,)
     assert phase('cold_reset_fresh_check',cold,limit='0')['completed']==780
     assert invoke(source,'assert-fenced',source,'entry-0000')=={'fenced':True}
     # Actual old APIs against positive collection must emit no data and never
     # probe another operation family.
     for command in ['pass','cap-pass']:
      before_bytes,before=hop.stats();saved_old=snapshot(full);old_timer=time.perf_counter()
      with hop.lock:statuses_before=hop.statuses.copy()
      r=subprocess.run([str(old),command,str(hop.port),str(full),'phone',READ,'0'],text=True,capture_output=True,timeout=30);assert r.returncode!=0 and snapshot(full)==saved_old
      _,after=hop.stats();ops={k:after.get(k,0)-before.get(k,0) for k in set(after)|set(before)};assert sum(ops.values())==1 and ops.get('11' if command=='pass' else '17')==1
      with hop.lock:refusals=hop.statuses-statuses_before
      assert dict(refusals)=={'4:0:5':1}
      end_bytes,_=hop.stats();cases.append({'phase':'old_'+command+'_positive_refusal','path':path,'repetition':rep+1,'outcome':'invalid','exchanges':1,'attempted_requests':1,'unanswered_requests':0,'wire_total_bytes':end_bytes-before_bytes,'protocol_bytes':end_bytes-before_bytes,'manifest_bytes':0,'payload_bytes':0,'incomplete_reply_bytes':0,'operations':{k:v for k,v in ops.items() if v},'process_ms':(time.perf_counter()-old_timer)*1000,'work_ms':None})
     with sqlite3.connect(source) as c:assert c.execute('SELECT count(*),sum(deleted),sum(manifest_visible) FROM catalogue_source_entries').fetchone()==(461,1,461)
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
    # The real pinned old server closes unknown kinds. No v1 request is made.
    child,port=start(old,source);owned,hop=hops(port,path)
    try:
     saved=snapshot(partial);r=phase('new_client_old_server',partial,limit='0');assert r['outcome']=='unavailable' and r['exchanges']==0;assert snapshot(partial)==saved;assert cases[-1]['operations']=={'20':1}
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
    # Interrupt a real value frame after another value was fully staged.
    source=f/'interruption-source';cache=f/'interruption-cache';producer(source,cache,'seed','620');child,port=start(network,source);owned,hop=hops(port,path)
    try:
     assert phase('interruption_seed',cache)['completed']==620
     for i in range(2):producer(source,cache,'upsert',f'entry-{i:04}','new-value')
     assert phase('interruption_admit',cache,limit='0')['active'];saved=snapshot(cache);hop.response_values=0;hop.cut_second_value=True
     r=phase('interrupted_payload',cache,command='saved-page');assert r['outcome']=='unavailable' and r['payload_bytes']>0 and snapshot(cache)==saved;assert cases[-1]['incomplete_reply_bytes']>0 and cases[-1]['unanswered_requests']==1
     assert phase('explicit_retry',cache)['completed']==622
     assert phase('retry_fresh_check',cache,limit='0')['completed']==622
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
    # Commit a new cutoff after one ordinary value reply has been staged.
    source=f/'race-source';cache=f/'race-cache';producer(source,cache,'seed','620');child,port=start(network,source);owned,hop=hops(port,path)
    try:
     assert phase('race_seed',cache)['completed']==620
     for i in range(2):producer(source,cache,'upsert',f'entry-{i:04}','new-value')
     invoke(source,'collect',source,'620');assert phase('race_admit',cache,limit='0')['active'];saved=snapshot(cache);hop.value_number=0;hop.hook=lambda:invoke(source,'collect',source,'622')
     r=phase('value_floor_advance',cache,command='saved-page');assert r['outcome']=='expired' and r['payload_bytes']>0 and snapshot(cache)==saved
    finally:
     for h in reversed(owned):h.close()
     child.terminate();child.wait(timeout=5);child.stderr.close()
  assert all(hashlib.sha256((baseline/path).read_bytes()).hexdigest()==digest for path,digest in baseline_fingerprints.items())
 assert hashes()==fingerprint
 summaries=[]
 for path,phase_name in sorted({(r['path'],r['phase']) for r in cases}):
  selected=[r for r in cases if r['path']==path and r['phase']==phase_name];fields=['work_ms','process_ms'];summary={'path':path,'phase':phase_name,'observations':len(selected)}
  for field in fields:
   if all(r[field] is not None for r in selected):summary[field]={'median':statistics.median(r[field] for r in selected),'min':min(r[field] for r in selected),'max':max(r[field] for r in selected)}
  for field in ['exchanges','attempted_requests','unanswered_requests','wire_total_bytes','protocol_bytes','manifest_bytes','payload_bytes']:summary[field]=sum(r[field] for r in selected)
  summaries.append(summary)
 report={'native_source_schema':6,'old_source_schema':4,'registered_pin_fixture':'actual C620/H780 journal pin clamps preferred780 to620; manual collector refuses; explicit protected collection at expiry10 advances780 without deleting ledger','runtime_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'old_server_baseline':BASE,'old_server_source_sha256':baseline_fingerprints,'source_sha256':fingerprint,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'conditions':['real TCP owned plaintext fixture route; direct counted framed hop,relay adds opaque raw hop; no secure production catalogue/session claim','620-entry fixtures; current native API publishes all cutoffs and membership; no raw cutoff/membership injection; ordinary future reader/writer requirements are owned compatibility fixtures','pass reports finite durable completion; explicit unchanged/reset fresh checks are separate','expired operation retains cache/progress/fences; validated passive observations may persist separately','protocol/request bytes counted on attempted exchange even when old peer sends no reply; exchanges count complete replies','unshaped debug timing; no threshold or performance gain claim; value-floor/delayed-head races include producer/observer work; MemoryAuthorizer is an owned always-allowed host fixture; token tests qualify server route refusal only','each cache operation reopens its store; head-only requests have no local cache access; warm recovery matches620 source identities/160 fences; cold recovery has461 served rows/one fence; partial/refused phases preserve their stated snapshot'],'cases':cases,'summary':summaries}
 if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
