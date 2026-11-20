#!/usr/bin/env python3
"""Real native collection and fresh-process reset; permanent identities remain owned."""
import argparse,hashlib,json,sqlite3,statistics,subprocess,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def rows(c,table):return c.execute(f'SELECT entry_id,creation,revision,deleted,payload FROM {table} ORDER BY entry_id').fetchall()
def snapshot(path):
 with sqlite3.connect(path) as c:return [c.execute(f'SELECT * FROM {table} ORDER BY 1,2,3,4').fetchall() for table in ['catalogue_entries','catalogue_progress','catalogue_candidates']]
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--profile',choices=['legacy-zero','v2-zero','v2-collected','legacy-collected'],default='v2-collected');p.add_argument('--repetitions',type=int,default=1);p.add_argument('--output',type=Path);a=p.parse_args();assert 1<=a.repetitions<=5;root=a.root.resolve()
 files=[*root.glob('src/**/*.rs'),*root.glob('examples/catalogue_reset/**/*.rs'),root/'examples/catalogue_reset.rs',root/'Cargo.toml',root/'Cargo.lock']
 def hashes():return {str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(files)}
 fingerprint=hashes();subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_reset'],cwd=root,check=True);cases=[]
 with tempfile.TemporaryDirectory(prefix='owned-collection-') as directory:
  for n in range(a.repetitions):
   source=Path(directory)/f'source-{n}';cache=Path(directory)/f'cache-{n}';binary=root/'target/debug/examples/catalogue_reset'
   def run(command,check=True):
    start=time.perf_counter();r=subprocess.run([str(binary),command,str(source),str(cache),'owned'],cwd=root,check=check,text=True,capture_output=True,timeout=30);return r,(time.perf_counter()-start)*1000
   prepared,_=run('collection-prepare');assert json.loads(prepared.stdout)=={'prepared':620,'retired':160,'head':780}
   def storage():
    with sqlite3.connect(source) as c:
     result={'allocated_bytes':c.execute('PRAGMA page_count').fetchone()[0]*c.execute('PRAGMA page_size').fetchone()[0], 'freelist_pages':c.execute('PRAGMA freelist_count').fetchone()[0], 'identity_counts':c.execute('SELECT count(*),sum(deleted),sum(manifest_visible),sum(deleted=0) FROM catalogue_source_entries').fetchone()}
     try:result['btree']=c.execute("SELECT name,pagetype,count(*),sum(ncell),sum(payload) FROM dbstat WHERE name IN ('catalogue_source_entries','catalogue_source_page') GROUP BY name,pagetype").fetchall()
     except sqlite3.OperationalError:result['btree']=None
     return result
   before_storage=storage()
   with sqlite3.connect(source) as c:before_rows=rows(c,'catalogue_source_entries')
   horizon=780 if 'collected' in a.profile else 0
   collected,collect_process_ms=run('collection-collect' if horizon else 'collection-zero');receipt=json.loads(collected.stdout)
   assert receipt['head']==780 and receipt['horizon']==horizon and receipt['hidden_markers']==(159 if horizon else 0)
   after_storage=storage();assert tuple(after_storage['identity_counts'])==(620,160,461 if horizon else 620,460)
   with sqlite3.connect(source) as c:assert rows(c,'catalogue_source_entries')==before_rows
   if a.profile=='legacy-collected':
    before=snapshot(cache);r,ms=run('start',check=False);assert r.returncode!=0 and 'HorizonRequired' in r.stderr;assert snapshot(cache)==before
    cases.append({'repetition':n+1,'typed_legacy_refusal':True,'durable_state_unchanged':True,'attempt_process_ms':ms,'source_payload_counters_available':False,'collection_receipt':receipt,'collection_process_ms':collect_process_ms,'storage_before':before_storage,'storage_after':after_storage});continue
   prefix='collection-' if a.profile.startswith('v2-') else '';trace=[];raw,ms=run(prefix+'start');r=json.loads(raw.stdout);r.update(input_phase='Start',process_ms=ms);trace.append(r);generation=r['generation'];assert r['completed']==0 and r['pending']==620
   for _ in range(64):
    previous=r;raw,ms=run(prefix+'step');r=json.loads(raw.stdout);r.update(input_phase=previous['phase'],process_ms=ms);trace.append(r);assert r['generation']==generation and r['pending']<=previous['pending']
    if r['phase'] in ['Enumerate','Reconcile']:assert r['completed']==0
    if r['completed']:assert r['pending']==0 and r['phase'] in ['Finish','Idle']
    if r['done']:break
   else:raise AssertionError('finite native reset failed to finish')
   assert r['completed']==780
   with sqlite3.connect(source) as s,sqlite3.connect(cache) as d:
    expected=rows(s,'catalogue_source_entries');actual=rows(d,'catalogue_entries');assert actual==expected and len(actual)==620 and sum(v[3] for v in actual)==160
    assert d.execute('SELECT completed,active_boundary,reset_phase,reset_boundary FROM catalogue_progress').fetchone()==(780,None,0,None)
    assert d.execute('SELECT count(*) FROM catalogue_candidates').fetchone()==(0,)
    if a.profile.startswith('v2-'):assert d.execute('SELECT horizon FROM catalogue_horizons').fetchone()==(horizon,)
    allocated=d.execute('PRAGMA page_count').fetchone()[0]*d.execute('PRAGMA page_size').fetchone()[0]
   total={k:sum(x[k] for x in trace) for k in ['head_reads','manifest_reads','resolve_reads','manifest_bytes','payload_bytes','authorizations','work_ms','process_ms']};assert total['head_reads']==2 and total['resolve_reads']==620
   assert total['manifest_reads']==(16 if horizon==0 else 12);assert total['manifest_bytes']==(16740 if horizon==0 else 12447)
   measured_effects={k:sum(x[k] for x in trace) if all(k in x for x in trace) else None for k in ['observation_calls','journal_effect_calls','candidate_read_calls']}
   if measured_effects['journal_effect_calls'] is not None:assert measured_effects['journal_effect_calls']==len(trace)
   if a.profile.startswith('v2-'):assert measured_effects['observation_calls']==(634 if horizon else 638)
   cases.append({'repetition':n+1,'collection_receipt':receipt,'collection_process_ms':collect_process_ms,'storage_before':before_storage,'storage_after':after_storage,**total,**measured_effects,'source_read_calls':total['head_reads']+total['manifest_reads']+total['resolve_reads'],'journal_steps':len(trace),'allocated_receiver_bytes':allocated,'source_identity_rows':620,'retired_identity_rows':160,'served_markers':160 if horizon==0 else 1,'exact_reopened_rows_and_progress':True,'trace':trace})
 assert hashes()==fingerprint
 fields=['process_ms','work_ms','source_read_calls','manifest_reads','resolve_reads','manifest_bytes','payload_bytes','journal_steps','allocated_receiver_bytes','observation_calls','journal_effect_calls','candidate_read_calls']
 summary={k:{'median':statistics.median(r[k] for r in cases),'min':min(r[k] for r in cases),'max':max(r[k] for r in cases)} for k in fields if all(k in r and r[k] is not None for r in cases)}
 report={'profile':a.profile,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'source_sha256':fingerprint,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'conditions':['620 prior cached live IDs;160 deletions;H780; real checked native source collection','zero retains all160 markers; positive activation hides159 strictly below780 and serves equal marker780','every start/step a fresh process; exact final source identity/current rows equal reopened receiver values/fences','source/candidate/observation/journal counts are application port calls; no network or disk fsync count claim','reset work_ms excludes open/verification; collection work_ms is only compare_collect; process_ms includes open and report; prepare/inspection excluded','unshaped debug shared-host timings; no speedup or storage-saving claim','native source activation only; no v2 network or retention-pin policy; manifest bytes are logical encoded descriptor sizes, not wire traffic'],'cases':cases,'summary':summary}
 if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
