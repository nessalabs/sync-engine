#!/usr/bin/env python3
"""Owned native reset/reopen lab; no transport or public marker collection."""
import argparse,hashlib,json,re,sqlite3,statistics,subprocess,tempfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--repetitions',type=int,default=1);p.add_argument('--output',type=Path);p.add_argument('--expected-resolve-reads',type=int,choices=[620,1080],default=620);a=p.parse_args();root=a.root.resolve();assert 1<=a.repetitions<=5
 tests=subprocess.run(['cargo','test','--locked','--all-features','--test','catalogue_recovery'],cwd=root,text=True,capture_output=True,check=True);count=int(re.search(r'test result: ok\. (\d+) passed',tests.stdout).group(1));assert count>=8
 subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_reset'],cwd=root,capture_output=True,check=True)
 files=[*root.glob('src/**/*.rs'),root/'examples/catalogue_reset.rs',root/'tests/catalogue_recovery.rs',root/'Cargo.toml',root/'Cargo.lock']
 def hashes():return {str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(files)}
 before=hashes();cases=[]
 with tempfile.TemporaryDirectory(prefix='owned-catalogue-reset-') as directory:
  for n in range(a.repetitions):
   sp=Path(directory)/f'source-{n}';cp=Path(directory)/f'cache-{n}';binary=root/'target/debug/examples/catalogue_reset'
   def run(command):
    t=time.perf_counter();r=subprocess.run([str(binary),command,str(sp),str(cp),'owned'],cwd=root,text=True,capture_output=True,check=True,timeout=30);return json.loads(r.stdout),(time.perf_counter()-t)*1000
   assert run('prepare')[0]=={'prepared':620,'retired':160,'head':780}
   trace=[];r,ms=run('start');r.update(input_phase='Start',process_ms=ms);trace.append(r);g=r['generation'];assert r['completed']==0 and r['pending']==620
   for _ in range(64):
    previous=r;r,ms=run('step');r.update(input_phase=previous['phase'],process_ms=ms);trace.append(r)
    assert r['generation']==g and r['pending']<=previous['pending']
    if r['phase'] in ['Enumerate','Reconcile']:assert r['completed']==0
    if r['completed']:assert r['phase'] in ['Finish','Idle'] and r['pending']==0
    if r['done']:break
   else:raise AssertionError('finite reset did not finish')
   assert r['completed']==780 and r['pending']==0
   with sqlite3.connect(sp) as s,sqlite3.connect(cp) as d:
    expected=s.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries ORDER BY entry_id').fetchall();actual=d.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_entries ORDER BY entry_id').fetchall();assert actual==expected and len(actual)==620 and sum(x[3] for x in actual)==160
    assert d.execute('SELECT completed,active_boundary,reset_phase,reset_boundary FROM catalogue_progress').fetchone()==(780,None,0,None)
    allocated=d.execute('PRAGMA page_count').fetchone()[0]*d.execute('PRAGMA page_size').fetchone()[0];free=d.execute('PRAGMA freelist_count').fetchone()[0]
   sums={k:sum(x[k] for x in trace) for k in ['head_reads','manifest_reads','resolve_reads','manifest_bytes','payload_bytes','authorizations','work_ms','process_ms']}
   assert sums['head_reads']==2 and sums['manifest_reads']==12 and sums['resolve_reads']==a.expected_resolve_reads
   assert sums['manifest_bytes']==12420
   cases.append({'repetition':n+1,**sums,'native_read_operations':sums['head_reads']+sums['manifest_reads']+sums['resolve_reads'],'bounded_commits':len(trace),'allocated_receiver_bytes':allocated,'receiver_freelist_pages':free,'exact_reopened_rows_and_progress':True,'steps':trace})
 assert hashes()==before
 summary={k:{'median':statistics.median(x[k] for x in cases),'min':min(x[k] for x in cases),'max':max(x[k] for x in cases)} for k in ['process_ms','work_ms','native_read_operations','resolve_reads','manifest_bytes','payload_bytes','bounded_commits','allocated_receiver_bytes']}
 report={'scope':'native same-scope recovery through real application/source/SQLite ports; no wire or collection API','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'source_sha256':before,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'tests':count,'expected_resolve_reads':a.expected_resolve_reads,'conditions':['620 cached live IDs;160 source deletions hidden only by an owned future-storage fixture;H780','every start/step is a fresh process, exercising persisted phase/candidates/checkpoint','prepare excluded; process_ms includes open/migrate/configure; work_ms excludes open/configure','native read operations are port calls, not network exchanges; manifest_bytes compact source descriptors and payload_bytes opaque bytes, not wire totals','unshaped debug timings, no timing CI gate, arbitrary power-loss/transport/absence semantics not claimed'],'cases':cases,'summary':summary}
 if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
