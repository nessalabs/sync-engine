#!/usr/bin/env python3
"""Compare identical seven-fact SQLite append processes on two owned checkouts."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import statistics
import subprocess
import tempfile
import time
ROOT=Path(__file__).resolve().parent.parent

def inputs(root):
 files=[*root.glob('src/**/*.rs'),root/'examples/session_records.rs',root/'Cargo.toml',root/'Cargo.lock']
 return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}

def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--before-root',required=True,type=Path);p.add_argument('--repetitions',type=int,default=5);p.add_argument('--output',type=Path)
 args=p.parse_args()
 if not 2<=args.repetitions<=20:p.error('repetitions must be2..20')
 roots={'before':args.before_root.resolve(),'after':ROOT};fingerprints={name:inputs(root) for name,root in roots.items()}
 for root in roots.values():subprocess.run(['cargo','build','--locked','--all-features','--example','session_records'],cwd=root,check=True,capture_output=True)
 cases=[]
 with tempfile.TemporaryDirectory(prefix='owned-publication-bench-') as folder:
  for phase in ['cold_file','existing_file']:
   for n in range(args.repetitions):
    # Alternate order to reduce a consistent first/second filesystem bias.
    for name in (['before','after'] if n%2==0 else ['after','before']):
     source=Path(folder)/f'{phase}-{n}-{name}.db';binary=roots[name]/'target/debug/examples/session_records'
     def append():
      started=time.perf_counter();r=subprocess.run([str(binary),'append',str(source)],text=True,capture_output=True,check=True)
      return round((time.perf_counter()-started)*1000,3),json.loads(r.stdout)
     if phase=='existing_file':assert append()[1]=={'head':7}
     elapsed,result=append();expected=7 if phase=='cold_file' else 14;assert result=={'head':expected}
     with sqlite3.connect(source) as db:
      records,payload=db.execute('SELECT count(*),sum(length(payload)) FROM source_records').fetchone()
      head=db.execute('SELECT head FROM source_meta').fetchone()[0]
      file_version=db.execute('PRAGMA user_version').fetchone()[0]
     assert records==head==expected and payload==4126*(expected//7)
     cases.append({'variant':name,'phase':phase,'repetition':n+1,'elapsed_ms':elapsed,'head':head,'records':records,'payload_bytes':payload,'file_version':file_version})
 assert {name:inputs(root) for name,root in roots.items()}==fingerprints
 summary=[]
 for phase in ['cold_file','existing_file']:
  for name in roots:
   values=[c['elapsed_ms'] for c in cases if c['phase']==phase and c['variant']==name]
   summary.append({'variant':name,'phase':phase,'n':len(values),'elapsed_ms_min':min(values),'elapsed_ms_median':round(statistics.median(values),3),'elapsed_ms_max':max(values)})
 report={'scope':'seven immutable facts, same existing append API and payload, owned native SQLite files','conditions':['debug builds, unshaped local filesystem; timings are evidence without threshold','elapsed includes process start, source open/schema transaction and seven separate WAL/FULL append commits','cold_file uses a new file; existing_file pre-seeds7 facts outside timer then appends7 more in a fresh process','actual before/after file versions are1/2; no future feature bits are published','ordered payload identity is established by the session/native correctness labs; this benchmark checks committed counts/head/byte totals','no power-loss, production rolling upgrade, weak-link append or throughput claim'], 'command':f'python3 scripts/measure-source-publication.py --before-root {args.before_root} --repetitions {args.repetitions}', 'commits':{name:subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip() for name,root in roots.items()},'source_sha256':fingerprints,'cases':cases,'summary':summary}
 if args.output:args.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
