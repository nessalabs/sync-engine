#!/usr/bin/env python3
"""Owned process-restart publication lab; future bit rows are compatibility fixtures."""
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parent.parent
subprocess.run(['cargo','test','--locked','--no-default-features','--features','sqlite','--test','source_publication'],cwd=ROOT,check=True,capture_output=True)
subprocess.run(['cargo','build','--locked','--no-default-features','--features','sqlite','--example','source_publication'],cwd=ROOT,check=True,capture_output=True)
def run(*args):
 r=subprocess.run([str(ROOT/'target/debug/examples/source_publication'),*map(str,args)],cwd=ROOT,text=True,capture_output=True,check=True)
 return json.loads(r.stdout)
with tempfile.TemporaryDirectory(prefix='owned-publication-process-') as folder:
 source=Path(folder)/'source.db';cache=Path(folder)/'cache.db'
 assert run('seed',source)=={'source_head':7}
 assert run('inspect',source)=={'head':7,'reader':0,'writer':0}
 supported=run('read',source,cache);assert supported=={'typed_incompatible':False,'checkpoint':7,'records':7,'page_reads':1,'payload_bytes':35}
 def future(reader,writer):
  with sqlite3.connect(source) as db:db.execute('UPDATE source_meta SET reader_required=?,writer_required=?',(reader.to_bytes(8,'big'),writer.to_bytes(8,'big')))
 # Owned future-writer fixture; no production API can assign undefined core bits.
 future(0,(1<<64)-1)
 assert run('inspect',source)=={'head':7,'reader':0,'writer':(1<<64)-1}
 writer=run('read',source,cache);assert writer=={'typed_incompatible':False,'checkpoint':7,'records':7,'page_reads':0,'payload_bytes':0}
 future(1,(1<<64)-1)
 refusal=run('read',source,cache);assert refusal=={'typed_incompatible':True,'checkpoint':7,'records':7,'page_reads':0,'payload_bytes':0}
 cold=run('read',source,Path(folder)/'cold-cache.db');assert cold=={'typed_incompatible':True,'checkpoint':0,'records':0,'page_reads':0,'payload_bytes':0}
print(json.dumps({'scope':'native record-source durable publication only','integration_tests':9,'supported':supported,'writer_only':writer,'reopened_refusal':refusal,'cold_refusal':cold,'catalogue_publication':False,'nonzero_publication_enabled':False},separators=(',',':')))
