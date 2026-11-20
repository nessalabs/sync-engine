#!/usr/bin/env python3
"""Owned native catalogue publication and receiver-restart compatibility lab."""
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
ROOT=Path(__file__).resolve().parent.parent
subprocess.run(['cargo','test','--locked','--all-features','--test','catalogue_publication'],cwd=ROOT,check=True,capture_output=True)
subprocess.run(['cargo','build','--locked','--features','sqlite','--example','catalogue_apps'],cwd=ROOT,check=True,capture_output=True)
def run(source,cache,*args):
 r=subprocess.run([str(ROOT/'target/debug/examples/catalogue_apps'),'transcript',str(source),str(cache),*args],cwd=ROOT,text=True,capture_output=True,check=True);return json.loads(r.stdout)
def snapshot(path):
 with sqlite3.connect(path) as db:return (db.execute('SELECT * FROM catalogue_progress').fetchall(),db.execute('SELECT * FROM catalogue_entries ORDER BY entry_id').fetchall())
with tempfile.TemporaryDirectory(prefix='owned-catalogue-publication-') as folder:
 source=Path(folder)/'source.db';partial=Path(folder)/'partial.db';full=Path(folder)/'full.db'
 assert run(source,partial,'seed','620')=={'seeded':620}
 first=run(source,partial,'cap-pass','1');assert first['count']==40 and first['completed']==0 and first['active_boundary']==620
 saved=snapshot(partial)
 with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET writer_required=?',(((1<<64)-1).to_bytes(8,'big'),))
 writer=run(source,full,'cap-pass','40');assert writer['count']==writer['completed']==620 and writer['active_boundary']==0
 full_saved=snapshot(full)
 with sqlite3.connect(source) as db:db.execute('UPDATE catalogue_source_meta SET reader_required=?',(bytes.fromhex('0000000000000001'),))
 for path,prior in [(partial,saved),(full,full_saved)]:
  result=run(source,path,'cap-pass','40');assert result=={'typed_incompatible':True,'manifest_reads':0,'resolve_reads':0,'payload_bytes':0};assert snapshot(path)==prior
 cold=Path(folder)/'cold.db';assert run(source,cold,'cap-pass','40')['typed_incompatible'];assert snapshot(cold)==([],[])
print(json.dumps({'scope':'native catalogue durable publication; not a session catalogue adapter','entries':620,'first_page_records':40,'active_boundary':620,'writer_only_completed':620,'reopened_cache_and_cursor_unchanged':True,'typed_cold_and_resumed_reader_refusal':True,'integration_tests':13,'nonzero_publication_enabled':False},separators=(',',':')))
