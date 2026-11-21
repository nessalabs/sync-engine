#!/usr/bin/env python3
"""Owned 620-ID SQLite membership migration costs; no marker collection."""
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

def fixture(path,version):
    with sqlite3.connect(path) as db:
        db.executescript('''PRAGMA application_id=1314083651;
CREATE TABLE catalogue_source_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),origin TEXT NOT NULL,stream TEXT NOT NULL,incarnation TEXT NOT NULL,schema_id TEXT NOT NULL,head INTEGER NOT NULL CHECK(head>=0));
CREATE TABLE catalogue_source_entries(entry_id TEXT PRIMARY KEY,creation INTEGER NOT NULL CHECK(creation>0),revision INTEGER NOT NULL CHECK(revision>=creation),deleted INTEGER NOT NULL CHECK(deleted IN(0,1)),payload BLOB NOT NULL);
CREATE INDEX catalogue_source_page ON catalogue_source_entries(creation,entry_id);
INSERT INTO catalogue_source_meta VALUES(1,'gateway','transcript-index','first','opaque-v1',780);
''')
        db.executemany('INSERT INTO catalogue_source_entries VALUES(?,?,?,?,?)',[(f'entry-{i:04}',i+1,621+i if i<160 else i+1,int(i<160),b'' if i<160 else f'session-{i:04}:hello'.encode()) for i in range(620)])
        if version==2:
            db.executescript("""ALTER TABLE catalogue_source_meta ADD COLUMN metadata_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE catalogue_source_meta ADD COLUMN reader_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(reader_required)='blob' AND length(reader_required)=8);
ALTER TABLE catalogue_source_meta ADD COLUMN writer_required BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(typeof(writer_required)='blob' AND length(writer_required)=8);""")
        db.execute(f'PRAGMA user_version={version}')

def snapshot(path):
    with sqlite3.connect(path) as db:
        rows=db.execute('SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries ORDER BY creation,entry_id').fetchall()
        version=db.execute('PRAGMA user_version').fetchone()[0]
        assert version in (1,2,3,4,5,6)
        tables=[x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        expected=['catalogue_source_entries','catalogue_source_meta']
        if version>=5:
            expected.insert(0,'catalogue_retention_pins')
        if version>=6:
            expected.insert(0,'catalogue_retired_proof')
        assert tables==sorted(expected),(version,tables)
        if version>=5:
            columns=[column[1] for column in db.execute("PRAGMA table_info(catalogue_retention_pins)")]
            assert columns==['holder','receiver','access_epoch','revision','completed','expires_at_ms']
            assert db.execute('SELECT count(*) FROM catalogue_retention_pins').fetchone()==(0,)
            assert db.execute('SELECT retention_now_ms FROM catalogue_source_meta').fetchone()==(0,)
        if version>=4:
            assert db.execute('SELECT horizon FROM catalogue_source_meta').fetchone()==(0,)
        count=db.execute('SELECT count(*) FROM catalogue_source_entries').fetchone()[0]
        assert count==620
        head=db.execute('SELECT head FROM catalogue_source_meta').fetchone()[0];assert head==780
        index_sql=db.execute("SELECT sql FROM sqlite_master WHERE name='catalogue_source_page'").fetchone()[0]
        result={'file_version':version,'page_size':db.execute('PRAGMA page_size').fetchone()[0],'page_count':db.execute('PRAGMA page_count').fetchone()[0],'freelist_pages':db.execute('PRAGMA freelist_count').fetchone()[0],'entries':count,'retired_ids':sum(r[3] for r in rows),'head':head,'index_sql':index_sql}
        result['database_allocated_bytes']=result['page_size']*result['page_count']
        # dbstat is optional in SQLite builds: record absence instead of inventing a size.
        try:
            result['serving_index_pages'],result['serving_index_bytes'],result['serving_index_cells']=db.execute("SELECT count(*),sum(pgsize),sum(ncell) FROM dbstat WHERE name='catalogue_source_page'").fetchone()
        except sqlite3.OperationalError:result['serving_index_pages']=result['serving_index_bytes']=result['serving_index_cells']=None
        visible='manifest_visible=1' if version>=3 else '1'
        assert db.execute(f'SELECT count(*) FROM catalogue_source_entries WHERE {visible}').fetchone()[0]==620
        result['manifest_members']=620
        return rows,result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--repetitions',type=int,default=5);p.add_argument('--output',type=Path);a=p.parse_args();root=a.root.resolve()
    if not 1<=a.repetitions<=10:p.error('1..10 repetitions required')
    measured=[*root.glob('src/**/*.rs'),root/'Cargo.toml',root/'Cargo.lock',root/'examples/catalogue_apps.rs']
    def hashes():return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
    before=hashes();subprocess.run(['cargo','build','--locked','--all-features','--example','catalogue_apps'],cwd=root,check=True,capture_output=True)
    cases=[];fresh=[]
    with tempfile.TemporaryDirectory(prefix='owned-catalogue-retirement-') as folder:
        for version in [1,2]:
            for n in range(a.repetitions):
                path=Path(folder)/f'v{version}-{n}.db';fixture(path,version);rows,prior=snapshot(path)
                for phase in ['first_open','reopen']:
                    start=time.perf_counter();r=subprocess.run([str(root/'target/debug/examples/catalogue_apps'),'transcript',str(path),str(Path(folder)/'unused.db'),'seed','0'],text=True,capture_output=True,check=True,timeout=30);elapsed=(time.perf_counter()-start)*1000
                    assert json.loads(r.stdout)=={'seeded':0}
                    actual,after=snapshot(path);assert actual==rows
                    assert after['file_version'] in (2,3,4,5,6)
                    if phase=='reopen':assert after['file_version']==prior['file_version']
                    cases.append({'input_version':version,'repetition':n+1,'phase':phase,'elapsed_ms':round(elapsed,6),'before':prior,'after':after,'exact_identity_revision_deletion_payload_preserved':True})
                    prior=after
        for n in range(a.repetitions):
            path=Path(folder)/f'fresh-{n}.db'
            binary=root/'target/debug/examples/catalogue_apps'
            start=time.perf_counter()
            for command in [['seed','620']]+[['delete',f'entry-{i:04}'] for i in range(160)]:
                subprocess.run([str(binary),'transcript',str(path),str(Path(folder)/'unused.db'),*command],check=True,text=True,capture_output=True,timeout=30)
            elapsed=(time.perf_counter()-start)*1000
            rows,state=snapshot(path)
            expected=[(f'entry-{i:04}',i+1,621+i if i<160 else i+1,int(i<160),b'' if i<160 else f'session-{i:04}:hello'.encode()) for i in range(620)]
            assert rows==expected
            fresh.append({'repetition':n+1,'publication_processes_ms':round(elapsed,6),'storage':state,'exact_identity_revision_deletion_payload_preserved':True})
    assert hashes()==before
    summaries=[]
    for v in [1,2]:
        for phase in ['first_open','reopen']:
            cs=[c for c in cases if c['input_version']==v and c['phase']==phase];ms=[c['elapsed_ms'] for c in cs]
            summaries.append({'input_version':v,'phase':phase,'n':len(ms),'elapsed_ms_min':min(ms),'elapsed_ms_median':statistics.median(ms),'elapsed_ms_max':max(ms),'before':cs[0]['before'],'after':cs[0]['after']})
    report={'scope':'source membership storage prerequisite; all620 IDs remain in manifests,160 retired; no horizon or collection','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'source_sha256':before,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'conditions':['owned legacy v1/v2 databases;620 IDs,460 live/160 retired,head780','timing includes fresh debug process start,source identity validation,migration/open and WAL/FULL configuration; excludes fixture creation and inspection','first_open includes migration when needed; reopen uses another process; no local source data change','fresh_publication creates620 then deletes160 through public example operations; timing includes161 processes and all commits; separate from migration timer','SQLite dbstat index size is optional and null when unavailable; no hidden markers or storage saving is claimed','unshaped local disk/CPU, no timing CI threshold'],'cases':cases,'summary':summaries,'fresh_publication':fresh}
    if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
