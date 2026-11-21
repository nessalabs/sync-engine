#!/usr/bin/env python3
"""Compare one evolved v5 source with v7 migration and physical compaction."""
import json
import os
import sqlite3
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = 'b65f4dee0909b44926c28e1455a5ddc4f52ec4ae'
# Use the actual base's public API, not a synthetic schema or SQL compactor.
FIXTURE = r'''use nessa_sync::replication::{catalogue::CatalogueHorizonSource, domain::{CapabilitySupport,Id,Scope}, infrastructure::SqliteCatalogueSource};
fn id(s:&str)->Id { Id::new(s).unwrap() }
fn main() {
 let p=std::env::args().nth(1).unwrap();
 let mut s=SqliteCatalogueSource::open(p,id("origin"),id("catalogue"),id("v1"),id("schema")).unwrap();
 for i in 0..20000 {s.upsert(&id(&format!("entry-{i:06}")), &[b'x';128]).unwrap();}
 for i in 0..12000 {s.delete(&id(&format!("entry-{i:06}"))).unwrap();}
 let scope=Scope::new(id("peer"),id("origin"),id("catalogue"),id("v1"),id("schema"),id("grant"));
 let h=s.horizon_head(&scope,CapabilitySupport::current()).unwrap();
 assert_eq!(s.compare_collect(&h,32000).unwrap().hidden_markers,11999);
 println!("ready");
 use std::io::Write;
 std::io::stdout().flush().unwrap();
 let mut line=String::new(); std::io::stdin().read_line(&mut line).unwrap();
}
'''
def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)
def snapshot(path):
    with sqlite3.connect(path) as db:
        size=db.execute('pragma page_size').fetchone()[0]
        pages=db.execute('pragma page_count').fetchone()[0]
        free=db.execute('pragma freelist_count').fetchone()[0]
        stats={name:{'pages':n,'allocated_bytes':n*size,'payload_bytes':payload,'cells':cells}
               for name,n,payload,cells in db.execute('select name,count(*),sum(payload),sum(ncell) from dbstat group by name order by name')}
        relevant=['catalogue_source_entries','sqlite_autoindex_catalogue_source_entries_1','catalogue_source_page','catalogue_retired_proof','catalogue_compaction_candidates']
        return {'schema':db.execute('pragma user_version').fetchone()[0], 'page_size':size,
                'allocated_bytes':pages*size,'reusable_bytes':free*size,
                'objects':{name:stats.get(name,{'pages':0,'allocated_bytes':0,'payload_bytes':0,'cells':0}) for name in relevant},
                'relevant_allocated_bytes':sum(stats.get(n,{}).get('allocated_bytes',0) for n in relevant),
                'relevant_payload_bytes':sum(stats.get(n,{}).get('payload_bytes',0) for n in relevant)}
def backup(src,dst):
    with sqlite3.connect(src) as a, sqlite3.connect(dst) as b: a.backup(b)
with tempfile.TemporaryDirectory(prefix='owned-compaction-large-') as folder:
    d=Path(folder); baseline=d/'baseline'; baseline.mkdir()
    archive=subprocess.check_output(['git','archive',BASE],cwd=ROOT)
    import io
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar: tar.extractall(baseline,filter='data')
    (baseline/'examples/compaction_fixture.rs').write_text(FIXTURE)
    with (baseline/'Cargo.toml').open('a') as f: f.write('\n[[example]]\nname="compaction_fixture"\nrequired-features=["sqlite"]\n')
    env=os.environ.copy(); env['CARGO_TARGET_DIR']=str(d/'target-base')
    run('cargo','build','--locked','--features','sqlite','--example','compaction_fixture',cwd=baseline,env=env,stdout=subprocess.DEVNULL)
    run('cargo','build','--locked','--features','sqlite','--example','catalogue_compaction',cwd=ROOT,stdout=subprocess.DEVNULL)
    source=d/'source.db'
    binary=str(ROOT/'target/debug/examples/catalogue_compaction')
    worker=subprocess.Popen([str(d/'target-base/debug/examples/compaction_fixture'),str(source)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert worker.stdout.readline().strip()=='ready' and worker.poll() is None
        with sqlite3.connect(source) as db:
            before=(db.execute('pragma user_version').fetchone(), db.execute('pragma schema_version').fetchone(),
                    db.execute('select count(*),sum(deleted),sum(manifest_visible) from catalogue_source_entries').fetchone(),
                    db.execute('select head,horizon from catalogue_source_meta').fetchone(),
                    db.execute('select type,name,sql from sqlite_master order by type,name').fetchall())
            assert before[:4]==((5,),before[1],(20000,12000,8001),(32000,32000))
        refused=subprocess.run([binary,str(source),'open'],capture_output=True,text=True)
        assert refused.returncode != 0 and 'OfflineUpgradeRequired' in refused.stderr
        with sqlite3.connect(source) as db:
            after=(db.execute('pragma user_version').fetchone(), db.execute('pragma schema_version').fetchone(),
                   db.execute('select count(*),sum(deleted),sum(manifest_visible) from catalogue_source_entries').fetchone(),
                   db.execute('select head,horizon from catalogue_source_meta').fetchone(),
                   db.execute('select type,name,sql from sqlite_master order by type,name').fetchall())
            assert after==before and worker.poll() is None
        worker.stdin.write('stop\n'); worker.stdin.flush()
        assert worker.wait(timeout=30)==0
    finally:
        if worker.poll() is None:
            worker.terminate(); worker.wait(timeout=30)
    control=d/'control.db'; migrated=d/'migrated.db'; compacted=d/'compacted.db'
    for path in [control,migrated,compacted]: backup(source,path)
    run(binary,str(migrated),'migrate',stdout=subprocess.DEVNULL)
    run(binary,str(compacted),'migrate',stdout=subprocess.DEVNULL)
    start=time.perf_counter()
    moved=int(subprocess.check_output([binary,str(compacted),'compact-all'],text=True))
    elapsed=(time.perf_counter()-start)*1000
    assert moved==11999
    run(binary,str(compacted),'open',stdout=subprocess.DEVNULL)
    run(binary,str(compacted),'fenced',stdout=subprocess.DEVNULL)
    with sqlite3.connect(compacted) as db:
        db.execute('attach database ? as original',(str(control),))
        combined="SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries UNION ALL SELECT entry_id,creation,revision,1,x'' FROM catalogue_retired_proof"
        original="SELECT entry_id,creation,revision,deleted,payload FROM original.catalogue_source_entries"
        for left,right in [(original,combined),(combined,original)]:
            assert db.execute(f"SELECT count(*) FROM (SELECT * FROM ({left}) EXCEPT SELECT * FROM ({right}))").fetchone()==(0,)
        assert db.execute('select count(*) from original.catalogue_source_entries').fetchone()==(20000,)
        assert db.execute('select count(*) from catalogue_source_entries').fetchone()==(8001,)
        assert db.execute('select count(*) from catalogue_retired_proof').fetchone()==(11999,)
    print(json.dumps({'base_commit':BASE,'workload':'20000 sequential IDs, 12000 retired, 8000 live 128-byte payloads, head/horizon 32000; 11999 eligible, one equal marker', 'evidence':'actual base API created one v5 source, SQLite backup created matched copies; no VACUUM', 'moved':moved,'compaction_process_ms':round(elapsed,3),'control_v5':snapshot(control),'migrated_v7':snapshot(migrated),'compacted_v7':snapshot(compacted),'exact_bidirectional_values_and_counts':True,'legacy_worker_refused_without_schema_change_then_joined':True,'reopen_and_no_resurrection':True},indent=2))
