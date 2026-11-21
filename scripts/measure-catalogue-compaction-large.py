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
    source=d/'source.db'; run(str(d/'target-base/debug/examples/compaction_fixture'),str(source))
    with sqlite3.connect(source) as db:
        assert db.execute('pragma user_version').fetchone()==(5,)
        assert db.execute('select count(*),sum(deleted),sum(manifest_visible) from catalogue_source_entries').fetchone()==(20000,12000,8001)
        assert db.execute('select head,horizon from catalogue_source_meta').fetchone()==(32000,32000)
    control=d/'control.db'; migrated=d/'migrated.db'; compacted=d/'compacted.db'
    for path in [control,migrated,compacted]: backup(source,path)
    binary=str(ROOT/'target/debug/examples/catalogue_compaction')
    run(binary,str(migrated),'open',stdout=subprocess.DEVNULL)
    run(binary,str(compacted),'open',stdout=subprocess.DEVNULL)
    start=time.perf_counter()
    moved=int(subprocess.check_output([binary,str(compacted),'compact-all'],text=True))
    elapsed=(time.perf_counter()-start)*1000
    assert moved==11999
    with sqlite3.connect(compacted) as db:
        db.execute('attach database ? as original',(str(control),))
        missing=db.execute("SELECT count(*) FROM original.catalogue_source_entries o LEFT JOIN (SELECT entry_id,creation,revision,deleted,payload FROM catalogue_source_entries UNION ALL SELECT entry_id,creation,revision,1,x'' FROM catalogue_retired_proof) n ON n.entry_id=o.entry_id AND n.creation=o.creation AND n.revision=o.revision AND n.deleted=o.deleted AND n.payload=o.payload WHERE n.entry_id IS NULL").fetchone()[0]
        assert missing==0
        assert db.execute('select count(*) from catalogue_retired_proof').fetchone()==(11999,)
    print(json.dumps({'base_commit':BASE,'workload':'20000 sequential IDs, 12000 retired, 8000 live 128-byte payloads, head/horizon 32000; 11999 eligible, one equal marker', 'evidence':'actual base API created one v5 source, SQLite backup created matched copies; no VACUUM', 'moved':moved,'compaction_process_ms':round(elapsed,3),'control_v5':snapshot(control),'migrated_v7':snapshot(migrated),'compacted_v7':snapshot(compacted),'all_original_values_match':True},indent=2))
