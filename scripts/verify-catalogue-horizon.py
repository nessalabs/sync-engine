#!/usr/bin/env python3
"""Native process/reopen coverage qualification; no network or public collector."""
import argparse,json,pathlib,subprocess,tempfile,sqlite3,hashlib,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repetitions',type=int,default=1);a=p.parse_args()
    if a.repetitions<1:p.error('repetitions must be positive')
    measured=[*ROOT.glob('src/**/*.rs'),ROOT/'examples/catalogue_horizon_lab.rs',ROOT/'scripts/verify-catalogue-horizon.py',ROOT/'Cargo.toml',ROOT/'Cargo.lock']
    def hashes():return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
    fingerprints=hashes()
    subprocess.run(['cargo','build','--locked','--features','sqlite','--example','catalogue_horizon_lab'],cwd=ROOT,check=True)
    binary=ROOT/'target/debug/examples/catalogue_horizon_lab';runs=[]
    for repeat in range(a.repetitions):
        for horizon in [0,620,621]:
            with tempfile.TemporaryDirectory(prefix='owned-catalogue-horizon-') as folder:
                directory=pathlib.Path(folder);phases=[]
                def run(operation):
                    start=time.perf_counter()
                    result=subprocess.run([str(binary),folder,operation],check=True,text=True,capture_output=True)
                    record=json.loads(result.stdout);record['process_elapsed_ms']=(time.perf_counter()-start)*1000;phases.append(record);return record
                run('init');boot=run('bootstrap');assert (boot['completed'],boot['live'],boot['deleted'],boot['active'])==(620,620,0,False)
                run('churn');partial=run('partial');assert partial['completed']==620 and partial['active']
                def state():
                    with sqlite3.connect(directory/'cache.db') as db:
                        return (db.execute('SELECT * FROM catalogue_entries ORDER BY entry_id').fetchall(),db.execute('SELECT * FROM catalogue_progress').fetchall())
                before=state()
                # Fixture-only activation: runtime exposes neither advancement nor collection.
                with sqlite3.connect(directory/'source.db') as db:db.execute('UPDATE catalogue_source_meta SET horizon=?',(horizon,))
                if horizon==621:
                    refused=run('refuse');assert state()==before;assert refused['manifest_reads']==refused['value_reads']==0
                    again=run('inspect');assert state()==before and again['completed']==620 and again['active']
                    with sqlite3.connect(directory/'cache.db') as db:assert db.execute('SELECT horizon FROM catalogue_horizons').fetchone()==(621,)
                else:
                    result=run('follow');assert (result['completed'],result['live'],result['deleted'],result['active'])==(780,460,160,False)
                    result=run('inspect');assert (result['completed'],result['live'],result['deleted'])==(780,460,160)
                storage={}
                for name in ['source','cache']:
                    with sqlite3.connect(directory/f'{name}.db') as db:
                        storage[name]={'file_version':db.execute('PRAGMA user_version').fetchone()[0],'allocated_bytes':db.execute('PRAGMA page_size').fetchone()[0]*db.execute('PRAGMA page_count').fetchone()[0]}
                runs.append({'repeat':repeat,'fixture_horizon':horizon,'phases':phases,'storage':storage})
    assert hashes()==fingerprints
    print(json.dumps({'source_sha256':fingerprints,'contract':'native-v2-development-fixture','collection_enabled':False,'wire_measured':False,'runs':runs},indent=2))
if __name__=='__main__':main()
