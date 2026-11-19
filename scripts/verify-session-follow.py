#!/usr/bin/env python3
"""Real persistent-hint TCP/SQLite fixture, with explicit polling comparison."""
import argparse
from collections import defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import re
import selectors
import statistics
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('owned_session_link',ROOT/'scripts/verify-session-e2e.py')
link=importlib.util.module_from_spec(spec)
spec.loader.exec_module(link)
BIN=ROOT/'target/debug/examples/session_follow'

class FollowLink(link.Forwarder):
    def __init__(self,*args,drop=False,**kwargs):
        self.drop=drop
        self.dropped=0
        self.up_bytes=0
        self.down_bytes=0
        super().__init__(*args,**kwargs)
    def pump(self,source,target,up):
        try:
            while not self.stop.is_set():
                data=link.frame(source)
                hint=data[4:8]==b'NSR1' and data[10]==64 and data[11]==0
                if not up and hint and len(data)==56 and int.from_bytes(data[48:56],'big')==28 and self.drop:
                    with self.lock:self.dropped+=1
                    self.drop=False
                    continue
                with self.lock:
                    if up:
                        self.up_bytes+=len(data)
                        self.requests[f'{data[4:8].decode("ascii")}:{data[10]}']+=1
                    else:self.down_bytes+=len(data)
                self.send(target,data)
        except (OSError,EOFError,ValueError,UnicodeError):
            pass
        finally:
            for sock in (source,target):
                try:sock.shutdown(link.socket.SHUT_RDWR)
                except OSError:pass

def start(path):
    child=subprocess.Popen([str(BIN),'serve',str(path),'0'],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
    selector=selectors.DefaultSelector();selector.register(child.stderr,selectors.EVENT_READ)
    until=time.monotonic()+10
    try:
        while time.monotonic()<until:
            if selector.select(.1):
                line=child.stderr.readline();found=re.search(r'127\.0\.0\.1:(\d+)',line)
                if found:return child,int(found.group(1))
            assert child.poll() is None,'follow fixture exited'
        raise AssertionError('follow fixture start timeout')
    except BaseException:
        child.terminate();child.wait(timeout=5);raise
    finally:selector.close()

def case(base,mode,path,kbit,rtt,cadence):
    base.mkdir();child,port=start(base/'source.db');hops=[]
    try:
        target=port
        if path=='relay':
            relay=link.Forwarder(port,'session');hops.append(relay);target=relay.port
        shaper=FollowLink(target,'session',framed=True,kbit=kbit,rtt=rtt,drop=mode=='push');hops.append(shaper)
        process=subprocess.run([str(BIN),'bench',mode,f'127.0.0.1:{shaper.port}',f'127.0.0.1:{port}',str(base/'cache.db'),str(cadence)],cwd=ROOT,text=True,capture_output=True,timeout=240)
        assert process.returncode==0,process.stderr
        report=json.loads(process.stdout);assert report['mode']==mode
        phases=report['phases'];assert len(phases)==6,phases
        for p,target,payload in zip(phases,[7,14,21,21,28,30],[4126,4126,4126,0,4126,9],strict=True):
            assert p['records']==p['checkpoint']==target and p['ordered_complete'],p
            assert p['payload_bytes']==payload and p['head_checks']==2,p
            assert p['total_bytes']==p['protocol_bytes']+payload,p
            if p['phase']=='warm_update':
                assert p['wake']==('push' if mode=='push' else 'explicit_poll'),p
                assert p['hint_frames']==(1 if mode=='push' else 0),p
                assert (p['notification_to_durable_ms'] is not None)==(mode=='push'),p
            if p['phase'] in ['idle_fallback','lost_hint_fallback']:
                assert p['wake']==('fallback' if mode=='push' else 'explicit_poll'),p
                assert p['notification_to_durable_ms'] is None,p
            if p['phase']=='coalesced_updates' and mode=='push':
                assert p['hint_frames']==2 and p['coalesced']>=1,p
        inspected=subprocess.run([str(BIN),'inspect',str(base/'cache.db')],cwd=ROOT,text=True,capture_output=True,check=True)
        assert json.loads(inspected.stdout)=={'checkpoint':30,'records':30,'payload_bytes':16513,'ordered_complete':True}
        with shaper.lock:
            requests=dict(shaper.requests);connections=shaper.connections;dropped=shaper.dropped
            delivered=shaper.up_bytes+shaper.down_bytes+connections*17
        assert sum(requests.values())==sum(p['exchanges'] for p in phases),(requests,phases)
        assert connections==2 and dropped==(1 if mode=='push' else 0),(connections,dropped)
        assert delivered==sum(p['total_bytes'] for p in phases),(delivered,phases)
        return {'mode':mode,'path':path,'kbit_per_second':kbit,'rtt_seconds':rtt,'connections':connections,'dropped_hint_frames':dropped,'observed_requests':requests,'observed_delivered_bytes':delivered,'fresh_process_cache_inspection':True,'phases':phases}
    finally:
        for hop in reversed(hops):hop.close()
        child.terminate();child.wait(timeout=5);child.stderr.close()

def summary(cases):
    groups=defaultdict(list)
    for run in cases:
        short=run['phases'][:3]
        aggregate={key:sum(p[key] for p in short) for key in ['elapsed_ms','exchanges','protocol_bytes','payload_bytes','total_bytes','hint_frames','coalesced']}
        aggregate.update(phase='short_session',commit_to_durable_ms=None,notification_to_durable_ms=None,wake_to_durable_ms=None)
        for p in [*run['phases'],aggregate]:groups[(run['mode'],run['path'],run['kbit_per_second'],run['rtt_seconds'],p['phase'])].append(p)
    rows=[]
    for (mode,path,kbit,rtt,phase),samples in sorted(groups.items()):
        row={'mode':mode,'path':path,'kbit_per_second':kbit,'rtt_seconds':rtt,'phase':phase,'samples':len(samples)}
        for key in ['exchanges','protocol_bytes','payload_bytes','total_bytes','hint_frames']:
            assert all(p[key]==samples[0][key] for p in samples),(key,samples)
            row[key]=samples[0][key]
        for key in ['elapsed_ms','commit_to_durable_ms','notification_to_durable_ms','wake_to_durable_ms']:
            values=[p[key] for p in samples if p[key] is not None]
            row[key]={'n':len(values),'min':min(values),'median':round(statistics.median(values),3),'max':max(values)} if values else None
        rows.append(row)
    return rows

def markdown(report):
    rows={(r['path'],r['kbit_per_second'],r['rtt_seconds'],r['phase'],r['mode']):r for r in report['summary']}
    lines=['Polling and push use the same 2s cadence/fallback and authorized read path. Short sums cold/warm/reconnect per run, excluding idle, producer control, close cleanup and verification. Notification timing is source hint-write start to actual SQLite apply; polling has no notification metric. n is per-mode samples; timings are evidence, not CI thresholds.','','| Path/profile | Phase | Poll: exchanges; protocol/total B; elapsed ms median [min,max] | Push: exchanges; protocol/total B; elapsed ms median [min,max] | Poll/Push commit→durable ms | Push notification→durable ms | n |','| --- | --- | --- | --- | --- | --- | ---: |']
    def timing(value):return 'unmeasured' if value is None else f"{value['median']} [{value['min']},{value['max']}]"
    for path,kbit,rtt,phase in sorted({key[:4] for key in rows}):
        left,right=[rows[(path,kbit,rtt,phase,m)] for m in ['poll','push']]
        profile=f'{kbit} kbit/s,{rtt}s RTT' if kbit else 'unshaped'
        def cell(r):return f"{r['exchanges']}; {r['protocol_bytes']}/{r['total_bytes']}; {timing(r['elapsed_ms'])}"
        lines.append(f"| {path}/{profile} | {phase} | {cell(left)} | {cell(right)} | {timing(left['commit_to_durable_ms'])} / {timing(right['commit_to_durable_ms'])} | {timing(right['notification_to_durable_ms'])} | {right['samples']} |")
    return '\n'.join(lines)+'\n'

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--profiles',action='store_true');parser.add_argument('--repetitions',type=int,default=1);parser.add_argument('--output',type=Path);parser.add_argument('--markdown',type=Path);args=parser.parse_args()
    if not 1<=args.repetitions<=10:parser.error('repetitions must be1..10')
    subprocess.run(['cargo','build','--locked','--all-features','--example','session_follow'],cwd=ROOT,check=True,capture_output=True)
    tested=subprocess.run(['cargo','test','--locked','--all-features','--test','session_follow'],cwd=ROOT,check=True,text=True,capture_output=True)
    count=re.search(r'test result: ok\. (\d+) passed',tested.stdout);assert count,tested.stdout
    measured=[*ROOT.glob('src/**/*.rs'),ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'examples/session_follow.rs',Path(__file__).resolve(),ROOT/'scripts/verify-session-e2e.py']
    def fingerprints():return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
    hashes=fingerprints();cases=[]
    with tempfile.TemporaryDirectory(prefix='owned-follow-lab-') as folder:
        for repeat in range(args.repetitions):
            for kbit,rtt in [(0,0)]+([(32,.8),(64,1.5)] if args.profiles else []):
                for path in ['direct','relay']:
                    for mode in ['poll','push']:
                        name=f'{repeat}-{kbit}-{path}-{mode}';print('measuring '+name,file=sys.stderr,flush=True)
                        run=case(Path(folder)/name,mode,path,kbit,rtt,2000);run['repetition']=repeat+1;cases.append(run)
    assert hashes==fingerprints(),'measured source changed'
    report={'scope':'one-scope persistent advisory hints and authorized follow; #54 remains incomplete','completion_ready':False,'contract_tests_passed':int(count.group(1)),'source_sha256':hashes,'base_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'environment':platform.platform(),'command':'python3 scripts/verify-session-follow.py'+(' --profiles' if args.profiles else '')+f' --repetitions {args.repetitions}','cadence_ms':2000,'conditions':['real TCP/direct or opaque raw-TCP relay, actual production follow adapter, reopened SQLite caches and new-process inspection','plaintext loopback fixture authentication; no deployed encrypted direct/relay trust qualification','producer commits occur on source via separate unshaped fixture-control connection; control bytes/time excluded from data-plane phase counters','notification-to-durable uses source wall-clock hint write start and receiver actual apply end on same host; not a distributed-clock guarantee','phase timer ends after post-pass head recheck; durable latency stops earlier at actual apply','coalesced_updates gates first hint reception before second commit without consuming; timing is a gated correctness episode, not throughput','one head28 hint deliberately dropped for push; same2s fallback deadline as explicit polling;16 repeated source wake signals deduplicated','no packet/IP/TCP/TLS overhead, radio/power, shared duplex contention or general packet loss/outage model','min/median/max repeated observations; no timing performance CI threshold'],'remaining':['production secure channels and capability/status/head metadata','history/catalogue/artifact routes and large/multistream resume','multiplexed priority/credit/cancel versus commit','controlled outages/loss and urgent-under-bulk qualification'],'cases':cases,'summary':summary(cases)}
    if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    if args.markdown:args.markdown.write_text(markdown(report))
    print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
