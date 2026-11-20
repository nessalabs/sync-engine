#!/usr/bin/env python3
"""Owned real-TCP early incompatibility lab; no deployed channel/publication claim."""
import argparse
from collections import Counter, defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('owned_capability_link',ROOT/'scripts/verify-session-e2e.py')
link=importlib.util.module_from_spec(spec);spec.loader.exec_module(link)

class CountLink(link.Forwarder):
    def __init__(self,*args,**kwargs):
        self.up_bytes=0;self.down_bytes=0;self.statuses=Counter()
        super().__init__(*args,**kwargs)
    def pump(self,source,target,up):
        try:
            while not self.stop.is_set():
                data=link.frame(source)
                with self.lock:
                    if up:
                        self.up_bytes+=len(data)
                        self.requests[f'{data[4:8].decode("ascii")}:{data[10]}']+=1
                    else:
                        self.down_bytes+=len(data)
                        if data[4:8]==b'NSR1':self.statuses[f'{data[10]}:{data[11]}:{len(data)}']+=1
                self.send(target,data)
        except (OSError,EOFError,ValueError,UnicodeError):pass
        finally:
            for sock in (source,target):
                try:sock.shutdown(link.socket.SHUT_RDWR)
                except OSError:pass

def run(binary_args):
    result=subprocess.run([str(link.BIN),*map(str,binary_args)],cwd=ROOT,text=True,capture_output=True,timeout=60)
    assert result.returncode==0,result.stderr
    return json.loads(result.stdout)

def hops(port,path):
    owned=[]
    if path=='relay':
        relay=link.Forwarder(port,'session-caps');owned.append(relay);port=relay.port
    counted=CountLink(port,'session-caps',framed=True);owned.append(counted)
    return owned,counted

def stop(child,owned):
    for hop in reversed(owned):hop.close()
    child.terminate();child.wait(timeout=5);child.stderr.close()

def case(folder,path,reconnect):
    folder.mkdir();source=folder/'source.db';cache=folder/'replica.db'
    if reconnect:
        child,port=link.start_server('session-caps',source,writer=(1<<64)-1);owned,counted=hops(port,path)
        try:
            seed=run(['bench','session-caps',f'127.0.0.1:{counted.port}',source,cache,link.READ])
            assert [p['checkpoint'] for p in seed['phases']]==[7,14,21,21]
        finally:stop(child,owned)
    appended=run(['append',source]);assert appended['head']==(28 if reconnect else 7)
    child,port=link.start_server('session-caps',source,reader=1,writer=(1<<64)-1);owned,counted=hops(port,path)
    try:
        report=run(['refuse',f'127.0.0.1:{counted.port}',cache,link.READ,21 if reconnect else 0])
        inspection=run(['inspect',cache]);assert inspection=={'checkpoint':21 if reconnect else 0,'records':21 if reconnect else 0,'payload_bytes':12378 if reconnect else 0,'ordered_complete':True}
        with counted.lock:
            requests=dict(counted.requests);statuses=dict(counted.statuses)
            observed=counted.up_bytes+counted.down_bytes+counted.connections*(len(link.READ)+2)
        assert report['typed_incompatible'] and report['cache_unchanged'] and report['record_payload_bytes']==0
        assert requests=={'host_auth':1,'NSC1:6':1,'NSR1:1':2,'NSR1:6':1},requests
        assert statuses.get('134:8:57')==1,statuses
        assert not any(k.startswith('134:0:') for k in statuses),statuses
        assert report['exchanges']==sum(requests.values())==5 and report['protocol_bytes']==observed,(report,observed)
        return {'path':path,'phase':'reconnect_refusal' if reconnect else 'cold_refusal','observed_requests':requests,'observed_reply_kind_status_frame_bytes':statuses,'fresh_process_cache_inspection':inspection,**report}
    finally:stop(child,owned)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repetitions',type=int,default=1);p.add_argument('--output',type=Path);args=p.parse_args()
    if not 1<=args.repetitions<=10:p.error('repetitions must be1..10')
    subprocess.run(['cargo','build','--locked','--all-features','--example','session_records'],cwd=ROOT,check=True,capture_output=True)
    tested=subprocess.run(['cargo','test','--locked','--all-features','--test','session_capabilities'],cwd=ROOT,check=True,text=True,capture_output=True)
    files=[*ROOT.glob('src/**/*.rs'),ROOT/'examples/session_records.rs',ROOT/'tests/session_capabilities.rs',ROOT/'scripts/verify-session-e2e.py',Path(__file__).resolve(),ROOT/'Cargo.toml',ROOT/'Cargo.lock']
    def hashes():return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}
    fingerprint=hashes();cases=[]
    with tempfile.TemporaryDirectory(prefix='owned-capability-tcp-') as folder:
        for n in range(args.repetitions):
            for path in ['direct','relay']:
                for reconnect in [False,True]:cases.append(case(Path(folder)/f'{n}-{path}-{reconnect}',path,reconnect))
    assert hashes()==fingerprint
    grouped=defaultdict(list)
    for c in cases:grouped[(c['path'],c['phase'])].append(c)
    summary=[]
    for (path,phase),values in sorted(grouped.items()):
        assert all(v['protocol_bytes']==values[0]['protocol_bytes'] for v in values)
        times=[v['elapsed_ms'] for v in values]
        summary.append({'path':path,'phase':phase,'n':len(values),'exchanges':5,'protocol_bytes':values[0]['protocol_bytes'],'record_payload_bytes':0,'elapsed_ms_min':min(times),'elapsed_ms_median':round(statistics.median(times),3),'elapsed_ms_max':max(times)})
    result={'scope':'version1 record capability admission over real direct/opaque-relay TCP, cold and reconnect refusals','full_issue47_ready':False,'conditions':['owned plaintext loopback authentication fixture; no deployed security qualification','existing same-incarnation writer-only catch-up precedes monotonic reader requirement0→1 on reconnect','unknown required reader bit is refused by status8 before page payload; exact reopened cache unchanged','one-way metadata/error bytes are protocol bytes; TCP/IP/TLS overhead excluded','unshaped debug timings with min/median/max, no performance threshold; weak-link refusal timing unmeasured','source view requirements are fixed for each fixture handle; durable requirement publication is not implemented'],'source_sha256':fingerprint,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'command':f'python3 scripts/verify-session-capabilities.py --repetitions {args.repetitions}','cases':cases,'summary':summary}
    if args.output:args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,separators=(',',':')))
if __name__=='__main__':main()
