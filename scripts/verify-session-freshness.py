#!/usr/bin/env python3
"""Actual policy reads on one owner across replaceable capability-follow channels."""
import argparse
from collections import Counter
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
import threading
import time
ROOT=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('owned_link',ROOT/'scripts/verify-session-e2e.py')
link=importlib.util.module_from_spec(spec);spec.loader.exec_module(link)
BIN=ROOT/'target/debug/examples/session_freshness'
class CountLink(link.Forwarder):
    def __init__(self,*args,**kwargs):
        self.up_bytes=self.down_bytes=0
        self.auth_bytes=0
        self.forwarding=0
        self.attempts=set();self.completed=set();self.terminal=set();self.terminal_contexts=set();self.terminal_statuses=Counter();self.controls=Counter()
        super().__init__(*args,**kwargs)
        self.settled=threading.Condition(self.lock)
    def send(self,sock,data):
        super().send(sock,data)
        if (len(data)==16 and data==b'\x0fdev-read-secret') or len(data)==1:
            with self.lock:self.auth_bytes+=len(data)
    def _observe(self,data,up):
        with self.lock:
            if data[4:8]==b'NSR1':
                kind=data[10];key=(data[12:40],kind if up else kind&127)
                context=data[12:20]+data[28:40]
                if up:
                    self.attempts.add(key)
                    if context in self.terminal_contexts:self.terminal.add(key)
                elif kind==64 and data[11]:
                    self.terminal_contexts.add(context)
                    self.terminal_statuses[str(data[11])]+=1
                    self.terminal.update(k for k in self.attempts-self.completed if k[0][:8]+k[0][16:]==context)
                elif kind&128 and (data[11] or int.from_bytes(data[44:48],'big')+len(data)-48==int.from_bytes(data[40:44],'big')):
                    self.completed.add(key)
                    self.terminal.discard(key)
            elif data[4:8]==b'NSC1':self.controls[('up:' if up else 'down:')+str(data[10])]+=1
    def wait_for_forwards(self):
        with self.settled:
            assert self.settled.wait_for(lambda:self.forwarding==0,timeout=5),'observer forward completion deadline'
    def pump(self,source,target,up):
        try:
            while not self.stop.is_set():
                data=link.frame(source)
                # Correlation is visible before either peer can consume a frame.
                # Snapshot waits for sends to settle; byte counts require success.
                with self.lock:self.forwarding+=1
                try:
                    self._observe(data,up)
                    self.send(target,data)
                    with self.lock:
                        if up:self.up_bytes+=len(data)
                        else:self.down_bytes+=len(data)
                finally:
                    with self.settled:
                        self.forwarding-=1
                        self.settled.notify_all()
        except (OSError,EOFError,ValueError):pass
        finally:
            for s in (source,target):
                try:s.shutdown(link.socket.SHUT_RDWR)
                except OSError:pass

def start(path):
    child=subprocess.Popen([str(BIN),'serve',str(path),'0'],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
    selector=selectors.DefaultSelector();selector.register(child.stderr,selectors.EVENT_READ)
    until=time.monotonic()+10
    try:
        while time.monotonic()<until:
            if selector.select(.1):
                match=re.search(r'127\.0\.0\.1:(\d+)',child.stderr.readline())
                if match:return child,int(match.group(1))
            assert child.poll() is None,'owned fixture stopped'
        raise AssertionError('fixture start deadline')
    finally:selector.close()

def case(folder,policy,path,kbit,rtt,logical,contracts):
    folder.mkdir();child,port=start(folder/'source.db');hops=[]
    try:
        upstream=port
        if path=='relay':
            relay=link.Forwarder(port,'session');hops.append(relay);upstream=relay.port
        shaper=CountLink(upstream,'session',framed=True,kbit=kbit,rtt=rtt);hops.append(shaper)
        run=subprocess.run([str(BIN),'bench',policy,'logical' if logical else 'real','contracts' if contracts else 'stable',f'127.0.0.1:{shaper.port}',f'127.0.0.1:{port}',str(folder/'cache.db'),'owned'],cwd=ROOT,text=True,capture_output=True,timeout=240)
        assert run.returncode==0,run.stderr
        d=json.loads(run.stdout)
        assert d['useful_reads']==6 and d['ordered_complete'] and d['restart_times_unknown']
        assert d['stable_payload_bytes']==0 and d['apply_ms_during_stable']==0
        if policy=='never':assert d['stable_head_attempts']==d['stable_authorizations']==0
        if policy=='always':assert d['stable_head_attempts']==d['stable_authorizations']==6 and d['responses_with_check_pending']==0
        if logical and policy=='within':assert d['stable_head_attempts']==d['stable_authorizations']==2 and d['responses_with_check_pending']==2
        assert d['stable_completed_exchanges']==2*d['stable_head_attempts']
        assert d['cold_apply_ms']>0 and d['total_apply_ms']>=d['cold_apply_ms']
        assert d['reopened_rows']==(17 if contracts else 8)
        reopened=subprocess.run([str(BIN),'inspect',str(folder/'cache.db')],cwd=ROOT,text=True,capture_output=True,check=True,timeout=10)
        assert json.loads(reopened.stdout)=={'rows':d['reopened_rows'],'checkpoint':d['reopened_rows'],'last_applied_unknown':True,'last_head_unknown':True,'dials':0,'ordered_complete':True}
        d['separate_process_cache_and_unknown_times']=True
        shaper.wait_for_forwards()
        with shaper.lock:
            assert shaper.forwarding==0
            attempted=len(shaper.attempts);completed=len(shaper.completed);unanswered=len(shaper.attempts-shaper.completed)
            assert shaper.completed<=shaper.attempts
            assert shaper.auth_bytes==17*shaper.connections
            assert d['completed_client_exchanges']-d['connections']==completed,(d['completed_client_exchanges'],d['connections'],completed)
            assert not (shaper.completed & shaper.terminal)
            assert shaper.terminal<=shaper.attempts
            assert len(shaper.attempts-shaper.completed-shaper.terminal)==1,(attempted,completed,unanswered,shaper.terminal_statuses)
            assert attempted==completed+len(shaper.terminal)+1
            counters={'attempted_data_rpcs':attempted,'completed_data_rpcs':completed,'unanswered_data_rpcs':unanswered,'terminal_hint_aborted_rpcs':len(shaper.terminal),'terminal_hint_statuses':dict(shaper.terminal_statuses),'transport_unanswered_rpcs':len(shaper.attempts-shaper.completed-shaper.terminal),'framed_up_bytes':shaper.up_bytes,'framed_down_bytes':shaper.down_bytes,'framed_total_bytes':shaper.up_bytes+shaper.down_bytes,'fixture_auth_bytes':shaper.auth_bytes,'total_application_bytes':shaper.up_bytes+shaper.down_bytes+shaper.auth_bytes,'physical_connections':shaper.connections,'control_frames':dict(shaper.controls)}
        d.update(path=path,kbit_per_second=kbit,rtt_seconds=rtt,observed=counters)
        return d
    finally:
        for hop in reversed(hops):hop.close()
        child.terminate();child.wait(timeout=5);child.stderr.close()

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--profiles',action='store_true');p.add_argument('--repetitions',type=int,default=1);p.add_argument('--output',type=Path);a=p.parse_args();assert 1<=a.repetitions<=5
    subprocess.run(['cargo','build','--locked','--all-features','--example','session_freshness'],cwd=ROOT,check=True,capture_output=True)
    measured=[*ROOT.glob('src/**/*.rs'),*ROOT.glob('examples/session_freshness/**/*.rs'),ROOT/'examples/session_freshness.rs',ROOT/'scripts/verify-session-freshness.py',ROOT/'scripts/verify-session-e2e.py',ROOT/'Cargo.toml',ROOT/'Cargo.lock']
    def hashes():return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(measured)}
    before=hashes();cases=[]
    with tempfile.TemporaryDirectory(prefix='owned-live-freshness-') as directory:
        base=Path(directory)
        for path in ['direct','relay']:
            for policy in ['never','always','within']:
                print('contract '+path+'/'+policy,file=sys.stderr,flush=True)
                cases.append(case(base/f'contract-{path}-{policy}',policy,path,0,0,True,True))
        for repeat in range(a.repetitions):
            for kbit,rtt in [(0,0)]+([(32,.8),(64,1.5)] if a.profiles else []):
                for path in ['direct','relay']:
                    for policy in ['never','always','within']:
                        print(f'measuring {repeat}/{path}/{policy}/{kbit}',file=sys.stderr,flush=True)
                        cases.append(case(base/f'measure-{repeat}-{path}-{policy}-{kbit}',policy,path,kbit,rtt,False,False))
    assert hashes()==before,'measured source changed'
    report={'scope':'per-scope record freshness owner across actual follow connections; host composition fixture','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'source_sha256':before,'environment':platform.platform(),'repetitions':a.repetitions,'profiles':a.profiles,'conditions':['owned real TCP direct or opaque raw relay; plaintext fixture credentials, not production secure transport','logical contracts advance one persistent injected clock; real measurement clock includes actual network time','matched six stable useful reads;250ms think delays before reads3/6 with200ms TTL; reported read/projection/check timings exclude think delays','same capability-follow path for every policy; source remains seven records during stable reads; cold_ms starts after opening empty cache and source seed, excludes those setup costs','whole-frame interruption before a metadata response; attempted/unanswered data RPCs counted separately from completed replies','framed counters exclude authentication preamble/ACK; total_application_bytes adds their separately observed bytes; both exclude IP/TCP/TLS and producer-control traffic; client counters exclude failed-opening objects, proxy framed totals cover all attempts','current source adapter never dials; only current authorizer lazily connects and delegates actual remote authorization','no latency threshold or extrapolated availability/power guarantee'],'cases':cases}
    if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
