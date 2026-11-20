"""Ordinary deterministic scheduling contracts for the owned lab's observer."""
import importlib.util
import io
from pathlib import Path
import struct
import threading
import unittest
from collections import Counter
spec=importlib.util.spec_from_file_location('owned_policy_observer',Path(__file__).resolve().parents[1]/'verify-session-freshness.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def frame(kind,status=0,request=2):
    return struct.pack('!I4sHBBQQIQII',44,b'NSR1',1,kind,status,1,request,1,1,0,0)
class Peer:
    def __init__(self,data=b''):self.stream=io.BytesIO(data)
    def recv(self,n):return self.stream.read(n)
    def shutdown(self,*_):pass
class ObserverContracts(unittest.TestCase):
    def observer(self):
        o=module.CountLink.__new__(module.CountLink)
        o.lock=threading.Lock();o.settled=threading.Condition(o.lock);o.stop=threading.Event();o.up_bytes=o.down_bytes=o.auth_bytes=o.forwarding=0
        o.attempts=set();o.completed=set();o.terminal=set();o.terminal_contexts=set();o.terminal_statuses=Counter();o.controls=Counter()
        return o
    def test_fast_terminal_hint_sees_registered_request_before_forward_returns(self):
        o=self.observer();request=frame(1)
        def forward(_target,_data):
            self.assertEqual(len(o.attempts),1)
            self.assertEqual(o.up_bytes,0)
            o._observe(frame(64,1,1),False)
        o.send=forward;o.pump(Peer(request),Peer(),True)
        self.assertEqual(o.up_bytes,len(request));self.assertEqual(len(o.terminal),1);self.assertFalse(o.completed)
    def test_idle_terminal_context_classifies_a_later_request_without_inventing_completion(self):
        o=self.observer();o._observe(frame(64,1,1),False);self.assertFalse(o.terminal)
        o._observe(frame(1),True)
        self.assertEqual(len(o.attempts),1);self.assertEqual(len(o.terminal),1);self.assertFalse(o.completed)
    def test_completed_reply_is_not_reclassified_by_a_later_idle_hint(self):
        o=self.observer();o._observe(frame(5),True);o._observe(frame(5|128,2),False);o._observe(frame(64,1,1),False)
        self.assertEqual(len(o.completed),1);self.assertFalse(o.terminal)
    def test_reply_is_observed_before_client_can_finish_and_snapshot_waits_for_send(self):
        o=self.observer();o._observe(frame(5),True)
        def forward(_target,_data):
            self.assertEqual(len(o.completed),1)
            self.assertEqual(o.forwarding,1)
            self.assertEqual(o.down_bytes,0)
        o.send=forward;o.pump(Peer(frame(5|128,2)),Peer(),False)
        self.assertEqual(o.down_bytes,48);self.assertEqual(o.forwarding,0)
    def test_snapshot_waits_until_admitted_reply_send_and_bytes_finish(self):
        o=self.observer();entered=threading.Event();done=threading.Event()
        class GatedCondition(threading.Condition):
            def wait(self,timeout=None):
                entered.set()
                return super().wait(timeout)
        o.settled=GatedCondition(o.lock)
        def snapshot():
            o.wait_for_forwards()
            self.assertEqual(o.down_bytes,48)
            done.set()
        worker=threading.Thread(target=snapshot)
        def forward(_target,_data):
            worker.start()
            self.assertTrue(entered.wait(5))
            self.assertFalse(done.is_set())
        o.send=forward;o.pump(Peer(frame(5|128,2)),Peer(),False)
        worker.join(5);self.assertFalse(worker.is_alive());self.assertTrue(done.is_set())
    def test_failed_forward_releases_barrier_without_successful_bytes(self):
        o=self.observer()
        def failed(_target,_data):raise OSError('ordinary interrupted send')
        o.send=failed;o.pump(Peer(frame(1)),Peer(),True)
        o.wait_for_forwards()
        self.assertEqual(o.forwarding,0);self.assertEqual(o.up_bytes,0)
        self.assertEqual(len(o.attempts),1);self.assertFalse(o.completed)
