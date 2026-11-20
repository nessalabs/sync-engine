#!/usr/bin/env python3
"""Pinned finite collection/reset safety checks and semantic negative controls."""
import argparse,hashlib,json,re,subprocess,tempfile,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
PIN='936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--jar',type=Path,required=True);p.add_argument('--output',type=Path);a=p.parse_args();jar=a.jar.resolve();assert hashlib.sha256(jar.read_bytes()).hexdigest()==PIN
 model=ROOT/'docs/state/tla/CatalogueCollection.tla';config=ROOT/'docs/state/tla/CatalogueCollection.cfg';results=[]
 cases=[('warm',{},None),('cold',{'Cold':'TRUE'},None),('split-publication',{'UnsafeAtomic':'TRUE'},'MarkersCovered'),('cursor-is-not-completion',{'UnsafeCursor':'TRUE'},'CoveredNotLive'),('delayed-lower',{'UnsafeLower':'TRUE'},'NoLowerHorizon'),('stale-generation',{'UnsafeGeneration':'TRUE'},'NoStaleApply'),('absence-is-not-proof',{'UnsafeAbsence':'TRUE'},'FenceProof'),('unaware-positive-read',{'UnsafeLegacy':'TRUE'},'LegacyBeforeData'),('finish-coverage',{'UnsafeFinish':'TRUE'},'NoExpiredEffects')]
 with tempfile.TemporaryDirectory(prefix='owned-collection-tlc-') as temp:
  for name,changes,broken in cases:
   folder=Path(temp)/name;folder.mkdir();spec=folder/model.name;spec.write_text(model.read_text());cfg=config.read_text()
   for key,value in changes.items():cfg=re.sub(rf'(?m)^{key} = (TRUE|FALSE)$',f'{key} = {value}',cfg)
   # Let the incorrect C/H policy reach completion to demonstrate the actual
   # stale-live consequence, rather than stopping at its earlier effect fence.
   if name=='cursor-is-not-completion':cfg=cfg.replace(' NoExpiredEffects','')
   cp=folder/config.name;cp.write_text(cfg);print('CHECK '+name,file=sys.stderr,flush=True)
   r=subprocess.run(['java','-Xmx2g','-XX:+UseParallelGC','-cp',str(jar),'tlc2.TLC','-workers','1','-deadlock','-config',str(cp),'-metadir',str(folder/'states'),str(spec)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=600)
   if broken:assert r.returncode!=0 and f'Invariant {broken} is violated' in r.stdout,(name,r.returncode,r.stdout[-4000:])
   else:assert r.returncode==0 and 'No error has been found' in r.stdout,(name,r.returncode,r.stdout[-4000:])
   count=re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found',r.stdout);assert count,r.stdout[-2000:]
   results.append({'case':name,'exit_code':r.returncode,'expected_broken':broken,'states_generated':int(count[1].replace(',','')),'distinct_states':int(count[2].replace(',','')),'output':r.stdout})
 report={'checker':'official v1.7.4 artifact / TLC2.19, one worker,2GiB heap','jar_sha256':PIN,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'model_sha256':hashlib.sha256(model.read_bytes()).hexdigest(),'config_sha256':hashlib.sha256(config.read_bytes()).hexdigest(),'scope':'two permanent IDs,one fixed full scope/incarnation,two reset generations,one queued metadata/data reply,one concurrent publisher offer,crash/drop and transient classification failures (stuttering without durable effects),current policy sampled at operation admission; no instantaneous-revocation/SQLite/payload/network/pin-policy proof','invariants':['TypeOK','SourceMonotone','MarkersCovered','FenceProof','NoResurrection','CoveredNotLive','RequiredFenced','NoEarlyCompletion','NoStaleApply','NoLowerHorizon','NoExpiredEffects','LegacyBeforeData'],'results':results}
 if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
