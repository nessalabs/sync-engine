#!/usr/bin/env python3
"""Pinned TLC finite reset safety and semantic negative controls; no downloads."""
import argparse,hashlib,json,re,subprocess,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
PIN='936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--jar',type=Path,required=True);p.add_argument('--output',type=Path);a=p.parse_args();jar=a.jar.resolve();assert hashlib.sha256(jar.read_bytes()).hexdigest()==PIN,'use official pinned v1.7.4 artifact'
 model=ROOT/'docs/state/tla/CatalogueReset.tla';config=ROOT/'docs/state/tla/CatalogueReset.cfg';results=[]
 cases=[('pruned',{},None),('all-candidates',{'Prune':'FALSE'},None),('stale-generation',{'UnsafeGeneration':'TRUE'},'NoStaleApply'),('early-completion',{'UnsafeCompletion':'TRUE'},'NoEarlyCompletion'),('absence-is-not-proof',{'UnsafeAbsence':'TRUE'},'FenceProof'),('cancel-journal',{'UnsafeCancel':'TRUE'},'IdleJournal')]
 with tempfile.TemporaryDirectory(prefix='owned-reset-tlc-') as temp:
  for name,changes,broken in cases:
   folder=Path(temp)/name;folder.mkdir();spec=folder/model.name;spec.write_text(model.read_text());cfg=config.read_text()
   for k,v in changes.items():cfg=re.sub(rf'(?m)^{k} = (TRUE|FALSE)$',f'{k} = {v}',cfg)
   cp=folder/config.name;cp.write_text(cfg)
   r=subprocess.run(['java','-XX:+UseParallelGC','-cp',str(jar),'tlc2.TLC','-workers','1','-deadlock','-config',str(cp),'-metadir',str(folder/'states'),str(spec)],cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=120)
   if broken:assert r.returncode!=0 and f'Invariant {broken} is violated' in r.stdout,(name,r.returncode,r.stdout[-3500:])
   else:assert r.returncode==0 and 'No error has been found' in r.stdout,(name,r.returncode,r.stdout[-3500:])
   counts=re.search(r'([\d,]+) states generated, ([\d,]+) distinct states found',r.stdout);assert counts,r.stdout[-2000:]
   results.append({'case':name,'exit_code':r.returncode,'states_generated':int(counts[1].replace(',','')),'distinct_states':int(counts[2].replace(',','')),'expected_broken':broken,'output':r.stdout})
 report={'checker':'official v1.7.4 release, TLC2 version2.19, one worker','jar_sha256':PIN,'vendor_url':'https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar','model_sha256':hashlib.sha256(model.read_bytes()).hexdigest(),'config_sha256':hashlib.sha256(config.read_bytes()).hexdigest(),'scope':'two IDs,three generation advances,one queued reply,one crash,epoch change/authorized cancellation,source deletion/marker omission,atomic page/batch commits; no payload encoding/SQLite/power-loss/horizon policy proof','invariants':['TypeOK','NoStaleApply','NoEarlyCompletion','FenceProof','NoResurrection','DeletedByBoundary','FreshFinish','IdleJournal'],'results':results}
 if a.output:a.output.write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,separators=(',',':')))
if __name__=='__main__':main()
