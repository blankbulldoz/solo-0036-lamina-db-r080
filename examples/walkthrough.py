#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','salvage-conflict']
class Session:
 def __init__(self,binary,root,restore=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def request(self,method,path,data=None):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:return response.status,json.loads(response.read())
  except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200,**policy):return self.call('POST',f'/transactions/{tx}/commit',policy or None,status)
 def rebase(self,tx):return self.call('POST',f'/transactions/{tx}/rebase',{})
 def confirm(self,tx,key,seq,status=200):return self.call('POST',f'/transactions/{tx}/confirm',{'relation':'spectra','key':key,'observed_sequence':seq},status)
 def inspect(self,tx,status=200):return self.call('GET',f'/transactions/{tx}',None,status)
 def vacuum(self,horizon):return self.call('POST','/vacuum',{'horizon':horizon})
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='salvage-conflict':
    s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band','unique':True},201)
    # same-key first-committer-wins with salvage elected on the loser
    a=s.begin();winner=s.begin()
    s.put(a,tuple_(1,amplitude=101));s.put(a,tuple_(8));s.put(winner,tuple_(1,amplitude=202));s.commit(winner)
    head_before_fail=s.stats()['sequence']
    code,failed=s.request('POST',f'/transactions/{a}/commit',{'on_conflict':'salvage'});require(code==409,'salvage conflict not 409')
    require(failed['error']['code']=='write_conflict','same-key failure must be write_conflict')
    report=failed['salvage'];stale=[e for e in report['edits'] if e['key']=='int:1'][0];clean=[e for e in report['edits'] if e['key']=='int:8'][0]
    require(report['state']=='salvaged' and report['retained_edits']==2,'write set not retained')
    require(stale['write_stale'] and not clean['write_stale'] and report['blocked'],'per-edit invalidation basis wrong')
    require(s.stats()['sequence']==head_before_fail,'validation failure consumed a sequence')
    # retained context still pins versions and keeps repeatable reads; no silent refresh
    require(s.query(transaction=a,where=[{'column':'sample','op':'=','value':1}])['rows'][0]['amplitude']==101,'salvaged read silently refreshed')
    s.put(a,tuple_(9),409);s.commit(a,409,on_conflict='salvage')
    rebased=s.rebase(a);require(rebased['state']=='active' and rebased['snapshot']==report['snapshot'],'rebase must keep the read basis, not silently advance it')
    require(rebased['head']==s.stats()['sequence'],'rebase report must still classify against the current head')
    require(rebased['write_stale_edits']==1,'rebase silently cleared a stale flag')
    # the retained read basis is unchanged: the session still sees its own edit,
    # while an explicit current read exposes the committed winner it must inspect
    require(s.query(transaction=a,where=[{'column':'sample','op':'=','value':1}])['rows'][0]['amplitude']==101,'rebase silently refreshed a transactional read')
    require(s.query(where=[{'column':'sample','op':'=','value':1}])['rows'][0]['amplitude']==202,'current read must expose the committed winner for explicit inspection')
    head=s.stats()['sequence']
    s.confirm(a,'int:1',head-1,409)
    confirmed=s.confirm(a,'int:1',head);require(not confirmed['blocked'],'observed confirmation did not clear write-stale edit')
    # a later racing commit makes the next attempt fail closed: salvage is not an infinite retry
    racer=s.begin();s.put(racer,tuple_(1,amplitude=303));s.commit(racer)
    s.commit(a,409,on_conflict='salvage');s.inspect(a,404)
    # unique-value contention is a distinct, non-confirmable failure kind
    u=s.begin();s.put(u,tuple_(9,band='b2'))
    ucode,uerr=s.request('POST',f'/transactions/{u}/commit',{'on_conflict':'salvage'});require(ucode==409,'unique conflict not 409')
    require(uerr['error']['code']=='unique_conflict' and uerr['salvage']['edits'][0]['unique_stale'],'unique contention misclassified')
    s.rebase(u);s.confirm(u,'int:9',s.stats()['sequence'],409)
    s.put(u,tuple_(9,band='b9'));require(not s.inspect(u)['blocked'],'restaged unique edit still blocked');s.commit(u)
    # default fail-closed client semantics: no body or explicit abort discards everything
    old=s.begin();other_tx=s.begin();s.put(old,tuple_(2,amplitude=222));s.put(other_tx,tuple_(2,amplitude=333));s.commit(other_tx)
    acode,aborted=s.request('POST',f'/transactions/{old}/commit',None);require(acode==409,'abort conflict not 409')
    require('salvage' not in aborted,'old client no longer fails closed');s.inspect(old,404)
    # retained edit basis and vacuum share one retention boundary; rollback releases it
    pin=s.begin();rival=s.begin();s.put(pin,tuple_(1,amplitude=777));s.put(rival,tuple_(1,amplitude=888));s.commit(rival)
    pcode,pinfail=s.request('POST',f'/transactions/{pin}/commit',{'on_conflict':'salvage'});require(pcode==409,'pin conflict not 409')
    pin_snap=pinfail['salvage']['snapshot'];pinned=s.vacuum(s.stats()['sequence'])
    require(pinned['pinned'] and pinned['horizon']==pin_snap and s.stats()['retention_pin_sequence']==pin_snap,'salvaged basis did not protect versions')
    s.call('POST',f'/transactions/{pin}/rollback');reclaimed=s.vacuum(s.stats()['sequence'])
    require(not reclaimed['pinned'],'releasing salvage did not free retention')
    # restart closes the in-memory salvage boundary; only durable commits survive
    rtx=s.begin();r2=s.begin();s.put(rtx,tuple_(3,amplitude=444));s.put(r2,tuple_(3,amplitude=555));s.commit(r2);rcode,_=s.request('POST',f'/transactions/{rtx}/commit',{'on_conflict':'salvage'});require(rcode==409,'restart conflict not 409')
    s.stop();s.start()
    require(s.stats()['salvaged_sessions']==0,'salvage context survived a restart');s.inspect(rtx,404)
    require(s.query(where=[{'column':'sample','op':'=','value':3}])['rows'][0]['amplitude']==555,'committed winner lost on restart')
    observations={'conflict_kind':'write_conflict vs unique_conflict','retained':report,'rebase':rebased,'vacuum_pin':pin_snap,'restart':'salvage dropped, durable data kept'}
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
