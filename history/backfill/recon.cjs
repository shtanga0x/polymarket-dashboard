// Backward share reconstruction for tracked markets from cached roster activity.
// Usage: node recon.cjs [--sql out.sql]   (reads d_*.json D1 dumps + act/*.json)
const fs=require('fs');
const L=f=>JSON.parse(fs.readFileSync(f));
const markets=L('d_markets.json'),traders=L('d_traders.json'),hold=L('d_holdings.json');
const tidOf=new Map(traders.map(t=>[t.addr,t.id]));
const holdByMid=new Map();for(const h of hold){if(!holdByMid.has(h.mid))holdByMid.set(h.mid,[]);holdByMid.get(h.mid).push(h);}
const NOW=Math.floor(Date.now()/1000);
const LAG=60;       // events newer than first_ts-LAG may not be in the baseline snapshot yet
const NEG_TOL=1;
const TOPPED=Math.floor(Date.now()/1000);    // shares
const rosters={};for(const s of['core','watch'])rosters[s]=new Set(L(`meta_${s}.json`).traders.map(t=>t.address.toLowerCase()));

// Per-trader events, plus per-trader completeness floor (oldest timestamp we hold for sure).
const ev=new Map(),floor=new Map();
// Index once: by conditionId (non-conversion) and conversions by event slug / conditionId.
const byCid=new Map(),convBySlug=new Map(),convByCid=new Map();
const push=(m,k,v)=>{if(!m.has(k))m.set(k,[]);m.get(k).push(v)};
for(const f of fs.readdirSync('act')){const a=f.replace('.json','');const d=L('act/'+f);floor.set(a,d.W);
  for(const e of d.events){if(e[1]==='CONVERSION'){if(e[6])push(convBySlug,e[6],[a,e[0]]);push(convByCid,e[2],[a,e[0]]);}else push(byCid,e[2],[a,e]);}}
const addrOfTid=new Map(traders.map(t=>[t.id,t.addr]));

// Share deltas per outcome for one event on a given market. CONVERSION → null (unknowable per market).
function deltas(e){const [ts,type,cid,oi,side,size]=e;
  if(type==='TRADE')return[[oi,side==='BUY'?size:-size]];
  if(type==='SPLIT')return[[0,size],[1,size]];
  if(type==='MERGE')return[[0,-size],[1,-size]];
  return []; }

const out={rows:[],marketStart:[]};const report=[];const ambiguous=[];const holdFail=[];
for(const m of markets.filter(m=>m.status==='active')){
  const roster=rosters[m.site];
  // baseline per key at first_ts
  const base=new Map(),recorded=new Map();
  for(const h of holdByMid.get(m.id)||[]){const a=addrOfTid.get(h.tid);const k=h.oi+'|'+a;
    if(h.ts===m.first_ts)base.set(k,h.size);else{if(!recorded.has(k))recorded.set(k,[]);recorded.get(k).push([h.ts,h.size]);}}
  // events on this market by roster traders + conversion taint in the same event
  let taint=0;const evByKey=new Map();let redeem=0;
  // Only conversions BEFORE the baseline matter: later ones are live and captured by snapshots.
  for(const [a,t] of [...(convBySlug.get(m.event_slug)||[]),...(convByCid.get(m.cid)||[])])if(roster.has(a)&&t<=m.first_ts)taint=Math.max(taint,t);
  for(const [a,e] of byCid.get(m.cid)||[]){if(!roster.has(a))continue;
    if(e[1]==='REDEEM'){redeem++;continue;}
    for(const [oi,d] of deltas(e)){const k=oi+'|'+a;if(!evByKey.has(k))evByKey.set(k,[]);evByKey.get(k).push([e[0],d]);}
  }
  const W=Math.max(...[...roster].map(a=>floor.get(a)||NOW));
  let start=Math.max(W,taint);
  const keys=new Set([...base.keys(),...evByKey.keys()]);
  // walk each key backward from baseline; record negative-size violations
  const series=new Map();let negAt=0;
  for(const k of keys){
    let size=base.get(k)||0;const evs=(evByKey.get(k)||[]).filter(x=>x[0]<=m.first_ts-LAG).sort((a,b)=>b[0]-a[0]);
    const pts=[];// [ts, sizeAfter]
    // tolerance: 1 share or 0.2% of the key's peak size (fee/rounding drift on big positions)
    let peak=size,tmp=size;for(const [,d] of evs){tmp-=d;peak=Math.max(peak,tmp);}
    const tol=Math.max(NEG_TOL,peak*0.002);
    for(const [ts,d] of evs){pts.push([ts,size]);size-=d;if(size<-tol)negAt=Math.max(negAt,ts);}
    series.set(k,{pts,startSize:size,evs});
  }
  start=Math.max(start,negAt);
  // keys with an event in the ambiguous pre-baseline window (may or may not be in the snapshot)
  for(const [k,list] of evByKey)if(list.some(([t])=>t>m.first_ts-120&&t<=m.first_ts))ambiguous.push([m.id,m.site,k,m.first_ts]);
  // holdout: forward replay from baseline vs recorded live rows (allow 0-150s lag)
  let hTot=0,hOk=0;
  for(const [k,rows] of recorded){const evs=(evByKey.get(k)||[]).filter(x=>x[0]>m.first_ts-LAG);
    for(const [ts,sz] of rows){if(ts>TOPPED-150)continue;hTot++;let ok=false;const cuts=[ts,...evs.map(e=>e[0]).filter(t=>t>=ts-300&&t<=ts)];const c0s=[m.first_ts-LAG,...evs.map(e=>e[0]).filter(t=>t>m.first_ts-LAG&&t<=m.first_ts)];for(const c0 of c0s){for(const c of cuts){let v=base.get(k)||0;for(const [t,d] of evs)if(t>c0&&t<=c)v+=d;if(Math.abs(v-sz)<0.05){ok=true;break}}if(ok)break}if(ok)hOk++;else holdFail.push([m.site,m.title.slice(0,40),k,ts,sz,base.get(k)||0,evs.filter(e=>e[0]<=ts).map(e=>[e[0],+e[1].toFixed(2)])]);}}
  // emit rows from start → first_ts
  let nRows=0;
  if(start<m.first_ts-LAG){
    for(const [k,s] of series){const [oi,a]=k.split('|');const tid=tidOf.get(a);
      // size at `start` = size after last event <= start (pts are descending)
      let sAt=s.startSize;for(const [ts,after] of [...s.pts].reverse()){if(ts<=start)sAt=after;else break;}
      const em=[];if(sAt>0.005)em.push([start,sAt]);
      for(const [ts,after] of [...s.pts].reverse())if(ts>start)em.push([ts,after]);
      for(const [ts,sz] of em){out.rows.push([m.id,+oi,tid??a,ts,Math.max(0,Math.round(sz*100)/100)]);nRows++;}
    }
    out.marketStart.push([m.id,start]);
  }
  report.push({site:m.site,title:m.title.slice(0,50),days:+((m.first_ts-start)/86400).toFixed(1),limit:start===W?'window':start===taint?'conversion':'negative',keys:keys.size,rows:nRows,holdout:hTot?`${hOk}/${hTot}`:'-',redeem});
}
fs.writeFileSync('recon_out.json',JSON.stringify(out));fs.writeFileSync('ambiguous.json',JSON.stringify(ambiguous));fs.writeFileSync('holdfail.json',JSON.stringify(holdFail));console.log('holdout failures',holdFail.length);console.log('ambiguous keys (event within 120s before baseline):',ambiguous.length);fs.writeFileSync('recon_report.json',JSON.stringify(report));
const by=s=>report.filter(r=>r.site===s);
for(const s of['core','watch']){const r=by(s);const d=r.map(x=>x.days).sort((a,b)=>a-b);const q=p=>d[Math.floor(p*(d.length-1))];
  const lim={};r.forEach(x=>lim[x.limit]=(lim[x.limit]||0)+1);
  let ho=[0,0];r.forEach(x=>{if(x.holdout!=='-'){const[a,b]=x.holdout.split('/').map(Number);ho[0]+=a;ho[1]+=b}});
  console.log(`${s}: markets ${r.length}, accurate days min ${q(0)} p25 ${q(.25)} median ${q(.5)} p75 ${q(.75)} max ${q(1)}; limited by ${JSON.stringify(lim)}; rows ${r.reduce((a,x)=>a+x.rows,0)}; holdout ${ho[0]}/${ho[1]}`);}
