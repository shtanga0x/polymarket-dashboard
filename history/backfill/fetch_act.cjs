// Page each roster trader's full activity back DAYS days via the end= cursor; cache per trader.
const fs=require('fs');const A='https://data-api.polymarket.com';
const DAYS=+process.argv[2]||90;const W=Math.floor(Date.now()/1000)-DAYS*86400;
const roster=new Set();for(const s of ['core','watch'])for(const t of JSON.parse(fs.readFileSync(`meta_${s}.json`)).traders)roster.add(t.address.toLowerCase());
const list=[...roster];let done=0,req=0;
async function get(u){for(let i=0;i<5;i++){try{const r=await fetch(u,{signal:AbortSignal.timeout(30000)});req++;if(r.ok)return r.json();if(r.status===429||r.status>=500){await new Promise(x=>setTimeout(x,2000*(i+1)));continue}throw new Error(r.status+' '+(await r.text()).slice(0,100))}catch(e){if(i===4)throw e;await new Promise(x=>setTimeout(x,2000*(i+1)))}}}
async function one(a){
  const f=`act/${a}.json`;if(fs.existsSync(f))return;
  let end=Math.floor(Date.now()/1000),all=[],seen=new Set();
  while(true){const d=await get(`${A}/activity?user=${a}&limit=500&end=${end}`);
    for(const x of d){const k=x.transactionHash+'|'+x.conditionId+'|'+x.type+'|'+x.outcomeIndex+'|'+x.size+'|'+x.timestamp;if(!seen.has(k)){seen.add(k);all.push([x.timestamp,x.type,x.conditionId,x.outcomeIndex,x.side,x.size,x.eventSlug])}}
    if(all.length%25000<500)console.log(a.slice(0,8),'events',all.length,'oldest',new Date(d[d.length-1].timestamp*1000).toISOString().slice(0,10));
    if(d.length<500)break;const last=d[d.length-1].timestamp;if(last<W)break;
    // step to last-1 is unsafe if >500 events share one second; re-query that second with end=last first.
    end=last===end?last-1:last;}
  fs.writeFileSync(f,JSON.stringify({W,events:all.filter(x=>x[0]>=W)}));console.log(a.slice(0,8),'saved',all.length);
}
(async()=>{const q=[...list];await Promise.all(Array.from({length:6},async()=>{while(q.length){const a=q.shift();try{await one(a)}catch(e){console.log('FAIL',a,e.message)}done++;if(done%20===0)console.log(done,'/',list.length,'req',req)}}));console.log('done',list.length,'req',req)})();
