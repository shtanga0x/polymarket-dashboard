// Extend each cached trader file BACKWARD from its window W to now-DAYS.
const fs=require('fs');const A='https://data-api.polymarket.com';const DAYS=+process.argv[2]||180;
const W2=Math.floor(Date.now()/1000)-DAYS*86400;
async function get(u){for(let i=0;i<6;i++){try{const r=await fetch(u,{signal:AbortSignal.timeout(30000)});if(r.ok)return r.json();if(r.status===429||r.status>=500){await new Promise(x=>setTimeout(x,1500*(i+1)));continue}throw new Error(r.status)}catch(e){if(i===5)throw e;await new Promise(x=>setTimeout(x,1500*(i+1)))}}}
async function one(f){const a=f.replace('.json','');const d=JSON.parse(fs.readFileSync('act/'+f));if(d.W<=W2)return;
  const oldest=d.events.reduce((m,e)=>Math.min(m,e[0]),Infinity);
  const key=e=>e.join('|');const have=new Set(d.events.filter(e=>e[0]<=oldest+1).map(key));
  let end=Math.min(oldest+1,d.W+1),n=0;
  if(!Number.isFinite(oldest))end=d.W+1;
  while(true){const page=await get(`${A}/activity?user=${a}&limit=500&end=${end}`);
    for(const x of page){const e=[x.timestamp,x.type,x.conditionId,x.outcomeIndex,x.side,x.size,x.eventSlug];if(x.timestamp<W2)continue;if(!have.has(key(e))){have.add(key(e));d.events.push(e);n++;}}
    if(page.length<500)break;const last=page[page.length-1].timestamp;if(last<W2)break;end=last===end?last-1:last;}
  d.W=W2;fs.writeFileSync('act/'+f,JSON.stringify(d));if(n>20000)console.log(a.slice(0,8),'+',n);}
(async()=>{const files=fs.readdirSync('act');const q=[...files];let done=0;await Promise.all(Array.from({length:6},async()=>{while(q.length){const f=q.shift();try{await one(f)}catch(e){console.log('FAIL',f,e.message)}if(++done%40===0)console.log(done,'/',files.length,new Date().toISOString().slice(11,19))}}));console.log('extended to',DAYS,'days at',new Date().toISOString())})();
