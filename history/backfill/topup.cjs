// Extend each cached trader file forward to now (page back until the cached newest second, overlap-deduped).
const fs=require('fs');const A='https://data-api.polymarket.com';
async function get(u){for(let i=0;i<6;i++){try{const r=await fetch(u,{signal:AbortSignal.timeout(30000)});if(r.ok)return r.json();if(r.status===429||r.status>=500){await new Promise(x=>setTimeout(x,1500*(i+1)));continue}throw new Error(r.status)}catch(e){if(i===5)throw e;await new Promise(x=>setTimeout(x,1500*(i+1)))}}}
const files=fs.readdirSync('act');let added=0;
async function one(f){const a=f.replace('.json','');const d=JSON.parse(fs.readFileSync('act/'+f));
  const newest=d.events.reduce((m,e)=>Math.max(m,e[0]),0);
  const key=e=>e.join('|');const have=new Set(d.events.filter(e=>e[0]>=newest).map(key));
  let end=Math.floor(Date.now()/1000);const fresh=[];
  while(true){const page=await get(`${A}/activity?user=${a}&limit=500&end=${end}`);
    for(const x of page){const e=[x.timestamp,x.type,x.conditionId,x.outcomeIndex,x.side,x.size,x.eventSlug];if(x.timestamp<newest)continue;if(!have.has(key(e))){have.add(key(e));fresh.push(e);}}
    if(page.length<500||page[page.length-1].timestamp<newest)break;end=page[page.length-1].timestamp;}
  d.events.push(...fresh);d.topped=Math.floor(Date.now()/1000);fs.writeFileSync('act/'+f,JSON.stringify(d));added+=fresh.length;}
(async()=>{const q=[...files];await Promise.all(Array.from({length:6},async()=>{while(q.length){const f=q.shift();try{await one(f)}catch(e){console.log('FAIL',f,e.message)}}}));console.log('topped',files.length,'added',added,'at',new Date().toISOString())})();
