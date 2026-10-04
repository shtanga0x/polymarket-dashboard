// recon_out.json → backfill.sql (holdings rows strictly before each market's first_ts + new first_ts)
const fs=require('fs');const o=JSON.parse(fs.readFileSync('recon_out.json'));
const markets=new Map(JSON.parse(fs.readFileSync('d_markets.json')).map(m=>[m.id,m]));
const traders=JSON.parse(fs.readFileSync('d_traders.json'));const tidOf=new Map(traders.map(t=>[t.addr,t.id]));
const out=[];const newTraders=new Set();
// traders that only appear in the backfill (exited before tracking) need dictionary ids
for(const r of o.rows)if(typeof r[2]==='string')newTraders.add(r[2]);
if(newTraders.size)out.push(`INSERT OR IGNORE INTO traders (addr) VALUES ${[...newTraders].map(a=>`('${a}')`).join(',')};`);
let n=0,bad=0;const vals=[];
for(const [mid,oi,t,ts,size] of o.rows){const m=markets.get(mid);if(ts>=m.first_ts){bad++;continue;}
  const tid=typeof t==='string'?`(SELECT id FROM traders WHERE addr='${t}')`:t;vals.push(`(${mid},${oi},${tid},${ts},${size})`);n++;}
for(let i=0;i<vals.length;i+=400)out.push(`INSERT OR REPLACE INTO holdings (mid,oi,tid,ts,size) VALUES ${vals.slice(i,i+400).join(',')};`);
for(const [mid,start] of o.marketStart)out.push(`UPDATE markets SET first_ts=${start} WHERE id=${mid} AND first_ts>${start};`);
fs.writeFileSync('backfill.sql',out.join('\n')+'\n');
console.log('rows',n,'skipped(ts>=first_ts)',bad,'new trader ids',newTraders.size,'markets moved',o.marketStart.length,'sql bytes',fs.statSync('backfill.sql').size);
