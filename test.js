'use strict';
/* Offline test of the HTTP API against a tiny in-memory stand-in for the Mongo collection (no database needed).
 * Run: node test.js   (the real server uses the same app.js with a real MongoDB collection) */
const http = require('node:http');
const assert = require('node:assert');
const { makeHandler } = require('./app');

function fakeCol() {
  const docs = new Map();
  const match = (d, f) => Object.entries(f).every(([k, v]) => (v && typeof v === 'object' && v.$in) ? v.$in.includes(d[k]) : (v && typeof v === 'object' && v.$regex) ? new RegExp(v.$regex).test(d[k]) : d[k] === v);
  const upd = (cur, u, upsert) => {
    const isNew = !cur; const d = cur || {};
    if (u.$set) Object.assign(d, u.$set);
    if (u.$setOnInsert && isNew) Object.assign(d, u.$setOnInsert);
    if (u.$max) Object.entries(u.$max).forEach(([k, v]) => { d[k] = Math.max(d[k] || 0, v); });
    return d;
  };
  return {
    docs,
    async bulkWrite(ops) {
      for (const o of ops) {
        if (o.updateOne) { const { filter, update, upsert } = o.updateOne; const cur = docs.get(filter._id); if (!cur && !upsert) continue; docs.set(filter._id, Object.assign({ _id: filter._id }, upd(cur && { ...cur }, update))); }
        else if (o.deleteMany) for (const [k, d] of [...docs]) if (match(d, o.deleteMany.filter) || (o.deleteMany.filter._id && k === o.deleteMany.filter._id)) docs.delete(k);
      }
    },
    async updateOne(f, u, opt) { const cur = docs.get(f._id); docs.set(f._id, Object.assign({ _id: f._id }, upd(cur && { ...cur }, u))); },
    async findOne(f) { return docs.get(f._id) || null; },
    async countDocuments(f) { return [...docs.values()].filter((d) => match(d, f)).length; },
    find(f) { const list = [...docs.values()].filter((d) => match(d, f)); return { async *[Symbol.asyncIterator]() { for (const d of list) yield d; } }; },
    aggregate() { const p = [...docs.values()].filter((d) => d.type === 'post'); return { toArray: async () => [{ videos: p.filter((d) => d.hasVideo).length, bytes: p.reduce((a, d) => a + (d.bytes || 0), 0), thumbs: p.filter((d) => d.hasThumb).length }] }; }
  };
}

(async () => {
  const col = fakeCol();
  const srv = http.createServer(makeHandler({ col, apiKey: 'test-key-123456' }));
  await new Promise((r) => srv.listen(0, r));
  const base = 'http://127.0.0.1:' + srv.address().port;
  const call = async (m, p, body, key = 'test-key-123456') => { const r = await fetch(base + p, { method: m, headers: { 'x-api-key': key, 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined }); return { s: r.status, j: await r.json() }; };

  assert.equal((await call('GET', '/health')).s, 200);
  assert.equal((await call('GET', '/api/ping', null, 'nope')).s, 401, 'wrong key rejected');
  assert.equal((await call('GET', '/api/ping')).j.posts, 0);

  const done = (k, extra) => Object.assign({ t: 'done', key: k, canonKey: k + '/c', videoUrl: 'https://v/' + k + '.mp4', host: 'x.com', url: 'https://' + k, title: 'T ' + k, bytes: 1000, hasVideo: true, hasThumb: true, ts: 111 }, extra);
  await call('POST', '/api/ops', { ops: [done('x.com/a'), done('x.com/b'), done('x.com/a')] });   // a sent twice
  let h = (await call('GET', '/api/history')).j;
  assert.deepEqual(Object.keys(h.posts).sort(), ['x.com/a', 'x.com/b']);
  assert.equal(Object.keys(h.canon).length, 2); assert.equal(Object.keys(h.videos).length, 2);
  assert.deepEqual(h.lifetime, { videos: 2, bytes: 2000, thumbs: 2 }, 'same post twice is counted once');

  await call('POST', '/api/ops', { ops: [{ t: 'dead', key: 'x.com/d', canonKey: 'x.com/d/c', reason: 'boom', title: 'D', attempts: 3, url: 'https://x.com/d', ts: 5 }] });
  h = (await call('GET', '/api/history')).j;
  assert.equal(h.dead['x.com/d'].reason, 'boom'); assert.ok(h.dead['x.com/d/c']);
  await call('POST', '/api/ops', { ops: [done('x.com/d')] });                       // success later removes the failure
  h = (await call('GET', '/api/history')).j; assert.equal(Object.keys(h.dead).length, 0); assert.ok(h.posts['x.com/d']);

  await call('POST', '/api/ops', { ops: [{ t: 'dead', key: 'x.com/e', reason: 'r' }, { t: 'dead', key: 'x.com/f', reason: 'r' }, { t: 'undead', keys: ['x.com/e'] }] });
  h = (await call('GET', '/api/history')).j; assert.deepEqual(Object.keys(h.dead), ['x.com/f']);
  await call('POST', '/api/ops', { ops: [{ t: 'resetDead' }] }); h = (await call('GET', '/api/history')).j; assert.equal(Object.keys(h.dead).length, 0);

  await call('POST', '/api/ops', { ops: [done('x.com/a'), done('www.other.org/z'), { t: 'dead', key: 'x.com/q', reason: 'r' }, done('x.com.evil.net/n')] });
  await call('POST', '/api/ops', { ops: [{ t: 'resetHost', host: 'www.X.com' }] });
  h = (await call('GET', '/api/history')).j;
  assert.ok(!h.posts['x.com/a'] && !h.dead['x.com/q'] && !h.canon['x.com/a/c'], 'site wiped');
  assert.ok(h.posts['www.other.org/z'] && h.posts['x.com.evil.net/n'], 'other sites (even look-alikes) kept');

  await call('POST', '/api/ops', { ops: [{ t: 'resetAll' }] });
  h = (await call('GET', '/api/history')).j;
  assert.equal(Object.keys(h.posts).length, 0); assert.ok(h.lifetime.videos >= 3, 'lifetime numbers survive a reset');

  const imp = await call('POST', '/api/import', { posts: { 'y.com/1': 1, 'y.com/2': 2 }, canon: {}, videos: {}, dead: { 'y.com/z': { ts: 3, reason: 'q' } }, lifetime: { videos: 99, bytes: 5, thumbs: 1 } });
  assert.equal(imp.s, 200);
  h = (await call('GET', '/api/history')).j;
  assert.equal(Object.keys(h.posts).length, 2); assert.equal(h.dead['y.com/z'].reason, 'q'); assert.equal(h.lifetime.videos, 99);
  assert.equal((await call('POST', '/api/ops', { nope: 1 })).s, 400);
  assert.equal((await call('GET', '/api/nothing')).s, 404);
  srv.close();
  console.log('mongo_server: all tests passed');
})().catch((e) => { console.error(e); process.exit(1); });
