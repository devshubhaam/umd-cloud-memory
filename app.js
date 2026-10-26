'use strict';
/* Request handling for the UMD cloud-memory API. No framework: only node:http + a Mongo collection.
 * Documents (one collection, "items"):  { _id: "<type>|<sha1(key)>", type: "post|canon|video|dead", key, ts, ...extra }
 *   post  = a post that was downloaded      canon = its canonical URL      video = its video file URL
 *   dead  = a post that failed for good (reason, title, video, attempts, url)
 * Plus one stats doc { _id: "stats", videos, bytes, thumbs }.  */
const crypto = require('node:crypto');

const TYPES = ['post', 'canon', 'video', 'dead'];
const sha1 = (s) => crypto.createHash('sha1').update(String(s)).digest('hex');
const id = (type, key) => type + '|' + sha1(key);
const str = (v, n) => String(v == null ? '' : v).slice(0, n);
const num = (v) => { v = Number(v); return Number.isFinite(v) && v >= 0 ? v : 0; };

async function applyOps(col, ops) {
  const writes = [];
  for (const op of ops) {
    if (!op || typeof op !== 'object') continue;
    const ts = num(op.ts) || Date.now();
    switch (op.t) {
      case 'done': {
        if (!op.key) break;
        writes.push({ updateOne: { filter: { _id: id('post', op.key) }, update: { $set: { type: 'post', key: str(op.key, 1000), ts, host: str(op.host, 200), url: str(op.url, 2000), title: str(op.title, 300), bytes: num(op.bytes), hasVideo: !!op.hasVideo, hasThumb: !!op.hasThumb } }, upsert: true } });
        writes.push({ deleteMany: { filter: { _id: id('dead', op.key) } } });
        if (op.canonKey) {
          writes.push({ updateOne: { filter: { _id: id('canon', op.canonKey) }, update: { $set: { type: 'canon', key: str(op.canonKey, 1000), ts } }, upsert: true } });
          writes.push({ deleteMany: { filter: { _id: id('dead', op.canonKey) } } });
        }
        if (op.videoUrl) writes.push({ updateOne: { filter: { _id: id('video', op.videoUrl) }, update: { $set: { type: 'video', key: str(op.videoUrl, 2000), ts } }, upsert: true } });
        break;
      }
      case 'dead': {
        if (!op.key) break;
        const rec = { type: 'dead', ts, reason: str(op.reason, 200), video: str(op.video, 2000), title: str(op.title, 300), attempts: num(op.attempts), url: str(op.url, 2000) };
        writes.push({ updateOne: { filter: { _id: id('dead', op.key) }, update: { $set: Object.assign({ key: str(op.key, 1000) }, rec) }, upsert: true } });
        if (op.canonKey) writes.push({ updateOne: { filter: { _id: id('dead', op.canonKey) }, update: { $set: Object.assign({ key: str(op.canonKey, 1000) }, rec) }, upsert: true } });
        break;
      }
      case 'undead':
        for (const k of Array.isArray(op.keys) ? op.keys : []) writes.push({ deleteMany: { filter: { _id: id('dead', k) } } });
        break;
      case 'resetDead': writes.push({ deleteMany: { filter: { type: 'dead' } } }); break;
      case 'resetAll': writes.push({ deleteMany: { filter: { type: { $in: TYPES } } } }); break;
      default: break;
    }
  }
  if (writes.length) {
    await col.bulkWrite(writes, { ordered: true });
    // counters are recomputed from the post docs, so the same post sent twice is never counted twice
    await recomputeStats(col);
  }
  return { applied: ops.length };
}

async function recomputeStats(col) {
  const agg = await col.aggregate([{ $match: { type: 'post' } }, { $group: { _id: null, videos: { $sum: { $cond: ['$hasVideo', 1, 0] } }, bytes: { $sum: '$bytes' }, thumbs: { $sum: { $cond: ['$hasThumb', 1, 0] } } } }]).toArray();
  const a = agg[0] || { videos: 0, bytes: 0, thumbs: 0 };
  const old = (await col.findOne({ _id: 'stats' })) || { videos: 0, bytes: 0, thumbs: 0 };
  // Reset keeps the lifetime numbers (same as the extension); otherwise they never go down
  const pick = (k) => Math.max(num(old[k]), num(a[k]));
  await col.updateOne({ _id: 'stats' }, { $set: { videos: pick('videos'), bytes: pick('bytes'), thumbs: pick('thumbs') } }, { upsert: true });
}

async function importAll(col, h) {
  const writes = [];
  const add = (type, map, extra) => Object.keys(map || {}).forEach((k) => {
    const v = map[k];
    const ts = num(typeof v === 'object' && v ? v.ts : v) || Date.now();
    const doc = Object.assign({ type, key: str(k, 2000), ts }, extra ? extra(v) : {});
    writes.push({ updateOne: { filter: { _id: id(type, k) }, update: { $setOnInsert: doc }, upsert: true } });
  });
  add('post', h.posts); add('canon', h.canon); add('video', h.videos);
  add('dead', h.dead, (v) => ({ reason: str(v && v.reason, 200), video: str(v && v.video, 2000), title: str(v && v.title, 300), attempts: num(v && v.attempts), url: str(v && v.url, 2000) }));
  for (let i = 0; i < writes.length; i += 1000) await col.bulkWrite(writes.slice(i, i + 1000), { ordered: false });
  const lt = h.lifetime || {};
  await col.updateOne({ _id: 'stats' }, { $max: { videos: num(lt.videos), bytes: num(lt.bytes), thumbs: num(lt.thumbs) } }, { upsert: true });
  return { imported: writes.length };
}

async function readAll(col) {
  const out = { posts: {}, canon: {}, videos: {}, dead: {}, lifetime: { videos: 0, bytes: 0, thumbs: 0 } };
  const cur = col.find({ type: { $in: TYPES } });
  for await (const d of cur) {
    if (d.type === 'post') out.posts[d.key] = d.ts;
    else if (d.type === 'canon') out.canon[d.key] = d.ts;
    else if (d.type === 'video') out.videos[d.key] = d.ts;
    else if (d.type === 'dead') out.dead[d.key] = { ts: d.ts, reason: d.reason || '', video: d.video || '', title: d.title || '', attempts: d.attempts || 0, url: d.url || '' };
  }
  const s = await col.findOne({ _id: 'stats' });
  if (s) out.lifetime = { videos: num(s.videos), bytes: num(s.bytes), thumbs: num(s.thumbs) };
  return out;
}

function makeHandler({ col, apiKey }) {
  const send = (res, code, obj) => {
    const body = JSON.stringify(obj);
    res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type,x-api-key', 'access-control-allow-methods': 'GET,POST,OPTIONS', 'cache-control': 'no-store' });
    res.end(body);
  };
  const readBody = (req) => new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', (c) => { n += c.length; if (n > 20 * 1024 * 1024) { reject(Object.assign(new Error('Body too large'), { code: 413 })); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); } catch (e) { reject(Object.assign(new Error('Invalid JSON'), { code: 400 })); } });
    req.on('error', reject);
  });
  const keyOk = (req) => {
    const got = Buffer.from(String(req.headers['x-api-key'] || '')), want = Buffer.from(apiKey);
    return got.length === want.length && crypto.timingSafeEqual(got, want);
  };
  return async (req, res) => {
    try {
      const path = new URL(req.url, 'http://x').pathname.replace(/\/+$/, '') || '/';
      if (req.method === 'OPTIONS') return send(res, 204, {});
      if (path === '/' || path === '/health') return send(res, 200, { ok: true, service: 'umd-mongo-server' });
      if (!keyOk(req)) return send(res, 401, { ok: false, error: 'Wrong API key' });
      if (req.method === 'GET' && path === '/api/ping') {
        const [posts, dead] = await Promise.all([col.countDocuments({ type: 'post' }), col.countDocuments({ type: 'dead' })]);
        return send(res, 200, { ok: true, posts, dead });
      }
      if (req.method === 'GET' && path === '/api/history') return send(res, 200, Object.assign({ ok: true }, await readAll(col)));
      if (req.method === 'POST' && path === '/api/ops') {
        const b = await readBody(req);
        if (!Array.isArray(b.ops)) return send(res, 400, { ok: false, error: 'ops[] missing' });
        if (b.ops.length > 2000) return send(res, 400, { ok: false, error: 'Too many ops in one request' });
        return send(res, 200, Object.assign({ ok: true }, await applyOps(col, b.ops)));
      }
      if (req.method === 'POST' && path === '/api/import') return send(res, 200, Object.assign({ ok: true }, await importAll(col, await readBody(req))));
      return send(res, 404, { ok: false, error: 'Not found' });
    } catch (e) {
      console.error(e);
      return send(res, e.code === 413 || e.code === 400 ? e.code : 500, { ok: false, error: e.message || 'Server error' });
    }
  };
}

module.exports = { makeHandler, applyOps, importAll, readAll, id };
