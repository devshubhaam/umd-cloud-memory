'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
// tiny .env loader (no dependency)
try {
  fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/).forEach((l) => {
    const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*?)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  });
} catch (e) { /* no .env: use real environment variables */ }

const { MongoClient } = require('mongodb');
const { makeHandler } = require('./app');

const { MONGODB_URI, API_KEY } = process.env;
const DB = process.env.MONGODB_DB || 'umd', PORT = Number(process.env.PORT) || 8787;
if (!MONGODB_URI) { console.error('MONGODB_URI is missing (see .env.example)'); process.exit(1); }
if (!API_KEY || API_KEY.length < 12) { console.error('API_KEY is missing or too short (use 12+ characters)'); process.exit(1); }

(async () => {
  const client = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 15000 });
  await client.connect();
  const col = client.db(DB).collection('items');
  await col.createIndex({ type: 1 });
  http.createServer(makeHandler({ col, apiKey: API_KEY })).listen(PORT, '0.0.0.0', () => console.log('UMD cloud memory ready on port ' + PORT));
})().catch((e) => { console.error('Could not start:', e.message); process.exit(1); });
