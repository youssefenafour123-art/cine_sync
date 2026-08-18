#!/usr/bin/env node
/**
 * IMDb → Stremio Sync Tool — Local Proxy Server
 *
 * WHY THIS IS NEEDED:
 *   api.strem.io validates the request Origin header server-side.
 *   Requests from file:// pages or non-Stremio origins are silently rejected.
 *   This proxy forwards your requests with Origin: https://web.stremio.com
 *   so Stremio treats them as legitimate web app requests.
 *
 * USAGE:
 *   node server.js
 *   Then open: http://localhost:3000
 *
 * REQUIREMENTS: Node.js 14+ (no npm install needed — uses built-in modules only)
 */

'use strict';

const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const zlib  = require('zlib');
const url   = require('url');

const PORT = process.env.PORT || 3000;

// ─── Direct server-side call to api.strem.io ─────────────────────
// Used by the /api/test endpoint to probe payload formats from Node.js
function directStremioCall(apiPath, payload) {
  return new Promise((resolve) => {
    const body = JSON.stringify(payload);
    const options = {
      hostname: 'api.strem.io',
      port: 443,
      path: apiPath,
      method: 'POST',
      headers: {
        'Content-Type':    'application/json',
        'Content-Length':  Buffer.byteLength(body),
        'Origin':          'https://web.stremio.com',
        'Referer':         'https://web.stremio.com/',
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Accept':          'application/json, */*',
        'Accept-Encoding': 'gzip, deflate, br',
      }
    };
    const req = https.request(options, res => {
      const enc = (res.headers['content-encoding'] || '').toLowerCase();
      let stream = res;
      if (enc.includes('br'))      stream = res.pipe(zlib.createBrotliDecompress());
      else if (enc.includes('gzip'))    stream = res.pipe(zlib.createGunzip());
      else if (enc.includes('deflate')) stream = res.pipe(zlib.createInflate());
      const chunks = [];
      stream.on('data', c => chunks.push(c));
      stream.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString('utf8') }));
      stream.on('error', err => resolve({ status: 0, body: 'stream error: ' + err.message }));
    });
    req.on('error', err => resolve({ status: 0, body: 'request error: ' + err.message }));
    req.write(body);
    req.end();
  });
}

// ─── Stremio API proxy ────────────────────────────────────────────
function handleStremioProxy(req, res) {
  // Strip "/api/stremio/" prefix → remainder is the Stremio API path
  const suffix = req.url.slice('/api/stremio/'.length);
  const targetPath = '/api/' + suffix;

  const reqChunks = [];
  req.on('data', c => reqChunks.push(c));
  req.on('end', () => {
    const body = Buffer.concat(reqChunks);

    console.log(`\n→ PROXY  POST https://api.strem.io${targetPath}`);
    if (body.length) console.log('  Body:', body.toString().substring(0, 300));

    const options = {
      hostname: 'api.strem.io',
      port:     443,
      path:     targetPath,
      method:   'POST',
      headers: {
        'Content-Type':     'application/json',
        'Content-Length':   Buffer.byteLength(body),
        'Origin':           'https://web.stremio.com',
        'Referer':          'https://web.stremio.com/',
        'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept':           'application/json, */*',
        'Accept-Language':  'en-US,en;q=0.9',
        'Accept-Encoding':  'gzip, deflate, br',
        'Connection':       'keep-alive',
        'Sec-Fetch-Site':   'same-site',
        'Sec-Fetch-Mode':   'cors',
        'Sec-Fetch-Dest':   'empty',
      },
    };

    const proxyReq = https.request(options, proxyRes => {
      const enc = (proxyRes.headers['content-encoding'] || '').toLowerCase();
      let stream = proxyRes;
      if (enc.includes('br'))      stream = proxyRes.pipe(zlib.createBrotliDecompress());
      else if (enc.includes('gzip'))    stream = proxyRes.pipe(zlib.createGunzip());
      else if (enc.includes('deflate')) stream = proxyRes.pipe(zlib.createInflate());

      const resChunks = [];
      stream.on('data', c => resChunks.push(c));
      stream.on('end', () => {
        const resBody = Buffer.concat(resChunks);
        const resText = resBody.toString('utf8');
        console.log(`← ${proxyRes.statusCode}  ${resText.substring(0, 400)}`);

        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.writeHead(proxyRes.statusCode || 200);
        res.end(resBody);
      });
      stream.on('error', err => {
        console.error('[stream error]', err.message);
        if (!res.headersSent) res.writeHead(502);
        res.end(JSON.stringify({ error: err.message }));
      });
    });

    proxyReq.on('error', err => {
      console.error('[proxy error]', err.message);
      if (!res.headersSent) res.writeHead(502);
      res.end(JSON.stringify({ error: 'Proxy error: ' + err.message }));
    });

    proxyReq.write(body);
    proxyReq.end();
  });
}

// ─── SSE Clients for Bookmarklet Sync ──────────────────────────────
const sseClients = new Set();

// ─── HTTP Server ──────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const pathname = url.parse(req.url).pathname;

  // ── SSE Endpoint for pushing items to UI
  if (pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write(': ping\n\n');
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
    return;
  }

  // ── Bookmarklet Endpoint to receive IMDb items
  if (pathname === '/api/sync-watchlist' && req.method === 'POST') {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      try {
        const data = Buffer.concat(chunks).toString();
        // Broadcast the received JSON to all connected UIs
        for (const client of sseClients) {
          client.write(`event: watchlist\ndata: ${data}\n\n`);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, count: sseClients.size }));
      } catch (err) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // ── Proxy Stremio API (/api/stremio/* → https://api.strem.io/api/*)
  if (pathname.startsWith('/api/stremio/')) {
    return handleStremioProxy(req, res);
  }

  // ── Debug: test various datastoreGet param formats to find correct read API
  // POST /api/test  body: { authKey }
  if (pathname === '/api/test' && req.method === 'POST') {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', async () => {
      try {
        const { authKey } = JSON.parse(Buffer.concat(chunks).toString());
        if (!authKey) throw new Error('authKey required');

        const nowIso = new Date().toISOString();
        const testId  = 'tt0111161';

        // 1. Write one test item with correct Format B
        const writeResult = await directStremioCall('/api/datastorePut', {
          authKey, collection: 'libraryItem', changes: [{
            _id: testId, name: 'Test - Shawshank', type: 'movie',
            poster: `https://images.metahub.space/poster/medium/${testId}/img`,
            removed: false, temp: false, _mtime: nowIso, _ctime: nowIso, _v: 0,
            state: { lastWatched: null, timeWatched: 0, timesWatched: 0,
                     flaggedWatched: 0, overallTimeWatched: 0, noNotif: false, watched: '' }
          }]
        });
        console.log('\n[TEST] Write test item:', writeResult.body);

        // 2. Try different datastoreGet parameter formats to see which returns data
        const reads = [
          { label: 'GET-A: ids:[], lastModified:0',    body: { authKey, collection: 'libraryItem', ids: [], lastModified: 0 } },
          { label: 'GET-B: no ids, no lastModified',   body: { authKey, collection: 'libraryItem' } },
          { label: 'GET-C: ids:[testId]',              body: { authKey, collection: 'libraryItem', ids: [testId] } },
          { label: 'GET-D: lastModified:0 only',       body: { authKey, collection: 'libraryItem', lastModified: 0 } },
        ];

        const results = [{ label: 'WRITE result', response: writeResult.body, status: writeResult.status }];
        for (const r of reads) {
          const res2 = await directStremioCall('/api/datastoreGet', r.body);
          console.log(`[TEST] ${r.label}: ${res2.body.substring(0, 300)}`);
          results.push({ label: r.label, response: res2.body.substring(0, 500), status: res2.status });
        }

        const body = JSON.stringify({ results });
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(body);
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }


  // ── Serve index.html
  if (pathname === '/' || pathname === '/index.html') {
    const fp = path.join(__dirname, 'index.html');
    fs.readFile(fp, (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Cannot read index.html: ' + err.message);
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  // ── Serve static images
  if (pathname.endsWith('.jpg') || pathname.endsWith('.png') || pathname.endsWith('.jpeg')) {
    const fp = path.join(__dirname, pathname);
    fs.readFile(fp, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end('Not found');
        return;
      }
      res.writeHead(200, { 'Content-Type': pathname.endsWith('.png') ? 'image/png' : 'image/jpeg' });
      res.end(data);
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});
server.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(`\n  ERROR: Port ${PORT} is already in use.`);
    console.error(`  Try: set PORT=3001 && node server.js\n`);
  } else {
    console.error('Server error:', err.message);
  }
  process.exit(1);
});

server.listen(PORT, () => {
  const border = '═'.repeat(47);
  console.log(`\n  ╔${border}╗`);
  console.log(`  ║   IMDb → Stremio Sync  (Proxy Mode)            ║`);
  console.log(`  ╠${border}╣`);
  console.log(`  ║   Open in browser: http://localhost:${PORT}          ║`);
  console.log(`  ║   API requests will be logged here             ║`);
  console.log(`  ║   Press Ctrl+C to stop                         ║`);
  console.log(`  ╚${border}╝\n`);
});
