'use strict';

// Локальный WS-прокси: наружу к Bybit — с permessage-deflate, локально —
// обычный WS. Коллекторы подключаются к ws://127.0.0.1:8765/{linear|spot}.
const { WebSocketServer, WebSocket } = require('ws');

const HOST = process.env.WS_PROXY_HOST || '127.0.0.1';
const PORT = Number(process.env.WS_PROXY_PORT || 8765);
const UPSTREAM = {
  linear: 'wss://stream.bybit.com/v5/public/linear',
  spot: 'wss://stream.bybit.com/v5/public/spot',
  inverse: 'wss://stream.bybit.com/v5/public/inverse',
};

const wss = new WebSocketServer({ host: HOST, port: PORT, perMessageDeflate: false });
let statMsgs = 0;
let statPrevWire = 0;
const upstreams = new Set();
setInterval(() => {
  let total = 0;
  for (const u of upstreams) if (u._socket) total += u._socket.bytesRead;
  const wire = total - statPrevWire;
  statPrevWire = total;
  console.log(`[proxy] stats msgs_s=${statMsgs} wire_kbps=${(wire/60/1024).toFixed(1)} conns=${upstreams.size}`);
  statMsgs = 0;
}, 60000);

wss.on('connection', (local, req) => {
  const path = (req.url || '/linear').split('?')[0].replace(/^\/+/, '') || 'linear';
  const upstreamUrl = UPSTREAM[path] || UPSTREAM.linear;
  const upstream = new WebSocket(upstreamUrl, {
    perMessageDeflate: { threshold: 0 },
    handshakeTimeout: 15000,
    maxPayload: 64 * 1024 * 1024,
  });
  const queue = [];
  upstreams.add(upstream);

  local.on('message', (data, isBinary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary: isBinary });
    } else if (queue.length < 256) {
      queue.push([data, isBinary]);
    }
  });

  upstream.on('open', () => {
    for (const [data, isBinary] of queue) upstream.send(data, { binary: isBinary });
    queue.length = 0;
    console.log(`[proxy] connected ${path} ext=${upstream.extensions || 'none'}`);
  });

  upstream.on('message', (data, isBinary) => {
    statMsgs++;
    if (local.readyState === WebSocket.OPEN) local.send(data, { binary: isBinary });
  });

  upstream.on('close', () => {
    upstreams.delete(upstream);
    if (local.readyState === WebSocket.OPEN) local.close();
  });

  upstream.on('error', (err) => {
    console.error(`[proxy] upstream ${path}: ${err.message}`);
    try { local.close(); } catch (_) {}
  });

  local.on('close', () => {
    try { upstream.close(); } catch (_) {}
  });

  local.on('error', () => {
    try { upstream.close(); } catch (_) {}
  });
});

wss.on('listening', () => console.log(`[proxy] listening ws://${HOST}:${PORT}`));
wss.on('error', (err) => console.error(`[proxy] server: ${err.message}`));
