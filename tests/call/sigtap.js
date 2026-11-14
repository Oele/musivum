// Sits between a client and a server that speaks LiveKit's protocol, passes everything
// through, and writes down the signalling: every HTTP request, and every message on the
// WebSocket, decoded.
//
//   node sigtap.js <listen port> <server host:port> <output file>
//
// The output is one JSON object per line. Access tokens are left out of it.
const fs = require('fs');
const http = require('http');
const {WebSocket, WebSocketServer} = require('ws');
const {SignalRequest, SignalResponse} = require('@livekit/protocol');

const [listenPort, upstream, outFile] = process.argv.slice(2);
if (!outFile) {
  console.error('usage: node sigtap.js <listen port> <server host:port> <output file>');
  process.exit(2);
}
const out = fs.createWriteStream(outFile);
const started = Date.now();
let connections = 0;
const write = o => out.write(JSON.stringify({t: Date.now() - started, ...o}) + '\n');
const redact = url => url.replace(/(access_token=)[^&]+/, '$1…');

// Long values (SDP is kept whole: it is what the exchange is about; tokens and keys are not).
function tidy(value) {
  return JSON.parse(JSON.stringify(value, (k, v) => {
    if (typeof v === 'string' && /token|credential|password|secret/i.test(k)) return '…';
    return v;
  }));
}

function decode(type, data, isBinary) {
  if (!isBinary) return {text: data.toString()};
  try {
    const msg = type.fromBinary(new Uint8Array(data));
    return {kind: msg.message.case, message: tidy(msg.toJson({emitDefaultValues: false}))};
  } catch (e) {
    return {undecoded: data.length, error: String(e)};
  }
}

const server = http.createServer((req, res) => {
  // Plain HTTP (the validate endpoints, the room API): passed on, noted.
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    const body = Buffer.concat(chunks);
    const headers = {...req.headers, host: upstream};
    const up = http.request(`http://${upstream}${req.url}`, {method: req.method, headers}, upRes => {
      const parts = [];
      upRes.on('data', c => parts.push(c));
      upRes.on('end', () => {
        const answer = Buffer.concat(parts);
        write({http: req.method, url: redact(req.url), status: upRes.statusCode, answer: answer.toString().slice(0, 500)});
        res.writeHead(upRes.statusCode, upRes.headers);
        res.end(answer);
      });
    });
    up.on('error', e => { write({http: req.method, url: redact(req.url), error: String(e)}); res.writeHead(502); res.end(); });
    up.end(body);
  });
});

const wss = new WebSocketServer({noServer: true});
server.on('upgrade', (req, socket, head) => {
  const id = ++connections;
  const protocols = req.headers['sec-websocket-protocol'];
  const up = new WebSocket(`ws://${upstream}${req.url}`, protocols ? protocols.split(',').map(s => s.trim()) : undefined, {
    headers: Object.fromEntries(Object.entries(req.headers).filter(([k]) => ['authorization', 'user-agent', 'origin'].includes(k))),
  });
  write({ws: id, open: redact(req.url), protocols, authorizationHeader: !!req.headers.authorization});
  const pending = [];
  let client;
  up.on('unexpected-response', (_r, upRes) => {
    write({ws: id, refused: upRes.statusCode});
    socket.write(`HTTP/1.1 ${upRes.statusCode} ${upRes.statusMessage}\r\n\r\n`);
    socket.destroy();
  });
  up.on('open', () => {
    wss.handleUpgrade(req, socket, head, ws => {
      client = ws;
      write({ws: id, accepted: true, protocol: up.protocol});
      for (const [data, isBinary] of pending) ws.send(data, {binary: isBinary});
      ws.on('message', (data, isBinary) => {
        write({ws: id, from: 'client', ...decode(SignalRequest, data, isBinary)});
        up.send(data, {binary: isBinary});
      });
      ws.on('close', (code, reason) => { write({ws: id, from: 'client', close: code, reason: reason.toString()}); up.close(); });
      ws.on('error', () => up.close());
    });
  });
  up.on('message', (data, isBinary) => {
    write({ws: id, from: 'server', ...decode(SignalResponse, data, isBinary)});
    if (client) client.send(data, {binary: isBinary}); else pending.push([data, isBinary]);
  });
  up.on('close', (code, reason) => { write({ws: id, from: 'server', close: code, reason: reason.toString()}); if (client) client.close(); });
  up.on('error', e => { write({ws: id, error: String(e)}); socket.destroy(); });
});

server.listen(Number(listenPort), '127.0.0.1', () => console.log(`sigtap on 127.0.0.1:${listenPort} -> ${upstream}`));
