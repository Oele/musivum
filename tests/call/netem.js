// The call on a degraded network: the scripted call's two participants (page.html, as in
// run.js), with their media going through the impairment relay of ../netem, scenario by
// scenario (../netem/scenarios.js). It measures what the subscriber sees, once a second,
// and writes out/<target>-<scenario>.json with one summary line per scenario.
//
//   NETEM=all ./run.sh reference              every scenario, against LiveKit
//   NETEM="loss-2 blackout-3s" ./run.sh musivum
//   node netem.js <target> <ws url> <api key> <api secret> <scenario ...|all>
//
// How the relay is put in the path: the server listens for media on port 39522 of 127.0.0.1
// (on that address only or on all of them) and advertises 127.0.0.2:39522 (for the reference,
// the YAML in ../netem). The relay listens on 127.0.0.2:39539 and forwards each client's
// packets to 127.0.0.1:39522 from a port of its own (39523-39537), so the server answers to
// the relay, which maps the answers back. The clients (filterCandidates below) take the
// server's candidate at 127.0.0.2:39522 to be the relay's, and drop all its others; they
// never tell the server their own candidates, so the server cannot reach them except through
// the relay. At the end of every scenario the packets the browsers say they sent and received
// are held against what the relay counted (the proof in the output).
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const net = require('net');
const os = require('os');
const path = require('path');
const {execSync, spawn} = require('child_process');
const puppeteer = require('puppeteer-core');
const {DURATION, scenarios} = require('../netem/scenarios');
const {format} = require('../netem/json');

const [target, wsUrl, apiKey, apiSecret, ...wanted] = process.argv.slice(2);
if (!apiSecret || !wanted.length) {
  console.error('usage: node netem.js <target> <ws url> <api key> <api secret> <scenario ...|all>');
  console.error('scenarios: ' + Object.keys(scenarios).join(' '));
  process.exit(2);
}
const names = wanted.join(' ').split(/\s+/).filter(Boolean).flatMap(n => n === 'all' ? Object.keys(scenarios) : [n]);
for (const n of names) if (!scenarios[n]) { console.error(`unknown scenario: ${n}`); process.exit(2); }

// What the server advertises, where it listens, and where the relay listens instead.
const ADVERTISED = {host: '127.0.0.2', port: 39522};
const MEDIA_PORT = 39522;
const RELAY = {host: '127.0.0.2', port: 39539};
const RELAY_FRONT = `${RELAY.host}:${RELAY.port}`;
const CONTROL_PORT = 39520;
const SEED = Number(process.env.NETEM_SEED || 1);
const WARMUP_MS = 45000;
const repo = path.resolve(__dirname, '../..');
const chrome = process.env.CHROME ||
  execSync('command -v google-chrome || command -v chromium || command -v chromium-browser', {shell: '/bin/sh'}).toString().trim();
const pagePort = Number(process.env.PAGE_PORT || 39510);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// As in run.js.
function token(identity, room) {
  const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const body = b64({alg: 'HS256', typ: 'JWT'}) + '.' + b64({
    iss: apiKey, sub: identity, name: identity, nbf: now - 10, exp: now + 3600,
    video: {room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true},
  });
  return body + '.' + crypto.createHmac('sha256', apiSecret).update(body).digest('base64url');
}

function servePage() {
  const files = {
    '/': ['page.html', 'text/html'],
    '/livekit-client.umd.js': ['node_modules/livekit-client/dist/livekit-client.umd.js', 'text/javascript'],
  };
  const server = http.createServer((req, res) => {
    const f = files[req.url.split('?')[0]];
    if (!f) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, {'content-type': f[1]});
    fs.createReadStream(path.join(__dirname, f[0])).pipe(res);
  });
  return new Promise(resolve => server.listen(pagePort, '127.0.0.1', () => resolve(server)));
}

async function until(fn, ms, step = 250) {
  const end = Date.now() + ms;
  let last;
  while (Date.now() < end) {
    last = await fn();
    if (last) return last;
    await sleep(step);
  }
  return last;
}

// ---- The relay ----

function buildRelay() {
  execSync('nice -n 10 cargo build --release --quiet -p musivum-netem', {cwd: repo, stdio: 'inherit'});
  return path.join(process.env.CARGO_TARGET_DIR || path.join(repo, 'target'), 'release', 'netem-relay');
}

function controlConnection(port) {
  return new Promise((resolve, reject) => {
    const sock = net.connect(port, '127.0.0.1');
    let buf = '';
    const waiting = [];
    sock.setEncoding('utf8');
    sock.on('data', d => {
      buf += d;
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        const w = waiting.shift();
        if (w) w(line);
      }
    });
    const send = cmd => new Promise((res, rej) => {
      waiting.push(line => line.startsWith('error') ? rej(new Error(`relay: ${cmd}: ${line}`)) : res(line));
      sock.write(cmd + '\n');
    });
    sock.once('connect', () => resolve({send, stats: async () => JSON.parse(await send('stats')), end: () => sock.end()}));
    sock.once('error', reject);
  });
}

async function startRelay(bin) {
  const args = ['--front', RELAY_FRONT, '--server', `127.0.0.1:${MEDIA_PORT}`, '--ports', '39523-39537',
    '--control', `127.0.0.1:${CONTROL_PORT}`, '--seed', String(SEED), '--stdin-lifeline'];
  const child = spawn('sh', ['-c', 'ulimit -d 8388608; exec timeout 600 "$@"', 'sh', bin, ...args],
    {stdio: ['pipe', 'pipe', 'inherit']});
  await new Promise((resolve, reject) => {
    child.stdout.once('data', resolve);
    child.once('exit', code => reject(new Error(`netem-relay exited with ${code}`)));
  });
  const control = await controlConnection(CONTROL_PORT);
  return {
    control,
    stop: () => { control.end(); child.stdin.end(); child.kill(); },
  };
}

// ---- In the browser ----

// Runs in each page before LiveKit's SDK loads. The server must learn the client's address
// only from packets that reached it through the relay, and the client must send only to the
// relay: so the client's own candidates are held back (not trickled, and taken out of the
// SDP it sends), and of the server's candidates only the UDP one at the advertised address
// is kept, pointed at the relay.
function filterCandidates(advertised, relay) {
  const P = RTCPeerConnection.prototype;
  const counts = window.netemFilter = {localHeld: 0, remoteKept: 0, remoteDropped: 0};
  // The candidate pointed at the relay, or null if it is to be dropped.
  // candidate:<foundation> <component> <transport> <priority> <address> <port> typ <type> ...
  const toRelay = line => {
    const f = line.trim().split(' ');
    if (f.length < 8 || f[2].toLowerCase() !== 'udp' || f[4] !== advertised.host || f[5] !== String(advertised.port)) {
      return null;
    }
    f[4] = relay.host;
    f[5] = String(relay.port);
    return f.join(' ');
  };
  const stripLocal = (sdp, count) => sdp
    .replace(/^a=candidate:.*\r?\n/gm, () => { if (count) counts.localHeld++; return ''; })
    .replace(/^a=end-of-candidates\r?\n/gm, '');
  const filterRemote = sdp => sdp.replace(/^(a=candidate:.*?)(\r?\n)/gm, (_, line, end) => {
    const kept = toRelay(line);
    if (kept) { counts.remoteKept++; return kept + end; }
    counts.remoteDropped++;
    return '';
  });
  for (const name of ['createOffer', 'createAnswer']) {
    const native = P[name];
    P[name] = async function (...args) {
      const d = await native.apply(this, args);
      return d && typeof d.sdp === 'string' ? {type: d.type, sdp: stripLocal(d.sdp, true)} : d;
    };
  }
  for (const name of ['localDescription', 'currentLocalDescription', 'pendingLocalDescription']) {
    const d = Object.getOwnPropertyDescriptor(P, name);
    if (!d || !d.get) continue;
    Object.defineProperty(P, name, {
      configurable: true, enumerable: d.enumerable,
      get() {
        const v = d.get.call(this);
        return v ? new RTCSessionDescription({type: v.type, sdp: stripLocal(v.sdp, false)}) : v;
      },
    });
  }
  const setRemote = P.setRemoteDescription;
  P.setRemoteDescription = function (d, ...rest) {
    if (d && typeof d.sdp === 'string') d = {type: d.type, sdp: filterRemote(d.sdp)};
    return setRemote.call(this, d, ...rest);
  };
  const addCandidate = P.addIceCandidate;
  P.addIceCandidate = function (c, ...rest) {
    const line = c && c.candidate;
    if (line) {
      const kept = toRelay(line);
      if (!kept) { counts.remoteDropped++; return Promise.resolve(); }
      counts.remoteKept++;
      c = {candidate: kept, sdpMid: c.sdpMid, sdpMLineIndex: c.sdpMLineIndex, usernameFragment: c.usernameFragment};
    }
    return addCandidate.call(this, c, ...rest);
  };
  // Gathered candidates are not handed to the page (an event without a candidate, the end
  // of gathering, still is).
  const wrapped = new WeakMap();
  const wrap = fn => {
    if (!wrapped.has(fn)) {
      wrapped.set(fn, function (ev) {
        if (ev.candidate) { counts.localHeld++; return undefined; }
        return typeof fn === 'function' ? fn.call(this, ev) : fn.handleEvent(ev);
      });
    }
    return wrapped.get(fn);
  };
  const on = Object.getOwnPropertyDescriptor(P, 'onicecandidate');
  Object.defineProperty(P, 'onicecandidate', {
    configurable: true, enumerable: on.enumerable,
    get() { return this.__netemOnIceCandidate || null; },
    set(fn) { this.__netemOnIceCandidate = fn; on.set.call(this, fn ? wrap(fn) : fn); },
  });
  const addListener = P.addEventListener;
  const removeListener = P.removeEventListener;
  P.addEventListener = function (type, fn, ...rest) {
    return addListener.call(this, type, type === 'icecandidate' && fn ? wrap(fn) : fn, ...rest);
  };
  P.removeEventListener = function (type, fn, ...rest) {
    return removeListener.call(this, type, type === 'icecandidate' && fn ? wrap(fn) : fn, ...rest);
  };
}

// Runs in a page once a second: the WebRTC statistics of both its peer connections, raw.
// Addresses other than loopback are not taken out of the page.
async function pageSample() {
  const loopback = a => typeof a === 'string' && (a.startsWith('127.') || a === '::1') ? a : (a ? 'non-loopback' : a);
  const pick = (s, keys) => Object.fromEntries(keys.filter(k => s[k] !== undefined).map(k => [k, s[k]]));
  const pcm = window.room && window.room.engine && window.room.engine.pcManager;
  const out = {now: performance.now(), filter: window.netemFilter, pcs: {}};
  for (const role of ['publisher', 'subscriber']) {
    const t = pcm && pcm[role];
    if (!t) continue;
    let stats;
    try { stats = await t.getStats(); } catch (e) { continue; }
    const byId = new Map();
    stats.forEach(s => byId.set(s.id, s));
    const pc = {inbound: [], outbound: [], remoteInbound: [], pairs: [], transport: null};
    stats.forEach(s => {
      if (s.type === 'inbound-rtp') {
        pc.inbound.push(pick(s, ['kind', 'framesDecoded', 'framesDropped', 'framesReceived', 'frameWidth', 'frameHeight',
          'framesPerSecond', 'freezeCount', 'totalFreezesDuration', 'pauseCount', 'totalPausesDuration', 'packetsReceived',
          'packetsLost', 'nackCount', 'pliCount', 'firCount', 'jitter', 'jitterBufferDelay', 'jitterBufferEmittedCount',
          'jitterBufferTargetDelay', 'keyFramesDecoded', 'bytesReceived', 'retransmittedPacketsReceived',
          'totalSamplesReceived', 'concealedSamples', 'concealmentEvents']));
      } else if (s.type === 'outbound-rtp') {
        pc.outbound.push(pick(s, ['kind', 'rid', 'active', 'frameWidth', 'frameHeight', 'framesPerSecond', 'framesSent',
          'bytesSent', 'packetsSent', 'retransmittedPacketsSent', 'targetBitrate', 'qualityLimitationReason', 'nackCount',
          'pliCount']));
      } else if (s.type === 'remote-inbound-rtp') {
        pc.remoteInbound.push(pick(s, ['kind', 'roundTripTime', 'fractionLost', 'packetsLost', 'jitter']));
      } else if (s.type === 'candidate-pair') {
        const l = byId.get(s.localCandidateId) || {};
        const r = byId.get(s.remoteCandidateId) || {};
        pc.pairs.push({
          ...pick(s, ['id', 'state', 'nominated', 'packetsSent', 'packetsReceived', 'bytesSent', 'bytesReceived',
            'requestsSent', 'responsesReceived', 'currentRoundTripTime', 'availableOutgoingBitrate']),
          local: {address: loopback(l.address), port: l.port, protocol: l.protocol, type: l.candidateType, relatedPort: l.relatedPort},
          remote: {address: loopback(r.address), port: r.port, protocol: r.protocol, type: r.candidateType},
        });
      } else if (s.type === 'transport') {
        pc.transport = pick(s, ['packetsSent', 'packetsReceived', 'bytesSent', 'bytesReceived', 'selectedCandidatePairId',
          'selectedCandidatePairChanges', 'iceState', 'dtlsState']);
      }
    });
    out.pcs[role] = pc;
  }
  return out;
}

// ---- What a sample means ----

const sum = xs => xs.reduce((a, b) => a + (b || 0), 0);
const median = xs => {
  const v = xs.filter(x => x !== undefined && x !== null).sort((a, b) => a - b);
  return v.length ? v[Math.floor(v.length / 2)] : null;
};
const mode = xs => {
  const n = new Map();
  for (const x of xs) n.set(x, (n.get(x) || 0) + 1);
  return [...n.entries()].sort((a, b) => b[1] - a[1] || a[0] - b[0]).map(e => e[0])[0] ?? null;
};
const round = (x, d = 0) => x === null || x === undefined || Number.isNaN(x) ? null : Math.round(x * 10 ** d) / 10 ** d;
const delta = (a, b, k) => (b && b[k] !== undefined && a && a[k] !== undefined) ? b[k] - a[k] : null;
// LiveKit's SDK may carry everything on one peer connection (then it is the publisher's):
// the streams are looked for on both.
const pcsOf = s => Object.values(s.pcs);
const video = s => pcsOf(s).flatMap(pc => pc.inbound).filter(i => i.kind === 'video')
  .sort((a, b) => (b.framesDecoded || 0) - (a.framesDecoded || 0))[0] || null;
const audio = s => pcsOf(s).flatMap(pc => pc.inbound).find(i => i.kind === 'audio') || null;
const selectedPair = pc => pc && pc.transport && pc.pairs.find(p => p.id === pc.transport.selectedCandidatePairId);
// The peer connection that carries a kind of stream in a direction.
const pcWith = (s, list, kind) => pcsOf(s).find(pc => pc[list].some(x => x.kind === kind)) || null;

// The layer a height is, by the publisher's layers at that moment.
function layerName(h, publisherRow) {
  if (!h) return null;
  const rid = publisherRow && Object.entries(publisherRow.layers).find(([, l]) => l.h === h);
  return rid ? `${h}p ${rid[0]}` : `${h}p`;
}

// Per second, from two consecutive raw samples of the subscriber's page.
function subscriberRow(t, prev, cur) {
  const a = video(prev), b = video(cur), aa = audio(prev), ba = audio(cur);
  const dt = (cur.now - prev.now) / 1000;
  const emitted = delta(a, b, 'jitterBufferEmittedCount');
  const pair = selectedPair(pcWith(cur, 'inbound', 'video'));
  return {
    t,
    w: b ? b.frameWidth ?? null : null,
    h: b ? b.frameHeight ?? null : null,
    fps: round(delta(a, b, 'framesDecoded') / dt, 1),
    dropped: delta(a, b, 'framesDropped'),
    freezes: delta(a, b, 'freezeCount'),
    freezeMs: round(delta(a, b, 'totalFreezesDuration') * 1000),
    pauses: delta(a, b, 'pauseCount'),
    lost: delta(a, b, 'packetsLost'),
    nack: delta(a, b, 'nackCount'),
    pli: delta(a, b, 'pliCount'),
    kbps: round(delta(a, b, 'bytesReceived') * 8 / dt / 1000),
    jbMs: emitted ? round(delta(a, b, 'jitterBufferDelay') / emitted * 1000) : null,
    jitterMs: b && b.jitter !== undefined ? round(b.jitter * 1000, 1) : null,
    rttMs: pair && pair.currentRoundTripTime !== undefined ? round(pair.currentRoundTripTime * 1000, 1) : null,
    audioLost: delta(aa, ba, 'packetsLost'),
    audioConcealedMs: ba && ba.concealedSamples !== undefined ? round(delta(aa, ba, 'concealedSamples') / 48) : null,
  };
}

function publisherRow(t, prev, cur) {
  const pc = pcWith(cur, 'outbound', 'video');
  const dt = (cur.now - prev.now) / 1000;
  const layers = {};
  const earlier = pcsOf(prev).flatMap(p => p.outbound);
  for (const o of (pc ? pc.outbound : []).filter(o => o.kind === 'video')) {
    const before = earlier.find(x => x.kind === 'video' && x.rid === o.rid);
    layers[o.rid || '-'] = {
      w: o.frameWidth ?? null, h: o.frameHeight ?? null, fps: o.framesPerSecond ?? 0, active: o.active,
      kbps: round(delta(before, o, 'bytesSent') * 8 / dt / 1000), limit: o.qualityLimitationReason,
    };
  }
  const pair = selectedPair(pc);
  const ri = pc ? pc.remoteInbound.filter(r => r.kind === 'video') : [];
  return {
    t, layers,
    outKbps: pair && pair.availableOutgoingBitrate !== undefined ? round(pair.availableOutgoingBitrate / 1000) : null,
    rttMs: ri.length ? round(Math.max(...ri.map(r => r.roundTripTime || 0)) * 1000) : null,
  };
}

const NON_STUN = ['dtls', 'rtp', 'rtcp', 'other'];
// The relay's counters by client and direction.
function relayTotals(stats) {
  const out = {};
  for (const m of stats.mappings) {
    for (const dir of ['up', 'down']) {
      const k = `${m.tag || '?'} ${dir}`;
      const c = m[dir];
      const o = out[k] || (out[k] = {in: 0, stunIn: 0, out: 0, loss: 0, queue: 0, blackout: 0});
      o.in += sum(NON_STUN.map(n => c.in[n]));
      o.stunIn += c.in.stun;
      o.out += sum(NON_STUN.map(n => c.out[n]));
      o.loss += c.loss; o.queue += c.queue; o.blackout += c.blackout;
    }
  }
  return out;
}

function relayRow(t, prev, cur) {
  const a = relayTotals(prev), b = relayTotals(cur), row = {t};
  for (const [k, v] of Object.entries(b)) {
    const p = a[k] || {};
    row[k] = Object.fromEntries(Object.entries(v).filter(([n]) => n !== 'stunIn').map(([n, x]) => [n, x - (p[n] || 0)]));
  }
  return row;
}

// What the browsers say they sent and received, against what the relay counted.
function clientCounts(sample) {
  let sent = 0, received = 0;
  const remotes = new Set();
  for (const pc of Object.values(sample.pcs)) {
    const used = pc.pairs.filter(p => (p.packetsSent || 0) + (p.packetsReceived || 0) > 0);
    for (const p of used) remotes.add(`${p.remote.protocol} ${p.remote.address}:${p.remote.port}`);
    if (pc.transport && pc.transport.packetsSent !== undefined) {
      sent += pc.transport.packetsSent; received += pc.transport.packetsReceived;
    } else {
      sent += sum(used.map(p => p.packetsSent)); received += sum(used.map(p => p.packetsReceived));
    }
  }
  return {sent, received, remotes: [...remotes]};
}

const SLACK = 50;
function proof(before, relay, after) {
  const b = Object.values(before).map(clientCounts), a = Object.values(after).map(clientCounts);
  const totals = Object.values(relayTotals(relay));
  const relayUp = sum(Object.entries(relayTotals(relay)).filter(([k]) => k.endsWith(' up')).map(([, v]) => v.in));
  const relayDown = sum(Object.entries(relayTotals(relay)).filter(([k]) => k.endsWith(' down')).map(([, v]) => v.out));
  const remotes = [...new Set(a.flatMap(c => c.remotes))].sort();
  const p = {
    // Chrome's counts are read before and after the relay's. Everything a browser had sent
    // must have reached the relay, and everything it had received must have come from the
    // relay; the relay may be ahead of the browsers by what passed between the readings (and
    // Chrome's statistics are cached for some milliseconds), but not by more than SLACK.
    up: {clientsSentBefore: sum(b.map(c => c.sent)), relayReceived: relayUp, clientsSentAfter: sum(a.map(c => c.sent))},
    down: {clientsReceivedBefore: sum(b.map(c => c.received)), relayDelivered: relayDown, clientsReceivedAfter: sum(a.map(c => c.received))},
    remoteAddressesUsed: remotes,
    relayStunReceived: sum(totals.map(v => v.stunIn)),
    unmapped: relay.unmapped,
    untaggedMedia: sum(Object.entries(relayTotals(relay)).filter(([k]) => k.startsWith('? ')).map(([, v]) => v.in + v.out)),
  };
  p.ok = remotes.length > 0 && remotes.every(r => r === `udp ${RELAY_FRONT}`) &&
    p.up.clientsSentBefore <= p.up.relayReceived && p.up.relayReceived <= p.up.clientsSentAfter + SLACK &&
    p.down.clientsReceivedBefore <= p.down.relayDelivered && p.down.relayDelivered <= p.down.clientsReceivedAfter + SLACK &&
    p.unmapped === 0;
  return p;
}

function summarise(rows, pubRows, imp) {
  const at = t => pubRows.find(r => r.t === t) || pubRows[pubRows.length - 1];
  const win = (a, b) => rows.filter(r => r.t > a && r.t <= b);
  const label = (h, among) => h ? ((among.find(r => r.h === h) || {}).layer || `${h}p`) : 'none';
  const before = imp ? win(imp.from - 5, imp.from) : win(0, DURATION);
  const refH = median(before.map(r => r.h)), refFps = median(before.map(r => r.fps));
  const ok = r => r.h !== null && r.h >= refH && r.fps !== null && r.fps >= 0.8 * refFps;
  const s = {
    reference: {h: refH, fps: refFps, layer: layerName(refH, at(imp ? imp.from : 1))},
    freezes: sum(rows.map(r => r.freezes)),
    freezeS: round(sum(rows.map(r => r.freezeMs)) / 1000, 2),
    pauses: sum(rows.map(r => r.pauses)),
    framesDropped: sum(rows.map(r => r.dropped)),
    lost: sum(rows.map(r => r.lost)),
    nack: sum(rows.map(r => r.nack)),
    pli: sum(rows.map(r => r.pli)),
    audioConcealedMs: sum(rows.map(r => r.audioConcealedMs)),
  };
  const after = rows.filter(r => r.t > DURATION - 5);
  s.after = {layer: label(median(after.map(r => r.h)), after), fps: median(after.map(r => r.fps)),
    rttMs: median(after.map(r => r.rttMs))};
  s.before = {rttMs: median(before.map(r => r.rttMs)), jbMs: median(before.map(r => r.jbMs))};
  if (imp) {
    // During: the seconds whose interval overlaps the impairment.
    const during = win(imp.from, imp.to);
    const lowest = Math.min(...during.map(r => r.h || 0));
    const usual = mode(during.map(r => r.h || 0));
    s.during = {
      layer: label(usual, during), lowest: label(lowest, during),
      fps: round(sum(during.map(r => r.fps)) / during.length, 1), minFps: Math.min(...during.map(r => r.fps || 0)),
      rttMs: median(during.map(r => r.rttMs)), jbMs: median(during.map(r => r.jbMs)),
    };
    // Recovered: from the first of three seconds in a row back at the layer and at 80% of
    // the frame rate from before. 0: it was there when the impairment ended.
    const from = rows.findIndex(r => r.t >= imp.to);
    let rec = null;
    for (let i = from; i >= 0 && i + 2 < rows.length; i++) {
      if (ok(rows[i]) && ok(rows[i + 1]) && ok(rows[i + 2])) { rec = Math.max(0, rows[i].t - imp.to); break; }
    }
    s.recoverS = rec;
    s.degraded = during.some(r => !ok(r)) || rows.slice(from, from + 3).some(r => !ok(r));
  }
  return s;
}

function summaryLine(scenario, s, extra) {
  const parts = [`${target} ${scenario}: freezes ${s.freezes} (${s.freezeS} s)`];
  if (s.during) {
    parts.push(`layer ${s.reference.layer} -> ${s.during.layer} (lowest ${s.during.lowest}) -> ${s.after.layer}`);
    parts.push(s.recoverS === null ? 'not recovered' : `recovered in ${s.recoverS} s`);
    parts.push(`fps ${s.reference.fps}/${s.during.fps}/${s.after.fps}`);
    parts.push(`rtt ${s.before.rttMs}/${s.during.rttMs}/${s.after.rttMs} ms`);
  } else {
    parts.push(`layer ${s.reference.layer}`, `fps ${s.reference.fps}`, `rtt ${s.before.rttMs} ms`);
  }
  parts.push(`lost ${s.lost}, nack ${s.nack}, pli ${s.pli}`);
  return parts.join(', ') + (extra ? `; ${extra}` : '');
}

// ---- One scenario ----

async function runScenario(relayBin, scenarioName) {
  const sc = scenarios[scenarioName];
  const imp = sc.set.length ? {from: sc.at, to: sc.at + sc.for, commands: sc.set} : null;
  const relay = await startRelay(relayBin);
  const browser = await puppeteer.launch({
    executablePath: chrome,
    headless: true,
    args: [
      '--use-fake-device-for-media-stream',
      '--use-fake-ui-for-media-stream',
      '--allow-loopback-in-peer-connection',
      '--autoplay-policy=no-user-gesture-required',
      // Two pages in one browser: neither may be slowed down for being in the background.
      '--disable-background-timer-throttling',
      '--disable-renderer-backgrounding',
      '--disable-backgrounding-occluded-windows',
    ],
  });
  const room = 'netem-' + crypto.randomBytes(4).toString('hex');
  const pages = {};
  try {
    for (const who of ['alice', 'bob']) {
      const page = pages[who] = await browser.newPage();
      page.on('pageerror', e => console.log(`  [${who}] page error: ${e.message}`));
      page.on('console', m => { if (m.type() === 'error' || process.env.VERBOSE) console.log(`  [${who}] ${m.text()}`); });
      await page.evaluateOnNewDocument(filterCandidates, ADVERTISED, RELAY);
      await page.goto(`http://127.0.0.1:${pagePort}/`);
    }
    const {alice, bob} = pages;
    await alice.evaluate((u, t) => window.join(u, t), wsUrl, token('alice', room));
    await alice.evaluate(() => window.publishCameraAndMicrophone());
    await bob.bringToFront();
    await bob.evaluate((u, t) => window.join(u, t), wsUrl, token('bob', room));
    const subscribed = await until(async () =>
      (await bob.evaluate(() => window.log.filter(e => e.event === 'TrackSubscribed').length)) >= 2, 15000);
    if (!subscribed) throw new Error('bob was not subscribed to alice\'s tracks');
    await bob.evaluate(() => window.setVideoSize(1280, 720));

    // Every client port the relay sees belongs to a participant.
    const tag = async () => {
      for (const [who, page] of Object.entries(pages)) {
        const s = await page.evaluate(pageSample);
        const ports = new Set();
        for (const pc of Object.values(s.pcs)) for (const p of pc.pairs) {
          if (p.local.port) ports.add(p.local.port);
          if (p.local.relatedPort) ports.add(p.local.relatedPort);
        }
        for (const port of ports) await relay.control.send(`tag ${port} ${who}`);
      }
    };

    // Warm up: until bob gets the highest layer at a good frame rate.
    const warmStart = Date.now();
    const reached = await until(async () => {
      await tag();
      const v = video(await bob.evaluate(pageSample));
      return v && v.frameWidth >= 1280 && v.framesPerSecond >= 20;
    }, WARMUP_MS, 1000);
    const warmup = {seconds: round((Date.now() - warmStart) / 1000, 1), reachedHighestLayer: !!reached};
    await sleep(3000);
    await tag();

    // The timeline.
    const raw = {alice: [], bob: []}, relayRaw = [], loads = [];
    const pageT0 = {};
    for (const [who, page] of Object.entries(pages)) pageT0[who] = await page.evaluate(() => performance.now());
    const logStart = {};
    for (const [who, page] of Object.entries(pages)) logStart[who] = await page.evaluate(() => window.log.length);
    const t0 = Date.now();
    for (let s = 0; s <= DURATION; s++) {
      const wait = t0 + s * 1000 - Date.now();
      if (wait > 0) await sleep(wait);
      if (imp && s === imp.from) for (const c of imp.commands) await relay.control.send(c);
      if (imp && s === imp.to) await relay.control.send('clear');
      const samples = await Promise.all(Object.values(pages).map(p => p.evaluate(pageSample)));
      relayRaw.push(await relay.control.stats());
      loads.push(os.loadavg()[0]);
      Object.keys(pages).forEach((who, i) => raw[who].push({t: s, ...samples[i]}));
      if (s % 5 === 4) await tag();
    }

    // The proof: Chrome's counts, the relay's, Chrome's again.
    const pb = {}, pa = {};
    for (const [who, page] of Object.entries(pages)) pb[who] = await page.evaluate(pageSample);
    const relayEnd = await relay.control.stats();
    for (const [who, page] of Object.entries(pages)) pa[who] = await page.evaluate(pageSample);
    const path_ = {relay: RELAY_FRONT, server: `127.0.0.1:${MEDIA_PORT}`, proof: proof(pb, relayEnd, pa)};

    const events = {};
    for (const [who, page] of Object.entries(pages)) {
      events[who] = (await page.evaluate(n => window.log.slice(n), logStart[who]))
        .filter(e => /Quality|StreamState|Reconnect|Disconnected|Resum|Track(Un)?(Subscribed|Muted|Published)/.test(e.event))
        .map(({t, ...e}) => ({t: round((t - pageT0[who]) / 1000, 1), ...e}));
    }
    await bob.evaluate(() => window.leave()).catch(() => {});
    await alice.evaluate(() => window.leave()).catch(() => {});

    const rows = [], pubRows = [], relayRows = [];
    for (let i = 1; i < raw.bob.length; i++) {
      rows.push(subscriberRow(raw.bob[i].t, raw.bob[i - 1], raw.bob[i]));
      pubRows.push(publisherRow(raw.alice[i].t, raw.alice[i - 1], raw.alice[i]));
      relayRows.push(relayRow(raw.bob[i].t, relayRaw[i - 1], relayRaw[i]));
    }
    rows.forEach(r => { r.layer = layerName(r.h, pubRows.find(p => p.t === r.t)); });
    const summary = summarise(rows, pubRows, imp);
    // What Chrome did not report, of what is asked for.
    const lastVideo = video(raw.bob[raw.bob.length - 1]) || {};
    const missing = ['freezeCount', 'totalFreezesDuration', 'pauseCount', 'framesDropped', 'jitterBufferDelay', 'nackCount', 'pliCount']
      .filter(k => lastVideo[k] === undefined);
    if (rows.every(r => r.rttMs === null)) missing.push('subscriber currentRoundTripTime');
    const cpuLimited = pubRows.some(r => Object.values(r.layers).some(l => l.limit === 'cpu'));
    const notes = [];
    if (!path_.proof.ok) notes.push('PATH NOT PROVEN');
    if (!warmup.reachedHighestLayer) notes.push('highest layer not reached in warm-up');
    if (cpuLimited) notes.push('publisher cpu-limited');
    if (missing.length) notes.push('not reported: ' + missing.join(' '));
    // A machine busy with other work makes the browsers and the relay late: such a run says
    // little about the server.
    const machine = {cpus: os.cpus().length, load1Max: round(Math.max(...loads), 1),
      load1Mean: round(sum(loads) / loads.length, 1)};
    if (machine.load1Max > machine.cpus || relayEnd.late_us.max > 10000) {
      notes.push(`MACHINE BUSY: load up to ${machine.load1Max} on ${machine.cpus} cpus, relay up to ${round(relayEnd.late_us.max / 1000)} ms late`);
    }
    const line = summaryLine(scenarioName, summary, notes.join('; '));
    const mappings = relayEnd.mappings.map(m => ({
      tag: m.tag || null, clientPort: Number(m.client.split(':').pop()),
      clientLoopback: m.client.startsWith('127.'), relayPort: m.port, up: m.up, down: m.down,
    }));
    const result = {
      target, scenario: scenarioName, about: sc.about, at: new Date().toISOString(), seed: SEED, duration: DURATION,
      impairment: imp, warmup, summary: line, subscribers: {bob: {summary, samples: rows}},
      publishers: {alice: {samples: pubRows}},
      relay: {lateUs: relayEnd.late_us, mappings, samples: relayRows},
      path: path_, filter: {alice: pa.alice.filter, bob: pa.bob.filter}, missing, machine, events,
    };
    fs.mkdirSync(path.join(__dirname, 'out'), {recursive: true});
    fs.writeFileSync(path.join(__dirname, 'out', `${target}-${scenarioName}.json`), format(result));
    return line;
  } finally {
    await browser.close().catch(() => {});
    relay.stop();
  }
}

(async () => {
  const relayBin = buildRelay();
  const pageServer = await servePage();
  let failed = false;
  for (const n of names) {
    try {
      const line = await runScenario(relayBin, n);
      console.log(line);
      if (line.includes('PATH NOT PROVEN')) failed = true;
    } catch (e) {
      console.error(`${target} ${n}: broke off: ${e.message}`);
      failed = true;
    }
  }
  pageServer.close();
  process.exit(failed ? 1 : 0);
})();
