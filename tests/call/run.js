// The scripted call: two participants in a headless browser, each with LiveKit's JavaScript
// SDK, against a server that speaks LiveKit's protocol. It writes down what the two clients
// observed, so that the same call against LiveKit and against musivum can be compared.
//
//   node run.js <name> <ws url> <api key> <api secret>
//
// <name> names the output: out/<name>.json. The browser is $CHROME, or the first of
// google-chrome, chromium, chromium-browser found.
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');
const {execSync} = require('child_process');
const puppeteer = require('puppeteer-core');

const [name, wsUrl, apiKey, apiSecret] = process.argv.slice(2);
if (!apiSecret) {
  console.error('usage: node run.js <name> <ws url> <api key> <api secret>');
  process.exit(2);
}
const chrome = process.env.CHROME ||
  execSync('command -v google-chrome || command -v chromium || command -v chromium-browser', {shell: '/bin/sh'}).toString().trim();
const pagePort = Number(process.env.PAGE_PORT || 39510);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// An access token as LiveKit's server SDKs make them: a JWT signed with the API secret.
function token(identity, room, grants = {}) {
  const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const body = b64({alg: 'HS256', typ: 'JWT'}) + '.' + b64({
    iss: apiKey, sub: identity, name: identity, nbf: now - 10, exp: now + 3600,
    video: {room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: true, ...grants},
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

async function until(fn, ms, step = 200) {
  const end = Date.now() + ms;
  let last;
  while (Date.now() < end) {
    last = await fn();
    if (last) return last;
    await sleep(step);
  }
  return last;
}

// What was observed, by name. The comparison between servers is over these.
const observed = {};
const failures = [];
function observe(key, value, expectation) {
  observed[key] = value;
  let ok = true;
  if (typeof expectation === 'function') ok = !!expectation(value);
  else if (expectation !== undefined) ok = JSON.stringify(value) === JSON.stringify(expectation);
  if (!ok) failures.push(key);
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${key}: ${JSON.stringify(value)}`);
  return ok;
}
const events = (log, name, match = {}) =>
  log.filter(e => e.event === name && Object.entries(match).every(([k, v]) => JSON.stringify(e[k]) === JSON.stringify(v)));
const has = (log, name, match) => events(log, name, match).length > 0;

(async () => {
  const pageServer = await servePage();
  const browser = await puppeteer.launch({
    executablePath: chrome,
    headless: true,
    args: [
      '--use-fake-device-for-media-stream',
      '--use-fake-ui-for-media-stream',
      '--allow-loopback-in-peer-connection',
      '--autoplay-policy=no-user-gesture-required',
    ],
  });
  const room = 'call-' + crypto.randomBytes(4).toString('hex');
  const pageUrl = `http://127.0.0.1:${pagePort}/`;
  let alice, bob;
  const logOf = page => page.evaluate(() => window.log);
  try {
    alice = await browser.newPage();
    bob = await browser.newPage();
    for (const [who, page] of [['alice', alice], ['bob', bob]]) {
      page.on('pageerror', e => console.log(`  [${who}] page error: ${e.message}`));
      page.on('console', m => { if (m.type() === 'error' || process.env.VERBOSE) console.log(`  [${who}] ${m.text()}`); });
      await page.goto(pageUrl);
    }

    // Alice joins and publishes.
    const a = await alice.evaluate((u, t) => window.join(u, t), wsUrl, token('alice', room));
    observe('alice joined', {name: a.name === room, identity: a.identity, sidPrefix: a.sidPrefix, others: a.remoteParticipants, canPublish: a.canPublish},
      {name: true, identity: 'alice', sidPrefix: 'PA_', others: [], canPublish: true});
    observe('server as alice sees it', {version: a.serverVersion, protocol: a.serverProtocol, edition: a.serverEdition});
    const pubs = await alice.evaluate(() => window.publishCameraAndMicrophone());
    observe('alice published', pubs.map(p => `${p.source} ${p.kind} ${p.sidPrefix} simulcast=${p.simulcasted} ${p.mimeType}`).sort(),
      v => v.length === 2 && v.every(x => x.includes('TR_')));

    // Bob joins and is subscribed to both.
    await bob.bringToFront();
    const b = await bob.evaluate((u, t) => window.join(u, t), wsUrl, token('bob', room));
    observe('bob joined', {identity: b.identity, others: b.remoteParticipants}, {identity: 'bob', others: ['alice']});
    await until(async () => has(await logOf(alice), 'ParticipantConnected', {identity: 'bob'}), 5000);
    observe('alice sees bob connect', has(await logOf(alice), 'ParticipantConnected', {identity: 'bob'}), true);
    await until(async () => events(await logOf(bob), 'TrackSubscribed').length >= 2, 10000);
    observe('bob subscribed to', events(await logOf(bob), 'TrackSubscribed').map(e => `${e.identity} ${e.source} ${e.kind}`).sort(),
      ['alice camera video', 'alice microphone audio']);

    // Media arrives.
    const video = await until(async () => {
      const v = (await bob.evaluate(() => window.inbound('video')))[0];
      return v && v.framesDecoded > 30 ? v : null;
    }, 20000);
    observe('bob decodes alice\'s video', !!video, true);
    const audio = await until(async () => {
      const x = (await bob.evaluate(() => window.inbound('audio')))[0];
      return x && x.packetsReceived > 100 ? x : null;
    }, 10000);
    observe('bob receives alice\'s audio', !!audio, true);

    // The size of the video element decides the layer (adaptive stream): small, then large.
    const small = await until(async () => {
      const v = (await bob.evaluate(() => window.inbound('video')))[0];
      return v && v.frameWidth && v.frameWidth <= 640 ? v : null;
    }, 15000);
    observe('a small video element gets a low layer', small ? small.frameWidth : (await bob.evaluate(() => window.inbound('video')))[0], w => w === 320 || w === 640);
    await bob.evaluate(() => window.setVideoSize(1280, 720));
    const large = await until(async () => {
      const v = (await bob.evaluate(() => window.inbound('video')))[0];
      return v && v.frameWidth === 1280 ? v : null;
    }, 30000);
    observe('a large video element gets the high layer', large ? large.frameWidth : (await bob.evaluate(() => window.inbound('video')))[0], 1280);
    observe('alice sends three simulcast layers', (await alice.evaluate(() => window.outbound())).filter(o => o.active && o.framesSent > 0).map(o => o.rid).sort(),
      v => v.length === 3);

    // Data messages, both kinds.
    await alice.evaluate(() => window.sendData('hello reliably', true, 'chat'));
    await alice.evaluate(() => window.sendData('hello lossily', false, 'chat'));
    await until(async () => events(await logOf(bob), 'DataReceived').length >= 2, 5000);
    observe('bob received data', events(await logOf(bob), 'DataReceived').map(e => `${e.identity} ${e.kind} ${e.topic} ${e.text}`).sort(),
      ['alice LOSSY chat hello lossily', 'alice RELIABLE chat hello reliably']);

    // Speaking: the fake microphone beeps.
    await until(async () => events(await logOf(bob), 'ActiveSpeakersChanged').some(e => e.identities.includes('alice')), 15000);
    observe('bob sees alice speak', events(await logOf(bob), 'ActiveSpeakersChanged').some(e => e.identities.includes('alice')), true);

    // Mute and unmute.
    await alice.evaluate(() => window.setMicrophoneMuted(true));
    await until(async () => has(await logOf(bob), 'TrackMuted', {identity: 'alice', source: 'microphone'}), 5000);
    observe('bob sees alice mute', has(await logOf(bob), 'TrackMuted', {identity: 'alice', source: 'microphone'}), true);
    await alice.evaluate(() => window.setMicrophoneMuted(false));
    await until(async () => has(await logOf(bob), 'TrackUnmuted', {identity: 'alice', source: 'microphone'}), 5000);
    observe('bob sees alice unmute', has(await logOf(bob), 'TrackUnmuted', {identity: 'alice', source: 'microphone'}), true);

    // Connection quality is reported: to each participant its own.
    await until(async () => has(await logOf(bob), 'ConnectionQualityChanged', {identity: 'bob'}) && has(await logOf(alice), 'ConnectionQualityChanged', {identity: 'alice'}), 30000);
    observe('each is told its own connection quality', {
      alice: events(await logOf(alice), 'ConnectionQualityChanged', {identity: 'alice'}).map(e => e.quality).slice(-1)[0],
      bob: events(await logOf(bob), 'ConnectionQualityChanged', {identity: 'bob'}).map(e => e.quality).slice(-1)[0],
    }, {alice: 'excellent', bob: 'excellent'});

    // No freezes on a clean network.
    const end = (await bob.evaluate(() => window.inbound('video')))[0] || {};
    observe('video at the end', {freezes: end.freezeCount, lost: end.packetsLost, decoding: end.framesPerSecond > 10}, v => v.decoding);

    // Unpublish.
    await alice.evaluate(() => window.unpublishCamera());
    await until(async () => has(await logOf(bob), 'TrackUnpublished', {identity: 'alice', source: 'camera'}), 5000);
    observe('bob sees alice unpublish the camera', has(await logOf(bob), 'TrackUnsubscribed', {identity: 'alice', source: 'camera'}) && has(await logOf(bob), 'TrackUnpublished', {identity: 'alice', source: 'camera'}), true);

    // Leave.
    await bob.evaluate(() => window.leave());
    await until(async () => has(await logOf(alice), 'ParticipantDisconnected', {identity: 'bob'}), 5000);
    observe('alice sees bob leave', has(await logOf(alice), 'ParticipantDisconnected', {identity: 'bob'}), true);
    observe('bob\'s own disconnect reason', (events(await logOf(bob), 'Disconnected')[0] || {}).reason, 'CLIENT_INITIATED');
    await alice.evaluate(() => window.leave());

    const result = {
      target: name, url: wsUrl, at: new Date().toISOString(), failures, observed,
      events: {
        alice: (await logOf(alice)).map(({t, ...e}) => e),
        bob: (await logOf(bob)).map(({t, ...e}) => e),
      },
    };
    fs.mkdirSync(path.join(__dirname, 'out'), {recursive: true});
    fs.writeFileSync(path.join(__dirname, 'out', name + '.json'), JSON.stringify(result, null, 1));
    console.log(`\n${Object.keys(observed).length - failures.length} of ${Object.keys(observed).length} as expected; written to out/${name}.json`);
  } catch (e) {
    console.error('the call broke off:', e.message);
    for (const [who, page] of [['alice', alice], ['bob', bob]]) {
      if (page) console.error(`  [${who}] events so far: ${JSON.stringify((await logOf(page).catch(() => [])).map(e => e.event))}`);
    }
    failures.push('the call broke off');
  } finally {
    await browser.close();
    pageServer.close();
  }
  process.exit(failures.length ? 1 : 0);
})();
