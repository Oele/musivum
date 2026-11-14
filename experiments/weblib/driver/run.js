// Drives the experiment: two pages of a headless browser against a forwarder, one publishing
// and one receiving, and a list of checks on what the browser and the forwarder each saw.
//
//   node run.js [base url]      (default http://127.0.0.1:39400)
//
// The browser is $CHROME, or the first of google-chrome, chromium, chromium-browser found.
const puppeteer = require('puppeteer-core');

const base = process.argv[2] || 'http://127.0.0.1:39400';
const {execSync} = require('child_process');
const chrome = process.env.CHROME ||
  execSync('command -v google-chrome || command -v chromium || command -v chromium-browser', {shell: '/bin/sh'}).toString().trim();
const sleep = ms => new Promise(r => setTimeout(r, ms));

const results = [];
function check(name, ok, detail) {
  results.push({name, ok: !!ok, detail});
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail !== undefined ? '  — ' + detail : ''}`);
}
async function serverStats() { return (await fetch(base + '/stats')).json(); }
async function control(body) {
  const res = await fetch(base + '/control', {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(body)});
  if (!res.ok) throw new Error('control: ' + res.status);
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
const inboundVideo = stats => stats.find(s => s.type === 'inbound-rtp' && s.kind === 'video');
const inboundAudio = stats => stats.find(s => s.type === 'inbound-rtp' && s.kind === 'audio');

(async () => {
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
  try {
    const pub = await browser.newPage();
    pub.on('console', m => console.log('  [publisher page]', m.text()));
    await pub.goto(base);
    await pub.evaluate(() => window.publish());
    const pubState = await until(() => pub.evaluate(() => window.pubPc.connectionState === 'connected'), 10000);
    check('publisher connects (ICE lite server, DTLS, one shared UDP port)', pubState,
      await pub.evaluate(() => window.pubPc.connectionState + ' / ice ' + window.pubPc.iceConnectionState));

    const sub = await browser.newPage();
    sub.on('console', m => console.log('  [subscriber page]', m.text()));
    await sub.goto(base);
    await sub.evaluate(() => window.subscribe());
    const subState = await until(() => sub.evaluate(() => window.subPc.connectionState === 'connected'), 10000);
    check('subscriber connects on an offer made by the server', subState,
      await sub.evaluate(() => window.subPc.connectionState + ' / ice ' + window.subPc.iceConnectionState));

    // Media arrives and decodes.
    const decoded = await until(async () => {
      const v = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
      return v && v.framesDecoded > 30 ? v : null;
    }, 20000);
    check('forwarded video decodes at the subscriber', decoded, decoded && `${decoded.framesDecoded} frames, ${decoded.frameWidth}x${decoded.frameHeight}`);
    const audio = await until(async () => {
      const a = inboundAudio(await sub.evaluate(() => window.rtcStats(window.subPc)));
      return a && a.packetsReceived > 50 ? a : null;
    }, 10000);
    check('forwarded audio arrives at the subscriber', audio, audio && `${audio.packetsReceived} packets`);

    // All three simulcast layers reach the program, each known by its rid.
    const layers = await until(async () => {
      const s = await serverStats();
      const rids = new Set(Object.keys(s.rtp_in).map(k => (k.match(/rid (\w)/) || [])[1]).filter(Boolean));
      return rids.size === 3 ? s : null;
    }, 20000);
    let s = layers || await serverStats();
    check('all three simulcast layers are handed over with their rid', layers, Object.keys(s.rtp_in).join(' | '));
    check('header extensions of incoming RTP are handed over as they are', Object.keys(s.rtp_in_extensions).length > 0, JSON.stringify(s.rtp_in_extensions));
    check('RTCP from the publisher is handed over (sender reports)', s.rtcp_in['Publisher SR'] > 0, JSON.stringify(s.rtcp_in));
    check('RTCP from the subscriber is handed over (receiver reports)', s.rtcp_in['Subscriber RR'] > 0);
    check('transport-wide feedback from the subscriber is handed over, for sequence numbers the program set itself', s.twcc_feedback_packets > 0, `${s.twcc_feedback_packets} packets, ${s.twcc_feedback_statuses} statuses`);
    check('RTCP written by the program reaches the publisher (REMB; PLI gave a key frame)', s.remb_sent > 0 && s.pli_passed_on > 0, `remb ${s.remb_sent}, pli ${s.pli_passed_on}`);

    // Switch to the highest layer.
    await control({rid: 'f'});
    const big = await until(async () => {
      const v = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
      return v && v.frameWidth === 1280 ? v : null;
    }, 20000);
    let switchDetail = big && `${big.frameWidth}x${big.frameHeight}`;
    if (!big) {
      // Why not: is the publisher sending that layer at all, and is the forwarder getting it?
      const outbound = async () => (await pub.evaluate(() => window.rtcStats(window.pubPc)))
        .filter(x => x.type === 'outbound-rtp' && x.kind === 'video')
        .map(x => ({rid: x.rid, active: x.active, framesSent: x.framesSent, keyFramesEncoded: x.keyFramesEncoded, pliCount: x.pliCount, limit: x.qualityLimitationReason, width: x.frameWidth}));
      const o1 = await outbound(); const s1 = await serverStats();
      await sleep(2000);
      const o2 = await outbound(); const s2 = await serverStats();
      const v2 = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
      switchDetail = JSON.stringify({publisherBefore: o1, publisherAfter: o2, forwarderRtpIn: [s1.rtp_in, s2.rtp_in], forwarded: [s1.rtp_forwarded, s2.rtp_forwarded], pli: [s1.pli_passed_on, s2.pli_passed_on], rtcpIn: s2.rtcp_in, subscriber: {w: v2.frameWidth, frames: v2.framesDecoded, key: v2.keyFramesDecoded, pli: v2.pliCount, received: v2.packetsReceived}});
    }
    check('switching the forwarded layer works (rewritten sequence numbers, key frame by PLI)', big, switchDetail);

    // Loss: the subscriber NACKs, the program answers with RTX on the repair stream.
    await control({drop: 0.05});
    await sleep(6000);
    await control({drop: 0});
    await sleep(1000);
    s = await serverStats();
    const v = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
    check('NACKs from the subscriber are handed over', s.rtcp_in['Subscriber NACK'] > 0, `${s.nack_sequence_numbers_requested} sequence numbers asked for, ${s.rtp_dropped_on_purpose} dropped on purpose`);
    check('RTX written by the program on the repair SSRC is sent', s.rtx_sent > 0, `${s.rtx_sent} sent, ${s.rtx_not_in_cache} not in cache`);
    check('the browser takes the retransmissions', v && v.retransmittedPacketsReceived > 0, v && `retransmittedPacketsReceived ${v.retransmittedPacketsReceived}, nackCount ${v.nackCount}, packetsLost ${v.packetsLost}`);

    // The other direction: the program NACKs the publisher and looks at how the answer comes.
    const before = await serverStats();
    await control({nack_publisher: 10});
    await sleep(1500);
    s = await serverStats();
    const wirePub = s.wire['Publisher 0'] || {rtp_in: {}};
    const rtxOnWire = Object.entries(wirePub.rtp_in).filter(([k]) => / pt 97/.test(k));
    const rtxToApp = Object.entries(s.rtp_in).filter(([k]) => / pt 97/.test(k));
    check('a NACK written by the program makes the publisher retransmit; its RTX is seen right above SRTP', s.nacks_sent_to_publisher === 10 && rtxOnWire.length > 0, JSON.stringify(rtxOnWire));
    check('above the interceptors the library has already unwrapped RTX (so the interceptor is where to take it)', rtxToApp.length === 0, `to the application as pt 97: ${JSON.stringify(rtxToApp)}`);

    // Padding for probing.
    await control({padding_packets: 20});
    await sleep(500);
    s = await serverStats();
    const beforePad = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
    check('padding-only packets on the RTX stream are accepted', s.padding_sent === 20, `sent ${s.padding_sent}; sizes on the wire: ${JSON.stringify(s.padding_wire_sizes)}`);
    const fullSize = Object.keys(s.padding_wire_sizes).some(k => parseInt(k) > 250);
    check('padding of a chosen size (255 bytes, as LiveKit probes) goes out at that size', fullSize, JSON.stringify(s.padding_wire_sizes));
    await sleep(2000);
    const afterPad = inboundVideo(await sub.evaluate(() => window.rtcStats(window.subPc)));
    check('the subscriber keeps decoding after the padding', afterPad.framesDecoded > beforePad.framesDecoded + 20, `${beforePad.framesDecoded} -> ${afterPad.framesDecoded} frames`);

    // Data channels, both ways of opening.
    await until(() => pub.evaluate(() => window.pubDc.readyState === 'open'), 5000);
    await pub.evaluate(() => { for (let i = 0; i < 5; i++) window.pubDc.send('hello ' + i); });
    const echo = await until(() => pub.evaluate(() => window.pubDcEcho.length === 5), 5000);
    check('data channel opened by the browser (unordered, no retransmits) echoes', echo, await pub.evaluate(() => JSON.stringify(window.pubDcEcho)));
    const subDcOpen = await until(() => sub.evaluate(() => window.subDc && window.subDc.readyState === 'open'), 5000);
    if (subDcOpen) await sub.evaluate(() => { window.subDc.send(new Uint8Array([1, 2, 3, 4])); });
    const echo2 = await until(() => sub.evaluate(() => window.subDcEcho.length === 1), 5000);
    check('data channel opened by the server echoes binary', subDcOpen && echo2);

    s = await serverStats();
    check('nothing the program wrote was refused', Object.keys(s.write_errors).length === 0, JSON.stringify(s.write_errors));

    console.log('\n--- what the forwarder counted ---');
    console.log(JSON.stringify(s, null, 1));
    if (process.env.SHOW_SDP) {
      console.log('\n--- publisher answer (by the library) ---\n' + await pub.evaluate(() => window.pubAnswer));
      console.log('\n--- subscriber offer (by the library) ---\n' + await sub.evaluate(() => window.subOffer));
    }
  } finally {
    await browser.close();
  }
  const failed = results.filter(r => !r.ok);
  console.log(`\n${results.length - failed.length} of ${results.length} checks passed`);
  process.exit(failed.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
