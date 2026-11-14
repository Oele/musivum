// A shorter run of the experiment with Firefox: does it connect to the forwarder, publish
// with simulcast, and decode what is forwarded to it?
//
//   node firefox-check.js [base url]      with the forwarder already running
const puppeteer = require('puppeteer-core');
const base = process.argv[2] || 'http://127.0.0.1:39400';
const firefox = process.env.FIREFOX || '/usr/bin/firefox';
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, ms, step = 250) {
  const end = Date.now() + ms; let last;
  while (Date.now() < end) { last = await fn(); if (last) return last; await sleep(step); }
  return last;
}
(async () => {
  const browser = await puppeteer.launch({
    browser: 'firefox', executablePath: firefox, headless: true,
    // A Firefox installed as a snap cannot see /tmp: its profile goes under the experiment.
    userDataDir: require('path').join(__dirname, '..', 'target', 'firefox-profile'),
    extraPrefsFirefox: {
      'media.navigator.streams.fake': true,
      'media.navigator.permission.disabled': true,
      'media.peerconnection.ice.loopback': true,
      'media.peerconnection.ice.obfuscate_host_addresses': false,
      'media.autoplay.default': 0,
    },
  });
  let failed = 0;
  const check = (name, ok, detail) => { if (!ok) failed++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail !== undefined ? '  — ' + detail : ''}`); };
  try {
    console.log(await browser.version());
    const pub = await browser.newPage();
    await pub.goto(base);
    await pub.evaluate(() => window.publish());
    check('Firefox publisher connects', await until(() => pub.evaluate(() => window.pubPc.connectionState === 'connected'), 15000),
      await pub.evaluate(() => window.pubPc.connectionState + ' / ice ' + window.pubPc.iceConnectionState));
    const sub = await browser.newPage();
    await sub.goto(base);
    await sub.evaluate(() => window.subscribe());
    check('Firefox subscriber connects on the server\'s offer', await until(() => sub.evaluate(() => window.subPc.connectionState === 'connected'), 15000),
      await sub.evaluate(() => window.subPc.connectionState + ' / ice ' + window.subPc.iceConnectionState));
    const v = await until(async () => {
      const s = (await sub.evaluate(() => window.rtcStats(window.subPc))).find(x => x.type === 'inbound-rtp' && x.kind === 'video');
      return s && s.framesDecoded > 30 ? s : null;
    }, 20000);
    check('forwarded video decodes in Firefox', v, v && `${v.framesDecoded} frames, ${v.frameWidth}x${v.frameHeight}`);
    const s = await (await fetch(base + '/stats')).json();
    check('simulcast layers from Firefox arrive with their rid', Object.keys(s.rtp_in).filter(k => /rid [qhf]/.test(k)).length >= 2, Object.keys(s.rtp_in).join(' | '));
    check('RTCP from Firefox is handed over', Object.keys(s.rtcp_in).length > 0, JSON.stringify(s.rtcp_in));
    check('nothing the program wrote was refused', Object.keys(s.write_errors).length === 0, JSON.stringify(s.write_errors));
  } finally {
    await browser.close();
  }
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(String(e).slice(0, 600)); process.exit(2); });
