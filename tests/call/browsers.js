// The browser of the scripted call when it is not Chrome: BROWSER=firefox or BROWSER=webkit
// runs both participants in that browser through Playwright (playwright-core, Apache-2.0),
// behind the small part of puppeteer's interface that run.js uses, so that the call itself is
// the same script. The browsers are Playwright's own builds: `npx playwright-core install
// firefox webkit` puts them in ~/.cache/ms-playwright. WebKit is the engine of Safari (and of
// every browser and web view on iOS, so of Element Call in Element X on iOS); Playwright's
// build for Linux is not Safari, but its WebRTC is WebKit's.
//
// Media: Firefox's fake devices (a test pattern and a tone) through its preferences; WebKit
// has mock capture devices of its own in Playwright's build.
//
// Playwright's WebKit wants two libraries a desktop may lack (libmanette, libhidapi-hidraw;
// `npx playwright-core install webkit` names them). Without installing them on the system:
// `apt-get download libmanette-0.2-0 libhidapi-hidraw0`, `dpkg-deb -x` each into a
// directory, and copy the `.so` files into ~/.cache/ms-playwright/webkit-*/minibrowser-wpe/lib
// (its launcher sets the library path itself).
//
// What was seen (2026-11-15): Firefox, the call as with Chrome against both servers. WebKit:
// everything but simulcast, against both servers alike; Playwright's build sends one layer of
// the three it announces, so the checks of layers and of the publisher's quality fail.

function wrapPage(page) {
  // puppeteer passes the arguments after the function on to it; Playwright takes one. The
  // function is sent as source with its arguments as JSON, which is all run.js passes.
  const call = (fn, args) => `(${fn.toString()})(...${JSON.stringify(args)})`;
  return {
    on: (event, f) => page.on(event, f),
    evaluate: (fn, ...args) => page.evaluate(call(fn, args)),
    evaluateOnNewDocument: (fn, ...args) => page.addInitScript(call(fn, args)),
    goto: url => page.goto(url),
    bringToFront: () => page.bringToFront(),
  };
}

async function launch(which) {
  const pw = require('playwright-core');
  const type = {firefox: pw.firefox, webkit: pw.webkit}[which];
  if (!type) throw new Error(`BROWSER=${which}: firefox or webkit`);
  const browser = await type.launch({
    headless: true,
    firefoxUserPrefs: which === 'firefox' ? {
      'media.navigator.streams.fake': true,
      'media.navigator.permission.disabled': true,
      'media.autoplay.default': 0,
      // host candidates on the loopback interface, as Chrome's --allow-loopback-in-peer-connection
      'media.peerconnection.ice.loopback': true,
      'media.peerconnection.ice.obfuscate_host_addresses': false,
    } : undefined,
  });
  const context = await browser.newContext(
    which === 'firefox' ? {} : {permissions: ['camera', 'microphone']});
  return {
    version: browser.version(),
    newPage: async () => wrapPage(await context.newPage()),
    close: () => browser.close(),
  };
}

module.exports = {launch};
