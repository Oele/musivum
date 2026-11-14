// Two servers' degraded-network results side by side, scenario by scenario, with the
// differences that matter marked: where the second does worse than the first in freezes,
// time to recover, the layer the subscriber got, or its frame rate.
//
//   node compare.js <first> <second>
//
// Each is where a target's results are: a target name (../call/out/<target>-<scenario>.json)
// or a path prefix (baseline/reference for baseline/reference-<scenario>.json). For example
// `node compare.js baseline/reference musivum` holds musivum's latest run against LiveKit's
// committed one. Exits 1 if anything is marked.
const fs = require('fs');
const path = require('path');
const {scenarios} = require('./scenarios');

const [first, second] = process.argv.slice(2);
if (!second) {
  console.error('usage: node compare.js <target or path prefix> <target or path prefix>');
  process.exit(2);
}
// A path prefix is taken from the current directory, else from this one.
const prefix = p => !p.includes('/') ? path.join(__dirname, '..', 'call', 'out', p)
  : fs.existsSync(path.dirname(path.resolve(p))) ? path.resolve(p) : path.join(__dirname, p);
function load(p, scenario) {
  const f = `${prefix(p)}-${scenario}.json`;
  return fs.existsSync(f) ? JSON.parse(fs.readFileSync(f, 'utf8')) : null;
}

const height = label => parseInt(label, 10) || 0;
const show = v => v === null || v === undefined ? '-' : String(v);

// The rows of the table: a name, the value shown, and when the second is worse.
const rows = [
  ['freezes (s)', s => `${s.freezes} (${s.freezeS})`,
    (a, b) => b.freezes > a.freezes + 1 || b.freezeS > a.freezeS + 0.5],
  ['layer before', s => s.reference.layer, (a, b) => height(b.reference.layer) < height(a.reference.layer)],
  ['layer during', s => s.during && s.during.layer, (a, b) => a.during && height(b.during.layer) < height(a.during.layer)],
  ['lowest during', s => s.during && s.during.lowest, (a, b) => a.during && height(b.during.lowest) < height(a.during.lowest)],
  ['layer after', s => s.after.layer, (a, b) => height(b.after.layer) < height(a.after.layer)],
  ['recovered (s)', s => s.during ? (s.recoverS === null ? 'never' : s.recoverS) : null,
    (a, b) => a.during && a.recoverS !== null && (b.recoverS === null || b.recoverS > a.recoverS + 3)],
  ['fps before/during/after', s => [s.reference.fps, s.during && s.during.fps, s.after.fps].map(show).join('/'),
    (a, b) => (a.during && b.during.fps < 0.8 * a.during.fps) || b.after.fps < 0.8 * a.after.fps],
  ['frames dropped', s => s.framesDropped, () => false],
  ['lost/nack/pli', s => `${s.lost}/${s.nack}/${s.pli}`, () => false],
  ['rtt ms before/during/after', s => [s.before.rttMs, s.during && s.during.rttMs, s.after.rttMs].map(show).join('/'), () => false],
];

let marked = 0;
const w = [28, 26, 26];
const line = cells => cells.map((c, i) => String(c).padEnd(w[i] || 0)).join(' ').trimEnd();
console.log(line(['', first, second]));
for (const name of Object.keys(scenarios)) {
  const ra = load(first, name), rb = load(second, name);
  if (!ra && !rb) continue;
  console.log(`\n${name}: ${scenarios[name].about}`);
  if (!ra || !rb) {
    console.log(`  only ${ra ? first : second} has run it`);
    continue;
  }
  const a = ra.subscribers.bob.summary, b = rb.subscribers.bob.summary;
  for (const [label, value, worse] of rows) {
    const bad = !!worse(a, b);
    if (bad) marked++;
    console.log(line([`  ${label}`, show(value(a)), show(value(b))]) + (bad ? '   <<' : ''));
  }
  const proven = r => r.path.proof.ok ? 'yes' : 'NO';
  const unproven = !ra.path.proof.ok || !rb.path.proof.ok;
  if (unproven) marked++;
  console.log(line(['  path through the relay', proven(ra), proven(rb)]) + (unproven ? '   <<' : ''));
}
console.log(`\n${marked ? marked + ' difference(s) marked <<: ' + second + ' does worse there' : 'nothing marked'}`);
process.exit(marked ? 1 : 0);
