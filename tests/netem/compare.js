// Two servers' degraded-network results side by side, scenario by scenario, with the
// differences that hold up marked: for each metric, the median of a target's runs and their
// range (min to max), and a mark only where the two targets' ranges do not overlap.
//
//   node compare.js <first> <second>
//
// Each is where a target's results are: a target name (../call/out/<target>-<scenario>.json),
// a path prefix (baseline/reference for baseline/reference-<scenario>.json), or a path prefix
// with a * in a directory's name for several runs of the target (runs/*/reference for
// runs/1/reference-<scenario>.json, runs/2/..., ...: the three-run sets are in runs/). For
// example `node compare.js 'runs/*/reference' 'runs/*/musivum'`, or `node compare.js
// baseline/reference musivum` for musivum's latest run against LiveKit's single baseline.
// runs/<n>/ is the n-th `NETEM=all ../call/run.sh <target>` of each target, its out/ files
// copied (the targets taken in turn, each run started below a load average of 2); a run the
// harness flags MACHINE BUSY is run once more, the flagged one kept as
// <target>-<scenario>.busy.json.
//
// The marks, from the second's point of view:
//   <<  worse: the ranges are apart by more than the metric's tolerance
//   >>  better, by the same rule
//   <   worse, ranges apart but within the tolerance (or a metric judged by no tolerance)
//   >   better, the same
//   <>  ranges apart, on a metric where neither is better
// The tolerances are the ones a single run was judged by before (1 freeze, 0.5 s of freezes,
// any lower layer, 3 s to recover, 20% of the frame rate), now held between the nearer ends of
// the two ranges; with one run on each side << is what that comparison marked. <, > and <>
// are shown only with two runs or more on both sides. With three runs against three, ranges
// that do not overlap still happen by chance one time in ten for a metric that does not
// differ, so a single mark is a hint; a difference holds up when it is large against the
// spread or other metrics agree. Exits 1 if anything is marked << (or a path is unproven).
const fs = require('fs');
const path = require('path');
const {scenarios} = require('./scenarios');

const [first, second] = process.argv.slice(2);
if (!second) {
  console.error('usage: node compare.js <target, path prefix or runs/*/target> <the same>');
  process.exit(2);
}
// A path prefix is taken from the current directory, else from this one.
const prefix = p => {
  if (!p.includes('/')) return path.join(__dirname, '..', 'call', 'out', p);
  const fixed = p.split('*')[0];
  return fs.existsSync(path.dirname(path.resolve(fixed + 'x'))) ? path.resolve(p) : path.join(__dirname, p);
};
// The prefixes a target stands for: itself, or one for each directory that matches its *.
function expand(p) {
  const full = prefix(p);
  if (!full.includes('*')) return [full];
  const parts = full.split(path.sep);
  const i = parts.findIndex(x => x.includes('*'));
  const parent = parts.slice(0, i).join(path.sep) || path.sep;
  const re = new RegExp('^' + parts[i].split('*').map(x => x.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('.*') + '$');
  const dirs = fs.existsSync(parent) ? fs.readdirSync(parent).filter(n => re.test(n)) : [];
  dirs.sort((a, b) => a.localeCompare(b, undefined, {numeric: true}));
  return dirs.map(d => [parent, d, ...parts.slice(i + 1)].join(path.sep));
}
function load(prefixes, scenario) {
  return prefixes.map(p => `${p}-${scenario}.json`).filter(f => fs.existsSync(f))
    .map(f => JSON.parse(fs.readFileSync(f, 'utf8')));
}

const height = label => parseInt(label, 10) || 0;
const round = (x, d = 2) => Math.round(x * 10 ** d) / 10 ** d;
const num = v => v === Infinity ? 'never' : String(round(v));
const px = v => `${v}p`;

// The rows: a name, the value of a run (a number, or null where it does not apply), which
// way is better ('up', 'down' or null), and the tolerance a difference has to pass to count:
// {abs: x} (apart by more than x) or {rel: f} (the worse end below f times the better one).
// A row without a tolerance is shown, and marked only with <, > or <>.
const rows = [
  ['freezes', s => s.freezes, 'down', {abs: 1}],
  ['freeze duration (s)', s => s.freezeS, 'down', {abs: 0.5}],
  ['layer before', s => height(s.reference.layer), 'up', {abs: 0}, px],
  ['layer during', s => s.during ? height(s.during.layer) : null, 'up', {abs: 0}, px],
  ['lowest during', s => s.during ? height(s.during.lowest) : null, 'up', {abs: 0}, px],
  ['layer after', s => height(s.after.layer), 'up', {abs: 0}, px],
  ['recovered (s)', s => !s.during ? null : s.recoverS === null ? Infinity : s.recoverS, 'down', {abs: 3}],
  ['fps before', s => s.reference.fps, 'up', null],
  ['fps during', s => s.during ? s.during.fps : null, 'up', {rel: 0.8}],
  ['fps after', s => s.after.fps, 'up', {rel: 0.8}],
  ['frames dropped', s => s.framesDropped, 'down', null],
  ['lost', s => s.lost, null, null],
  ['nack', s => s.nack, null, null],
  ['pli', s => s.pli, null, null],
  ['audio concealed (ms)', s => s.audioConcealedMs, 'down', null],
  ['rtt during (ms)', s => s.during ? s.during.rttMs : null, null, null],
];

const median = xs => {
  const n = xs.length, m = Math.floor(n / 2);
  return n % 2 ? xs[m] : (xs[m - 1] + xs[m]) / 2;
};
function spread(values, show) {
  const xs = values.filter(v => v !== null && v !== undefined).sort((a, b) => a - b);
  if (!xs.length) return {text: '-'};
  const lo = xs[0], hi = xs[xs.length - 1];
  return {lo, hi, text: lo === hi ? show(lo) : `${show(median(xs))} [${show(lo)}-${show(hi)}]`};
}
// Whether `worse` is beyond `better` by more than the tolerance, in the row's direction.
function past(better, worse, dir, tol) {
  if (dir === 'down') return tol.abs !== undefined ? worse > better + tol.abs : worse > better / tol.rel;
  return tol.abs !== undefined ? worse < better - tol.abs : worse < tol.rel * better;
}
function mark(a, b, dir, tol, many) {
  if (a.lo === undefined || b.lo === undefined) return '';
  const apart = a.hi < b.lo || b.hi < a.lo;
  if (!apart) return '';
  if (!dir) return many ? '<>' : '';
  // Which end of each range faces the other.
  const bWorse = dir === 'down' ? b.lo > a.hi : b.hi < a.lo;
  const [better, worse] = bWorse ? (dir === 'down' ? [a.hi, b.lo] : [a.lo, b.hi])
    : (dir === 'down' ? [b.hi, a.lo] : [b.lo, a.hi]);
  if (tol && past(better, worse, dir, tol)) return bWorse ? '<<' : '>>';
  return many ? (bWorse ? '<' : '>') : '';
}

const runsA = expand(first), runsB = expand(second);
let marked = 0;
const w = [28, 26, 26];
const line = cells => cells.map((c, i) => String(c).padEnd(w[i] || 0)).join(' ').trimEnd();
console.log(line(['', first, second]));
for (const name of Object.keys(scenarios)) {
  const ra = load(runsA, name), rb = load(runsB, name);
  if (!ra.length && !rb.length) continue;
  console.log(`\n${name}: ${scenarios[name].about}`);
  if (!ra.length || !rb.length) {
    console.log(`  only ${ra.length ? first : second} has run it`);
    continue;
  }
  const many = ra.length > 1 && rb.length > 1;
  if (ra.length > 1 || rb.length > 1) console.log(line(['  runs', ra.length, rb.length]));
  const sa = ra.map(r => r.subscribers.bob.summary), sb = rb.map(r => r.subscribers.bob.summary);
  for (const [label, value, dir, tol, show = num] of rows) {
    const a = spread(sa.map(value), show), b = spread(sb.map(value), show);
    const m = mark(a, b, dir, tol, many);
    if (m === '<<') marked++;
    console.log(line([`  ${label}`, a.text, b.text]) + (m ? `   ${m}` : ''));
  }
  const proven = rs => {
    const ok = rs.filter(r => r.path.proof.ok).length;
    return rs.length === 1 ? (ok ? 'yes' : 'NO') : `${ok}/${rs.length}${ok < rs.length ? ' (NOT ALL)' : ''}`;
  };
  const unproven = !ra.every(r => r.path.proof.ok) || !rb.every(r => r.path.proof.ok);
  if (unproven) marked++;
  console.log(line(['  path through the relay', proven(ra), proven(rb)]) + (unproven ? '   <<' : ''));
  const busy = rs => rs.filter(r => r.summary.includes('MACHINE BUSY')).length;
  if (busy(ra) || busy(rb)) console.log(line(['  runs flagged busy', busy(ra), busy(rb)]));
}
console.log(`\n${marked ? marked + ' difference(s) marked <<: ' + second + ' does worse there' : 'nothing marked <<'}`);
process.exit(marked ? 1 : 0);
