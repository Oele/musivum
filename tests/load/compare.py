"""Two targets' load-test results side by side, scenario by scenario: for each metric the
median of a target's runs and their range (min to max), with a mark only where the two
targets' ranges do not overlap.

    python3 -I compare.py [first] [second]       (default: reference musivum)

Each is a target name (results/<target>-<scenario>.json), a path prefix (some/dir/<target>
for some/dir/<target>-<scenario>.json, taken from the current directory, else from this
one), or a path prefix with a * in a directory's name for several runs of the target
(runs/*/reference for runs/1/reference-<scenario>.json, runs/2/..., ...: the three-run sets
are in runs/). For example `python3 -I compare.py 'runs/*/reference' 'runs/*/musivum'`.
runs/<n>/ is the n-th `LOAD_RESULTS=runs/<n> ./run.sh <target> <scenario>` of each target and
scenario (the targets taken in turn, each run started below a load average of 2).

The marks, from the second's point of view:
  <<  worse: the ranges are apart by more than the metric's tolerance
  >>  better, by the same rule
  <   worse, ranges apart but within the tolerance (or a metric judged by no tolerance)
  >   better, the same
  <>  ranges apart, on a metric where neither is better
The tolerances are the ones a single run was judged by before (fewer subscribers or tracks
with media, 0.5 points more loss, 20% less bitrate, more errors, twice the CPU above 0.2
cores, three times the memory above 500 MB, a stop or an exit), now held between the nearer
ends of the two ranges; with one run on each side << is what that comparison marked. <, >
and <> are shown only with two runs or more on both sides. With three runs against three,
ranges that do not overlap still happen by chance one time in ten for a metric that does
not differ, so a single mark is a hint; a difference holds up when it is large against the
spread or other metrics agree. Exits 1 if anything is marked <<.
"""

import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def prefixes(target):
    """The path prefixes a target stands for: one, or one for each directory its * matches."""
    if '/' not in target:
        return [os.path.join(HERE, 'results', target)]
    fixed = target.split('*')[0]
    base = os.getcwd() if os.path.isdir(os.path.dirname(os.path.abspath(fixed + 'x'))) else HERE
    full = os.path.join(base, target)
    if '*' not in full:
        return [full]
    head, tail = os.path.split(full)
    num = lambda d: [int(x) if x.isdigit() else x for x in re.split(r'(\d+)', d)]
    return [os.path.join(d, tail) for d in sorted(glob.glob(head), key=num) if os.path.isdir(d)]


def load(prefs, scenario):
    files = [f'{p}-{scenario}.json' for p in prefs]
    return [json.load(open(f)) for f in files if os.path.exists(f)]


def get(r, path):
    for k in path.split('.'):
        if r is None:
            return None
        r = r.get(k)
    return r


def field(path):
    return lambda r: get(r, path)


# The rows: a name, the value of a run, which way is better ('up', 'down' or None), and the
# tolerance a difference has to pass to count: {'abs': x} (apart by more than x),
# {'rel': f} (the worse end below f times the better one) or {'ratio': k, 'floor': x} (the
# worse end above k times the better one and above x). A row without a tolerance is shown,
# and marked only with <, > or <>.
ROWS = [
    ('subscribers with all media', field('subscribers.withAllMedia'), 'up', {'abs': 0}),
    ('tracks received', field('subscribers.tracksReceived'), 'up', {'abs': 0}),
    ('loss %', field('subscribers.lossPct'), 'down', {'abs': 0.5}),
    ('loss %, worst subscriber', field('subscribers.worstSubscriberLossPct'), 'down', None),
    ('kbps per subscriber', field('subscribers.kbpsPerSubscriber.mean'), 'up', {'rel': 0.8}),
    ('video kbps per track (median)', field('subscribers.videoKbpsPerTrack.median'), None, None),
    ('errors', field('subscribers.errors'), 'down', {'abs': 0}),
    ('cpu cores, steady mean', field('server.cpuSteady.mean'), 'down', {'ratio': 2, 'floor': 0.2}),
    ('cpu cores, steady peak', field('server.cpuSteady.max'), 'down', None),
    ('cpu millicores per participant', field('server.cpuPerParticipantMilli'), 'down', None),
    ('rss MB, idle', field('server.rssIdleMB'), 'down', None),
    ('rss MB, peak', field('server.rssPeakMB'), 'down', {'ratio': 3, 'floor': 500}),
    ('threads, peak', field('server.threadsPeak'), None, None),
    ('lk cpu cores, steady', field('loadGenerator.cpuSteady.mean'), None, None),
    ('load average (max)', field('machine.load1.max'), None, None),
    ('temperature °C (max)', field('machine.tempC.max'), None, None),
]


def median(xs):
    n, m = len(xs), len(xs) // 2
    return xs[m] if n % 2 else (xs[m - 1] + xs[m]) / 2


def show(v):
    return str(round(v, 3)) if isinstance(v, float) else str(v)


def spread(values):
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None, None, '-'
    lo, hi = xs[0], xs[-1]
    text = show(lo) if lo == hi else f'{show(median(xs))} [{show(lo)}-{show(hi)}]'
    return lo, hi, text


def past(better, worse, way, tol):
    """Whether `worse` is beyond `better` by more than the tolerance, in the row's direction."""
    if 'abs' in tol:
        return worse > better + tol['abs'] if way == 'down' else worse < better - tol['abs']
    if 'rel' in tol:
        return worse < tol['rel'] * better
    return worse > tol['floor'] and worse > tol['ratio'] * better


def mark(a, b, way, tol, many):
    (alo, ahi), (blo, bhi) = a, b
    if alo is None or blo is None or not (ahi < blo or bhi < alo):
        return ''
    if not way:
        return '<>' if many else ''
    b_worse = blo > ahi if way == 'down' else bhi < alo
    if way == 'down':
        better, worse = (ahi, blo) if b_worse else (bhi, alo)
    else:
        better, worse = (alo, bhi) if b_worse else (blo, ahi)
    if tol and past(better, worse, way, tol):
        return '<<' if b_worse else '>>'
    return ('<' if b_worse else '>') if many else ''


def main():
    first, second = (sys.argv[1:3] + ['reference', 'musivum'][len(sys.argv[1:3]):])[:2]
    pa, pb = prefixes(first), prefixes(second)
    marked = 0
    for scenario in 'abcd':
        ra, rb = load(pa, scenario), load(pb, scenario)
        if not ra and not rb:
            continue
        some = (ra or rb)[0]
        print(f"\n{scenario}: {some['about']}")
        print(f"  {some['subscribers']['expected']} subscribers expected, "
              f"{some['subscribers']['tracksExpected']} tracks")
        print(f"  {'':34} {first:>24} {second:>24}")
        if not ra or not rb:
            print(f'  only {first if ra else second} has run it')
            continue
        many = len(ra) > 1 and len(rb) > 1
        if len(ra) > 1 or len(rb) > 1:
            print(f"  {'runs':34} {len(ra):>24} {len(rb):>24}")
        for name, value, way, tol in ROWS:
            alo, ahi, at = spread(value(r) for r in ra)
            blo, bhi, bt = spread(value(r) for r in rb)
            m = mark((alo, ahi), (blo, bhi), way, tol, many)
            if m == '<<':
                marked += 1
            print(f'  {name:34} {at:>24} {bt:>24}' + (f'   {m}' if m else ''))
        # A stop or an exit of the server: marked when the second has one and the first none.
        for name, key in (('stopped', 'stops'), ('server exited', 'serverExit')):
            na, nb = sum(1 for r in ra if r[key]), sum(1 for r in rb if r[key])
            m = '<<' if nb and not na else ''
            if m:
                marked += 1
            print(f'  {name:34} {f"{na}/{len(ra)}":>24} {f"{nb}/{len(rb)}":>24}' + (f'   {m}' if m else ''))
    sys.exit(1 if marked else 0)


if __name__ == '__main__':
    main()
