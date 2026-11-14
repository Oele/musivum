"""Two targets' load-test results side by side, scenario by scenario, with where the second
does worse marked (*): fewer subscribers with media, more loss, errors, a stop or an exit, or
CPU or memory well above the first's.

    python3 -I compare.py [first] [second]       (default: reference musivum)

Reads results/<target>-<scenario>.json. Exits 1 if anything is marked.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def load(target, scenario):
    f = os.path.join(HERE, 'results', f'{target}-{scenario}.json')
    return json.load(open(f)) if os.path.exists(f) else None


def get(r, path):
    for k in path.split('.'):
        if r is None:
            return None
        r = r.get(k)
    return r


def ratio_worse(limit, floor):
    """b worse than a when above limit times a, and above an absolute floor."""
    return lambda a, b: a is not None and b is not None and b > floor and b > limit * a


# The rows: a name, the value shown, and when the second is worse.
ROWS = [
    ('subscribers with all media', lambda r: f"{get(r, 'subscribers.withAllMedia')}/{get(r, 'subscribers.expected')}",
     lambda a, b: get(b, 'subscribers.withAllMedia') < get(a, 'subscribers.withAllMedia')),
    ('tracks received', lambda r: f"{get(r, 'subscribers.tracksReceived')}/{get(r, 'subscribers.tracksExpected')}",
     lambda a, b: get(b, 'subscribers.tracksReceived') < get(a, 'subscribers.tracksReceived')),
    ('loss % (worst subscriber)', lambda r: f"{get(r, 'subscribers.lossPct')} ({get(r, 'subscribers.worstSubscriberLossPct')})",
     lambda a, b: (get(b, 'subscribers.lossPct') or 0) > (get(a, 'subscribers.lossPct') or 0) + 0.5),
    ('kbps per subscriber', lambda r: get(r, 'subscribers.kbpsPerSubscriber.mean'),
     lambda a, b: (get(b, 'subscribers.kbpsPerSubscriber.mean') or 0) < 0.8 * (get(a, 'subscribers.kbpsPerSubscriber.mean') or 0)),
    ('video kbps per track (median)', lambda r: get(r, 'subscribers.videoKbpsPerTrack.median'), None),
    ('errors', lambda r: get(r, 'subscribers.errors'),
     lambda a, b: get(b, 'subscribers.errors') > get(a, 'subscribers.errors')),
    ('cpu cores, steady (mean / peak)', lambda r: f"{get(r, 'server.cpuSteady.mean')} / {get(r, 'server.cpuSteady.max')}",
     lambda a, b: ratio_worse(2, 0.2)(get(a, 'server.cpuSteady.mean'), get(b, 'server.cpuSteady.mean'))),
    ('cpu millicores per participant', lambda r: get(r, 'server.cpuPerParticipantMilli'), None),
    ('rss MB (idle -> peak)', lambda r: f"{get(r, 'server.rssIdleMB')} -> {get(r, 'server.rssPeakMB')}",
     lambda a, b: ratio_worse(3, 500)(get(a, 'server.rssPeakMB'), get(b, 'server.rssPeakMB'))),
    ('threads (idle / peak)', lambda r: f"{get(r, 'server.threadsIdle')} / {get(r, 'server.threadsPeak')}", None),
    ('lk cpu cores, steady', lambda r: get(r, 'loadGenerator.cpuSteady.mean'), None),
    ('load average (max)', lambda r: get(r, 'machine.load1.max'), None),
    ('temperature °C (max)', lambda r: get(r, 'machine.tempC.max'), None),
    ('stopped', lambda r: '; '.join(s['reason'] for s in r['stops']) or '-',
     lambda a, b: bool(b['stops']) and not a['stops']),
    ('server exited', lambda r: r['serverExit'] and f"at {r['serverExit']['t']} s" or '-',
     lambda a, b: bool(b['serverExit']) and not a['serverExit']),
]


def main():
    first, second = (sys.argv[1:3] + ['reference', 'musivum'][len(sys.argv[1:3]):])[:2]
    marked = 0
    for scenario in 'abcd':
        a, b = load(first, scenario), load(second, scenario)
        if not a and not b:
            continue
        about = (a or b)['about']
        print(f'\n{scenario}: {about}')
        print(f"  {'':34} {first:>24} {second:>24}")
        for name, show, worse in ROWS:
            va = show(a) if a else '-'
            vb = show(b) if b else '-'
            mark = ''
            if a and b and worse and worse(a, b):
                mark = ' *'
                marked += 1
            print(f'  {name:34} {str(va):>24} {str(vb):>24}{mark}')
    sys.exit(1 if marked else 0)


if __name__ == '__main__':
    main()
