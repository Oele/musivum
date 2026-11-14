"""One load-test run: a server, LiveKit's load generator (`lk perf load-test`) against it, and
the server's CPU, memory and threads, the machine's load and the CPU temperature each second.

    python3 -I load.py <target> <scenario> <server binary> <config> <lk binary>

run.sh is the way in (it builds or checks the server first). The results go to
results/<target>-<scenario>.json, the logs (server, lk) to target/load/ of the repository.

Safety, as the machine may also run other services, a LiveKit among them:
- the server and lk talk on 127.0.0.1, ports 39560 to 39562 only (the configurations here);
  every lk call gets an explicit --url, --api-key and --api-secret, and LIVEKIT_* is removed
  from its environment;
- a run starts only below 75 °C and a load average below 8 (it waits up to 5 minutes);
- a run is stopped (lk first, then the server) when the temperature stays above 90 °C for
  30 s (the CPU fan takes a few seconds to spin up, and the CPU throttles itself near
  100 °C) or above 97 °C for 10 s, the load average passes 16, or the server's RSS passes
  6 GB; the stop is recorded;
- the server runs in a scope of its own with MemoryMax=8G (systemd-run --user), everything
  under nice -n 10, and what is started is stopped by its process group.
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
LOGS = os.path.join(ROOT, 'target', 'load')
# LOAD_RESULTS puts a run's results elsewhere, for a run that is not one of record.
RESULTS = os.environ.get('LOAD_RESULTS', os.path.join(HERE, 'results'))

LK_VERSION = '2.19.4'
PORT = 39560
URL = f'ws://127.0.0.1:{PORT}'
API_KEY = 'loadtestkey'
API_SECRET = 'loadtestsecret-0123456789abcdef0123456789abcdef'

# LOAD_DURATION_S shortens a run, for trying the harness out.
DURATION_S = int(os.environ.get('LOAD_DURATION_S', 60))
JOINS_PER_S = 5

# The scenarios. lk's publishers publish and do not subscribe; its subscribers subscribe and
# do not publish (each to at most 6 participants, the first video at the high layer, the
# others at the low one: its "speaker" layout). With video and audio publishers both set, the
# first ones publish both.
SCENARIOS = {
    'a': dict(about='one room, 2 publishers of simulcast video and audio, 10 subscribers',
              rooms=1, video=2, audio=2, subscribers=10),
    'b': dict(about='one room, 2 publishers of simulcast video and audio, 50 subscribers',
              rooms=1, video=2, audio=2, subscribers=50),
    # A call of 4 that all publish and subscribe to each other, as lk can make it: 4
    # publishers of video and audio and 3 subscribers. The server gets the same tracks in
    # (4 simulcast videos, 4 audios) and sends the same number out (12 videos, 12 audios)
    # as in a call of 4, with 7 participants instead of 4.
    'c': dict(about='10 rooms, each a call of 4: 4 publishers of simulcast video and audio, '
                    '3 subscribers', rooms=10, video=4, audio=4, subscribers=3),
    'd': dict(about='one room, 1 publisher of simulcast video, 100 subscribers',
              rooms=1, video=1, audio=0, subscribers=100),
}

START_TEMP_C = 75
START_LOAD = 8
STOP_TEMP_C = 90
STOP_TEMP_S = 30
HARD_STOP_TEMP_C = 97
HARD_STOP_TEMP_S = 10
STOP_LOAD = 16
STOP_RSS = 6 * 1000 ** 3
WAIT_COOL_S = 300

CLK_TCK = os.sysconf('SC_CLK_TCK')
PAGE = os.sysconf('SC_PAGE_SIZE')


def temp_source():
    """The CPU package temperature: coretemp's "Package id 0" (what lm-sensors shows), else
    the x86_pkg_temp thermal zone."""
    base = '/sys/class/hwmon'
    for h in sorted(os.listdir(base)):
        d = os.path.join(base, h)
        try:
            name = open(os.path.join(d, 'name')).read().strip()
        except OSError:
            continue
        if name != 'coretemp':
            continue
        for f in sorted(os.listdir(d)):
            if f.endswith('_label') and open(os.path.join(d, f)).read().strip() == 'Package id 0':
                return os.path.join(d, f.replace('_label', '_input')), 'coretemp Package id 0 (hwmon)'
    base = '/sys/class/thermal'
    for z in sorted(os.listdir(base)):
        d = os.path.join(base, z)
        try:
            if open(os.path.join(d, 'type')).read().strip() == 'x86_pkg_temp':
                return os.path.join(d, 'temp'), 'x86_pkg_temp (thermal zone)'
        except OSError:
            continue
    sys.exit('no CPU temperature found')


def read_temp(path):
    return int(open(path).read()) / 1000


def load1():
    return float(open('/proc/loadavg').read().split()[0])


def machine_busy():
    """The machine's busy CPU time in seconds, all cores, from /proc/stat."""
    f = open('/proc/stat').readline().split()[1:]
    return sum(int(x) for i, x in enumerate(f[:8]) if i not in (3, 4)) / CLK_TCK


def proc_stat(pid):
    """(CPU seconds, RSS bytes, threads) of a process, or None once it is gone."""
    try:
        s = open(f'/proc/{pid}/stat').read()
    except OSError:
        return None
    f = s[s.rindex(')') + 2:].split()
    if f[0] == 'Z':
        return None
    return (int(f[11]) + int(f[12])) / CLK_TCK, int(f[21]) * PAGE, int(f[17])


def port_in_use(port):
    import socket
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(('127.0.0.1', port)) == 0


def stop_group(proc, first, wait_s):
    """Stops a process started in a session of its own: `first`, then SIGKILL after wait_s."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, first)
    except ProcessLookupError:
        return
    try:
        proc.wait(wait_s)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


BITRATE = re.compile(r'([\d.]+)(bps|kbps|mbps)')
LOSS = re.compile(r'(\d+) \(([\d.]+)%\)')


def kbps(text):
    m = BITRATE.search(text)
    if not m:
        return None
    return round(float(m.group(1)) * {'bps': 0.001, 'kbps': 1, 'mbps': 1000}[m.group(2)], 1)


def dropped(text):
    m = LOSS.search(text)
    return int(m.group(1)) if m else None


def parse_lk(out, room):
    """lk's two tables, "Track loading" and "Subscriber summaries", as numbers."""
    subs, section, tester = {}, None, None
    for line in out.splitlines():
        if line.startswith('Track loading'):
            section = 'tracks'
            continue
        if line.startswith('Subscriber summaries'):
            section = 'subs'
            continue
        if not line.startswith('│'):
            continue
        cells = [c.strip() for c in line.strip('│').split('│')]
        if cells[0] == 'Tester' or cells[0] == 'Total':
            continue
        if section == 'tracks':
            if cells[0]:
                tester = cells[0]
            if not cells[2]:
                continue
            sub = subs.setdefault(tester, {'room': room, 'name': tester, 'trackStats': []})
            sub['trackStats'].append({'kind': cells[2], 'packets': int(cells[3]),
                                      'kbps': kbps(cells[4]), 'dropped': dropped(cells[5])})
        elif section == 'subs':
            sub = subs.setdefault(cells[0], {'room': room, 'name': cells[0], 'trackStats': []})
            got, want = cells[1].split('/')
            sub.update(tracks=int(got), expected=int(want), kbps=kbps(cells[2]),
                       dropped=dropped(cells[3]), error=None if cells[4] == '-' else cells[4])
    return list(subs.values())


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {'mean': round(sum(xs) / len(xs), 3), 'max': round(max(xs), 3)}


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    return xs[len(xs) // 2] if xs else None


def main():
    target, scenario, server_bin, config, lk = sys.argv[1:6]
    sc = SCENARIOS[scenario]
    os.makedirs(LOGS, exist_ok=True)
    os.makedirs(RESULTS, exist_ok=True)
    assert URL.startswith('ws://127.0.0.1:395'), URL

    have = subprocess.run([lk, '--version'], capture_output=True, text=True).stdout.split()[-1]
    if have != LK_VERSION:
        sys.exit(f'lk is {have}, the harness is for {LK_VERSION}')
    for port in (PORT, PORT + 1):
        if port_in_use(port):
            sys.exit(f'127.0.0.1:{port} is in use')

    tpath, tsource = temp_source()
    waited = 0
    while read_temp(tpath) >= START_TEMP_C or load1() >= START_LOAD:
        if waited >= WAIT_COOL_S:
            sys.exit(f'not started: {read_temp(tpath)} °C, load {load1()} after {waited} s')
        print(f'waiting: {read_temp(tpath)} °C, load {load1()}', flush=True)
        time.sleep(10)
        waited += 10
    before = {'tempC': read_temp(tpath), 'load1': load1(), 'waitedS': waited}

    tag = f'{target}-{scenario}'
    procs = []
    stops = []

    def cleanup(*_):
        for p in procs[1:]:
            stop_group(p, signal.SIGINT, 5)
        if procs:
            stop_group(procs[0], signal.SIGTERM, 10)

    def on_term(signum, _frame):
        cleanup()
        sys.exit(f'stopped by signal {signum}')

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    # The server: in a scope with MemoryMax=8G when systemd-run --user works, else with its
    # data segment capped at 8 GB (the shell's ulimit -d, set by run.sh).
    cap = ['systemd-run', '--user', '--scope', '--quiet', '-p', 'MemoryMax=8G', '--']
    memory_cap = 'systemd-run --user --scope -p MemoryMax=8G'
    if subprocess.run(cap + ['true'], capture_output=True).returncode != 0:
        cap, memory_cap = [], 'ulimit -d 8388608 (systemd-run --user failed)'
    server_log = open(os.path.join(LOGS, f'{tag}-server.log'), 'w')
    server = subprocess.Popen(cap + ['nice', '-n', '10', server_bin, '--config', config],
                              stdout=server_log, stderr=subprocess.STDOUT,
                              start_new_session=True, cwd=HERE)
    procs.append(server)
    try:
        return run(target, scenario, sc, lk, tag, procs, stops, server, tpath, tsource,
                   before, memory_cap)
    finally:
        cleanup()


def run(target, scenario, sc, lk, tag, procs, stops, server, tpath, tsource, before, memory_cap):
    t0 = time.monotonic()
    # The health check goes straight to the server, never through a proxy of the environment.
    direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for _ in range(150):
        try:
            if direct.open(f'http://127.0.0.1:{PORT}/', timeout=1).read().strip() == b'OK':
                break
        except OSError:
            pass
        if server.poll() is not None:
            sys.exit(f'{target} exited at start with {server.returncode}')
        time.sleep(0.1)
    else:
        sys.exit(f'{target} did not answer its health check')
    up_s = round(time.monotonic() - t0, 1)

    samples = []
    last = {}

    def sample(phase, lks):
        now = time.monotonic()
        s = proc_stat(server.pid)
        # lk's CPU: what each of its processes used since the last sample.
        lk_cpu = 0
        for p in lks:
            ps = proc_stat(p.pid)
            if ps:
                lk_cpu += ps[0] - last.get(p.pid, 0)
                last[p.pid] = ps[0]
        row = {'t': round(now - t0, 1), 'phase': phase}
        if s:
            cpu, rss, threads = s
            busy = machine_busy()
            if 'cpu' in last:
                row['cpu'] = round((cpu - last['cpu']) / (now - last['at']), 3)
                row['lkCpu'] = round(lk_cpu / (now - last['at']), 3)
                # Everything the machine did, ours and the rest (other builds, live services).
                row['machineCpu'] = round((busy - last['busy']) / (now - last['at']), 2)
            row.update(rssMB=round(rss / 1e6, 1), threads=threads)
            last.update(cpu=cpu, busy=busy, at=now)
        row.update(load1=load1(), tempC=read_temp(tpath))
        samples.append(row)
        return s

    # Three seconds idle, for the server's memory before any participant.
    for _ in range(3):
        sample('idle', [])
        time.sleep(1)

    env = {k: v for k, v in os.environ.items() if not k.startswith('LIVEKIT_')}
    env['LK_NO_SKILLS_PROMPT'] = '1'
    lks = []
    for r in range(sc['rooms']):
        args = ['nice', '-n', '10', lk, 'perf', 'load-test',
                '--url', URL, '--api-key', API_KEY, '--api-secret', API_SECRET,
                '--room', f'load-{scenario}-{r}', '--identity-prefix', f'{scenario}{r}',
                '--duration', f'{DURATION_S}s', '--num-per-second', str(JOINS_PER_S),
                '--video-codec', 'h264', '--layout', 'speaker',
                '--video-publishers', str(sc['video']), '--audio-publishers', str(sc['audio']),
                '--subscribers', str(sc['subscribers'])]
        out = open(os.path.join(LOGS, f'{tag}-lk{r}.out'), 'w')
        err = open(os.path.join(LOGS, f'{tag}-lk{r}.err'), 'w')
        # Its working directory has no livekit.toml, so that lk reads no project of its own.
        p = subprocess.Popen(args, stdout=out, stderr=err, env=env, cwd=LOGS,
                             start_new_session=True)
        procs.append(p)
        lks.append(p)
        if sc['rooms'] > 1:
            time.sleep(0.5)

    t_lk = time.monotonic()
    participants = sc['rooms'] * (max(sc['video'], sc['audio']) + sc['subscribers'])
    deadline = t_lk + DURATION_S + participants / JOINS_PER_S + 120
    hot_s = 0
    very_hot_s = 0
    steady_at = None
    server_exit = None
    next_tick = time.monotonic() + 1
    while any(p.poll() is None for p in lks):
        time.sleep(max(0, next_tick - time.monotonic()))
        next_tick += 1
        if steady_at is None:
            done = sum('Finished connecting to room' in open(os.path.join(LOGS, f'{tag}-lk{r}.err')).read()
                       for r in range(len(lks)))
            if done == len(lks):
                steady_at = round(time.monotonic() - t0, 1)
        s = sample('steady' if steady_at is not None else 'ramp', lks)
        row = samples[-1]
        reason = None
        if s is None:
            server_exit = {'t': row['t'], 'code': server.poll()}
            reason = f'{target} exited (code {server.poll()})'
        elif s[1] > STOP_RSS:
            reason = f'server RSS {round(s[1] / 1e6)} MB above {STOP_RSS // 10 ** 6} MB'
        hot_s = hot_s + 1 if row['tempC'] > STOP_TEMP_C else 0
        very_hot_s = very_hot_s + 1 if row['tempC'] > HARD_STOP_TEMP_C else 0
        if hot_s >= STOP_TEMP_S:
            reason = f'above {STOP_TEMP_C} °C for {hot_s} s'
        if very_hot_s >= HARD_STOP_TEMP_S:
            reason = f'above {HARD_STOP_TEMP_C} °C for {very_hot_s} s'
        if row['load1'] > STOP_LOAD:
            reason = f'load average {row["load1"]} above {STOP_LOAD}'
        if time.monotonic() > deadline:
            reason = 'lk did not finish in time'
        if reason:
            stops.append({'t': row['t'], 'reason': reason})
            print(f'STOP: {reason}', flush=True)
            for p in lks:
                stop_group(p, signal.SIGINT, 5)
            break
    end_lk = round(time.monotonic() - t0, 1)
    # Two seconds after the participants left.
    for _ in range(2):
        time.sleep(1)
        if sample('after', []) is None and server_exit is None:
            server_exit = {'t': samples[-1]['t'], 'code': server.poll()}
    stop_group(server, signal.SIGTERM, 10)

    subs = []
    errors = []
    for r in range(len(lks)):
        subs += parse_lk(open(os.path.join(LOGS, f'{tag}-lk{r}.out')).read(), r)
        for line in open(os.path.join(LOGS, f'{tag}-lk{r}.err')):
            if 'could not connect' in line or 'subscription failed' in line or 'panic' in line:
                errors.append(line.split(':')[0].strip()[:80])
    # What lk expects each subscriber to receive: a track per publisher and kind.
    expected = sc['video'] + sc['audio']
    want_subs = sc['rooms'] * sc['subscribers']
    with_media = sum(1 for s in subs if s.get('tracks') == expected
                     and len(s['trackStats']) == expected
                     and all(t['packets'] > 0 for t in s['trackStats']))
    tracks = [t for s in subs for t in s['trackStats']]
    pk = sum(t['packets'] for t in tracks)
    dr = sum(t['dropped'] or 0 for t in tracks)
    loss_pct = round(100 * dr / (pk + dr), 3) if pk + dr else None
    sub_loss = [100 * sum(t['dropped'] or 0 for t in s['trackStats'])
                / max(1, sum(t['packets'] + (t['dropped'] or 0) for t in s['trackStats']))
                for s in subs]
    steady = [x for x in samples if x['phase'] == 'steady' and 'cpu' in x]
    loaded = [x for x in samples if x['phase'] in ('ramp', 'steady') and 'cpu' in x]
    idle_rss = max(x['rssMB'] for x in samples if x['phase'] == 'idle')
    rss_peak = max(x.get('rssMB', 0) for x in samples)
    cpu = stats(x['cpu'] for x in steady)
    cpu_all = stats(x['cpu'] for x in loaded)
    video_kbps = [t['kbps'] for t in tracks if t['kind'] == 'video']
    audio_kbps = [t['kbps'] for t in tracks if t['kind'] == 'audio']
    result = {
        'target': target,
        'scenario': scenario,
        'about': sc['about'],
        'at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'lk': {'version': LK_VERSION, 'durationS': DURATION_S, 'joinsPerS': JOINS_PER_S,
               'videoCodec': 'h264', 'simulcast': True, 'layout': 'speaker'},
        'rooms': sc['rooms'], 'videoPublishers': sc['video'], 'audioPublishers': sc['audio'],
        'subscribersPerRoom': sc['subscribers'], 'participants': participants,
        'memoryCap': memory_cap, 'tempSource': tsource, 'cpus': os.cpu_count(),
        'before': before,
        'serverUpS': up_s,
        'steadyFromS': steady_at, 'lkEndS': end_lk,
        'stops': stops,
        'serverExit': server_exit,
        'server': {
            'cpuSteady': cpu, 'cpuRampAndSteady': cpu_all,
            'cpuPerParticipantMilli': round(1000 * cpu['mean'] / participants, 1) if cpu else None,
            'rssIdleMB': idle_rss, 'rssPeakMB': rss_peak,
            'rssPerParticipantMB': round((rss_peak - idle_rss) / participants, 2),
            'threadsIdle': max(x['threads'] for x in samples if x['phase'] == 'idle'),
            'threadsPeak': max(x.get('threads', 0) for x in samples),
        },
        'loadGenerator': {'cpuSteady': stats(x['lkCpu'] for x in steady)},
        'machine': {'load1': stats(x['load1'] for x in loaded),
                    'tempC': stats(x['tempC'] for x in loaded),
                    'cpuBusy': stats(x['machineCpu'] for x in loaded)},
        'subscribers': {
            'expected': want_subs, 'reported': len(subs), 'withAllMedia': with_media,
            'tracksExpected': want_subs * expected, 'tracksReceived': len(tracks),
            'tracksWithoutPackets': sum(1 for t in tracks if t['packets'] == 0),
            'packets': pk, 'dropped': dr, 'lossPct': loss_pct,
            'worstSubscriberLossPct': round(max(sub_loss), 3) if sub_loss else None,
            'kbpsPerSubscriber': stats(s.get('kbps') for s in subs),
            'videoKbpsPerTrack': {'median': median(video_kbps),
                                  'min': min(video_kbps) if video_kbps else None,
                                  'max': max(video_kbps) if video_kbps else None},
            'audioKbpsPerTrack': {'median': median(audio_kbps)},
            'errors': len(errors) + sum(1 for s in subs if s.get('error')),
            'errorLines': sorted(set(errors))[:10],
        },
        'perSubscriber': subs,
        'samples': samples,
    }
    s = result['subscribers']
    result['summary'] = (
        f'{target} {scenario}: cpu {fmt(cpu, "mean")} avg / {fmt(cpu, "max")} peak cores, '
        f'rss {idle_rss:.0f} -> {rss_peak:.0f} MB, threads {result["server"]["threadsPeak"]}, '
        f'subs with media {with_media}/{want_subs}, tracks {s["tracksReceived"]}/{s["tracksExpected"]}, '
        f'loss {loss_pct}% (worst sub {s["worstSubscriberLossPct"]}%), '
        f'{fmt(s["kbpsPerSubscriber"], "mean")} kbps/sub, errors {s["errors"]}, '
        f'lk {fmt(result["loadGenerator"]["cpuSteady"], "mean")} cores, '
        f'temp max {fmt(result["machine"]["tempC"], "max")} °C, '
        f'load max {fmt(result["machine"]["load1"], "max")}, '
        f'machine busy {fmt(result["machine"]["cpuBusy"], "mean")} cores'
        + (f', STOPPED: {stops[0]["reason"]}' if stops else '')
        + (f', SERVER EXITED at {server_exit["t"]} s' if server_exit else ''))
    # A run that was stopped and is run again keeps the record of the stop.
    path = os.path.join(RESULTS, f'{tag}.json')
    if os.path.exists(path):
        old = json.load(open(path))
        earlier = old.get('earlierStoppedRuns', [])
        if old['stops'] or old['serverExit']:
            earlier.append({k: old[k] for k in ('at', 'stops', 'serverExit', 'steadyFromS', 'lkEndS',
                                                'summary')})
        if earlier:
            result['earlierStoppedRuns'] = earlier
    with open(path, 'w') as f:
        f.write(dump(result))
    print(result['summary'], flush=True)


def dump(result):
    """JSON with a line for each sample and each subscriber."""
    lists = ('perSubscriber', 'samples')
    head = json.dumps({k: v for k, v in result.items() if k not in lists}, indent=1,
                      ensure_ascii=False)
    tail = ''.join(f',\n "{k}": [\n  '
                   + ',\n  '.join(json.dumps(x, separators=(',', ':')) for x in result[k])
                   + '\n ]' for k in lists)
    return head[:-2] + tail + '\n}\n'


def fmt(st, key):
    return '-' if not st else st[key]


if __name__ == '__main__':
    main()
