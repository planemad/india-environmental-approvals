import argparse
import csv
import glob
import io
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone

STATES = {
    '1': 'Jammu And Kashmir', '2': 'Himachal Pradesh', '3': 'Punjab', '4': 'Chandigarh', '5': 'Uttarakhand',
    '6': 'Haryana', '7': 'Delhi', '8': 'Rajasthan', '9': 'Uttar Pradesh', '10': 'Bihar', '11': 'Sikkim',
    '12': 'Arunachal Pradesh', '13': 'Nagaland', '14': 'Manipur', '15': 'Mizoram', '16': 'Tripura',
    '17': 'Meghalaya', '18': 'Assam', '19': 'West Bengal', '20': 'Jharkhand', '21': 'Odisha',
    '22': 'Chhattisgarh', '23': 'Madhya Pradesh', '24': 'Gujarat', '27': 'Maharashtra', '28': 'Andhra Pradesh',
    '29': 'Karnataka', '30': 'Goa', '31': 'Lakshadweep', '32': 'Kerala', '33': 'Tamil Nadu', '34': 'Puducherry',
    '35': 'Andaman And Nicobar Islands', '36': 'Telangana', '37': 'Ladakh',
    '38': 'Dadra And Nagar Haveli And Daman And Diu',
}
GRANTED = 'EC Granted'
PIPE = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipeline.json'), encoding='utf-8'))
RULES = [(re.compile(r['re'], re.I), r['cat']) for r in PIPE['rules']]
BAR_CATS = [c['id'] for c in PIPE['categories'] if c['bar']]
STATUS = 'Last Visible Status'
PCAT = 'Project Category (Code)'
PC_CODES = ['A', 'B1', 'B2', 'OTHER']
TYPE_CODES = ['ec', 'forest', 'wildlife', 'crz', 'other']
COST = 'Total Cost (Lakhs)'
LAND = 'Project Land Requirement (Hectares)'
OUT = 'csv/Dashboard.csv'
COLUMNS = ['scope', 'state_code', 'state', 'section', 'rank', 'label', 'value', 'proposal_number', 'project_name',
           'organization', 'district', 'status', 'application_date', 'grant_date', 'total_cost_lakhs', 'land_ha',
           'proposal_url']
WINDOW_DAYS = 30
MAX_COST_LAKHS = 1e7
TOP = 10
RECENT = 10


def norm(v):
    return ' '.join((v or '').split())


def pc_code(r):
    c = norm(r.get(PCAT)).upper()
    return c if c in PC_CODES else 'OTHER'


def type_code(r):
    cat, desc = norm(r.get('Category')).upper(), norm(r.get('Description')).lower()
    if cat == 'CRZ' or 'crz' in desc:
        return 'crz'
    if 'wildlife' in desc:
        return 'wildlife'
    if cat in ('EC', 'TOR'):
        return 'ec'
    if cat == 'OTHER' or 'pollution load' in desc or not desc:
        return 'other'
    return 'forest' if cat == '' else 'other'


def classify(status):
    s = norm(status)
    return next(cat for rx, cat in RULES if rx.search(s))


def cost(r):
    v = num(r.get(COST))
    return v if v <= MAX_COST_LAKHS else 0.0


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def git(*args):
    r = subprocess.run(['git', *args], capture_output=True)
    return r.stdout.decode('utf-8') if r.returncode == 0 else None


def read_rows(text):
    return {r['ID']: r for r in csv.DictReader(io.StringIO(text)) if r.get('ID')}


def load_baseline(path):
    head = git('show', f'HEAD:{path}')
    if head is None:
        return None, None
    if os.path.exists(path) and open(path, newline='', encoding='utf-8').read() == head:
        commits = (git('log', '-2', '--format=%H', '--', path) or '').split()
        if len(commits) < 2:
            return None, None
        ref = commits[1]
        head = git('show', f'{ref}:{path}')
    else:
        ref = 'HEAD'
    date = (git('log', '-1', '--format=%aI', ref) or '').strip()
    return read_rows(head), date


def load_window(path, days=WINDOW_DAYS):
    ref = (git('rev-list', '-1', f'--before={days} days ago', 'HEAD', '--', path) or '').strip()
    if not ref:
        ref = ((git('rev-list', '--reverse', 'HEAD', '--', path) or '').split() or [''])[0]
    text = git('show', f'{ref}:{path}') if ref else None
    if text is None:
        return None, ''
    return read_rows(text), (git('log', '-1', '--format=%aI', ref) or '').strip()


def diff_rows(cur, prev, since=''):
    if prev is None:
        return {'added': [], 'removed': 0, 'updated': 0, 'status': []}
    added = [r for r in cur.values() if (r.get('Application Date') or '')[:10] >= since[:10]]
    new_ids = {r['ID'] for r in added}
    removed = sum(1 for i in prev if i not in cur)
    updated, status = 0, []
    for i, r in cur.items():
        b = prev.get(i)
        if not b:
            continue
        if i not in new_ids and any(norm(b.get(f)) != norm(r.get(f)) for f in r if f in b):
            updated += 1
        if norm(b.get(STATUS)) != norm(r.get(STATUS)):
            status.append((norm(b.get(STATUS)) or '(none)', r))
    return {'added': added, 'removed': removed, 'updated': updated, 'status': status}


def active_only(m):
    return {i: r for i, r in m.items() if classify(r.get(STATUS)) != 'delisted'}


def diff_pair(cur, prev, since=''):
    d = diff_rows(cur, prev, since)
    d['a'] = diff_rows(active_only(cur), active_only(prev), since) if prev is not None else diff_rows(cur, None, since)
    return d


def project_cols(r):
    return {
        'state_code': r.get('_code', ''), 'state': r.get('_state', ''), 'proposal_number': r.get('Proposal Number', ''), 'project_name': r.get('Project Name', ''),
        'organization': r.get('Organization Name', ''), 'district': r.get('District', ''),
        'status': r.get(STATUS, ''), 'application_date': r.get('Application Date', ''),
        'grant_date': r.get('Grant Date', ''), 'total_cost_lakhs': r.get(COST, ''), 'land_ha': r.get(LAND, ''),
        'proposal_url': r.get('proposal_url', ''),
    }


def scope_rows(scope, code, name, rows, diff, run, compared, win):
    out = []

    def add(section, label='', value='', rank='', **extra):
        out.append({'scope': scope, 'state_code': code, 'state': name, 'section': section, 'rank': rank,
                    'label': label, 'value': value, **extra})

    by = {c: [r for r in rows if r['_cat'] == c] for c in BAR_CATS + ['delisted']}
    active = [r for r in rows if r['_cat'] != 'delisted']
    granted = by['granted']
    per_year = {c: {} for c in BAR_CATS}
    applied = {}
    per_pc = {c: {} for c in PC_CODES}
    cube = {}
    for r in active:
        y = (r.get('Application Date') or '')[:4]
        if y.isdigit():
            applied[y] = applied.get(y, 0) + 1
            per_year[r['_cat']][y] = per_year[r['_cat']].get(y, 0) + 1
            pc = per_pc[pc_code(r)]
            pc[y] = pc.get(y, 0) + 1
            k = (y, r['_cat'], pc_code(r), type_code(r))
            cube[k] = cube.get(k, 0) + 1
    stats = [
        ('projects', len(rows)), ('active_projects', len(active)), ('applications', len(active)),
        *[(f'ec_{c}', len(by[c])) for c in BAR_CATS],
        *[(f'pc_{c}', sum(1 for r in active if pc_code(r) == c)) for c in PC_CODES],
        *[(f'ty_{c}', sum(1 for r in active if type_code(r) == c)) for c in TYPE_CODES],
        ('delisted_or_removed', len(by['delisted'])),
        ('total_cost_lakhs', round(sum(cost(r) for r in active), 2)),
        ('cost_outliers_excluded', sum(1 for r in active if num(r.get(COST)) > MAX_COST_LAKHS)),
        ('total_land_ha', round(sum(num(r.get(LAND)) for r in active), 4)),
        ('latest_application', max((r.get('Application Date', '') for r in rows), default='')),
        ('latest_grant', max((r.get('Grant Date', '') for r in granted), default='')),
        ('changes_added', len(diff['added'])), ('changes_removed', diff['removed']),
        ('changes_updated', diff['updated']), ('changes_status', len(diff['status'])),
        ('compared_with', compared or ''),
        ('changes_added_active', len(diff['a']['added'])), ('changes_removed_active', diff['a']['removed']),
        ('changes_updated_active', diff['a']['updated']), ('changes_status_active', len(diff['a']['status'])),
        ('window_added_active', len(win['a']['added'])), ('window_updated_active', win['a']['updated']),
        ('window_added', len(win['added'])), ('window_updated', win['updated']), ('window_since', win['since']),
    ]
    for k, v in stats:
        add('stat', k, v)
    counts = {}
    for r in rows:
        s = r.get(STATUS) or '(none)'
        counts[s] = counts.get(s, 0) + 1
    for s, n in sorted(counts.items(), key=lambda x: -x[1]):
        add('status', s, n)
    for y in sorted(applied):
        add('applications_by_year', y, applied[y])
    for c in BAR_CATS:
        for y in sorted(per_year[c]):
            add(f'year_{c}', y, per_year[c][y])
    for c in PC_CODES:
        for y in sorted(per_pc[c]):
            add(f'year_pc_{c}', y, per_pc[c][y])
    for k in sorted(cube):
        add('cube', '|'.join(k), cube[k])
    for k, v in run.items():
        add('run', k, v)
    tops = [
        ('top_recent_ec', [r for r in granted if r.get('Grant Date')], lambda r: r['Grant Date']),
        ('top_cost', active, cost),
        ('top_land', active, lambda r: num(r.get(LAND))),
    ]
    for section, pool, key in tops:
        for i, r in enumerate(sorted(pool, key=key, reverse=True)[:TOP], 1):
            add(section, rank=i, **project_cols(r))
    changes = []
    for old, r in diff['status']:
        prio = 0 if r.get(STATUS) == GRANTED else 1
        changes.append((prio, r.get('Application Date', ''), f'status: {old} → {norm(r.get(STATUS)) or "(none)"}', r))
    for r in diff['added']:
        changes.append((2, r.get('Application Date', ''), 'added', r))
    changes.sort(key=lambda c: c[1], reverse=True)
    changes.sort(key=lambda c: c[0])
    for i, (_, _, label, r) in enumerate(changes[:RECENT], 1):
        add('recent_change', label=label, rank=i, **project_cols(r))
    return out


def previous_runs():
    prev = {}
    if not os.path.exists(OUT):
        return prev
    for r in csv.DictReader(open(OUT, newline='', encoding='utf-8')):
        if r['section'] == 'run' and r['scope'] != 'all':
            prev.setdefault(r['state_code'], {})[r['label']] = r['value']
    return prev


def read_status(status_dir, code):
    if not status_dir:
        return None
    p = os.path.join(status_dir, f'{code}.txt')
    return open(p).read().strip() if os.path.exists(p) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--status-dir')
    ap.add_argument('--run-url', default='')
    ap.add_argument('--run-date', default=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
    args = ap.parse_args()

    prev_runs = previous_runs()
    zero = lambda: {'added': [], 'removed': 0, 'updated': 0, 'status': []}
    all_win = {**zero(), 'since': '', 'a': zero()}
    all_rows, all_diff, per_state = [], {**zero(), 'a': zero()}, []
    for code, name in STATES.items():
        path = f'csv/Projects_{code}.csv'
        rows = list(read_rows(open(path, newline='', encoding='utf-8').read()).values()) if os.path.exists(path) else []
        for r in rows:
            r['_code'], r['_state'] = code, name
            r['_cat'] = classify(r.get(STATUS))
        prev, compared = load_baseline(path) if rows else (None, None)
        diff = diff_pair({r['ID']: r for r in rows}, prev, compared or '')
        wprev, wsince = load_window(path) if rows else (None, '')
        win = diff_pair({r['ID']: r for r in rows}, wprev, (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).strftime('%Y-%m-%d'))
        win['since'] = wsince
        outcome = read_status(args.status_dir, code)
        if outcome is None and not args.status_dir:
            outcome = 'success' if rows else None
        if outcome is None:
            run = prev_runs.get(code) or {'run_status': 'not_run', 'run_date': '', 'run_url': ''}
        else:
            state = 'failed' if outcome != 'success' else ('ok' if rows else 'no_data')
            run = {'run_status': state, 'run_date': args.run_date, 'run_url': args.run_url}
        per_state.append((code, name, rows, diff, run, compared, win))
        all_rows += rows
        for tot, d in ((all_diff, diff), (all_win, win)):
            for g, src in ((tot, d), (tot['a'], d['a'])):
                g['added'] += src['added']
                g['removed'] += src['removed']
                g['updated'] += src['updated']
                g['status'] += src['status']
        all_win['since'] = max(all_win['since'], win['since'])

    statuses = [p[4]['run_status'] for p in per_state]
    all_run = {'run_status': 'failed' if 'failed' in statuses else 'ok', 'run_date': args.run_date,
               'run_url': args.run_url, 'states_ok': statuses.count('ok'), 'states_failed': statuses.count('failed'),
               'states_no_data': statuses.count('no_data'), 'states_not_run': statuses.count('not_run'),
               'states_total': len(statuses)}
    compared_dates = [p[5] for p in per_state if p[5]]
    out = scope_rows('all', '', 'All India', all_rows, all_diff, all_run, max(compared_dates, default=''), all_win)
    for code, name, rows, diff, run, compared, win in per_state:
        out += scope_rows('state', code, name, rows, diff, run, compared, win)

    with open(OUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in out:
            w.writerow({c: r.get(c, '') for c in COLUMNS})
    print(f'Wrote {OUT}: {len(out)} rows, {len(all_rows)} projects')


if __name__ == '__main__':
    main()
