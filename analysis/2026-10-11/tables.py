"""Render readable result tables from the selected measurements in analysis.json."""

import argparse
import json
from pathlib import Path
import re
import statistics


HERE = Path(__file__).resolve().parent
CASES = {
    '10x100x1G': '1 GB', '10x1024x100M': '100 MB',
    '10x10x10x1024x1M': '1 MB', '10x10x10x10x1024x1K': '1 kB',
    'linux-source': 'Linux source',
}
FILESYSTEMS = {'lustre': 'Lustre', 'weka': 'Weka', 'nfs': 'NFS', 'xfs': 'XFS', 'zfs': 'ZFS'}
RECIPES = {
    'rcp-default': 'rcp auto', 'rcp-8': 'rcp 8', 'rcp-32': 'rcp 32', 'rcp-128': 'rcp 128',
    'dcp-8': 'dcp · 8 ranks', 'dcp-32': 'dcp · 32 ranks', 'dcp-default': 'dcp · 32 ranks',
    'dcp-128': 'dcp · 128 ranks', 'cp-1x': 'cp', 'cp-10x': 'cp · scripted ×10',
    'rsync-1x': 'rsync', 'rsync-10x': 'rsync · scripted ×10',
}
RESOURCE_COLUMNS = [
    ('copy_seconds', 'Wall time (s)', 1, 3),
    ('user_cpu_seconds', 'CPU user (s)', 1, 3),
    ('system_cpu_seconds', 'CPU system (s)', 1, 3),
    ('max_rss_kib', 'Peak RSS (MiB)', 1024, 1),
]
SUMMARY_IDS = ('network', 'local', 'remote', 'linux')
INTRO = """# Complete performance results

These tables accompany the [performance report](performance.md). The
[machine-readable analysis](../../analysis/2026-10-11/analysis.json) contains the full-precision
statistics, individual repetitions, commands, selection decisions, and provenance. The tables use
the same selected measurements as the figures, including the latest equivalent 32-rank dcp results.
The [reproduction guide](../../analysis/2026-10-11/README.md) explains how to regenerate them.

## Reading the tables

Each successful row has **three repetitions**. Values are **mean [minimum–maximum]**, with times in
seconds and memory in MiB (1 MiB = 1,048,576 bytes). The ranges describe the observed repetitions,
not confidence intervals. Peak RSS is the mean and range of the three recorded per-run peaks, not
average memory over the duration of a copy. Display rounding adds no measurement precision.

Wall time is the complete copy command's elapsed time, including startup and the launcher where
applicable. CPU time is split into user and system time; their sum is the accounted CPU time and can
exceed wall time when multiple threads or processes run concurrently. The tables retain the original
GNU-time accounting scope described below.

**Timeout rows have one unfinished attempt.** Their wall time is a lower bound, with no mean or
completion time. Any CPU/RSS value on that row describes only the interrupted attempt. A dash means
unavailable, never zero. Configurations excluded by the experiment's resource policy are identified
below their fileset; they are not failed copies.

## Resource accounting

| Measurement | Scope |
| --- | --- |
| Local rcp and cp/rsync recipes | GNU `time -v` around the command or its shell wrapper, including locally accounted children |
| dcp | GNU time around the MPI launcher; aggregate CPU and memory across ranks were not validated |
| Remote rcp | GNU time largely observes the master CLI; separate daemon measurements are shown under remote rcp roles |
| Remote rsync | Local command accounting only; receiver-host work is excluded |

**These CPU and memory columns are not a whole-job resource ranking.** In particular, a remote rcp
master's small CPU time does not represent its data-transfer workers. Peak RSS is a process
high-water mark, not simultaneous aggregate memory across a process tree, ten scripted commands,
MPI ranks, or both hosts. Linux child-resource accounting keeps the largest child's RSS, rather
than summing concurrent children; see the
[Linux resource-accounting documentation](https://man7.org/linux/man-pages/man2/getrusage.2.html).

The local XFS results include reflinking for rcp and cp; ZFS includes filesystem-assisted copying.
The report's [ZFS cache caveat](performance.md#local-filesystems) also applies to these tables.
Neither apparent bytes divided by wall time nor the GNU-time I/O counters measures physical
storage throughput for these comparisons.
"""


def recipe_label(name):
    return RECIPES[name]


def raw_link(document):
    return '../../analysis/2026-10-11/raw/' + document['source_path']


def marker(name, end=False):
    return f"[//]: # ({'END' if end else 'BEGIN'} GENERATED: {name}-summary)"


def markdown_table(headers, rows, text_columns=1):
    widths = [max(3, len(header), *(len(row[i]) for row in rows)) for i, header in enumerate(headers)]
    def line(cells):
        return '| ' + ' | '.join(cell.ljust(width) if i < text_columns else cell.rjust(width)
                                for i, (cell, width) in enumerate(zip(cells, widths))) + ' |'
    separator = ['-' * width if i < text_columns else '-' * (width - 1) + ':'
                 for i, width in enumerate(widths)]
    return '\n'.join([line(headers), '| ' + ' | '.join(separator) + ' |', *(line(row) for row in rows)])


def number(value, divisor=1, precision=3):
    return '—' if value is None else f'{value / divisor:,.{precision}f}'


def measurement(summary, divisor=1, precision=3, ranges=True):
    if summary is None:
        return '—'
    mean = number(summary['mean'], divisor, precision)
    if not ranges:
        return mean
    return f"{mean} [{number(summary['min'], divisor, precision)}–{number(summary['max'], divisor, precision)}]"


def resource_cells(variant, ranges=True):
    return [measurement(variant['statistics'][key], divisor, precision, ranges)
            for key, _, divisor, precision in RESOURCE_COLUMNS]


def ordered_variants(document):
    return sorted(document['variants'], key=lambda v: list(RECIPES).index(v['variant']))


def groups(payload):
    for scope, fs in [('local', fs) for fs in FILESYSTEMS] + [('remote', 'xfs')]:
        documents = [d for d in payload['results'] if d['scope'] == scope and d['filesystem'] == fs
                     and d['fileset']['name'] != 'linux-source']
        yield f'{scope.title()} {FILESYSTEMS[fs]}', sorted(documents, key=lambda d: list(CASES).index(d['fileset']['name']))
    yield 'Linux source', sorted([d for d in payload['results'] if d['fileset']['name'] == 'linux-source'],
                                key=lambda d: d['scope'])


def role_statistics(variant):
    """Aggregate the separately measured roles, without summing unrelated process peaks."""
    samples = variant['samples']
    if not any('rcp_runtime_stats' in sample for sample in samples):
        return {}
    expected = {'master', 'source', 'destination'}
    if any(set(sample.get('rcp_runtime_stats', {}).get('roles', {})) != expected for sample in samples):
        raise ValueError(f"Incomplete runtime role measurements: {variant['variant']}")
    summaries = {}
    for role in ('master', 'source', 'destination'):
        records = [sample['rcp_runtime_stats']['roles'][role] for sample in samples]
        summary = {}
        for key in ('cpu_total_seconds', 'cpu_user_seconds', 'cpu_system_seconds', 'peak_rss_bytes'):
            values = [record[key] for record in records]
            summary[key] = {'mean': statistics.mean(values), 'min': min(values), 'max': max(values)}
        summary['peak_rss_exact'] = all(record['peak_rss_exact'] for record in records)
        summaries[role] = summary
    return summaries


def render_results(payload):
    sections = [INTRO.rstrip(),
                f"The selected data contains {payload['audit']['selected_variants']} successful configurations "
                f"({payload['audit']['selected_successful_samples']} copies) and {len(payload['censored'])} timed-out attempts."]
    headers = ['Recipe'] + [column[1] for column in RESOURCE_COLUMNS]
    for group, documents in groups(payload):
        sections.append(f'## {group}')
        if group == 'Remote XFS' or group == 'Linux source':
            sections.append('**Remote CPU/RSS values below cover the timed local command only.** '
                            'See [resource accounting](#resource-accounting) and '
                            '[remote rcp roles](#remote-rcp-role-measurements).')
        for document in documents:
            fileset = document['fileset']
            label = document['scope'].title() + ' XFS' if group == 'Linux source' else CASES[fileset['name']] + ' files'
            sections.append(f'### {group}: {label}')
            sections.append(f"{fileset['files']:,} regular files; {fileset['apparent_bytes']:,} apparent bytes. "
                            f"[Original measurements]({raw_link(document)}).")
            rows = [[recipe_label(v['variant']), *resource_cells(v)] for v in ordered_variants(document)]
            for outcome in payload['censored']:
                if outcome['source_path'] == document['source_path']:
                    recorded = outcome['measurements']
                    rows.append([recipe_label(outcome['variant']),
                                 f"**Timed out ≥{outcome['time_limit_seconds']:,} s**",
                                 *[number(recorded[key], divisor, precision)
                                   for key, _, divisor, precision in RESOURCE_COLUMNS[1:]]])
            sections.append(markdown_table(headers, rows))
            excluded = [recipe_label(row['variant']) for row in payload['coverage']['configurations']
                        if row['source_path'] == document['source_path'] and row['status'] == 'documented_exclusion']
            if excluded:
                sections.append('Excluded by the experiment’s resource policy: ' + ', '.join(excluded) + '.')
    sections.extend([
        '## Additional timing measurements',
        'These supplemental tables use **means in seconds** over the same three successful repetitions. '
        'Copy + sync + validation is measured from copy start through destination sync and count/size '
        'validation, including harness overhead. It is not the command wall time used in the figures. '
        'The narrower timings were recorded only for Lustre and Weka; a dash means unavailable. '
        'The phase times need not sum exactly because harness overhead falls between phases. '
        'Minima, medians, maxima, and individual values remain in the JSON. Unfinished attempts are excluded.',
    ])
    for group, documents in groups(payload):
        sections.append(f'### {group} timing breakdown')
        rows = []
        detailed = group in {'Local Lustre', 'Local Weka'}
        columns = [('completion_seconds', 'Copy + sync + validation (s)')]
        if detailed:
            columns += [('copy_through_sync_seconds', 'Copy through sync (s)'),
                        ('sync_only_seconds', 'Sync only (s)'), ('validation_seconds', 'Validation (s)')]
        for document in documents:
            label = document['scope'].title() if group == 'Linux source' else CASES[document['fileset']['name']]
            for variant in ordered_variants(document):
                rows.append([label, recipe_label(variant['variant']),
                             *[measurement(variant['statistics'].get(key), ranges=False) for key, _ in columns]])
        sections.append(markdown_table(['Fileset / scope', 'Recipe', *[label for _, label in columns]], rows, 2))
    sections.extend([
        '## Remote rcp role measurements',
        'Generated remote rcp copies also recorded separate `--summary` measurements for the master, '
        'source daemon, and destination daemon. Each cell is **mean [minimum–maximum]** over three '
        'repetitions. Total CPU is the role’s recorded user + system time. Memory is that role’s '
        'per-process peak; the peaks are not added across roles. The ≈ prefix identifies RSS recovered '
        'from rounded human-readable summaries. These measurements are unavailable for the Linux '
        'source copies and for rsync. They must not be compared with the master-only GNU-time figures '
        'as if their scopes were the same.',
    ])
    for group, documents in groups(payload):
        if group != 'Remote XFS':
            continue
        for document in documents:
            sections.append(f"### Remote roles: {CASES[document['fileset']['name']]} files")
            rows = []
            for variant in ordered_variants(document):
                for role, summary in role_statistics(variant).items():
                    memory = measurement(summary['peak_rss_bytes'], 1024 ** 2, 1)
                    if not summary['peak_rss_exact']:
                        memory = '≈' + memory
                    rows.append([recipe_label(variant['variant']), role,
                                 *[measurement(summary[key]) for key in ('cpu_total_seconds', 'cpu_user_seconds', 'cpu_system_seconds')],
                                 memory])
            sections.append(markdown_table(['Recipe', 'Role', 'CPU total (s)', 'CPU user (s)', 'CPU system (s)', 'Peak RSS (MiB)'], rows, 2))
    sections.extend([
        '## Other recorded evidence',
        'The [original packet](../../analysis/2026-10-11/raw/) also retains GNU-time logs with major '
        'and minor page faults, voluntary and involuntary context switches, CPU utilization, and '
        'filesystem input/output counters. They have the same local accounting limitations. Some '
        'GNU-time fields, including socket-message and swap counters, are unmaintained on Linux; '
        'their printed zeros are not evidence of zero activity. See the '
        '[Linux resource-accounting documentation](https://man7.org/linux/man-pages/man2/getrusage.2.html).',
        'The packet retains source/destination file counts and apparent byte counts, exit status, '
        'command logs, environment and cache-preparation evidence, and timestamps for provenance. '
        'It does not provide validated whole-job memory, physical byte traffic, or per-copy content '
        'hashes. The [report’s limitations](performance.md#8-limitations) apply throughout.',
    ])
    return '\n\n'.join(sections) + '\n'


def summary_blocks(payload):
    blocks = {name: [] for name in SUMMARY_IDS}
    columns = [column[1] for column in RESOURCE_COLUMNS]
    for group, documents in groups(payload):
        name = ('linux' if group == 'Linux source' else 'remote' if group == 'Remote XFS'
                else 'network' if group in {'Local Lustre', 'Local Weka', 'Local NFS'} else 'local')
        rows = []
        for document in documents:
            for variant in ordered_variants(document):
                if name in {'local', 'network'} and variant['variant'] not in {'rcp-default', 'dcp-32'}:
                    continue
                if name == 'remote' and variant['variant'] not in {'rcp-default', 'rsync-1x', 'rsync-10x'}:
                    continue
                label = document['scope'].title() if name == 'linux' else CASES[document['fileset']['name']]
                rows.append([label, recipe_label(variant['variant']), *resource_cells(variant, ranges=False)])
        if name in {'local', 'network'}:
            blocks[name].append(f'#### {group} summary')
        blocks[name].append(markdown_table(['Scope' if name == 'linux' else 'File size', 'Recipe', *columns], rows, 2))
    return {name: '\n\n'.join(parts) for name, parts in blocks.items()}


def write_reports(payload, reports, check=False):
    report = reports / 'performance.md'
    original = report.read_text(encoding='utf-8')
    updated = original
    for name, table in summary_blocks(payload).items():
        start, end = marker(name), marker(name, end=True)
        if updated.count(start) != 1 or updated.count(end) != 1 or updated.index(start) > updated.index(end):
            raise ValueError(f'Missing or duplicate generated table markers: {name}')
        before, remainder = updated.split(start)
        _, after = remainder.split(end)
        updated = before + start + '\n\n' + table + '\n\n' + end + after
    outputs = {report: updated, reports / 'performance-results.md': render_results(payload)}
    stale = [path for path, text in outputs.items()
             if not path.exists() or normalized_markdown(path.read_text(encoding='utf-8')) != normalized_markdown(text)]
    if check and stale:
        raise ValueError('Stale generated tables: ' + ', '.join(str(path) for path in stale) +
                         '; run just report-tables, then just fmt')
    if not check:
        for path, text in outputs.items():
            path.write_text(text, encoding='utf-8')


def normalized_markdown(text):
    """Ignore formatter padding and wrapping while retaining cells and column alignment."""
    text = re.sub(r'^\|[ \t:|\-]+\|$', lambda match: re.sub(r'-+', '-', match[0]), text, flags=re.MULTILINE)
    return text.split()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--analysis', type=Path, default=HERE / 'analysis.json')
    parser.add_argument('--reports-dir', type=Path, default=HERE.parents[1] / 'docs/reports')
    parser.add_argument('--check', action='store_true', help='fail on stale tables without writing files')
    args = parser.parse_args()
    try:
        write_reports(json.loads(args.analysis.read_text()), args.reports_dir, args.check)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f'result tables: {error}\n')
    print('Result tables match the selected analysis.' if args.check else 'Wrote report summaries and complete result tables.')


if __name__ == '__main__':
    main()
