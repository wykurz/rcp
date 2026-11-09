"""Build the RCP landing page and reports beside optional CI benchmark history."""

import argparse
import html
from html.parser import HTMLParser
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
from string import Template
import sys
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from markdown_it import MarkdownIt

# support direct script execution as well as imports from the repository root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmarks.site import read_theme


REPOSITORY = 'https://github.com/wykurz/rcp'
ASSETS = Path(__file__).with_name('report-site')


def relative_url(target, page):
    return Path(os.path.relpath(target, page.parent)).as_posix()


def site_header(route, *, has_aws_history=False):
    """Share navigation while keeping landing-page anchors in the current document."""
    home = '' if route == Path('index.html') else relative_url(Path('index.html'), route)
    return Template((ASSETS / 'header.html').read_text(encoding='utf-8')).substitute(
        home=home, logo=relative_url(Path('assets/logo.svg'), route),
        reports=relative_url(Path('reports/index.html'), route), repository=REPOSITORY,
        aws_benchmarks=(f'<a href="{relative_url(Path("aws-benchmarks/index.html"), route)}">AWS benchmarks</a>'
                        if has_aws_history else ''),
    )


def featured_measurements(source):
    """Read featured values from the report's checked tables, including their workload context."""
    tokens = MarkdownIt('commonmark').enable('table').parse(source.read_text(encoding='utf-8'))
    rows, section, cells = [], '', None
    for index, token in enumerate(tokens):
        if token.type == 'heading_open':
            section = tokens[index + 1].content
        elif token.type == 'table_open':
            columns = None
        elif token.type == 'tr_open':
            cells = []
        elif token.type == 'inline' and cells is not None:
            cells.append(token.content)
        elif token.type == 'tr_close':
            if columns is None:
                columns = cells
            else:
                rows.append((section, dict(zip(columns, cells, strict=True))))
            cells = None
    def value(column, match, section=None):
        matches = [row[column] for heading, row in rows if column in row
                   and (section is None or heading == section)
                   and all(row.get(key) == expected for key, expected in match.items())]
        if len(matches) != 1:
            raise ValueError(f'Missing or ambiguous landing-page measurement in {source.name}: {section}, {match}, {column}')
        return matches[0]
    def seconds(match, section):
        value_text = value('Wall time (s)', match, section)
        try:
            result = float(value_text.replace(',', ''))
        except ValueError:
            result = math.nan
        if not math.isfinite(result) or result <= 0:
            raise ValueError(f'Invalid landing-page measurement in {source.name}: {value_text}')
        return result
    minutes = {f'{filesystem}_{tool}': seconds({'File size': '1 MB', 'Recipe': recipe},
                                             f'Local {filesystem.title()} summary') / 60
               for filesystem in ('lustre', 'weka')
               for tool, recipe in (('rcp', 'rcp auto'), ('dcp', 'dcp · 32 ranks'))}
    scale = math.ceil(max(minutes.values()) / 50) * 50
    featured = {key: f'{number:.1f}' for key, number in minutes.items()}
    featured.update({f'{key}_width': f'{number / scale * 100:.1f}' for key, number in minutes.items()})
    featured.update(scale=scale, midpoint=f'{scale / 2:g}',
                    files=html.escape(value('Regular files', {'File size': '1 MB'})),
                    total_bytes=html.escape(value('Total file bytes', {'File size': '1 MB'})))
    linux = {tool: f"{seconds({'Scope': 'Local', 'Recipe': recipe}, '5 Linux source tree'):.1f}"
             for tool, recipe in (('rcp', 'rcp auto'), ('cp', 'cp'))}
    if linux['rcp'] == linux['cp']:
        featured['linux_comparison'] = f"local rcp and cp both take about {linux['rcp']} seconds"
    else:
        featured['linux_comparison'] = f"local rcp takes about {linux['rcp']} seconds and cp about {linux['cp']} seconds"
    return featured


def redirect_script(target, *, run_links_only=False):
    """Keep existing job-summary bookmarks, including their query strings and fragments."""
    condition = "location.hash.startsWith('#run-')" if run_links_only else 'true'
    return f'''<script id="legacy-dashboard">
(() => {{
  function redirect() {{
    if ({condition}) location.replace({json.dumps(target)} + location.search + location.hash);
  }}
  redirect();
  window.addEventListener('hashchange', redirect);
}})();
</script>'''


def landing_page(root, routes, has_dashboard, has_aws_history=False):
    """Render the project introduction with links to its maintained performance reports."""
    report_links = {}
    for key, name in (('reports', 'README'), ('performance', 'performance'), ('results', 'performance-results')):
        source = root / f'docs/reports/{name}.md'
        if source not in routes:
            raise ValueError(f'Missing landing-page report: {name}.md')
        report_links[key] = relative_url(routes[source], Path('index.html'))
    return Template((ASSETS / 'landing.html').read_text(encoding='utf-8')).substitute(
        theme=read_theme(), header=site_header(Path('index.html'), has_aws_history=has_aws_history), repository=REPOSITORY,
        legacy_redirect=redirect_script('ci-benchmarks/index.html', run_links_only=True) if has_dashboard else '',
        **report_links, **featured_measurements(root / 'docs/reports/performance.md'),
    )


class RepositoryLinks(HTMLParser):
    """Send generated navigation through the same Markdown section checks as report links."""
    def __init__(self, page, root):
        super().__init__()
        self.root = root
        self.sections = []
        self.feed(page)

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.add_link(dict(attrs).get('href', ''))

    def add_link(self, value):
        url = urlsplit(value)
        if not url.fragment:
            return
        base = urlunsplit((url.scheme, url.netloc, url.path.rstrip('/'), '', ''))
        if base == REPOSITORY:
            target = self.root / 'README.md'
        elif base.startswith(REPOSITORY + '/blob/main/') and url.path.endswith('.md'):
            target = (self.root / unquote(base.removeprefix(REPOSITORY + '/blob/main/'))).resolve()
        else:
            return
        if not target.is_relative_to(self.root) or not target.is_file():
            raise ValueError(f'Missing repository reference: {value}')
        self.sections.append((target, unquote(url.fragment), value))


def walk(tokens):
    for token in tokens:
        yield token
        if token.children:
            yield from walk(token.children)


def heading_ids(tokens, reserved=()):
    """Use the same heading slugs for rendered navigation and Markdown link checks."""
    seen = set(reserved)
    for index, token in enumerate(tokens):
        if token.type != 'heading_open':
            continue
        text = ''.join(child.content for child in tokens[index + 1].children
                       if child.type in {'text', 'code_inline'})
        base = re.sub(r'[^\w\s-]', '', text.lower()).replace(' ', '-')
        slug, suffix = base, 0
        while slug in seen:
            suffix += 1
            slug = f'{base}-{suffix}'
        seen.add(slug)
        yield token, text, slug


def render(source, root, route, routes, *, has_aws_history=False):
    """Resolve Markdown references and render a page with working heading navigation."""
    parser = MarkdownIt('commonmark', {'html': False}).enable('table')
    tokens = parser.parse(source.read_text(encoding='utf-8'))
    assets = {}
    sections = []
    repository_links = RepositoryLinks('', root)
    toc = []
    seen = {'report-content', 'top'}
    title = None
    for token, text, slug in heading_ids(tokens, seen):
        seen.add(slug)
        token.attrSet('id', slug)
        if token.tag == 'h1' and title is None:
            title = text
        if token.tag in {'h2', 'h3'}:
            toc.append(f'<li class="level-{token.tag}"><a href="#{html.escape(slug)}">{html.escape(text)}</a></li>')
    for index, token in enumerate(tokens):
        if token.type == 'paragraph_open' and tokens[index + 1].type == 'inline':
            if any(child.type == 'image' for child in tokens[index + 1].children):
                token.attrSet('class', 'figure')
    for token in walk(tokens):
        attribute = 'src' if token.type == 'image' else 'href' if token.type == 'link_open' else None
        if attribute is None:
            continue
        value = token.attrGet(attribute)
        url = urlsplit(value)
        if token.type == 'image' and (url.scheme or url.netloc or not url.path):
            raise ValueError(f'Report images must be local SVG files under docs/reports: {value}')
        if url.scheme or url.netloc:
            repository_links.add_link(value)
            continue
        if not url.path:
            if url.fragment:
                sections.append((source, unquote(url.fragment), value))
            continue
        target = (source.parent / unquote(url.path)).resolve()
        if not target.is_relative_to(root) or not target.exists():
            raise ValueError(f'Missing report reference in {source.name}: {value}')
        if token.type == 'image':
            if target.suffix != '.svg' or not target.is_relative_to(root / 'docs/reports'):
                raise ValueError(f'Report images must be local SVG files under docs/reports: {value}')
            destination = Path('reports') / target.relative_to(root / 'docs/reports')
            assets[destination] = target
            rewritten = relative_url(destination, route)
        elif target in routes:
            rewritten = relative_url(routes[target], route)
            if url.fragment:
                sections.append((target, unquote(url.fragment), value))
        else:
            kind = 'tree' if target.is_dir() else 'blob'
            rewritten = f'{REPOSITORY}/{kind}/main/{quote(target.relative_to(root).as_posix())}'
            if url.fragment and target.suffix.lower() == '.md':
                sections.append((target, unquote(url.fragment), value))
        parsed = urlsplit(rewritten)
        token.attrSet(attribute, urlunsplit((parsed.scheme, parsed.netloc, parsed.path, url.query, url.fragment)))
    if not title:
        raise ValueError(f'Report needs a level-one title: {source}')
    image_rule = parser.renderer.rules['image']
    parser.renderer.rules['image'] = lambda items, index, options, env: (
        f'<a class="figure-link" href="{html.escape(items[index].attrGet("src"), quote=True)}" title="Open full-size SVG">'
        + image_rule(items, index, options, env) + '</a>'
    )
    parser.renderer.rules['table_open'] = lambda *_: '<div class="table-scroll"><table>\n'
    parser.renderer.rules['table_close'] = lambda *_: '</table></div>\n'
    body = parser.renderer.render(tokens, parser.options, {})
    source_url = f'{REPOSITORY}/blob/main/{quote(source.relative_to(root).as_posix())}'
    header = site_header(route, has_aws_history=has_aws_history)
    repository_links.feed(header)
    sections.extend(repository_links.sections)
    page = Template((ASSETS / 'page.html').read_text(encoding='utf-8')).substitute(
        title=html.escape(title), body=body, toc='\n'.join(toc),
        theme=read_theme(), header=header,
        site_stylesheet=relative_url(Path('assets/site.css'), route),
        stylesheet=relative_url(Path('assets/report.css'), route),
        reports=relative_url(Path('reports/index.html'), route),
        source=html.escape(source_url, quote=True),
    )
    return page, assets, seen, sections


def aws_benchmark_pages(repository):
    """Read legacy Git history as data, never checked-out code."""
    from infra.aws_benchmark import production_publish
    records, commit = production_publish.history(repository)
    if not records:
        return {}
    return render_aws_benchmarks(records, footer=(
        '<p>Legacy read-only history. History commit: <code>' + html.escape(commit) + '</code></p>'))


def retained_aws_benchmark_pages(directory, reader_revision, inventory_sha256):
    """Consume a bounded, proof-bound directory without consulting Git history."""
    from infra.aws_benchmark import production_reports
    records, index = production_reports.public_batch(directory, reader_revision, inventory_sha256)
    rows = []
    for observation in [index['active'], *index['observations']]:
        fields = (observation.get('run_key', '—'), observation['state'],
                  observation.get('phase', '—'), observation.get('reason', '—'),
                  'yes' if observation['retired'] else 'no')
        rows.append('<tr>' + ''.join('<td>' + html.escape(value) + '</td>' for value in fields) + '</tr>')
    processing = 'complete' if index['processing_complete'] else 'incomplete; some records are deferred'
    status = (
        '<section aria-labelledby="collection-status"><h2 id="collection-status">Snapshot status</h2>'
        '<p>Observed at: <time>' + html.escape(index['observed_at']) + '</time>. '
        'Discovery: complete. Record processing: ' + processing + '.</p>'
        '<p>The first row is the lease observation at that time; it is not a fresh physical teardown check. '
        'Retirement and cleanup verification belong to individual retained records. '
        'Pending, held and deferred entries are not successful measurements.</p>'
        '<div class="table-scroll"><table><thead><tr><th>Run</th><th>State</th><th>Phase</th>'
        '<th>Details</th><th>Retirement recorded</th></tr></thead><tbody>' + ''.join(rows) +
        '</tbody></table></div></section>')
    if not records:
        empty = ('Discovery and processing completed; no retained public records were found.'
                 if index['processing_complete'] and not index['observations'] and index['active']['state'] == 'idle'
                 else 'No public records are available to display in this snapshot. See the observations below.')
        status = '<p>' + empty + '</p>' + status
    diagnostics = production_reports.public_diagnostics(directory, index, records=records)
    return render_aws_benchmarks(records, diagnostics=diagnostics, retained=True, status=status, footer=(
        '<p>Reader revision: <code>' + html.escape(reader_revision) + '</code><br>'
        'Snapshot inventory SHA256: <code>' + html.escape(inventory_sha256) + '</code></p>'))


def render_aws_benchmarks(records, *, diagnostics=None, retained=False, status='', footer=''):
    """Render frozen public JSON and numeric companions; never retained HTML or binaries."""
    from infra.aws_benchmark import production_export, production_history
    title = 'AWS benchmarks · retained records' if retained else 'AWS benchmark history'
    retention = ('<p>Still-retained records under the 90-day AWS lifecycle; deletion timing can vary. '
                 'This view is not permanent history.</p>' if retained else '')
    pages, rows, setups, normalized = {}, [], {}, {}
    diagnostics = diagnostics or {}
    if not set(diagnostics) <= set(records):
        raise ValueError('Resource diagnostics have no frozen public record')
    for run_key, raw in sorted(records.items(), key=lambda item: int(item[0].rsplit('-', 1)[1]), reverse=True):
        value = production_export.validate_public(production_export.results.json_value(
            raw, production_export.MAX_PUBLIC_RECORD))
        normalized[run_key] = value
        identity = value['identity']
        report = Path('aws-benchmarks/runs') / (run_key + '.html')
        record = Path('aws-benchmarks/records') / (run_key + '.json')
        pages[record] = raw.decode('utf-8')
        extra = diagnostics.get(run_key)
        if extra is None:
            extra = production_export.resource_diagnostics.unavailable(value)
        setup_href = None
        if extra is not None:
            production_export.resource_diagnostics.validate(extra, value)
            if extra['setup'] is not None:
                setups[extra['setup_id']] = extra['setup']
                setup_href = '../setups.html#setup-' + extra['setup_id']
            pages[Path('aws-benchmarks/diagnostics') / (run_key + '.json')] = (
                production_export.collection.encoded(extra).decode('utf-8'))
        pages[report] = production_export.render(value, diagnostics=extra, setup_href=setup_href) + retention + (
            '<p><a href="../index.html">' + html.escape(title) + '</a> · '
            f'<a href="../records/{run_key}.json">Public JSON record</a> · '
            '<a href="../../index.html#top">RCP Tools</a></p>')
        qualification = production_export.projection.outcome_label(value)
        fields = (identity['run'], production_export.projection.collected_time(value), identity['suite'], value['comparison'], qualification)
        cells = '<td><a href="runs/' + run_key + '.html">' + html.escape(fields[0]) + '</a></td>'
        cells += ''.join('<td>' + html.escape(field) + '</td>' for field in fields[1:])
        rows.append('<tr>' + cells + '</tr>')
    setup_content = production_export.resource_diagnostics.configuration_html(setups)
    if len(setup_content.encode('utf-8')) > production_export.MAX_PUBLIC_HTML:
        raise ValueError('Central AWS setup page exceeds public HTML bound')
    pages[Path('aws-benchmarks/setups.html')] = (
        "<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
        "<title>AWS benchmark setup</title>" + read_theme() +
        "<link rel='stylesheet' href='../assets/site.css'><link rel='stylesheet' href='../assets/report.css'>"
        "<body>" + site_header(Path('aws-benchmarks/setups.html'), has_aws_history=True) +
        "<main>" + setup_content + "<p><a href='index.html'>AWS benchmark runs</a></p></main></body></html>")
    history_content = production_history.render(normalized, diagnostics)
    pages[Path('aws-benchmarks/history.html')] = (
        "<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
        "<title>AWS benchmark history plots</title>" + read_theme() +
        "<link rel='stylesheet' href='../assets/site.css'><link rel='stylesheet' href='../assets/report.css'>"
        "<body>" + site_header(Path('aws-benchmarks/history.html'), has_aws_history=True) +
        "<main>" + history_content + "<p><a href='index.html'>AWS benchmark runs</a> · "
        "<a href='setups.html'>Hardware and test setup</a></p></main></body></html>")
    pages[Path('aws-benchmarks/index.html')] = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)} · RCP Tools</title>{read_theme()}
<link rel="stylesheet" href="../assets/site.css"><link rel="stylesheet" href="../assets/report.css"></head>
<body><a class="skip-link" href="#report-content">Skip to report</a>
{site_header(Path('aws-benchmarks/index.html'), has_aws_history=True)}
<main id="report-content"><h1>{html.escape(title)}</h1>{retention}
<p><a href="history.html">Duration, CPU and memory history plots by file set</a></p>
<p>Retired AWS runs with verified resource termination. Failed consistency checks and unavailable evidence remain
visible and are excluded from performance history. These records are separate from historical Depot measurements.</p>
<p><a href="setups.html">Hardware, storage, filesystem and test setup</a>. Each run's resource diagnostics
link to its observed setup. Missing historical measurements remain explicitly unavailable.</p>
<p>{html.escape(production_export.projection.MEASUREMENT_CAVEAT)}</p>
<p>{html.escape(production_export.VALIDATION)} Each file set has four copies forming two local pairs; a small run does not
establish broad variance. Compare only matching workload and measurement contracts.</p>
<div class="table-scroll"><table><thead><tr><th>Run</th><th>Collected at</th><th>Suite</th>
<th>Comparison</th><th>Qualification</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
{status}{footer}</main></body></html>
"""
    return pages


def refresh_aws_output(directory):
    """Refresh only generated AWS pages in a job-owned output without concurrent writers.

    Observed symlinks and special files are rejected before deletion. These pathname
    checks do not provide a general atomic filesystem snapshot against hostile races.
    """
    if not directory.exists() and not directory.is_symlink():
        return
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise ValueError('Generated AWS output must be a real directory')
    pending = [directory]
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISDIR(mode):
                    pending.append(Path(entry.path))
                elif not stat.S_ISREG(mode):
                    raise ValueError('Generated AWS output contains a symlink or special file')
    shutil.rmtree(directory)


def build(root, output, *, has_dashboard=False, aws_history=None, aws_records=None,
          aws_reader_revision=None, aws_inventory_sha256=None):
    """Build the public project pages while preserving history under ci-benchmarks/."""
    proof = (aws_records, aws_reader_revision, aws_inventory_sha256)
    if any(value is not None for value in proof) and not all(value is not None for value in proof):
        raise ValueError('--aws-records, --aws-reader-revision and --aws-inventory-sha256 are required together')
    if aws_history is not None and aws_records is not None:
        raise ValueError('Choose retained AWS records or legacy AWS history, not both')
    if aws_records is not None:
        source, destination = Path(aws_records).resolve(), output.resolve()
        if source.is_relative_to(destination) or destination.is_relative_to(source):
            raise ValueError('Retained AWS input and generated output directories must not overlap')
    root = root.resolve()
    sources = sorted((root / 'docs/reports').glob('*.md'))
    if root / 'docs/reports/README.md' not in sources:
        raise ValueError('Missing reports index: docs/reports/README.md')
    routes = {source: Path('reports/index.html') if source.name == 'README.md'
              else Path('reports') / source.stem / 'index.html' for source in sources}
    dashboard = output / 'ci-benchmarks/index.html'
    if has_dashboard and not dashboard.is_file():
        raise ValueError(f'Missing benchmark dashboard: {dashboard}')
    pages = (retained_aws_benchmark_pages(aws_records, aws_reader_revision, aws_inventory_sha256)
             if aws_records is not None else aws_benchmark_pages(aws_history) if aws_history is not None else {})
    has_aws_history = bool(pages)
    assets, headings, sections = {}, {}, {}
    for source, route in routes.items():
        page, referenced, headings[source], sections[route] = render(source, root, route, routes, has_aws_history=has_aws_history)
        pages[route] = page
        assets.update(referenced)
    pages[Path('index.html')] = landing_page(root, routes, has_dashboard, has_aws_history)
    sections[Path('index.html')] = RepositoryLinks(pages[Path('index.html')], root).sections
    for page, references in sections.items():
        for target, fragment, value in references:
            if target not in headings:
                tokens = MarkdownIt('commonmark').parse(target.read_text(encoding='utf-8'))
                headings[target] = {slug for _, _, slug in heading_ids(tokens)}
            if fragment not in headings[target]:
                kind = 'report' if target in routes else 'repository'
                raise ValueError(f'Missing {kind} section in {page}: {value}')
    for name in ('site.css', 'report.css', 'landing.css', 'landing.js'):
        assets[Path('assets') / name] = ASSETS / name
    assets[Path('assets/logo.svg')] = root / 'assets/logo.svg'
    if has_dashboard:
        for name in ('history.json', 'changes.json', 'changes.md'):
            assets[Path(name)] = output / 'ci-benchmarks' / name
        if not (output / 'ci-benchmarks/changes.html').is_file():
            raise ValueError('Missing benchmark dashboard: ci-benchmarks/changes.html')
        pages[Path('changes.html')] = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>CI benchmark changes · RCP Tools</title>
{read_theme()}
{redirect_script('ci-benchmarks/changes.html')}
<noscript><meta http-equiv="refresh" content="0;url=ci-benchmarks/changes.html"></noscript>
</head><body><a href="ci-benchmarks/changes.html">Continue to CI benchmark changes</a></body></html>
'''
    for destination, source in assets.items():
        if not source.is_file():
            raise ValueError(f'Missing report asset: {source}')
    if aws_records is not None:
        refresh_aws_output(output / 'aws-benchmarks')
    for route, text in pages.items():
        target = output / route
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
    for destination, source in assets.items():
        target = output / destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return len(pages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--output', type=Path, default=Path('target/report-site'))
    parser.add_argument('--with-benchmark-history', action='store_true',
                        help='require an existing dashboard under the output directory\'s ci-benchmarks/')
    parser.add_argument('--aws-history', type=Path,
                        help='legacy read-only bare Git repository with the fixed aws-benchmark-history ref')
    parser.add_argument('--aws-records', type=Path, help='proof-bound directory of retained AWS public records')
    parser.add_argument('--aws-reader-revision', help='exact trusted reader revision for --aws-records')
    parser.add_argument('--aws-inventory-sha256', help='trusted predecessor inventory digest for --aws-records')
    args = parser.parse_args()
    try:
        count = build(args.root, args.output, has_dashboard=args.with_benchmark_history, aws_history=args.aws_history,
                      aws_records=args.aws_records, aws_reader_revision=args.aws_reader_revision,
                      aws_inventory_sha256=args.aws_inventory_sha256)
    except (OSError, ValueError) as error:
        parser.exit(1, f'report site: {error}\n')
    print(f'Wrote {count} project pages to {args.output}')


if __name__ == '__main__':
    main()
