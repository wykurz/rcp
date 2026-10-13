"""Render repository reports as a static site beside the benchmark dashboard."""

import argparse
import html
import os
from pathlib import Path
import re
import shutil
from string import Template
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from markdown_it import MarkdownIt


REPOSITORY = 'https://github.com/wykurz/rcp'
ASSETS = Path(__file__).with_name('report-site')


def relative_url(target, page):
    return Path(os.path.relpath(target, page.parent)).as_posix()


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


def render(source, root, route, routes, has_dashboard):
    """Resolve Markdown references and render a page with working heading navigation."""
    parser = MarkdownIt('commonmark', {'html': False}).enable('table')
    tokens = parser.parse(source.read_text(encoding='utf-8'))
    assets = {}
    sections = []
    toc = []
    seen = {'report-content'}
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
    history_url = relative_url(Path('index.html'), route) if has_dashboard else f'{REPOSITORY}/blob/main/docs/benchmarking.md#view-results'
    page = Template((ASSETS / 'page.html').read_text(encoding='utf-8')).substitute(
        title=html.escape(title), body=body, toc='\n'.join(toc),
        theme=(ASSETS.parent.parent / 'assets/site-theme.html').read_text(encoding='utf-8'),
        stylesheet=relative_url(Path('assets/report.css'), route),
        logo=relative_url(Path('assets/logo.svg'), route),
        reports=relative_url(Path('reports/index.html'), route),
        history=html.escape(history_url, quote=True), repository=REPOSITORY,
        source=html.escape(source_url, quote=True),
    )
    return page, assets, seen, sections


def build(root, output):
    """Add reports and their SVG assets without replacing benchmark-history output."""
    root = root.resolve()
    sources = sorted((root / 'docs/reports').glob('*.md'))
    if root / 'docs/reports/README.md' not in sources:
        raise ValueError('Missing reports index: docs/reports/README.md')
    routes = {source: Path('reports/index.html') if source.name == 'README.md'
              else Path('reports') / source.stem / 'index.html' for source in sources}
    index = output / 'index.html'
    has_dashboard = index.exists() and 'rcp-report-index' not in index.read_text(encoding='utf-8')
    pages, assets, headings, sections = {}, {}, {}, {}
    for source, route in routes.items():
        page, referenced, headings[source], sections[source] = render(source, root, route, routes, has_dashboard)
        pages[route] = page
        assets.update(referenced)
    for source, references in sections.items():
        for target, fragment, value in references:
            if target not in headings:
                tokens = MarkdownIt('commonmark').parse(target.read_text(encoding='utf-8'))
                headings[target] = {slug for _, _, slug in heading_ids(tokens)}
            if fragment not in headings[target]:
                kind = 'report' if target in routes else 'repository'
                raise ValueError(f'Missing {kind} section in {source.name}: {value}')
    assets[Path('assets/report.css')] = ASSETS / 'report.css'
    assets[Path('assets/logo.svg')] = root / 'assets/logo.svg'
    for destination, source in assets.items():
        if not source.is_file():
            raise ValueError(f'Missing report asset: {source}')
    for route, text in pages.items():
        target = output / route
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
    for destination, source in assets.items():
        target = output / destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    if not index.exists():
        index.write_text('<!doctype html><html lang="en"><meta charset="utf-8">'
                         '<meta name="rcp-report-index" content="redirect">'
                         '<meta http-equiv="refresh" content="0; url=reports/index.html">'
                         '<title>RCP reports</title><a href="reports/index.html">Read the reports</a></html>\n',
                         encoding='utf-8')
    return len(pages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--output', type=Path, default=Path('target/report-site'))
    args = parser.parse_args()
    try:
        count = build(args.root, args.output)
    except (OSError, ValueError) as error:
        parser.exit(1, f'report site: {error}\n')
    print(f'Wrote {count} report pages to {args.output / "reports"}')


if __name__ == '__main__':
    main()
