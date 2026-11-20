"""Shared morning-briefing edition pipeline.

Used by the daily render path (via data.json / gn.json produced with the same
steps) and by `build.py --update`. Candidate gathering, scoring, category
assignment, vote weighting, dedupe and verification all live here so an
intraday update is the same algorithm as the morning build — the only
difference is that new picks are appended to today's edition instead of
replacing it.

Perplexity Discover needs a browser; --update reuses the latest
perplexity-YYYY-MM-DD.json on disk (the morning snapshot) and does not
re-scrape Discover.
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import hashlib
import html
import io
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from zoneinfo import ZoneInfo

from PIL import Image
import trafilatura
from lxml import html as lhtml

TZ = ZoneInfo('America/Toronto')
import dedup as _dedup
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'
ROOT = Path('/workspace/news')
SITE = Path('/workspace/news/site')
ARCH = Path('/workspace/news/archive')
MAXH = float(os.environ.get('MAXH', '30'))
IMG_DIR = SITE / 'img'
FEEDS_PATH = SITE / 'allfeeds.json'

R = ['North America', 'Latin America', 'Europe', 'Middle East', 'Asia-Pacific', 'Africa']
T = ['Geopolitics & conflict', 'Politics & elections', 'Economy & markets', 'Climate & disasters', 'Society & justice']
ST_ORDER = [
    ('Technology business news', ['Big Tech', 'Enterprise & software', 'Semis & hardware business']),
    ('Investments and deals', ['Venture rounds', 'Public markets & IPOs', 'Government contracts']),
    ('Sciences', ['Physics & Astronomy', 'Biology & Medicine', 'Earth & Climate', 'Materials & Chemistry']),
    ('Technology & Engineering', ['Space', 'AI & Robotics', 'Energy & Nuclear', 'Quantum', 'Semiconductors']),
]
PAPER_CATS = {
    'Space & Astronomy': 'cat:astro-ph.EP+OR+cat:astro-ph.IM+OR+cat:astro-ph.HE+OR+cat:astro-ph.GA',
    'AI & Robotics': 'cat:cs.RO+OR+cat:cs.AI+OR+cat:cs.LG',
    'Energy & Nuclear': 'cat:physics.plasm-ph+OR+cat:nucl-ex+OR+cat:physics.app-ph',
    'Quantum': 'cat:quant-ph',
    'Semiconductors & Materials': 'cat:cond-mat.mtrl-sci+OR+cat:cond-mat.supr-con+OR+cat:physics.optics',
    'Manufacturing': (
        '(cat:eess.SY+OR+cat:cs.RO+OR+cat:physics.app-ph+OR+cat:cs.CE+OR+cat:cond-mat.mtrl-sci)'
        '+AND+(all:manufacturing+OR+all:%22additive+manufacturing%22+OR+all:%223D+printing%22'
        '+OR+all:%223d+printing%22+OR+all:%223D+printed%22+OR+all:%22additively+manufactured%22'
        '+OR+all:machining+OR+all:welding+OR+all:%22smart+manufacturing%22'
        '+OR+all:%22industrial+automation%22+OR+all:%22production+line%22'
        '+OR+all:%22assembly+line%22+OR+all:%22semiconductor+fabrication%22'
        '+OR+all:%22powder+bed+fusion%22+OR+all:%22directed+energy+deposition%22)'
    ),
    'Biology & Medicine': 'cat:q-bio.BM+OR+cat:q-bio.QM+OR+cat:q-bio.NC',
}

# Title/abstract filter for Manufacturing. The arXiv query is keyword-restricted
# but still admits passing mentions ("manufacturing industries") and loose
# neighbours ("digital twin", "process control", "factory") that are not
# about manufacturing. Strong process terms always qualify; a generic
# "manufacturing" mention qualifies unless it is only an economic-sector
# aside; loose terms need manufacturing context.
_MFG_STRONG = re.compile(
    r'\b(?:additive manufactur(?:e|ed|ing)|additively manufactured|'
    r'3[\-\s]?d[\-\s]?print(?:ing|ed|er|ers)?|three-dimensional print|'
    r'powder bed fusion|directed energy deposition|selective laser (?:melting|sintering)|'
    r'machining|\bcnc\b|computer numerical control|toolpaths?|'
    r'weld(?:ing|ed|s)?|'
    r'smart manufacturing|industrial automation|industry 4\.0|'
    r'production lines?|assembly lines?|factory floors?|shop floors?|'
    r'industrial robots?|industrial manipulat(?:ion|or|ors)|'
    r'semiconductor fabrication|wafer fabrication|chip fabrication|'
    r'injection mould?ing|die casting|metal forming|sheet metal|'
    r'thermoforming|disassembl(?:y|ies|ability))\b',
    re.I,
)
_MFG_PRINT_ONLY = re.compile(
    r'\b(?:3[\-\s]?d[\-\s]?print(?:ing|ed|er|ers)?|three-dimensional print)\b',
    re.I,
)
_MFG_GENERIC = re.compile(r'\bmanufactur(?:e|ed|ing)\b', re.I)
_MFG_GENERIC_ASIDE = re.compile(
    r'\bmanufactur(?:e|ed|ing|er|ers)\s+(?:industr(?:y|ies)|sector|output|pmi|gdp)\b',
    re.I,
)
_MFG_PASSING = re.compile(
    r'compatibility with .{0,80}manufactur|'
    r'manufacturing errors|'
    r'\bhow .{0,40}manufactured\b|'
    r'applications in manufacturing|'
    r'against manufacturing',
    re.I,
)
_MFG_LOOSE = re.compile(r'\b(?:digital twins?|process control|factory)\b', re.I)
_MFG_CONTEXT = re.compile(
    r'\b(?:manufactur|production|assembl(?:y|ies)|factory floor|shop floor|'
    r'industrial|machin(?:e|ing)|fab\b|plant)\b',
    re.I,
)

# Manufacturing is a thin weekly stream, so it looks back further and
# pulls more API hits than the broad category queries (which fill easily
# from 4 days / 50 results).
PAPER_LOOKBACK_DAYS = {'Manufacturing': 14}
PAPER_MAX_RESULTS = {'Manufacturing': 100}


def paper_cutoff_date(cat: str, date: str) -> str:
    days = PAPER_LOOKBACK_DAYS.get(cat, 4)
    return (dt.date.fromisoformat(date) - dt.timedelta(days=days)).isoformat()


def is_manufacturing_paper(title: str, summ: str = '') -> bool:
    """True when a paper is actually about manufacturing, not a passing mention.

    Call this with the full abstract. A 260-character teaser is not enough:
    manufacturing terms often appear after the opening sentence.
    """
    title = title or ''
    summ = summ or ''
    blob = f'{title} {summ}'
    strong = list(_MFG_STRONG.finditer(blob))
    if strong:
        # A 3D-printed test article is not a manufacturing paper unless
        # the title is about printing or another manufacturing term is present.
        if all(_MFG_PRINT_ONLY.search(m.group(0)) for m in strong):
            if not _MFG_PRINT_ONLY.search(title) and not _MFG_GENERIC.search(blob):
                return False
        return True
    generic_hits = list(_MFG_GENERIC.finditer(blob))
    if generic_hits:
        if _MFG_GENERIC.search(title):
            return True
        passing = [m.span() for m in _MFG_PASSING.finditer(blob)]
        asides = [m.span() for m in _MFG_GENERIC_ASIDE.finditer(blob)]
        real = []
        for g in generic_hits:
            if any(g.start() < e and g.end() > s for s, e in passing + asides):
                continue
            real.append(g)
        return len(real) > 0
    if _MFG_LOOSE.search(blob) and _MFG_CONTEXT.search(blob):
        return True
    return False


def paper_cat_query_order():
    """Manufacturing is queried first so keyword-restricted hits are not
    claimed by the broader cs.RO / physics.app-ph / cond-mat.mtrl-sci
    category queries. Display order stays the PAPER_CATS insertion order.
    """
    keys = list(PAPER_CATS)
    if 'Manufacturing' in PAPER_CATS:
        keys = ['Manufacturing'] + [k for k in keys if k != 'Manufacturing']
    return [(k, PAPER_CATS[k]) for k in keys]

# Same source set as the morning RSS pull (allfeeds.json). Weight is the
# relative trust/priority used when ranking, mirroring the outlets that
# typically make the edition.
SOURCE_WEIGHT = {
    'BBC': 10, 'Reuters': 10, 'AP': 10, 'Financial Times': 10, 'Bloomberg': 9,
    'CNBC': 9, 'The New York Times': 9, 'The Guardian': 9, 'The Globe and Mail': 8,
    'CBC': 8, 'Al Jazeera': 8, 'Axios': 8, 'The Straits Times': 8, 'CNA': 7,
    'South China Morning Post': 8, 'Nikkei Asia': 8, 'DW': 7, 'France 24': 7,
    'TechCrunch': 8, 'The Verge': 8, 'WIRED': 8, 'Ars Technica': 8,
    'MIT Technology Review': 8, 'Nature': 9, 'Science': 9, 'Phys.org': 7,
    'SpaceNews': 8, 'Space.com': 7, 'ESA': 7, 'NASA': 7, 'Payload': 7,
    'World Nuclear News': 7, 'DigiTimes': 7, 'STAT': 7, 'BetaKit': 6,
    'HPCwire': 6, 'Data Center Dynamics': 6, 'POLITICO Europe': 7,
    'Fortune': 6, 'Live Science': 6, 'Scientific American': 7, 'Quanta': 7,
    'IEEE Spectrum': 7, 'New Scientist': 7, 'arXiv': 7, 'NPR': 8,
}
DOMAIN_SRC = {
    'bbc.co.uk': 'BBC', 'bbc.com': 'BBC', 'reuters.com': 'Reuters', 'apnews.com': 'AP',
    'ft.com': 'Financial Times', 'bloomberg.com': 'Bloomberg', 'cnbc.com': 'CNBC',
    'nytimes.com': 'The New York Times', 'theguardian.com': 'The Guardian',
    'theglobeandmail.com': 'The Globe and Mail', 'cbc.ca': 'CBC',
    'aljazeera.com': 'Al Jazeera', 'axios.com': 'Axios', 'straitstimes.com': 'The Straits Times',
    'channelnewsasia.com': 'CNA', 'scmp.com': 'South China Morning Post',
    'asia.nikkei.com': 'Nikkei Asia', 'dw.com': 'DW', 'france24.com': 'France 24',
    'techcrunch.com': 'TechCrunch', 'theverge.com': 'The Verge', 'wired.com': 'WIRED',
    'arstechnica.com': 'Ars Technica', 'technologyreview.com': 'MIT Technology Review',
    'nature.com': 'Nature', 'science.org': 'Science', 'phys.org': 'Phys.org',
    'spacenews.com': 'SpaceNews', 'space.com': 'Space.com', 'esa.int': 'ESA',
    'nasa.gov': 'NASA', 'payloadspace.com': 'Payload', 'world-nuclear-news.org': 'World Nuclear News',
    'digitimes.com': 'DigiTimes', 'statnews.com': 'STAT', 'betakit.com': 'BetaKit',
    'hpcwire.com': 'HPCwire', 'datacenterdynamics.com': 'Data Center Dynamics',
    'politico.eu': 'POLITICO Europe', 'fortune.com': 'Fortune', 'livescience.com': 'Live Science',
    'scientificamerican.com': 'Scientific American', 'quantamagazine.org': 'Quanta',
    'spectrum.ieee.org': 'IEEE Spectrum', 'newscientist.com': 'New Scientist',
    'arxiv.org': 'arXiv', 'npr.org': 'NPR', 'kyivindependent.com': 'Kyiv Independent',
    'theregister.com': 'The Register', 'engadget.com': 'Engadget', 'venturebeat.com': 'VentureBeat',
    'electrek.co': 'Electrek', 'semianalysis.com': 'SemiAnalysis', 'eetimes.com': 'EE Times',
    'geekwire.com': 'GeekWire', 'mit.edu': 'MIT', 'sciencedaily.com': 'ScienceDaily',
    'wsj.com': 'The Wall Street Journal', 'economist.com': 'The Economist',
}

# Feed key → default region (news) when the item doesn't say otherwise.
FEED_REGION = {
    'bbcus': 'North America', 'cbccan': 'North America', 'cbc': 'North America',
    'cbcbiz': 'North America', 'npr': 'North America', 'globe': 'North America',
    'globebiz': 'North America', 'nyt': 'North America', 'nytbiz': 'North America',
    'bbclatam': 'Latin America',
    'bbceu': 'Europe', 'guard': 'Europe', 'guardworld': 'Europe', 'dw': 'Europe',
    'f24': 'Europe',
    'bbcme': 'Middle East', 'guardme': 'Middle East', 'f24me': 'Middle East',
    'bbcasia': 'Asia-Pacific', 'st_world': 'Asia-Pacific', 'st_asia': 'Asia-Pacific',
    'st_biz': 'Asia-Pacific', 'st_tech': 'Asia-Pacific', 'scmp': 'Asia-Pacific',
    'asianikkei': 'Asia-Pacific',
    'bbcafrica': 'Africa', 'africa': 'Africa', 'guardafrica': 'Africa',
    'f24africa': 'Africa', 'dwafrica': 'Africa',
}


PAPERS_CAT = 'Scientific papers'

def split_scitech_and_papers(data):
    """Split data.json cats: Science & Technology news vs Research Papers.

    Papers subsections become top-level sections in the papers feed
    (Space & Astronomy, AI & Robotics, ...). The Scientific papers
    category itself stays in data.json so the morning/--update pipeline
    is unchanged.
    """
    st, papers_raw = [], None
    for c in (data or [])[1:]:
        if c.get('cat') == PAPERS_CAT:
            papers_raw = c
        else:
            st.append(c)
    papers = []
    if papers_raw:
        for s in papers_raw.get('subs') or []:
            papers.append({
                'cat': s.get('sub') or 'Papers',
                'subs': [{'sub': '', 'items': list(s.get('items') or [])}],
            })
    return st, papers

def count_items(cats):
    return sum(len(s.get('items') or []) for c in (cats or []) for s in c.get('subs') or [])


# Feed key → scitech (cat, sub) when the feed is specialized.
FEED_ST = {
    'spacenews': ('Technology & Engineering', 'Space'),
    'spacenews2': ('Technology & Engineering', 'Space'),
    'spacecom': ('Technology & Engineering', 'Space'),
    'spacecom2': ('Technology & Engineering', 'Space'),
    'nasa': ('Technology & Engineering', 'Space'),
    'esa': ('Technology & Engineering', 'Space'),
    'esa2': ('Technology & Engineering', 'Space'),
    'payload': ('Technology & Engineering', 'Space'),
    'universetoday': ('Technology & Engineering', 'Space'),
    'spacepolicy': ('Technology & Engineering', 'Space'),
    'utoday': ('Technology & Engineering', 'Space'),
    'aviation': ('Technology & Engineering', 'Space'),
    'qi': ('Technology & Engineering', 'Quantum'),
    'wnn': ('Technology & Engineering', 'Energy & Nuclear'),
    'wnn2': ('Technology & Engineering', 'Energy & Nuclear'),
    'fusion': ('Technology & Engineering', 'Energy & Nuclear'),
    'nuclearnews': ('Technology & Engineering', 'Energy & Nuclear'),
    'electrek': ('Technology & Engineering', 'Energy & Nuclear'),
    'eenews': ('Technology & Engineering', 'Energy & Nuclear'),
    'semi': ('Technology & Engineering', 'Semiconductors'),
    'semianalysis': ('Technology & Engineering', 'Semiconductors'),
    'digitimes': ('Semis & hardware business', None),  # handled below
    'trendforce': ('Technology & Engineering', 'Semiconductors'),
    'eetimes': ('Technology & Engineering', 'Semiconductors'),
    'ieee': ('Technology & Engineering', 'Semiconductors'),
    'ieee2': ('Technology & Engineering', 'Semiconductors'),
    'tc': ('Technology business news', 'Big Tech'),
    'tcvc': ('Investments and deals', 'Venture rounds'),
    'cnbcvc': ('Investments and deals', 'Venture rounds'),
    'crunch': ('Investments and deals', 'Venture rounds'),
    'betakit': ('Investments and deals', 'Venture rounds'),
    'venturebeat': ('Investments and deals', 'Venture rounds'),
    'guardsci': ('Sciences', 'Physics & Astronomy'),
    'bbcsci': ('Sciences', 'Physics & Astronomy'),
    'sciencedaily': ('Sciences', 'Physics & Astronomy'),
    'phys': ('Sciences', 'Physics & Astronomy'),
    'nature': ('Sciences', 'Physics & Astronomy'),
    'nature2': ('Sciences', 'Physics & Astronomy'),
    'natnews': ('Sciences', 'Physics & Astronomy'),
    'science': ('Sciences', 'Physics & Astronomy'),
    'sciadv': ('Sciences', 'Physics & Astronomy'),
    'sciam': ('Sciences', 'Physics & Astronomy'),
    'nsci': ('Sciences', 'Physics & Astronomy'),
    'newsci': ('Sciences', 'Physics & Astronomy'),
    'quanta': ('Sciences', 'Physics & Astronomy'),
    'stat': ('Sciences', 'Biology & Medicine'),
    'neim': ('Sciences', 'Biology & Medicine'),
    'sfn': ('Sciences', 'Biology & Medicine'),
    'nsf': ('Sciences', 'Physics & Astronomy'),
    'eurek': ('Sciences', 'Physics & Astronomy'),
    'live': ('Sciences', 'Physics & Astronomy'),
    'sdi': ('Sciences', 'Physics & Astronomy'),
}


def now_tz():
    return dt.datetime.now(TZ)


def iso(ts=None):
    t = ts or now_tz()
    if t.tzinfo is None:
        t = t.replace(tzinfo=TZ)
    return t.astimezone(TZ).strftime('%Y-%m-%dT%H:%M:%S%z')


def short_date(d):
    """'Nov 14' in Toronto, matching mk.py."""
    if isinstance(d, dt.datetime):
        d = d.astimezone(TZ)
        return f"{d.strftime('%b')} {d.day}"
    return d


def canon(u: str) -> str:
    """Canonical story URL (AMP/mobile/www/scheme/tracking stripped)."""
    return _dedup.canon_url(u)


def sid_for(u: str) -> str:
    return 'u' + hashlib.sha1(u.encode()).hexdigest()[:10]


def img_name(u: str) -> str:
    return hashlib.sha1(u.encode()).hexdigest()[:10] + '.jpg'


def source_from_url(u: str, fallback=''):
    try:
        host = urlparse(u).netloc.lower().removeprefix('www.')
    except Exception:
        return fallback or 'Unknown'
    for dom, name in DOMAIN_SRC.items():
        if host == dom or host.endswith('.' + dom):
            return name
    return fallback or (host.split('.')[0].replace('-', ' ').title() if host else 'Unknown')


def curl(url, timeout=20, binary=False):
    try:
        r = subprocess.run(
            ['curl', '-sL', '-m', str(timeout), '-A', UA,
             '-H', 'Accept: text/html,application/xhtml+xml,application/rss+xml,application/xml,image/*,*/*;q=0.8',
             '-H', 'Accept-Language: en-US,en',
             '-w', '\n%{http_code}', url],
            capture_output=True, timeout=timeout + 8,
        )
        raw = r.stdout
        if not raw:
            return b'' if binary else '', '000'
        # last line is status
        if binary:
            # split last 4 bytes typically \n + 3 digit; safer: rsplit
            try:
                body, _, code = raw.rpartition(b'\n')
                return body, code.decode()
            except Exception:
                return raw, '000'
        text = raw.decode('utf8', 'replace')
        if text and text[-1].isdigit():
            # last line
            i = text.rfind('\n')
            return (text[:i] if i >= 0 else ''), text[i + 1:].strip()
        return text, '000'
    except Exception:
        return (b'' if binary else ''), 'ERR'


# ---------- gathering (same methods as rss.py / ax.py) ----------

def gather_rss(feeds: dict) -> list:
    """Same RSS pull as d1114/rss.py: item/entry, pubDate, MAXH window."""
    now = dt.datetime.now(dt.timezone.utc)

    def go(item):
        k, url = item
        body, code = curl(url, timeout=20)
        out = []
        if not body or code in ('ERR', '000'):
            return k, out
        for it in re.findall(r'<(?:item|entry)\b.*?</(?:item|entry)>', body, re.S):
            g = lambda p: (lambda m: html.unescape(re.sub(r'<!\[CDATA\[|\]\]>', '', m.group(1))).strip() if m else '')(re.search(p, it, re.S))
            title = g(r'<title[^>]*>(.*?)</title>')
            link = g(r'<link>(.*?)</link>')
            if not link:
                m = re.search(r'<link[^>]+href="([^"]+)"', it)
                link = m.group(1) if m else ''
            link = html.unescape(link).strip()
            ds = g(r'<pubDate>(.*?)</pubDate>') or g(r'<dc:date>(.*?)</dc:date>') or g(r'<published>(.*?)</published>') or g(r'<updated>(.*?)</updated>')
            pub = None
            try:
                if ds:
                    pub = parsedate_to_datetime(ds) if (',' in ds or ds[:3].isalpha()) else dt.datetime.fromisoformat(ds.replace('Z', '+00:00'))
                    if pub.tzinfo is None:
                        pub = pub.replace(tzinfo=dt.timezone.utc)
            except Exception:
                pub = None
            age = (now - pub).total_seconds() / 3600 if pub else -1
            if pub and 0 <= age <= MAXH and title and link.startswith('http'):
                desc = g(r'<description>(.*?)</description>') or g(r'<summary[^>]*>(.*?)</summary>') or g(r'<content[^>]*>(.*?)</content>')
                desc = re.sub(r'<[^>]+>', ' ', desc)
                desc = html.unescape(re.sub(r'\s+', ' ', desc)).strip()[:300]
                out.append({'feed': k, 'title': title[:140], 'url': link, 'age_h': round(age, 1), 'pub': pub, 'desc': desc})
        return k, out

    rows = []
    with cf.ThreadPoolExecutor(16) as ex:
        for k, items in ex.map(go, feeds.items()):
            rows.extend(items)
    return rows


def gather_arxiv(used_ids: set, date: str) -> list:
    """Same arXiv query as d1114/ax.py, with a rolling date cutoff from DATE.

    Each arXiv id is assigned to at most one subgroup. Manufacturing is
    queried first (keyword-restricted) so a manufacturing paper that is
    also in cs.RO, physics.app-ph, or cond-mat.mtrl-sci is not taken by
    those broader category queries. Papers that match the Manufacturing
    query but fail is_manufacturing_paper stay available to later
    subgroups. Manufacturing uses a longer lookback and more results
    because genuine weekly volume is much thinner than the broad
    category queries.
    """
    out = []
    seen = set(used_ids)
    for cat, q in paper_cat_query_order():
        cutoff = paper_cutoff_date(cat, date)
        maxn = PAPER_MAX_RESULTS.get(cat, 50)
        url = f'https://export.arxiv.org/api/query?search_query={q}&sortBy=submittedDate&sortOrder=descending&max_results={maxn}'
        body, code = curl(url, timeout=45)
        n_raw = n_used = n_old = n_filt = n_keep = 0
        for e in re.findall(r'<entry>(.*?)</entry>', body or '', re.S):
            m = re.search(r'<id>https?://arxiv.org/abs/([\d.]+)v\d', e)
            if not m:
                continue
            aid = m.group(1)
            n_raw += 1
            tm = re.search(r'<title>(.*?)</title>', e, re.S)
            title = ' '.join(html.unescape(tm.group(1)).split()) if tm else ''
            p = re.search(r'<published>(.*?)</published>', e) or re.search(r'<updated>(.*?)</updated>', e)
            pub = p.group(1) if p else ''
            sm = re.search(r'<summary>(.*?)</summary>', e, re.S)
            full = ' '.join(html.unescape(sm.group(1)).split()) if sm else ''
            summ = full[:260]
            if aid in seen:
                n_used += 1
                continue
            if not pub or pub[:10] < cutoff:
                n_old += 1
                continue
            if cat == 'Manufacturing' and not is_manufacturing_paper(title, full):
                n_filt += 1
                continue
            seen.add(aid)
            n_keep += 1
            try:
                pub_dt = dt.datetime.fromisoformat(pub.replace('Z', '+00:00'))
            except Exception:
                pub_dt = None
            out.append({
                'feed': 'arxiv', 'title': title, 'url': 'https://arxiv.org/abs/' + aid,
                'desc': summ, 'pub': pub_dt, 'age_h': ((dt.datetime.now(dt.timezone.utc) - pub_dt).total_seconds() / 3600) if pub_dt else 12,
                'arxiv_id': aid, 'paper_cat': cat, 'src': 'arXiv',
            })
        # Retry once on an empty/failed Manufacturing query (rate limits, blips).
        if cat == 'Manufacturing' and n_keep == 0 and (code in ('429', '000', '503') or n_raw == 0):
            import time as _time
            _time.sleep(3)
            body, code = curl(url, timeout=45)
            for e in re.findall(r'<entry>(.*?)</entry>', body or '', re.S):
                m = re.search(r'<id>https?://arxiv.org/abs/([\d.]+)v\d', e)
                if not m:
                    continue
                aid = m.group(1)
                n_raw += 1
                tm = re.search(r'<title>(.*?)</title>', e, re.S)
                title = ' '.join(html.unescape(tm.group(1)).split()) if tm else ''
                p = re.search(r'<published>(.*?)</published>', e) or re.search(r'<updated>(.*?)</updated>', e)
                pub = p.group(1) if p else ''
                sm = re.search(r'<summary>(.*?)</summary>', e, re.S)
                full = ' '.join(html.unescape(sm.group(1)).split()) if sm else ''
                summ = full[:260]
                if aid in seen:
                    n_used += 1
                    continue
                if not pub or pub[:10] < cutoff:
                    n_old += 1
                    continue
                if not is_manufacturing_paper(title, full):
                    n_filt += 1
                    continue
                seen.add(aid)
                n_keep += 1
                try:
                    pub_dt = dt.datetime.fromisoformat(pub.replace('Z', '+00:00'))
                except Exception:
                    pub_dt = None
                out.append({
                    'feed': 'arxiv', 'title': title, 'url': 'https://arxiv.org/abs/' + aid,
                    'desc': summ, 'pub': pub_dt, 'age_h': ((dt.datetime.now(dt.timezone.utc) - pub_dt).total_seconds() / 3600) if pub_dt else 12,
                    'arxiv_id': aid, 'paper_cat': cat, 'src': 'arXiv',
                })
        print(f'  arxiv {cat}: raw={n_raw} used={n_used} old={n_old} filter={n_filt} keep={n_keep} http={code} cutoff={cutoff}')
    return out


def latest_perplexity():
    """Reuse the most recent perplexity-YYYY-MM-DD.json (morning snapshot)."""
    files = sorted(ROOT.glob('perplexity-????-??-??.json'))
    if not files:
        return None, [], 'no perplexity-*.json found'
    path = files[-1]
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        return path, [], f'failed to read {path.name}: {e}'
    rows = data if isinstance(data, list) else data.get('stories') or data.get('items') or []
    note = f"reused morning snapshot {path.name} (Discover is not re-scraped on --update)"
    return path, rows, note


def _as_url_list(value) -> list:
    """Normalize a string or list/tuple of URL values into a list."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def publisher_links(row) -> list[str]:
    """Story links from a Perplexity snapshot row.

    Accepts `publisher_url`, `url`, and `publisher_urls`. Each key may be a
    string or a list of strings. Returns unique http(s) URLs in first-seen
    order (`publisher_url`, then `url`, then `publisher_urls`).
    """
    if not isinstance(row, dict):
        return []
    out, seen = [], set()
    for key in ('publisher_url', 'url', 'publisher_urls'):
        for raw in _as_url_list(row.get(key)):
            if not isinstance(raw, str):
                continue
            u = raw.strip()
            if not (u.startswith('http://') or u.startswith('https://')):
                continue
            keyc = canon(u)
            if not keyc or keyc in seen:
                continue
            seen.add(keyc)
            out.append(u)
    return out


def pick_publisher_url(urls: list[str], verified_urls: list[str] | None = None) -> str | None:
    """Choose one URL for a Perplexity row.

    If any of `verified_urls` overlap the row, those are preferred (first
    verified, then best-ranked among them). Otherwise the highest
    SOURCE_WEIGHT wins, with first-seen order as the tie-breaker.
    """
    if not urls:
        return None
    pool = urls
    if verified_urls:
        vcanon = {canon(u) for u in verified_urls if u}
        preferred = [u for u in urls if canon(u) in vcanon]
        if preferred:
            pool = preferred
    best_u, best_w = pool[0], -1
    for u in pool:
        w = SOURCE_WEIGHT.get(source_from_url(u), 5)
        if w > best_w:
            best_u, best_w = u, w
    return best_u


def perplexity_candidates(rows, used_urls=None) -> tuple[set, list]:
    """Build the scoring URL set and candidate dicts from snapshot rows.

    Every valid publisher link is added to the scoring set so RSS matches
    still get the Perplexity boost. Each row is one story: if any of its
    URLs is already in `used_urls` (today's edition plus the last-3-day
    dedup window), the whole row is dropped. Otherwise at most one
    candidate is emitted, using the best-ranked or first verified URL.
    Rows with verified=False still contribute to the scoring set but are
    not emitted as candidates.
    """
    used = {canon(u) for u in (used_urls or []) if u}
    pplx_urls: set = set()
    pplx_cands: list = []
    for row in rows or []:
        urls = publisher_links(row)
        if not urls:
            continue
        for u in urls:
            pplx_urls.add(canon(u))
        if not (row.get('verified', True) if isinstance(row, dict) else True):
            continue
        if any(canon(u) in used for u in urls):
            continue
        title = (row.get('headline') or row.get('title') or '') if isinstance(row, dict) else ''
        desc = ''
        verified_urls = []
        publisher = None
        if isinstance(row, dict):
            desc = row.get('notes') or row.get('desc') or row.get('summary') or ''
            publisher = row.get('publisher')
            verified_urls = publisher_links({
                'publisher_url': row.get('verified_url'),
                'publisher_urls': row.get('verified_urls'),
            })
        u = pick_publisher_url(urls, verified_urls=verified_urls)
        if not u:
            continue
        pplx_cands.append({
            'feed': 'perplexity', 'title': title,
            'url': u, 'desc': desc,
            'src': publisher or source_from_url(u),
            'pub': None, 'age_h': 8, 'from_perplexity': True,
            'pplx_urls': urls,
        })
    return pplx_urls, pplx_cands


def load_used_urls(date: str) -> tuple[set, set]:
    """URLs already used in archives + today; arXiv ids already used."""
    urls, axids = set(), set()

    def swallow(obj):
        if isinstance(obj, str) and 'arxiv.org/abs/' in obj:
            m = re.search(r'arxiv.org/abs/(\d{4}\.\d{5})', obj)
            if m:
                axids.add(m.group(1))
        if isinstance(obj, str) and obj.startswith('http'):
            urls.add(canon(obj))
        elif isinstance(obj, list):
            # item array: url at [4]
            if len(obj) >= 5 and isinstance(obj[4], str) and obj[4].startswith('http'):
                urls.add(canon(obj[4]))
                if 'arxiv.org/abs/' in obj[4]:
                    m = re.search(r'arxiv.org/abs/(\d{4}\.\d{5})', obj[4])
                    if m:
                        axids.add(m.group(1))
            for x in obj:
                swallow(x)
        elif isinstance(obj, dict):
            for v in obj.values():
                swallow(v)

    for p in [SITE / 'data.json', SITE / 'gn.json']:
        if p.exists():
            try:
                swallow(json.loads(p.read_text()))
            except Exception:
                pass
    if ARCH.exists():
        for p in ARCH.glob('*/data.json'):
            try:
                swallow(json.loads(p.read_text()))
            except Exception:
                pass
        for p in ARCH.glob('*/gn.json'):
            try:
                swallow(json.loads(p.read_text()))
            except Exception:
                pass
    pu = ROOT / 'd1114' / 'prev_used.json'
    if pu.exists():
        try:
            prev = json.loads(pu.read_text())
            for u in (prev if isinstance(prev, list) else prev.keys()):
                urls.add(canon(u))
        except Exception:
            pass
    used_ax = ROOT / 'd1114' / 'used_ax.json'
    if used_ax.exists():
        try:
            axids |= set(json.loads(used_ax.read_text()))
        except Exception:
            pass
    return urls, axids


def load_vote_weights():
    """Story/category vote weights from feedback.jsonl + likes.json (same signals the morning curation checks)."""
    up_tokens, down_tokens = defaultdict(int), defaultdict(int)
    up_cats, down_cats = defaultdict(int), defaultdict(int)
    files = [ROOT / 'feedback.jsonl']
    if ARCH.exists():
        files += list(ARCH.glob('*/feedback.jsonl'))
    for f in files:
        if not f.exists():
            continue
        for line in f.read_text().splitlines():
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            vote = r.get('vote')
            title = (r.get('title') or '')
            level = r.get('level') or ''
            bucket = up_tokens if vote == 'up' else down_tokens if vote == 'down' else None
            if bucket is None:
                continue
            for tok in re.findall(r'[A-Za-z]{3,}', title.lower()):
                bucket[tok] += 1
            if level in ('category', 'subcategory') and title:
                (up_cats if vote == 'up' else down_cats)[title.lower()] += 1
    likes = SITE / 'likes.json'
    if likes.exists():
        try:
            for e in json.loads(likes.read_text()):
                for tok in re.findall(r'[A-Za-z]{3,}', (e.get('title') or '').lower()):
                    up_tokens[tok] += 1
                if e.get('cat'):
                    up_cats[e['cat'].lower()] += 1
        except Exception:
            pass
    return up_tokens, down_tokens, up_cats, down_cats


# ---------- classify / score (same buckets as the morning edition) ----------

KW_TOPIC = [
    ('Geopolitics & conflict', r'\b(war|ceasefire|missile|troop|invasion|conflict|airstrike|sanctions|nato|ukraine|gaza|israel|hamas|hezbollah|houthis|taiwan strait|military|defence|defense|arms)\b'),
    ('Politics & elections', r'\b(election|vote|ballot|senate|parliament|president|minister|midterm|campaign|democrat|republican|congress|prime minister|lawmaker|leadership)\b'),
    ('Climate & disasters', r'\b(climate|hurricane|earthquake|flood|wildfire|typhoon|drought|heatwave|disaster|emissions|warming|storm)\b'),
    ('Economy & markets', r'\b(market|stock|gdp|inflation|interest rate|fed |ecb|recession|tariff|trade deal|bank|bond|ipo|earnings|unemployment|economy|oil price)\b'),
    ('Society & justice', r'\b(court|supreme court|lawsuit|crime|police|health|outbreak|measles|vaccine|migration|asylum|rights|strike|school|hospital)\b'),
]
KW_REGION = [
    ('Middle East', r'\b(israel|gaza|iran|iraq|syria|lebanon|saudi|yemen|qatar|uae|tehran|houthis|hamas|hezbollah|west bank)\b'),
    ('Europe', r'\b(eu\b|europe|ukraine|russia|uk\b|britain|france|germany|italy|spain|poland|nato|brussels|london|paris|berlin|moscow|kyiv)\b'),
    ('Latin America', r'\b(brazil|mexico|argentina|venezuela|colombia|chile|peru|bolivia|latam|latin america|são paulo|mexico city)\b'),
    ('Africa', r'\b(africa|nigeria|kenya|ethiopia|ghana|south africa|sudan|egypt|sahel|rwanda|uganda)\b'),
    ('Asia-Pacific', r'\b(china|japan|india|korea|taiwan|australia|singapore|indonesia|vietnam|philippines|pakistan|asia|beijing|tokyo|delhi|seoul)\b'),
    ('North America', r'\b(u\.?s\.?|united states|canada|mexico|washington|ottawa|white house|congress|trudeau|faa|fbi|cdc)\b'),
]
KW_ST = [
    (('Technology & Engineering', 'Space'), r'\b(spacex|starship|nasa|esa|rocket|satellite|orbit|lunar|mars |astronaut|launch|spacecraft|iss\b|artemis)\b'),
    (('Technology & Engineering', 'Quantum'), r'\b(quantum|qubit|superconduct)\b'),
    (('Technology & Engineering', 'Energy & Nuclear'), r'\b(nuclear|fusion|reactor|smr\b|grid|renewable|battery|solar|wind power|hydrogen)\b'),
    (('Technology & Engineering', 'Semiconductors'), r'\b(semiconductor|chip|fabs?|tsmc|asml|lithograph|wafer|foundry|gpu)\b'),
    (('Technology & Engineering', 'AI & Robotics'), r'\b(artificial intelligence|\bai\b|chatgpt|openai|anthropic|robot|autonomous|llm\b|machine learning|deepmind)\b'),
    (('Sciences', 'Biology & Medicine'), r'\b(cancer|vaccine|crispr|gene |clinical|drug |protein|brain|covid|virus|genome|biotech|alzheimer)\b'),
    (('Sciences', 'Earth & Climate'), r'\b(climate|carbon|ocean|glacier|biodiversity|earthquake|volcano|atmosphere)\b'),
    (('Sciences', 'Materials & Chemistry'), r'\b(material|polymer|catalyst|graphene|chemistry|crystal|alloy)\b'),
    (('Sciences', 'Physics & Astronomy'), r'\b(physics|black hole|jwst|telescope|particle|quark|neutron star|exoplanet|astronomy|cosmology)\b'),
    (('Investments and deals', 'Venture rounds'), r'\b(raises? \$?\d|series [a-f]\b|venture|funding round|seed round|valu(ed|ation)|back(ed|s) by)\b'),
    (('Investments and deals', 'Public markets & IPOs'), r'\b(ipo|goes public|stock|shares|nasdaq|s&p|dow |market cap|buyback)\b'),
    (('Investments and deals', 'Government contracts'), r'\b(contract|pentagon|darpa|procurement|awarded .*contract)\b'),
    (('Technology business news', 'Big Tech'), r'\b(apple|google|microsoft|amazon|meta|facebook|alphabet|nvidia|tesla|openai|samsung|netflix)\b'),
    (('Technology business news', 'Enterprise & software'), r'\b(saas|enterprise|cloud|software|microsoft 365|outage|aws |azure|oracle|salesforce)\b'),
    (('Technology business news', 'Semis & hardware business'), r'\b(chipmaker|nvidia|amd\b|intel|arm\b|broadcom|qualcomm|hardware)\b'),
]


def classify(c):
    title = (c.get('title') or '')
    desc = (c.get('desc') or '')
    blob = f"{title} {desc} {c.get('url') or ''}".lower()
    feed = c.get('feed') or ''

    if c.get('arxiv_id') or c.get('paper_cat'):
        c['feed_kind'] = 'paper'
        c['cat'] = 'Scientific papers'
        c['sub'] = c.get('paper_cat') or 'AI & Robotics'
        return c

    st = None
    if feed in FEED_ST:
        cat, sub = FEED_ST[feed]
        if cat in dict(ST_ORDER) or cat == 'Semis & hardware business':
            if cat == 'Semis & hardware business':
                st = ('Technology business news', 'Semis & hardware business')
            else:
                st = (cat, sub)
    if st is None:
        for pair, rx in KW_ST:
            if re.search(rx, blob, re.I):
                st = pair
                break

    # Specialized tech/science feeds default to scitech even without keywords.
    tech_feeds = set(FEED_ST) | {'cnbctech', 'guardtech', 'bbctech', 'verge', 'ars', 'wired', 'engadget',
                                 'mittr', 'theregister', 'reuterstech', 'bloombergtech', 'theinfo',
                                 'hpcwire', 'datacenterdyn', 'fierce', 'nextbig', 'tom', 'geekwire'}
    kind = 'scitech' if (st or feed in tech_feeds) else 'news'
    # General world/business feeds stay news unless clearly tech/science.
    world_feeds = {'bbc', 'bbcbiz', 'aj', 'cbc', 'cbccan', 'cbcbiz', 'guard', 'npr', 'f24', 'dw',
                   'cnbc', 'cnbcw', 'cnbcworld', 'globe', 'globebiz', 'nyt', 'nytbiz', 'ap',
                   'st_world', 'st_asia', 'st_biz', 'africa', 'axios', 'ft', 'reutersbiz'}
    if feed in world_feeds and not st:
        kind = 'news'
    if kind == 'scitech' and not st:
        st = ('Technology business news', 'Big Tech')

    if kind == 'scitech':
        c['feed_kind'] = 'scitech'
        c['cat'], c['sub'] = st
        return c

    region = FEED_REGION.get(feed)
    if not region:
        for name, rx in KW_REGION:
            if re.search(rx, blob, re.I):
                region = name
                break
    if not region:
        region = 'North America'
    topic = T[-1]
    for name, rx in KW_TOPIC:
        if re.search(rx, blob, re.I):
            topic = name
            break
    # business-y leftover
    if topic == T[-1] and re.search(r'\b(bank|market|oil|trade|economy|inflation|shares?)\b', blob, re.I):
        topic = 'Economy & markets'
    c['feed_kind'] = 'news'
    c['region'] = region
    c['topic'] = topic
    return c


def score(c, votes, pplx_urls: set):
    up_tok, down_tok, up_cats, down_cats = votes
    src = c.get('src') or source_from_url(c.get('url') or '')
    c['src'] = src
    sw = SOURCE_WEIGHT.get(src, 5)
    age = c.get('age_h')
    if age is None or age < 0:
        rec = 4
    else:
        rec = max(0, (MAXH - min(age, MAXH)) / MAXH) * 10
    px = 5 if canon(c.get('url') or '') in pplx_urls else 0
    text = f"{c.get('title') or ''} {c.get('cat') or ''} {c.get('sub') or ''} {c.get('region') or ''} {c.get('topic') or ''}".lower()
    toks = re.findall(r'[A-Za-z]{3,}', text)
    vw = 0
    for tok in toks:
        vw += 0.35 * up_tok.get(tok, 0)
        vw -= 0.25 * down_tok.get(tok, 0)
    for key, n in up_cats.items():
        if key and key in text:
            vw += 1.5 * n
    for key, n in down_cats.items():
        if key and key in text:
            vw -= 1.2 * n
    # Slight preference for deep-tech / space / markets — the desk this briefing is for.
    if re.search(r'\b(space|quantum|semiconductor|fusion|ai\b|venture|deep tech|satellite|chip)\b', text, re.I):
        rec += 1.5
    c['score'] = round(sw + rec + px + vw, 3)
    return c['score']


def existing_bucket_counts(gn, data):
    """How many stories already occupy each section (the morning cap is 10)."""
    counts = defaultdict(int)
    for row in gn:
        if isinstance(row, list) and row and isinstance(row[0], list):
            counts[('news', row[1] or '', '')] += 1
            counts[('news-topic', row[2] or '', '')] += 1
    for cat in (data or [])[1:]:
        kind = 'paper' if cat.get('cat') == 'Scientific papers' else 'scitech'
        cname = 'Scientific papers' if kind == 'paper' else cat.get('cat') or ''
        for s in cat.get('subs') or []:
            counts[(kind, cname, s.get('sub') or '')] += len(s.get('items') or [])
    return counts


def select_picks(cands, existing_urls: set, per_bucket=10, occup=None):
    """Same 'top 5 plus up to 5 more' cap (10) per section. Existing stories
    keep their slots; only remaining slots are filled with new picks."""
    occup = occup or {}
    buckets = defaultdict(list)
    for c in cands:
        if c.get('feed_kind') == 'news':
            buckets[('news', c.get('region') or '', '')].append(c)
            buckets[('news-topic', c.get('topic') or '', '')].append(c)
        elif c.get('feed_kind') == 'paper':
            buckets[('paper', 'Scientific papers', c.get('sub') or '')].append(c)
        else:
            buckets[('scitech', c.get('cat') or '', c.get('sub') or '')].append(c)
    picked, seen = [], set()
    for key, rows in buckets.items():
        room = max(0, per_bucket - int(occup.get(key, 0)))
        if not room:
            continue
        rows = sorted(rows, key=lambda x: (-x.get('score', 0), x.get('age_h') or 99))
        uniq, u, added = [], set(), 0
        for r in rows:
            cu = canon(r['url'])
            if cu in u or cu in seen:
                continue
            u.add(cu)
            if cu in existing_urls:
                continue
            uniq.append(r)
            seen.add(cu)
            added += 1
            if added >= room:
                break
        picked.extend(uniq)
    return picked


# ---------- verify (fetch.py + imgx.py methods) ----------

def parse_meta_date(ds: str):
    if not ds:
        return None
    ds = ds.strip()
    try:
        if re.fullmatch(r'\d{10}', ds):
            return dt.datetime.strptime(ds, '%Y%m%d%H').replace(tzinfo=dt.timezone.utc)
        ds2 = ds.replace('Z', '+00:00').replace('+0000', '+00:00')
        t = dt.datetime.fromisoformat(ds2)
        if t.tzinfo is None:
            t = t.replace(tzinfo=dt.timezone.utc)
        return t
    except Exception:
        return None


def verify_one(c):
    """Fetch the publisher page (same curl + og/date regexes as fetch.py / mk.py)."""
    u = c['url']
    if 'arxiv.org/abs/' in u:
        # already have title/date/summary from the API
        c['verified'] = True
        if not c.get('desc'):
            c['desc'] = ''
        c['src'] = 'arXiv'
        if c.get('pub'):
            c['dt'] = short_date(c['pub'])
        return c
    body, code = curl(u, timeout=25)
    c['http'] = code
    if not body or code in ('ERR', '404', '410', '000'):
        c['verified'] = False
        c['reject'] = f'http {code}'
        return c
    def m(p):
        x = re.search(p, body, re.I | re.S)
        return html.unescape(x.group(1)).strip() if x else ''
    title = m(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)') or m(r'<title>(.*?)</title>')
    if title:
        title = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', title)).strip()
        # drop site suffix
        title = re.split(r'\s+[|\-–—]\s+(?:BBC|Reuters|CNBC|The Guardian|Financial Times|Bloomberg|Axios)', title)[0].strip() or title
        if len(title) > 20:
            c['title'] = title[:140]
        if BOT_RX.search(title):
            c['verified'] = False
            c['reject'] = 'bot wall'
            return c

    desc = m(r'<meta[^>]+(?:property|name)=["\']og:description["\'][^>]+content=["\']([^"\']+)') or m(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)')
    if desc and (not c.get('desc') or len(c['desc']) < 40):
        c['desc'] = re.sub(r'\s+', ' ', desc)[:300]
    ds = m(r'(?:article:published_time|datePublished|citation_publication_date|dc.date|citation_online_date|pubdate|publish-date)["\']?\s*(?:content=|:)\s*["\']([^"\']+)')
    pub = parse_meta_date(ds) or c.get('pub')
    if pub:
        c['pub'] = pub
        c['age_h'] = (dt.datetime.now(dt.timezone.utc) - (pub if pub.tzinfo else pub.replace(tzinfo=dt.timezone.utc))).total_seconds() / 3600
        c['dt'] = short_date(pub)
    og = m(r'<meta[^>]+property=["\']og:image(?::url)?["\'][^>]+content=["\']([^"\']+)') or m(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image') or m(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)')
    if og.startswith('//'):
        og = 'https:' + og
    elif og.startswith('/'):
        og = urlparse(u).scheme + '://' + urlparse(u).netloc + og
    c['img_src'] = og
    # too old after verification?
    if c.get('age_h') is not None and c['age_h'] > MAXH + 6:
        c['verified'] = False
        c['reject'] = f'too old {c["age_h"]:.1f}h'
        return c
    if not c.get('dt'):
        # last resort: date in the URL
        x = re.search(r'/20(\d\d)(?:/|-)(\d\d)(?:/|-)(\d\d)', u)
        if x:
            y, mo, d = 2000 + int(x.group(1)), int(x.group(2)), int(x.group(3))
            try:
                pub = dt.datetime(y, mo, d, 12, tzinfo=dt.timezone.utc)
                c['pub'] = pub
                c['dt'] = short_date(pub)
                c['age_h'] = (dt.datetime.now(dt.timezone.utc) - pub).total_seconds() / 3600
            except Exception:
                pass
    if not c.get('dt'):
        c['verified'] = False
        c['reject'] = 'no date'
        return c
    c['verified'] = True
    c['src'] = c.get('src') or source_from_url(u)
    return c


def fetch_image(c):
    """Same thumbnail path as d1114/imgx.py: og:image (or arXiv html figure), JPEG under img/<sha1>.jpg."""
    u = c['url']
    dest = IMG_DIR / img_name(u)
    if dest.exists() and dest.stat().st_size > 800:
        c['img'] = dest.name
        return c
    iu = c.get('img_src') or ''
    try:
        if 'arxiv.org/abs/' in u and not iu:
            aid = u.rsplit('/', 1)[1]
            base = f'https://arxiv.org/html/{aid}v1/'
            body, code = curl(base, timeout=25)
            m = re.search(r'<img[^>]+src="([^"]+\.(?:png|jpg|jpeg|gif))"[^>]*class="ltx_graphics', body or '') or \
                re.search(r'class="ltx_graphics[^"]*"[^>]+src="([^"]+)"', body or '') or \
                re.search(r'<figure[^>]*>.*?<img[^>]+src="([^"]+)"', body or '', re.S)
            iu = m.group(1) if m else ''
            if iu and not iu.startswith('http'):
                iu = base + iu.lstrip('./') if not iu.startswith('/') else 'https://arxiv.org' + iu
        if not iu:
            c['img'] = ''
            return c
        IMG_DIR.mkdir(parents=True, exist_ok=True)
        ib, ic = curl(iu, timeout=25, binary=True)
        if not ib:
            c['img'] = ''
            return c
        im = Image.open(io.BytesIO(ib)); im.load()
        if im.mode in ('RGBA', 'LA', 'P'):
            im = im.convert('RGBA')
            bg = Image.new('RGB', im.size, 'white')
            bg.paste(im, mask=im.split()[3] if im.mode == 'RGBA' else None)
            im = bg
        else:
            im = im.convert('RGB')
        if im.size[0] < 120:
            c['img'] = ''
            return c
        im.thumbnail((192, 192 * 3))
        im.save(dest, 'JPEG', quality=82)
        c['img'] = dest.name
    except Exception:
        c['img'] = c.get('img') or ''
    return c


# ---------- append to today's edition ----------

def stamp_existing(items_kind, built_at: str):
    """Ensure existing items have added_at / is_new without changing order or ids."""
    def stamp_item(it):
        if not isinstance(it, list) or len(it) < 7:
            return it
        # [n,t,d,s,u,dt,img] optionally + id + added_at + is_new
        while len(it) < 10:
            it.append(None)
        if not it[7]:
            it[7] = sid_for(it[4])
        if not it[8]:
            it[8] = built_at
        if it[9] is None:
            it[9] = False
        return it

    if items_kind == 'gn':
        return  # stamped in place below
    return stamp_item


def load_meta(date: str):
    p = SITE / 'edition_meta.json'
    if p.exists():
        try:
            m = json.loads(p.read_text())
            if m.get('date') == date:
                return m
        except Exception:
            pass
    # Infer morning built_at from data.json mtime (the morning assemble).
    built = dt.datetime.now(TZ).replace(hour=7, minute=23, second=0, microsecond=0)
    try:
        ts = (SITE / 'data.json').stat().st_mtime
        built = dt.datetime.fromtimestamp(ts, TZ)
    except Exception:
        pass
    return {'date': date, 'built_at': iso(built), 'updated_at': iso(built)}


def save_meta(meta):
    (SITE / 'edition_meta.json').write_text(json.dumps(meta, indent=2) + '\n')


def item_array(c, added_at: str, is_new=True):
    return [0, c['title'], c.get('desc') or '', c.get('src') or source_from_url(c['url']),
            c['url'], c.get('dt') or short_date(now_tz()), c.get('img') or '',
            sid_for(c['url']), added_at, is_new]


def append_picks(picks, added_at: str, date: str):
    gn = json.loads((SITE / 'gn.json').read_text())
    data = json.loads((SITE / 'data.json').read_text())
    meta = load_meta(date)
    built_at = meta.get('built_at') or added_at

    # stamp existing
    for row in gn:
        if isinstance(row, list) and row and isinstance(row[0], list):
            it = row[0]
            while len(it) < 10:
                it.append(None)
            if not it[7]:
                it[7] = sid_for(it[4])
            if not it[8]:
                it[8] = built_at
            if it[9] is None:
                it[9] = False
    for cat in data:
        for sub in cat.get('subs') or []:
            for it in sub.get('items') or []:
                if isinstance(it, list):
                    while len(it) < 10:
                        it.append(None)
                    if not it[7]:
                        it[7] = sid_for(it[4])
                    if not it[8]:
                        it[8] = built_at
                    if it[9] is None:
                        it[9] = False

    added = []
    existing = {canon(row[0][4]) for row in gn if row and isinstance(row[0], list)}
    existing |= {canon(it[4]) for c in data for s in c.get('subs') or [] for it in s.get('items') or [] if isinstance(it, list) and len(it) > 4}

    # ensure scitech cats/subs exist
    by_cat = {c['cat']: c for c in data}
    if 'General global news' not in by_cat:
        data.insert(0, {'cat': 'General global news', 'subs': []})
        by_cat = {c['cat']: c for c in data}
    for cat_name, subs in ST_ORDER:
        if cat_name not in by_cat:
            data.append({'cat': cat_name, 'subs': [{'sub': s, 'items': []} for s in subs]})
            by_cat = {c['cat']: c for c in data}
        got = {s.get('sub') for s in by_cat[cat_name].get('subs') or []}
        for s in subs:
            if s not in got:
                by_cat[cat_name]['subs'].append({'sub': s, 'items': []})
    if 'Scientific papers' not in by_cat:
        data.append({'cat': 'Scientific papers', 'subs': [{'sub': k, 'items': []} for k in PAPER_CATS]})
        by_cat = {c['cat']: c for c in data}
    else:
        got = {s.get('sub') for s in by_cat['Scientific papers'].get('subs') or []}
        for k in PAPER_CATS:
            if k not in got:
                by_cat['Scientific papers']['subs'].append({'sub': k, 'items': []})

    for c in picks:
        if canon(c['url']) in existing:
            continue
        arr = item_array(c, added_at, True)
        if c.get('feed_kind') == 'news':
            gn.append([arr, c.get('region') or 'North America', c.get('topic') or 'Society & justice'])
        else:
            cat_name = c.get('cat') or 'Technology business news'
            sub_name = c.get('sub') or (list(PAPER_CATS)[0] if cat_name == 'Scientific papers' else 'Big Tech')
            cat = by_cat.get(cat_name)
            if not cat:
                cat = {'cat': cat_name, 'subs': [{'sub': sub_name, 'items': []}]}
                data.append(cat)
                by_cat[cat_name] = cat
            sub = next((s for s in cat['subs'] if s.get('sub') == sub_name), None)
            if not sub:
                sub = {'sub': sub_name, 'items': []}
                cat['subs'].append(sub)
            sub['items'].append(arr)
        existing.add(canon(c['url']))
        added.append(c)

    (SITE / 'gn.json').write_text(json.dumps(gn, ensure_ascii=False))
    (SITE / 'data.json').write_text(json.dumps(data, ensure_ascii=False))
    return added, gn, data


def write_latest_json(date: str, updated_at: str, built_at: str, dest_root: Path, gn=None, data=None):
    """news/latest.json, scitech/latest.json and papers/latest.json next to the feed pages."""
    gn = gn if gn is not None else json.loads((SITE / 'gn.json').read_text())
    data = data if data is not None else json.loads((SITE / 'data.json').read_text())

    def pack(it, **extra):
        while len(it) < 10:
            it.append(None)
        rec = {
            'n': it[0], 'title': it[1], 'desc': it[2], 'src': it[3], 'url': it[4],
            'dt': it[5], 'img': it[6] or '', 'id': it[7] or sid_for(it[4]),
            'added_at': it[8] or built_at, 'new': bool(it[9]),
        }
        rec.update(extra)
        return rec

    news = []
    seen = set()
    for row in gn:
        if not (isinstance(row, list) and row and isinstance(row[0], list)):
            continue
        it, region, topic = row[0], row[1], row[2]
        rec = pack(it, region=region, topic=topic, cat=region, sub='')
        if rec['id'] in seen:
            continue
        seen.add(rec['id'])
        news.append(rec)
    scitech_cats, papers_cats = split_scitech_and_papers(data)
    scitech = []
    papers = []
    seen = set()
    for cat in scitech_cats:
        for sub in cat.get('subs') or []:
            for it in sub.get('items') or []:
                if not isinstance(it, list):
                    continue
                rec = pack(it, cat=cat['cat'], sub=sub.get('sub') or '')
                if rec['id'] in seen:
                    continue
                seen.add(rec['id'])
                scitech.append(rec)
    seen = set()
    for cat in papers_cats:
        for sub in cat.get('subs') or []:
            for it in sub.get('items') or []:
                if not isinstance(it, list):
                    continue
                rec = pack(it, cat=cat['cat'], sub=sub.get('sub') or '', region='', topic='')
                if rec['id'] in seen:
                    continue
                seen.add(rec['id'])
                papers.append(rec)

    for slug, stories in (('news', news), ('scitech', scitech), ('papers', papers)):
        payload = {'date': date, 'updated_at': updated_at, 'built_at': built_at, 'stories': stories}
        for root in {SITE, dest_root}:
            d = Path(root) / slug
            d.mkdir(parents=True, exist_ok=True)
            (d / 'latest.json').write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    return len(news), len(scitech), len(papers)



def end_of_day(date: str) -> bool:
    return now_tz().date().isoformat() != date



# ---------- in-app reader: extract a sanitized article per story ----------

ARTICLES_DIR = SITE / 'articles'
ARTICLE_TIMEOUT = 8
MIN_ARTICLE_WORDS = 150
MAX_ARTICLE_WORDS = 2500
ARTICLE_ALLOW = {
    'p', 'h2', 'h3', 'h4', 'blockquote', 'ul', 'ol', 'li',
    'strong', 'em', 'b', 'i', 'a', 'br', 'hr', 'img', 'figure', 'figcaption',
}
PAYWALL_RX = re.compile(
    r'\b(subscribe to (continue|read|the)|sign in to (read|continue)|already a subscriber|'
    r'this article is (for subscribers|exclusive)|for subscribers only|'
    r'create (a free )?account to|to continue reading|remaining free articles?|'
    r'become a subscriber|this content is for|paywall)\b',
    re.I,
)
BOT_RX = re.compile(
    r'are you a robot|attention required|access denied|just a moment|enable javascript|'
    r'verify you are|cf-browser|request unsuccessful|checking your connection|'
    r'security verification|please wait|403 forbidden|404 not found',
    re.I,
)
DROP_TAGS = {
    'script', 'style', 'iframe', 'object', 'embed', 'form', 'input', 'button',
    'link', 'meta', 'noscript', 'svg', 'video', 'audio', 'source', 'canvas',
    'template', 'textarea', 'select',
}


def https_url(u: str, base: str = '') -> str:
    if not u:
        return ''
    u = html.unescape(u.strip())
    if u.startswith('//'):
        u = 'https:' + u
    elif u.startswith('/') and base:
        try:
            p = urlparse(base)
            u = f'{p.scheme or "https"}://{p.netloc}{u}'
        except Exception:
            return ''
    elif u.startswith('http://'):
        u = 'https://' + u[7:]
    if not u.startswith('https://'):
        return ''
    if re.match(r'https?://javascript:', u, re.I):
        return ''
    return u


def word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9]+(?:['’][A-Za-z]+)?", text or ''))


def sanitize_article_html(raw_html: str, base_url: str) -> tuple[str, int]:
    """Allowlisted tags only; no scripts/iframes; images rewritten to https."""
    if not raw_html or not raw_html.strip():
        return '', 0
    try:
        doc = lhtml.fromstring(raw_html)
    except Exception:
        return '', 0
    if isinstance(doc.tag, str) and doc.tag.lower() == 'html':
        body = doc.find('body')
        doc = body if body is not None else doc

    def clean(el):
        if not isinstance(getattr(el, 'tag', None), str):
            return []
        tag = el.tag.lower()
        if tag in DROP_TAGS:
            return []
        # unwrap non-allowlisted containers, keep children / text
        kids = []
        if el.text and tag not in ('img', 'br', 'hr'):
            kids.append(('text', el.text))
        for child in list(el):
            kids.extend(clean(child))
            if child.tail:
                kids.append(('text', child.tail))
        if tag not in ARTICLE_ALLOW:
            return kids
        node = lhtml.Element(tag)
        if tag == 'a':
            href = https_url(el.get('href') or '', base_url)
            if href:
                node.set('href', href)
                node.set('target', '_blank')
                node.set('rel', 'noopener')
            else:
                return kids
        elif tag == 'img':
            src = https_url(el.get('src') or el.get('data-src') or '', base_url)
            if not src:
                return []
            node.set('src', src)
            if el.get('alt'):
                node.set('alt', el.get('alt'))
            node.set('loading', 'lazy')
            return [('el', node)]
        out_text = []
        for kind, val in kids:
            if kind == 'text':
                if out_text and out_text[-1][0] == 'text':
                    out_text[-1] = ('text', out_text[-1][1] + val)
                else:
                    out_text.append(('text', val))
            else:
                out_text.append((kind, val))
        if not out_text and tag in ('p', 'h2', 'h3', 'h4', 'blockquote', 'li', 'figure', 'figcaption'):
            return []
        if out_text and out_text[0][0] == 'text':
            node.text = out_text[0][1]
            rest = out_text[1:]
        else:
            rest = out_text
        last = node
        for kind, val in rest:
            if kind == 'text':
                last.tail = (last.tail or '') + val
            else:
                node.append(val)
                last = val
        return [('el', node)]

    parts = clean(doc)
    wrapped = lhtml.Element('div')
    last = wrapped
    if parts and parts[0][0] == 'text':
        wrapped.text = parts[0][1]
        parts = parts[1:]
    for kind, val in parts:
        if kind == 'text':
            last.tail = (last.tail or '') + val
        else:
            wrapped.append(val)
            last = val

    # cap length at a paragraph boundary
    words = 0
    kept = []
    for child in list(wrapped):
        w = word_count(child.text_content())
        if words + w > MAX_ARTICLE_WORDS and kept:
            tail = lhtml.Element('p')
            tail.set('class', 'trimmed')
            tail.text = '…'
            kept.append(tail)
            break
        kept.append(child)
        words += w
    wrapped[:] = kept
    html_out = ''.join(
        lhtml.tostring(child, encoding='unicode', method='html') for child in wrapped
    )
    text = ' '.join(wrapped.text_content().split())
    return html_out, word_count(text)


def article_path(date: str, sid: str, root: Path | None = None) -> Path:
    """articles/<date>/<id>.json under the given tree (site or Pages)."""
    base = Path(root) if root is not None else SITE
    return base / 'articles' / date / (sid + '.json')


def collect_edition_stories(gn, data) -> list:
    items, seen = [], set()

    def add(it):
        if not isinstance(it, list) or len(it) < 5:
            return
        sid = (it[7] if len(it) > 7 and it[7] else sid_for(it[4]))
        if not sid or sid in seen:
            return
        seen.add(sid)
        items.append({
            'id': sid,
            'title': it[1] or '',
            'summary': it[2] or '',
            'source': it[3] or source_from_url(it[4]),
            'url': it[4],
            'date': it[5] or '',
            'img': (it[6] or '') if len(it) > 6 else '',
        })

    for row in gn or []:
        if isinstance(row, list) and row and isinstance(row[0], list):
            add(row[0])
    for cat in data or []:
        if not isinstance(cat, dict):
            continue
        for sub in cat.get('subs') or []:
            for it in sub.get('items') or []:
                add(it)
    return items


def _fallback_record(story, reason, extra=None):
    rec = {
        'id': story['id'],
        'url': story['url'],
        'source': story['source'],
        'title': story['title'],
        'byline': '',
        'date': story['date'],
        'image': '',
        'thumb': story.get('img') or '',
        'html': '',
        'word_count': 0,
        'status': 'fallback',
        'reason': reason,
        'summary': story.get('summary') or '',
        'extracted_at': iso(),
    }
    if extra:
        rec.update(extra)
    return rec


def extract_one_article(story: dict) -> dict:
    """Fetch a publisher page and extract a readable, sanitized article."""
    url = story['url']
    if not url:
        return _fallback_record(story, 'no url')
    body, code = curl(url, timeout=ARTICLE_TIMEOUT)
    if code in ('401', '402', '403', '451'):
        return _fallback_record(story, 'paywall', {'http': code})
    if not body or code in ('ERR', '000', '404', '410'):
        return _fallback_record(story, 'blocked', {'http': code or '000'})
    if BOT_RX.search(body[:4000] if isinstance(body, str) else ''):
        return _fallback_record(story, 'blocked', {'http': code})
    try:
        extracted = trafilatura.extract(
            body, url=url, include_comments=False, include_tables=False,
            include_images=True, output_format='html', favor_precision=True,
        )
        meta = trafilatura.extract_metadata(body, default_url=url)
    except Exception:
        extracted, meta = None, None
    title = story['title']
    byline = ''
    date = story['date']
    image = ''
    if meta:
        if meta.title and len(meta.title) > 12:
            title = meta.title.strip()[:200]
        byline = (meta.author or '').strip()[:160]
        if meta.date:
            try:
                d = dt.date.fromisoformat(meta.date[:10])
                date = short_date(dt.datetime(d.year, d.month, d.day, tzinfo=TZ))
            except Exception:
                date = story['date']
        image = https_url(meta.image or '', url)
    if BOT_RX.search(title or ''):
        return _fallback_record(story, 'blocked', {'http': code})
    extracted_words = 0
    if extracted:
        try:
            extracted_words = word_count(lhtml.fromstring(extracted).text_content())
        except Exception:
            extracted_words = word_count(re.sub(r'<[^>]+>', ' ', extracted))
    if PAYWALL_RX.search(body[:8000] if isinstance(body, str) else '') and extracted_words < 400:
        return _fallback_record(story, 'paywall', {'http': code, 'title': title, 'byline': byline, 'date': date, 'image': image})
    html_out, words = sanitize_article_html(extracted or '', url)
    is_arxiv = 'arxiv.org' in url
    thin = words < (40 if is_arxiv else MIN_ARTICLE_WORDS)
    if thin or not html_out:
        return _fallback_record(story, 'thin', {
            'http': code, 'title': title, 'byline': byline, 'date': date,
            'image': image, 'html': html_out, 'word_count': words,
        })
    return {
        'id': story['id'],
        'url': url,
        'source': story['source'],
        'title': title,
        'byline': byline,
        'date': date,
        'image': image,
        'thumb': story.get('img') or '',
        'html': html_out,
        'word_count': words,
        'status': 'full',
        'reason': None,
        'summary': story.get('summary') or '',
        'extracted_at': iso(),
        'http': code,
    }


def extract_articles(date: str, gn=None, data=None, dest_roots=None) -> dict:
    """Write articles/<date>/<id>.json for each story. Skip ids that already exist."""
    t0 = dt.datetime.now()
    gn = gn if gn is not None else (json.loads((SITE / 'gn.json').read_text()) if (SITE / 'gn.json').exists() else [])
    data = data if data is not None else (json.loads((SITE / 'data.json').read_text()) if (SITE / 'data.json').exists() else [])
    roots = []
    for r in (dest_roots or [SITE]):
        r = Path(r)
        if r not in roots:
            roots.append(r)
    if SITE not in roots:
        roots.insert(0, SITE)
    stories = collect_edition_stories(gn, data)
    todo, skipped = [], 0
    for s in stories:
        existing = ARTICLES_DIR / date / (s['id'] + '.json')
        if existing.exists() and existing.stat().st_size > 40:
            skipped += 1
            # still copy to other dest roots if missing
            rec = None
            for root in roots:
                dest = article_path(date, s['id'], root)
                if dest == existing:
                    continue
                if not dest.exists():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(existing.read_text())
            continue
        todo.append(s)
    print(f'articles: {len(stories)} stories, {skipped} cached, {len(todo)} to fetch')
    full = fallback = 0
    results = []

    def go(s):
        try:
            return extract_one_article(s)
        except Exception as e:
            return _fallback_record(s, 'error', {'error': str(e)[:200]})

    if todo:
        with cf.ThreadPoolExecutor(12) as ex:
            for i, rec in enumerate(ex.map(go, todo), 1):
                results.append(rec)
                if rec.get('status') == 'full':
                    full += 1
                else:
                    fallback += 1
                text = json.dumps(rec, ensure_ascii=False)
                for root in roots:
                    dest = article_path(date, rec['id'], root)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(text)
                if i % 20 == 0 or i == len(todo):
                    print(f'  extracted {i}/{len(todo)} (full={full} fallback={fallback})')
    elapsed = (dt.datetime.now() - t0).total_seconds()
    # recount including cached
    cached_full = cached_fb = 0
    adir = ARTICLES_DIR / date
    if adir.exists():
        for f in adir.glob('*.json'):
            try:
                st = json.loads(f.read_text()).get('status')
            except Exception:
                continue
            if st == 'full':
                cached_full += 1
            else:
                cached_fb += 1
    print(f'articles: done in {elapsed:.1f}s — full={cached_full} fallback={cached_fb} '
          f'(this run fetched {len(todo)}: full={full} fallback={fallback})')
    return {
        'seconds': elapsed, 'total': len(stories), 'skipped': skipped,
        'fetched': len(todo), 'full': cached_full, 'fallback': cached_fb,
        'fetched_full': full, 'fetched_fallback': fallback,
    }


def run_update(date: str, dest_root: Path | None = None, dry_run: bool = False) -> dict:
    """Intraday update: same pipeline as the morning build, append-only."""
    t0 = dt.datetime.now()
    dest_root = Path(dest_root or SITE)
    if end_of_day(date):
        msg = f'skip --update: local date is {now_tz().date().isoformat()}, edition is {date} (end of day)'
        print(msg)
        return {'skipped': True, 'reason': msg, 'seconds': 0}

    os.chdir(SITE)
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    feeds = json.loads(FEEDS_PATH.read_text())
    used_urls, used_ax = load_used_urls(date)
    votes = load_vote_weights()
    gn0 = json.loads((SITE / 'gn.json').read_text())
    data0 = json.loads((SITE / 'data.json').read_text())
    existing = _dedup.stories_from_edition(gn0, data0, date=date)
    prior = _dedup.load_prior_stories(date, days=3)
    window_urls = {canon(s.get('url') or '') for s in (existing + prior) if s.get('url')}
    pplx_path, pplx_rows, pplx_note = latest_perplexity()
    print('perplexity:', pplx_note)
    pplx_urls, pplx_cands = perplexity_candidates(pplx_rows, used_urls=window_urls)
    n_stories = sum(1 for r in pplx_rows if publisher_links(r))
    n_dropped = 0
    sibling_urls = set()
    for r in pplx_rows:
        urls = publisher_links(r)
        if urls and any(canon(u) in window_urls for u in urls):
            n_dropped += 1
            sibling_urls.update(canon(u) for u in urls)
    blocked_urls = used_urls | sibling_urls
    print(f'perplexity candidates: {len(pplx_cands)} stories '
          f'({len(pplx_urls)} unique urls; {n_dropped} dropped, '
          f'already in edition or last 3 days)')

    print('rss: fetching', len(feeds), 'feeds…')
    rss = gather_rss(feeds)
    print('rss:', len(rss), 'items within', MAXH, 'h')
    print('arxiv: querying…')
    papers = gather_arxiv(used_ax, date)
    print('arxiv:', len(papers), 'new ids')

    # Classify and score every gathered item, then run the shared dedup stage
    # (canonical URL / arXiv / DOI, near-duplicate event clustering, today +
    # last 3 archived days) before ranking and appending.
    raw = []
    for c in rss + papers + pplx_cands:
        if not (c.get('url') or ''):
            continue
        c['url'] = c.get('url')
        c['src'] = c.get('src') or source_from_url(c['url'])
        classify(c)
        score(c, votes, pplx_urls)
        raw.append(c)
    liked = _dedup.load_liked_ids()
    pool, dedup_logs = _dedup.dedup_candidates(
        raw,
        existing=existing,
        prior=prior,
        liked_ids=liked,
        extra_urls=blocked_urls,
        extra_arxiv=used_ax,
        weights=SOURCE_WEIGHT,
        date=date,
    )
    log_run = 'update-dry' if dry_run else 'update'
    _dedup.append_dedup_log(date, dedup_logs, run=log_run)
    n_drop = sum(1 for r in dedup_logs if r.get('action') == 'dropped')
    print('novel candidates after dedupe:', len(pool), f'(dropped {n_drop})')
    occup = existing_bucket_counts(gn0, data0)

    # Pre-select a shortlist with a little slack for verify dropouts, then the same top-10 cap.
    shortlist = select_picks(pool, blocked_urls, per_bucket=12, occup=occup)
    print('shortlist before verify:', len(shortlist))
    verified = []
    with cf.ThreadPoolExecutor(12) as ex:
        for c in ex.map(verify_one, shortlist):
            if c.get('verified'):
                verified.append(c)
            else:
                print('  drop', c.get('reject'), (c.get('url') or '')[:80])
    print('verified:', len(verified))
    # rescore after better titles/dates
    for c in verified:
        score(c, votes, pplx_urls)
    picks = select_picks(verified, blocked_urls, per_bucket=10, occup=occup)
    print('picks to append:', len(picks))

    if dry_run:
        elapsed = (dt.datetime.now() - t0).total_seconds()
        print(f'dry-run: not appending {len(picks)} picks ({elapsed:.1f}s)')
        for c in picks:
            print(' would +', c.get('feed_kind'), c.get('region') or c.get('cat'),
                  c.get('topic') or c.get('sub'), '|', (c.get('title') or '')[:90])
        return {
            'skipped': False, 'dry_run': True, 'added': 0, 'would_add': len(picks),
            'seconds': elapsed, 'dedup_dropped': n_drop, 'pool': len(pool),
        }

    with cf.ThreadPoolExecutor(10) as ex:
        picks = list(ex.map(fetch_image, picks))

    added_at = iso()
    added, gn, data = append_picks(picks, added_at, date)
    meta = load_meta(date)
    meta['date'] = date
    meta['updated_at'] = added_at
    meta['perplexity'] = pplx_path.name if pplx_path else None
    meta['perplexity_note'] = pplx_note
    meta['last_added'] = len(added)
    save_meta(meta)
    n_news, n_st, n_papers = write_latest_json(date, added_at, meta.get('built_at') or added_at, dest_root)
    art_roots = [SITE]
    if dest_root and Path(dest_root) != SITE:
        art_roots.append(Path(dest_root))
    art = extract_articles(date, gn, data, dest_roots=art_roots)
    elapsed = (dt.datetime.now() - t0).total_seconds()
    print(f'appended {len(added)} new stories in {elapsed:.1f}s; edition now news={n_news} scitech={n_st} papers={n_papers}')
    print(f'articles this update: fetched={art.get("fetched")} full={art.get("fetched_full")} fallback={art.get("fetched_fallback")}')
    for c in added:
        print(' +', c.get('feed_kind'), c.get('region') or c.get('cat'), c.get('topic') or c.get('sub'), '|', (c.get('title') or '')[:90])
    return {
        'skipped': False, 'added': len(added), 'seconds': elapsed,
        'news': n_news, 'scitech': n_st, 'papers': n_papers, 'perplexity': pplx_note,
        'updated_at': added_at, 'articles': art,
    }


if __name__ == '__main__':
    argv = [a for a in sys.argv[1:] if a != '--']
    dry = '--dry-run' in argv
    argv = [a for a in argv if not a.startswith('--')]
    date = argv[0] if argv else dt.datetime.now(TZ).date().isoformat()
    run_update(date, dry_run=dry)
