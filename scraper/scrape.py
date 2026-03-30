#!/usr/bin/env python3
"""
BSD Advies — Opdrachten Scraper v2
Config-driven, multi-platform, met LinkedIn authenticatie en deduplicatie op inhoud.
"""

import os
import re
import sys
import json
import time
import hashlib
import logging
import requests
import yaml
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, quote_plus

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)
log = logging.getLogger('bsd')

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
PLATFORMS_FILE = os.path.join(os.path.dirname(__file__), 'platforms.yml')
OUTPUT_FILE    = os.path.join(os.path.dirname(__file__), '..', 'data', 'results.json')
REQUEST_DELAY  = 2
VALIDATE_DELAY = 1

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

# ── FILTER DEFINITIES ─────────────────────────────────────────────────────────

TARGET_TERMS = [
    'procesmanager', 'proces manager', 'process manager',
    'servicemanager', 'service manager',
    'service delivery manager', 'service delivery',
    'sdm', 'itsm', 'siam', 'itil',
    'leveranciersregie', 'leveranciersmanagement',
    'service integratie', 'service integration',
    'it governance', 'regieorganisatie',
]

KNOCKOUT_PAIRS = [
    ('delivery management', ['service delivery', 'it service', 'service management']),
    ('alleen detachering',  []),
    ('zzp niet mogelijk',   []),
    ('geen zzp',            []),
    ('niet mogelijk als zzp', []),
]

WORK_SIGNALS = {
    'onsite':  ['100% op locatie', 'volledig op locatie', 'op kantoor', '5 dagen'],
    'hybrid':  ['hybride', 'hybrid', 'deels thuis', 'deels op locatie', '2 dagen', '3 dagen'],
    'remote':  ['volledig remote', '100% remote', 'volledig thuis', 'thuiswerk'],
}

CONTRACT_SIGNALS = {
    'zzp':  ['zzp', 'freelance', 'zelfstandige', 'inhuuropdracht', 'zelfstandig'],
    'mid':  ['midlance', 'interlance'],
    'pay':  ['payroll'],
    'loon': ['loondienst', 'vaste dienst', 'in dienst', 'dienstverband'],
}

CLOSED_SIGNALS = [
    'vacature is gesloten', 'opdracht is gesloten', 'niet meer beschikbaar',
    'niet meer actief', 'verlopen', 'vervallen', 'inschrijving gesloten',
    'sluitingsdatum verstreken', 'al ingevuld', 'positie is ingevuld',
    'geen reacties meer mogelijk', 'deze vacature bestaat niet',
    'opdracht bestaat niet meer', 'is niet langer beschikbaar',
    'job is closed', 'position has been filled', 'no longer available',
]

OPEN_SIGNALS = [
    'reageer', 'solliciteer', 'reactie sturen', 'aanmelden',
    'sluitingsdatum', 'startdatum', 'inzenden', 'apply',
    'reageren kan', 'uren per week', 'uurtarief', 'tarief',
]

SKIP_VALIDATION_DOMAINS = ['funle.nl', 'google.nl', 'google.com', 'linkedin.com']

STEDEN = [
    'Amsterdam', 'Rotterdam', 'Den Haag', 'Utrecht', 'Eindhoven', 'Groningen',
    'Tilburg', 'Almere', 'Breda', 'Nijmegen', 'Apeldoorn', 'Haarlem', 'Arnhem',
    'Zwolle', 'Zoetermeer', 'Leiden', 'Maastricht', 'Dordrecht', 'Ede', 'Venlo',
    'Deventer', 'Delft', 'Assen', 'Amersfoort', 'Heerlen', 'Leeuwarden', 'Helmond',
    'Alkmaar', 'Emmen', 'Enschede', 'Den Bosch', 'Schiedam', 'Maarssen',
    'Haarlemmermeer', 'Westland', 'Eindhoven',
]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def clean(text):
    if not text:
        return ''
    return re.sub(r'\s+', ' ', str(text).strip())


def tl(text):
    return clean(text).lower()


def detect_location(text):
    for stad in STEDEN:
        if stad.lower() in text.lower():
            return stad
    return ''


def detect_work(text):
    t = tl(text)
    found = [wt for wt, signals in WORK_SIGNALS.items() if any(s in t for s in signals)]
    return found if found else ['hybrid']


def detect_contract(text):
    t = tl(text)
    found = [ct for ct, signals in CONTRACT_SIGNALS.items() if any(s in t for s in signals)]
    return found if found else ['zzp']


def is_target(title, description=''):
    combined = tl(f"{title} {description}")
    if not any(t in combined for t in TARGET_TERMS):
        return False, 'Geen relevante zoekterm'
    for ko, exceptions in KNOCKOUT_PAIRS:
        if ko in combined:
            if exceptions and any(e in combined for e in exceptions):
                continue
            return False, f'Knock-out: "{ko}"'
    return True, 'Match'


def content_hash(title, opdrachtgever='', regio='', startdatum=''):
    def norm(s):
        s = tl(s)
        s = re.sub(r'[^a-z0-9 ]', '', s)
        return re.sub(r'\s+', ' ', s).strip()
    key = f"{norm(title)}|{norm(opdrachtgever)}|{norm(regio)}|{norm(startdatum)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def make_result(title, url, source, platform_id, category,
                location='', description='', tarief='', duration='',
                hours='', published='', opdrachtgever='', startdatum=''):
    ok, reason = is_target(title, description)
    combined = f"{title} {description}"
    return {
        'title':                clean(title),
        'url':                  url,
        'source':               source,
        'platform_id':          platform_id,
        'category':             category,
        'location':             clean(location) or detect_location(combined),
        'opdrachtgever':        clean(opdrachtgever),
        'description':          clean(description)[:300],
        'tarief':               clean(tarief),
        'duration':             clean(duration),
        'hours':                clean(hours),
        'published':            published,
        'startdatum':           clean(startdatum),
        'contract':             detect_contract(combined),
        'work':                 detect_work(combined),
        'filtered_in':          ok,
        'filter_reason':        reason,
        'content_hash':         content_hash(title, opdrachtgever, location, startdatum),
        'scraped_at':           datetime.now(timezone.utc).isoformat(),
        'vacancy_status':       'unknown',
        'vacancy_status_reason': 'Nog niet gevalideerd',
        'sources':              [source],
    }


# ── HTTP ──────────────────────────────────────────────────────────────────────

def new_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch(session, url, timeout=15):
    try:
        log.info(f'  GET {url[:90]}')
        r = session.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text
    except requests.exceptions.HTTPError as e:
        log.warning(f'  HTTP {e.response.status_code}')
    except requests.exceptions.Timeout:
        log.warning('  Timeout')
    except Exception as e:
        log.warning(f'  Fout: {e}')
    return None


# ── PARSERS ───────────────────────────────────────────────────────────────────

def parse_itcontracts(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    base = 'https://www.it-contracts.nl'
    for link in soup.find_all('a', href=re.compile(r'/vacature_')):
        href = link.get('href', '')
        full = urljoin(base, href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        row = link.find_parent(['tr', 'div', 'li'])
        loc = tarief = ''
        if row:
            text = row.get_text()
            loc = detect_location(text)
            m = re.search(r'(\d{2,3})\s*[-–]\s*(\d{2,3})\s*(per uur|/uur|€)', text)
            if m: tarief = f"€{m.group(1)}-{m.group(2)}/u"
        results.append(make_result(title, full, source, pid, 'itcontracts', location=loc, tarief=tarief))
    log.info(f'  → {len(results)} items')
    return results


def parse_freep(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=re.compile(r'/opdracht/')):
        href = link.get('href', '')
        full = urljoin('https://www.freep.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['li', 'div', 'article'])
        loc = hours = ''
        if parent:
            text = parent.get_text()
            loc = detect_location(text)
            m = re.search(r'(\d{2,3})\s*uur', text)
            if m: hours = f"{m.group(1)}u/wk"
        results.append(make_result(title, full, source, pid, 'zzp', location=loc, hours=hours))
    log.info(f'  → {len(results)} items')
    return results


def parse_zzpopdrachten(html, source, pid, url):
    results = []
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for article in soup.select('article') or []:
        title_el = article.find(['h2', 'h3', 'h4'])
        link = article.find('a', href=True)
        if not title_el or not link: continue
        title = clean(title_el.get_text())
        href = link.get('href', '')
        if len(title) < 8: continue
        desc_el = article.find(class_=re.compile(r'excerpt|summary|description', re.I))
        desc = clean(desc_el.get_text()) if desc_el else ''
        loc = detect_location(f"{title} {desc}")
        results.append(make_result(title, href, source, pid, 'zzp', location=loc, description=desc))
    if not results:
        for link in soup.find_all('a', href=re.compile(r'/vacature')):
            title = clean(link.get_text())
            if len(title) > 8:
                results.append(make_result(title, link['href'], source, pid, 'zzp'))
    log.info(f'  → {len(results)} items')
    return results


def parse_funle(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for el in soup.find_all(class_=re.compile(r'job|opdracht|vacancy|card', re.I)):
        link = el.find('a', href=True)
        title_el = el.find(['h2', 'h3', 'h4', 'strong'])
        if not link or not title_el: continue
        title = clean(title_el.get_text())
        href = link.get('href', '')
        full = urljoin('https://funle.nl', href)
        if full in seen or len(title) < 8: continue
        seen.add(full)
        loc = detect_location(el.get_text())
        results.append(make_result(title, full, source, pid, 'aggregator', location=loc))
    if not results:
        for link in soup.find_all('a', href=re.compile(r'/opdrachten/')):
            href = link.get('href', '')
            title = clean(link.get_text())
            if len(title) > 8 and href not in seen:
                seen.add(href)
                results.append(make_result(title, urljoin('https://funle.nl', href), source, pid, 'aggregator'))
    log.info(f'  → {len(results)} items')
    return results


def parse_striive(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for el in soup.find_all(class_=re.compile(r'job|opdracht|vacancy|card|result', re.I)):
        link = el.find('a', href=True)
        title_el = el.find(['h2', 'h3', 'h4'])
        if not link or not title_el: continue
        title = clean(title_el.get_text())
        href = link.get('href', '')
        full = urljoin('https://striive.com', href)
        if full in seen or len(title) < 8: continue
        seen.add(full)
        loc = detect_location(el.get_text())
        results.append(make_result(title, full, source, pid, 'aggregator', location=loc))
    if not results:
        for link in soup.find_all('a', href=re.compile(r'/nl/opdrachten/')):
            href = link.get('href', '')
            title = clean(link.get_text())
            if len(title) > 8 and href not in seen:
                seen.add(href)
                results.append(make_result(title, urljoin('https://striive.com', href), source, pid, 'aggregator'))
    log.info(f'  → {len(results)} items')
    return results


def parse_planet(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=re.compile(r'planetinterim\.nl.*\d{5,}')):
        href = link.get('href', '')
        full = urljoin('https://planetinterim.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['li', 'div', 'tr'])
        loc = detect_location(parent.get_text()) if parent else ''
        results.append(make_result(title, full, source, pid, 'aggregator', location=loc))
    log.info(f'  → {len(results)} items')
    return results


def parse_publiekepartner(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=re.compile(r'(opdracht|bekijk)', re.I)):
        href = link.get('href', '')
        if not href.startswith('http'):
            href = urljoin('https://depubliekepartner.nl', href)
        if href in seen: continue
        seen.add(href)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['div', 'li', 'article'])
        loc = tarief = org = ''
        if parent:
            text = parent.get_text()
            loc = detect_location(text)
            m = re.search(r'Tarief:\s*(\d+)', text)
            if m: tarief = f"€{m.group(1)}/u"
            m2 = re.search(r'Organisatie:\s*([^\n]+)', text)
            if m2: org = clean(m2.group(1))
        results.append(make_result(title, href, source, pid, 'overheid',
                                   location=loc, tarief=tarief, opdrachtgever=org))
    log.info(f'  → {len(results)} items')
    return results


def parse_circle8(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=re.compile(r'(opdracht|vacature)', re.I)):
        href = link.get('href', '')
        full = urljoin('https://www.circle8.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['div', 'li', 'article'])
        loc = detect_location(parent.get_text()) if parent else ''
        results.append(make_result(title, full, source, pid, 'bureau', location=loc))
    log.info(f'  → {len(results)} items')
    return results


def parse_freelancenl(html, source, pid, url):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    for link in soup.find_all('a', href=re.compile(r'/opdracht/\d+')):
        href = link.get('href', '')
        full = urljoin('https://www.freelance.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['div', 'li', 'article', 'tr'])
        loc = detect_location(parent.get_text()) if parent else ''
        results.append(make_result(title, full, source, pid, 'zzp', location=loc))
    log.info(f'  → {len(results)} items')
    return results


def parse_generic(html, source, pid, url, base_url=None):
    results, seen = [], set()
    if not html: return results
    soup = BeautifulSoup(html, 'html.parser')
    base = base_url or f"https://{urlparse(url).netloc}"
    pattern = re.compile(r'/(vacature|opdracht|job|klus|positie)s?/', re.I)
    for link in soup.find_all('a', href=pattern):
        href = link.get('href', '')
        full = urljoin(base, href) if not href.startswith('http') else href
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        parent = link.find_parent(['li', 'div', 'article', 'tr'])
        loc = detect_location(parent.get_text()) if parent else ''
        results.append(make_result(title, full, source, pid, 'zzp', location=loc))
    log.info(f'  → {len(results)} items (generiek)')
    return results


# ── LINKEDIN VIA PLAYWRIGHT ───────────────────────────────────────────────────

def scrape_linkedin(platform, results_list):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning('Playwright niet beschikbaar — LinkedIn overgeslagen')
        return

    creds = platform.get('credentials', {})
    email    = os.environ.get(creds.get('username_secret', ''), '')
    password = os.environ.get(creds.get('password_secret', ''), '')

    if not email or not password:
        log.warning('LinkedIn credentials niet gevonden — overgeslagen')
        return

    pid   = platform['id']
    label = platform['label']
    parser_type = platform['parser']

    log.info(f'LinkedIn: browser starten, inloggen als {email}')

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        ctx  = browser.new_context(user_agent=HEADERS['User-Agent'], locale='nl-NL')
        page = ctx.new_page()
        try:
            page.goto('https://www.linkedin.com/login', timeout=30000)
            page.fill('#username', email)
            page.fill('#password', password)
            page.click('[type=submit]')
            page.wait_for_load_state('networkidle', timeout=20000)

            if 'checkpoint' in page.url or 'challenge' in page.url:
                log.warning('LinkedIn: CAPTCHA/verificatie — overgeslagen')
                return

            log.info('LinkedIn: ingelogd ✓')

            if parser_type == 'linkedin_jobs':
                _li_jobs(page, platform, pid, label, results_list)
            elif parser_type == 'linkedin_feed':
                _li_feed(page, platform, pid, label, results_list)

        except Exception as e:
            log.error(f'LinkedIn fout: {e}')
        finally:
            browser.close()


def _li_jobs(page, platform, pid, label, results_list):
    for search in platform.get('searches', []):
        term    = search.get('term', '')
        filters = search.get('filters', '')
        url     = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(term)}&{filters}"
        log.info(f'  LI Jobs: {term}')
        try:
            page.goto(url, timeout=20000)
            page.wait_for_selector('.jobs-search__results-list, .job-card-container', timeout=10000)
            time.sleep(2)
            soup = BeautifulSoup(page.content(), 'html.parser')
            for card in soup.select('.job-card-container, .jobs-search__results-list li'):
                title_el = card.find(['h3', 'h4'])
                link = card.find('a', href=re.compile(r'/jobs/view/'))
                if not title_el or not link: continue
                title = clean(title_el.get_text())
                href  = link.get('href', '').split('?')[0]
                job_url = urljoin('https://www.linkedin.com', href)
                org_el  = card.find(class_=re.compile(r'company|employer', re.I))
                org = clean(org_el.get_text()) if org_el else ''
                loc_el  = card.find(class_=re.compile(r'location|locati', re.I))
                loc = clean(loc_el.get_text()) if loc_el else detect_location(card.get_text())
                results_list.append(make_result(title, job_url, label, pid, 'linkedin',
                                                location=loc, opdrachtgever=org))
        except Exception as e:
            log.warning(f'  LI Jobs fout voor "{term}": {e}')
        time.sleep(REQUEST_DELAY)


def _li_feed(page, platform, pid, label, results_list):
    keywords = platform.get('keywords', [])
    opp_words = ['opdracht', 'gezocht', 'zoeken naar', 'vacature', 'interim', 'freelance', 'zzp']
    log.info('  LI Feed: timeline scannen')
    try:
        page.goto('https://www.linkedin.com/feed/', timeout=20000)
        page.wait_for_selector('.feed-shared-update-v2', timeout=10000)
        for _ in range(3):
            page.keyboard.press('End')
            time.sleep(2)
        soup = BeautifulSoup(page.content(), 'html.parser')
        found = 0
        for post in soup.select('.feed-shared-update-v2'):
            text = clean(post.get_text())
            tlow = text.lower()
            if not (any(kw.lower() in tlow for kw in keywords) and
                    any(ow in tlow for ow in opp_words)):
                continue
            link = post.find('a', href=re.compile(r'http'))
            post_url = link['href'] if link else 'https://www.linkedin.com/feed/'
            lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 15]
            title = lines[0][:120] if lines else text[:80]
            author_el = post.find(class_=re.compile(r'actor|author', re.I))
            org = clean(author_el.get_text()) if author_el else ''
            results_list.append(make_result(
                f"[Feed] {title}", post_url, f"{label} — Feed", pid, 'linkedin',
                description=text[:300], opdrachtgever=org
            ))
            found += 1
        log.info(f'  → {found} feed-posts met opdracht-signalen')
    except Exception as e:
        log.warning(f'  LI Feed fout: {e}')


# ── VALIDATIE ─────────────────────────────────────────────────────────────────

def validate_vacancy(result, session):
    url = result.get('url', '')
    if not url or not url.startswith('http'):
        return 'unknown', 'Geen geldige URL'
    domain = urlparse(url).netloc.replace('www.', '')
    if any(s in domain for s in SKIP_VALIDATION_DOMAINS):
        return 'unknown', f'Overgeslagen: {domain}'
    try:
        r = session.get(url, timeout=12, allow_redirects=True)
        if r.status_code in (404, 410):
            return 'closed', f'HTTP {r.status_code}'
        if r.status_code >= 400:
            return 'unknown', f'HTTP {r.status_code}'
        tlow = r.text.lower()
        for sig in CLOSED_SIGNALS:
            if sig in tlow:
                return 'closed', f'"{sig}"'
        open_hits = sum(1 for s in OPEN_SIGNALS if s in tlow)
        if open_hits >= 2:
            return 'open', f'{open_hits} open-signalen'
        return 'open', 'Geen gesloten signalen'
    except Exception as e:
        return 'unknown', str(e)[:60]


# ── DEDUPLICATIE ──────────────────────────────────────────────────────────────

def deduplicate(results):
    seen = {}
    for r in results:
        h = r['content_hash']
        if h not in seen:
            seen[h] = r
        else:
            ex = seen[h]
            if r['source'] not in ex['sources']:
                ex['sources'].append(r['source'])
            if len(r.get('description', '')) > len(ex.get('description', '')):
                ex['description'] = r['description']
            for field in ['tarief', 'location', 'opdrachtgever', 'hours']:
                if r.get(field) and not ex.get(field):
                    ex[field] = r[field]
    return list(seen.values())


# ── PARSER MAP ────────────────────────────────────────────────────────────────

PARSERS = {
    'itcontracts':     parse_itcontracts,
    'freep':           parse_freep,
    'zzpopdrachten':   parse_zzpopdrachten,
    'funle':           parse_funle,
    'striive':         parse_striive,
    'planet':          parse_planet,
    'publiekepartner': parse_publiekepartner,
    'circle8':         parse_circle8,
    'freelancenl':     parse_freelancenl,
    'generic':         parse_generic,
}

LINKEDIN_PARSERS = {'linkedin_jobs', 'linkedin_feed'}


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    with open(PLATFORMS_FILE, 'r', encoding='utf-8') as f:
        platforms = yaml.safe_load(f)['platforms']

    session     = new_session()
    all_results = []
    errors      = []
    n_platforms = 0

    for plat in platforms:
        if not plat.get('active', True):
            log.info(f'Skip: {plat["label"]}')
            continue

        pid         = plat['id']
        label       = plat['label']
        parser_type = plat.get('parser', 'generic')
        searches    = plat.get('searches', [])

        # LinkedIn via Playwright
        if parser_type in LINKEDIN_PARSERS:
            log.info(f'\nPlatform: {label} [browser]')
            n_platforms += 1
            try:
                scrape_linkedin(plat, all_results)
            except Exception as e:
                log.error(f"LinkedIn fout (niet fataal): {e}")
                errors.append(f"{label}: {e}")
            continue

        parser_fn = PARSERS.get(parser_type)
        if not parser_fn:
            log.warning(f'Geen parser voor "{parser_type}" — {label}')
            continue

        log.info(f'\nPlatform: {label} [{len(searches)} zoekopdrachten]')
        n_platforms += 1

        for search in searches:
            url = search.get('url', '')
            if not url:
                continue
            html = fetch(session, url)
            try:
                results = parser_fn(html, label, pid, url)
                all_results.extend(results)
            except Exception as e:
                msg = f'{label}: {e}'
                log.error(f'  Parser fout: {msg}')
                errors.append(msg)
            time.sleep(REQUEST_DELAY)

    log.info(f'\n=== Totaal: {len(all_results)} voor deduplicatie ===')
    all_results = deduplicate(all_results)
    log.info(f'=== Totaal: {len(all_results)} na deduplicatie ===')

    relevant   = [r for r in all_results if r['filtered_in']]
    irrelevant = [r for r in all_results if not r['filtered_in']]
    log.info(f'Relevant: {len(relevant)} | Irrelevant: {len(irrelevant)}')

    # Validatie
    log.info('\n=== Vacature validatie ===')
    open_n = closed_n = unknown_n = 0
    for r in relevant:
        status, reason = validate_vacancy(r, session)
        r['vacancy_status']        = status
        r['vacancy_status_reason'] = reason
        if status == 'open':    open_n += 1
        elif status == 'closed': closed_n += 1
        else:                    unknown_n += 1
        time.sleep(VALIDATE_DELAY)

    log.info(f'Open: {open_n} | Onbekend: {unknown_n} | Gesloten: {closed_n}')

    open_r    = [r for r in relevant if r['vacancy_status'] == 'open']
    unknown_r = [r for r in relevant if r['vacancy_status'] == 'unknown']
    closed_r  = [r for r in relevant if r['vacancy_status'] == 'closed']

    final = sorted(open_r + unknown_r, key=lambda x: x.get('source', ''))

    output = {
        'scraped_at':          datetime.now(timezone.utc).isoformat(),
        'scraped_at_nl':       datetime.now().strftime('%d %B %Y om %H:%M'),
        'total_found':         len(all_results),
        'total_relevant':      len(relevant),
        'total_open':          open_n,
        'total_unknown':       unknown_n,
        'total_closed':        closed_n,
        'total_filtered_out':  len(irrelevant),
        'platforms_scraped':   n_platforms,
        'errors':              errors,
        'results':             final,
        'closed_debug':        [{'title': r['title'], 'reason': r['vacancy_status_reason']} for r in closed_r[:10]],
        'filtered_out_debug':  [{'title': r['title'], 'source': r['source'], 'reason': r['filter_reason']} for r in irrelevant[:10]],
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f'\n✅ Klaar — geschreven naar {OUTPUT_FILE}')
    return output


if __name__ == '__main__':
    run()
