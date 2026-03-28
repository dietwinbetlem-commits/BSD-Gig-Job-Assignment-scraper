#!/usr/bin/env python3
"""
BSD Advies — Dagelijkse Opdrachten Scraper
Scrapt NL ZZP/freelance platforms voor IT Procesmanager / Servicemanager /
Service Delivery Manager / ITSM/SIAM consultant opdrachten.
Output: data/results.json
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('bsd-scraper')

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

DELAY = 2  # seconden tussen requests

# ─────────────────────────────────────────────────────────────────────────────
# FILTER LOGICA
# Gespiegeld met de filterregels in het dashboard
# ─────────────────────────────────────────────────────────────────────────────

# Zoektermen die we targeten
TARGET_TERMS = [
    'procesmanager', 'servicemanager', 'service manager',
    'service delivery manager', 'service delivery',
    'itsm', 'siam', 'itil', 'it-governance',
    'leveranciersmanagement', 'leveranciersregie',
    'regie', 'service integratie', 'service integration',
    'it procesmanager', 'it servicemanager',
]

# Knock-out termen — direct afvallen
KNOCKOUT_TERMS = [
    'alleen detachering', 'zzp niet mogelijk', 'geen zzp',
    'loondienst only', 'vaste dienst only',
    'delivery management',   # zonder "service" = project/supply chain
]

# Knock-out termen die OK zijn als ze gecombineerd voorkomen
KNOCKOUT_UNLESS = {
    'delivery management': ['service delivery', 'it service', 'service management'],
}

# ZZP-positieve signalen
ZZP_SIGNALS = [
    'zzp', 'freelance', 'zelfstandige', 'interim', 'zelfstandig',
    'midlance', 'interlance', 'payroll', 'inhuur', 'inhuuropdracht',
    'opdracht', 'contract',
]

# Werkwijze signalen
WORK_SIGNALS = {
    'onsite': ['op locatie', 'op kantoor', '100% kantoor', 'volledig op locatie'],
    'hybrid': ['hybride', 'hybrid', 'deels thuis', 'deels op locatie', 'flex', '2 dagen'],
    'remote': ['volledig remote', '100% remote', 'thuiswerk', 'volledig thuis'],
}


def extract_date(soup):
    """Probeer publicatiedatum te extraheren uit diverse HTML patronen."""
    # Meta tags
    for meta in ['article:published_time', 'datePublished', 'date', 'pubdate']:
        el = soup.find('meta', attrs={'property': meta}) or soup.find('meta', attrs={'name': meta})
        if el and el.get('content'):
            return el['content'][:10]

    # Time tags
    time_el = soup.find('time', attrs={'datetime': True})
    if time_el:
        return time_el['datetime'][:10]

    # Klasse-gebaseerde datum velden
    for cls in ['date', 'datum', 'published', 'publicatiedatum', 'publicatie-datum',
                'post-date', 'entry-date', 'created', 'geplaatst']:
        el = soup.find(class_=re.compile(cls, re.I))
        if el:
            txt = el.get_text().strip()
            # Zoek datumpatroon dd-mm-yyyy of yyyy-mm-dd
            m = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2})', txt)
            if m:
                return m.group(1)

    # Zoek in gewone tekst naar datumpatronen
    text = soup.get_text()
    m = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})', text)
    if m:
        return m.group(1)

    return ''




def clean(text):
    """Schoon tekst op — verwijder extra whitespace."""
    if not text:
        return ''
    return re.sub(r'\s+', ' ', text.strip())


def text_lower(text):
    return clean(text).lower()


def extract_date(soup):
    """Probeer publicatiedatum te extraheren."""
    for meta in ['article:published_time', 'datePublished', 'date', 'pubdate']:
        el = soup.find('meta', attrs={'property': meta}) or soup.find('meta', attrs={'name': meta})
        if el and el.get('content'):
            return el['content'][:10]
    time_el = soup.find('time', attrs={'datetime': True})
    if time_el:
        return time_el['datetime'][:10]
    for cls in ['date', 'datum', 'published', 'publicatiedatum', 'post-date', 'entry-date', 'geplaatst']:
        el = soup.find(class_=re.compile(cls, re.I))
        if el:
            txt = el.get_text().strip()
            m = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2})', txt)
            if m:
                return m.group(1)
    return ''


    tl = text_lower(text)
    return [s for s in ZZP_SIGNALS if s in tl]


def detect_work(text):
    tl = text_lower(text)
    found = []
    for wtype, signals in WORK_SIGNALS.items():
        if any(s in tl for s in signals):
            found.append(wtype)
    if not found:
        found = ['hybrid']  # default aanname
    return found


def is_target(title, description=''):
    """Controleer of een vacature relevant is voor het zoekprofiel."""
    combined = text_lower(f"{title} {description}")

    # Moet minimaal één target term bevatten
    if not any(t in combined for t in TARGET_TERMS):
        return False, 'Geen relevante zoekterm gevonden'

    # Knock-out check
    for ko in KNOCKOUT_TERMS:
        if ko in combined:
            # Check of er een uitzondering is
            unless = KNOCKOUT_UNLESS.get(ko, [])
            if unless and any(u in combined for u in unless):
                continue  # Uitzondering van toepassing
            return False, f'Knock-out term: "{ko}"'

    return True, 'Match'


def infer_contract(title, description=''):
    """Bepaal contract types op basis van tekst."""
    combined = text_lower(f"{title} {description}")
    types = []
    if any(t in combined for t in ['zzp', 'freelance', 'zelfstandige', 'inhuuropdracht']):
        types.append('zzp')
    if any(t in combined for t in ['midlance', 'interlance']):
        types.append('mid')
    if 'payroll' in combined:
        types.append('pay')
    if any(t in combined for t in ['loondienst', 'vaste dienst', 'in dienst']):
        types.append('loon')
    if not types:
        types = ['zzp']  # default voor ZZP platforms
    return types


def fetch(url, timeout=15):
    """Haal URL op met foutafhandeling."""
    try:
        log.info(f'  GET {url}')
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text
    except requests.exceptions.HTTPError as e:
        log.warning(f'  HTTP {e.response.status_code} voor {url}')
    except requests.exceptions.ConnectionError:
        log.warning(f'  Verbindingsfout voor {url}')
    except requests.exceptions.Timeout:
        log.warning(f'  Timeout voor {url}')
    except Exception as e:
        log.warning(f'  Fout voor {url}: {e}')
    return None


def make_result(title, url, source, category, location='', description='',
                tarief='', duration='', hours='', published=''):
    """Maak een genormaliseerd resultaat object."""
    ok, reason = is_target(title, description)
    contract = infer_contract(title, description)
    work = detect_work(description or title)
    zzp_signals = detect_zzp(title + ' ' + description)

    return {
        'title': clean(title),
        'url': url,
        'source': source,
        'category': category,
        'location': clean(location),
        'description': clean(description)[:300] if description else '',
        'tarief': clean(tarief),
        'duration': clean(duration),
        'hours': clean(hours),
        'published': published,
        'contract': contract,
        'work': work,
        'zzp_signals': zzp_signals[:5],
        'filtered_in': ok,
        'filter_reason': reason,
        'scraped_at': datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# PLATFORM PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_itcontracts(html, source_label, base_url):
    """Parser voor IT-Contracts.nl listing pages."""
    results = []
    if not html:
        return results
    soup = BeautifulSoup(html, 'html.parser')

    # IT-Contracts gebruikt <table> of <div> structuur voor vacatures
    # Probeer meerdere selectors
    items = (
        soup.select('table.vacatureoverzicht tr') or
        soup.select('.vacature-item') or
        soup.select('tr.vacature') or
        soup.select('div.opdracht') or
        []
    )

    if not items:
        # Fallback: zoek alle links die eruitzien als vacature URLs
        links = soup.find_all('a', href=re.compile(r'/vacature_|/from/|/search/'))
        for link in links[:30]:
            title = clean(link.get_text())
            if len(title) < 10:
                continue
            href = link.get('href', '')
            url = urljoin(base_url, href) if href.startswith('/') else href
            results.append(make_result(
                title=title, url=url,
                source=source_label, category='itcontracts'
            ))
        return results

    for item in items:
        link = item.find('a', href=True)
        if not link:
            continue
        title = clean(link.get_text())
        if len(title) < 8:
            continue
        href = link.get('href', '')
        url = urljoin(base_url, href)

        # Zoek locatie en andere metadata
        tds = item.find_all('td')
        location = clean(tds[1].get_text()) if len(tds) > 1 else ''
        tarief = ''
        for td in tds:
            t = td.get_text()
            if '€' in t or 'tarief' in t.lower() or 'marktconform' in t.lower():
                tarief = clean(t)

        results.append(make_result(
            title=title, url=url,
            source=source_label, category='itcontracts',
            location=location, tarief=tarief
        ))

    log.info(f'  → {len(results)} items gevonden')
    return results


def parse_freep(html, source_label):
    """Parser voor Freep.nl categoriepagina's."""
    results = []
    if not html:
        return results
    soup = BeautifulSoup(html, 'html.parser')

    # Freep listings zijn typisch <article> of <div class="opdracht">
    items = (
        soup.select('article.opdracht') or
        soup.select('.opdracht-item') or
        soup.select('div.vacancy') or
        soup.select('.list-item') or
        []
    )

    if not items:
        # Fallback: links die verwijzen naar /opdracht/
        links = soup.find_all('a', href=re.compile(r'/opdracht/'))
        seen = set()
        for link in links:
            href = link.get('href', '')
            if href in seen:
                continue
            seen.add(href)
            title = clean(link.get_text())
            if len(title) < 10:
                continue
            url = urljoin('https://www.freep.nl', href)
            # Probeer parent element voor metadata
            parent = link.find_parent(['li', 'div', 'article', 'tr'])
            loc = ''
            if parent:
                loc_el = parent.find(class_=re.compile(r'locati|location|stad|provincie', re.I))
                if loc_el:
                    loc = clean(loc_el.get_text())
            results.append(make_result(
                title=title, url=url,
                source=source_label, category='zzp',
                location=loc
            ))
        log.info(f'  → {len(results)} items (fallback) gevonden')
        return results

    for item in items:
        link = item.find('a', href=re.compile(r'/opdracht/'))
        if not link:
            link = item.find('a', href=True)
        if not link:
            continue
        title = clean(link.get_text())
        href = link.get('href', '')
        url = urljoin('https://www.freep.nl', href)
        # Locatie
        loc_el = item.find(class_=re.compile(r'locati|location|stad', re.I))
        location = clean(loc_el.get_text()) if loc_el else ''
        # Uren
        uren_el = item.find(text=re.compile(r'\d+ uur'))
        hours = clean(uren_el) if uren_el else ''

        results.append(make_result(
            title=title, url=url,
            source=source_label, category='zzp',
            location=location, hours=hours
        ))

    log.info(f'  → {len(results)} items gevonden')
    return results


def parse_zzpopdrachten(html, source_label, search_url):
    """Parser voor ZZP-Opdrachten.nl (WordPress gebaseerd)."""
    results = []
    if not html:
        return results
    soup = BeautifulSoup(html, 'html.parser')

    # WordPress vacature posts
    items = (
        soup.select('article.type-vacature') or
        soup.select('article') or
        soup.select('.vacature-card') or
        soup.select('.job-listing') or
        []
    )

    if not items:
        # Fallback
        links = soup.find_all('a', href=re.compile(r'/vacatures?/'))
        seen = set()
        for link in links:
            href = link.get('href', '')
            if href in seen or href == search_url:
                continue
            seen.add(href)
            title = clean(link.get_text())
            if len(title) < 10 or title.lower() in ['lees meer', 'meer info', 'bekijk']:
                continue
            results.append(make_result(
                title=title, url=href,
                source=source_label, category='zzp'
            ))
        log.info(f'  → {len(results)} items (fallback) gevonden')
        return results

    for item in items:
        title_el = item.find(['h2', 'h3', 'h4']) or item.find('a')
        if not title_el:
            continue
        title = clean(title_el.get_text())
        if len(title) < 8:
            continue

        link = item.find('a', href=True)
        url = link.get('href', '') if link else ''

        # Metadata
        desc_el = item.find(class_=re.compile(r'excerpt|summary|omschrijving|description', re.I))
        description = clean(desc_el.get_text()) if desc_el else ''
        loc_el = item.find(class_=re.compile(r'locati|location|stad', re.I))
        location = clean(loc_el.get_text()) if loc_el else ''

        results.append(make_result(
            title=title, url=url,
            source=source_label, category='zzp',
            location=location, description=description
        ))

    log.info(f'  → {len(results)} items gevonden')
    return results


def parse_planet_interim(html, source_label, base_url):
    """Parser voor Planet Interim."""
    results = []
    if not html:
        return results
    soup = BeautifulSoup(html, 'html.parser')

    items = (
        soup.select('.vacancy-item') or
        soup.select('.opdracht-card') or
        soup.select('article') or
        soup.select('.item') or
        []
    )

    if not items:
        links = soup.find_all('a', href=re.compile(r'planetinterim\.nl'))
        links += soup.find_all('a', href=re.compile(r'/interim-|/zzp-|/opdracht'))
        seen = set()
        for link in links[:25]:
            href = link.get('href', '')
            if href in seen:
                continue
            seen.add(href)
            title = clean(link.get_text())
            if len(title) < 10:
                continue
            url = urljoin(base_url, href)
            results.append(make_result(
                title=title, url=url,
                source=source_label, category='aggregators'
            ))
        log.info(f'  → {len(results)} items (fallback) gevonden')
        return results

    for item in items:
        link = item.find('a', href=True)
        if not link:
            continue
        title = clean(item.find(['h2', 'h3', 'h4', 'strong']).get_text()) if item.find(['h2', 'h3', 'h4', 'strong']) else clean(link.get_text())
        if len(title) < 8:
            continue
        href = link.get('href', '')
        url = urljoin(base_url, href)
        loc_el = item.find(class_=re.compile(r'locati|location|stad|regio', re.I))
        location = clean(loc_el.get_text()) if loc_el else ''
        results.append(make_result(
            title=title, url=url,
            source=source_label, category='aggregators',
            location=location
        ))

    log.info(f'  → {len(results)} items gevonden')
    return results


def parse_generic(html, source_label, category, base_url, link_pattern=None):
    """Generieke parser als fallback voor onbekende site-structuren."""
    results = []
    if not html:
        return results
    soup = BeautifulSoup(html, 'html.parser')

    # Zoek alle links die er uitzien als vacatures
    pattern = re.compile(link_pattern) if link_pattern else re.compile(
        r'/(vacature|opdracht|job|zzp|freelance|interim)/', re.I
    )
    links = soup.find_all('a', href=pattern)

    seen = set()
    for link in links:
        href = link.get('href', '')
        full_url = urljoin(base_url, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        title = clean(link.get_text())
        if len(title) < 10:
            continue
        # Zoek metadata in parent
        parent = link.find_parent(['li', 'div', 'article', 'tr'])
        location = ''
        if parent:
            text = parent.get_text()
            # Zoek provincie/stad
            for stad in ['Amsterdam', 'Utrecht', 'Rotterdam', 'Den Haag', 'Haarlem',
                         'Zwolle', 'Arnhem', 'Eindhoven', 'Groningen', 'Tilburg',
                         'Apeldoorn', 'Enschede', 'Den Bosch', 'Maastricht']:
                if stad.lower() in text.lower():
                    location = stad
                    break

        results.append(make_result(
            title=title, url=full_url,
            source=source_label, category=category,
            location=location
        ))

    log.info(f'  → {len(results)} items (generiek) gevonden')
    return results


# ─────────────────────────────────────────────────────────────────────────────
# BRONNEN DEFINITIE
# ─────────────────────────────────────────────────────────────────────────────

SOURCES = [
    # IT-Contracts
    {
        'label': 'IT-Contracts — Servicemanager',
        'url': 'https://www.it-contracts.nl/freelance-ict-opdrachten-management-servicemanager',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    {
        'label': 'IT-Contracts — Procesmanager',
        'url': 'https://www.it-contracts.nl/freelance-ict-opdrachten-management-procesmanager',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    {
        'label': 'IT-Contracts — Service Delivery Manager',
        'url': 'https://www.it-contracts.nl/nieuwste-freelance-ict-opdrachten/search/service+delivery+manager/from/',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    {
        'label': 'IT-Contracts — ITIL',
        'url': 'https://www.it-contracts.nl/nieuwste-freelance-ict-opdrachten/search/ITIL/from/',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    {
        'label': 'IT-Contracts — ITSM Consultant',
        'url': 'https://www.it-contracts.nl/nieuwste-freelance-ict-opdrachten/search/ITSM+consultant/from/',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    {
        'label': 'IT-Contracts — SIAM',
        'url': 'https://www.it-contracts.nl/nieuwste-freelance-ict-opdrachten/search/SIAM/from/',
        'category': 'itcontracts',
        'parser': 'itcontracts',
    },
    # Freep
    {
        'label': 'Freep — Procesmanager',
        'url': 'https://www.freep.nl/zzp/procesmanager',
        'category': 'zzp',
        'parser': 'freep',
    },
    {
        'label': 'Freep — Servicemanager',
        'url': 'https://www.freep.nl/zzp/servicemanager',
        'category': 'zzp',
        'parser': 'freep',
    },
    # ZZP-Opdrachten
    {
        'label': 'ZZP-Opdrachten — ITIL procesmanager',
        'url': 'https://www.zzp-opdrachten.nl/vacatures/?s=ITIL+procesmanager',
        'category': 'zzp',
        'parser': 'zzpopdrachten',
    },
    {
        'label': 'ZZP-Opdrachten — Service Delivery Manager',
        'url': 'https://www.zzp-opdrachten.nl/vacatures/?s=service+delivery+manager',
        'category': 'zzp',
        'parser': 'zzpopdrachten',
    },
    {
        'label': 'ZZP-Opdrachten — ITSM consultant',
        'url': 'https://www.zzp-opdrachten.nl/vacatures/?s=ITSM+consultant',
        'category': 'zzp',
        'parser': 'zzpopdrachten',
    },
    # Planet Interim
    {
        'label': 'Planet Interim — Procesmanager',
        'url': 'https://planetinterim.nl/interim-procesmanager/280009/p13/default.html',
        'category': 'aggregators',
        'parser': 'planet',
    },
    {
        'label': 'Planet Interim — ITIL Service Manager',
        'url': 'https://planetinterim.nl/itil-service-manager-ai/474674/p13/default.html',
        'category': 'aggregators',
        'parser': 'planet',
    },
    # Funle (JS-heavy, generieke fallback)
    {
        'label': 'Funle — Service Delivery Manager',
        'url': 'https://funle.nl/opdrachten?q=service+delivery+manager',
        'category': 'aggregators',
        'parser': 'generic',
        'link_pattern': r'/opdrachten?',
    },
    {
        'label': 'Funle — Procesmanager ITIL',
        'url': 'https://funle.nl/opdrachten?q=procesmanager+ITIL',
        'category': 'aggregators',
        'parser': 'generic',
        'link_pattern': r'/opdrachten?',
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# DEDUPLICATIE
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(results):
    """Verwijder duplicaten op basis van URL en gelijksoortige titels."""
    seen_urls = set()
    seen_titles = set()
    deduped = []

    for r in results:
        url = r.get('url', '').rstrip('/')
        title_key = re.sub(r'\s+', ' ', r.get('title', '').lower()[:60])

        if url in seen_urls:
            continue
        if title_key in seen_titles and len(title_key) > 15:
            continue

        seen_urls.add(url)
        seen_titles.add(title_key)
        deduped.append(r)

    return deduped


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    all_results = []
    errors = []

    for source in SOURCES:
        label = source['label']
        url = source['url']
        parser_type = source['parser']
        category = source['category']

        log.info(f'Scraping: {label}')
        html = fetch(url)

        try:
            if parser_type == 'itcontracts':
                results = parse_itcontracts(html, label, url)
            elif parser_type == 'freep':
                results = parse_freep(html, label)
            elif parser_type == 'zzpopdrachten':
                results = parse_zzpopdrachten(html, label, url)
            elif parser_type == 'planet':
                results = parse_planet_interim(html, label, url)
            elif parser_type == 'generic':
                results = parse_generic(
                    html, label, category, url,
                    link_pattern=source.get('link_pattern')
                )
            else:
                results = []

            all_results.extend(results)

        except Exception as e:
            msg = f'{label}: {e}'
            log.error(f'  Parser fout — {msg}')
            errors.append(msg)

        time.sleep(DELAY)

    # Dedupliceer
    log.info(f'Totaal voor deduplicatie: {len(all_results)}')
    all_results = deduplicate(all_results)
    log.info(f'Totaal na deduplicatie: {len(all_results)}')

    # Splits in gefilterd en niet-gefilterd
    filtered_in = [r for r in all_results if r['filtered_in']]
    filtered_out = [r for r in all_results if not r['filtered_in']]

    log.info(f'Gefilterd IN: {len(filtered_in)} | Gefilterd UIT: {len(filtered_out)}')

    # Sorteer — gefilterd_in eerst, dan op bron
    filtered_in.sort(key=lambda x: x.get('source', ''))
    filtered_out.sort(key=lambda x: x.get('source', ''))

    output = {
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'scraped_at_nl': datetime.now().strftime('%d %B %Y om %H:%M'),
        'total_found': len(all_results),
        'total_relevant': len(filtered_in),
        'total_filtered_out': len(filtered_out),
        'sources_scraped': len(SOURCES),
        'errors': errors,
        'results': filtered_in,
        'filtered_out': filtered_out[:20],  # max 20 voor debug
    }

    # Schrijf naar data/results.json
    import os
    os.makedirs('data', exist_ok=True)
    with open('data/results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f'✅ Geschreven naar data/results.json — {len(filtered_in)} relevante opdrachten')
    return output


if __name__ == '__main__':
    run()
