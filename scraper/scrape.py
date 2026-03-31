#!/usr/bin/env python3
"""
BSD Advies — Opdrachten Scraper v3
Verbeteringen:
- Betere filterlogica: IT-context verplicht bij procesmanager
- Kwartiermaker, Change Manager, Risk Manager (IT), ISO toegevoegd
- ZZP-sortering: expliciet ZZP eerst, twijfel tweede, rest derde
- Striive authenticated via Playwright
- Freelance.nl authenticated via Playwright
- Funle authenticated via Playwright
"""

import os, re, sys, json, time, hashlib, logging, requests, yaml
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, quote_plus

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S', stream=sys.stdout)
log = logging.getLogger('bsd')

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
PLATFORMS_FILE = os.path.join(os.path.dirname(__file__), 'platforms.yml')
OUTPUT_FILE    = os.path.join(os.path.dirname(__file__), '..', 'data', 'results.json')
REQUEST_DELAY  = 2
VALIDATE_DELAY = 1

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/122.0.0.0 Safari/537.36'),
    'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
}

# ── FILTER DEFINITIES ─────────────────────────────────────────────────────────

# Termen die ALTIJD matchen (IT-specifiek genoeg)
IT_SPECIFIC_TERMS = [
    'servicemanager', 'service manager',
    'service delivery manager', 'service delivery',
    'sdm', 'itsm', 'siam', 'itil',
    'leveranciersregie', 'leveranciersmanagement',
    'service integratie', 'service integration',
    'it governance', 'regieorganisatie',
    'it service management', 'incident management',
    'change management', 'problem management',
    'release management', 'configuration management',
    'it sourcing', 'managed services manager',
]

# Termen die ALLEEN matchen als ook een IT-context aanwezig is
CONTEXT_DEPENDENT_TERMS = [
    'procesmanager', 'proces manager', 'process manager',
    'change manager',
    'kwartiermaker',
    'risk manager',
    'iso 27001', 'iso27001', 'nen 7510', 'nen7510',
    'it risk', 'informatiebeveiliging',
    'projectmanager', 'project manager',
    'programmamanager', 'programma manager',
]

# IT-context vereist als combinatie met CONTEXT_DEPENDENT_TERMS
IT_CONTEXT_SIGNALS = [
    'it-', 'it ', 'ict', 'ict-', 'digital', 'software', 'systeem', 'system',
    'applicatie', 'application', 'infra', 'cloud', 'netwerk', 'network',
    'itsm', 'siam', 'itil', 'agile', 'scrum', 'devops',
    'security', 'informatiebeveiliging', 'compliance', 'governance',
    'platform', 'outsourc', 'managed service', 'technolog', 'automatiser',
    'informatievoorziening', 'informatiemanagement',
    'servicedesk', 'helpdesk', 'operations', 'incident management',
    'change management', 'problem management', 'configuration', 'cmdb',
    'leveranciersregie', 'leveranciersmanagement', 'sla-beheer',
    'iso 27001', 'nen 7510', 'cism', 'ciso',
    'service integratie', 'service integration', 'service delivery',
    'servicemanager', 'service management',
]

KNOCKOUT_TERMS = [
    'woningbouw', 'wonen', 'vastgoed', 'ruimtelijke ordening',
    'civiel', 'infra project',
    'jeugd', 'zorg', 'welzijn', 'sociaal domein',
    'inkoop procesmanager', 'logistiek procesmanager',
    'hr procesmanager', 'personeelsmanager',
    'financieel procesmanager', 'financiele processen',
    'communicatie procesmanager',
    'stikstof', 'woontafel', 'mobiliteitshu', 'bosbele',
    'recreatiewoning', 'campus & facilit', 'vervoer gemeente',
    'brandpreventie', 'brabantse ontwikkel',
]

# ZZP-detectie signalen
ZZP_EXPLICIT = [
    'zzp', 'zzp\'er', 'zelfstandige', 'freelance', 'freelancer',
    'inhuuropdracht', 'inhuur', 'zelfstandig professional',
    'interim opdracht', 'opdracht voor',
]

ZZP_DOUBTFUL = [
    'interim', 'tijdelijk', 'contract', 'detachering', 'uitzend',
]

ZZP_NEGATIVE = [
    'vaste dienst', 'loondienst', 'dienstverband', 'vast dienstverband',
    'payroll', 'in loondienst', 'in dienst treden',
]

WORK_SIGNALS = {
    'onsite': ['100% op locatie', 'volledig op locatie', '5 dagen', 'volledig kantoor'],
    'hybrid': ['hybride', 'hybrid', 'deels thuis', 'deels op locatie', '2 dagen', '3 dagen'],
    'remote': ['volledig remote', '100% remote', 'volledig thuis', 'thuiswerk'],
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
    'reageer', 'solliciteer', 'reactie sturen', 'aanmelden', 'sluitingsdatum',
    'startdatum', 'inzenden', 'apply', 'reageren kan', 'uren per week',
    'uurtarief', 'tarief', 'schrijf je in',
]

SKIP_VALIDATION_DOMAINS = [
    'funle.nl', 'google.nl', 'google.com', 'linkedin.com',
    'depubliekepartner.nl', 'striive.com', 'freep.nl',
    'freelance.nl',
]

STEDEN = [
    'Amsterdam', 'Rotterdam', 'Den Haag', 'Utrecht', 'Eindhoven', 'Groningen',
    'Tilburg', 'Almere', 'Breda', 'Nijmegen', 'Apeldoorn', 'Haarlem', 'Arnhem',
    'Zwolle', 'Zoetermeer', 'Leiden', 'Maastricht', 'Dordrecht', 'Ede', 'Venlo',
    'Deventer', 'Delft', 'Assen', 'Amersfoort', 'Heerlen', 'Leeuwarden', 'Helmond',
    'Alkmaar', 'Emmen', 'Enschede', 'Den Bosch', 'Schiedam', 'Maarssen',
    'Haarlemmermeer', 'Westland', 'Houten', 'Alphen', 'Barendrecht',
]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def clean(text):
    if not text: return ''
    return re.sub(r'\s+', ' ', str(text).strip())

def tl(text):
    return clean(text).lower()

def detect_location(text):
    """Detecteer stad in tekst. Vermijd provincie-namen die stad overschrijven."""
    # Specifieke steden eerst (meest specifiek)
    specific_cities = [
        'Amsterdam', 'Rotterdam', 'Den Haag', 'Eindhoven', 'Groningen',
        'Tilburg', 'Almere', 'Breda', 'Nijmegen', 'Apeldoorn', 'Haarlem', 'Arnhem',
        'Zwolle', 'Zoetermeer', 'Leiden', 'Maastricht', 'Dordrecht', 'Ede', 'Venlo',
        'Deventer', 'Delft', 'Assen', 'Amersfoort', 'Heerlen', 'Leeuwarden', 'Helmond',
        'Alkmaar', 'Emmen', 'Enschede', 'Den Bosch', 'Schiedam', 'Maarssen',
        'Haarlemmermeer', 'Westland', 'Houten', 'Alphen', 'Barendrecht',
        'Woerden', 'Zeist', 'Nieuwegein', 'Veenendaal', 'Culemborg',
    ]
    for stad in specific_cities:
        if stad.lower() in text.lower():
            return stad

    # Utrecht als stad alleen als het niet "Provincie Utrecht" of "Waterschap" is
    if 'utrecht' in text.lower():
        if 'provincie utrecht' not in text.lower() and 'waterschap' not in text.lower():
            return 'Utrecht'

    return ''

def detect_work(text):
    t = tl(text)
    found = [wt for wt, sigs in WORK_SIGNALS.items() if any(s in t for s in sigs)]
    return found if found else ['hybrid']

def detect_zzp_tier(text):
    """
    Bepaal ZZP-tier voor sortering:
    1 = expliciet ZZP (bovenaan)
    2 = twijfel (midden)
    3 = niet-ZZP (onderaan)
    """
    t = tl(text)
    if any(s in t for s in ZZP_NEGATIVE):
        return 3
    if any(s in t for s in ZZP_EXPLICIT):
        return 1
    if any(s in t for s in ZZP_DOUBTFUL):
        return 2
    return 2  # default: twijfel

def detect_contract(text):
    t = tl(text)
    types = []
    if any(s in t for s in ZZP_EXPLICIT): types.append('zzp')
    if any(s in t for s in ['midlance', 'interlance']): types.append('mid')
    if 'payroll' in t: types.append('pay')
    if any(s in t for s in ZZP_NEGATIVE): types.append('loon')
    return types if types else ['zzp']  # default aanname voor opdrachten-platforms

def is_target(title, description='', search_term='', it_category=False):
    """
    Gelaagde filterlogica:
    1. Knock-out check (altijd)
    2. Als it_category=True: vertrouw platform-categorie, alleen knock-out check
    3. IT-specifieke termen in titel/omschrijving: direct match
    4. Context-afhankelijke termen: IT-synoniem vereist in gecombineerde tekst
    """
    combined = tl(f"{title} {description} {search_term}")

    # Laag 1: Knock-out altijd (ook bij IT-categorie)
    for ko in KNOCKOUT_TERMS:
        if ko in combined:
            return False, f'Knock-out: "{ko}"'

    # Laag 2: Platform-categorie is al IT → vertrouw het
    if it_category:
        return True, 'IT platform-categorie'

    # Laag 3: IT-specifieke termen → direct match
    if any(t in combined for t in IT_SPECIFIC_TERMS):
        return True, 'IT-specifieke term'

    # Laag 4: Context-afhankelijke termen → IT-synoniem vereist
    has_context_term = any(t in combined for t in CONTEXT_DEPENDENT_TERMS)
    if has_context_term:
        has_it = any(c in combined for c in IT_CONTEXT_SIGNALS)
        return (True, 'Context + IT-synoniem') if has_it else (False, 'Geen IT-synoniem')

    return False, 'Geen relevante zoekterm'

def content_hash(title, opdrachtgever='', regio='', startdatum=''):
    def norm(s):
        s = tl(s)
        s = re.sub(r'[^a-z0-9 ]', '', s)
        return re.sub(r'\s+', ' ', s).strip()
    key = f"{norm(title)}|{norm(opdrachtgever)}|{norm(regio)}|{norm(startdatum)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def make_result(title, url, source, platform_id, category,
                location='', description='', tarief='', duration='',
                hours='', published='', opdrachtgever='', startdatum='',
                search_term='', it_category=False):
    ok, reason = is_target(title, description, search_term, it_category)
    combined   = f"{title} {description}"
    zzp_tier   = detect_zzp_tier(combined)
    return {
        'title':           clean(title),
        'url':             url,
        'source':          source,
        'platform_id':     platform_id,
        'category':        category,
        'location':        clean(location) or detect_location(combined),
        'opdrachtgever':   clean(opdrachtgever),
        'description':     clean(description)[:600],
        'tarief':          clean(tarief),
        'duration':        clean(duration),
        'hours':           clean(hours),
        'published':       published,
        'startdatum':      clean(startdatum),
        'contract':        detect_contract(combined),
        'work':            detect_work(combined),
        'zzp_tier':        zzp_tier,   # 1=zzp, 2=twijfel, 3=niet-zzp
        'filtered_in':     ok,
        'filter_reason':   reason,
        'content_hash':    content_hash(title, opdrachtgever, location, startdatum),
        'scraped_at':      datetime.now(timezone.utc).isoformat(),
        'vacancy_status':  'unknown',
        'vacancy_status_reason': 'Nog niet gevalideerd',
        'sources':         [source],
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
    for tag in soup.find_all(['footer', 'nav', 'aside', 'header']):
        tag.decompose()
    for link in soup.find_all('a', href=re.compile(r'/vacature_')):
        href = link.get('href', '')
        full = urljoin('https://www.it-contracts.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue
        row = link.find_parent(['tr', 'div', 'li'])
        loc = tarief = ''
        if row:
            text = row.get_text()[:300]  # Begrens tot 300 chars
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

    # Verwijder footer/nav/sidebar zodat die locaties niet meegepakt worden
    for tag in soup.find_all(['footer', 'nav', 'aside', 'header']):
        tag.decompose()

    for link in soup.find_all('a', href=re.compile(r'/opdracht/')):
        href = link.get('href', '')
        full = urljoin('https://www.freep.nl', href)
        if full in seen: continue
        seen.add(full)
        title = clean(link.get_text())
        if len(title) < 8: continue

        parent = link.find_parent(['li', 'div', 'article'])
        loc = hours = desc = ''
        if parent:
            # Locatie alleen uit specifieke elementen
            for loc_cls in ['locatie', 'location', 'stad', 'city', 'regio', 'plaats']:
                loc_el = parent.find(class_=re.compile(loc_cls, re.I))
                if loc_el:
                    loc = detect_location(loc_el.get_text())
                    if loc: break

            # Als nog geen locatie: zoek alleen in een korte tekst direct rond de link
            if not loc:
                # Zoek locatie in de titel zelf
                loc = detect_location(title)

            # Uren
            m = re.search(r'(\d{2,3})\s*uur', parent.get_text()[:200])
            if m: hours = f"{m.group(1)}u/wk"

            # Beschrijving — alleen eerste 200 chars van de card
            desc_el = parent.find(class_=re.compile(r'desc|omschrijving|tekst|intro|summary', re.I))
            if desc_el:
                desc = clean(desc_el.get_text())

        results.append(make_result(title, full, source, pid, 'zzp',
                                   location=loc, hours=hours, description=desc))
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
        results.append(make_result(title, href, source, pid, 'zzp',
                                   location=detect_location(f"{title} {desc}"), description=desc))
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
        full = urljoin('https://funle.nl', link.get('href', ''))
        if full in seen or len(title) < 8: continue
        seen.add(full)
        results.append(make_result(title, full, source, pid, 'aggregator',
                                   location=detect_location(el.get_text())))
    if not results:
        for link in soup.find_all('a', href=re.compile(r'/opdrachten/')):
            title = clean(link.get_text())
            full = urljoin('https://funle.nl', link.get('href', ''))
            if len(title) > 8 and full not in seen:
                seen.add(full)
                results.append(make_result(title, full, source, pid, 'aggregator'))
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
        full = urljoin('https://striive.com', link.get('href', ''))
        if full in seen or len(title) < 8: continue
        seen.add(full)
        results.append(make_result(title, full, source, pid, 'aggregator',
                                   location=detect_location(el.get_text())))
    if not results:
        for link in soup.find_all('a', href=re.compile(r'/nl/opdrachten/')):
            title = clean(link.get_text())
            full = urljoin('https://striive.com', link.get('href', ''))
            if len(title) > 8 and full not in seen:
                seen.add(full)
                results.append(make_result(title, full, source, pid, 'aggregator'))
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
    for link in soup.find_all('a', href=re.compile(r'/(vacature|opdracht|job)s?/', re.I)):
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


# ── PLAYWRIGHT BROWSER HELPERS ────────────────────────────────────────────────

def get_browser():
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
    ctx = browser.new_context(user_agent=HEADERS['User-Agent'], locale='nl-NL')
    return pw, browser, ctx


def parse_page_results(page, source, pid, base_url, link_pattern):
    """Parseer huidige pagina voor opdracht-links."""
    results = []
    soup = BeautifulSoup(page.content(), 'html.parser')
    seen = set()
    for link in soup.find_all('a', href=re.compile(link_pattern, re.I)):
        href = link.get('href', '')
        full = urljoin(base_url, href) if not href.startswith('http') else href
        if full in seen: continue
        seen.add(full)
        # Zoek titel in parent element
        parent = link.find_parent(['li', 'div', 'article', 'tr'])
        title_el = parent.find(['h2', 'h3', 'h4', 'strong']) if parent else None
        title = clean(title_el.get_text()) if title_el else clean(link.get_text())
        if len(title) < 8: continue
        text = parent.get_text() if parent else ''
        loc = detect_location(text)
        tarief = ''
        m = re.search(r'[€]\s*(\d{2,3})', text)
        if m: tarief = f"€{m.group(1)}/u"
        results.append(make_result(title, full, source, pid, 'aggregator',
                                   location=loc, tarief=tarief))
    return results


# ── STRIIVE AUTHENTICATED ─────────────────────────────────────────────────────

def _browser_login(page, login_url, email, password, success_check_fn,
                   label, timeout=30000):
    """Generieke login helper. Geeft True terug als login geslaagd."""
    page.goto(login_url, timeout=timeout)
    page.wait_for_load_state('networkidle', timeout=15000)
    time.sleep(2)

    # Probeer meerdere selector varianten
    for sel in ['input[type="email"]', 'input[name="email"]', '#email',
                'input[name="username"]', '#username']:
        try:
            if page.locator(sel).count() > 0:
                page.fill(sel, email)
                break
        except Exception:
            continue

    for sel in ['input[type="password"]', 'input[name="password"]', '#password']:
        try:
            if page.locator(sel).count() > 0:
                page.fill(sel, password)
                break
        except Exception:
            continue

    for sel in ['button[type="submit"]', 'input[type="submit"]',
                'button:has-text("Inloggen")', 'button:has-text("Login")',
                'button:has-text("Aanmelden")']:
        try:
            if page.locator(sel).count() > 0:
                page.click(sel)
                break
        except Exception:
            continue

    page.wait_for_load_state('networkidle', timeout=20000)
    time.sleep(3)

    if success_check_fn(page):
        log.info(f'{label}: ingelogd ✓')
        return True
    else:
        log.warning(f'{label}: login mislukt — URL: {page.url[:80]}')
        return False


def _extract_results_from_page(page, source, pid, base_url, link_pattern):
    """Haal alle opdracht-cards van huidige pagina op."""
    results = []
    soup = BeautifulSoup(page.content(), 'html.parser')
    seen = set()

    # Probeer meerdere card-selectors
    cards = (soup.select('article') or
             soup.select('[class*="vacancy"]') or
             soup.select('[class*="opdracht"]') or
             soup.select('[class*="job"]') or
             soup.select('li'))

    for card in cards:
        link = card.find('a', href=re.compile(link_pattern, re.I))
        if not link:
            continue
        href = link.get('href', '')
        full = urljoin(base_url, href) if not href.startswith('http') else href
        if full in seen:
            continue
        seen.add(full)

        title_el = card.find(['h2', 'h3', 'h4', 'strong'])
        title = clean(title_el.get_text()) if title_el else clean(link.get_text())
        if len(title) < 8:
            continue

        text = card.get_text()
        loc = detect_location(text)
        tarief = ''
        m = re.search(r'[€]\s*(\d{2,3})', text)
        if m:
            tarief = f"€{m.group(1)}/u"

        org_el = card.find(class_=re.compile(r'company|opdrachtgever|client|org', re.I))
        org = clean(org_el.get_text()) if org_el else ''

        results.append(make_result(title, full, source, pid, 'aggregator',
                                   location=loc, tarief=tarief, opdrachtgever=org,
                                   description=text[:200]))
    return results


def scrape_striive_auth(platform, results_list):
    """
    Striive: login → ga naar opdrachten → scrape 'Aanbevolen' tab.
    """
    creds    = platform.get('credentials', {})
    email    = os.environ.get(creds.get('username_secret', ''), '')
    password = os.environ.get(creds.get('password_secret', ''), '')
    if not email or not password:
        log.warning('Striive credentials niet gevonden — overgeslagen')
        return

    pid, label = platform['id'], platform['label']
    log.info(f'Striive: browser starten, inloggen als {email}')

    pw, browser, ctx = get_browser()
    page = ctx.new_page()
    try:
        ok = _browser_login(
            page, 'https://login.striive.com/',
            email, password,
            lambda p: 'login' not in p.url.lower() or 'striive.com' in p.url,
            label
        )
        if not ok:
            return

        log.info(f'  Striive URL na login: {page.url}')
        page.goto('https://striive.com/nl/opdrachten', timeout=20000)
        page.wait_for_load_state('networkidle', timeout=15000)
        time.sleep(3)
        log.info(f'  Striive URL na navigatie: {page.url}')

        # Log de page title en eerste 500 chars HTML voor debugging
        log.info(f'  Striive page title: {page.title()}')
        content_preview = page.content()[:500]
        log.info(f'  Striive HTML preview: {content_preview[:200]}')

        # Klik op Aanbevolen tab
        for tab_sel in [
            'button:has-text("Aanbevolen")', 'a:has-text("Aanbevolen")',
            '[data-tab="aanbevolen"]', '[class*="recommended"]',
            'button:has-text("Matches")', 'a:has-text("Matches")',
        ]:
            try:
                if page.locator(tab_sel).count() > 0:
                    page.click(tab_sel)
                    time.sleep(2)
                    log.info(f'  Striive: tab geklikt: {tab_sel}')
                    break
            except Exception:
                continue

        items = _extract_results_from_page(page, label, pid,
                                           'https://striive.com',
                                           r'/nl/opdrachten/\d+')
        log.info(f'  → {len(items)} items van Striive')
        results_list.extend(items)

        # Scroll voor meer
        for _ in range(3):
            try:
                page.keyboard.press('End')
                time.sleep(2)
                more = _extract_results_from_page(page, label, pid,
                                                  'https://striive.com',
                                                  r'/nl/opdrachten/\d+')
                new = [r for r in more if r['url'] not in
                       {x['url'] for x in results_list}]
                results_list.extend(new)
                if not new: break
            except Exception:
                break

        log.info(f'  → Striive totaal: {sum(1 for r in results_list if r["platform_id"] == pid)}')

    except Exception as e:
        log.error(f'Striive fout: {e}')
    finally:
        browser.close()
        pw.stop()


# ── FREELANCE.NL AUTHENTICATED ────────────────────────────────────────────────

def scrape_freelancenl_auth(platform, results_list):
    """
    Freelance.nl: login → Mijn zoekopdrachten → Open zoekopdracht 'Normaal'
    """
    creds    = platform.get('credentials', {})
    email    = os.environ.get(creds.get('username_secret', ''), '')
    password = os.environ.get(creds.get('password_secret', ''), '')
    if not email or not password:
        log.warning('Freelance.nl credentials niet gevonden — overgeslagen')
        return

    pid, label = platform['id'], platform['label']
    log.info(f'Freelance.nl: browser starten, inloggen als {email}')

    pw, browser, ctx = get_browser()
    page = ctx.new_page()
    try:
        ok = _browser_login(
            page, 'https://www.freelance.nl/inloggen',
            email, password,
            lambda p: 'inloggen' not in p.url.lower(),
            label
        )
        if not ok:
            return

        log.info(f'  Freelance.nl URL na login: {page.url}')
        log.info(f'  Freelance.nl page title: {page.title()}')

        page.goto('https://www.freelance.nl/mijn-zoekopdrachten', timeout=20000)
        page.wait_for_load_state('networkidle', timeout=15000)
        time.sleep(3)
        log.info(f'  Freelance.nl URL na navigatie: {page.url}')
        log.info(f'  Freelance.nl page title: {page.title()}')

        # Log alle links op de pagina voor debugging
        soup = BeautifulSoup(page.content(), 'html.parser')
        all_links = soup.find_all('a', href=True)
        log.info(f'  Freelance.nl: {len(all_links)} links op pagina')
        for lnk in all_links[:10]:
            log.info(f'    link: {lnk.get("href", "")[:60]} | text: {lnk.get_text()[:30]}')

        opened = False
        # Klik "Open zoekopdracht" direct via Playwright
        try:
            page.click('a:has-text("Open zoekopdracht")', timeout=8000)
            page.wait_for_load_state('networkidle', timeout=15000)
            time.sleep(3)
            opened = True
            log.info(f'  Freelance.nl: zoekopdracht geopend, URL: {page.url}')
        except Exception as e:
            log.warning(f'  Freelance.nl: Open zoekopdracht niet gevonden: {e}')

        if not opened:
            # Fallback: directe opdrachten pagina
            log.info('  Freelance.nl: fallback naar /opdrachten')
            page.goto('https://www.freelance.nl/opdrachten', timeout=20000)
            page.wait_for_load_state('networkidle', timeout=15000)
            time.sleep(3)

        log.info(f'  Freelance.nl scrape URL: {page.url}')
        items = _extract_results_from_page(page, label, pid,
                                           'https://www.freelance.nl',
                                           r'/opdracht/\d+')
        log.info(f'  → {len(items)} items van Freelance.nl')
        results_list.extend(items)

    except Exception as e:
        log.error(f'Freelance.nl fout: {e}')
    finally:
        browser.close()
        pw.stop()


# ── FUNLE AUTHENTICATED ───────────────────────────────────────────────────────

def scrape_funle_auth(platform, results_list):
    """
    Funle: login → navigeer naar opdrachten met zoekterm → scrape JS-rendered resultaten.
    Fallback naar publieke scrape als credentials ontbreken.
    """
    creds    = platform.get('credentials', {})
    email    = os.environ.get(creds.get('username_secret', ''), '')
    password = os.environ.get(creds.get('password_secret', ''), '')

    if not email or not password:
        log.warning('Funle credentials niet gevonden — publieke fallback')
        session = new_session()
        for search in platform.get('searches', []):
            url = search.get('url', '')
            if not url: continue
            html = fetch(session, url)
            items = parse_funle(html, platform['label'], platform['id'], url)
            results_list.extend(items)
            time.sleep(REQUEST_DELAY)
        return

    pid, label = platform['id'], platform['label']
    log.info(f'Funle: browser starten, inloggen als {email}')

    pw, browser, ctx = get_browser()
    page = ctx.new_page()
    try:
        # Login — Funle gebruikt waarschijnlijk een OAuth/SSO flow
        ok = _browser_login(
            page,
            'https://funle.nl/inloggen',
            email, password,
            lambda p: 'inloggen' not in p.url.lower() and 'login' not in p.url.lower(),
            label
        )
        if not ok:
            log.warning('Funle: login mislukt — publieke fallback')
            session = new_session()
            for search in platform.get('searches', []):
                url = search.get('url', '')
                if not url: continue
                html = fetch(session, url)
                items = parse_funle(html, label, pid, url)
                results_list.extend(items)
                time.sleep(REQUEST_DELAY)
            return

        # Scrape elke zoekopdracht URL
        for search in platform.get('searches', []):
            url  = search.get('url', '')
            term = search.get('term', '')
            if not url: continue
            log.info(f'  Funle: {term}')
            try:
                page.goto(url, timeout=20000)
                page.wait_for_load_state('networkidle', timeout=15000)
                time.sleep(4)  # Funle is zwaar JS

                items = _extract_results_from_page(page, label, pid,
                                                   'https://funle.nl',
                                                   r'/opdrachten/[a-z0-9-]+')
                log.info(f'  → {len(items)} items')
                results_list.extend(items)
            except Exception as e:
                log.warning(f'  Funle zoek fout "{term}": {e}')
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        log.error(f'Funle fout: {e}')
    finally:
        browser.close()
        pw.stop()


# ── LINKEDIN ──────────────────────────────────────────────────────────────────

def scrape_linkedin(platform, results_list):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning('Playwright niet beschikbaar — LinkedIn overgeslagen')
        return

    creds    = platform.get('credentials', {})
    email    = os.environ.get(creds.get('username_secret', ''), '')
    password = os.environ.get(creds.get('password_secret', ''), '')
    if not email or not password:
        log.warning('LinkedIn credentials niet gevonden — overgeslagen')
        return

    pid, label, parser_type = platform['id'], platform['label'], platform['parser']

    pw, browser, ctx = get_browser()
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
        pw.stop()


def _li_jobs(page, platform, pid, label, results_list):
    for search in platform.get('searches', []):
        term, filters = search.get('term', ''), search.get('filters', '')
        url = f"https://www.linkedin.com/jobs/search/?keywords={quote_plus(term)}&{filters}"
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
                job_url = urljoin('https://www.linkedin.com', link.get('href', '').split('?')[0])
                org_el = card.find(class_=re.compile(r'company|employer', re.I))
                org = clean(org_el.get_text()) if org_el else ''
                loc_el = card.find(class_=re.compile(r'location|locati', re.I))
                loc = clean(loc_el.get_text()) if loc_el else detect_location(card.get_text())
                results_list.append(make_result(title, job_url, label, pid, 'linkedin',
                                                location=loc, opdrachtgever=org))
        except Exception as e:
            log.warning(f'  LI Jobs fout "{term}": {e}')
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
                description=text[:300], opdrachtgever=org))
            found += 1
        log.info(f'  → {found} feed-posts')
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
        if r.status_code in (404, 410): return 'closed', f'HTTP {r.status_code}'
        if r.status_code >= 400: return 'unknown', f'HTTP {r.status_code}'
        tlow = r.text.lower()
        for sig in CLOSED_SIGNALS:
            if sig in tlow: return 'closed', f'"{sig}"'
        open_hits = sum(1 for s in OPEN_SIGNALS if s in tlow)
        if open_hits >= 2: return 'open', f'{open_hits} open-signalen'
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
            for field in ['description', 'tarief', 'location', 'opdrachtgever', 'hours']:
                if r.get(field) and (not ex.get(field) or
                   (field == 'description' and len(r[field]) > len(ex.get(field, '')))):
                    ex[field] = r[field]
            # Neem laagste ZZP tier (meest zeker ZZP)
            if r.get('zzp_tier', 2) < ex.get('zzp_tier', 2):
                ex['zzp_tier'] = r['zzp_tier']
                ex['contract'] = r['contract']
    return list(seen.values())


# ── PARSER / PLATFORM TYPE MAP ────────────────────────────────────────────────

PARSERS = {
    'itcontracts':     parse_itcontracts,
    'freep':           parse_freep,
    'zzpopdrachten':   parse_zzpopdrachten,
    'funle':           parse_funle,
    'striive':         parse_striive,
    'publiekepartner': parse_publiekepartner,
    'circle8':         parse_circle8,
    'freelancenl':     parse_freelancenl,
    'generic':         parse_generic,
}

LINKEDIN_PARSERS   = {'linkedin_jobs', 'linkedin_feed'}


def scrape_browser_public(platform, results_list):
    """
    Generieke Playwright scraper voor publieke platforms met bot-detectie.
    Geen login nodig — echte browser omzeilt 403/429.
    """
    pid   = platform['id']
    label = platform['label']
    searches = platform.get('searches', [])

    log.info(f'Browser (publiek): {label}')

    pw, browser, ctx = get_browser()
    ctx.set_extra_http_headers({
        'Accept-Language': 'nl-NL,nl;q=0.9',
        'Referer': 'https://www.google.nl/',
    })
    page = ctx.new_page()

    try:
        for search in searches:
            url  = search.get('url', '')
            term = search.get('term', '')
            link_pattern = search.get('link_pattern', r'/(opdracht|vacature)s?/')
            if not url: continue

            log.info(f'  {label}: {term}')
            try:
                page.goto(url, timeout=25000)
                page.wait_for_load_state('networkidle', timeout=15000)
                time.sleep(3)

                if any(x in page.content().lower() for x in
                       ['access denied', 'blocked', 'captcha', 'cloudflare']):
                    log.warning(f'  {label}: geblokkeerd op {url[:60]}')
                    continue

                items = _extract_results_from_page(page, label, pid,
                                                   url, link_pattern)
                log.info(f'  → {len(items)} items')
                results_list.extend(items)

            except Exception as e:
                log.warning(f'  {label} fout voor "{term}": {e}')
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        log.error(f'{label} browser fout: {e}')
    finally:
        browser.close()
        pw.stop()


BROWSER_PARSERS    = {
    'striive_auth':     scrape_striive_auth,
    'freelancenl_auth': scrape_freelancenl_auth,
    'funle_auth':       scrape_funle_auth,
    'browser':          scrape_browser_public,
}


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
        n_platforms += 1

        # Authenticated browser scrapers
        if parser_type in BROWSER_PARSERS:
            log.info(f'\nPlatform: {label} [browser + login]')
            try:
                BROWSER_PARSERS[parser_type](plat, all_results)
            except Exception as e:
                log.error(f'{label} fout (niet fataal): {e}')
                errors.append(f'{label}: {e}')
            continue

        # LinkedIn
        if parser_type in LINKEDIN_PARSERS:
            log.info(f'\nPlatform: {label} [browser]')
            try:
                scrape_linkedin(plat, all_results)
            except Exception as e:
                log.error(f'{label} fout (niet fataal): {e}')
                errors.append(f'{label}: {e}')
            continue

        # Publieke HTTP scrapers
        parser_fn = PARSERS.get(parser_type)
        if not parser_fn:
            log.warning(f'Geen parser voor "{parser_type}" — {label}')
            continue

        log.info(f'\nPlatform: {label} [{len(searches)} zoekopdrachten]')
        for search in searches:
            url         = search.get('url', '')
            search_term = search.get('term', '')
            it_cat      = search.get('it_category', plat.get('it_category', False))
            if not url: continue
            html = fetch(session, url)
            try:
                items = parser_fn(html, label, pid, url)
                # Pas search_term en it_category toe op alle items van deze search
                for item in items:
                    if search_term and not item.get('description'):
                        # Herbereken filter met search_term als extra context
                        ok, reason = is_target(
                            item['title'], item.get('description', ''),
                            search_term, it_cat
                        )
                        item['filtered_in']  = ok
                        item['filter_reason'] = reason
                    elif it_cat:
                        ok, reason = is_target(
                            item['title'], item.get('description', ''),
                            search_term, it_cat
                        )
                        item['filtered_in']  = ok
                        item['filter_reason'] = reason
                all_results.extend(items)
            except Exception as e:
                msg = f'{label}: {e}'
                log.error(f'  Parser fout: {msg}')
                errors.append(msg)
            time.sleep(REQUEST_DELAY)

    log.info(f'\n=== {len(all_results)} voor deduplicatie ===')
    all_results = deduplicate(all_results)
    log.info(f'=== {len(all_results)} na deduplicatie ===')

    relevant   = [r for r in all_results if r['filtered_in']]
    irrelevant = [r for r in all_results if not r['filtered_in']]
    log.info(f'Relevant: {len(relevant)} | Irrelevant: {len(irrelevant)}')

    # Validatie
    log.info('\n=== Validatie ===')
    open_n = closed_n = unknown_n = 0
    for r in relevant:
        status, reason = validate_vacancy(r, session)
        r['vacancy_status'] = status
        r['vacancy_status_reason'] = reason
        if status == 'open':     open_n += 1
        elif status == 'closed': closed_n += 1
        else:                    unknown_n += 1
        time.sleep(VALIDATE_DELAY)

    log.info(f'Open: {open_n} | Onbekend: {unknown_n} | Gesloten: {closed_n}')

    # Sortering: open eerst, dan onbekend
    # Binnen elke groep: ZZP tier 1 (expliciet) → tier 2 (twijfel) → tier 3 (niet-zzp)
    def sort_key(r):
        status_order = {'open': 0, 'unknown': 1, 'closed': 2}.get(r.get('vacancy_status', 'unknown'), 1)
        zzp_order    = r.get('zzp_tier', 2)
        return (status_order, zzp_order)

    closed_r = [r for r in relevant if r['vacancy_status'] == 'closed']
    final    = sorted(
        [r for r in relevant if r['vacancy_status'] != 'closed'],
        key=sort_key
    )

    output = {
        'scraped_at':         datetime.now(timezone.utc).isoformat(),
        'scraped_at_nl':      datetime.now().strftime('%d %B %Y om %H:%M'),
        'total_found':        len(all_results),
        'total_relevant':     len(relevant),
        'total_open':         open_n,
        'total_unknown':      unknown_n,
        'total_closed':       closed_n,
        'total_filtered_out': len(irrelevant),
        'platforms_scraped':  n_platforms,
        'errors':             errors,
        'results':            final,
        'closed_debug':       [{'title': r['title'], 'reason': r['vacancy_status_reason']} for r in closed_r[:10]],
        'filtered_out_debug': [{'title': r['title'], 'source': r['source'], 'reason': r['filter_reason']} for r in irrelevant[:15]],
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f'\n✅ Klaar — {len(final)} resultaten geschreven')
    return output


if __name__ == '__main__':
    run()
