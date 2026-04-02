#!/usr/bin/env python3
"""
Funle API test — draai éénmalig om de API-structuur te ontdekken.
Verwijderen na analyse.
"""
import requests, json

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://funle.nl/',
    'Origin': 'https://funle.nl',
}

endpoints = [
    'https://funle.nl/api/public/assignments?q=procesmanager+ITIL&size=5',
    'https://funle.nl/api/assignments?q=procesmanager+ITIL&size=5',
    'https://funle.nl/api/search?q=procesmanager+ITIL',
    'https://funle.nl/api/public/opdrachten?q=procesmanager+ITIL',
    'https://funle.nl/api/v1/assignments?query=procesmanager+ITIL',
    'https://funle.nl/api/v1/search?q=procesmanager+ITIL',
]

print('=== Funle API test ===')
for url in endpoints:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        ct = r.headers.get('content-type', '')
        print(f'\n{r.status_code} | {url}')
        print(f'  Content-Type: {ct}')
        if r.status_code == 200 and 'json' in ct:
            data = r.json()
            print(f'  JSON keys: {list(data.keys()) if isinstance(data, dict) else type(data)}')
            print(f'  Preview: {json.dumps(data, default=str)[:300]}')
        else:
            print(f'  Body: {r.text[:100]}')
    except Exception as e:
        print(f'\nFOUT: {e}')
        print(f'  URL: {url}')
