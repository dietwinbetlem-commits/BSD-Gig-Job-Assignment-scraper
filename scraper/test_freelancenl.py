#!/usr/bin/env python3
"""
Freelance.nl login test — diagnose login flow via Playwright.
Tijdelijk script, verwijderen na analyse.
"""
import os, time
from playwright.sync_api import sync_playwright

EMAIL    = os.environ.get('FREELANCENL_EMAIL', '')
PASSWORD = os.environ.get('FREELANCENL_PASSWORD', '')

def test():
    print(f'=== Freelance.nl login test ===')
    print(f'Email: {EMAIL[:4]}***')

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        # Stap 1: laad loginpagina
        print('\n1. Navigeer naar inlogpagina...')
        page.goto('https://www.freelance.nl/inloggen', timeout=30000)
        page.wait_for_load_state('networkidle', timeout=15000)
        print(f'   URL: {page.url}')
        print(f'   Title: {page.title()}')

        # Stap 2: zoek login-formulier velden
        print('\n2. Zoek input velden...')
        for sel in ['input[type="email"]', 'input[name="email"]', '#email', 'input[name="username"]', '#username']:
            count = page.locator(sel).count()
            if count > 0:
                print(f'   Email veld gevonden: {sel}')
                break
        else:
            print('   GEEN email veld gevonden!')
            print(f'   Body tekst: {page.locator("body").inner_text()[:300]}')

        # Stap 3: vul in en submit
        print('\n3. Vul credentials in...')
        try:
            page.fill('input[type="email"], input[name="email"], #email', EMAIL)
            page.fill('input[type="password"], input[name="password"], #password', PASSWORD)
            print('   Credentials ingevuld')

            page.click('button[type="submit"], input[type="submit"]')
            page.wait_for_load_state('networkidle', timeout=15000)
            time.sleep(2)
        except Exception as e:
            print(f'   FOUT bij invullen: {e}')

        # Stap 4: check resultaat
        print(f'\n4. Na submit:')
        print(f'   URL: {page.url}')
        print(f'   Title: {page.title()}')

        if 'inloggen' in page.url.lower():
            print('   STATUS: LOGIN MISLUKT — nog op loginpagina')
            # Log wat er op de pagina staat
            print(f'   Body: {page.locator("body").inner_text()[:400]}')
        else:
            print('   STATUS: LOGIN GESLAAGD ✓')

            # Stap 5: navigeer naar mijn zoekopdrachten
            print('\n5. Navigeer naar Mijn zoekopdrachten...')
            page.goto('https://www.freelance.nl/mijn-zoekopdrachten', timeout=20000)
            page.wait_for_load_state('networkidle', timeout=15000)
            time.sleep(2)
            print(f'   URL: {page.url}')
            print(f'   Body: {page.locator("body").inner_text()[:600]}')

        browser.close()

if __name__ == '__main__':
    test()
