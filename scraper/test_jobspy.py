#!/usr/bin/env python3
"""
JobSpy test — diagnose dekking voor NL IT ZZP markt.
Tijdelijk script, verwijderen na analyse.
"""
import json
from jobspy import scrape_jobs

SEARCHES = [
    {"term": "IT procesmanager ITSM", "loc": "Nederland"},
    {"term": "service delivery manager ITIL", "loc": "Nederland"},
    {"term": "SIAM consultant interim", "loc": "Nederland"},
]

print("=== JobSpy test — NL IT ZZP ===\n")

for s in SEARCHES:
    print(f"Zoekterm: '{s['term']}'")
    try:
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=s["term"],
            google_search_term=f"{s['term']} opdracht nederland",
            location=s["loc"],
            results_wanted=10,
            hours_old=72,
            country_indeed="Netherlands",
            job_type="contract",
            verbose=0,
        )
        print(f"  Gevonden: {len(jobs)} resultaten")
        for _, row in jobs.iterrows():
            print(f"  [{row.get('site','?')}] {row.get('title','?')[:50]} | {row.get('company','?')[:25]} | {row.get('location','?')[:20]}")
            print(f"    URL: {str(row.get('job_url',''))[:80]}")
    except Exception as e:
        print(f"  FOUT: {e}")
    print()
