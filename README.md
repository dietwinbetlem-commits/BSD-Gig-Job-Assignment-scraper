# BSD Opdrachten Dashboard

> Dagelijks opdrachten-scannen voor IT Procesmanager · IT Servicemanager · Service Delivery Manager · ITSM/SIAM Consultant

**BSD Advies & Interim · Dietwin Betlem · Haarlem**

---

## Wat is dit?

Een single-file HTML dashboard voor het dagelijks bijhouden en filteren van freelance/ZZP IT-managementopdrachten in Nederland. Geen server, geen installatie — gewoon het HTML-bestand openen in je browser.

Gebouwd voor het zoekprofiel van een senior IT-procesmanager/servicemanager met ITIL/SIAM expertise, actief op de Nederlandse markt.

---

## Bestanden

| Bestand | Omschrijving |
|---|---|
| `index.html` | Het dashboard zelf — alles in één bestand |
| `infographic.html` | Visuele uitleg van de functionaliteit |
| `README.md` | Dit bestand |

---

## Functionaliteit

### 🗺️ Interactieve kaart
- **Leaflet.js + CartoDB Dark Matter tiles** — echte kaart, geografisch correct
- Gekleurde pins per contracttype (groen = ZZP, oranje = Payroll, rood = Loondienst)
- Klik op een pin voor opdrachtdetails en directe link
- Zoom/pan via scrollwiel, slepen en pinch-to-zoom

### 🔘 Contract-type filters
Filterlogica als pills, klik om aan/uit te zetten:

| Type | Kleur | Standaard |
|---|---|---|
| ZZP ✅ | Groen | Aan |
| Midlance/Interlance | Lichtgroen | Aan |
| Payroll | Oranje | Uit |
| Loondienst | Rood | Verborgen |

> Als ZZP mogelijk is → Midlance en Payroll zijn impliciet ook opties (weergegeven als ↔ badges)

### 🔗 Platforms accordion
- 30+ bronnen verdeeld over 6 categorieën
- Vink individuele links aan/uit
- "Open geselecteerde" → klikbaar link-panel (omzeilt popup-blocker)

| Categorie | Platforms |
|---|---|
| IT-Contracts.nl | Servicemanager, Procesmanager, SDM, ITIL, ITSM, SIAM |
| Aggregators | Funle, Planet Interim |
| ZZP Platforms | Freep, ZZP-Opdrachten |
| Bureaus | Harvey Nash, YER, Malt, Jellow, IPmarktplaats |
| LinkedIn | 4 zoeklinks (24u filter) + profielbeheer |
| Extra | Jobhob, Google (24u), ZZP-ICT-Opdrachten |

### 📋 Status tracking per opdracht
`Nieuw` → `Gereageerd` → `Interview` → `Afgewezen` → `Aangenomen`

### ⚙️ Volledig bewerkbaar
Editor tab met CRUD voor:
- Opdrachten (incl. stad/locatie voor kaartpin)
- Platform links
- Filterregels (altijd reageren / afvallen)
- Reset naar standaard

---

## Gebruik

1. Download `index.html`
2. Open in browser (Chrome/Firefox/Edge)
3. Internetverbinding nodig voor de kaarttegels

Alle data wordt lokaal opgeslagen via `localStorage` (key: `bsd_v6`). Wijzigingen blijven bewaard na sluiten van de browser.

---

## Filterregels

### Altijd reageren
- ZZP / freelance / midlance / interlance expliciet vermeld
- Procesverbetering, ITIL-inrichting, herinrichting processen
- Service Delivery Management bij privaat bedrijf
- Leveranciersregie, SIAM, multi-vendor omgeving
- ITSM/SIAM consultant opdracht
- Duur 3–12 maanden + optie verlenging
- Hybride of remote werkwijze

### Afvallen
- Expliciet "alleen detachering" of "ZZP niet mogelijk"
- "Delivery management" zonder "service" — is project/supply chain, niet SDM
- Knock-out: alleen G4 gemeente ervaring vereist
- Knock-out: alleen overheid 1500+ FTE als eis
- Tooling-specialist (ServiceNow developer, geen management)
- Tarief onder €90/uur expliciet vermeld

---

## Technisch

- Pure HTML/CSS/JS — geen framework, geen build-stap
- [Leaflet.js 1.9.4](https://leafletjs.com/) voor de kaart
- [CartoDB Dark Matter](https://carto.com/basemaps/) kaarttiles
- `localStorage` voor persistentie
- Alle data en logica in `index.html`

---

## Actuele matches (27 maart 2026)

| Prioriteit | Opdracht | Bron | ZZP |
|---|---|---|---|
| ⭐⭐⭐ | IT Procesmanager — Univé, Zwolle | Harvey Nash / IT-Contracts | ✅ |
| ⭐⭐⭐ | IT Servicemanager — Univé, Zwolle/Assen | Harvey Nash / IT-Contracts | ✅ |
| ⭐⭐ | Procesmanager Service Integratie | ZZP-Opdrachten.nl | ✅ |
| ⭐⭐ | IT Procesmanager Operationeel — Kadaster | Freep.nl | ✅ |

**Harvey Nash contact:**
- 📞 0346-581070 · Industrieweg 4, Maarssen
- Procesmanager: raya.al-jazairy@harveynash.com (Ref: ITC-BBBH119568)
- Servicemanager: Ricardo Heijveld (Ref: ITC-BBBH119567)

---

*BSD Advies & Interim · IT Governance · ITSM/SIAM · Digital Transformation · Haarlem*
