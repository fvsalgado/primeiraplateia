"""
Scraper: Teatro Variedades
Fonte: https://teatrovariedades-capitolio.pt/agenda/teatro-variedades/

Toda a lógica de scraping está em scrapers/base_variedades.py,
partilhada com o Capitólio.
"""
from scrapers.base_variedades import scrape_theater

THEATER = {
    "id":          "variedades",
    "name":        "Teatro Variedades",
    "short":       "Variedades",
    "color":       "#E8372C",
    "city":        "Lisboa",
    "site":        "https://teatrovariedades-capitolio.pt",
    "programacao": "https://teatrovariedades-capitolio.pt/agenda/teatro-variedades/",
    "address":     "Rua António Pedro 32, 1000-040 Lisboa",
    "lat":         38.7172,
    "lng":         -9.1394,
    "logo_url":    "https://teatrovariedades-capitolio.pt/wp-content/themes/variedades/assets/images/logo-variedades.svg",
    "favicon_url": "https://teatrovariedades-capitolio.pt/favicon.ico",
    "facade_url":  "https://teatrovariedades-capitolio.pt/wp-content/uploads/facade-variedades.jpg",
    "description": "O Teatro Variedades é um dos mais emblemáticos teatros de Lisboa, com uma longa tradição de espectáculos de teatro, comédia e variedades.",
}

AGENDA_URL  = "https://teatrovariedades-capitolio.pt/agenda/teatro-variedades/?layout=grid&espaco=variedades"
SOURCE_SLUG = "variedades"


def scrape() -> list[dict]:
    return scrape_theater(
        theater_name=THEATER["name"],
        source_slug=SOURCE_SLUG,
        agenda_url=AGENDA_URL,
    )
