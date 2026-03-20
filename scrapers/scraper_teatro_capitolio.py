"""
Scraper: Capitólio
Fonte: https://teatrovariedades-capitolio.pt/agenda/capitolio/

Toda a lógica de scraping está em scrapers/base_variedades.py,
partilhada com o Teatro Variedades.
"""
from scrapers.base_variedades import scrape_theater

THEATER = {
    "id":          "capitolio",
    "name":        "Capitólio",
    "short":       "Capitólio",
    "color":       "#1A3A5C",
    "city":        "Lisboa",
    "site":        "https://teatrovariedades-capitolio.pt",
    "programacao": "https://teatrovariedades-capitolio.pt/agenda/capitolio/",
    "address":     "Parque Mayer, Av. da Liberdade, 1250-096 Lisboa",
    "lat":         38.7213,
    "lng":         -9.1492,
    "logo_url":    "https://teatrovariedades-capitolio.pt/wp-content/themes/variedades/assets/images/logo-capitolio.svg",
    "favicon_url": "https://teatrovariedades-capitolio.pt/favicon.ico",
    "facade_url":  "https://teatrovariedades-capitolio.pt/wp-content/uploads/facade-capitolio.jpg",
    "description": "O Capitólio é um espaço cultural histórico de Lisboa, situado no Parque Mayer, dedicado a espectáculos de teatro, música e artes performativas.",
}

AGENDA_URL  = "https://teatrovariedades-capitolio.pt/agenda/capitolio/?layout=grid&espaco=capitolio"
SOURCE_SLUG = "capitolio"


def scrape() -> list[dict]:
    return scrape_theater(
        theater_name=THEATER["name"],
        source_slug=SOURCE_SLUG,
        agenda_url=AGENDA_URL,
    )
