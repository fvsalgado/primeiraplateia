"""
Scraper: Capitólio
Fonte: https://teatrovariedades-capitolio.pt/agenda/capitolio/

Toda a lógica de scraping está em scrapers/base_variedades.py,
partilhada com o Teatro Variedades.
"""
from scrapers.base_variedades import scrape_theater

AGENDA_URL   = "https://teatrovariedades-capitolio.pt/agenda/capitolio/?categoria=teatro&layout=grid&espaco=capitolio"
THEATER_NAME = "Capitólio"
SOURCE_SLUG  = "teatro-capitolio"


def scrape() -> list[dict]:
    return scrape_theater(
        theater_name=THEATER_NAME,
        source_slug=SOURCE_SLUG,
        agenda_url=AGENDA_URL,
    )
