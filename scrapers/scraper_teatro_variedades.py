"""
Scraper: Teatro Variedades
Fonte: https://teatrovariedades-capitolio.pt/agenda/teatro-variedades/

Toda a lógica de scraping está em scrapers/base_variedades.py,
partilhada com o Capitólio.
"""
from scrapers.base_variedades import scrape_theater

AGENDA_URL   = "https://teatrovariedades-capitolio.pt/agenda/teatro-variedades/?categoria=teatro&layout=grid&espaco=variedades"
THEATER_NAME = "Teatro Variedades"
SOURCE_SLUG  = "teatro-variedades"


def scrape() -> list[dict]:
    return scrape_theater(
        theater_name=THEATER_NAME,
        source_slug=SOURCE_SLUG,
        agenda_url=AGENDA_URL,
    )
