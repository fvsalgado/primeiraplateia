#!/usr/bin/env python3
"""
scripts/new_scraper.py
Primeira Plateia — Gerador de scaffolding para novos scrapers.

Gera um ficheiro scrapers/scraper_<id>.py com toda a estrutura obrigatória
preenchida interactivamente. O sync_scrapers.py trata do registo automático.

Uso:
    python scripts/new_scraper.py
    python scripts/new_scraper.py --id meu_teatro --name "Meu Teatro" --url https://meutreatro.pt
"""

import argparse
import re
import sys
from pathlib import Path

ROOT        = Path(__file__).parent.parent
SCRAPERS_DIR = ROOT / "scrapers"

TEMPLATE = '''\
"""
scrapers/scraper_{id}.py
Primeira Plateia — Scraper para {name}.

Site: {url}
Adicionado: {date_added}
"""

import logging
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    HEADERS,
    parse_date,
    parse_date_range,
    truncate_synopsis,
    build_sessions,
    build_image_object,
    fetch_with_retry,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Identificação do teatro — lido automaticamente pelo sync_scrapers.py
# ─────────────────────────────────────────────────────────────
THEATER = {{
    "id":      "{id}",
    "name":    "{name}",
    "short":   "{short}",
    "url":     "{url}",
    "city":    "{city}",
    "address": "{address}",
    "lat":     {lat},
    "lng":     {lng},
    "type":    "teatro",
    "aliases": [],
}}

BASE_URL  = "{url}"
LIST_URL  = "{url}"   # TODO: ajustar para a página de programação


def _parse_event(item, session: requests.Session) -> dict | None:
    """
    Extrai dados de um evento a partir de um elemento BeautifulSoup.
    Devolve dict de evento ou None se não conseguir extrair dados mínimos.
    """
    try:
        # TODO: ajustar seletores CSS/XPath ao HTML do site
        title = item.select_one(".event-title")
        if not title:
            return None
        title_text = title.get_text(strip=True)
        if not title_text:
            return None

        # URL de detalhe
        link = item.select_one("a[href]")
        source_url = link["href"] if link else BASE_URL
        if source_url and not source_url.startswith("http"):
            source_url = BASE_URL.rstrip("/") + "/" + source_url.lstrip("/")

        # Datas — ajustar ao formato do site
        date_text  = (item.select_one(".event-date") or item).get_text(strip=True)
        date_start, date_end = parse_date_range(date_text)

        if not date_start:
            logger.debug(f"[{THEATER['name']}] sem data para: {{title_text!r}}")
            return None

        # Imagem
        img_tag = item.select_one("img")
        img_url = ""
        if img_tag:
            img_url = img_tag.get("src") or img_tag.get("data-src") or ""
        image = build_image_object(img_url, None, THEATER["name"], source_url)

        # Sinopse
        synopsis_tag = item.select_one(".event-synopsis, .description, .intro")
        synopsis = truncate_synopsis(synopsis_tag.get_text(strip=True)) if synopsis_tag else ""

        # Preço
        price_tag = item.select_one(".price, .ticket-price")
        price_info = price_tag.get_text(strip=True) if price_tag else ""

        return {{
            "title":      title_text,
            "theater":    THEATER["name"],
            "date_start": date_start,
            "date_end":   date_end,
            "synopsis":   synopsis,
            "image":      image,
            "source_url": source_url,
            "ticket_url": source_url,
            "price_info": price_info,
            "category":   "",     # TODO: extrair categoria
            "sessions":   build_sessions(date_start, date_end),
        }}

    except Exception as exc:
        logger.warning(f"[{{THEATER['name']}}] erro ao analisar evento: {{exc}}")
        return None


def scrape() -> list[dict]:
    """
    Ponto de entrada principal. Devolve lista de eventos do teatro.
    Chamado pelo orquestrador (scraper.py).
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    events = []

    try:
        resp = fetch_with_retry(session, LIST_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # TODO: ajustar seletor para a grelha de eventos do site
        items = soup.select(".event-item, .programacao-item, article.event")
        logger.info(f"[{{THEATER['name']}}] {{len(items)}} candidatos na listagem")

        for item in items:
            ev = _parse_event(item, session)
            if ev:
                events.append(ev)

    except Exception as exc:
        logger.error(f"[{{THEATER['name']}}] erro na listagem: {{exc}}")

    logger.info(f"[{{THEATER['name']}}] {{len(events)}} eventos recolhidos")
    return events
'''


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower().strip()).strip("_")


def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    try:
        val = input(f"  {prompt}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    return val or default


def main() -> None:
    parser = argparse.ArgumentParser(description="Primeira Plateia — Criar novo scraper")
    parser.add_argument("--id",   help="ID do teatro (ex: meu_teatro)")
    parser.add_argument("--name", help="Nome completo do teatro")
    parser.add_argument("--url",  help="URL do site do teatro")
    args = parser.parse_args()

    from datetime import date

    print("\nPrimeira Plateia — Novo Scraper")
    print("─" * 40)
    print("Preenche os campos abaixo. Prima Enter para usar o valor sugerido.\n")

    name    = args.name or ask("Nome completo do teatro", "Teatro XYZ")
    id_     = args.id   or ask("ID (slug sem espaços)",   slugify(name))
    short   = ask("Nome curto (para logs)",               name.split(" ")[0])
    url     = args.url  or ask("URL do site",             f"https://www.{slugify(name)}.pt")
    city    = ask("Cidade",                               "Lisboa")
    address = ask("Morada",                               "")
    lat     = ask("Latitude",                             "38.7169")
    lng     = ask("Longitude",                            "-9.1395")

    out_path = SCRAPERS_DIR / f"scraper_{id_}.py"

    if out_path.exists():
        print(f"\n⚠  Ficheiro já existe: {out_path}")
        confirm = ask("Sobrescrever? (s/N)", "N")
        if confirm.lower() != "s":
            print("Cancelado.")
            return

    content = TEMPLATE.format(
        id=id_,
        name=name,
        short=short,
        url=url,
        city=city,
        address=address,
        lat=lat or "0.0",
        lng=lng or "0.0",
        date_added=date.today().isoformat(),
    )

    out_path.write_text(content, encoding="utf-8")
    print(f"\n✓ Criado: {out_path}")
    print(f"\nPróximos passos:")
    print(f"  1. Edita {out_path} e ajusta os selectores CSS ao HTML do site")
    print(f"  2. Testa localmente: python scraper.py --scraper scraper_{id_}")
    print(f"  3. O sync_scrapers.py regista-o automaticamente no próximo CI run")
    print(f"  4. Verifica: python scripts/test_scrapers.py --scraper scraper_{id_}\n")


if __name__ == "__main__":
    main()
