"""
scrapers/base_variedades.py
Primeira Plateia — Lógica partilhada entre Capitólio e Teatro Variedades.

Ambos os teatros partilham o mesmo site (teatrovariedades-capitolio.pt),
a mesma estrutura HTML e a mesma lógica de parsing.
A única diferença é o THEATER_NAME, SOURCE_SLUG e AGENDA_URL,
que são passados como parâmetros para a função scrape_theater().

Uso:
    from scrapers.base_variedades import scrape_theater
    def scrape():
        return scrape_theater(
            theater_name="Capitólio",
            source_slug="teatro-capitolio",
            agenda_url="https://...",
        )
"""

import re
import time
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    make_id,
    parse_date_range,
    parse_date,
    log,
    HEADERS,
    can_scrape,
    build_image_object,
    build_sessions,
    truncate_synopsis,
)

BASE = "https://teatrovariedades-capitolio.pt"

# Categorias aceites — filtro na listagem, antes de qualquer pedido HTTP
THEATRE_CATEGORIES = {"teatro", "circo", "performance", "dança"}

# CSS vars usadas como background nas tags de espaço (p.copy-xs-bold)
THEATER_STYLE_MAP = {
    "var(--color-teatro-variedades)": "Teatro Variedades",
    "var(--color-capitolio)":         "Capitólio",
}


def scrape_theater(
    theater_name: str,
    source_slug: str,
    agenda_url: str,
) -> list[dict]:
    """
    Ponto de entrada partilhado.
    Recolhe eventos do teatro indicado via agenda_url.
    """
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    urls, stubs = _collect(agenda_url, theater_name, source_slug)

    events: list[dict] = []
    seen_ids: set[str] = set()

    # Eventos com página própria — scraping completo
    for url in sorted(urls):
        ev = _scrape_event(url, theater_name, source_slug)
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.3)

    # Stubs da listagem (sem página própria) — dados parciais
    for title, stub in stubs.items():
        eid = make_id(source_slug, title)
        if eid not in seen_ids:
            seen_ids.add(eid)
            events.append(stub)

    log(f"[{theater_name}] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de URLs e stubs da listagem
# ─────────────────────────────────────────────────────────────

def _collect(
    agenda_url: str,
    theater_name: str,
    source_slug: str,
) -> tuple[set[str], dict[str, dict]]:
    """
    Percorre a listagem e separa:
    - urls: eventos com página própria (/evento/slug/) que passam o filtro
    - stubs: eventos sem página própria que passam o filtro

    Estrutura HTML da listagem:
      Cada evento está num container  div.flex-col.gap-y-5.justify-center.items-center
      com dois tipos de p.copy-xs-bold:
        1. Tag de espaço  — background CSS var identifica o teatro
           ex: style="background:var(--color-teatro-variedades)"  → "Teatro Variedades"
               style="background:var(--color-capitolio)"          → "Capitólio"
        2. Tag de categoria — background:var(--color-gray)
           ex: "Teatro", "Música", "Performance", ...

      O filtro duplo (teatro + categoria) é feito aqui, antes de qualquer pedido HTTP.
      Eventos partilhados pelos dois espaços (dois tags de teatro) só são aceites
      se o theater_name do scraper constar na lista de teatros do evento.
    """
    try:
        r = requests.get(agenda_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro na listagem: {e}")
        return set(), {}

    soup  = BeautifulSoup(r.text, "lxml")
    urls  = set()
    stubs = {}

    for container in soup.select(
        "div.flex.flex-col.gap-y-5.justify-center.items-center"
    ):
        tags = container.select("p.copy-xs-bold")
        if not tags:
            continue

        # Separar tags de teatro das tags de categoria
        theaters: list[str] = []
        category: str = ""
        for tag in tags:
            style  = tag.get("style", "")
            text   = tag.get_text(strip=True)
            mapped = next(
                (v for k, v in THEATER_STYLE_MAP.items() if k in style), None
            )
            if mapped:
                theaters.append(mapped)
            else:
                category = text  # última tag sem cor de teatro = categoria

        # Filtro 1: este evento pertence ao teatro deste scraper?
        if theaters and theater_name not in theaters:
            continue

        # Filtro 2: categoria aceite?
        if category and category.lower() not in THEATRE_CATEGORIES:
            log(f"[{theater_name}] Ignorado (categoria '{category}'): "
                + container.select_one("a[href]", href=True)["href"] if container.select_one("a[href]") else "")
            continue

        # Link para página própria do evento
        ev_a = container.select_one("a[href*='/evento/']")
        if ev_a:
            href = ev_a["href"]
            full = href if href.startswith("http") else BASE + href
            urls.add(full)
            continue

        # Sem página própria — stub a partir da listagem
        title_a = container.select_one("a")
        if not title_a:
            continue
        title = title_a.get_text(strip=True)
        if not title:
            continue

        card_text = container.get_text(" ")
        dates_label, date_start, date_end = _parse_dates(card_text)
        if not date_start:
            continue

        stubs[title] = {
            "id":              make_id(source_slug, title),
            "title":           title,
            "theater":         theater_name,
            "category":        category or "Teatro",
            "dates_label":     dates_label,
            "date_start":      date_start,
            "date_end":        date_end,
            "sessions":        build_sessions(date_start, date_end, ""),
            "schedule":        "",
            "synopsis":        "",
            "image":           None,
            "source_url":      agenda_url,
            "ticket_url":      "",
            "price_info":      "",
            "duration":        "",
            "age_rating":      "",
            "accessibility":   "",
            "technical_sheet": {},
        }

    return urls, stubs


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(
    url: str,
    theater_name: str,
    source_slug: str,
) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ")

    # Título
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Datas
    dates_label, date_start, date_end = _parse_dates(text)

    # Imagem
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, theater_name, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(x in href for x in ["bol.pt", "ticketline", "eventbrite"]):
            ticket_url = href
            break

    # Sinopse
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()
    if not synopsis:
        for p in soup.select("main p, article p, .entry-content p"):
            t = p.get_text(strip=True)
            if len(t) > 60:
                synopsis = t
                break

    # Preço
    price_info = ""
    pm = re.search(
        r"(Entrada\s+livre"
        r"|\d+(?:[,\.]\d+)?\s*€\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?\s*€)",
        text, re.IGNORECASE,
    )
    if pm:
        price_info = pm.group(1).strip()

    # Duração
    duration = ""
    dm = re.search(r"(\d+\s*min\.?|\d+h\d*)", text, re.IGNORECASE)
    if dm:
        duration = dm.group(1).strip()

    # Classificação etária
    age_rating = ""
    am = re.search(r"\b(M\s*/\s*\d+|Livre|\+\d+)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")

    return {
        "id":              make_id(source_slug, title),
        "title":           title,
        "theater":         theater_name,
        "category":        "Teatro",
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        build_sessions(date_start, date_end, ""),
        "schedule":        "",
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "accessibility":   "",
        "technical_sheet": {},
    }


# ─────────────────────────────────────────────────────────────
# Parse de datas (partilhado entre listagem e evento)
# ─────────────────────────────────────────────────────────────

def _parse_dates(text: str) -> tuple[str, str, str]:
    """
    Tenta extrair datas de texto livre.
    Formatos suportados:
      - DD.MM[.YYYY] – DD.MM.YYYY  (intervalo)
      - DD.MM.YYYY                 (data única)
    Devolve (dates_label, date_start, date_end).
    """
    # Intervalo: DD.MM[.YYYY] – DD.MM.YYYY
    m = re.search(
        r"(\d{2}\.\d{2}(?:\.\d{4})?)\s*[–—\-]\s*(\d{2}\.\d{2}\.\d{4})",
        text,
    )
    if m:
        dates_label = f"{m.group(1)} – {m.group(2)}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Data única: DD.MM.YYYY
    m2 = re.search(r"(\d{2}\.\d{2}\.\d{4})", text)
    if m2:
        dates_label = m2.group(1)
        date_start = date_end = parse_date(dates_label)
        return dates_label, date_start, date_end

    return "", "", ""
