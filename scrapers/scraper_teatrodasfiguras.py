"""
Scraper: Teatro das Figuras
Fonte: https://teatrodasfiguras.pt/agenda?categories=3,15
Cidade: Faro

Estrutura do site (HTML estático após JS render):
  - Listagem em /agenda?categories=3,15 (categories=3 Teatro, 15 Teatro Educativo)
  - Os eventos estão em div.events-card dentro de secções mensais
    div.events-grid-content__month-section.
  - Cada secção mensal tem spans com mês e ano — fonte do contexto de data.
  - Cada card tem:
      a.events-card__link-wrapper      → URL do evento (/agenda/slug)
      img.img--generic                 → imagem (src absoluto)
      .events-card__body__date__start  → dia e (opcionalmente) mês abreviado
      .events-card__body__date__schedule → horário (ex: "21h30")
      .events-card__body__categories__main-category → categoria
      .events-card__body__title__text  → título
      .events-card__body__author__text → autor / subtítulo
  - Categorias aceites: Teatro, Performance, Dança, Circo, Musical, Comédia
  - Categorias rejeitadas na listagem: Música, Cinema, Multidisciplinar, etc.
  - URL de bilhetes: extraída da página individual do evento.
  - A listagem tem dados suficientes para a maioria dos campos.
    Visita a página individual apenas para bilhetes, sinopse e ficha técnica.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)

# ─────────────────────────────────────────────────────────────
# Metadados do teatro
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "teatrodasfiguras",
    "name":        "Teatro das Figuras",
    "short":       "T. das Figuras",
    "color":       "#c8102e",
    "city":        "Faro",
    "address":     "Rua Pedro Franque, 8000-281 Faro",
    "site":        "https://teatrodasfiguras.pt",
    "programacao": "https://teatrodasfiguras.pt/agenda?categories=3,15",
    "lat":         37.0179,
    "lng":         -7.9307,
    "salas":       ["Grande Auditório", "Pequeno Auditório"],
    "aliases":     ["teatro das figuras", "tdf", "teatro municipal de faro"],
    "description": (
        "O Teatro das Figuras é o principal equipamento cultural da cidade de Faro, "
        "com programação de teatro, dança, música e cinema."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://teatrodasfiguras.pt"
AGENDA_URL   = f"{BASE}/agenda?categories=3,15"

# Categorias aceites — filtradas na listagem antes de visitar páginas individuais
THEATRE_CATEGORIES = {"teatro", "performance", "dança", "circo", "musical", "comédia"}

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    try:
        r = requests.get(AGENDA_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup     = BeautifulSoup(r.text, "lxml")
    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for month_section in soup.select(".events-grid-content__month-section"):
        month_num, year_num = _parse_month_section_header(month_section)

        for card in month_section.select("div.events-card"):
            ev = _parse_card(card, month_num, year_num)
            if not ev:
                continue
            if ev["id"] in seen_ids:
                continue
            seen_ids.add(ev["id"])

            # Visitar página individual para enriquecer com bilhetes e sinopse
            if ev["source_url"] and ev["source_url"] != BASE:
                _enrich_from_event_page(ev)
                time.sleep(0.3)

            events.append(ev)

    log(f"[{THEATER_NAME}] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Parse do cabeçalho da secção mensal
# ─────────────────────────────────────────────────────────────

def _parse_month_section_header(section) -> tuple[int, int]:
    """
    Extrai mês e ano do cabeçalho da secção mensal.
    Estrutura:
        <span class="...month-title__month">março</span>
        <span class="...month-title__year">2026</span>
    """
    month_el = section.select_one(
        ".events-grid-content__month-section__month-title__month"
    )
    year_el = section.select_one(
        ".events-grid-content__month-section__month-title__year"
    )
    month_str = month_el.get_text(strip=True).lower() if month_el else ""
    year_str  = year_el.get_text(strip=True)          if year_el  else ""

    month_num = _PT_MONTHS.get(month_str, 0)
    year_num  = int(year_str) if year_str.isdigit() else 0
    return month_num, year_num


# ─────────────────────────────────────────────────────────────
# Parse de card de evento
# ─────────────────────────────────────────────────────────────

def _parse_card(card, section_month: int, section_year: int) -> dict | None:
    """
    Extrai evento a partir de um div.events-card.
    Devolve None se não for categoria aceite ou faltar data.
    """
    # Categoria — filtrar antes de processar
    cat_el   = card.select_one(".events-card__body__categories__main-category")
    category = cat_el.get_text(strip=True) if cat_el else ""
    if category.lower() not in THEATRE_CATEGORIES:
        return None

    # URL
    url_el = card.select_one("a.events-card__link-wrapper")
    href   = url_el["href"] if url_el else ""
    url    = href if href.startswith("http") else urljoin(BASE, href)

    # Título
    title_el = card.select_one(".events-card__body__title__text")
    title    = title_el.get_text(strip=True) if title_el else ""
    if not title or len(title) < 3:
        return None

    # Autor / subtítulo
    author_el = card.select_one(".events-card__body__author__text")
    subtitle  = author_el.get_text(strip=True) if author_el else ""

    # Data: o card mostra apenas o dia de início (e às vezes o mês).
    # O mês e o ano vêm do contexto da secção mensal.
    date_el   = card.select_one(".events-card__body__date__start")
    date_text = date_el.get_text(strip=True) if date_el else ""
    date_start, dates_label = _parse_card_date(date_text, section_month, section_year)
    if not date_start:
        return None

    # Horário
    sched_el = card.select_one(".events-card__body__date__schedule")
    schedule = sched_el.get_text(strip=True) if sched_el else ""
    # Limpar horários complexos (ex: "Dias 5 e 6 às 21h30 | Dia...") — guardar raw
    # O campo schedule fica para uso directo na UI

    # Imagem
    image    = None
    img_tag  = card.select_one("img.img--generic")
    if img_tag:
        src = img_tag.get("src", "")
        if src and src.startswith("http"):
            image = build_image_object(src, card, THEATER_NAME, url)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "subtitle":        subtitle,
        "theater":         THEATER_NAME,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_start,   # só temos data de início no card; enriquecido depois
        "sessions":        build_sessions(date_start, date_start, schedule),
        "schedule":        schedule,
        "synopsis":        "",
        "image":           image,
        "source_url":      url,
        "ticket_url":      "",
        "price_info":      "",
        "duration":        "",
        "age_rating":      "",
        "technical_sheet": {},
    }


# ─────────────────────────────────────────────────────────────
# Enriquecimento a partir da página individual
# ─────────────────────────────────────────────────────────────

def _enrich_from_event_page(ev: dict) -> None:
    """
    Visita a página individual do evento e preenche campos em falta:
    ticket_url, synopsis, price_info, duration, age_rating, date_end,
    e ficha técnica.
    Modifica ev in-place.
    """
    try:
        r = requests.get(ev["source_url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {ev['source_url']}: {e}")
        return

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ")

    # Bilhetes
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(x in href for x in ["ticketline", "bol.pt", "bilhete", "comprar"]):
            ev["ticket_url"] = href if href.startswith("http") else urljoin(BASE, href)
            break

    # Sinopse — parágrafos longos do main
    synopsis = ""
    main_el  = soup.find("main") or soup
    for p in main_el.find_all("p"):
        t = p.get_text(" ", strip=True)
        if len(t) > 80 and not re.match(
            r"^(Texto|Encena|Interpreta|Tradu|Cenografia|Figurinos|"
            r"Música|Luz|Som|Produ|Coprodu|Dire[çc])",
            t, re.IGNORECASE,
        ):
            synopsis += (" " if synopsis else "") + t
            if len(synopsis) > 1500:
                break
    if not synopsis:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            synopsis = og_desc.get("content", "").strip()
    ev["synopsis"] = truncate_synopsis(synopsis)

    # Preço
    pm = re.search(
        r"(Entrada\s+(?:livre|gratuita)"
        r"|gratuito"
        r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if pm:
        ev["price_info"] = pm.group(1).strip()

    # Duração
    dm = re.search(r"(\d+)\s*min", full_text, re.IGNORECASE)
    if dm:
        ev["duration"] = f"{dm.group(1)} min."

    # Classificação etária
    am = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if am:
        ev["age_rating"] = f"M/{am.group(1)}"

    # date_end — tentar extrair da página se houver intervalo
    date_end = _extract_date_end(soup, full_text, ev["date_start"])
    if date_end:
        ev["date_end"]    = date_end
        ev["dates_label"] = f"{ev['dates_label']} – {date_end}" if date_end != ev["date_start"] else ev["dates_label"]
        ev["sessions"]    = build_sessions(ev["date_start"], date_end, ev.get("schedule", ""))

    # Ficha técnica
    ev["technical_sheet"] = _parse_ficha(full_text)


# ─────────────────────────────────────────────────────────────
# Parse de datas
# ─────────────────────────────────────────────────────────────

def _parse_card_date(
    date_text: str,
    section_month: int,
    section_year: int,
) -> tuple[str, str]:
    """
    Converte o texto de data do card para YYYY-MM-DD.
    O card mostra "19 mar", "05", "31 mai", etc.
    O mês/ano de contexto vêm da secção mensal.
    Devolve (date_start, dates_label).
    """
    if not date_text:
        return "", ""

    day_m = re.match(r"(\d{1,2})", date_text.strip())
    if not day_m:
        return "", ""
    day = int(day_m.group(1))

    # O card pode conter o mês abreviado (ex: "19 mar") — tem precedência
    month_in_text = re.search(r"[a-záéíóúãç]{3,}", date_text, re.IGNORECASE)
    if month_in_text:
        m = _PT_MONTHS.get(month_in_text.group().lower())
        if m:
            section_month = m

    if not section_month or not section_year or not day:
        return "", ""

    date_iso   = f"{section_year}-{section_month:02d}-{day:02d}"
    month_name = _month_name(section_month)
    label      = f"{day} {month_name} {section_year}"
    return date_iso, label


def _extract_date_end(soup, text: str, date_start: str) -> str:
    """
    Tenta extrair a data de fim a partir da página individual.
    Procura padrões como "19 mar – 15 mai 2026" ou "até 15 de maio".
    """
    m = re.search(
        r"\d{1,2}\s+[a-záéíóúãç]+\s*[–—-]\s*(\d{1,2})\s+([a-záéíóúãç]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d2, mon_s, yr = m.group(1), m.group(2), m.group(3)
        n = _PT_MONTHS.get(mon_s.lower()[:3]) or _PT_MONTHS.get(mon_s.lower())
        if n:
            y = int(yr) if yr else int(date_start[:4]) if date_start else 2026
            candidate = f"{y}-{n:02d}-{int(d2):02d}"
            if candidate >= date_start:
                return candidate
    return ""


def _month_name(n: int) -> str:
    names = ["", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
             "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    return names[n] if 1 <= n <= 12 else ""


# ─────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(text: str) -> dict:
    ficha      = {}
    known_keys = [
        ("texto",         r"[Tt]exto\s*[:\|]\s*"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s*[:\|]\s*"),
        ("dramaturgia",   r"[Dd]ramaturgia\s*[:\|]\s*"),
        ("direção",       r"[Dd]ire[çc][aã]o\s+[Aa]rt[íi]stica\s*[:\|]\s*|[Dd]ire[çc][aã]o\s*[:\|]\s*"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s*[:\|]\s*"),
        ("cenografia",    r"[Cc]enografia(?:\s+e\s+[Ff]igurinos?)?\s*[:\|]\s*"),
        ("figurinos",     r"[Ff]igurinos?\s*[:\|]\s*"),
        ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\|]\s*|[Ii]lumina[çc][aã]o\s*[:\|]\s*"),
        ("som",           r"[Dd]esenho\s+de\s+[Ss]om\s*[:\|]\s*|[Ss]onoplastia\s*[:\|]\s*"),
        ("música",        r"[Mm][úu]sica\s*[:\|]\s*"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s*[:\|]\s*"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
        ("coprodução",    r"[Cc]o-?[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
        ("coreografia",   r"[Cc]oreografia\s*[:\|]\s*"),
    ]
    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\s*(?:Apoios?|Agradecimentos|©)\s*[:\|]", value, flags=re.IGNORECASE)[0]
        value      = value.rstrip(" |").strip()[:200]
        if value and key not in ficha:
            ficha[key] = value

    return ficha
