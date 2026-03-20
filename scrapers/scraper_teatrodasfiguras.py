"""
Scraper: Teatro das Figuras
Fonte: https://teatrodasfiguras.pt/agenda
Cidade: Faro

Estrutura do site (Next.js — HTML parcialmente renderizado no servidor):
  - A listagem /agenda devolve HTML com os cards dos eventos.
  - As imagens dos cards NÃO estão no HTML estático (carregadas por JS);
    são sempre obtidas via og:image na página individual.
  - Cada secção mensal tem spans com mês e ano.
  - Cada card tem:
      a.events-card__link-wrapper          → URL do evento (/agenda/slug)
      .events-card__body__date__start      → dia + mês (ex: "20" ou "05 mar")
      .events-card__body__date__end        → dia de fim + mês (ex: "22 ago"), se existir
      .events-card__body__date__schedule   → horário (ex: "21h30")
      .events-card__body__categories__main-category → categoria raw
      .events-card__body__title__text      → título
      .events-card__body__author__text     → autor / subtítulo
  - Sem filtro de categoria na listagem: importa-se tudo;
    normalize_category() (schema.py) decide a categoria canónica.
  - Página individual: og:image, sinopse, bilhetes, preço, duração, idade, ficha técnica.
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
from scrapers.schema import normalize_category

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
    "programacao": "https://teatrodasfiguras.pt/agenda",
    "lat":         37.0179,
    "lng":         -9.1336,
    "salas":       ["Grande Auditório", "Pequeno Auditório"],
    "aliases":     ["teatro das figuras", "tdf", "teatro municipal de faro"],
    "description": (
        "O Teatro das Figuras é o principal equipamento cultural da cidade de Faro, "
        "com programação de teatro, dança, música e cinema."
    ),
    # Activos visuais do teatro — obtidos uma vez, fixos
    "logo_url":    "https://teatrodasfiguras.pt/assets/png/default.png",
    "favicon_url": "https://teatrodasfiguras.pt/assets/favicon/apple-touch-icon.png",
    "facade_url":  "https://teatrodasfiguras.pt/assets/png/facade.png",  # actualizar se necessário
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://teatrodasfiguras.pt"
AGENDA_URL   = f"{BASE}/agenda"

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

    soup      = BeautifulSoup(r.text, "lxml")
    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for month_section in soup.select(".events-grid-content__month-section"):
        month_num, year_num = _parse_month_section_header(month_section)

        for card in month_section.select("div.events-card"):
            try:
                ev = _parse_card(card, month_num, year_num)
                if not ev:
                    continue
                if ev["id"] in seen_ids:
                    continue
                seen_ids.add(ev["id"])

                # Sempre vai à página individual:
                # imagem (og:image), sinopse, bilhetes, preço, duração, idade, ficha
                if ev["source_url"] and ev["source_url"] != BASE:
                    _enrich_from_event_page(ev)
                    time.sleep(0.4)

                events.append(ev)
            except Exception as exc:
                log(f"[{THEATER_NAME}] Erro a processar card: {exc}")

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
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
    Importa todas as categorias — normalize_category() decide depois.
    Devolve None se faltar título ou data.
    """
    # Título
    title_el = card.select_one(".events-card__body__title__text")
    title    = title_el.get_text(strip=True) if title_el else ""
    if not title or len(title) < 2:
        return None

    # URL
    url_el = card.select_one("a.events-card__link-wrapper")
    href   = url_el["href"] if url_el and url_el.get("href") else ""
    url    = href if href.startswith("http") else urljoin(BASE, href)

    # Categoria raw — será normalizada pelo harmonizer via normalize_category()
    cat_el      = card.select_one(".events-card__body__categories__main-category")
    category_raw = cat_el.get_text(strip=True) if cat_el else ""

    # Autor / subtítulo
    author_el = card.select_one(".events-card__body__author__text")
    subtitle  = author_el.get_text(strip=True) if author_el else ""

    # ── Datas ──────────────────────────────────────────────────
    # O card pode ter dois padrões:
    #   A) apenas início:  <div class="...date__start">19 mar</div>
    #   B) intervalo:      <div class="...date__start">20</div>
    #                      <div class="...date__separator">-</div>
    #                      <div class="...date__end">22 ago</div>
    start_el  = card.select_one(".events-card__body__date__start")
    end_el    = card.select_one(".events-card__body__date__end")

    start_text = start_el.get_text(strip=True) if start_el else ""
    end_text   = end_el.get_text(strip=True)   if end_el   else ""

    date_start, dates_label, card_month, card_year = _parse_start_date(
        start_text, section_month, section_year
    )
    if not date_start:
        return None

    # Se há data de fim no card, extrair
    date_end = date_start  # default: mesmo dia
    if end_text:
        date_end_candidate = _parse_end_date(end_text, card_month, card_year, date_start)
        if date_end_candidate:
            date_end = date_end_candidate
            # Actualizar label para mostrar intervalo
            end_label = _format_date_label(date_end)
            dates_label = f"{dates_label} – {end_label}"

    # Horário
    sched_el = card.select_one(".events-card__body__date__schedule")
    schedule = sched_el.get_text(strip=True).strip() if sched_el else ""

    # Imagem: NÃO está no HTML estático (Next.js carrega por JS).
    # Será obtida na página individual via og:image.

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "subtitle":        subtitle,
        "theater":         THEATER_NAME,
        "category":        normalize_category(category_raw),
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        build_sessions(date_start, date_end, schedule),
        "schedule":        schedule,
        "synopsis":        "",
        "image":           None,   # preenchido em _enrich_from_event_page
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
    Visita a página individual e preenche:
      - image        (og:image — a única fonte fiável)
      - ticket_url
      - synopsis
      - price_info
      - duration / duration_min
      - age_rating / age_min
      - date_end     (se houver intervalo na página)
      - technical_sheet
    Modifica ev in-place.
    """
    try:
        r = requests.get(ev["source_url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {ev['source_url']}: {e}")
        return

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # ── Imagem via og:image ────────────────────────────────────
    og_img = soup.find("meta", property="og:image")
    if og_img:
        img_url = og_img.get("content", "").strip()
        # Ignorar a imagem genérica do site (default.png)
        if img_url and "default.png" not in img_url:
            ev["image"] = build_image_object(img_url, soup, THEATER_NAME, ev["source_url"])

    # Fallback: primeiro <img> relevante no main (se og:image falhou)
    if not ev["image"]:
        main_el = soup.find("main") or soup
        for img in main_el.find_all("img", src=True):
            src = img.get("src", "")
            if src.startswith("http") and "default" not in src and "logo" not in src:
                ev["image"] = build_image_object(src, img, THEATER_NAME, ev["source_url"])
                break

    # ── Bilhetes ───────────────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(x in href for x in ["ticketline", "bol.pt", "bilhete", "comprar"]):
            ev["ticket_url"] = href if href.startswith("http") else urljoin(BASE, href)
            break

    # ── Sinopse ────────────────────────────────────────────────
    synopsis = ""
    # Tentar og:description primeiro (geralmente bom resumo)
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()

    # Se for genérico ("Eventos de Teatro das Figuras"), descartar e ir ao main
    if not synopsis or "Eventos de Teatro" in synopsis:
        synopsis = ""
        main_el = soup.find("main") or soup
        for p in main_el.find_all("p"):
            t = p.get_text(" ", strip=True)
            if len(t) > 80 and not re.match(
                r"^(Texto|Encena|Interpreta|Tradu|Cenografia|Figurinos|"
                r"Música|Luz|Som|Produ|Coprodu|Dire[çc]|©|Apoio)",
                t, re.IGNORECASE,
            ):
                synopsis += (" " if synopsis else "") + t
                if len(synopsis) > 1500:
                    break

    ev["synopsis"] = truncate_synopsis(synopsis)

    # ── Preço ──────────────────────────────────────────────────
    pm = re.search(
        r"(Entrada\s+(?:livre|gratuita)"
        r"|gratuito"
        r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if pm:
        ev["price_info"] = pm.group(1).strip()

    # ── Duração ────────────────────────────────────────────────
    dm = re.search(r"(\d+)\s*min", full_text, re.IGNORECASE)
    if dm:
        mins = int(dm.group(1))
        ev["duration"]     = f"{mins} min."
        ev["duration_min"] = mins

    # ── Classificação etária ───────────────────────────────────
    am = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if am:
        age = int(am.group(1))
        ev["age_rating"] = f"M/{age}"
        ev["age_min"]    = age

    # ── date_end vinda da página (se não veio do card) ─────────
    if ev["date_start"] == ev["date_end"]:
        date_end = _extract_date_end_from_text(full_text, ev["date_start"])
        if date_end:
            ev["date_end"]    = date_end
            ev["dates_label"] = (
                f"{ev['dates_label']} – {_format_date_label(date_end)}"
                if ev["dates_label"] else date_end
            )
            ev["sessions"] = build_sessions(
                ev["date_start"], date_end, ev.get("schedule", "")
            )

    # ── Ficha técnica ──────────────────────────────────────────
    ev["technical_sheet"] = _parse_ficha(full_text)


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_start_date(
    date_text: str,
    section_month: int,
    section_year: int,
) -> tuple[str, str, int, int]:
    """
    Converte o texto de data de início do card para YYYY-MM-DD.
    O card mostra "19 mar", "05", "31 mai", etc.
    O mês/ano de contexto vêm da secção mensal.
    Devolve (date_iso, dates_label, month_used, year_used).
    """
    if not date_text:
        return "", "", section_month, section_year

    day_m = re.match(r"(\d{1,2})", date_text.strip())
    if not day_m:
        return "", "", section_month, section_year
    day = int(day_m.group(1))

    # O card pode conter o mês abreviado (ex: "19 mar") — tem precedência
    month_in_text = re.search(r"[a-záéíóúãç]{3,}", date_text, re.IGNORECASE)
    if month_in_text:
        m = _PT_MONTHS.get(month_in_text.group().lower()[:3])
        if m:
            section_month = m

    if not section_month or not section_year or not day:
        return "", "", section_month, section_year

    date_iso = f"{section_year}-{section_month:02d}-{day:02d}"
    label    = f"{day} {_month_name(section_month)} {section_year}"
    return date_iso, label, section_month, section_year


def _parse_end_date(
    end_text: str,
    ref_month: int,
    ref_year: int,
    date_start: str,
) -> str:
    """
    Converte o texto de data de fim do card para YYYY-MM-DD.
    end_text exemplos: "22 ago", "15", "7 jun"
    """
    if not end_text:
        return ""

    day_m = re.match(r"(\d{1,2})", end_text.strip())
    if not day_m:
        return ""
    day = int(day_m.group(1))

    month_in_text = re.search(r"[a-záéíóúãç]{3,}", end_text, re.IGNORECASE)
    if month_in_text:
        m = _PT_MONTHS.get(month_in_text.group().lower()[:3])
        if m:
            ref_month = m

    if not ref_month or not ref_year:
        return ""

    candidate = f"{ref_year}-{ref_month:02d}-{day:02d}"
    return candidate if candidate >= date_start else ""


def _extract_date_end_from_text(text: str, date_start: str) -> str:
    """
    Tenta extrair a data de fim a partir do texto da página individual.
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


def _format_date_label(date_iso: str) -> str:
    """'2026-08-22' → '22 Ago 2026'"""
    try:
        y, mo, d = date_iso.split("-")
        return f"{int(d)} {_month_name(int(mo))} {y}"
    except Exception:
        return date_iso


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
