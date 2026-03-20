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
import logging
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
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

BASE = "https://teatrovariedades-capitolio.pt"

# CSS vars usadas como background nas tags de espaço (p.copy-xs-bold)
THEATER_STYLE_MAP = {
    "var(--color-teatro-variedades)": "Teatro Variedades",
    "var(--color-capitolio)":         "Capitólio",
}

# Labels da ficha técnica (PT → chave normalizada)
TECHNICAL_LABELS: dict[str, str] = {
    "texto":          "texto",
    "encenação":      "encenacao",
    "dramaturgia":    "dramaturgia",
    "direção":        "direcao",
    "direção artística": "direcao_artistica",
    "tradução":       "traducao",
    "cenografia":     "cenografia",
    "figurinos":      "figurinos",
    "luz":            "luz",
    "som":            "som",
    "música":         "musica",
    "interpretação":  "interpretacao",
    "produção":       "producao",
    "coprodução":     "coproducao",
    "coreografia":    "coreografia",
    "dramaturgia":    "dramaturgia",
    "adaptação":      "adaptacao",
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada público
# ─────────────────────────────────────────────────────────────

def scrape_theater(
    theater_name: str,
    source_slug:  str,
    agenda_url:   str,
) -> list[dict]:
    """
    Ponto de entrada partilhado.
    Recolhe todos os eventos do teatro indicado via agenda_url.
    Não filtra por categoria — filtra apenas por venue.
    """
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    card_data, urls_without_card = _collect(agenda_url, theater_name, source_slug)

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    # Eventos com página própria — scraping completo, seed com dados do card
    for url, seed in sorted(card_data.items()):
        try:
            ev = _scrape_event(url, theater_name, source_slug, seed)
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            logger.warning("[%s] Erro inesperado em %s: %s", theater_name, url, e)
        time.sleep(0.3)

    # URLs descobertas fora do contexto de card (fallback raro)
    for url in sorted(urls_without_card):
        if url in card_data:
            continue
        try:
            ev = _scrape_event(url, theater_name, source_slug, {})
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            logger.warning("[%s] Erro inesperado em %s: %s", theater_name, url, e)
        time.sleep(0.3)

    log(f"[{theater_name}] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de URLs e dados parciais da listagem
# ─────────────────────────────────────────────────────────────

def _collect(
    agenda_url:   str,
    theater_name: str,
    source_slug:  str,
) -> tuple[dict[str, dict], set[str]]:
    """
    Percorre a listagem e devolve:
    - card_data: {url → dict com dados do card} para eventos com página própria
    - urls_without_card: URLs encontradas sem contexto de card (raro)

    Filtro: apenas por venue (theater_name). Categorias não são filtradas.

    Estrutura HTML da listagem:
      Cada evento está num container div.flex-col.gap-y-5.justify-center.items-center
      com p.copy-xs-bold para espaço e categoria.
      O título está num elemento <a> ou heading.
      O subtitle/companhia está tipicamente num <p> ou <span> abaixo do título.
      A data/horário estão em elementos com classes de data.
    """
    try:
        r = requests.get(agenda_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro na listagem: {e}")
        return {}, set()

    soup             = BeautifulSoup(r.text, "lxml")
    card_data:       dict[str, dict] = {}
    urls_without_card: set[str]      = set()

    for container in soup.select(
        "div.flex.flex-col.gap-y-5.justify-center.items-center"
    ):
        tags = container.select("p.copy-xs-bold")
        if not tags:
            continue

        # Separar tags de venue das tags de categoria
        theaters: list[str] = []
        raw_category: str   = ""
        for tag in tags:
            style  = tag.get("style", "")
            text   = tag.get_text(strip=True)
            mapped = next(
                (v for k, v in THEATER_STYLE_MAP.items() if k in style), None
            )
            if mapped:
                theaters.append(mapped)
            else:
                raw_category = text

        # Filtro: este evento pertence ao teatro deste scraper?
        if theaters and theater_name not in theaters:
            continue

        # Link para página própria do evento
        ev_a = container.select_one("a[href*='/evento/']")
        if not ev_a:
            # Sem página própria — tentar link genérico
            ev_a = container.select_one("a[href]")

        url = ""
        if ev_a:
            href = ev_a.get("href", "")
            url  = href if href.startswith("http") else BASE + href

        # Título
        title = ""
        for sel in ("h2", "h3", "h4", ".titulo", ".title"):
            el = container.select_one(sel)
            if el:
                title = el.get_text(strip=True)
                break
        if not title and ev_a:
            title = ev_a.get_text(strip=True)

        # Subtitle / companhia (elemento imediatamente após o título, se existir)
        subtitle = _extract_subtitle(container)

        # Datas e horário do card
        card_text   = container.get_text(" ")
        dates_label, date_start, date_end = _parse_dates(card_text)
        schedule    = _extract_schedule(card_text)

        seed = {
            "title":        title,
            "subtitle":     subtitle,
            "raw_category": raw_category,
            "dates_label":  dates_label,
            "date_start":   date_start,
            "date_end":     date_end,
            "schedule":     schedule,
        }

        if url and "/evento/" in url:
            card_data[url] = seed
        elif url:
            # URL existe mas não é página de evento — regista sem seed
            urls_without_card.add(url)

    return card_data, urls_without_card


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(
    url:          str,
    theater_name: str,
    source_slug:  str,
    seed:         dict,
) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{theater_name}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ")

    # ── Título ────────────────────────────────────────────────
    title_el = soup.select_one("h1")
    title    = title_el.get_text(strip=True) if title_el else seed.get("title", "")
    if not title or len(title) < 3:
        return None

    # ── Subtitle / companhia ──────────────────────────────────
    subtitle = (
        _extract_subtitle(soup.select_one("main, article, body"))
        or seed.get("subtitle", "")
    )

    # ── Categoria ────────────────────────────────────────────
    raw_category = seed.get("raw_category", "")
    # Tentar confirmar/melhorar a categoria na página individual
    if not raw_category:
        for sel in (".categoria", ".category", "p.copy-xs-bold"):
            el = soup.select_one(sel)
            if el:
                raw_category = el.get_text(strip=True)
                break
    category = normalize_category(raw_category) if raw_category else "Outro"

    # ── Datas ─────────────────────────────────────────────────
    dates_label = seed.get("dates_label", "")
    date_start  = seed.get("date_start", "")
    date_end    = seed.get("date_end",   "")
    if not date_start:
        dates_label, date_start, date_end = _parse_dates(text)

    # ── Horário ───────────────────────────────────────────────
    schedule = seed.get("schedule", "") or _extract_schedule(text)

    # ── Imagem ────────────────────────────────────────────────
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, theater_name, url)

    # ── Bilhetes ─────────────────────────────────────────────
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        txt  = a.get_text(strip=True).lower()
        if any(x in href for x in ["bol.pt", "ticketline", "eventbrite"]) or \
           any(x in txt  for x in ["bilhete", "comprar", "tickets"]):
            ticket_url = href
            break

    # ── Sinopse ───────────────────────────────────────────────
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        candidate = og_desc.get("content", "").strip()
        # Rejeitar descrições genéricas do site
        if candidate and len(candidate) > 30 and theater_name.lower() not in candidate.lower():
            synopsis = candidate
    if not synopsis:
        for p in soup.select("main p, article p, .entry-content p, .descricao p"):
            t = p.get_text(strip=True)
            if len(t) > 60:
                synopsis = t
                break

    # ── Preço ─────────────────────────────────────────────────
    price_info, price_min, price_max = _parse_price(text)

    # ── Duração ───────────────────────────────────────────────
    duration, duration_min = _parse_duration(text)

    # ── Classificação etária ─────────────────────────────────
    age_rating, age_min = _parse_age(text)

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_technical_sheet(soup)

    event: dict = {
        "id":               make_id(source_slug, title),
        "title":            title,
        "theater":          theater_name,
        "category":         category,
        "dates_label":      dates_label,
        "date_start":       date_start,
        "date_end":         date_end,
        "sessions":         build_sessions(date_start, date_end, schedule),
        "schedule":         schedule,
        "synopsis":         truncate_synopsis(synopsis),
        "image":            image,
        "source_url":       url,
        "ticket_url":       ticket_url,
        "price_info":       price_info,
        "age_rating":       age_rating,
        "accessibility":    "",
        "technical_sheet":  technical_sheet,
    }

    # Campos opcionais — só incluir se tiverem valor
    if subtitle:
        event["subtitle"] = subtitle
    if price_min is not None:
        event["price_min"] = price_min
    if price_max is not None:
        event["price_max"] = price_max
    if duration:
        event["duration"] = duration
    if duration_min is not None:
        event["duration_min"] = duration_min
    if age_min is not None:
        event["age_min"] = age_min

    return event


# ─────────────────────────────────────────────────────────────
# Helpers de parsing
# ─────────────────────────────────────────────────────────────

def _extract_subtitle(container) -> str:
    """
    Tenta extrair o subtitle / companhia de um container BeautifulSoup.
    Procura em elementos típicos abaixo do título.
    """
    if container is None:
        return ""
    for sel in (".subtitle", ".subtitulo", ".companhia", ".author",
                "h2 + p", "h1 + p", ".h3 + p"):
        el = container.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if t and len(t) < 120:
                return t
    return ""


def _extract_schedule(text: str) -> str:
    """
    Extrai horário no formato "21h30", "21:30", "21h", etc.
    Devolve a primeira ocorrência ou string vazia.
    """
    m = re.search(r"\b(\d{1,2}h\d{0,2}|\d{1,2}:\d{2})\b", text)
    return m.group(1) if m else ""


def _parse_price(text: str) -> tuple[str, float | None, float | None]:
    """
    Extrai price_info (string), price_min e price_max (floats).
    Devolve ("", None, None) se não encontrar.
    """
    # Entrada livre / gratuito
    if re.search(r"entrada\s+livre|gratuito|free", text, re.IGNORECASE):
        return "Entrada livre", 0.0, 0.0

    # Intervalo: 10€ – 25€  ou  10 – 25€  ou  10,50 – 25,00€
    m = re.search(
        r"(\d+(?:[,\.]\d+)?)\s*€?\s*[-–]\s*(\d+(?:[,\.]\d+)?)\s*€",
        text,
    )
    if m:
        lo = float(m.group(1).replace(",", "."))
        hi = float(m.group(2).replace(",", "."))
        return f"{m.group(1)}€ – {m.group(2)}€", lo, hi

    # Valor único
    m2 = re.search(r"(\d+(?:[,\.]\d+)?)\s*€", text)
    if m2:
        val = float(m2.group(1).replace(",", "."))
        return f"{m2.group(1)}€", val, val

    return "", None, None


def _parse_duration(text: str) -> tuple[str, int | None]:
    """
    Extrai duração como string e como inteiro (minutos).
    Exemplos: "90 min" → ("90 min", 90); "1h30" → ("1h30", 90)
    """
    # "90 min" / "90min"
    m = re.search(r"(\d{2,3})\s*min\.?", text, re.IGNORECASE)
    if m:
        mins = int(m.group(1))
        return f"{mins} min", mins

    # "1h30" / "2h" / "1h 30"
    m2 = re.search(r"(\d{1,2})h\s*(\d{0,2})", text, re.IGNORECASE)
    if m2:
        hours = int(m2.group(1))
        minutes_part = int(m2.group(2)) if m2.group(2) else 0
        total = hours * 60 + minutes_part
        label = f"{m2.group(1)}h{m2.group(2)}" if minutes_part else f"{hours}h"
        return label, total

    return "", None


def _parse_age(text: str) -> tuple[str, int | None]:
    """
    Extrai classificação etária como string e como inteiro (age_min).
    Exemplos: "M/6" → ("M/6", 6); "+16" → ("+16", 16); "Livre" → ("Livre", 0)
    """
    m = re.search(r"\b(M\s*/\s*(\d+))\b", text)
    if m:
        return m.group(1).replace(" ", ""), int(m.group(2))

    m2 = re.search(r"\+\s*(\d+)", text)
    if m2:
        return f"+{m2.group(1)}", int(m2.group(1))

    if re.search(r"\blivre\b", text, re.IGNORECASE):
        return "Livre", 0

    return "", None


def _parse_technical_sheet(soup: BeautifulSoup) -> dict:
    """
    Extrai a ficha técnica a partir de pares label: valor no HTML.
    Procura em listas de definição (dl/dt/dd), tabelas, ou padrões "Label: Valor".
    Devolve dict com chaves normalizadas (ver TECHNICAL_LABELS).
    """
    sheet: dict = {}

    # Estratégia 1: dl > dt + dd
    for dl in soup.select("dl"):
        dts = dl.select("dt")
        dds = dl.select("dd")
        for dt, dd in zip(dts, dds):
            key   = dt.get_text(strip=True).lower().rstrip(":")
            value = dd.get_text(strip=True)
            norm  = TECHNICAL_LABELS.get(key)
            if norm and value:
                sheet[norm] = value

    # Estratégia 2: "Label: Valor" em texto de parágrafo ou li
    for el in soup.select("main p, main li, article p, article li, .ficha-tecnica p"):
        raw = el.get_text(strip=True)
        if ":" not in raw:
            continue
        parts = raw.split(":", 1)
        key   = parts[0].strip().lower()
        value = parts[1].strip()
        norm  = TECHNICAL_LABELS.get(key)
        if norm and value:
            sheet[norm] = value

    return sheet


# ─────────────────────────────────────────────────────────────
# Parse de datas (partilhado entre listagem e evento)
# ─────────────────────────────────────────────────────────────

def _parse_dates(text: str) -> tuple[str, str, str]:
    """
    Tenta extrair datas de texto livre.
    Formatos suportados (por ordem de preferência):
      1. DD.MM[.YYYY] – DD.MM.YYYY   (intervalo com ponto)
      2. DD/MM[/YYYY] – DD/MM/YYYY   (intervalo com barra)
      3. DD de Mês [de YYYY] – DD de Mês de YYYY  (intervalo por extenso)
      4. Data única nos formatos acima
    Devolve (dates_label, date_start, date_end).
    """
    # Intervalo: DD.MM[.YYYY] – DD.MM.YYYY
    m = re.search(
        r"(\d{1,2}\.\d{2}(?:\.\d{4})?)\s*[–—\-]\s*(\d{1,2}\.\d{2}\.\d{4})",
        text,
    )
    if m:
        dates_label = f"{m.group(1)} – {m.group(2)}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Intervalo: DD/MM[/YYYY] – DD/MM/YYYY
    m = re.search(
        r"(\d{1,2}/\d{2}(?:/\d{4})?)\s*[–—\-]\s*(\d{1,2}/\d{2}/\d{4})",
        text,
    )
    if m:
        dates_label = f"{m.group(1)} – {m.group(2)}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Intervalo por extenso: "20 de março de 2026 – 22 de março de 2026"
    MONTHS = (
        "janeiro|fevereiro|março|abril|maio|junho|"
        "julho|agosto|setembro|outubro|novembro|dezembro"
    )
    m = re.search(
        rf"(\d{{1,2}}\s+(?:de\s+)?(?:{MONTHS})(?:\s+(?:de\s+)?\d{{4}})?)"
        rf"\s*[–—\-]\s*"
        rf"(\d{{1,2}}\s+(?:de\s+)?(?:{MONTHS})(?:\s+(?:de\s+)?\d{{4}})?)",
        text, re.IGNORECASE,
    )
    if m:
        dates_label = f"{m.group(1).strip()} – {m.group(2).strip()}"
        date_start, date_end = parse_date_range(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Data única: DD.MM.YYYY ou DD/MM/YYYY
    m2 = re.search(r"(\d{1,2}[./]\d{2}[./]\d{4})", text)
    if m2:
        dates_label = m2.group(1)
        date_start = date_end = parse_date(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    # Data única por extenso
    m3 = re.search(
        rf"(\d{{1,2}}\s+(?:de\s+)?(?:{MONTHS})(?:\s+(?:de\s+)?\d{{4}})?)",
        text, re.IGNORECASE,
    )
    if m3:
        dates_label = m3.group(1).strip()
        date_start = date_end = parse_date(dates_label)
        if date_start:
            return dates_label, date_start, date_end

    return "", "", ""
