"""
Scraper: Theatro Circo
Fonte: https://theatrocirco.com/programa/
Cidade: Braga

Estrutura do site (HTML estático, WordPress / The Events Calendar):
  - Listagem: /programa/ — página única com TODOS os eventos (sem paginação).
  - Pedidos de detalhe correm em paralelo via ThreadPoolExecutor.

[Ver docstring original para detalhes da estrutura HTML]
"""

import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
    build_sessions,
)
from scrapers.schema import normalize_category

# ─────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────

_BASE    = "https://theatrocirco.com"
_AGENDA  = f"{_BASE}/programa/"

# Paralelismo
_DETAIL_WORKERS = 5
_DETAIL_SLEEP   = 0.1   # segundos por worker entre pedidos

_PT_MONTHS = {
    "janeiro": 1,  "fevereiro": 2,  "março": 3,   "marco": 3,
    "abril": 4,    "maio": 5,       "junho": 6,    "julho": 7,
    "agosto": 8,   "setembro": 9,   "outubro": 10,
    "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

THEATER = {
    "id":          "theatrocirco",
    "name":        "Theatro Circo",
    "short":       "TC",
    "color":       "#b71c1c",
    "city":        "Braga",
    "address":     "Av. da Liberdade, 697, 4710-251 Braga",
    "site":        "https://theatrocirco.com",
    "programacao": "https://theatrocirco.com/programa/",
    "logo_url":    "https://theatrocirco.com/wp-content/themes/theatrocirco/assets/img/logo.svg",
    "favicon_url": "https://theatrocirco.com/favicon.ico",
    "facade_url":  "https://theatrocirco.com/wp-content/uploads/2019/09/fachada.jpg",
    "lat":         41.5454,
    "lng":         -8.4265,
    "salas":       ["Sala Principal", "Pequeno Auditório"],
    "aliases": [
        "theatro circo",
        "theatro circo braga",
        "teatro circo",
        "teatro circo braga",
    ],
    "description": (
        "O Theatro Circo é um dos mais emblemáticos teatros do norte de Portugal, "
        "inaugurado em 1915 em Braga. Com uma programação diversa de teatro, música, "
        "dança e cinema, é referência cultural da cidade e da região."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(_BASE):
        log(f"[{THEATER_NAME}] robots.txt: scraping bloqueado para {_BASE}")
        return []

    candidates = _collect_candidates()
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos na listagem")

    # ── Pedidos de detalhe em paralelo ────────────────────────
    results_raw: list[tuple[int, dict]] = []
    lock = threading.Lock()

    def fetch_candidate(idx_item: tuple[int, dict]) -> tuple[int, dict | None]:
        idx, item = idx_item
        time.sleep(_DETAIL_SLEEP)
        try:
            ev = _scrape_event(item)
            return idx, ev
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro em {item['url']}: {e}")
            return idx, None

    with ThreadPoolExecutor(max_workers=_DETAIL_WORKERS) as executor:
        futures = {
            executor.submit(fetch_candidate, (idx, item)): idx
            for idx, item in enumerate(candidates)
        }
        for future in as_completed(futures):
            idx, ev = future.result()
            if ev is not None:
                with lock:
                    results_raw.append((idx, ev))

    # Ordenar pela ordem original da listagem
    results_raw.sort(key=lambda x: x[0])

    # Deduplicar por ID
    seen_ids: set[str] = set()
    events: list[dict] = []
    for _, ev in results_raw:
        eid = ev["id"]
        if eid not in seen_ids:
            seen_ids.add(eid)
            events.append(ev)

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates() -> list[dict]:
    try:
        r = requests.get(_AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    for card in soup.select("div.highlight-card"):

        a_img = card.select_one("div.highlight-image a[href]")
        if not a_img:
            continue
        href = a_img["href"]
        url  = href if href.startswith("http") else urljoin(_BASE, href)
        url  = url.rstrip("/") + "/"
        if url in seen_urls:
            continue
        seen_urls.add(url)

        meta_el   = card.select_one("p.highlight-meta.desktop") or card.select_one("p.highlight-meta")
        meta_text = meta_el.get_text(" ", strip=True) if meta_el else ""

        raw_category = ""
        dates_raw    = ""
        if "→" in meta_text:
            parts        = meta_text.split("→", 1)
            dates_raw    = parts[0].strip()
            raw_category = parts[1].strip()

        title    = ""
        subtitle = ""
        h3_one = card.select_one("h3.line-one")
        if h3_one:
            nxt = h3_one.find_next_sibling("p")
            if nxt:
                title = nxt.get_text(strip=True)
        h3_two = card.select_one("h3.line-two")
        if h3_two:
            nxt = h3_two.find_next_sibling("p")
            if nxt:
                subtitle = nxt.get_text(strip=True)

        tags = list({a.get_text(strip=True) for a in card.select("a.tag")})

        ticket_url = ""
        t = card.select_one("div.highlight-ticket a[href]")
        if t:
            ticket_url = t["href"]

        thumb_url = ""
        img_el = card.select_one("div.highlight-image img[src]")
        if img_el:
            src = img_el.get("src", "")
            thumb_url = src if src.startswith("http") else urljoin(_BASE, src)

        dates_label, date_start, date_end = _parse_date_text(dates_raw or meta_text)

        candidates.append({
            "url":          url,
            "title":        title,
            "subtitle":     subtitle,
            "raw_category": raw_category,
            "tags":         tags,
            "ticket_url":   ticket_url,
            "thumb_url":    thumb_url,
            "dates_label":  dates_label,
            "date_start":   date_start,
            "date_end":     date_end,
            "dates_raw":    dates_raw,
        })

    return candidates


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(item: dict) -> dict | None:
    url = item["url"]
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title:
        title = item.get("title", "")
    if not title or len(title) < 2:
        return None

    subtitle = item.get("subtitle", "")
    h2 = soup.find("h2")
    if h2:
        h2_text = h2.get_text(strip=True)
        if h2_text and len(h2_text) < 150 and h2_text != title:
            subtitle = h2_text

    raw_cat   = item.get("raw_category", "")
    page_tags = [a.get_text(strip=True) for a in soup.find_all("a", href=re.compile(r"/event_tag/"))]
    all_tags  = list({t for t in (item.get("tags", []) + page_tags) if t})
    category  = normalize_category(raw_cat) if raw_cat else "Outro"

    dates_label = item.get("dates_label", "")
    date_start  = item.get("date_start", "")
    date_end    = item.get("date_end", "")

    if not date_start:
        for el in soup.find_all(string=re.compile(
            r"\d{1,2}\s+(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)",
            re.IGNORECASE,
        )):
            dl, ds, de = _parse_date_text(el.strip())
            if ds:
                dates_label, date_start, date_end = dl, ds, de
                break

    if not date_start:
        log(f"[{THEATER_NAME}] Sem data: {url}")
        return None

    schedule = ""
    sala     = ""
    hora_m   = re.search(r"\b(\d{1,2}[hH]\d{2})\b", full_text)
    if hora_m:
        schedule = hora_m.group(1).lower()

    sala_m = re.search(
        r"(Sala\s+Principal|Pequeno\s+Audit[oó]rio|Grande\s+Audit[oó]rio|Foyer)",
        full_text, re.IGNORECASE,
    )
    if sala_m:
        sala = sala_m.group(1)

    price_info = ""
    price_min  = None
    price_max  = None
    preco_m = re.search(
        r"(Entrada\s+(?:livre|gratuita)|Gratuito|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if preco_m:
        price_info = preco_m.group(1).strip()
        nums   = re.findall(r"\d+(?:[,\.]\d+)?", price_info)
        floats = [float(n.replace(",", ".")) for n in nums]
        if floats:
            price_min = min(floats)
            price_max = max(floats)

    age_rating = ""
    age_min    = None
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if age_m:
        age_min    = int(age_m.group(1))
        age_rating = f"M/{age_min}"

    duration     = ""
    duration_min = None
    dur_m = re.search(r"[Dd]ura[çc][aã]o\D{0,10}?(\d+)\s*minutos?", full_text)
    if not dur_m:
        dur_m = re.search(r"\b(\d{2,3})\s*min(?:utos?)?\b", full_text)
    if dur_m:
        duration_min = int(dur_m.group(1))
        duration     = f"{duration_min} min."

    image   = None
    raw_img = ""
    og_tags = soup.find_all("meta", property="og:image")
    for og in reversed(og_tags):
        candidate = og.get("content", "").strip()
        if candidate and "wp-content/uploads" in candidate:
            raw_img = candidate
            break
    if not raw_img and og_tags:
        raw_img = og_tags[-1].get("content", "").strip()
    if not raw_img or "share-image" in raw_img:
        thumb = item.get("thumb_url", "")
        if thumb:
            raw_img = thumb
    if not raw_img or "share-image" in raw_img:
        for img in soup.find_all("img", src=re.compile(r"/wp-content/uploads/")):
            src = img.get("src", "")
            if src and "logo" not in src.lower() and "cropped" not in src.lower():
                raw_img = src if src.startswith("http") else urljoin(_BASE, src)
                break
    if raw_img and raw_img.startswith("http"):
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    ticket_url = item.get("ticket_url", "")
    if not ticket_url:
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if any(k in h for k in ("bol.pt", "ticketline.pt")):
                ticket_url = h if h.startswith("http") else urljoin(_BASE, h)
                break

    synopsis        = _extract_synopsis(soup)
    technical_sheet = _parse_ficha(soup, full_text)

    accessibility = []
    if re.search(r"L[Gg][Pp]|L[íi]ngua\s+[Gg]estual", full_text):
        accessibility.append("LGP")
    if re.search(r"[Aa]udiodes[ck]ri[çc][aã]o", full_text):
        accessibility.append("Audiodescrição")

    sessions = build_sessions(date_start, date_end, schedule)

    ev = {
        "id":               make_id(SOURCE_SLUG, title),
        "title":            title,
        "theater":          THEATER_NAME,
        "source_url":       url,
        "date_start":       date_start,
        "category":         category,
        "dates_label":      dates_label,
        "sessions":         sessions,
        "image":            image,
        "synopsis":         truncate_synopsis(synopsis),
        "ticket_url":       ticket_url,
        "price_info":       price_info,
        "technical_sheet":  technical_sheet,
        "tags":             all_tags,
        "accessibility":    accessibility,
    }

    if subtitle:
        ev["subtitle"] = subtitle
    if date_end and date_end != date_start:
        ev["date_end"] = date_end
    if schedule:
        ev["schedule"] = schedule
    if sala:
        ev["sala"] = sala
    if duration:
        ev["duration"]     = duration
        ev["duration_min"] = duration_min
    if age_rating:
        ev["age_rating"] = age_rating
        ev["age_min"]    = age_min
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max

    return ev


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_date_text(text: str) -> tuple[str, str, str]:
    if not text:
        return "", "", ""

    text = text.strip()
    text = re.sub(r"^[Ss]ess[õo]es\s+de\s+", "", text)

    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, d2, mo2, yr = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            y2 = int(yr) if yr else _infer_year(n2, int(d2))
            y1 = _infer_year_start(n1, n2, y2)
            label = f"{d1} {mo1.capitalize()} a {d2} {mo2.capitalize()}"
            return (label,
                    f"{y1}-{n1:02d}-{int(d1):02d}",
                    f"{y2}-{n2:02d}-{int(d2):02d}")

    m = re.search(
        r"(\d{1,2})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            label = f"{d1} a {d2} {mon_s.capitalize()}"
            return (label,
                    f"{y}-{n:02d}-{int(d1):02d}",
                    f"{y}-{n:02d}-{int(d2):02d}")

    m = re.search(
        r"((?:\d{1,2}[,\s]+)+(?:e\s+)?\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        days_str, mon_s, yr = m.groups()
        all_days = [int(d) for d in re.findall(r"\d{1,2}", days_str)]
        n = _mon(mon_s)
        if n and all_days:
            d1, d2 = min(all_days), max(all_days)
            y = int(yr) if yr else _infer_year(n, d2)
            label = f"{d1} e {d2} {mon_s.capitalize()}"
            return (label,
                    f"{y}-{n:02d}-{d1:02d}",
                    f"{y}-{n:02d}-{d2:02d}")

    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d, mon_s, yr = m.groups()
        n = _mon(mon_s)
        if n:
            y  = int(yr) if yr else _infer_year(n, int(d))
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{d} {mon_s.capitalize()}", ds, ds

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower().strip())


def _infer_year(month: int, day: int) -> int:
    from datetime import datetime
    now = datetime.now()
    if month > now.month or (month == now.month and day >= now.day):
        return now.year
    return now.year + 1


def _infer_year_start(ini_month: int, end_month: int, end_year: int) -> int:
    return end_year if ini_month <= end_month else end_year - 1


# ─────────────────────────────────────────────────────────────
# Extracção de sinopse
# ─────────────────────────────────────────────────────────────

def _extract_synopsis(soup) -> str:
    synopsis = ""
    ficha_start = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            ficha_start = el
            break

    if ficha_start:
        all_descendants = list(soup.descendants)
        try:
            fic_pos = all_descendants.index(ficha_start)
        except ValueError:
            fic_pos = len(all_descendants)
    else:
        fic_pos = None

    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) < 40:
            continue
        if re.match(
            r"^(Este site|Inscreva|Pretende receber|campos de preenchimento"
            r"|A reserva|Os seus dados|Ao submeter)",
            t, re.IGNORECASE,
        ):
            continue
        if fic_pos is not None:
            try:
                all_descendants = list(soup.descendants)
                p_pos = all_descendants.index(p)
                if p_pos > fic_pos:
                    break
            except ValueError:
                pass

        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break

    if not synopsis:
        og = soup.find("meta", property="og:description")
        if og:
            desc = og.get("content", "").strip()
            if desc and "theatrocirco.com" not in desc.lower() and len(desc) > 40:
                synopsis = desc

    return synopsis


# ─────────────────────────────────────────────────────────────
# Parsing da ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(soup, full_text: str) -> dict:
    _KNOWN = [
        ("criação",        r"[Cc]ria[çc][aã]o(?:\s*,\s*texto\s*e\s*interpreta[çc][aã]o)?(?:\s*e\s*\w+)*"),
        ("texto",          r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?"),
        ("encenação",      r"[Ee]ncena[çc][aã]o"),
        ("direção",        r"[Dd]ire[çc][aã]o(?:\s+[Aa]rt[íi]stica)?"),
        ("dramaturgia",    r"[Aa]poio\s+[àa]\s+cria[çc][aã]o\s+e\s+dramaturgia|[Dd]ramaturgia"),
        ("interpretação",  r"[Ii]nterpreta[çc][aã]o"),
        ("coreografia",    r"[Cc]oreografia"),
        ("tradução",       r"[Tt]radu[çc][aã]o"),
        ("adaptação",      r"[Aa]dapta[çc][aã]o"),
        ("cenografia",     r"[Cc]enografia|[Ee]spa[çc]o\s+[Cc][ée]nico|[Dd]esenho\s+de\s+[Ee]spa[çc]o"),
        ("figurinos",      r"[Ff]igurinos?"),
        ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz|[Ii]lumina[çc][aã]o"),
        ("som",            r"[Ss]onoplastia|[Pp]rodu[çc][aã]o\s+[Mm]usical\s+e\s+[Ss]onoplastia|[Dd]esenho\s+[Ss]onoro|[Dd]esenho\s+de\s+[Ss]om"),
        ("música",         r"[Mm][úu]sica\s+[Oo]riginal|[Mm][úu]sica"),
        ("produção",       r"[Pp]rodu[çc][aã]o\s+[Ee]xecutiva|[Pp]rodu[çc][aã]o"),
        ("coprodução",     r"[Cc]o-?[Pp]rodu[çc][aã]o"),
        ("fotografia",     r"[Ff]otografia(?:\s+e\s+[Dd]esign\s+[Gg]ráfico)?"),
        ("design",         r"[Dd]esign\s+[Gg]ráfico"),
    ]

    ficha    = {}
    ficha_el = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            ficha_el = el.find_parent(["div", "section", "article", "p"]) or el.find_parent()
            break

    text = ficha_el.get_text(" ", strip=True) if ficha_el else full_text

    positions = []
    for key, pattern in _KNOWN:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start]).strip()
        value = re.split(
            r"\s+(?:Apoio|Agradecimentos|Duração|©|Parcerias|Residências|Agradecimento)",
            value, flags=re.IGNORECASE,
        )[0]
        value = value[:200].strip()
        value = re.sub(r"^[\s:,;]+", "", value).strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha
