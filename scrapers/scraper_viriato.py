"""
Scraper: Teatro Viriato
Fonte: https://www.teatroviriato.com/pt/programacao
Cidade: Viseu

Estratégia:
  1. Parsear a listagem estática (sem pedidos extra por item).
  2. Visitar cada página individual em paralelo (ThreadPoolExecutor)
     para obter image, synopsis, ticket_url, price_info, duration,
     age_rating, ficha técnica e confirmação/correcção de datas.
  3. Aceitar TODOS os eventos (sem filtragem por categoria).
  4. Ignorar apenas eventos CMV e CANCELADO.
"""

import re
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

from scrapers.utils import (
    make_id, log,
    HEADERS, can_scrape, truncate_synopsis, build_image_object, build_sessions,
)
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Metadados do teatro
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "viriato",
    "name":        "Teatro Viriato",
    "short":       "Viriato",
    "color":       "#1565c0",
    "city":        "Viseu",
    "address":     "Largo Mouzinho de Albuquerque, 3500-160 Viseu",
    "site":        "https://www.teatroviriato.com",
    "programacao": "https://www.teatroviriato.com/pt/programacao",
    "lat":         40.6566,
    "lng":         -7.9122,
    "salas":       ["Sala de Espetáculos", "Sala Estúdio"],
    "logo_url":    "https://www.teatroviriato.com/assets/images/logo.svg",
    "favicon_url": "https://www.teatroviriato.com/assets/favicon/favicon.svg",
    "facade_url":  "https://www.teatroviriato.com/assets/images/teatro-viriato-fachada.jpg",
    "aliases": [
        "teatro viriato",
        "viriato",
        "centro de artes do espectáculo de viseu",
        "centro de artes do espetaculo de viseu",
        "caev",
    ],
    "description": (
        "O Teatro Viriato — Centro de Artes do Espectáculo de Viseu é uma das mais "
        "relevantes estruturas culturais do interior de Portugal, com uma programação "
        "eclética de teatro, dança, música e artes performativas."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://www.teatroviriato.com"
AGENDA       = f"{BASE}/pt/programacao"

# Paralelismo
_DETAIL_WORKERS = 5
_DETAIL_SLEEP   = 0.1   # segundos por worker entre pedidos

_SKIP_CATEGORIES = {"cmv", "cancelado"}

# Mapa de categorias raw do Viriato → valor para normalize_category
_VIRIATO_CATEGORY_MAP: dict[str, str] = {
    "teatro":           "teatro",
    "dança":            "dança",
    "música":           "música",
    "concerto":         "concerto",
    "ópera":            "ópera",
    "cinema":           "cinema",
    "performance":      "performance",
    "circo":            "circo",
    "infantil":         "infantil",
    "família":          "família",
    "workshop":         "workshop",
    "oficina":          "workshop",
    "residência":       "residência",
    "conversa":         "conversa",
    "conferência":      "conferência",
    "exposição":        "exposição",
    "festival":         "festival",
    "visita":           "visita guiada",
}

import re as _re_v

def _infer_category_viriato(category_raw: str, title: str, subtitle: str, synopsis: str) -> str:
    """Inferência de categoria para eventos Viriato sem categoria explícita."""
    # Tentar primeiro o mapeamento directo
    mapped = _VIRIATO_CATEGORY_MAP.get(category_raw.lower().strip())
    if mapped:
        return mapped

    t = (title + " " + subtitle + " " + (synopsis or "")[:200]).lower()

    if _re_v.search(r"\bresid[eê]ncia\b|laborat[oó]rio\b", t):
        return "residência"
    if _re_v.search(r"\boficina\b|workshop\b|forma[cç][aã]o\b|summer lab\b|candidatura", t):
        return "workshop"
    if _re_v.search(r"\bconversa[s]?\b|di[aá]logo\b|col[oó]quio\b|debate\b|confer[eê]ncia\b", t):
        return "conversa"
    if _re_v.search(r"\bvisita[s]? guiada[s]?\b|percurso\b", t):
        return "visita guiada"
    if _re_v.search(r"\bconcerto\b|m[uú]sica\b|recital\b|tun[ao]\b|coro\b|banda\b|cantora?\b", t):
        return "concerto"
    if _re_v.search(r"\bfilm[e]?\b|cinema\b|document[aá]rio\b|proje[cç][aã]o\b", t):
        return "cinema"
    if _re_v.search(r"\bfestival\b", t):
        return "festival"
    if _re_v.search(r"\binfantil\b|para\s+crian[cç]as?\b|espet[aá]culo\s+infantil|fam[ií]li[ao]\b", t):
        return "infantil"
    if _re_v.search(r"\bperformance\b|instala[cç][aã]o\b", t):
        return "performance"
    if _re_v.search(r"\bdan[cç]a\b|coreografi", t):
        return "dança"
    return category_raw  # deixar para o harmonizer decidir

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

_MONTH_NAMES = {v: k for k, v in _PT_MONTHS.items()}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro ao carregar listagem: {e}")
        return []

    soup  = BeautifulSoup(r.text, "lxml")
    stubs = _parse_listing(soup)
    log(f"[{THEATER_NAME}] {len(stubs)} candidatos na listagem")

    # ── Pedidos de detalhe em paralelo ────────────────────────
    results_raw: list[tuple[int, dict]] = []
    lock = threading.Lock()

    def fetch_stub(idx_stub: tuple[int, dict]) -> tuple[int, dict | None]:
        idx, stub = idx_stub
        time.sleep(_DETAIL_SLEEP)
        try:
            ev = _scrape_event(stub)
            return idx, ev
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro inesperado em {stub.get('url', '?')}: {e}")
            return idx, None

    with ThreadPoolExecutor(max_workers=_DETAIL_WORKERS) as executor:
        futures = {
            executor.submit(fetch_stub, (idx, stub)): idx
            for idx, stub in enumerate(stubs)
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
# Parsing da listagem estática
# ─────────────────────────────────────────────────────────────

def _parse_listing(soup: BeautifulSoup) -> list[dict]:
    cal = soup.find("section", id="calendar")
    if not cal:
        log(f"[{THEATER_NAME}] Secção #calendar não encontrada")
        return []

    year_div = cal.find("div", class_=re.compile(r"is-year"))
    year_m   = re.search(r"\d{4}", year_div.get_text()) if year_div else None
    listing_year = int(year_m.group()) if year_m else datetime.now().year

    stubs     = []
    seen_urls = set()

    for month_panel in cal.find_all("div", attrs={"data-month": True}):
        month_num = int(month_panel.get("data-month", 0))

        for card in month_panel.find_all("a", class_="show-card"):
            href = card.get("href", "")
            if not href or "espetaculo" not in href:
                continue
            url = href if href.startswith("http") else urljoin(BASE, href)
            if url in seen_urls:
                continue

            card_text = card.get_text(" ", strip=True)
            if "CANCELADO" in card_text.upper():
                continue

            cat_div      = card.find("div", class_="category")
            category_raw = cat_div.get_text(strip=True) if cat_div else ""
            if category_raw.lower().strip() in _SKIP_CATEGORIES:
                continue

            title_div    = card.find("div", class_="title")
            subtitle_div = card.find("div", class_="subtitle")
            title        = title_div.get_text(strip=True) if title_div else ""
            subtitle     = subtitle_div.get_text(strip=True) if subtitle_div else ""
            if not title:
                continue

            dates_div  = card.find("div", class_="dates")
            dates_raw  = dates_div.get_text(strip=True) if dates_div else ""
            date_start, date_end, dates_label = _parse_listing_dates(
                dates_raw, month_num, listing_year
            )

            hour_div = card.find("div", class_="hour-info")
            schedule = _extract_schedule(hour_div.get_text(" ", strip=True) if hour_div else "")

            img_tag     = card.find("img", class_="image")
            listing_img = ""
            if img_tag:
                src = img_tag.get("src") or img_tag.get("data-src") or ""
                if src:
                    listing_img = src if src.startswith("http") else urljoin(BASE, src)

            seen_urls.add(url)
            stubs.append({
                "url":          url,
                "title":        title,
                "subtitle":     subtitle,
                "category_raw": category_raw,
                "date_start":   date_start,
                "date_end":     date_end,
                "dates_label":  dates_label,
                "schedule":     schedule,
                "listing_img":  listing_img,
                "month_num":    month_num,
                "listing_year": listing_year,
            })

    return stubs


def _parse_listing_dates(
    raw: str, month_num: int, year: int
) -> tuple[str, str, str]:
    raw = raw.strip()
    if not raw:
        return "", "", ""

    m = re.match(
        r"^(\d{1,2})\s+([a-záéíóú]{3})\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3})\s*(\d{4})?$",
        raw, re.IGNORECASE,
    )
    if m:
        d1, mo1, d2, mo2, yr = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            y = int(yr) if yr else year
            ds = f"{y}-{n1:02d}-{int(d1):02d}"
            de = f"{y}-{n2:02d}-{int(d2):02d}"
            return ds, de, f"{int(d1)} {mo1} – {int(d2)} {mo2} {y}"

    m = re.match(r"^(\d{1,2})\s*[-–]\s*(\d{1,2})$", raw)
    if m:
        d1, d2 = int(m.group(1)), int(m.group(2))
        mo_s = _MONTH_NAMES.get(month_num, "")
        ds = f"{year}-{month_num:02d}-{d1:02d}"
        de = f"{year}-{month_num:02d}-{d2:02d}"
        return ds, de, f"{d1} – {d2} {mo_s} {year}"

    m = re.match(r"^(\d{1,2})(?:\s*,\s*\d{1,2})+$", raw)
    if m:
        days = [int(x) for x in re.findall(r"\d{1,2}", raw)]
        d1, d2 = days[0], days[-1]
        mo_s = _MONTH_NAMES.get(month_num, "")
        ds = f"{year}-{month_num:02d}-{d1:02d}"
        de = f"{year}-{month_num:02d}-{d2:02d}"
        label = ", ".join(str(d) for d in days) + f" {mo_s} {year}"
        return ds, de, label

    m = re.match(r"^(\d{1,2})$", raw)
    if m:
        d = int(m.group(1))
        mo_s = _MONTH_NAMES.get(month_num, "")
        ds = f"{year}-{month_num:02d}-{d:02d}"
        return ds, ds, f"{d} {mo_s} {year}"

    return "", "", raw


def _extract_schedule(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    m = re.search(r"(\d{1,2}[h:]\d{2})", text, re.IGNORECASE)
    return m.group(1) if m else ""


# ─────────────────────────────────────────────────────────────
# Scraping da página individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(stub: dict) -> dict | None:
    url = stub["url"]
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    title = stub["title"] or _get_title(soup)
    if not title or len(title) < 2:
        return None

    date_start  = stub["date_start"]
    date_end    = stub["date_end"]
    dates_label = stub["dates_label"]

    if not date_start:
        date_start, date_end, dates_label = _extract_dates_from_page(
            soup, full_text, stub["month_num"], stub["listing_year"]
        )
    if not date_start:
        log(f"[{THEATER_NAME}] '{title}' sem date_start — ignorado")
        return None

    category_raw_norm = _infer_category_viriato(
        stub["category_raw"],
        stub.get("title", ""),
        stub.get("subtitle", ""),
        synopsis if synopsis else "",
    )
    category = normalize_category(category_raw_norm)

    schedule = stub["schedule"]
    if not schedule:
        m = re.search(r"(\d{1,2}[h:]\d{2})", full_text)
        if m:
            schedule = m.group(1)

    synopsis = _extract_synopsis(soup)

    raw_img = ""
    og = soup.find("meta", property="og:image")
    if og:
        raw_img = og.get("content", "").strip()
    if not raw_img or raw_img == BASE + "/" or raw_img == BASE:
        for img in soup.find_all("img", src=re.compile(r"/contents/")):
            src = img.get("src", "")
            if src:
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break
    if not raw_img and stub.get("listing_img"):
        raw_img = stub["listing_img"]
    image = build_image_object(raw_img, soup, THEATER_NAME, url) if raw_img else None

    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"ticketline|bol\.pt|bilhete|comprar", href, re.IGNORECASE):
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

    price_info = ""
    price_min  = None
    price_max  = None
    m_p = re.search(
        r"(Entrada\s+livre|gratuito|\d+(?:[,\.]\d+)?\s*€(?:\s*/\s*\d+(?:[,\.]\d+)?\s*€)?(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if m_p:
        price_info = m_p.group(1).strip()
        prices = [float(p.replace(",", ".")) for p in re.findall(r"\d+(?:[,.]\d+)?(?=\s*€)", price_info)]
        if prices:
            price_min = min(prices)
            price_max = max(prices)

    duration     = ""
    duration_min = None
    m_d = re.search(r"(\d+)\s*min\.?", full_text, re.IGNORECASE)
    if m_d:
        mins         = int(m_d.group(1))
        duration     = f"{mins} min."
        duration_min = mins

    age_rating = ""
    age_min    = None
    m_a = re.search(r"M\s*/\s*(\d+)", full_text) or re.search(r"\+\s*(\d+)", full_text)
    if m_a:
        age_num    = int(m_a.group(1))
        age_rating = f"M/{age_num}"
        age_min    = age_num

    sala = ""
    m_sala = re.search(r"LOCAL\s*[:\-]?\s*([^\n]{3,60})", full_text, re.IGNORECASE)
    if m_sala:
        sala = m_sala.group(1).strip()
    else:
        for s in THEATER.get("salas", []):
            if s.lower() in full_text.lower():
                sala = s
                break

    technical_sheet = _parse_ficha(full_text)

    director = (
        technical_sheet.get("encenação")
        or technical_sheet.get("direção")
        or technical_sheet.get("coreografia")
        or ""
    )
    cast = []
    interp = technical_sheet.get("interpretação", "")
    if interp:
        cast = [p.strip() for p in re.split(r"[,/|]", interp) if p.strip()]

    return {
        "id":               make_id(SOURCE_SLUG, title),
        "title":            title,
        "subtitle":         stub.get("subtitle") or "",
        "theater":          THEATER_NAME,
        "category":         category,
        "dates_label":      dates_label,
        "date_start":       date_start,
        "date_end":         date_end or date_start,
        "sessions":         build_sessions(date_start, date_end or date_start, schedule),
        "schedule":         schedule,
        "synopsis":         truncate_synopsis(synopsis),
        "image":            image,
        "source_url":       url,
        "ticket_url":       ticket_url,
        "price_info":       price_info,
        **({"price_min": price_min} if price_min is not None else {}),
        **({"price_max": price_max} if price_max is not None else {}),
        "duration":         duration,
        **({"duration_min": duration_min} if duration_min is not None else {}),
        "age_rating":       age_rating,
        **({"age_min": age_min} if age_min is not None else {}),
        "sala":             sala,
        "director":         director,
        "cast":             cast,
        "technical_sheet":  technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _get_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower()[:3])


def _extract_dates_from_page(
    soup: BeautifulSoup, text: str, month_num: int, year: int
) -> tuple[str, str, str]:
    for el in soup.find_all(["h2", "h3", "p", "span"], limit=30):
        t = el.get_text(" ", strip=True)
        m = re.search(
            r"(\d{1,2})\s+([a-záéíóú]{3,})(?:\s+(\d{4}))?\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?",
            t, re.IGNORECASE,
        )
        if m:
            d1, mo1, y1, d2, mo2, y2 = m.groups()
            n1, n2 = _mon(mo1), _mon(mo2)
            if n1 and n2:
                yr2 = int(y2) if y2 else year
                yr1 = int(y1) if y1 else yr2
                return (
                    f"{yr1}-{n1:02d}-{int(d1):02d}",
                    f"{yr2}-{n2:02d}-{int(d2):02d}",
                    f"{int(d1)} {mo1} – {int(d2)} {mo2} {yr2}",
                )
        m = re.search(r"(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?", t, re.IGNORECASE)
        if m:
            d, mon_s, yr_s = m.groups()
            n = _mon(mon_s)
            if n:
                y = int(yr_s) if yr_s else year
                ds = f"{y}-{n:02d}-{int(d):02d}"
                return ds, ds, f"{int(d)} {mon_s} {y}"

    return "", "", ""


def _extract_synopsis(soup: BeautifulSoup) -> str:
    og_desc = soup.find("meta", property="og:description")
    og_text = og_desc.get("content", "").strip() if og_desc else ""

    _GENERIC = "casa cultural em Viseu"
    if og_text and _GENERIC not in og_text:
        return og_text

    main = soup.find("main") or soup.find("div", id="app") or soup
    synopsis = ""
    for p in main.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) < 80:
            continue
        if re.match(
            r"^(\d+%|Mecenas|Sócios|Famílias|Profissionais|Funcionários|m/\s*\d+|"
            r"Os descontos|Este site|Todos os direitos|©)",
            t, re.IGNORECASE,
        ):
            continue
        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break
    return synopsis.strip() or og_text


def _parse_ficha(text: str) -> dict:
    ficha      = {}
    known_keys = [
        ("texto",          r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?\s+"),
        ("encenação",      r"[Ee]ncena[çc][aã]o\s+"),
        ("coreografia",    r"[Cc]oreografia\s+"),
        ("dramaturgia",    r"[Dd]ramaturgia\s+"),
        ("direção",        r"[Dd]ire[çc][aã]o(?:\s+artística)?\s+"),
        ("tradução",       r"[Tt]radu[çc][aã]o\s+"),
        ("adaptação",      r"[Aa]dapta[çc][aã]o\s+"),
        ("cenografia",     r"[Cc]enografia\s+"),
        ("figurinos",      r"[Ff]igurinos?\s+"),
        ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz\s+|[Ii]lumina[çc][aã]o\s+"),
        ("som",            r"[Dd]esenho\s+de\s+[Ss]om\s+|[Ss]onoplastia\s+"),
        ("música",         r"[Mm][úu]sica(?:\s+original)?\s+"),
        ("interpretação",  r"[Ii]nterpreta[çc][aã]o\s+"),
        ("produção",       r"[Pp]rodu[çc][aã]o(?:\s+[Ee]xecutiva)?\s+"),
        ("coprodução",     r"[Cc]oprodu[çc][aã]o\s+"),
        ("fotografia",     r"[Ff]otografia(?:\s+e\s+identidade\s+gráfica)?\s+"),
    ]
    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value = re.sub(r"\s+", " ", text[end:next_start].strip())
        value = re.split(r"\s+(?:Apoio|Agradecimentos|©|\d{4})", value)[0]
        value = value[:200].strip()
        if value and key not in ficha:
            ficha[key] = value
    return ficha
