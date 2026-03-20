"""
Scraper: Teatro Maria Matos
Listagem: https://teatromariamatos.pt/tipo/teatro/
URLs eventos: /espetaculos/slug/
"""
import re
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from scrapers.utils import (
    make_id, parse_date, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)

BASE   = "https://teatromariamatos.pt"
AGENDA = f"{BASE}/tipo/teatro/"

THEATER = {
    "id":          "mariamatos",
    "name":        "Teatro Maria Matos",
    "short":       "Maria Matos",
    "color":       "#e65100",
    "city":        "Lisboa",
    "address":     "Avenida Frei Miguel Contreiras, 52, 1700-213 Lisboa",
    "site":        "https://teatromariamatos.pt",
    "programacao": "https://teatromariamatos.pt/tipo/teatro/",
    "lat":         38.7466,
    "lng":         -9.1365,
    "salas":       ["Grande Sala", "Sala Estúdio"],
    "aliases":     ["maria matos", "tmm", "teatro maria matos"],
    "description": "O Teatro Maria Matos é um espaço de referência para as artes performativas contemporâneas em Lisboa, com programação inovadora de teatro, dança, música e performance.",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

WEEKDAYS_PT = r"(?:segunda|terça|quarta|quinta|sexta|sábado|domingo)"

SCHEDULE_LINE = re.compile(
    r"(domingos?|segundas?|terças?|quartas?|quintas?|sextas?|sábados?)"
    r"\s*[·•]\s*(\d{1,2}:\d{2})",
    re.IGNORECASE,
)


def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[Maria Matos] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen, events = set(), []

    for a in soup.find_all("a", href=re.compile(r"/espetaculos/")):
        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)
        ev = _scrape_event(full)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[Maria Matos] {len(events)} eventos")
    return events


def _scrape_event(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[Maria Matos] Erro em {url}: {e}")
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

    # Imagem
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, THEATER_NAME, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "ticketline" in href or "sapo.pt" in href:
            ticket_url = href
            break

    # Categoria
    category = "Teatro"
    cat_links = soup.select(
        ".tipo a, [class*='tipo'] a, [rel='category tag'], .entry-meta a[href*='/tipo/']"
    )
    if cat_links:
        category = cat_links[0].get_text(strip=True).capitalize()
    else:
        cm = re.search(r"^(Teatro|Dança|Música|Performance|Ópera)", text.strip(), re.IGNORECASE)
        if cm:
            category = cm.group(1).capitalize()

    # Datas e horários
    dates_label, date_start, date_end, schedule, sessions = _parse_dates_and_schedule(text, soup)

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
    dm = re.search(r"(\d+\s*min\.?)", text, re.IGNORECASE)
    if dm:
        duration = dm.group(1).strip()

    # Classificação etária
    age_rating = ""
    am = re.search(r"\b(M\s*/\s*\d+|Livre)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")

    # Ficha técnica
    technical_sheet = _parse_ficha(text)

    # Sinopse — versão completa dos <p>, fallback para og:description
    synopsis = ""
    for p in soup.select("main p, article p, .entry-content p"):
        t = p.get_text(strip=True)
        if len(t) > 60 and not re.match(
            r"^(Autor|Texto|Encenação|Tradução|Cenário|Figurinos|Com |Interpretação)",
            t, re.IGNORECASE,
        ):
            synopsis = t if not synopsis else synopsis + " " + t
            if len(synopsis) > 800:
                break
    if not synopsis:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            synopsis = og_desc.get("content", "").strip()

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        sessions,
        "schedule":        schedule,
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Parse de datas e horários
# ─────────────────────────────────────────────────────────────

def _parse_dates_and_schedule(text: str, soup) -> tuple[str, str, str, str, list]:
    """Devolve (dates_label, date_start, date_end, schedule, sessions[])."""
    from scrapers.utils import _WEEKDAYS_PT
    dates_label = date_start = date_end = schedule = ""
    sessions_out: list[dict] = []

    def _make_sessions(sess_tuples: list[tuple]) -> list[dict]:
        """Converte [(date, time), ...] para [{date, time, weekday}, ...]."""
        result = []
        for d, h in sorted(set(sess_tuples)):
            if not d:
                continue
            try:
                from datetime import date as _date
                wd = _WEEKDAYS_PT[_date.fromisoformat(d).weekday()]
            except Exception:
                wd = ""
            result.append({"date": d, "time": h, "weekday": wd})
        return result

    range_m = re.search(
        r"(\d{1,2})\s+([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)"
        r"\s*[–—-]\s*"
        r"(\d{1,2})\s+([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )

    anchor_year = None
    if range_m:
        yr_m = re.search(r"\d{4}", range_m.group(0))
        if yr_m:
            anchor_year = int(yr_m.group())
        else:
            from scrapers.utils import parse_date as _pd
            de_try = _pd(f"{range_m.group(3)} {range_m.group(4)}")
            if de_try:
                anchor_year = int(de_try[:4])

    session_pattern = re.compile(
        WEEKDAYS_PT + r"\s+(\d{1,2})\s+"
        r"([A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,})"
        r"(?:\s+(\d{4}))?\s*[•·]\s*(\d{1,2}:\d{2})",
        re.IGNORECASE,
    )

    from collections import Counter
    raw_sessions = [
        (m.group(1), m.group(2), m.group(3), m.group(4))
        for m in session_pattern.finditer(text)
    ]

    if not anchor_year and raw_sessions:
        from scrapers.utils import MONTHS as _M
        now         = datetime.now()
        year_counts: Counter = Counter()
        for d_num, mon_s, yr_s, _ in raw_sessions:
            mon = _M.get(mon_s.lower()) or _M.get(mon_s.lower()[:3])
            if not mon:
                continue
            if yr_s:
                year_counts[int(yr_s)] += 1
            elif mon > now.month or (mon == now.month and int(d_num) >= now.day):
                year_counts[now.year] += 1
            else:
                year_counts[now.year + 1] += 1
        if year_counts:
            anchor_year = year_counts.most_common(1)[0][0]

    from scrapers.utils import parse_date as _pd
    sessions = [
        (_pd(f"{d_num} {mon_s}{' '+yr_s if yr_s else ''}", force_year=anchor_year), hhmm)
        for d_num, mon_s, yr_s, hhmm in raw_sessions
    ]
    sessions = [(d, h) for d, h in sessions if d]

    if range_m:
        ds = _pd(f"{range_m.group(1)} {range_m.group(2)}", force_year=anchor_year)
        de = _pd(f"{range_m.group(3)} {range_m.group(4)}", force_year=anchor_year)
        if ds and de:
            dates_label = range_m.group(0).strip()
            date_start  = ds
            date_end    = de
            scheds      = [f"{m.group(1).capitalize()} {m.group(2)}" for m in SCHEDULE_LINE.finditer(text)]
            if scheds:
                schedule = " | ".join(scheds)
            extra = [(d, h) for d, h in sessions if d < ds]
            if extra:
                extra_labels = [f"{d} {h}" for d, h in sorted(extra)]
                schedule = (
                    "Sessões especiais: " + ", ".join(extra_labels)
                    + (" | " + schedule if schedule else "")
                ).strip(" |")
            sessions_out = _make_sessions(sessions)
            return dates_label, date_start, date_end, schedule, sessions_out

    if sessions:
        sessions.sort()
        date_start  = sessions[0][0]
        date_end    = sessions[-1][0]
        dates_label = f"{date_start} – {date_end}" if len(sessions) > 1 else date_start
        schedule    = " | ".join(f"{d} {h}" for d, h in sessions)
        sessions_out = _make_sessions(sessions)
        return dates_label, date_start, date_end, schedule, sessions_out

    m = re.search(
        r"[Aa]t[eé]\s+(\d{1,2}\s+(?:de\s+)?[A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )
    if m:
        dates_label = m.group(0).strip()
        date_end    = _pd(m.group(1))
        date_start  = date_end
        return dates_label, date_start, date_end, schedule, []

    m = re.search(
        r"[Aa]\s+[Pp]artir\s+[Dd]e\s+(\d{1,2}\s+[A-Za-z\u00e7\u00e3\u00e1\u00e9\u00ed\u00f3\u00fa]{3,}(?:\s+\d{4})?)",
        text,
    )
    if m:
        dates_label = m.group(0).strip()
        date_start  = date_end = _pd(m.group(1))
        return dates_label, date_start, date_end, schedule, []

    return dates_label, date_start, date_end, schedule, []


# ─────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(text: str) -> dict:
    ficha      = {}
    known_keys = [
        ("texto",         r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?\s*[:\s]\s*"),
        ("autor",         r"[Aa]utor[a]?\s*[:\s]\s*"),
        ("dramaturgia",   r"[Dd]ramaturgia\s*[:\s]\s*"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s*[:\s]\s*"),
        ("adaptação",     r"[Aa]dapta[çc][aã]o\s*[:\s]\s*"),
        ("cenário",       r"[Cc]en[aá]rio\s*[:\s]\s*"),
        ("cenografia",    r"[Cc]enografia\s*[:\s]\s*"),
        ("figurinos",     r"[Ff]igurinos?\s*[:\s]\s*"),
        ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\s]\s*|[Ii]lumina[çc][aã]o\s*[:\s]\s*"),
        ("som",           r"[Dd]esenho\s+de\s+[Ss]om\s*[:\s]\s*|[Ss]onoplastia\s*[:\s]\s*"),
        ("música",        r"[Mm][úu]sica\s*[:\s]\s*|[Cc]omposi[çc][aã]o\s*[:\s]\s*"),
        ("coreografia",   r"[Cc]oreografia(?:\s+e\s+movimento)?\s*[:\s]\s*"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s*[:\s]\s*"),
        ("coprodução",    r"[Cc]oprodu[çc][aã]o\s*[:\s]\s*"),
        ("direção",       r"[Dd]ire[çc][aã]o\s*[:\s]\s*"),
        ("ass_encenação", r"[Aa]ss(?:istente)?\.?\s+(?:de\s+)?[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s*[:\s]\s*"),
        ("elenco",        r"[Cc]om\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
    ]
    positions = []
    for key, pattern in known_keys:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        if key in ("interpretação", "elenco"):
            value = re.split(r"\s+(?:M/\d+|Todos |Uma |Com |Para |O |A |As |Os )", value)[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value
    return ficha
