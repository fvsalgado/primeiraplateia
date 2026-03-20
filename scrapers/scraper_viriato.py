"""
Scraper: Teatro Viriato
Fonte: https://www.teatroviriato.com/pt/programacao
Cidade: Viseu

Estrutura do site (HTML estático):
  - Listagem: página única com todos os eventos do ano agrupados por mês.
  - Página de evento: /pt/programacao/espetaculo/<slug>

Estratégia de filtragem:
  1. Categorias TEATRO → aceitar directamente.
  2. Categorias fora de âmbito → rejeitar sem fazer pedido à página.
  3. "Cruzamento Disciplinar" → verificar ficha técnica na página do evento.
     Aceitar se tiver "Encenação" ou "Dramaturgia".
  4. Eventos com "CANCELADO" ou categoria "CMV" → ignorar.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, parse_date, log,
    HEADERS, can_scrape, truncate_synopsis, build_image_object, build_sessions,
)

# ─────────────────────────────────────────────────────────────
# Metadados do teatro — lidos pelo sync_scrapers.py
# para actualizar automaticamente theaters.json e scraper.py
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

# Categorias aceites directamente da listagem
_ACCEPT_CATEGORIES = {"teatro"}

# Categorias rejeitadas sem verificar a página do evento
_REJECT_CATEGORIES = {
    "dança", "música", "cinema", "exposição", "exposicao",
    "residência", "residencia", "oficina", "documentário", "documentario",
    "masterclass", "seminário", "seminario", "formação", "formacao",
    "open call", "visita guiada", "cine-concerto", "cmv",
    "conferência", "conferencia", "encontro", "pensamento",
    "circo contemporâneo", "circo contemporaneo",
}

# Categorias que requerem verificação na página do evento
_VERIFY_CATEGORIES = {"cruzamento disciplinar"}

# Palavras-chave na ficha técnica que confirmam que é teatro
_THEATER_FICHA_RE = re.compile(
    r"\b(encena[çc][aã]o|texto\s+e\s+encena[çc][aã]o|dramaturgia)\b",
    re.IGNORECASE,
)

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    candidates = _collect_candidates()
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos após filtragem da listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in candidates:
        ev = _scrape_event(item["url"], item["category_raw"], item.get("stub"))
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.4)

    log(f"[{THEATER_NAME}] {len(events)} eventos de teatro")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates() -> list[dict]:
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    for a in soup.find_all("a", href=re.compile(r"/pt/programacao/espetaculo/")):
        href = a.get("href", "")
        url  = href if href.startswith("http") else urljoin(BASE, href)
        if url in seen_urls:
            continue

        block_text = a.get_text(" ", strip=True)

        # Ignorar cancelados
        if "CANCELADO" in block_text.upper():
            continue

        # Ignorar CMV
        if re.search(r"\bCMV\b|Câmara Municipal", block_text, re.IGNORECASE):
            continue

        category_raw = _extract_category(a)
        category_key = category_raw.lower().strip()

        if category_key in _REJECT_CATEGORIES:
            continue
        if category_key not in _ACCEPT_CATEGORIES and category_key not in _VERIFY_CATEGORIES:
            continue

        seen_urls.add(url)
        candidates.append({
            "url":          url,
            "category_raw": category_raw,
            "stub":         _extract_stub(a, url),
        })

    return candidates


def _extract_category(a_tag) -> str:
    for cls in ["category", "tag", "tipo", "label"]:
        el = a_tag.find(class_=re.compile(cls, re.IGNORECASE))
        if el:
            return el.get_text(strip=True)
    texts = [t.strip() for t in a_tag.stripped_strings if t.strip()]
    for t in reversed(texts):
        if not re.match(r"^\d", t) and len(t) < 40:
            return t
    return ""


def _extract_stub(a_tag, url: str) -> dict:
    texts   = [t.strip() for t in a_tag.stripped_strings if t.strip()]
    title   = max((t for t in texts if not re.match(r"^\d", t) and len(t) > 3),
                  key=len, default="")
    img_tag = a_tag.find("img")
    img_url = ""
    if img_tag:
        src = img_tag.get("src") or img_tag.get("data-src") or ""
        img_url = src if src.startswith("http") else (urljoin(BASE, src) if src else "")
    date_text = next((t for t in texts if re.match(r"^\d{1,2}", t)), "")
    return {"title": title, "img_url": img_url, "date_text": date_text, "url": url}


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str, category_raw: str, stub: dict | None) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # Verificação para "Cruzamento Disciplinar"
    if category_raw.lower().strip() == "cruzamento disciplinar":
        if not _THEATER_FICHA_RE.search(full_text):
            title = _get_title(soup)
            log(f"[{THEATER_NAME}] '{title}' ignorado (Cruzamento Disciplinar sem encenação)")
            return None

    # Título
    title = _get_title(soup)
    if not title or len(title) < 3:
        return None

    # Datas
    dates_label, date_start, date_end = _parse_dates(soup, full_text)
    if not date_start and stub and stub.get("date_text"):
        dates_label, date_start, date_end = _parse_date_text(stub["date_text"])
    if not date_start:
        return None

    # Horário
    schedule = ""
    m_s = re.search(r"\b([a-záéíóúàãõç]+)\s+(\d{1,2}[h:]\d{2})\b", full_text, re.IGNORECASE)
    if m_s:
        schedule = f"{m_s.group(1).capitalize()} {m_s.group(2)}"

    # Duração
    duration = ""
    m_d = re.search(r"(\d+)\s*min\.?", full_text, re.IGNORECASE)
    if m_d:
        duration = f"{m_d.group(1)} min."

    # Sala
    sala = ""
    m_sala = re.search(r"LOCAL\s+([^\n]{3,60})", full_text, re.IGNORECASE)
    if m_sala:
        sala = m_sala.group(1).strip()

    # Preço
    price_info = ""
    m_p = re.search(
        r"(Entrada\s+livre|gratuito|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if m_p:
        price_info = m_p.group(1).strip()

    # Classificação etária
    age_rating = ""
    m_a = re.search(r"\+(\d+)\s*(?:Maiores de)?", full_text)
    if not m_a:
        m_a = re.search(r"M\s*/\s*(\d+)", full_text)
    if m_a:
        age_rating = f"+{m_a.group(1)}"

    # Imagem
    image   = None
    raw_img = ""
    og      = soup.find("meta", property="og:image")
    if og:
        raw_img = og.get("content", "")
    if not raw_img or not raw_img.startswith("http"):
        for img in soup.find_all("img", src=re.compile(r"/contents/galleryimage/")):
            src = img.get("src", "")
            if src:
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break
    if not raw_img and stub and stub.get("img_url"):
        raw_img = stub["img_url"]
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "bol.pt" in href or "bilhete" in href.lower():
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

    # Sinopse
    synopsis = _extract_synopsis(soup)

    # Ficha técnica
    technical_sheet = _parse_ficha(full_text)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        "Teatro",
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        build_sessions(date_start, date_end, schedule),
        "schedule":        schedule,
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _get_title(soup) -> str:
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""


def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    date_el  = soup.find("h2")
    date_src = date_el.get_text(" ", strip=True) if date_el else ""
    for src in [date_src, text]:
        result = _parse_date_text(src)
        if result[1]:
            return result
    return "", "", ""


def _parse_date_text(text: str) -> tuple[str, str, str]:
    if not text:
        return "", "", ""
    text = text.strip()

    # DD - DD MMM [YYYY]
    m = re.search(
        r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr_s = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr_s) if yr_s else _infer_year(n, int(d1))
            return f"{d1} - {d2} {mon_s} {y}", f"{y}-{n:02d}-{int(d1):02d}", f"{y}-{n:02d}-{int(d2):02d}"

    # DD MMM [YYYY] – DD MMM [YYYY]
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóú]{3,})(?:\s+(\d{4}))?\s*[-–]\s*(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            yr2 = int(y2) if y2 else _infer_year(n2, int(d2))
            yr1 = int(y1) if y1 else yr2
            return (
                f"{d1} {mo1} – {d2} {mo2} {yr2}",
                f"{yr1}-{n1:02d}-{int(d1):02d}",
                f"{yr2}-{n2:02d}-{int(d2):02d}",
            )

    # DD MMM [YYYY]
    m = re.search(r"(\d{1,2})\s+([a-záéíóú]{3,})\s*(\d{4})?", text, re.IGNORECASE)
    if m:
        d, mon_s, yr_s = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr_s) if yr_s else _infer_year(n, int(d))
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{d} {mon_s} {y}", ds, ds

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower()[:3])


def _infer_year(month: int, day: int) -> int:
    from datetime import datetime
    now = datetime.now()
    if month > now.month or (month == now.month and day >= now.day):
        return now.year
    return now.year + 1


def _extract_synopsis(soup) -> str:
    og_desc = soup.find("meta", property="og:description")
    og_text = og_desc.get("content", "").strip() if og_desc else ""
    synopsis = ""
    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) < 80:
            continue
        if re.match(
            r"^(\d+%|Mecenas|Sócios|Famílias|Profissionais|Funcionários|m/\s*\d+|Os descontos|Este site)",
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
        ("texto",         r"[Tt]exto\s+(?:e\s+[Ee]ncena[çc][aã]o\s+)?"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s+"),
        ("dramaturgia",   r"[Dd]ramaturgia\s+"),
        ("direção",       r"[Dd]ire[çc][aã]o\s+(?:artística\s+)?"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s+"),
        ("adaptação",     r"[Aa]dapta[çc][aã]o\s+"),
        ("cenografia",    r"[Cc]enografia\s+"),
        ("figurinos",     r"[Ff]igurinos?\s+"),
        ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s+|[Ii]lumina[çc][aã]o\s+"),
        ("som",           r"[Dd]esenho\s+de\s+[Ss]om\s+|[Ss]onoplastia\s+"),
        ("música",        r"[Mm][úu]sica\s+(?:original\s+)?"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s+"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s+(?:[Ee]xecutiva\s+)?"),
        ("coprodução",    r"[Cc]oprodu[çc][aã]o\s+"),
        ("fotografia",    r"[Ff]otografia(?:\s+e\s+identidade\s+gráfica)?\s+"),
    ]
    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\s+(?:Apoio|Agradecimentos|©)", value)[0]
        value      = value[:200].strip()
        if value and key not in ficha:
            ficha[key] = value
    return ficha
