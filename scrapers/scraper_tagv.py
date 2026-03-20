"""
Scraper: TAGV — Teatro Académico Gil Vicente
Site: https://tagv.pt
Listagem: https://tagv.pt/agenda/?categoria=teatro
           https://tagv.pt/agenda/?categoria=danca
           https://tagv.pt/agenda/?categoria=performance
URLs de evento: /agenda/slug/

Estrutura do site:
  - A listagem é HTML estático (WordPress custom).
  - Cada item é um <li> com um <a href="/agenda/slug/"> que contém:
      · <span class="category"> (ou texto adjacente) com a categoria
      · A imagem de thumbnail via <img src="..."> ou background-image no CSS
  - Na secção de miniaturas (parte inferior) cada evento é um <article> ou <a>
    com a imagem e categoria visíveis.
  - A página de cada evento tem:
      · <h1> com o título
      · <div class="event-info"> ou similar com data, hora, preço, etc.
      · og:image para a imagem principal
      · Links para tagv.bol.pt para compra de bilhetes
"""

import re
import time
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    make_id,
    parse_date,
    log,
    HEADERS,
    can_scrape,
    build_image_object,
    build_sessions,
    truncate_synopsis,
)

BASE = "https://tagv.pt"

THEATER = {
    "id":          "tagv",
    "name":        "TAGV — Teatro Académico Gil Vicente",
    "short":       "TAGV",
    "color":       "#c0392b",
    "city":        "Coimbra",
    "address":     "Praça da República, 3000-343 Coimbra",
    "site":        "https://tagv.pt",
    "programacao": "https://tagv.pt/agenda/",
    "lat":         40.2093,
    "lng":         -8.4206,
    "salas":       ["Grande Auditório", "Sala Estúdio"],
    "aliases":     [
        "tagv", "teatro académico gil vicente", "teatro academico gil vicente",
        "gil vicente", "tagv coimbra",
    ],
    "description": (
        "O Teatro Académico Gil Vicente (TAGV) é o principal espaço de artes "
        "performativas de Coimbra, gerido pela Universidade de Coimbra. "
        "Apresenta programação regular de teatro, dança, performance, música, "
        "cinema e outras formas artísticas."
    ),
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

# Categorias da agenda do TAGV que nos interessam
THEATRE_CATEGORIES = {"teatro", "dança", "performance", "danca"}

# URLs de listagem a percorrer — uma por categoria aceite
_LISTING_URLS = [
    f"{BASE}/agenda/?categoria=teatro",
    f"{BASE}/agenda/?categoria=danca",
    f"{BASE}/agenda/?categoria=performance",
]

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    urls = _collect_urls()
    log(f"[{THEATER_NAME}] {len(urls)} URLs recolhidos")

    events: list[dict] = []
    seen_ids: set[str] = set()

    for url in sorted(urls):
        ev = _scrape_event(url)
        if ev:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
        time.sleep(0.3)

    log(f"[{THEATER_NAME}] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de URLs da listagem
# ─────────────────────────────────────────────────────────────

def _collect_urls() -> set[str]:
    """
    Percorre as listagens por categoria e recolhe URLs únicos de eventos.

    Estrutura HTML da listagem:
      Secção de lista (acima do dobrador):
        <li>
          <a class="categoria" href="...?categoria=teatro">Teatro</a>
          <a href="/agenda/slug/">...</a>
        </li>

      Secção de miniaturas (thumbnails):
        <a href="/agenda/slug/">
          <img src="...">
          <span>DD Mês</span>
          <h3>Título</h3>
        </a>
        + link de categoria adjacente

    Ambas as secções usam /agenda/slug/ como padrão de URL de evento.
    """
    urls: set[str] = set()

    for listing_url in _LISTING_URLS:
        try:
            r = requests.get(listing_url, headers=HEADERS, timeout=15)
            r.raise_for_status()
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro na listagem {listing_url}: {e}")
            continue

        soup = BeautifulSoup(r.text, "lxml")

        # Apanhar todos os links /agenda/<slug>/ que não sejam de filtro
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Excluir links de filtro/navegação (contêm ?categoria= ou ?temporada=)
            if "?" in href:
                continue
            # Aceitar apenas /agenda/<slug>/ com slug não vazio
            m = re.match(r"^(?:https?://tagv\.pt)?/agenda/([^/]+)/?$", href)
            if not m:
                continue
            slug = m.group(1)
            # Excluir slugs de navegação/arquivo
            if slug in {"", "agenda"}:
                continue
            full = BASE + "/agenda/" + slug + "/"
            urls.add(full)

        time.sleep(0.3)

    return urls


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text(" ")

    # ── Título ────────────────────────────────────────────────
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # ── Categoria ─────────────────────────────────────────────
    # O TAGV mostra a categoria num link acima do título, ex: <a class="category">Teatro</a>
    # ou como breadcrumb.
    category = _extract_category(soup)

    # Filtro: só importar categorias de teatro/dança/performance
    if category.lower() not in THEATRE_CATEGORIES and category.lower() not in {
        "teatro", "dança", "danca", "performance", "circo",
    }:
        log(f"[{THEATER_NAME}] Ignorado (categoria '{category}'): {url}")
        return None

    # ── Datas ─────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates(soup, text)
    if not date_start:
        return None

    # ── Horário ───────────────────────────────────────────────
    schedule = _extract_schedule(soup, text)

    # ── Imagem ────────────────────────────────────────────────
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, THEATER_NAME, url)
    if not image:
        # Fallback: primeira imagem no conteúdo principal
        for img in soup.select("main img, article img, .entry-content img"):
            src = img.get("src", "")
            if src and src.startswith("http") and "logo" not in src.lower():
                image = build_image_object(src, soup, THEATER_NAME, url)
                break

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "tagv.bol.pt" in href or "bol.pt/Comprar" in href:
            ticket_url = href
            break
    if not ticket_url:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(x in href for x in ["ticketline", "eventbrite", "bol.pt"]):
                ticket_url = href
                break

    # ── Sinopse ───────────────────────────────────────────────
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()
    if not synopsis:
        for p in soup.select("main p, article p, .entry-content p, .event-description p"):
            t = p.get_text(strip=True)
            if len(t) > 60 and not re.match(
                r"^(Autor|Texto|Encenação|Tradução|Bilhetes|Data|Hora|Preço|Local|Duração)",
                t, re.IGNORECASE,
            ):
                synopsis = t if not synopsis else synopsis + " " + t
                if len(synopsis) > 800:
                    break

    # ── Preço ─────────────────────────────────────────────────
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

    # ── Duração ───────────────────────────────────────────────
    duration = ""
    dm = re.search(r"(\d+\s*min\.?|\d+h\d*)", text, re.IGNORECASE)
    if dm:
        duration = dm.group(1).strip()

    # ── Classificação etária ──────────────────────────────────
    age_rating = ""
    am = re.search(r"\b(M\s*/\s*\d+|Livre|\+\d+)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")

    # ── Acessibilidade ────────────────────────────────────────
    accessibility = ""
    ac_m = re.search(
        r"(Audiodescri[çc][aã]o|LGP|L[íi]ngua\s+Gestual|Legendas\s+em\s+[Ii]ngl[eê]s)",
        text, re.IGNORECASE,
    )
    if ac_m:
        accessibility = ac_m.group(1)

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_ficha(text)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
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
        "accessibility":   accessibility,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _extract_category(soup) -> str:
    """
    Tenta extrair a categoria a partir de vários padrões do TAGV:
      1. <a> com classe ou href com ?categoria= dentro do conteúdo principal
      2. <span class="category"> ou similar
      3. Breadcrumb
    Devolve "Teatro" como fallback.
    """
    # Padrão 1: link de categoria com ?categoria= (presente na listagem e em eventos)
    for a in soup.find_all("a", href=re.compile(r"\?categoria=")):
        cat_text = a.get_text(strip=True)
        if cat_text:
            return cat_text.capitalize()

    # Padrão 2: span.category
    cat_el = soup.select_one("span.category, .category a, [class*='category']")
    if cat_el:
        t = cat_el.get_text(strip=True)
        if t:
            return t.capitalize()

    # Padrão 3: breadcrumb
    bc = soup.select_one(".breadcrumb, [class*='breadcrumb']")
    if bc:
        m = re.search(
            r"\b(teatro|dança|danca|m[uú]sica|cinema|performance|circo|poesia|humor)\b",
            bc.get_text(" "), re.IGNORECASE,
        )
        if m:
            return m.group(1).capitalize()

    return "Teatro"


def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    """
    Estratégia de extracção de datas para o TAGV.

    O site apresenta as datas de várias formas:
      - Na listagem: "DD Jan", "DD Fev", etc. (dia + mês abreviado)
      - Na página de evento: texto livre com "DD de Mês de YYYY"
        ou "DD Mês YYYY" ou "DD – DD Mês YYYY"
      - Possível bloco <time datetime="YYYY-MM-DD">

    Tenta por ordem:
      1. <time datetime="...">
      2. DD Mês YYYY (texto livre, com/sem "de")
      3. DD Mês (sem ano — infere o ano)
    """
    # 1. <time datetime="YYYY-MM-DD">
    time_tags = soup.find_all("time", attrs={"datetime": True})
    dates_found = []
    for t in time_tags:
        dt = t["datetime"]
        if re.match(r"^\d{4}-\d{2}-\d{2}", dt):
            dates_found.append(dt[:10])
    if dates_found:
        dates_found.sort()
        date_start = dates_found[0]
        date_end   = dates_found[-1]
        dates_label = (
            f"{date_start} – {date_end}" if date_end != date_start else date_start
        )
        return dates_label, date_start, date_end

    # 2. DD Mês YYYY com intervalo: "DD – DD Mês YYYY" ou "DD Mês – DD Mês YYYY"
    m = re.search(
        r"(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})(?:\s+(?:de\s+)?(\d{4}))?"
        r"\s*[–—\-]+\s*"
        r"(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d1, mo1, y1_opt, d2, mo2, y2 = m.groups()
        n1 = _mon(mo1)
        n2 = _mon(mo2)
        if n1 and n2:
            y1 = y1_opt or y2
            dates_label = f"{d1} {mo1} – {d2} {mo2} {y2}"
            date_start  = f"{y1}-{n1:02d}-{int(d1):02d}"
            date_end    = f"{y2}-{n2:02d}-{int(d2):02d}"
            return dates_label, date_start, date_end

    # 3. DD – DD Mês YYYY (mesmo mês)
    m = re.search(
        r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+"
        r"(?:de\s+)?([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d1, d2, mo, y = m.groups()
        n = _mon(mo)
        if n:
            dates_label = f"{d1} – {d2} {mo} {y}"
            date_start  = f"{y}-{n:02d}-{int(d1):02d}"
            date_end    = f"{y}-{n:02d}-{int(d2):02d}"
            return dates_label, date_start, date_end

    # 4. DD Mês YYYY (data única)
    m = re.search(
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d, mo, y = m.groups()
        n = _mon(mo)
        if n:
            dates_label = f"{d} {mo} {y}"
            date_start  = f"{y}-{n:02d}-{int(d):02d}"
            date_end    = date_start
            return dates_label, date_start, date_end

    # 5. Fallback: parse_date genérico
    d = parse_date(text[:200])
    if d:
        return d, d, d

    return "", "", ""


def _extract_schedule(soup, text: str) -> str:
    """
    Tenta extrair o horário (hora de início) da página do TAGV.
    Formatos encontrados: "21h30", "18h30", "16h00", "10h30", "14h30"
    Pode haver múltiplas horas (sessões diferentes no mesmo dia).
    """
    # Procurar todas as ocorrências de HHhMM no texto principal
    times = re.findall(r"\b(\d{1,2})[h:](\d{2})\b", text)
    valid = []
    seen  = set()
    for hh_s, mm_s in times:
        hh = int(hh_s)
        mm = int(mm_s)
        if 8 <= hh <= 23:
            t = f"{hh:02d}:{mm:02d}"
            if t not in seen:
                seen.add(t)
                valid.append(t)
    if valid:
        return " | ".join(valid[:4])  # máximo 4 horas distintas
    return ""


def _parse_ficha(text: str) -> dict:
    """
    Extrai ficha técnica do texto livre da página de evento.
    Segue os mesmos padrões dos outros scrapers do projecto.
    """
    ficha      = {}
    known_keys = [
        ("texto",         r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?\s*[:\s]\s*"),
        ("autor",         r"[Aa]utor[a]?\s*[:\s]\s*"),
        ("dramaturgia",   r"[Dd]ramaturgia\s*[:\s]\s*"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
        ("direção",       r"[Dd]ire[çc][aã]o\s*[:\s]\s*"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s*[:\s]\s*"),
        ("adaptação",     r"[Aa]dapta[çc][aã]o\s*[:\s]\s*"),
        ("cenografia",    r"[Cc]enografia\s*[:\s]\s*"),
        ("figurinos",     r"[Ff]igurinos?\s*[:\s]\s*"),
        ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\s]\s*|[Ii]lumina[çc][aã]o\s*[:\s]\s*"),
        ("som",           r"[Dd]esenho\s+de\s+[Ss]om\s*[:\s]\s*|[Ss]onoplastia\s*[:\s]\s*"),
        ("música",        r"[Mm][úu]sica\s*[:\s]\s*|[Cc]omposi[çc][aã]o\s*[:\s]\s*"),
        ("coreografia",   r"[Cc]oreografia\s*[:\s]\s*"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s*[:\s]\s*"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s*[:\s]\s*"),
        ("coprodução",    r"[Cc]oprodu[çc][aã]o\s*[:\s]\s*"),
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
        value      = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value
    return ficha


def _mon(s: str) -> int | None:
    """Converte nome de mês PT para número."""
    key = s.lower().strip()
    if key in _PT_MONTHS:
        return _PT_MONTHS[key]
    # Tentar com os 3 primeiros caracteres
    return _PT_MONTHS.get(key[:3])
