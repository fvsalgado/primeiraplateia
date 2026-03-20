"""
Scraper: Culturgest
Site: https://www.culturgest.pt

O site carrega a listagem por JavaScript — o HTML da listagem está vazio.
Estratégia: crawl progressivo a partir de seeds, descobrindo novos eventos
via a secção "Próximos Eventos" (HTML estático em cada página de evento).

Resiliência de seeds:
  1. Tentar carregar a página de listagem e extrair URLs directamente
  2. Fallback para seeds hardcoded (eventos recentes conhecidos)
  3. Crawl progressivo a partir de qualquer seed válida
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, parse_date, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)

BASE = "https://www.culturgest.pt"

THEATER = {
    "id":          "culturgest",
    "name":        "Culturgest",
    "short":       "Culturgest",
    "color":       "#6d4c97",
    "city":        "Lisboa",
    "address":     "Rua Arco do Cego, 50, 1000-020 Lisboa",
    "site":        "https://www.culturgest.pt",
    "programacao": "https://www.culturgest.pt/pt/programacao/",
    "lat":         38.7316,
    "lng":         -9.1387,
    "salas":       ["Grande Auditório", "Pequeno Auditório"],
    "aliases":     ["culturgest", "fundação caixa geral de depósitos", "cgd culturgest"],
    "description": "A Culturgest — Fundação Caixa Geral de Depósitos dedica-se à criação contemporânea, com programação regular de teatro, dança, música e artes visuais em Lisboa e Porto.",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

# URL de listagem (carregada por JS — usado como ponto de partida para extrair seeds)
LISTING_URL  = f"{BASE}/pt/programacao/por-evento/?typology=1"

# Seeds hardcoded — fallback quando a listagem está vazia por JS
# Actualizar periodicamente com eventos recentes para garantir crawl contínuo
SEEDS_FALLBACK = [
    "https://www.culturgest.pt/pt/programacao/catarina-rolo-salgueiro-e-isabel-costa-os-possessos-burn-burn-burn-2026/",
    "https://www.culturgest.pt/pt/programacao/alex-cassal-ma-criacao-hotel-paradoxo/",
    "https://www.culturgest.pt/pt/programacao/mala-voadora-polo-norte/",
]

_PT_MONTHS_ABBR = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

# Slugs que não são eventos individuais — excluir do crawl
_SKIP_SLUGS = {
    "por-evento", "agenda-pdf", "archive", "schedule",
    "por-tipo", "participacao", "convite", "open-call",
    "temporada-2025-26", "temporada-2024-25", "concluido",
}

# Tipologias aceites — li.type[data-property="typology"] data-id na página do evento
# 1=Teatro · 2=Dança · 4=Artes Visuais · 5=Cinema · 8=Música
_THEATRE_TYPOLOGY_IDS   = {"1"}                                    # Teatro
_THEATRE_TYPOLOGY_NAMES = {"teatro", "dança", "performance", "circo"}  # fallback por nome


def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    seeds      = _collect_seeds()
    event_urls = _discover_urls(seeds)
    log(f"[{THEATER_NAME}] {len(event_urls)} URLs descobertos")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for url in sorted(event_urls):
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
# Recolha de seeds — mais resiliente que lista fixa
# ─────────────────────────────────────────────────────────────

def _collect_seeds() -> set[str]:
    """
    Tenta extrair seeds directamente da página de listagem.
    Se a listagem estiver vazia (carregada por JS), usa o fallback hardcoded.
    """
    seeds = set()
    try:
        r = requests.get(LISTING_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            full = a["href"] if a["href"].startswith("http") else urljoin(BASE, a["href"])
            if _is_event_url(full):
                seeds.add(full.rstrip("/") + "/")
    except Exception as e:
        log(f"[{THEATER_NAME}] Listagem inacessível ({e}) — a usar seeds de fallback")

    if not seeds:
        log(f"[{THEATER_NAME}] Listagem vazia (JS) — a usar seeds de fallback")
        seeds = set(SEEDS_FALLBACK)
    else:
        log(f"[{THEATER_NAME}] {len(seeds)} seeds extraídas da listagem")
        # Adicionar sempre as seeds hardcoded como seguro adicional
        seeds.update(SEEDS_FALLBACK)

    return seeds


# ─────────────────────────────────────────────────────────────
# Descoberta de URLs por crawl
# ─────────────────────────────────────────────────────────────

def _discover_urls(seeds: set[str]) -> set[str]:
    """Crawl progressivo a partir das seeds via links na página."""
    found:   set[str] = set()
    queue:   set[str] = set(seeds)
    visited: set[str] = set()

    while queue:
        url = queue.pop()
        if url in visited:
            continue
        visited.add(url)
        links = _event_links_from_page(url)
        if links:
            found.add(url)
            for lnk in links:
                if lnk not in visited:
                    queue.add(lnk)
        time.sleep(0.2)

    # Garantir que as seeds originais são incluídas
    found.update(seeds)
    return found


def _event_links_from_page(url: str) -> set[str]:
    """Extrai links de eventos de uma página."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Inacessível {url}: {e}")
        return set()

    soup  = BeautifulSoup(r.text, "lxml")
    links = set()
    for a in soup.find_all("a", href=True):
        full = a["href"] if a["href"].startswith("http") else urljoin(BASE, a["href"])
        if _is_event_url(full):
            links.add(full.rstrip("/") + "/")
    return links


def _is_event_url(url: str) -> bool:
    if not url.startswith(BASE):
        return False
    path  = url.replace(BASE, "").strip("/")
    if not path.startswith("pt/programacao/"):
        return False
    parts = [p for p in path.split("/") if p]
    if len(parts) < 3:
        return False
    return parts[2] not in _SKIP_SLUGS


# ─────────────────────────────────────────────────────────────
# Scraping de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ")

    # Título
    title_el = soup.find("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Subtítulo (segundo h1 ou h2 imediato)
    all_h1 = soup.find_all("h1")
    if len(all_h1) > 1:
        sub = all_h1[1].get_text(strip=True)
        if sub and sub != title:
            title = f"{title} — {sub}"

    # Imagem
    image = None
    og    = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, THEATER_NAME, url)
    if not image:
        img = soup.find("img", src=re.compile(r"/media/filer_public"))
        if img:
            image = build_image_object(urljoin(BASE, img["src"]), soup, THEATER_NAME, url)

    # Datas
    dates_label, date_start, date_end = _parse_dates(soup, full_text)

    # Categoria + filtro de tipologia
    # li.type[data-property="typology"] tem data-id e texto da categoria.
    # Rejeitar imediatamente se a tipologia não for teatro —
    # evita importar eventos descobertos via crawl noutras categorias.
    category    = "Teatro"
    typology_id = None
    typ_el = soup.select_one('li.type[data-property="typology"]')
    if typ_el:
        typology_id = typ_el.get("data-id", "")
        category    = typ_el.get_text(strip=True) or "Teatro"

    # Fallback: ler do link de filtro na página (presente em eventos descobertos via crawl)
    if not typology_id:
        for a in soup.select("ul li a[href*='typology']"):
            txt = a.get_text(strip=True)
            if txt:
                category = txt
                m = re.search(r"typology=(\d+)", a.get("href", ""))
                if m:
                    typology_id = m.group(1)
                break

    # Filtro: rejeitar se não for tipologia de teatro/performance
    is_theatre = (
        typology_id in _THEATRE_TYPOLOGY_IDS
        or category.lower() in _THEATRE_TYPOLOGY_NAMES
    )
    if typology_id and not is_theatre:
        log(f"[{THEATER_NAME}] Ignorado (tipologia '{category}'): {url}")
        return None

    # Sinopse
    synopsis   = ""
    main_el    = soup.find("main") or soup.find("article") or soup
    long_paras = [p.get_text(" ", strip=True) for p in main_el.find_all("p") if len(p.get_text(" ", strip=True)) > 80]
    if long_paras:
        synopsis = " ".join(long_paras)[:2000]

    # Preço
    price_info = ""
    m_p = re.search(r"(\d+\s?€(?:\s?[–\-]\s?\d+\s?€)?|[Ee]ntrada gratuita|gratuito)", full_text)
    if m_p:
        price_info = m_p.group(1)

    # Duração
    duration = ""
    m_d = re.search(r"\bDura[çc][aã]o\s+([\w\s]+?)(?:\n|$|[A-Z])", full_text)
    if not m_d:
        m_d = re.search(r"(\d+h\d*|\d+\s?min(?:utos)?)", full_text, re.IGNORECASE)
    if m_d:
        duration = m_d.group(1).strip()

    # Classificação etária
    age_rating = ""
    m_a = re.search(r"M/\d+|Maiores de \d+", full_text, re.IGNORECASE)
    if m_a:
        age_rating = m_a.group(0)

    # Horário
    schedule = ""
    m_s = re.search(r"\b(\d{1,2}[h:]\d{2})\b", full_text)
    if m_s:
        schedule = m_s.group(1)

    # Acessibilidade
    accessibility = ""
    m_ac = re.search(
        r"(Audiodes[ck]ri[çc][aã]o|LGP|Língua Gestual|legendas em inglês)",
        full_text, re.IGNORECASE,
    )
    if m_ac:
        accessibility = m_ac.group(1)

    # Bilhetes
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href   = a["href"]
        text_a = a.get_text(strip=True).lower()
        if any(x in href.lower() for x in ["ticketline", "bol.pt", "bilhete", "comprar"]) \
                or "comprar bilhete" in text_a:
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

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
        "technical_sheet": {},
    }


# ─────────────────────────────────────────────────────────────
# Parse de datas
# ─────────────────────────────────────────────────────────────

def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    date_blocks = [
        el.strip()
        for el in soup.find_all(string=re.compile(r"\b\d{1,2}\s+[A-Za-z]{3,4}\s+\d{4}\b"))
    ]
    sources = date_blocks + [text] if date_blocks else [text]

    for src in sources:
        # DD MMM [YYYY] – DD MMM YYYY  (meses distintos)
        m = re.search(
            r"(\d{1,2})\s+([A-Za-z]{3,})(?:\s+(\d{4}))?"
            r"\s*[–—\-]+\s*"
            r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})",
            src,
        )
        if m:
            d1, mo1, y1_opt, d2, mo2, y2 = m.groups()
            n1 = _mon(mo1)
            n2 = _mon(mo2)
            if n1 and n2 and mo1.lower()[:3] != mo2.lower()[:3]:
                y1          = y1_opt or y2
                dates_label = f"{d1} {mo1} – {d2} {mo2} {y2}"
                date_start  = f"{y1}-{n1:02d}-{int(d1):02d}"
                date_end    = f"{y2}-{n2:02d}-{int(d2):02d}"
                return dates_label, date_start, date_end

        # DD–DD MMM YYYY  (mesmo mês)
        m = re.search(r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", src)
        if m:
            d1, d2, mo, y = m.groups()
            n = _mon(mo)
            if n:
                dates_label = f"{d1}–{d2} {mo} {y}"
                date_start  = f"{y}-{n:02d}-{int(d1):02d}"
                date_end    = f"{y}-{n:02d}-{int(d2):02d}"
                return dates_label, date_start, date_end

        # DD MMM YYYY  (data única)
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", src)
        if m:
            d, mo, y = m.groups()
            n = _mon(mo)
            if n:
                dates_label = f"{d} {mo} {y}"
                date_start  = f"{y}-{n:02d}-{int(d):02d}"
                date_end    = date_start
                return dates_label, date_start, date_end

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS_ABBR.get(s.lower()[:3])
