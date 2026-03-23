"""
Scraper: Cine-Teatro Avenida (Castelo Branco)
Fonte: https://www.ticketline.pt/salas/sala/139

Estrutura do site (Ticketline, HTML):
  - Listagem: /salas/sala/139 — lista todos os próximos eventos do teatro.
    Cada item é um <li itemscope> dentro de ul.events_list, com:
      - <div class="date" data-date="YYYY-MM-DD"> — data fiável, sem JS
      - <p class="title" itemprop="name"> — título do evento
      - <img data-src-original="..."> — imagem em lazy-load
      - <p class="weekday"> — dia da semana abreviado
      - <a itemprop="url" href="/evento/slug"> — link para detalhe
  - Página de evento: /evento/<slug>
    Fonte de sinopse, categoria, preço, duração, idade, ticket_url.
    Ticketline bloqueia alguns pedidos — cada detalhe é tratado
    individualmente com try/except; falha não descarta o evento.

Notas:
  - Não há paginação: a listagem mostra todos os próximos eventos.
  - A categoria não aparece na listagem; vem apenas do detalhe.
  - ticket_url = source_url (a mesma página serve de bilheteira).
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id,
    log,
    HEADERS,
    can_scrape,
    truncate_synopsis,
    build_image_object,
)
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Metadados do teatro — lidos pelo sync_scrapers.py
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "cine-teatro-avenida",
    "name":        "Cine-Teatro Avenida",
    "short":       "C.T. Avenida",
    "color":       "#8B1A1A",
    "city":        "Castelo Branco",
    "address":     "Av. General Humberto Delgado, 6000-081 Castelo Branco",
    "site":        "https://www.ticketline.pt/salas/sala/139",
    "programacao": "https://www.ticketline.pt/salas/sala/139",
    "lat":         39.8237,
    "lng":         -7.4906,
    "salas":       ["Grande Sala"],
    "aliases": [
        "cine-teatro avenida",
        "cine teatro avenida",
        "cine-teatro avenida castelo branco",
    ],
    "description": (
        "O Cine-Teatro Avenida é o principal equipamento cultural de Castelo Branco, "
        "acolhendo espectáculos de teatro, dança, música e cinema. "
        "A sua programação é disponibilizada através da plataforma Ticketline."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://www.ticketline.pt"
VENUE_URL    = "https://www.ticketline.pt/salas/sala/139"

# Mapeamento de categorias Ticketline → vocabulário controlado
_CATEGORY_MAP = {
    "teatro":          "Teatro",
    "dança":           "Dança",
    "danca":           "Dança",
    "ópera":           "Ópera",
    "opera":           "Ópera",
    "musical":         "Teatro Musical",
    "circo":           "Circo",
    "mais novos":      "Infanto-Juvenil",
    "infantil":        "Infanto-Juvenil",
    "música":          "Música",
    "musica":          "Música",
    "concerto":        "Música",
    "performance":     "Performance",
    "stand up comedy": "Outro",
    "outros produtos": "Outro",
    "lazer":           "Outro",
}

_WEEKDAY_PT = {
    "seg": "Seg", "ter": "Ter", "qua": "Qua",
    "qui": "Qui", "sex": "Sex", "sáb": "Sáb",
    "sab": "Sáb", "dom": "Dom",
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    # Nota: o Ticketline é um agregador terceiro que bloqueia o User-Agent genérico.
    # Usamos headers de browser para este scraper específico.
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; PrimeiraPlateiaBot/1.0; +https://www.primeiraplateia.pt)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.ticketline.pt/",
    })

    listing = _get_soup(VENUE_URL, session)
    if listing is None:
        log(f"[{THEATER_NAME}] Erro ao obter listagem: {VENUE_URL}")
        return []

    items = listing.select("ul.events_list li[itemscope]")
    log(f"[{THEATER_NAME}] {len(items)} eventos na listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in items:
        try:
            stub = _parse_listing_item(item)
            if not stub:
                continue

            time.sleep(0.5)
            ev = _build_event(session, stub)
            if not ev:
                continue

            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)

        except Exception as e:
            log(f"[{THEATER_NAME}] Erro inesperado num item da listagem: {e}")

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Parsing da listagem
# ─────────────────────────────────────────────────────────────

def _parse_listing_item(item) -> dict | None:
    """
    Extrai stub do <li> da listagem.
    Devolve dict com title, date_start, weekday, img_url, source_url,
    ou None se faltar título ou data.
    """
    # URL do evento
    link = item.select_one("a[itemprop='url'], a[href*='/evento/']")
    if not link:
        return None
    href       = link.get("href", "")
    source_url = href if href.startswith("http") else urljoin(BASE, href)

    # Título
    title_el = item.select_one("p.title[itemprop='name'], p.title")
    title    = title_el.get_text(strip=True) if title_el else ""
    if not title:
        return None

    # Data — atributo data-date é fiável e independente de JS
    date_el    = item.select_one("div.date[data-date]")
    date_start = date_el["data-date"] if date_el else ""
    if not date_start:
        sd = item.find(itemprop="startDate")
        if sd:
            content = sd.get("content", "")
            m = re.match(r"(\d{4}-\d{2}-\d{2})", content)
            date_start = m.group(1) if m else ""
    if not date_start:
        return None

    # Dia da semana
    wd_el   = item.select_one("p.weekday")
    wd_raw  = wd_el.get_text(strip=True).lower() if wd_el else ""
    weekday = _WEEKDAY_PT.get(wd_raw)

    # Imagem (lazy-load — remover querystring de cache)
    img_el  = item.select_one("div.thumb img[data-src-original]")
    img_url = ""
    if img_el:
        img_url = re.sub(r"\?rev=\d+", "", img_el["data-src-original"])

    return {
        "title":      title,
        "date_start": date_start,
        "weekday":    weekday,
        "img_url":    img_url,
        "source_url": source_url,
    }


# ─────────────────────────────────────────────────────────────
# Construção do evento com enriquecimento via página de detalhe
# ─────────────────────────────────────────────────────────────

def _build_event(session: requests.Session, stub: dict) -> dict | None:
    """
    Cria evento com dados do stub + campos extra do detalhe.
    A falha no detalhe não descarta o evento — usa o stub.
    """
    title      = stub["title"]
    date_start = stub["date_start"]
    source_url = stub["source_url"]

    # Imagem base (da listagem)
    image = None
    if stub["img_url"]:
        image = {
            "url":     stub["img_url"],
            "credit":  None,
            "source":  source_url,
            "theater": THEATER_NAME,
        }

    # Sessão mínima (da listagem)
    sessions = [{
        "date":    date_start,
        "time":    None,
        "weekday": stub["weekday"],
    }]

    # Campos opcionais — preenchidos pelo detalhe
    category   = None
    synopsis   = ""
    ticket_url = source_url
    price_info = ""
    price_min  = None
    price_max  = None
    duration   = ""
    dur_min    = None
    age_rating = ""
    age_min    = None

    # ── Tentar página de detalhe ──────────────────────────────
    soup = _get_soup(source_url, session)
    if soup:
        full_text = soup.get_text(" ", strip=True)

        # Categoria
        cat_el = soup.select_one(
            "p.metadata.categories, span.category, "
            "a[href*='/pesquisa?category']"
        )
        if cat_el:
            category = _map_category(cat_el.get_text(strip=True))

        # Sinopse
        og = soup.find("meta", property="og:description")
        if og and og.get("content", "").strip():
            synopsis = truncate_synopsis(og["content"].strip())
        else:
            main = soup.find("main") or soup.find("article") or soup
            parts = []
            for p in main.find_all("p"):
                t = p.get_text(strip=True)
                if len(t) >= 60:
                    parts.append(t)
                    if sum(len(x) for x in parts) > 600:
                        break
            synopsis = truncate_synopsis(" ".join(parts))

        # Imagem — og:image tem maior resolução que o cartaz da listagem
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content", "").startswith("http"):
            image = build_image_object(og_img["content"], soup, THEATER_NAME, source_url)

        # Preço
        price_el = soup.select_one(
            "p.price, span.price, div.price, span[itemprop='price']"
        )
        price_text = price_el.get_text(strip=True) if price_el else ""
        if not price_text:
            pm = re.search(
                r"(Entrada\s+(?:livre|gratuita)|gratuito"
                r"|\d+(?:[,.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,.]\d+)?\s*€)?)",
                full_text, re.IGNORECASE,
            )
            if pm:
                price_text = pm.group(1).strip()
        if price_text:
            price_info, price_min, price_max = _parse_price(price_text)

        # Duração
        dur_el   = soup.select_one("p.duration, span.duration, li.duration")
        dur_text = dur_el.get_text(strip=True) if dur_el else ""
        if not dur_text:
            dm = re.search(r"(\d+)\s*min(?:utos?)?", full_text, re.IGNORECASE)
            if dm:
                dur_text = dm.group(0)
        if dur_text:
            duration, dur_min = _parse_duration(dur_text)

        # Classificação etária
        age_el   = soup.select_one("p.age_rating, span.age, li.age_rating, p.rating")
        age_text = age_el.get_text(strip=True) if age_el else ""
        if not age_text:
            am = re.search(
                r"\bM\s*/\s*(\d+)\b|todos\s+os\s+p[úu]blicos",
                full_text, re.IGNORECASE,
            )
            if am:
                age_text = am.group(0)
        if age_text:
            age_rating, age_min = _parse_age(age_text)

        # URL de bilhetes explícito
        buy = soup.select_one("a.buy_ticket, a[href*='/comprar/'], a.button.buy")
        if buy and buy.get("href"):
            href = buy["href"]
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)

    # ── Montar evento (omitir campos vazios) ──────────────────
    ev: dict = {
        "id":         make_id(SOURCE_SLUG, title),
        "title":      title,
        "theater":    THEATER_NAME,
        "date_start": date_start,
        "source_url": source_url,
        "ticket_url": ticket_url,
        "sessions":   sessions,
    }

    if category:
        ev["category"] = category
    if synopsis:
        ev["synopsis"] = synopsis
    if image:
        ev["image"] = image
    if price_info:
        ev["price_info"] = price_info
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max
    if duration:
        ev["duration"] = duration
    if dur_min is not None:
        ev["duration_min"] = dur_min
    if age_rating:
        ev["age_rating"] = age_rating
    if age_min is not None:
        ev["age_min"] = age_min

    return ev


# ─────────────────────────────────────────────────────────────
# Utilitários internos
# ─────────────────────────────────────────────────────────────

def _get_soup(url: str, session: requests.Session) -> BeautifulSoup | None:
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except requests.RequestException as e:
        log(f"[{THEATER_NAME}] Erro ao obter {url}: {e}")
        return None


def _map_category(raw: str) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    for k, v in _CATEGORY_MAP.items():
        if k in key:
            return v
    return normalize_category(raw) or None


def _parse_price(text: str) -> tuple[str, float | None, float | None]:
    text = text.strip()
    if re.search(r"entr[ae]da\s+livre|gratuito", text, re.I):
        return "Entrada livre", 0.0, None
    prices = [float(p.replace(",", ".")) for p in re.findall(r"(\d+(?:[,.]\d+)?)\s*€", text)]
    if prices:
        pmin = min(prices)
        pmax = max(prices) if len(prices) > 1 else None
        return text, pmin, pmax
    return text, None, None


def _parse_duration(text: str) -> tuple[str, int | None]:
    m = re.search(r"(\d+)\s*min", text, re.I)
    if m:
        mins = int(m.group(1))
        return f"{mins} min.", mins
    return "", None


def _parse_age(text: str) -> tuple[str, int | None]:
    if re.search(r"todos\s+os\s+p[úu]blicos", text, re.I):
        return "Todos os públicos", 0
    m = re.search(r"M\s*/\s*(\d+)", text, re.I)
    if m:
        n = int(m.group(1))
        return f"M/{n}", n
    return "", None


# ─────────────────────────────────────────────────────────────
# Execução directa (teste local)
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.DEBUG)
    result = scrape()
    print(json.dumps(result, ensure_ascii=False, indent=2))
