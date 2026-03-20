"""
Scraper para o Cine-Teatro Avenida (Castelo Branco)
Fonte: https://www.ticketline.pt/salas/sala/139
"""

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from scrapers.utils import make_id, truncate_synopsis
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

THEATER = {
    "id":    "cine-teatro-avenida",
    "name":  "Cine-Teatro Avenida",
    "city":  "Castelo Branco",
    "url":   "https://www.ticketline.pt/salas/sala/139",
    "color": "#8B1A1A",
}

BASE_URL = "https://www.ticketline.pt"
VENUE_URL = "https://www.ticketline.pt/salas/sala/139"

# Mapeamento das categorias Ticketline → vocabulário controlado
CATEGORY_MAP = {
    "teatro": "Teatro",
    "dança": "Dança",
    "ópera": "Ópera",
    "opera": "Ópera",
    "musical": "Teatro Musical",
    "circo": "Circo",
    "mais novos": "Infanto-Juvenil",
    "infantil": "Infanto-Juvenil",
    "música": "Música",
    "musica": "Música",
    "concerto": "Música",
    "performance": "Performance",
    "stand up comedy": "Outro",
    "outros produtos": "Outro",
    "lazer": "Outro",
}

WEEKDAY_MAP = {
    "seg": "Seg", "ter": "Ter", "qua": "Qua",
    "qui": "Qui", "sex": "Sex", "sáb": "Sáb", "dom": "Dom",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PrimeiraPlateiaBot/1.0; "
        "+https://primeiraplateia.pt)"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9",
}


def _get(url: str, session: requests.Session) -> BeautifulSoup | None:
    """GET com tratamento de erros; devolve BeautifulSoup ou None."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as exc:
        logger.warning("Erro ao obter %s: %s", url, exc)
        return None


def _clean_text(text: str | None) -> str:
    """Remove HTML residual e normaliza espaços."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _map_category(raw: str) -> str | None:
    """Converte categoria Ticketline para vocabulário controlado."""
    if not raw:
        return None
    key = raw.strip().lower()
    # Tentativa directa
    if key in CATEGORY_MAP:
        return CATEGORY_MAP[key]
    # Tentativa via normalize_category (harmonizer)
    normalized = normalize_category(raw)
    if normalized:
        return normalized
    # Fallback parcial
    for k, v in CATEGORY_MAP.items():
        if k in key:
            return v
    return "Outro"


def _parse_price(text: str | None) -> dict:
    """Extrai price_info, price_min, price_max de uma string de preço."""
    result: dict = {}
    if not text:
        return result
    text = _clean_text(text)
    if re.search(r"entr[ae]da\s+livre|gratuito|free", text, re.I):
        result["price_info"] = "Entrada livre"
        result["price_min"] = 0.0
        return result
    prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", text)
    if prices:
        floats = [float(p.replace(",", ".")) for p in prices]
        result["price_min"] = min(floats)
        result["price_max"] = max(floats)
        result["price_info"] = text
    return result


def _parse_duration(text: str | None) -> dict:
    """Extrai duration e duration_min de 'aprox. 90 min' etc."""
    result: dict = {}
    if not text:
        return result
    m = re.search(r"(\d+)\s*min", text, re.I)
    if m:
        mins = int(m.group(1))
        result["duration_min"] = mins
        result["duration"] = f"{mins} min."
    return result


def _scrape_detail(url: str, session: requests.Session) -> dict:
    """
    Visita a página de detalhe do evento e extrai campos extra.
    Devolve um dict parcial (só os campos encontrados).
    """
    extra: dict = {}
    soup = _get(url, session)
    if soup is None:
        return extra

    # --- Sinopse ---
    synopsis_el = soup.select_one(
        'div.description, div.synopsis, div[itemprop="description"], '
        'div.text p, section.description p'
    )
    if synopsis_el:
        raw_syn = _clean_text(synopsis_el.get_text(" ", strip=True))
        if raw_syn:
            extra["synopsis"] = truncate_synopsis(raw_syn, 300)

    # --- Categoria ---
    cat_el = soup.select_one(
        'p.metadata.categories, span.category, '
        'a[href*="/pesquisa?category"]'
    )
    if cat_el:
        cat_raw = _clean_text(cat_el.get_text())
        if cat_raw:
            mapped = _map_category(cat_raw)
            if mapped:
                extra["category"] = mapped

    # --- Datas ---
    # Tentamos schema.org startDate / endDate
    start_el = soup.find(itemprop="startDate")
    if start_el:
        date_val = start_el.get("content") or start_el.get("datetime", "")
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_val)
        if m:
            extra["date_start"] = m.group(1)
    end_el = soup.find(itemprop="endDate")
    if end_el:
        date_val = end_el.get("content") or end_el.get("datetime", "")
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_val)
        if m:
            extra["date_end"] = m.group(1)

    # --- Sessões ---
    sessions = []
    for row in soup.select("ul.sessions li, table.sessions tr, div.session"):
        date_el = row.find(class_=re.compile(r"date|day"))
        time_el = row.find(class_=re.compile(r"time|hour|hora"))
        if not date_el:
            continue
        date_str = (date_el.get("data-date") or
                    date_el.get("content") or
                    date_el.get_text(strip=True))
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str or "")
        if not m:
            continue
        sess: dict = {"date": m.group(1)}
        if time_el:
            t = _clean_text(time_el.get_text())
            m2 = re.match(r"(\d{1,2}:\d{2})", t)
            sess["time"] = m2.group(1) if m2 else None
        else:
            sess["time"] = None
        # Dia da semana
        wd_el = row.find(class_=re.compile(r"weekday|week"))
        sess["weekday"] = (
            WEEKDAY_MAP.get(
                _clean_text(wd_el.get_text()).lower(), None
            )
            if wd_el else None
        )
        sessions.append(sess)
    if sessions:
        extra["sessions"] = sessions

    # --- Preço ---
    price_el = soup.select_one(
        'p.price, span.price, div.price, '
        'span[itemprop="price"], p.ticket_price'
    )
    if price_el:
        extra.update(_parse_price(_clean_text(price_el.get_text())))

    # --- Duração ---
    dur_el = soup.select_one(
        'p.duration, span.duration, li.duration'
    )
    if dur_el:
        extra.update(_parse_duration(_clean_text(dur_el.get_text())))

    # --- Classificação etária ---
    age_el = soup.select_one(
        'p.age_rating, span.age, li.age_rating, '
        'p.rating, span[class*="age"]'
    )
    if age_el:
        age_text = _clean_text(age_el.get_text())
        m = re.search(r"M[/\s]*(\d+)|todos\s+os\s+públicos", age_text, re.I)
        if m:
            if m.group(1):
                extra["age_rating"] = f"M/{m.group(1)}"
                extra["age_min"] = int(m.group(1))
            else:
                extra["age_rating"] = "Todos os públicos"
                extra["age_min"] = 0

    # --- URL de bilhetes ---
    # Normalmente o próprio URL Ticketline serve de ticket_url
    buy_el = soup.select_one(
        'a.buy_ticket, a[href*="/comprar/"], a.button.buy'
    )
    if buy_el and buy_el.get("href"):
        href = buy_el["href"]
        extra["ticket_url"] = (
            href if href.startswith("http") else BASE_URL + href
        )

    # --- Acessibilidade ---
    access: list[str] = []
    access_text = soup.get_text(" ", strip=True).lower()
    if "lgp" in access_text or "língua gestual" in access_text:
        access.append("LGP")
    if "audiodescrição" in access_text or "audiodescri" in access_text:
        access.append("Audiodescrição")
    if "legendagem" in access_text or "legendad" in access_text:
        access.append("Legendagem")
    if "cadeira de rodas" in access_text or "mobilidade reduzida" in access_text:
        access.append("Acesso cadeira de rodas")
    if "relaxed" in access_text:
        access.append("Relaxed performance")
    if access:
        extra["accessibility"] = access

    return extra


def scrape() -> list[dict]:
    """Devolve lista de eventos do Cine-Teatro Avenida."""
    events: list[dict] = []
    session = requests.Session()

    soup = _get(VENUE_URL, session)
    if soup is None:
        logger.error("Não foi possível obter a página principal: %s", VENUE_URL)
        return events

    event_items = soup.select("ul.events_list li[itemscope]")
    logger.info("Encontrados %d eventos na listagem", len(event_items))

    for item in event_items:
        try:
            # --- Campos da listagem ---
            link_el = item.select_one("a[itemprop='url'], a[href]")
            if not link_el:
                continue
            href = link_el.get("href", "")
            source_url = href if href.startswith("http") else BASE_URL + href

            # Título
            title_el = item.select_one("p.title[itemprop='name'], p.title")
            title = _clean_text(title_el.get_text()) if title_el else None
            if not title:
                logger.warning("Evento sem título em %s — a saltar", source_url)
                continue

            # Data início (data-date no div.date)
            date_el = item.select_one("div.date[data-date]")
            date_start = date_el["data-date"] if date_el else None
            if not date_start:
                # Fallback: itemprop startDate
                sd_el = item.find(itemprop="startDate")
                if sd_el:
                    date_start = (
                        sd_el.get("content") or
                        re.match(r"\d{4}-\d{2}-\d{2}",
                                 sd_el.get_text(strip=True) or "")
                        and re.match(r"\d{4}-\d{2}-\d{2}",
                                     sd_el.get_text(strip=True)).group(0)
                    )
            if not date_start:
                logger.warning("Evento '%s' sem data — a saltar", title)
                continue

            # Imagem (data-src-original no lazy-load img)
            img_el = item.select_one("div.thumb img[data-src-original]")
            image = None
            if img_el:
                img_url = img_el["data-src-original"]
                # Remover querystring de cache se presente
                img_url_clean = re.sub(r"\?rev=\d+", "", img_url)
                image = {
                    "url":     img_url_clean,
                    "credit":  None,
                    "source":  source_url,
                    "theater": THEATER["name"],
                }

            # Dia da semana (para a sessão da listagem)
            wd_el = item.select_one("p.weekday")
            weekday_raw = _clean_text(wd_el.get_text()).lower() if wd_el else None
            weekday = WEEKDAY_MAP.get(weekday_raw, None) if weekday_raw else None

            # Sessão mínima com o que temos da listagem
            listing_session = {
                "date":    date_start,
                "time":    None,       # Ticketline não expõe hora na listagem
                "weekday": weekday,
            }

            # --- Construção do evento base ---
            event: dict = {
                "id":         make_id(THEATER["id"], title),
                "title":      title,
                "theater":    THEATER["name"],
                "date_start": date_start,
                "source_url": source_url,
            }
            if image:
                event["image"] = image
            event["sessions"] = [listing_session]

            # --- Enriquecimento via página de detalhe ---
            time.sleep(0.5)
            detail = _scrape_detail(source_url, session)

            # Mesclar: campos do detalhe têm precedência, excepto
            # date_start (já temos da listagem e é fiável)
            for key, val in detail.items():
                if key == "date_start" and event.get("date_start"):
                    continue          # não sobrescrever data que já temos
                if key == "sessions" and val:
                    # Substituir a sessão mínima pelas sessões completas
                    event["sessions"] = val
                else:
                    event[key] = val

            # ticket_url: se não veio do detalhe, usar source_url
            if "ticket_url" not in event:
                event["ticket_url"] = source_url

            events.append(event)
            logger.debug("Evento adicionado: %s (%s)", title, date_start)

        except Exception as exc:
            logger.exception("Erro inesperado a processar evento: %s", exc)
            continue

    logger.info("Total de eventos recolhidos: %d", len(events))
    return events


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.DEBUG)
    result = scrape()
    print(json.dumps(result, ensure_ascii=False, indent=2))
