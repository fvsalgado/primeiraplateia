"""
Scraper: São Luiz Teatro Municipal
URL listagem: https://www.teatrosaoluiz.pt/programacao/
URLs eventos:  /espetaculo/slug/
"""
import re
import time
import requests
from bs4 import BeautifulSoup
from scrapers.utils import (
    make_id, parse_date_range, parse_date, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)

BASE      = "https://www.teatrosaoluiz.pt"
AGENDA    = f"{BASE}/programacao/"
IMG_DOMAIN = "www.teatrosaoluiz.pt"

THEATER = {
    "id":          "saoluiz",
    "name":        "São Luiz Teatro Municipal",
    "short":       "São Luiz",
    "color":       "#1a73e8",
    "city":        "Lisboa",
    "address":     "Rua António Maria Cardoso, 38, 1200-027 Lisboa",
    "site":        "https://www.teatrosaoluiz.pt",
    "programacao": "https://www.teatrosaoluiz.pt/programacao/",
    "lat":         38.7098,
    "lng":         -9.1421,
    "salas":       ["Grande Sala", "Sala Estúdio"],
    "aliases":     ["são luiz", "sao luiz", "teatro são luiz", "teatro municipal são luiz", "saoluiz"],
    "description": "O São Luiz Teatro Municipal é um dos mais emblemáticos teatros de Lisboa, com programação diversa de teatro, dança e performance. Situado no Chiado.",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

# Categorias aceites — <span class="category"> nos cards da listagem
THEATRE_CATEGORIES = {"teatro", "circo", "performance", "dança"}


def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen, events = set(), []

    for a in soup.find_all("a", href=re.compile(r"/espetaculo/")):
        cat = _card_category(a)
        if cat and cat not in THEATRE_CATEGORIES:
            log(f"[São Luiz] Ignorado (categoria '{cat}'): {a['href']}")
            continue

        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)
        ev = _scrape_event(full)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[São Luiz] {len(events)} eventos")
    return events


def _card_category(a_tag) -> str:
    """
    Lê a categoria de um link /espetaculo/ na listagem.
    Há duas zonas na página com estruturas distintas:

      1. Cards  (div.card.event-item):
         <a href="...">
           <span class="category">teatro</span>  ← dentro do <a>
           <span class="title">...</span>
         </a>

      2. Calendário inline (div.calendar-day):
         <span class="category to-lower">Teatro</span>  ← FORA do <a>, irmão
         <span class="title"><a href="...">...</a></span>

    Devolve a categoria em minúsculas, ou "" se não encontrada.
    """
    # Caso 1 — span.category dentro do próprio <a>
    inner = a_tag.select_one("span.category")
    if inner:
        return inner.get_text(strip=True).lower()

    # Caso 2 — span.category irmão (estrutura do calendário)
    parent = a_tag.parent  # <span class="title">
    if parent:
        container = parent.parent  # <div class="col-12"> ou similar
        if container:
            sibling = container.select_one("span.category")
            if sibling:
                return sibling.get_text(strip=True).lower()

    return ""


def _scrape_event(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    raw  = r.text

    # Título
    title_el = soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # Subtítulo
    subtitle = ""
    sub_el   = title_el.find_next_sibling()
    if sub_el:
        sub = sub_el.get_text(strip=True)
        if sub and len(sub) < 120 and not re.match(
            r"^(©|COMPRAR|BILHETE|DATAS|LOCAL|DURA|PRE[ÇC]O|CLASSI|ACESSI)",
            sub, re.IGNORECASE,
        ):
            subtitle = sub

    # Categoria
    category = "Teatro"
    cat_el   = soup.select_one(".breadcrumbs a, [class*='breadcrumb'] a, [class*='categoria'] a")
    if not cat_el:
        bc = soup.select_one(".breadcrumbs, [class*='breadcrumb']")
        if bc:
            cat_m = re.search(
                r"\b(teatro|m[uú]sica|dan[çc]a|circo|performance|"
                r"pensamento|exposi[çc][aã]o|visita|espa[çc]o p[uú]blico)\b",
                bc.get_text(" "), re.IGNORECASE,
            )
            if cat_m:
                category = cat_m.group(1).capitalize()
    else:
        category = cat_el.get_text(strip=True).capitalize()

    # Campos estruturados
    fields        = _parse_subtitle_fields(soup)
    dates_label   = fields.get("datas_label", "")
    schedule      = fields.get("schedule", "")
    sala          = fields.get("local", "")
    duration      = fields.get("duracao", "")
    price_info    = fields.get("preco", "")
    age_rating    = fields.get("classificacao", "")
    accessibility = fields.get("acessibilidade", "")

    # Datas
    date_start, date_end = _parse_dates_from_field(dates_label)

    # Imagem
    image = None
    raw_img = _get_image_url(soup, raw)
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "saoluiz.bol.pt" in href or "bol.pt/Comprar" in href or "ticketline" in href:
            ticket_url = href
            break
    if not ticket_url:
        m = re.search(r"href='(https?://[^']*(?:saoluiz\.bol\.pt|bol\.pt/Comprar)[^']*)'", raw)
        if m:
            ticket_url = m.group(1)

    # Sinopse
    synopsis = ""
    desc_el  = soup.select_one(".event-description, section.event-description")
    if desc_el:
        for p in desc_el.select("p"):
            t = p.get_text(strip=True)
            if len(t) > 60:
                synopsis += (" " if synopsis else "") + t
                if len(synopsis) > 1000:
                    break
    if not synopsis:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            synopsis = og_desc.get("content", "").strip()

    # Ficha técnica
    technical_sheet = _parse_ficha(soup)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "subtitle":        subtitle,
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
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Parsing dos campos estruturados
# ─────────────────────────────────────────────────────────────

def _parse_subtitle_fields(soup) -> dict:
    result    = {}
    LABEL_MAP = {
        "DATAS E HORÁRIOS": "datas_label",
        "DATAS":            "datas_label",
        "LOCAL":            "local",
        "DURAÇÃO":          "duracao",
        "PREÇO":            "preco",
        "CLASSIFICAÇÃO":    "classificacao",
        "ACESSIBILIDADE":   "acessibilidade",
    }
    for span in soup.select("span.subtitle"):
        label_raw = span.get_text(strip=True).upper()
        key       = LABEL_MAP.get(label_raw)
        if not key:
            continue
        container = span.parent
        if not container:
            continue
        full_text = container.get_text("\n", strip=True)
        value     = full_text[len(span.get_text(strip=True)):].strip()
        value     = re.sub(r"\n{3,}", "\n\n", value).strip()
        if value:
            result[key] = value

    if "datas_label" in result:
        lines = [l.strip() for l in result["datas_label"].splitlines() if l.strip()]
        if lines:
            result["datas_label"] = lines[0]
            if len(lines) > 1:
                result["schedule"] = "\n".join(lines[1:])

    return result


def _parse_dates_from_field(dates_label: str) -> tuple[str, str]:
    if not dates_label:
        return "", ""
    date_start, date_end = parse_date_range(dates_label)
    if date_start:
        return date_start, date_end
    d = parse_date(dates_label)
    return d, d


def _parse_ficha(soup) -> dict:
    ficha   = {}
    tech_el = soup.select_one(".event-tech-details")
    if not tech_el:
        return ficha

    text  = tech_el.get_text(" ")
    spans = tech_el.select("span.subtitle")
    if not spans:
        return ficha

    positions = []
    for span in spans:
        label = span.get_text(strip=True)
        key   = _normalise_ficha_key(label)
        if not key:
            continue
        idx = text.find(label)
        if idx >= 0:
            positions.append((idx, idx + len(label), key))

    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        if key not in ("coprodução", "parceria", "apoio"):
            value = re.split(
                r"\s+COPRODUÇÃO\b|\s+PARCERIA\b|\s+APOIO\b|\s+AGRADECIMENTOS\b",
                value, flags=re.IGNORECASE,
            )[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _normalise_ficha_key(label: str) -> str | None:
    label_up = label.upper().strip()
    KEY_MAP  = [
        ("TEXTO E ENCENAÇÃO",        "texto_encenação"),
        ("TEXTO",                    "texto"),
        ("ENCENAÇÃO",                "encenação"),
        ("DRAMATURGIA",              "dramaturgia"),
        ("DIREÇÃO ARTÍSTICA",        "direção"),
        ("DIREÇÃO DE PRODUÇÃO",      "direção_produção"),
        ("DIREÇÃO",                  "direção"),
        ("TRADUÇÃO",                 "tradução"),
        ("ADAPTAÇÃO",                "adaptação"),
        ("CENOGRAFIA E FIGURINOS",   "cenografia"),
        ("ESPAÇO CÉNICO",            "cenografia"),
        ("CENOGRAFIA",               "cenografia"),
        ("FIGURINOS",                "figurinos"),
        ("DESENHO DE LUZ",           "luz"),
        ("ILUMINAÇÃO",               "luz"),
        ("MÚSICA E ESPAÇO SONORO",   "música"),
        ("MÚSICA E DESENHO DE SOM",  "música"),
        ("DESENHO DE SOM",           "som"),
        ("SONOPLASTIA",              "som"),
        ("MÚSICA",                   "música"),
        ("COMPOSIÇÃO",               "música"),
        ("COREOGRAFIA",              "coreografia"),
        ("INTERPRETAÇÃO",            "interpretação"),
        ("ELENCO",                   "interpretação"),
        ("PRODUÇÃO EXECUTIVA",       "produção"),
        ("PRODUÇÃO E COMUNICAÇÃO",   "produção"),
        ("PRODUÇÃO",                 "produção"),
        ("COPRODUÇÃO",               "coprodução"),
        ("ASSISTENTE DE ENCENAÇÃO",  "ass_encenação"),
        ("ASSISTÊNCIA DE ENCENAÇÃO", "ass_encenação"),
    ]
    for label_key, mapped in KEY_MAP:
        if label_up == label_key:
            return mapped
    return None


def _get_image_url(soup, raw: str) -> str:
    og = soup.find("meta", property="og:image")
    if og:
        src = og.get("content", "")
        if src.startswith("http"):
            return src
    skip = {"blank", "logo", "tsl/icons", "tsl/assets", "lgp.svg", "ad.svg"}
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and src.startswith("http") and not any(s in src for s in skip) and len(src) > 30:
            return src
    for img in soup.find_all("img"):
        for attr in ["data-lazysrc", "data-src", "data-original"]:
            src = img.get(attr, "")
            if src and "blank" not in src and len(src) > 20:
                return src if src.startswith("http") else BASE + src
    pattern = r"https?://" + re.escape(IMG_DOMAIN) + r"/wp-content/uploads/[\w/._-]+\.(?:jpg|jpeg|png|webp)"
    m = re.search(pattern, raw)
    return m.group(0) if m else ""
