"""
Scraper: CCB — Centro Cultural de Belém
Categoria: Teatro (PT)
Listagem: https://www.ccb.pt/eventos/category/teatro/

Estrutura:
  - A listagem é HTML estático (WordPress + The Events Calendar).
  - As datas fiáveis vêm do JSON-LD (startDate / endDate ISO 8601).
  - Imagem: og:image
  - Bilhetes: primeiro link ccb.bol.pt ou bol.pt/Comprar na página
"""
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from scrapers.utils import make_id, log, HEADERS, can_scrape, truncate_synopsis, build_image_object, build_sessions

BASE   = "https://www.ccb.pt"
AGENDA = f"{BASE}/eventos/category/teatro/"

THEATER = {
    "id":          "ccb",
    "name":        "CCB — Centro Cultural de Belém",
    "short":       "CCB",
    "color":       "#00695c",
    "city":        "Lisboa",
    "address":     "Praça do Império, 1449-003 Lisboa",
    "site":        "https://www.ccb.pt",
    "programacao": "https://www.ccb.pt/eventos/category/teatro/",
    "lat":         38.6974,
    "lng":         -9.2059,
    "salas":       ["Grande Auditório", "Pequeno Auditório", "Black Box"],
    "aliases":     ["ccb", "centro cultural de belém", "centro cultural belem", "c.c.b.", "ccb belém"],
    "description": "O Centro Cultural de Belém é um dos maiores centros culturais de Portugal. Inaugurado em 1992, acolhe espetáculos de teatro, música, dança e ópera nas suas várias salas.",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]


def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    urls   = _collect_urls()
    events = []
    for url in sorted(urls):
        ev = _scrape_event(url)
        if ev:
            events.append(ev)
        time.sleep(0.4)
    log(f"[CCB] {len(events)} eventos de {len(urls)} URLs")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de URLs da listagem
# ─────────────────────────────────────────────────────────────

def _collect_urls() -> set[str]:
    """
    A listagem é HTML estático — os links /evento/ estão no source.
    Normaliza URLs com data (/evento/slug/YYYY-MM-DD/) para base slug.
    """
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[CCB] Erro na listagem: {e}")
        return set()

    raw  = set(re.findall(r'href="(https?://www\.ccb\.pt/evento/[^"]+)"', r.text))
    urls = set()
    skip = {"mercado-ccb"}

    for url in raw:
        url  = re.sub(r"/\d{4}-\d{2}-\d{2}/?$", "/", url)
        url  = url.rstrip("/") + "/"
        slug = url.rstrip("/").split("/")[-1]
        if slug and slug not in skip:
            urls.add(url)

    return urls


# ─────────────────────────────────────────────────────────────
# Scraping de página individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[CCB] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # JSON-LD — fonte mais fiável para título e datas
    ld_data = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict) and data.get("@type") == "Event":
                ld_data = data
                break
        except Exception:
            continue

    # Título
    title = ld_data.get("name", "").strip()
    if not title:
        h1 = soup.select_one("h1")
        title = h1.get_text(strip=True) if h1 else ""
    if not title or len(title) < 3:
        return None

    # Datas
    date_start = date_end = dates_label = ""
    if ld_data.get("startDate"):
        date_start = ld_data["startDate"][:10]
    if ld_data.get("endDate"):
        date_end = ld_data["endDate"][:10]
    if not date_end:
        date_end = date_start
    if date_start:
        dates_label = (
            f"{date_start} – {date_end}"
            if date_end and date_end != date_start
            else date_start
        )
    if not date_start:
        return None

    content_el   = soup.select_one(".tribe_events-template-default")
    content_text = content_el.get_text("\n") if content_el else soup.get_text("\n")

    # Imagem
    image = None
    og_img = soup.find("meta", property="og:image")
    raw_img = ""
    if og_img and og_img.get("content", "").startswith("http"):
        raw_img = og_img["content"]
    if not raw_img and ld_data.get("image"):
        img = ld_data["image"]
        raw_img = img if isinstance(img, str) else (img.get("url", "") if isinstance(img, dict) else "")
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # Bilhetes
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "ccb.bol.pt" in href or ("bol.pt/Comprar" in href and "ccb" in href.lower()):
            ticket_url = href
            break
    if not ticket_url:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "bol.pt/Comprar" in href:
                ticket_url = href
                break

    # Sala
    sala = ""
    sala_m = re.search(
        r"\b(BLACK BOX|PEQUENO AUDIT[OÓ]RIO|GRANDE AUDIT[OÓ]RIO|"
        r"ESPA[CÇ]O F[AÁ]BRICA DAS ARTES|F[AÁ]BRICA DAS ARTES|"
        r"AUDIT[OÓ]RIO|GALERIA)\b",
        content_text, re.IGNORECASE,
    )
    if sala_m:
        sala = sala_m.group(1).title()

    # Horários
    schedule = _extract_schedule(content_text)

    # Sinopse
    synopsis = ""
    og_desc  = soup.find("meta", property="og:description")
    if og_desc:
        synopsis = og_desc.get("content", "").strip()
    if content_el:
        for p in content_el.select("p"):
            t = p.get_text(strip=True)
            if len(t) > 80 and not re.match(
                r"^(A Funda|PARTILHAR|Para quaisquer|Descarregue|FICHA|IDADES|PREÇOS|Preços)",
                t, re.IGNORECASE,
            ):
                synopsis = t if not synopsis else synopsis + " " + t
                if len(synopsis) > 1000:
                    break

    # Classificação etária
    age_rating = ""
    age_block  = _extract_block(content_text, "IDADES", ["FICHA TÉCNICA", "PREÇOS", "Preços"])
    if age_block:
        age_m = re.search(
            r"(\+\s*\d+|M\s*/\s*\d+|Livre|[Tt]odas as idades|[Cc]lassificação etária a designar)",
            age_block,
        )
        age_rating = age_m.group(1).strip() if age_m else age_block.strip()[:40]

    # Ficha técnica
    technical_sheet = {}
    ficha_block = _extract_block(content_text, "FICHA TÉCNICA", ["Preços e Descontos", "PREÇOS", "COMPRAR"])
    if ficha_block:
        technical_sheet = _parse_ficha(ficha_block)

    # Preço
    price_info = ""
    price_block = _extract_block(content_text, "Preços e Descontos", ["DESCONTOS", "COMPRAR BILHETE\n"])
    if not price_block:
        price_block = _extract_block(content_text, "PREÇOS", ["COMPRAR", "DESCONTOS"])
    if price_block:
        pm = re.search(
            r"(Entrada\s+livre"
            r"|\d+(?:[,\.]\d+)?€\s*[-–]\s*\d+(?:[,\.]\d+)?€"
            r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?€"
            r"|\d+(?:[,\.]\d+)?\s*€)",
            price_block, re.IGNORECASE,
        )
        if pm:
            price_info = pm.group(1).strip()

    # Subtítulo
    subtitle = ""
    if content_el:
        h1_el   = content_el.select_one("h1")
        next_el = h1_el.find_next_sibling() if h1_el else None
        if next_el:
            sub = next_el.get_text(strip=True)
            if sub and len(sub) < 120 and not re.match(
                r"^(DATAS|PREÇOS|IDADES|COMPRAR|BLACK|PEQU|GRAND)", sub, re.IGNORECASE
            ):
                subtitle = sub

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "subtitle":        subtitle,
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
        "age_rating":      age_rating,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _extract_schedule(text: str) -> str:
    label        = "DATAS / HORÁRIOS"
    first        = text.find(label)
    if first == -1:
        return ""
    second       = text.find(label, first + len(label))
    content_start = (second + len(label)) if second != -1 else (first + len(label))
    end_idx      = len(text)
    for end_label in ["Teatro\n", "\nIDADES", "\nFICHA", "PARTILHAR", "\nPREÇOS"]:
        pos = text.find(end_label, content_start)
        if pos != -1 and pos < end_idx:
            end_idx = pos
    return re.sub(r"\n{3,}", "\n\n", text[content_start:end_idx].strip())


def _extract_block(text: str, start_label: str, end_labels: list[str]) -> str:
    idx = text.find(start_label)
    if idx == -1:
        m = re.search(re.escape(start_label), text, re.IGNORECASE)
        if not m:
            return ""
        idx = m.start()
    content_start = idx + len(start_label)
    end_idx       = len(text)
    for label in end_labels:
        pos = text.find(label, content_start)
        if pos != -1 and pos < end_idx:
            end_idx = pos
    return re.sub(r"\n{3,}", "\n\n", text[content_start:end_idx].strip())


def _parse_ficha(text: str) -> dict:
    ficha     = {}
    known_keys = [
        ("criação",       r"(?<!\w)[Cc]ria[çc][aã]o\s+"),
        ("texto",         r"(?<!\w)[Tt]exto\s+"),
        ("autor",         r"(?<!\w)[Aa]utor[a]?\s*[:\s]\s*"),
        ("dramaturgia",   r"(?<!\w)[Dd]ramaturgia\s+"),
        ("encenação",     r"(?<!\w)[Ee]ncena[çc][aã]o\s+"),
        ("direção",       r"(?<!\w)[Dd]ire[çc][aã]o\s+(?:artística\s+|de\s+[Pp]rodu[çc][aã]o\s+)?"),
        ("tradução",      r"(?<!\w)[Tt]radu[çc][aã]o\s+"),
        ("adaptação",     r"(?<!\w)[Aa]dapta[çc][aã]o\s+"),
        ("cenografia",    r"(?<!\w)[Cc]enografia\s+"),
        ("figurinos",     r"(?<!\w)[Ff]igurinos?\s+"),
        ("luz",           r"(?<!\w)[Dd]esenho\s+de\s+[Ll]uz\s+|(?<!\w)[Ii]lumina[çc][aã]o\s+"),
        ("som",           r"(?<!\w)[Dd]esenho\s+de\s+[Ss]om\s+|(?<!\w)[Ss]onoplastia\s+"),
        ("música",        r"(?<!\w)[Mm][úu]sica\s+"),
        ("coreografia",   r"(?<!\w)[Cc]oreografia\s+"),
        ("interpretação", r"(?<!\w)[Ii]nterpreta[çc][aã]o\s+"),
        ("produção",      r"(?<!\w)[Pp]rodu[çc][aã]o\s+"),
        ("coprodução",    r"(?<!\w)[Cc]oprodu[çc][aã]o\s+"),
        ("elenco",        r"(?<!\w)[Cc]om\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕ])"),
    ]
    positions = []
    for key, pattern in known_keys:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\s+(?:Preços|COMPRAR|Planta|DESCONTOS|Agradecimentos)", value)[0].strip()[:300]
        if value and key not in ficha:
            ficha[key] = value
    return ficha
