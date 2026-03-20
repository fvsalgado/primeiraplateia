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
from scrapers.schema import normalize_category

BASE       = "https://www.teatrosaoluiz.pt"
AGENDA     = f"{BASE}/programacao/"
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
    "logo_url":    "https://www.teatrosaoluiz.pt/wp-content/themes/saoluiz/assets/images/logo.svg",
    "favicon_url": "https://www.teatrosaoluiz.pt/wp-content/themes/saoluiz/assets/images/favicon.ico",
    "facade_url":  "https://www.teatrosaoluiz.pt/wp-content/uploads/2022/01/teatro-sao-luiz-fachada.jpg",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]


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
        href = a["href"]
        full = href if href.startswith("http") else BASE + href
        if full in seen:
            continue
        seen.add(full)

        # Extrair dados disponíveis no card da listagem
        card_data = _extract_card_data(a)

        ev = _scrape_event(full, card_data)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[São Luiz] {len(events)} eventos")
    return events


# ─────────────────────────────────────────────────────────────
# Extracção de dados do card na listagem
# ─────────────────────────────────────────────────────────────

def _extract_card_data(a_tag) -> dict:
    """
    Extrai campos disponíveis directamente no card da listagem:
    title, subtitle, category (raw), dates_label, schedule.

    Suporta duas estruturas:
      1. Cards (div.card.event-item): todos os spans dentro do <a>
      2. Calendário inline (div.calendar-day): spans irmãos do <a>
    """
    data = {}

    # Título
    title_el = a_tag.select_one("span.title, h2, h3")
    if title_el:
        data["title"] = title_el.get_text(strip=True)

    # Subtítulo / companhia (span.subtitle ou span.company dentro do card)
    sub_el = a_tag.select_one("span.subtitle, span.company, span.author")
    if sub_el:
        data["subtitle"] = sub_el.get_text(strip=True)

    # Categoria raw — dentro do <a> (caso 1)
    cat_el = a_tag.select_one("span.category")
    if cat_el:
        data["category_raw"] = cat_el.get_text(strip=True).lower()
    else:
        # Caso 2 — span.category fora do <a> (estrutura calendário)
        parent = a_tag.parent
        if parent:
            container = parent.parent
            if container:
                sibling = container.select_one("span.category")
                if sibling:
                    data["category_raw"] = sibling.get_text(strip=True).lower()

    # Datas no card
    date_el = a_tag.select_one("span.dates, span.date, [class*='date']")
    if date_el:
        data["dates_label_raw"] = date_el.get_text(strip=True)

    # Horário no card
    time_el = a_tag.select_one("span.time, span.hour, span.horario, [class*='hour'], [class*='time']")
    if time_el:
        data["schedule_raw"] = time_el.get_text(strip=True)

    return data


# ─────────────────────────────────────────────────────────────
# Scraping da página individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str, card_data: dict) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[São Luiz] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    raw  = r.text

    # ── Título ──────────────────────────────────────────────
    title = card_data.get("title", "")
    if not title:
        title_el = soup.select_one("h1")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # ── Subtítulo / autor / companhia ───────────────────────
    subtitle = card_data.get("subtitle", "")
    if not subtitle:
        h1 = soup.select_one("h1")
        if h1:
            sub_el = h1.find_next_sibling()
            if sub_el:
                sub = sub_el.get_text(strip=True)
                if sub and len(sub) < 120 and not re.match(
                    r"^(©|COMPRAR|BILHETE|DATAS|LOCAL|DURA|PRE[ÇC]O|CLASSI|ACESSI)",
                    sub, re.IGNORECASE,
                ):
                    subtitle = sub

    # ── Categoria ───────────────────────────────────────────
    # Prioridade: card → breadcrumb da página → "Outro"
    category_raw = card_data.get("category_raw", "")
    if not category_raw:
        bc = soup.select_one(".breadcrumbs, [class*='breadcrumb']")
        if bc:
            m = re.search(
                r"\b(teatro|m[uú]sica|dan[çc]a|circo|performance|[oó]pera|"
                r"pensamento|exposi[çc][aã]o|visita|espa[çc]o p[uú]blico|"
                r"literatura|infanto.juvenil|teatro musical)\b",
                bc.get_text(" "), re.IGNORECASE,
            )
            if m:
                category_raw = m.group(1).lower()
    category = normalize_category(category_raw) if category_raw else "Outro"

    # ── Campos estruturados (spans.subtitle na página) ──────
    fields      = _parse_subtitle_fields(soup)
    dates_label = fields.get("datas_label", card_data.get("dates_label_raw", ""))
    schedule    = fields.get("schedule", card_data.get("schedule_raw", ""))
    sala        = fields.get("local", "")
    duration    = fields.get("duracao", "")
    price_info  = fields.get("preco", "")
    age_rating  = fields.get("classificacao", "")
    accessibility_raw = fields.get("acessibilidade", "")

    # ── Datas ───────────────────────────────────────────────
    date_start, date_end = _parse_dates_from_field(dates_label)

    # fallback: datas do card
    if not date_start and card_data.get("dates_label_raw"):
        date_start, date_end = _parse_dates_from_field(card_data["dates_label_raw"])

    # ── Duração ─────────────────────────────────────────────
    duration_min = None
    if duration:
        m = re.search(r"(\d+)\s*min", duration, re.IGNORECASE)
        if m:
            duration_min = int(m.group(1))
    if not duration_min:
        # tentar no corpo da página
        m = re.search(r"(\d+)\s*min(?:utos)?", raw, re.IGNORECASE)
        if m:
            duration_min = int(m.group(1))
            if not duration:
                duration = f"{duration_min} min"

    # ── Classificação etária ─────────────────────────────────
    age_min = None
    if age_rating:
        m = re.search(r"[Mm]\s*/?\s*(\d+)", age_rating)
        if m:
            age_min = int(m.group(1))
    if not age_rating:
        m = re.search(r"[Mm]\s*/?\s*(\d+)", raw)
        if m:
            age_rating = m.group(0)
            age_min = int(m.group(1))

    # ── Preços ──────────────────────────────────────────────
    price_min = price_max = None
    if not price_info:
        # procurar no corpo
        m_free = re.search(r"entrada\s+livre|gratuito|free\s+entry", raw, re.IGNORECASE)
        if m_free:
            price_info = "Entrada livre"
        else:
            prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", raw)
            if prices:
                vals = sorted({float(p.replace(",", ".")) for p in prices})
                price_min = vals[0]
                price_max = vals[-1]
                price_info = f"{price_min:.0f}€" if price_min == price_max else f"{price_min:.0f}€ – {price_max:.0f}€"
    else:
        # extrair min/max do price_info já capturado
        m_free = re.search(r"entrada\s+livre|gratuito", price_info, re.IGNORECASE)
        if m_free:
            price_min = 0.0
        else:
            prices = re.findall(r"(\d+(?:[.,]\d+)?)\s*€", price_info)
            if prices:
                vals = sorted({float(p.replace(",", ".")) for p in prices})
                price_min = vals[0]
                price_max = vals[-1]

    # ── Acessibilidade ──────────────────────────────────────
    accessibility = []
    if accessibility_raw:
        accessibility = [a.strip() for a in re.split(r"[,/|;\n]+", accessibility_raw) if a.strip()]

    # ── Imagem ──────────────────────────────────────────────
    image = None
    raw_img = _get_image_url(soup, raw)
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Bilhetes ────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(k in href for k in ("saoluiz.bol.pt", "bol.pt/Comprar", "ticketline", "bilhete", "comprar")):
            ticket_url = href
            break
    if not ticket_url:
        m = re.search(r"href='(https?://[^']*(?:saoluiz\.bol\.pt|bol\.pt/Comprar|ticketline)[^']*)'", raw)
        if m:
            ticket_url = m.group(1)

    # ── Sinopse ─────────────────────────────────────────────
    synopsis = ""
    # Tentar og:description primeiro (mais limpa)
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        og_text = og_desc.get("content", "").strip()
        # Rejeitar descrições genéricas do site
        if og_text and len(og_text) > 60 and "programação" not in og_text.lower()[:40]:
            synopsis = og_text

    # fallback: parágrafos do <main> / .event-description
    if not synopsis:
        desc_el = soup.select_one(".event-description, section.event-description, main article, .entry-content")
        if desc_el:
            for p in desc_el.select("p"):
                t = p.get_text(strip=True)
                if len(t) > 60:
                    synopsis += (" " if synopsis else "") + t
                    if len(synopsis) > 1000:
                        break

    # ── Ficha técnica ────────────────────────────────────────
    technical_sheet = _parse_ficha(soup)

    # ── Montar evento ────────────────────────────────────────
    ev = {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        category,
        "source_url":      url,
        "date_start":      date_start,
    }

    # Campos opcionais — só incluir se tiverem valor
    if subtitle:
        ev["subtitle"] = subtitle
    if date_end:
        ev["date_end"] = date_end
    if dates_label:
        ev["dates_label"] = dates_label
    if schedule:
        ev["schedule"] = schedule
    if date_start:
        ev["sessions"] = build_sessions(date_start, date_end, schedule)
    if synopsis:
        ev["synopsis"] = truncate_synopsis(synopsis)
    if image:
        ev["image"] = image
    if ticket_url:
        ev["ticket_url"] = ticket_url
    if price_info:
        ev["price_info"] = price_info
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max
    if duration:
        ev["duration"] = duration
    if duration_min is not None:
        ev["duration_min"] = duration_min
    if age_rating:
        ev["age_rating"] = age_rating
    if age_min is not None:
        ev["age_min"] = age_min
    if sala:
        ev["sala"] = sala
    if accessibility:
        ev["accessibility"] = accessibility
    if technical_sheet:
        ev["technical_sheet"] = technical_sheet

    return ev


# ─────────────────────────────────────────────────────────────
# Parsing dos campos estruturados (spans.subtitle na página)
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

    # Separar horário das datas quando vêm juntos no mesmo campo
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


# ─────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Imagem
# ─────────────────────────────────────────────────────────────

def _get_image_url(soup, raw: str) -> str:
    # og:image é a fonte mais fiável
    og = soup.find("meta", property="og:image")
    if og:
        src = og.get("content", "")
        if src.startswith("http"):
            return src

    # Imagens inline, excluindo assets do tema
    skip = {"blank", "logo", "tsl/icons", "tsl/assets", "lgp.svg", "ad.svg"}
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and src.startswith("http") and not any(s in src for s in skip) and len(src) > 30:
            return src

    # Lazy-load attributes
    for img in soup.find_all("img"):
        for attr in ["data-lazysrc", "data-src", "data-original"]:
            src = img.get(attr, "")
            if src and "blank" not in src and len(src) > 20:
                return src if src.startswith("http") else BASE + src

    # Fallback: regex no HTML cru
    pattern = r"https?://" + re.escape(IMG_DOMAIN) + r"/wp-content/uploads/[\w/._-]+\.(?:jpg|jpeg|png|webp)"
    m = re.search(pattern, raw)
    return m.group(0) if m else ""
