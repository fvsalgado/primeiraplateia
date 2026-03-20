"""
Scraper: Teatro do Bairro
Fonte: https://teatrodobairro.org/
Cidade: Lisboa

Estrutura do site (SPA — Single Page Application):
  - Toda a programação está numa única página (homepage).
  - A barra de navegação, sob "Programação", lista links âncora para cada evento.
  - Cada espetáculo está numa <section class="module" id="slug">, com os dados
    dentro de <div class="work-details">.

  NOTAS DE PARSING:
  1. O HTML está frequentemente malformado (tags <div> e <ul> não fechadas).
     O lxml "adota" conteúdo das secções seguintes para a primeira, misturando
     os h5 de todas as secções. Usar html.parser resolve o problema.

  2. Âncora de extracção: div.work-details (mais robusta que iterar <section>
     directamente — está mais perto do conteúdo e menos afectada por tags
     externas não fechadas). O slug é obtido do <section> ancestral via
     find_parent().

  Estrutura interna de cada work-details:
      <h5>  → datas (começa com dígito, ex: "5 a 15 Março")
      <h3>  → título
      <h5>  → categoria (ex: "Teatro Griot - Acolhimento")
      <ul>  → horários, preços, bilhetes, classificação, duração
      <p>   → sinopse (parágrafos longos)
      <p>   → ficha técnica ("Texto: X | Encenação: Y | ...")
      <img> → imagem (assets/imagens/programa/<slug>.jpg)
"""

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
    build_sessions,
)
from scrapers.schema import normalize_category

# ─────────────────────────────────────────────────────────────
# Metadados do teatro
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "teatrodobairro",
    "name":        "Teatro do Bairro",
    "short":       "T. do Bairro",
    "color":       "#212121",
    "city":        "Lisboa",
    "address":     "Rua Luz Soriano, 63, 1200-246 Lisboa",
    "site":        "https://teatrodobairro.org",
    "programacao": "https://teatrodobairro.org/",
    "lat":         38.7131,
    "lng":         -9.1432,
    "salas":       ["Teatro do Bairro"],
    "logo_url":    "https://teatrodobairro.org/assets/imagens/logos/logo_tdb.svg",
    "favicon_url": "https://teatrodobairro.org/assets/imagens/favicons/favicon-32x32.png",
    "facade_url":  "https://teatrodobairro.org/assets/imagens/artigos/TeatrodoBairro_img.jpg",
    "aliases": [
        "teatro do bairro",
        "tdb",
        "companhia do teatro do bairro",
        "ar de filmes",
    ],
    "description": (
        "O Teatro do Bairro é um espaço cultural no coração do Bairro Alto, em Lisboa, "
        "inaugurado pela Ar de Filmes em 2011. Combina produção própria de teatro "
        "com acolhimento de criadores convidados, num programa que inclui também cinema."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://teatrodobairro.org"
AGENDA       = BASE + "/"

_REJECT_KEYWORDS = {"cinema", "dança", "danca", "música", "musica"}

# IDs de secções da homepage que não são espetáculos
_NON_EVENT_IDS = {
    "home", "teatrodobairro", "cartaodeamigo", "acompanhia",
    "alexandreoliveira", "antoniopires", "luisacostagomes",
    "historicodacompanhia", "news", "contactos",
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro ao carregar homepage: {e}")
        return []

    # CRÍTICO: usar html.parser, não lxml.
    # O HTML do Teatro do Bairro tem <div>/<ul> não fechadas que fazem o lxml
    # colapsar o conteúdo de secções subsequentes para dentro da primeira,
    # misturando os h5 de todos os eventos. html.parser é mais tolerante.
    soup = BeautifulSoup(r.text, "html.parser")

    # Slugs válidos da nav — fonte autoritativa dos eventos actuais
    valid_slugs = set(_collect_event_slugs_from_nav(soup))
    log(f"[{THEATER_NAME}] Slugs na nav: {sorted(valid_slugs)}")

    events   = []
    seen_ids: set[str] = set()

    # Iterar por div.work-details; obter slug do <section> ancestral.
    # Ancorar em work-details é mais robusto do que iterar <section>
    # directamente porque este div está mais próximo do conteúdo e menos
    # afectado por tags externas não fechadas.
    for wd in soup.select("div.work-details"):
        section = wd.find_parent("section")
        slug    = section.get("id", "") if section else ""

        # Ignorar secções que não são espetáculos
        if slug in _NON_EVENT_IDS:
            continue

        # Se temos slugs da nav, só processar os que lá estão
        if valid_slugs and slug not in valid_slugs:
            continue

        try:
            ev = _parse_work_details(wd, slug)
            if ev and ev["id"] not in seen_ids:
                seen_ids.add(ev["id"])
                events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro em work-details (slug='{slug}'): {e}")

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de slugs da navegação
# ─────────────────────────────────────────────────────────────

def _collect_event_slugs_from_nav(soup) -> list[str]:
    """
    Procura o dropdown "Programação" na navbar Bootstrap e recolhe
    os slugs de âncora de cada evento listado.
    """
    slugs: list[str] = []
    nav = soup.find("nav") or soup.find("header") or soup

    for li in nav.find_all("li"):
        first_a = li.find("a", recursive=False)
        label   = first_a.get_text(strip=True) if first_a else ""
        if not re.search(r"\bprograma[çc][aã]o\b", label, re.IGNORECASE):
            continue
        submenu = li.find("ul")
        if submenu:
            for a in submenu.find_all("a", href=True):
                href = a["href"]
                if "#" in href:
                    slug = href.split("#")[-1].strip()
                    if slug and slug not in slugs:
                        slugs.append(slug)
        break

    return slugs


# ─────────────────────────────────────────────────────────────
# Parsing de um bloco work-details
# ─────────────────────────────────────────────────────────────

def _parse_work_details(wd, slug: str) -> dict | None:
    """
    Extrai todos os campos de um <div class="work-details">.
    Devolve None se não for um espetáculo de teatro válido.
    """
    # ── Título ────────────────────────────────────────────────
    h3 = wd.find("h3")
    if not h3:
        return None
    title = h3.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # ── H5s: datas e categoria ────────────────────────────────
    # Garantido pelo html.parser: só h5 DESTE bloco, não de outros.
    h5s = wd.find_all("h5")
    dates_raw    = ""
    category_raw = ""
    for h5 in h5s:
        t = h5.get_text(strip=True)
        if not t:
            continue
        if re.match(r"^\d", t) and not dates_raw:
            dates_raw = t
        elif not re.match(r"^\d", t) and not category_raw:
            category_raw = t

    # ── Filtro de categoria ───────────────────────────────────
    cat_lower = category_raw.lower()
    if any(kw in cat_lower for kw in _REJECT_KEYWORDS):
        return None
    if category_raw and "teatro" not in cat_lower:
        return None

    category = normalize_category(category_raw) if category_raw else "Teatro"

    # ── Datas ─────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates(dates_raw)
    if not date_start:
        return None

    # ── Texto completo do bloco (para regexes) ────────────────
    full = wd.get_text(" ", strip=True)

    # ── URL canónica ──────────────────────────────────────────
    source_url = f"{BASE}/#{slug}" if slug else BASE + "/"

    # ── Imagem ────────────────────────────────────────────────
    # A imagem está fora do work-details, na coluna irmã. Subir ao .row.
    image   = None
    row_div = wd.find_parent("div", class_=re.compile(r"\brow\b"))
    if not row_div:
        row_div = wd.find_parent("section")
    if row_div:
        img = row_div.find("img", src=re.compile(r"assets/imagens/programa"))
        if img:
            src = img.get("src", "")
            raw_url = src if src.startswith("http") else urljoin(BASE, src)
            image = build_image_object(raw_url, row_div, THEATER_NAME, source_url)

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    for a in wd.find_all("a", href=True):
        h = a["href"].lower()
        if any(kw in h for kw in ("bol.pt", "ticketline", "bilhete", "comprar")):
            ticket_url = a["href"]
            break

    # ── Horários ──────────────────────────────────────────────
    horas = list(dict.fromkeys(re.findall(r"\d{1,2}h\d{2}", full)))
    schedule = " / ".join(horas) if horas else ""

    # ── Preços ────────────────────────────────────────────────
    price_info = ""
    price_min  = None
    price_max  = None

    if re.search(r"entrada\s+(?:livre|gratuita)|gratuito", full, re.IGNORECASE):
        price_info = "Entrada livre"
        price_min  = 0.0
        price_max  = 0.0
    else:
        valores = []
        for p in re.findall(r"(\d+(?:[,\.]\d+)?)\s*€", full):
            try:
                valores.append(float(p.replace(",", ".")))
            except ValueError:
                pass
        if valores:
            price_min  = min(valores)
            price_max  = max(valores)
            price_info = (
                f"{price_min:.2g}€"
                if price_min == price_max
                else f"{price_min:.2g}€ – {price_max:.2g}€"
            )

    # ── Classificação etária ──────────────────────────────────
    age_rating = ""
    age_min    = None
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full)
    if age_m:
        age_min    = int(age_m.group(1))
        age_rating = f"M/{age_min}"

    # ── Duração ───────────────────────────────────────────────
    duration     = ""
    duration_min = None
    dur_m = re.search(r"(\d+)\s*min", full, re.IGNORECASE)
    if dur_m:
        duration_min = int(dur_m.group(1))
        duration     = f"{duration_min} min."

    # ── Sinopse e ficha técnica ───────────────────────────────
    synopsis, technical_sheet = _extract_synopsis_and_ficha(wd)

    # ── Subtítulo ─────────────────────────────────────────────
    subtitle = _extract_subtitle(category_raw, technical_sheet, full)

    ev = {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "subtitle":        subtitle,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        build_sessions(date_start, date_end, schedule),
        "schedule":        schedule,
        "synopsis":        truncate_synopsis(synopsis) if synopsis else "",
        "image":           image,
        "source_url":      source_url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "price_min":       price_min,
        "price_max":       price_max,
        "duration":        duration,
        "duration_min":    duration_min,
        "age_rating":      age_rating,
        "age_min":         age_min,
        "sala":            "",
        "technical_sheet": technical_sheet,
    }

    # Remover campos vazios/None opcionais
    return {k: v for k, v in ev.items() if v is not None and v != "" and v != []}


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

_PT_MONTHS = {
    "janeiro": 1,  "fevereiro": 2, "março": 3,    "marco": 3,
    "abril": 4,    "maio": 5,      "junho": 6,     "julho": 7,
    "agosto": 8,   "setembro": 9,  "outubro": 10,
    "novembro": 11,"dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10,"nov": 11,"dez": 12,
}


def _parse_dates(text: str) -> tuple[str, str, str]:
    """
    Suporta os formatos encontrados no Teatro do Bairro:
      "5 a 15 Março"       →  range
      "21 e 22 Março"      →  dois dias
      "3 Abril"            →  data única
      "5 a 15 Março 2026"  →  com ano explícito
    """
    if not text:
        return "", "", ""
    text = text.strip()

    # "DD a DD Mês [YYYY]"
    m = re.search(
        r"(\d{1,2})\s+[aà]\s+(\d{1,2})\s+([A-Za-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr = m.groups()
        n = _PT_MONTHS.get(mon_s.lower())
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            return (
                f"{d1} a {d2} {mon_s.capitalize()}",
                f"{y}-{n:02d}-{int(d1):02d}",
                f"{y}-{n:02d}-{int(d2):02d}",
            )

    # "DD e DD Mês [YYYY]"
    m = re.search(
        r"(\d{1,2})\s+e\s+(\d{1,2})\s+([A-Za-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr = m.groups()
        n = _PT_MONTHS.get(mon_s.lower())
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            return (
                f"{d1} e {d2} {mon_s.capitalize()}",
                f"{y}-{n:02d}-{int(d1):02d}",
                f"{y}-{n:02d}-{int(d2):02d}",
            )

    # "DD Mês [YYYY]" — data única
    m = re.search(
        r"(\d{1,2})\s+([A-Za-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d, mon_s, yr = m.groups()
        n = _PT_MONTHS.get(mon_s.lower())
        if n:
            y = int(yr) if yr else _infer_year(n, int(d))
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{d} {mon_s.capitalize()}", ds, ds

    return "", "", ""


def _infer_year(month: int, day: int) -> int:
    from datetime import datetime
    now = datetime.now()
    if month > now.month or (month == now.month and day >= now.day):
        return now.year
    return now.year + 1


# ─────────────────────────────────────────────────────────────
# Sinopse e ficha técnica
# ─────────────────────────────────────────────────────────────

_FICHA_START_RE = re.compile(
    r"^(Texto|Encena[çc][aã]o|Interpreta[çc][aã]o|Concep[çc][aã]o|"
    r"Tradu[çc][aã]o|Cenografia|Figurinos|M[úu]sica|Luz|Som|"
    r"Co-produ[çc][aã]o|Coprodu[çc][aã]o|Produ[çc][aã]o|"
    r"Coreografia|Dramaturgia|Dire[çc][aã]o|"
    r"Composi[çc][aã]o|Apoios?)\s*[:\|]",
    re.IGNORECASE,
)


def _extract_synopsis_and_ficha(wd) -> tuple[str, dict]:
    synopsis   = ""
    ficha_text = ""
    in_ficha   = False

    for p in wd.find_all("p"):
        t = p.get_text(" ", strip=True)
        if not t or len(t) < 5:
            continue
        if _FICHA_START_RE.match(t):
            in_ficha = True
        if in_ficha:
            ficha_text += (" " if ficha_text else "") + t
        elif len(t) > 40:
            synopsis += (" " if synopsis else "") + t

    return synopsis, _parse_ficha(ficha_text) if ficha_text else {}


def _parse_ficha(text: str) -> dict:
    """
    Analisa ficha técnica em texto corrido separado por " | ".
    Ex: "Texto: X | Encenação: Y | Interpretação: A, B, C"
    """
    ficha = {}

    known_keys = [
        ("texto",          r"[Tt]exto\s*[:\|]\s*"),
        ("encenação",      r"[Ee]ncena[çc][aã]o\s*[:\|]\s*"),
        ("dramaturgia",    r"[Dd]ramaturgia\s*[:\|]\s*"),
        ("direção",        r"[Dd]ire[çc][aã]o\s+[Aa]rt[íi]stica\s*[:\|]\s*"
                           r"|[Dd]ire[çc][aã]o\s*[:\|]\s*"),
        ("tradução",       r"[Tt]radu[çc][aã]o\s*[:\|]\s*"),
        ("cenografia",     r"[Cc]enografia\s*(?:e\s+[Ff]igurinos?)?\s*[:\|]\s*"),
        ("figurinos",      r"[Ff]igurinos?\s*[:\|]\s*"),
        ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\|]\s*"
                           r"|[Dd]esign\s+de\s+[Ll]uz\s*[:\|]\s*"),
        ("som",            r"[Dd]esenho\s+de\s+[Ss]om\s*[:\|]\s*"
                           r"|[Dd]esign\s+de\s+[Ss]om\s*[:\|]\s*"
                           r"|[Ss]onoplastia\s*[:\|]\s*"),
        ("música",         r"[Mm][úu]sica\s+e\s+[Dd]esign\s+de\s+[Ss]om\s*[:\|]\s*"
                           r"|[Mm][úu]sica\s*[:\|]\s*"
                           r"|[Cc]omposi[çc][aã]o\s+[Mm]usical\s*[:\|]\s*"),
        ("interpretação",  r"[Ii]nterpreta[çc][aã]o\s*[:\|]\s*"
                           r"|[Cc]om\s*[:\|]\s*"),
        ("concepção",      r"[Cc]oncep[çc][aã]o\s+e\s+[Ii]nterpreta[çc][aã]o\s*[:\|]\s*"
                           r"|[Cc]oncep[çc][aã]o\s*[:\|]\s*"),
        ("coreografia",    r"[Cc]oreografia\s*[:\|]\s*"),
        ("produção",       r"[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
        ("coprodução",     r"[Cc]o-?[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
        ("apoios",         r"[Aa]poios?\s*[:\|]\s*"),
    ]

    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 500
        value = re.sub(r"\s+", " ", text[end:next_start].strip())
        value = re.split(r"\s*©\s*", value)[0]
        value = value.rstrip(" |;,").strip()[:300]
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _extract_subtitle(category_raw: str, technical_sheet: dict, full_text: str) -> str:
    """
    Subtítulo: autor (campo 'texto' da ficha), companhia na categoria,
    ou companhia mencionada no texto.
    """
    if technical_sheet.get("texto"):
        return technical_sheet["texto"]

    # "Teatro Griot - Acolhimento" → subtítulo = "Teatro Griot"
    if " - " in category_raw:
        parte = category_raw.split(" - ")[0].strip()
        if parte.lower() not in ("teatro", "dança", "música"):
            return parte

    # Companhia mencionada no texto corrido
    m = re.search(r"\b(Teatro\s+\w+|Companhia\s+[\w\s]{3,20}?)\b", full_text)
    if m:
        candidate = m.group(1).strip()
        if "bairro" not in candidate.lower():
            return candidate

    return ""
