"""
Scraper: Teatro do Bairro
Fonte: https://teatrodobairro.org/
Cidade: Lisboa

Estrutura do site (SPA — Single Page Application):
  - Toda a programação está numa única página (homepage).
  - A barra de navegação, sob "Programação", lista links âncora para cada evento:
      <a href="#slug-do-evento">Título</a>
  - Estes links são a fonte autoritativa da lista de eventos actuais.
  - Cada espetáculo é uma secção âncora: <section id="slug">.
  - Não há páginas individuais de evento — todos os dados estão na homepage.
  - Estrutura de cada evento:
      <h5>  → datas (ex: "5 a 15 Março")
      <h3>  → título
      <h5>  → categoria (ex: "Teatro Griot - Acolhimento" ou "Teatro")
      lista → horários, preços, bilhetes, classificação etária, duração
      <p>   → sinopse em parágrafos
      texto → ficha técnica em texto corrido após a sinopse
      <img> → imagem (assets/imagens/programa/<slug>.jpg)
  - Bilhetes: link bol.pt dentro de cada secção.
  - Categorias: só "Teatro" e variantes ("Teatro - Acolhimento", etc.).
    O teatro tem também Cinema e Dança mas são raros.
"""

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
    parse_date_range, parse_date,
)

# ─────────────────────────────────────────────────────────────
# Metadados do teatro — lidos pelo sync_scrapers.py
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

# Categorias a rejeitar — o site usa "Teatro", "Teatro - Acolhimento", etc.
_REJECT_KEYWORDS = {"cinema", "dança", "danca", "música", "musica"}


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

    soup   = BeautifulSoup(r.text, "lxml")
    events = []
    seen_ids: set[str] = set()

    # ── Estratégia principal: extrair slugs da nav de Programação ──────────
    # A barra de navegação lista links âncora (#slug) para cada evento actual.
    # Isto é mais fiável do que iterar todas as <section> da página.
    event_slugs = _collect_event_slugs_from_nav(soup)

    if event_slugs:
        log(f"[{THEATER_NAME}] {len(event_slugs)} eventos encontrados na nav: {event_slugs}")
        for slug in event_slugs:
            section = soup.find("section", id=slug)
            if not section:
                # Tentar sem case sensitivity (algumas páginas têm ids em maiúsculas)
                section = soup.find("section", id=re.compile(f"^{re.escape(slug)}$", re.IGNORECASE))
            if not section:
                log(f"[{THEATER_NAME}] Secção não encontrada para slug '{slug}'")
                continue
            try:
                ev = _parse_section(section, slug, soup)
                if ev and ev["id"] not in seen_ids:
                    seen_ids.add(ev["id"])
                    events.append(ev)
            except Exception as e:
                log(f"[{THEATER_NAME}] Erro na secção '{slug}': {e}")
    else:
        # ── Fallback: percorrer todas as <section> com id ──────────────────
        # Usado se a nav não tiver links de âncora (mudança de estrutura futura).
        log(f"[{THEATER_NAME}] Nav sem links de evento — a usar fallback por <section>")
        for section in soup.find_all("section", id=True):
            slug = section.get("id", "")
            try:
                ev = _parse_section(section, slug, soup)
                if ev and ev["id"] not in seen_ids:
                    seen_ids.add(ev["id"])
                    events.append(ev)
            except Exception as e:
                log(f"[{THEATER_NAME}] Erro na secção '{slug}': {e}")

    log(f"[{THEATER_NAME}] {len(events)} eventos de teatro")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de slugs a partir da navegação
# ─────────────────────────────────────────────────────────────

def _collect_event_slugs_from_nav(soup) -> list[str]:
    """
    Percorre a barra de navegação à procura do dropdown "Programação"
    e recolhe os slugs de âncora de cada evento listado.

    O site usa Bootstrap dropdown:
        <li class="dropdown">
            <a>Programação</a>
            <ul class="dropdown-menu">
                <li><a href="https://teatrodobairro.org/#slug">Título</a></li>
            </ul>
        </li>

    Nota: os hrefs podem ter URL completa (https://.../#slug) ou apenas #slug.
    O scraper extrai sempre o fragmento após o último '#'.
    """
    slugs: list[str] = []

    nav = soup.find("nav") or soup.find("header") or soup

    # Procurar o <li> cujo link/texto directo seja "Programação"
    for li in nav.find_all("li"):
        first_a = li.find("a", recursive=False)
        label   = first_a.get_text(strip=True) if first_a else ""
        if not re.search(r"\bprograma[çc][aã]o\b", label, re.IGNORECASE):
            continue

        # Encontrámos o li de Programação — recolher links do sub-menu
        submenu = li.find("ul")
        if not submenu:
            continue

        for a in submenu.find_all("a", href=True):
            href = a["href"]
            # Apanha "#slug" e "https://.../#slug"
            if "#" in href:
                slug = href.split("#")[-1].strip()
                if slug and slug not in slugs:
                    slugs.append(slug)

        break  # dropdown de Programação encontrado

    return slugs



# ─────────────────────────────────────────────────────────────
# Parsing de secção de evento
# ─────────────────────────────────────────────────────────────

def _parse_section(section, section_id: str, full_soup) -> dict | None:
    """
    Extrai evento de uma <section> da homepage.
    Devolve None se não for um espetáculo de teatro válido.
    """
    # Título — h3 dentro da secção
    h3 = section.find("h3")
    if not h3:
        return None
    title = h3.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    full_text = section.get_text(" ", strip=True)

    # Categoria — h5 que não seja data
    # O site tem dois h5: um para datas, outro para categoria
    category_raw = ""
    for h5 in section.find_all("h5"):
        t = h5.get_text(strip=True)
        # h5 de categoria não começa com número (datas começam com número)
        if t and not re.match(r"^\d", t):
            category_raw = t
            break

    # Filtrar por categoria
    cat_lower = category_raw.lower()
    if any(kw in cat_lower for kw in _REJECT_KEYWORDS):
        return None
    # Aceitar se contiver "teatro" ou se categoria for vazia (assumir teatro)
    if category_raw and "teatro" not in cat_lower:
        return None

    # Categoria normalizada para o schema
    category = "Teatro"

    # Datas — h5 que começa com número
    dates_raw = ""
    for h5 in section.find_all("h5"):
        t = h5.get_text(strip=True)
        if re.match(r"^\d", t):
            dates_raw = t
            break

    dates_label, date_start, date_end = _parse_dates(dates_raw)
    if not date_start:
        return None

    # URL canónica — âncora da secção
    source_url = f"{BASE}/#{section_id}" if section_id else BASE + "/"

    # Imagem — src com assets/imagens/programa
    image   = None
    raw_img = ""
    img_tag = section.find("img", src=re.compile(r"assets/imagens/programa"))
    if img_tag:
        src = img_tag.get("src", "")
        raw_img = src if src.startswith("http") else urljoin(BASE, src)
    if raw_img:
        image = build_image_object(raw_img, section, THEATER_NAME, source_url)

    # Bilhetes — link bol.pt
    ticket_url = ""
    for a in section.find_all("a", href=True):
        if "bol.pt" in a["href"]:
            ticket_url = a["href"]
            break

    # Horários — linhas com "h00" ou "hXX"
    schedule = ""
    hora_m = re.search(r"(\d{1,2}h\d{2})", full_text)
    if hora_m:
        schedule = hora_m.group(1)

    # Preço
    price_info = ""
    preco_m = re.search(
        r"(Entrada\s+(?:livre|gratuita)"
        r"|gratuito"
        r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if preco_m:
        price_info = preco_m.group(1).strip()

    # Classificação etária
    age_rating = ""
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if age_m:
        age_rating = f"M/{age_m.group(1)}"

    # Duração
    duration = ""
    dur_m = re.search(r"(\d+)\s*min", full_text, re.IGNORECASE)
    if dur_m:
        duration = f"{dur_m.group(1)} min."

    # Sinopse e ficha técnica
    synopsis, technical_sheet = _extract_synopsis_and_ficha(section)

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
        "source_url":      source_url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "sala":            "",
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

_PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


def _parse_dates(text: str) -> tuple[str, str, str]:
    """
    Formatos no Teatro do Bairro:
      "5 a 15 Março"
      "21 e 22 Março"
      "3 a 26 Abril"
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
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            return (
                f"{d1} a {d2} {mon_s}",
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
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            return (
                f"{d1} e {d2} {mon_s}",
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
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d))
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{d} {mon_s}", ds, ds

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower().strip())


def _infer_year(month: int, day: int) -> int:
    from datetime import datetime
    now = datetime.now()
    if month > now.month or (month == now.month and day >= now.day):
        return now.year
    return now.year + 1


# ─────────────────────────────────────────────────────────────
# Extracção de sinopse e ficha técnica
# ─────────────────────────────────────────────────────────────

def _extract_synopsis_and_ficha(section) -> tuple[str, dict]:
    """
    Na homepage do Teatro do Bairro, sinopse e ficha técnica estão
    em parágrafos dentro da secção.
    A ficha técnica começa com "Texto:", "Encenação:", etc. em texto corrido
    separado por " | " ou em linha própria.
    """
    synopsis   = ""
    ficha_text = ""
    in_ficha   = False

    for p in section.find_all("p"):
        t = p.get_text(" ", strip=True)
        if not t or len(t) < 5:
            continue

        # Detectar início da ficha técnica
        # Padrão: começa com chave conhecida seguida de ":"
        if re.match(
            r"^(Texto|Encena[çc][aã]o|Interpreta[çc][aã]o|Concep[çc][aã]o|"
            r"Tradu[çc][aã]o|Cenografia|Figurinos|M[úu]sica|Luz|Som|"
            r"Co-produ[çc][aã]o|Coprodu[çc][aã]o|Produ[çc][aã]o|"
            r"Apoios?|Agradecimentos)\s*[:\|]",
            t, re.IGNORECASE,
        ):
            in_ficha = True

        if in_ficha:
            ficha_text += (" " if ficha_text else "") + t
        else:
            if len(t) > 40:
                synopsis += (" " if synopsis else "") + t

    technical_sheet = _parse_ficha(ficha_text) if ficha_text else {}
    return synopsis, technical_sheet


def _parse_ficha(text: str) -> dict:
    """
    Ficha técnica do Teatro do Bairro em texto corrido com " | " como separador
    ou em linhas separadas.
    Ex: "Texto: X | Encenação: Y | Interpretação: Z"
    """
    ficha      = {}
    known_keys = [
        ("texto",         r"[Tt]exto\s*[:\|]\s*"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s*[:\|]\s*"),
        ("dramaturgia",   r"[Dd]ramaturgia\s*[:\|]\s*"),
        ("direção",       r"[Dd]ire[çc][aã]o\s+[Aa]rt[íi]stica\s*[:\|]\s*|[Dd]ire[çc][aã]o\s*[:\|]\s*"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s*[:\|]\s*"),
        ("cenografia",    r"[Cc]enografia\s*(?:e\s+[Ff]igurinos?)?\s*[:\|]\s*"),
        ("figurinos",     r"[Ff]igurinos?\s*[:\|]\s*"),
        ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\|]\s*"),
        ("som",           r"[Mm][úu]sica\s+e\s+[Dd]esign\s+de\s+[Ss]om\s*[:\|]\s*|[Ss]onoplastia\s*[:\|]\s*"),
        ("música",        r"[Mm][úu]sica\s*[:\|]\s*"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s*[:\|]\s*"),
        ("concepção",     r"[Cc]oncep[çc][aã]o\s+e\s+[Ii]nterpreta[çc][aã]o\s*[:\|]\s*|[Cc]oncep[çc][aã]o\s*[:\|]\s*"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
        ("coprodução",    r"[Cc]o-?[Pp]rodu[çc][aã]o\s*[:\|]\s*"),
    ]

    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\s*(?:Apoios?|Agradecimentos|©)\s*[:\|]", value, flags=re.IGNORECASE)[0]
        value      = value.rstrip(" |").strip()[:200]
        if value and key not in ficha:
            ficha[key] = value

    return ficha
