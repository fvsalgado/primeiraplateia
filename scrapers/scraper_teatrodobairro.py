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

  NOTA DE PARSING:
  O HTML da homepage está frequentemente malformado (tags <div> e <ul> não fechadas),
  o que faz parsers como lxml colapsarem secções subsequentes dentro da primeira.
  Solução: usar html.parser (mais tolerante) E extrair cada secção pelo id
  procurando directamente no HTML em bruto com um split por âncora, como fallback.

  Estrutura de cada evento:
      <h5>  → datas (ex: "5 a 15 Março")
      <h3>  → título
      <h5>  → categoria (ex: "Teatro Griot - Acolhimento" ou "Teatro")
      lista → horários, preços, bilhetes, classificação etária, duração
      <p>   → sinopse em parágrafos
      texto → ficha técnica em texto corrido após a sinopse
      <img> → imagem (assets/imagens/programa/<slug>.jpg)
  - Bilhetes: link bol.pt dentro de cada secção.
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
    parse_date_range, parse_date, build_sessions,
)
from scrapers.schema import normalize_category

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

    raw_html = r.text

    # Usar html.parser em vez de lxml: mais tolerante com HTML malformado.
    # O HTML do Teatro do Bairro tem frequentemente <div>/<ul> não fechadas
    # que fazem o lxml colapsar secções subsequentes dentro da primeira.
    soup = BeautifulSoup(raw_html, "html.parser")

    events    = []
    seen_ids: set[str] = set()

    # ── Recolher slugs da nav ──────────────────────────────────────────────
    event_slugs = _collect_event_slugs_from_nav(soup)

    if not event_slugs:
        log(f"[{THEATER_NAME}] Nenhum slug encontrado na nav — a usar fallback por <section>")

    log(f"[{THEATER_NAME}] Slugs encontrados na nav: {event_slugs}")

    for slug in event_slugs:
        # Estratégia 1: encontrar <section id="slug"> no soup normal
        section = soup.find("section", id=slug)
        if not section:
            section = soup.find("section", id=re.compile(f"^{re.escape(slug)}$", re.IGNORECASE))

        # Estratégia 2 (fallback para HTML malformado): extrair fragmento do HTML em bruto
        if not section:
            section = _extract_section_from_raw(raw_html, slug)

        if not section:
            log(f"[{THEATER_NAME}] Secção não encontrada para slug '{slug}'")
            continue

        try:
            ev = _parse_section(section, slug)
            if ev and ev["id"] not in seen_ids:
                seen_ids.add(ev["id"])
                events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro na secção '{slug}': {e}")

    # ── Fallback geral: percorrer <section id=...> se nav vazia ───────────
    if not events:
        log(f"[{THEATER_NAME}] A usar fallback por <section id=...>")
        for section in soup.find_all("section", id=True):
            slug = section.get("id", "")
            if not slug or slug in ("home", "teatrodobairro", "cartaodeamigo",
                                     "acompanhia", "alexandreoliveira", "antoniopires",
                                     "luisacostagomes", "historicodacompanhia",
                                     "news", "contactos"):
                continue
            try:
                ev = _parse_section(section, slug)
                if ev and ev["id"] not in seen_ids:
                    seen_ids.add(ev["id"])
                    events.append(ev)
            except Exception as e:
                log(f"[{THEATER_NAME}] Erro na secção '{slug}': {e}")

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Extracção de secção a partir de HTML em bruto (fallback)
# ─────────────────────────────────────────────────────────────

def _extract_section_from_raw(html: str, slug: str) -> BeautifulSoup | None:
    """
    Quando o HTML está malformado e o BeautifulSoup não consegue separar
    as secções correctamente, extraímos o fragmento entre o id âncora e
    o próximo id de secção de programação.

    Procura o padrão: id="<slug"> ... até ao próximo <section
    """
    # Localizar o início da secção com este id
    pattern = re.compile(
        rf'<section[^>]+id=["\']?{re.escape(slug)}["\']?[^>]*>',
        re.IGNORECASE,
    )
    m = pattern.search(html)
    if not m:
        return None

    start = m.start()

    # Encontrar o próximo <section depois deste
    next_section = re.search(r'<section[\s>]', html[m.end():])
    if next_section:
        end = m.end() + next_section.start()
    else:
        end = len(html)

    fragment = html[start:end]
    # Fechar tags abertas para ajudar o parser
    fragment += "</div></div></div></section>"

    return BeautifulSoup(fragment, "html.parser")


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
    """
    slugs: list[str] = []

    nav = soup.find("nav") or soup.find("header") or soup

    for li in nav.find_all("li"):
        first_a = li.find("a", recursive=False)
        label   = first_a.get_text(strip=True) if first_a else ""
        if not re.search(r"\bprograma[çc][aã]o\b", label, re.IGNORECASE):
            continue

        submenu = li.find("ul")
        if not submenu:
            continue

        for a in submenu.find_all("a", href=True):
            href = a["href"]
            if "#" in href:
                slug = href.split("#")[-1].strip()
                if slug and slug not in slugs:
                    slugs.append(slug)

        break

    return slugs


# ─────────────────────────────────────────────────────────────
# Parsing de secção de evento
# ─────────────────────────────────────────────────────────────

def _parse_section(section, section_id: str) -> dict | None:
    """
    Extrai evento de uma <section> da homepage.
    Devolve None se não for um espetáculo de teatro válido.
    """
    # ── Título ────────────────────────────────────────────────
    h3 = section.find("h3")
    if not h3:
        return None
    title = h3.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    full_text = section.get_text(" ", strip=True)

    # ── Categoria ─────────────────────────────────────────────
    # Segundo h5: não começa com dígito
    category_raw = ""
    subtitle      = ""
    for h5 in section.find_all("h5"):
        t = h5.get_text(strip=True)
        if t and not re.match(r"^\d", t):
            category_raw = t
            break

    # Filtrar categorias não-teatro
    cat_lower = category_raw.lower()
    if any(kw in cat_lower for kw in _REJECT_KEYWORDS):
        return None
    # Aceitar se contiver "teatro" ou se categoria for vazia
    if category_raw and "teatro" not in cat_lower:
        return None

    # Normalizar categoria
    category = normalize_category(category_raw) if category_raw else "Teatro"

    # Subtítulo: parte da categoria após " - " (ex: "Teatro Griot - Acolhimento" → "Teatro Griot")
    # Ou extrair autor/companhia do texto da ficha técnica
    if " - " in category_raw:
        subtitle = category_raw.split(" - ", 1)[1].strip()
    elif "–" in category_raw:
        subtitle = category_raw.split("–", 1)[1].strip()

    # ── Datas ─────────────────────────────────────────────────
    dates_raw = ""
    for h5 in section.find_all("h5"):
        t = h5.get_text(strip=True)
        if re.match(r"^\d", t):
            dates_raw = t
            break

    dates_label, date_start, date_end = _parse_dates(dates_raw)
    if not date_start:
        return None

    # ── URL canónica ──────────────────────────────────────────
    source_url = f"{BASE}/#{section_id}" if section_id else BASE + "/"

    # ── Imagem ────────────────────────────────────────────────
    image   = None
    raw_img = ""
    img_tag = section.find("img", src=re.compile(r"assets/imagens/programa"))
    if img_tag:
        src = img_tag.get("src", "")
        raw_img = src if src.startswith("http") else urljoin(BASE, src)
    if raw_img:
        image = build_image_object(raw_img, section, THEATER_NAME, source_url)

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    for a in section.find_all("a", href=True):
        href_lower = a["href"].lower()
        if any(kw in href_lower for kw in ("bol.pt", "ticketline", "bilhete", "comprar")):
            ticket_url = a["href"]
            break

    # ── Horários ──────────────────────────────────────────────
    # Tentar capturar múltiplos horários (ex: "21h00" e "16h00")
    schedule = ""
    hora_matches = re.findall(r"\d{1,2}h\d{2}", full_text)
    if hora_matches:
        schedule = " / ".join(dict.fromkeys(hora_matches))  # deduplicar preservando ordem

    # ── Preço ─────────────────────────────────────────────────
    price_info  = ""
    price_min   = None
    price_max   = None

    # Entrada livre / gratuito
    if re.search(r"entrada\s+(?:livre|gratuita)|gratuito", full_text, re.IGNORECASE):
        price_info = "Entrada livre"
        price_min  = 0.0
        price_max  = 0.0
    else:
        # Extrair todos os preços em €
        precos = re.findall(r"(\d+(?:[,\.]\d+)?)\s*€", full_text)
        valores = []
        for p in precos:
            try:
                valores.append(float(p.replace(",", ".")))
            except ValueError:
                pass
        if valores:
            price_min = min(valores)
            price_max = max(valores)
            price_info = f"{price_min:.0f}€" if price_min == price_max else f"{price_min:.0f}€ – {price_max:.0f}€"

    # ── Classificação etária ──────────────────────────────────
    age_rating = ""
    age_min    = None
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if age_m:
        age_min    = int(age_m.group(1))
        age_rating = f"M/{age_min}"

    # ── Duração ───────────────────────────────────────────────
    duration     = ""
    duration_min = None
    dur_m = re.search(r"(\d+)\s*min", full_text, re.IGNORECASE)
    if dur_m:
        duration_min = int(dur_m.group(1))
        duration     = f"{duration_min} min."

    # ── Sinopse e ficha técnica ───────────────────────────────
    synopsis, technical_sheet = _extract_synopsis_and_ficha(section)

    # Tentar extrair subtítulo da ficha técnica se ainda não temos
    if not subtitle:
        subtitle = _extract_subtitle(technical_sheet, full_text)

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

    # Limpar campos None/vazios opcionais para não poluir o schema
    return {k: v for k, v in ev.items() if v is not None and v != ""}


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
    Formatos encontrados no Teatro do Bairro:
      "5 a 15 Março"
      "21 e 22 Março"
      "3 a 26 Abril"
      "5 a 15 Março 2026"
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
        n = _mon(mon_s)
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
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d))
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{d} {mon_s.capitalize()}", ds, ds

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

# Padrão para detectar início de ficha técnica
_FICHA_START_RE = re.compile(
    r"^(Texto|Encena[çc][aã]o|Interpreta[çc][aã]o|Concep[çc][aã]o|"
    r"Tradu[çc][aã]o|Cenografia|Figurinos|M[úu]sica|Luz|Som|"
    r"Co-produ[çc][aã]o|Coprodu[çc][aã]o|Produ[çc][aã]o|"
    r"Coreografia|Dramaturgia|Dire[çc][aã]o|"
    r"Apoios?|Agradecimentos)\s*[:\|]",
    re.IGNORECASE,
)


def _extract_synopsis_and_ficha(section) -> tuple[str, dict]:
    """
    Sinopse: parágrafos longos antes da ficha técnica.
    Ficha técnica: parágrafo(s) com chaves conhecidas.
    """
    synopsis   = ""
    ficha_text = ""
    in_ficha   = False

    for p in section.find_all("p"):
        t = p.get_text(" ", strip=True)
        if not t or len(t) < 5:
            continue

        if _FICHA_START_RE.match(t):
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
    Ficha técnica em texto corrido separado por " | " ou em linha própria.
    Ex: "Texto: X | Encenação: Y | Interpretação: Z"
    """
    ficha = {}

    known_keys = [
        ("texto",          r"[Tt]exto\s*[:\|]\s*"),
        ("encenação",      r"[Ee]ncena[çc][aã]o\s*[:\|]\s*"),
        ("dramaturgia",    r"[Dd]ramaturgia\s*[:\|]\s*"),
        ("direção",        r"[Dd]ire[çc][aã]o\s+[Aa]rt[íi]stica\s*[:\|]\s*|[Dd]ire[çc][aã]o\s*[:\|]\s*"),
        ("tradução",       r"[Tt]radu[çc][aã]o\s*[:\|]\s*"),
        ("cenografia",     r"[Cc]enografia\s*(?:e\s+[Ff]igurinos?)?\s*[:\|]\s*"),
        ("figurinos",      r"[Ff]igurinos?\s*[:\|]\s*"),
        ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\|]\s*|[Dd]esign\s+de\s+[Ll]uz\s*[:\|]\s*"),
        ("som",            r"[Dd]esenho\s+de\s+[Ss]om\s*[:\|]\s*|[Dd]esign\s+de\s+[Ss]om\s*[:\|]\s*|[Ss]onoplastia\s*[:\|]\s*"),
        ("música",         r"[Mm][úu]sica\s+e\s+[Dd]esign\s+de\s+[Ss]om\s*[:\|]\s*|[Mm][úu]sica\s*[:\|]\s*|[Cc]omposi[çc][aã]o\s+[Mm]usical\s*[:\|]\s*"),
        ("interpretação",  r"[Ii]nterpreta[çc][aã]o\s*[:\|]\s*|[Cc]om\s*[:\|]\s*"),
        ("concepção",      r"[Cc]oncep[çc][aã]o\s+e\s+[Ii]nterpreta[çc][aã]o\s*[:\|]\s*|[Cc]oncep[çc][aã]o\s*[:\|]\s*"),
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
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\s*(?:©)\s*", value, flags=re.IGNORECASE)[0]
        value      = value.rstrip(" |;,").strip()[:300]
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _extract_subtitle(technical_sheet: dict, full_text: str) -> str:
    """
    Tenta inferir subtítulo (autor/companhia) a partir da ficha técnica.
    Prioridade: texto > encenação > companhia mencionada.
    """
    if technical_sheet.get("texto"):
        return technical_sheet["texto"]
    # Tentar extrair companhia — "Teatro GRIOT", "Companhia X"
    m = re.search(
        r"\b(Teatro\s+\w+|Companhia\s+[\w\s]+?)\b",
        full_text,
    )
    if m:
        candidate = m.group(1).strip()
        # Evitar "Teatro do Bairro" como subtítulo
        if "bairro" not in candidate.lower():
            return candidate
    return ""
