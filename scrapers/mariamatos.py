"""
Scraper: Teatro Maria Matos
Listagem: https://teatromariamatos.pt/tipo/programacao/  (todas as categorias, paginado)
URLs eventos: /espetaculos/<slug>/

Notas de implementação
──────────────────────
• O WordPress guarda os títulos em CAPS LOCK — normalização feita aqui via
  _smart_title() que preserva acrónimos reconhecidos e nomes próprios comuns.
• Categorias retiradas dos links /tipo/<cat>/ de cada card; passadas por
  normalize_category() do schema.
• Datas retiradas da listagem (texto livre) e da página individual com fallback.
• build_sessions() chamado com date_start, date_end, schedule para gerar sessions[].
• Nunca se chama harmonize() nem validate() aqui.
"""

import re
import time
import logging
from datetime import datetime, date as _date
from bs4 import BeautifulSoup
import requests

from scrapers.utils import (
    make_id,
    log,
    HEADERS,
    can_scrape,
    truncate_synopsis,
    build_image_object,
    parse_date,
    parse_date_range,
    build_sessions,
)
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

BASE      = "https://teatromariamatos.pt"
AGENDA    = f"{BASE}/tipo/programacao/"

THEATER = {
    "id":          "mariamatos",
    "name":        "Teatro Maria Matos",
    "short":       "Maria Matos",
    "color":       "#e65100",
    "city":        "Lisboa",
    "address":     "Avenida Frei Miguel Contreiras, 52, 1700-213 Lisboa",
    "site":        "https://teatromariamatos.pt",
    "programacao": "https://teatromariamatos.pt/tipo/programacao/",
    "lat":         38.7466,
    "lng":         -9.1365,
    "salas":       ["Grande Sala", "Sala Estúdio"],
    "aliases":     ["maria matos", "tmm", "teatro maria matos"],
    "description": (
        "O Teatro Maria Matos é um espaço de referência para as artes performativas "
        "contemporâneas em Lisboa, com programação inovadora de teatro, dança, música e performance."
    ),
    "logo_url":    "https://teatromariamatos.pt/wp-content/uploads/2023/03/logo-light.png",
    "favicon_url": "https://teatromariamatos.pt/wp-content/uploads/2023/03/cropped-favicon-32x32.png",
    "facade_url":  "https://teatromariamatos.pt/wp-content/uploads/2023/03/teatro-maria-matos-fachada.jpg",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

# ─────────────────────────────────────────────────────────────
# Normalização de títulos em CAPS LOCK
# ─────────────────────────────────────────────────────────────

# Palavras que devem manter-se em minúsculas dentro de um título
_LOWER_WORDS = {
    "a", "à", "ao", "aos", "as", "às", "de", "da", "das", "do", "dos",
    "e", "em", "o", "os", "um", "uma", "por", "para", "com", "que",
    "na", "nas", "no", "nos", "se", "the", "a", "of", "and", "in",
}

# Acrónimos/abreviaturas conhecidos que devem manter-se em maiúsculas
_KEEP_UPPER = {
    "LAP", "TMM", "DJ", "MC", "CCB", "TM", "SP", "UK", "USA",
    "PT", "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI",
}


def _smart_title(raw: str) -> str:
    """
    Converte string em CAPS LOCK para title case de forma inteligente:
    - Preserva acrónimos de _KEEP_UPPER
    - Minuscula artigos/preposições no interior do título
    - Respeita separadores "|" e "–" como início de nova «frase»
    - Se o texto original NÃO estiver todo em caps, devolve-o sem alteração.
    """
    if not raw:
        return raw
    # Se já tem minúsculas, não tocar (pode ser título misto intencional)
    letters = [c for c in raw if c.isalpha()]
    if letters and sum(1 for c in letters if c.islower()) / len(letters) > 0.3:
        return raw.strip()

    # Dividir por separadores que reiniciam o capitalise
    separators = re.compile(r"(\s*[|–—]\s*)")
    parts = separators.split(raw)
    result_parts = []
    for part in parts:
        if separators.fullmatch(part):
            result_parts.append(part)
            continue
        words = part.split()
        out = []
        for i, word in enumerate(words):
            # Remove pontuação circundante para comparar
            core = re.sub(r"^[^\w]+|[^\w]+$", "", word, flags=re.UNICODE).upper()
            if core in _KEEP_UPPER:
                out.append(word.upper())
            elif i > 0 and core.lower() in _LOWER_WORDS:
                out.append(word.lower())
            else:
                out.append(word.capitalize())
        result_parts.append(" ".join(out))
    return "".join(result_parts).strip()


# ─────────────────────────────────────────────────────────────
# Scrape principal
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    # Recolher todos os URLs de eventos de todas as páginas da listagem
    event_urls: list[tuple[str, str]] = []   # (url, raw_category)
    page = 1
    seen_urls: set[str] = set()

    while True:
        url = AGENDA if page == 1 else f"{AGENDA}page/{page}/"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
        except Exception as e:
            log(f"[Maria Matos] Erro na listagem p{page}: {e}")
            break

        soup = BeautifulSoup(r.text, "lxml")
        items = soup.select("ul.portfolio-list > li, .portfolio-loop li, article")

        # Fallback: qualquer <li> que contenha link para /espetaculos/
        if not items:
            items = [
                a.find_parent("li") or a.find_parent("article")
                for a in soup.find_all("a", href=re.compile(r"/espetaculos/"))
                if a.find_parent("li") or a.find_parent("article")
            ]
            # dedup
            items = list({id(i): i for i in items if i}.values())

        found_new = False
        for item in items:
            a_ev = item.find("a", href=re.compile(r"/espetaculos/"))
            if not a_ev:
                continue
            href = a_ev["href"]
            full = href if href.startswith("http") else BASE + href
            if full in seen_urls:
                continue
            seen_urls.add(full)
            found_new = True

            # Categoria raw da listagem (pode ser múltipla, ex: "Dança, Infantil, Música")
            cat_links = item.find_all("a", href=re.compile(r"/tipo/"))
            raw_cat = ", ".join(
                a.get_text(strip=True) for a in cat_links
                if "arquivo" not in a["href"]
            )
            event_urls.append((full, raw_cat))

        # Verificar se há página seguinte
        next_link = soup.find("a", string=re.compile(r"Next|Seguinte", re.I))
        if not next_link and not soup.find("a", href=re.compile(rf"page/{page + 1}")):
            break
        if not found_new:
            break
        page += 1
        time.sleep(0.5)

    log(f"[Maria Matos] {len(event_urls)} URLs recolhidos em {page} páginas")

    events = []
    for ev_url, raw_cat in event_urls:
        ev = _scrape_event(ev_url, raw_cat)
        if ev:
            events.append(ev)
        time.sleep(0.3)

    log(f"[Maria Matos] {len(events)} eventos extraídos")
    return events


# ─────────────────────────────────────────────────────────────
# Scrape de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str, raw_cat_listing: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[Maria Matos] Erro em {url}: {e}")
        return None

    soup  = BeautifulSoup(r.text, "lxml")
    text  = soup.get_text(" ", strip=True)

    # ── Título ──────────────────────────────────────────────
    h1 = soup.select_one("h1")
    if not h1:
        return None
    raw_title = h1.get_text(strip=True)
    if not raw_title or len(raw_title) < 2:
        return None
    title = _smart_title(raw_title)

    # ── Subtítulo ───────────────────────────────────────────
    # Padrão: "TÍTULO | subtítulo" ou "TÍTULO — subtítulo"
    subtitle = ""
    sep_m = re.search(r"[|–—]\s*(.+)", raw_title)
    if sep_m:
        subtitle = _smart_title(sep_m.group(1)).strip()

    # ── Categoria ───────────────────────────────────────────
    # Preferir categoria da página individual sobre a da listagem
    cat_links_page = soup.find_all("a", href=re.compile(r"/tipo/(?!arquivo)"))
    raw_cat_page = ", ".join(a.get_text(strip=True) for a in cat_links_page) if cat_links_page else ""
    raw_cat = raw_cat_page or raw_cat_listing or "multidisciplinar"
    # normalize_category espera uma string simples; passar a primeira categoria
    first_cat = re.split(r"[,/]", raw_cat)[0].strip()
    category = normalize_category(first_cat)

    # ── Imagem ──────────────────────────────────────────────
    image = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content", "").startswith("http"):
        image = build_image_object(og["content"], soup, THEATER_NAME, url)

    # ── Bilheteira ──────────────────────────────────────────
    ticket_url = ""
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if any(kw in href for kw in ("ticketline", "bol.pt", "bilhete", "comprar")):
            ticket_url = href
            break

    # ── Preço ────────────────────────────────────────────────
    price_info = ""
    price_min: float | None = None
    price_max: float | None = None
    pm = re.search(
        r"(Entrada\s+livre"
        r"|[Gg]ratu[íi]to"
        r"|\d+(?:[,\.]\d+)?\s*€\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?[-–]\d+(?:[,\.]\d+)?\s*€"
        r"|\d+(?:[,\.]\d+)?\s*€)",
        text, re.IGNORECASE,
    )
    if pm:
        price_info = pm.group(1).strip()
        nums = re.findall(r"\d+(?:[,\.]\d+)?", price_info)
        floats = [float(n.replace(",", ".")) for n in nums]
        if floats:
            price_min = min(floats)
            price_max = max(floats)

    # ── Duração ──────────────────────────────────────────────
    duration     = ""
    duration_min = None
    dm = re.search(r"(\d+)\s*min\.?", text, re.IGNORECASE)
    if dm:
        duration_min = int(dm.group(1))
        duration     = f"{duration_min} min"

    # ── Classificação etária ─────────────────────────────────
    age_rating = ""
    age_min: int | None = None
    am = re.search(r"\b(M\s*/\s*(\d+)|[Ll]ivre)\b", text)
    if am:
        age_rating = am.group(1).replace(" ", "")
        if am.group(2):
            age_min = int(am.group(2))

    # ── Datas e horários ─────────────────────────────────────
    date_start, date_end, dates_label, schedule, sessions = _parse_dates(text, soup)

    # ── Sinopse ──────────────────────────────────────────────
    synopsis = _extract_synopsis(soup, url)

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_ficha(text)

    event: dict = {
        "id":         make_id(SOURCE_SLUG, title),
        "title":      title,
        "theater":    THEATER_NAME,
        "category":   category,
        "source_url": url,
    }
    if subtitle:
        event["subtitle"] = subtitle
    if dates_label:
        event["dates_label"] = dates_label
    if date_start:
        event["date_start"] = date_start
    if date_end:
        event["date_end"] = date_end
    if schedule:
        event["schedule"] = schedule
    if sessions:
        event["sessions"] = sessions
    if synopsis:
        event["synopsis"] = synopsis
    if image:
        event["image"] = image
    if ticket_url:
        event["ticket_url"] = ticket_url
    if price_info:
        event["price_info"] = price_info
    if price_min is not None:
        event["price_min"] = price_min
    if price_max is not None:
        event["price_max"] = price_max
    if duration:
        event["duration"] = duration
    if duration_min is not None:
        event["duration_min"] = duration_min
    if age_rating:
        event["age_rating"] = age_rating
    if age_min is not None:
        event["age_min"] = age_min
    if technical_sheet:
        event["technical_sheet"] = technical_sheet

    return event


# ─────────────────────────────────────────────────────────────
# Parse de datas e horários
# ─────────────────────────────────────────────────────────────

# Ex: "18 março – 17 maio"  ou  "24 e 25 de Março 2026"
_MONTHS_PT = (
    r"(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|"
    r"setembro|outubro|novembro|dezembro|"
    r"jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)"
)
_DATE_WORD = rf"\d{{1,2}}\s+(?:de\s+)?{_MONTHS_PT}(?:\s+\d{{4}})?"
_RANGE_RE  = re.compile(
    rf"({_DATE_WORD})\s*[–—-]\s*({_DATE_WORD})",
    re.IGNORECASE,
)
_SINGLE_DATE_RE = re.compile(
    rf"({_DATE_WORD})",
    re.IGNORECASE,
)

# Ex: "domingos · 17:00"  ou  "quintas · 21:00"
_SCHED_LINE_RE = re.compile(
    r"(domingos?|segundas?|terças?|quartas?|quintas?|sextas?|sábados?)"
    r"\s*[·•]\s*(\d{1,2}:\d{2})",
    re.IGNORECASE,
)

# Ex: "terça 17 março • 21:00"  (sessão avulsa)
_SESSION_AVULSA_RE = re.compile(
    r"(?:segunda|terça|quarta|quinta|sexta|sábado|domingo)\s+"
    rf"({_DATE_WORD})"
    r"\s*[·•,]\s*(\d{{1,2}}:\d{{2}})",
    re.IGNORECASE,
)


def _parse_dates(text: str, soup) -> tuple[str, str, str, str, list[dict]]:
    """Devolve (date_start, date_end, dates_label, schedule, sessions)."""
    date_start = date_end = dates_label = schedule = ""
    sessions: list[dict] = []

    # 1) Tentar range de datas
    rm = _RANGE_RE.search(text)
    if rm:
        ds, de = parse_date_range(rm.group(0))
        if ds:
            date_start  = ds
            date_end    = de or ds
            dates_label = rm.group(0).strip()

    # 2) Horários recorrentes por dia da semana
    sched_parts = []
    for m in _SCHED_LINE_RE.finditer(text):
        sched_parts.append(f"{m.group(1).capitalize()} · {m.group(2)}")
    if sched_parts:
        schedule = " | ".join(sched_parts)

    # 3) Sessões avulsas
    avulsas: list[tuple[str, str]] = []
    for m in _SESSION_AVULSA_RE.finditer(text):
        d = parse_date(m.group(1))
        if d:
            avulsas.append((d, m.group(2)))

    # 4) Se não temos range mas temos sessões avulsas, inferir start/end
    if not date_start and avulsas:
        avulsas.sort()
        date_start = avulsas[0][0]
        date_end   = avulsas[-1][0]
        dates_label = f"{date_start} – {date_end}" if len(avulsas) > 1 else date_start

    # 5) Se ainda não temos nada, última tentativa: data solta
    if not date_start:
        sm = _SINGLE_DATE_RE.search(text)
        if sm:
            date_start = parse_date(sm.group(1)) or ""
            date_end   = date_start
            dates_label = sm.group(1).strip()

    # 6) Construir sessions via build_sessions (recorrentes) + avulsas
    if date_start and (schedule or avulsas):
        try:
            sessions = build_sessions(date_start, date_end, schedule)
        except Exception:
            sessions = []
        # Adicionar avulsas não incluídas
        existing_dates = {s["date"] for s in sessions}
        for d, h in avulsas:
            if d not in existing_dates:
                try:
                    wd = _WEEKDAYS_SHORT[_date.fromisoformat(d).weekday()]
                except Exception:
                    wd = ""
                sessions.append({"date": d, "time": h, "weekday": wd})
        sessions.sort(key=lambda s: s["date"])

    return date_start, date_end, dates_label, schedule, sessions


_WEEKDAYS_SHORT = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


# ─────────────────────────────────────────────────────────────
# Sinopse
# ─────────────────────────────────────────────────────────────

_FICHA_MARKERS = re.compile(
    r"^(Autor|Texto|Encena[çc]|Tradu[çc]|Cen[áa]rio|Cenografia|"
    r"Figurino|Luz|Som|M[úu]sica|Composi[çc]|Coreografia|"
    r"Produ[çc]|Coprodu[çc]|Dire[çc]|Interpreta[çc]|Com\s+[A-Z])",
    re.IGNORECASE,
)


def _extract_synopsis(soup, url: str) -> str:
    # 1) og:description se não for genérica
    og = soup.find("meta", property="og:description")
    og_text = og.get("content", "").strip() if og else ""
    if og_text and len(og_text) > 40 and "Teatro Maria Matos" not in og_text[:30]:
        return truncate_synopsis(og_text)

    # 2) Parágrafos do <main> / <article> que não sejam ficha técnica
    chunks = []
    for p in soup.select("main p, article p, .entry-content p, .post-content p"):
        t = p.get_text(strip=True)
        if len(t) < 40:
            continue
        if _FICHA_MARKERS.match(t):
            continue
        chunks.append(t)
        if sum(len(c) for c in chunks) > 800:
            break
    if chunks:
        return truncate_synopsis(" ".join(chunks))

    return og_text  # fallback mesmo que curto


# ─────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────

_FICHA_KEYS = [
    ("texto",         r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?\s*[:\s]\s*"),
    ("autor",         r"[Aa]utor[a]?\s*[:\s]\s*"),
    ("dramaturgia",   r"[Dd]ramaturgia\s*[:\s]\s*"),
    ("encenação",     r"[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
    ("tradução",      r"[Tt]radu[çc][aã]o\s*[:\s]\s*"),
    ("adaptação",     r"[Aa]dapta[çc][aã]o\s*[:\s]\s*"),
    ("cenário",       r"[Cc]en[aá]rio\s*[:\s]\s*"),
    ("cenografia",    r"[Cc]enografia\s*[:\s]\s*"),
    ("figurinos",     r"[Ff]igurinos?\s*[:\s]\s*"),
    ("luz",           r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\s]\s*|[Ii]lumina[çc][aã]o\s*[:\s]\s*"),
    ("som",           r"[Dd]esenho\s+de\s+[Ss]om\s*[:\s]\s*|[Ss]onoplastia\s*[:\s]\s*"),
    ("música",        r"[Mm][úu]sica\s*[:\s]\s*|[Cc]omposi[çc][aã]o\s*[:\s]\s*"),
    ("coreografia",   r"[Cc]oreografia(?:\s+e\s+movimento)?\s*[:\s]\s*"),
    ("direção",       r"[Dd]ire[çc][aã]o\s*[:\s]\s*"),
    ("produção",      r"[Pp]rodu[çc][aã]o\s*[:\s]\s*"),
    ("coprodução",    r"[Cc]oprodu[çc][aã]o\s*[:\s]\s*"),
    ("ass_encenação", r"[Aa]ss(?:istente)?\.?\s+(?:de\s+)?[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
    ("interpretação", r"[Ii]nterpreta[çc][aã]o\s*[:\s]\s*"),
    ("elenco",        r"[Cc]om\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
]


def _parse_ficha(text: str) -> dict:
    ficha     = {}
    positions = []
    for key, pattern in _FICHA_KEYS:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        value = re.sub(r"\s+", " ", text[end:next_start].strip())
        # Cortar lixo no campo interpretação/elenco
        if key in ("interpretação", "elenco"):
            value = re.split(r"\s+(?:M/\d+|Todos |Uma |Com |Para |O |A |As |Os )", value)[0]
        value = value[:300].strip()
        if value and key not in ficha:
            ficha[key] = value
    return ficha
