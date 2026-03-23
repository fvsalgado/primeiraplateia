"""
Scraper: TAGV — Teatro Académico Gil Vicente
URL base: https://tagv.pt
Listagem: https://tagv.pt/agenda/

Estratégia
──────────
1. Faz GET a https://tagv.pt/agenda/ (HTML estático, sem JS necessário)
2. Extrai da secção de lista (#mostra-list) os dados disponíveis de cada evento:
   - título, URL, imagem thumbnail, categoria, dia, mês (do cabeçalho .o-mes)
3. Visita cada página de evento individual em paralelo (ThreadPoolExecutor)
   para completar todos os campos
4. Não filtra por categoria — importa todos os eventos

Notas sobre o HTML da listagem
───────────────────────────────
A listagem tem três representações do mesmo evento:
  A) Secção #mostra-list (lista cronológica com dia + hora)
  B) Secção .thumb-content (grelha de miniaturas)
  C) Secção .list-content (outra grelha)

Usamos A) como fonte principal porque tem o dia e a hora.
A estrutura de A) é:
  <li class="row o-mes">   ← cabeçalho de mês (ex: "jan", "fev")
  <li class="evento">
    <div class="cat-link-wrapper"><a class="cat-link">Categoria</a></div>
    <a class="link" href="/agenda/slug/" data-picture="URL_THUMB" data-title="Título">
      <div class="xxlarge-1 ... evento-dias">
        <p>05</p>   ← dia
        <hr>
        <p>21h30</p>  ← hora(s)
      </div>
      <div class="xxlarge-9 ...">
        <h3 class="evento-titulo">Título</h3>
      </div>
    </a>
  </li>
"""

import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    make_id,
    log,
    HEADERS,
    can_scrape,
    build_image_object,
    build_sessions,
    truncate_synopsis,
    parse_date_range,
)
from scrapers.schema import normalize_category

BASE      = "https://tagv.pt"

# ─────────────────────────────────────────────────────────────
# Mapa de categorias raw do TAGV → valor para normalize_category
# ─────────────────────────────────────────────────────────────
_TAGV_CATEGORY_MAP: dict[str, str] = {
    "teatro":              "teatro",
    "dança":               "dança",
    "música":              "música",
    "concerto":            "concerto",
    "cinema":              "cinema",
    "filme":               "cinema",
    "documentário":        "documentário",
    "exposição":           "exposição",
    "performance":         "performance",
    "workshop":            "workshop",
    "oficina":             "workshop",
    "conferência":         "conferência",
    "conversa":            "conversa",
    "debate":              "debate",
    "leitura":             "leitura",
    "poesia":              "poesia",
    "festival":            "festival",
    "infantil":            "infantil",
    "família":             "família",
    "residência":          "residência",
}

import re as _re

def _infer_category_tagv(title: str, full_text: str) -> str:
    """
    Inferência de categoria para eventos TAGV sem categoria explícita,
    a partir de padrões no título e texto da página.
    """
    t = (title + " " + full_text[:300]).lower()

    if _re.search(r"\bfilm[e]?\b|cinema|doc\.coimbra|cineeco|labirinto de sombras|festival de cinema|sess[aã]o.*film|proje[cç][aã]o", t):
        return "cinema"
    if _re.search(r"\bconvert[ao]\b|conversa[s]?\b|ciclo de convers|convers.*basc[aã]o|di[aá]logo|debate|col[oó]quio|painel\b|confer[eê]ncia", t):
        return "conversa"
    if _re.search(r"\bpoesia\b|declam|spoken word|po[eé]tico|leituras? (queer|indisciplin|encen)", t):
        return "poesia"
    if _re.search(r"\bleitura[s]?\b|lançamento.*livro|apresenta[cç][aã]o.*livro|clube de leitura", t):
        return "leitura"
    if _re.search(r"\boficina\b|workshop\b|laborat[oó]rio\b|forma[cç][aã]o\b|resid[eê]ncia\b|bolsa\b|candidatura", t):
        return "workshop"
    if _re.search(r"\bconcerto\b|m[uú]sica\b|tun[ao]\b|orquestra\b|coro\b|banda\b|recital\b|jazz|fado|cantora?\b", t):
        return "concerto"
    if _re.search(r"\bfestival\b", t):
        return "festival"
    if _re.search(r"\bexpos[ií][cç][aã]o\b|instala[cç][aã]o\b|galeria\b", t):
        return "exposição"
    if _re.search(r"\binfantil\b|para\s+crian[cç]as?\b|espet[aá]culo\s+infantil|teatro\s+infantil|fam[íi]li[ao]\b|beb[eé]s?\b", t):
        return "infantil"
    if _re.search(r"\bdan[cç]a\b|bailado\b|coreografi", t):
        return "dança"
    if _re.search(r"\bteatr[ao]\b|pe[cç]a\b|encena[cç][aã]o\b|dramaturgi", t):
        return "teatro"
    if _re.search(r"\bperformance\b|acontecimento\b", t):
        return "performance"
    if _re.search(r"\bvisita[s]? guiada[s]?\b|percurso\b|caminhada\b", t):
        return "visita guiada"
    return "multidisciplinar"

AGENDA    = f"{BASE}/agenda/"

# Paralelismo: pedidos de detalhe em simultâneo
_DETAIL_WORKERS = 6
_DETAIL_SLEEP   = 0.1   # segundos entre pedidos por worker

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

THEATER = {
    "id":          "tagv",
    "name":        "TAGV — Teatro Académico Gil Vicente",
    "short":       "TAGV",
    "color":       "#000000",
    "city":        "Coimbra",
    "address":     "Praça da República, 3000-343 Coimbra",
    "site":        "https://tagv.pt",
    "programacao": "https://tagv.pt/agenda/",
    "lat":         40.2093,
    "lng":         -8.4206,
    "salas":       ["Grande Auditório", "Sala Estúdio"],
    "aliases":     [
        "tagv", "teatro académico gil vicente",
        "teatro academico gil vicente", "gil vicente", "tagv coimbra",
    ],
    "description": (
        "O Teatro Académico Gil Vicente (TAGV) é uma estrutura da Universidade "
        "de Coimbra. Inaugurado em 1961, remodelado em 2003, é um polo de "
        "conhecimento, formação e programação artísticos em Coimbra."
    ),
    "logo_url":    "https://tagv.pt/public/imgs/id/favicon-32x32.png",
    "favicon_url": "https://tagv.pt/public/imgs/id/favicon-32x32.png",
    "facade_url":  "https://tagv.pt/public/uploads/2020/05/tagv.pt-estamos-de-volta-ivotavares-05.jpg",
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]


# ═══════════════════════════════════════════════════════════════
# Ponto de entrada
# ═══════════════════════════════════════════════════════════════

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"[{SOURCE_SLUG}] robots.txt: scraping bloqueado para {BASE}")
        return []

    listing_items = _parse_listing()
    log(f"[{SOURCE_SLUG}] {len(listing_items)} itens na listagem")

    if not listing_items:
        return []

    # ── Pedidos de detalhe em paralelo ────────────────────────
    events_raw: list[tuple[int, dict]] = []   # (índice_original, evento)
    lock = threading.Lock()

    def fetch_item(idx_item: tuple[int, dict]) -> tuple[int, dict | None]:
        idx, item = idx_item
        time.sleep(_DETAIL_SLEEP)
        try:
            return idx, _scrape_event(item)
        except Exception as exc:
            log(f"[{SOURCE_SLUG}] Erro em {item.get('url', '?')}: {exc}")
            return idx, None

    with ThreadPoolExecutor(max_workers=_DETAIL_WORKERS) as executor:
        futures = {
            executor.submit(fetch_item, (idx, item)): idx
            for idx, item in enumerate(listing_items)
        }
        for future in as_completed(futures):
            idx, ev = future.result()
            if ev is not None:
                with lock:
                    events_raw.append((idx, ev))

    # Ordenar pela ordem original da listagem (cronológica)
    events_raw.sort(key=lambda x: x[0])

    # Deduplicar por ID (pode haver eventos duplicados na listagem)
    seen_ids: set[str] = set()
    events: list[dict] = []
    for _, ev in events_raw:
        eid = ev["id"]
        if eid not in seen_ids:
            seen_ids.add(eid)
            events.append(ev)

    log(f"[{SOURCE_SLUG}] {len(events)} eventos finais")
    return events


# ═══════════════════════════════════════════════════════════════
# Parsing da listagem principal
# ═══════════════════════════════════════════════════════════════

def _parse_listing() -> list[dict]:
    """
    Faz GET a /agenda/ e extrai os dados de cada <li class="evento">
    na secção #mostra-list.
    """
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{SOURCE_SLUG}] Erro na listagem: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")

    mostra = soup.find(id="mostra-list")
    if not mostra:
        mostra = soup.find("div", class_="normal-content")
    if not mostra:
        log(f"[{SOURCE_SLUG}] Não encontrei #mostra-list — a tentar toda a página")
        mostra = soup

    items = []
    current_month = 0
    current_year  = _infer_year()

    for li in mostra.find_all("li"):
        classes = li.get("class", [])

        if "o-mes" in classes:
            mes_text = li.get_text(strip=True).lower()
            n = _month_num(mes_text)
            if n:
                if items and n < current_month:
                    current_year += 1
                current_month = n
            continue

        if "evento" not in classes:
            continue

        link_el = li.find("a", class_="link")
        if not link_el:
            continue

        href = link_el.get("href", "")
        if not href or "?" in href:
            continue

        if href.startswith("/"):
            url = BASE + href
        elif href.startswith("http"):
            url = href
        else:
            continue

        title = link_el.get("data-title", "").strip()
        if not title:
            h3 = link_el.find("h3")
            title = h3.get_text(strip=True) if h3 else ""
        if not title or len(title) < 2:
            continue

        thumb_url = link_el.get("data-picture", "").strip()

        cat_el = li.find("a", class_="cat-link")
        category_raw = cat_el.get_text(strip=True) if cat_el else ""

        day = 0
        dias_div = link_el.find("div", class_="evento-dias")
        ps = []
        if dias_div:
            ps = dias_div.find_all("p")
            if ps:
                try:
                    day = int(ps[0].get_text(strip=True))
                except ValueError:
                    pass

        schedule_parts = []
        if dias_div and len(ps) > 1:
            raw_time = ps[1].get_text(separator="\n", strip=True)
            for part in raw_time.split("\n"):
                t = part.strip()
                m = re.match(r"(\d{1,2})[h:](\d{2})", t)
                if m:
                    hh, mm = int(m.group(1)), int(m.group(2))
                    if 7 <= hh <= 23:
                        schedule_parts.append(f"{hh:02d}:{mm:02d}")
        schedule = " | ".join(schedule_parts) if schedule_parts else ""

        items.append({
            "url":          url,
            "title":        title,
            "category_raw": category_raw,
            "day":          day,
            "month_num":    current_month,
            "year":         current_year,
            "schedule":     schedule,
            "thumb_url":    thumb_url,
        })

    return items


# ═══════════════════════════════════════════════════════════════
# Scraping de página de evento individual
# ═══════════════════════════════════════════════════════════════

def _scrape_event(item: dict) -> dict | None:
    url = item["url"]

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{SOURCE_SLUG}] Erro em {url}: {e}")
        return None

    soup = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    title = item["title"]
    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(strip=True)
        if h1_text and len(h1_text) > 2:
            title = h1_text
    if not title:
        return None

    cat_raw = item["category_raw"]
    for a in soup.find_all("a", href=re.compile(r"\?categoria=")):
        t = a.get_text(strip=True)
        if t:
            cat_raw = t
            break
    category = normalize_category(cat_raw) if cat_raw else _infer_category_tagv(title, full_text)

    day       = item["day"]
    month_num = item["month_num"]
    year      = item["year"]

    if day and month_num and year:
        date_start = f"{year}-{month_num:02d}-{day:02d}"
    else:
        date_start = ""

    date_end    = date_start
    dates_label = _build_dates_label(day, month_num, year)

    dl, ds, de = _parse_dates_from_page(soup, full_text, date_start)
    if ds:
        date_start  = ds
        date_end    = de or ds
        dates_label = dl or dates_label
    elif not date_start:
        return None

    schedule = item.get("schedule", "")
    if not schedule:
        schedule = _extract_schedule_from_page(full_text)

    image = None
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content", "").startswith("http"):
        image = build_image_object(og_img["content"], soup, THEATER_NAME, url)
    if not image and item.get("thumb_url", "").startswith("http"):
        image = build_image_object(item["thumb_url"], soup, THEATER_NAME, url)

    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        s = og_desc.get("content", "").strip()
        if s and "Teatro Académico de Gil Vicente" not in s and len(s) > 40:
            synopsis = s
    if not synopsis:
        synopsis = _extract_synopsis(soup)

    subtitle    = _extract_subtitle(soup, full_text)
    ticket_url  = _extract_ticket_url(soup)
    price_info, price_min, price_max = _parse_price(full_text)
    duration, duration_min           = _parse_duration(full_text)
    age_rating, age_min              = _parse_age(full_text)
    accessibility                    = _parse_accessibility(full_text)
    technical_sheet                  = _parse_ficha(soup, full_text)
    cast   = _extract_list_from_ficha(technical_sheet, "interpretação")
    people = _extract_people(technical_sheet)

    ev: dict = {
        "id":         make_id(SOURCE_SLUG, title),
        "title":      title,
        "theater":    THEATER_NAME,
        "category":   category,
        "date_start": date_start,
        "source_url": url,
    }

    if date_end and date_end != date_start:
        ev["date_end"] = date_end
    if dates_label:
        ev["dates_label"] = dates_label
    if subtitle:
        ev["subtitle"] = subtitle
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
    if duration_min:
        ev["duration_min"] = duration_min
    if age_rating:
        ev["age_rating"] = age_rating
    if age_min is not None:
        ev["age_min"] = age_min
    if accessibility:
        ev["accessibility"] = accessibility
    if technical_sheet:
        ev["technical_sheet"] = technical_sheet
    if cast:
        ev["cast"] = cast
    if people:
        ev["people"] = people
    if schedule:
        ev["sessions"] = build_sessions(date_start, date_end or date_start, schedule)

    return ev


# ═══════════════════════════════════════════════════════════════
# Helpers — datas
# ═══════════════════════════════════════════════════════════════

def _infer_year() -> int:
    import datetime
    return datetime.date.today().year


def _month_num(s: str) -> int:
    s = s.strip().lower()
    if s in _PT_MONTHS:
        return _PT_MONTHS[s]
    return _PT_MONTHS.get(s[:3], 0)


def _build_dates_label(day: int, month_num: int, year: int) -> str:
    if not (day and month_num and year):
        return ""
    months_abbr = ["", "jan", "fev", "mar", "abr", "mai", "jun",
                   "jul", "ago", "set", "out", "nov", "dez"]
    m = months_abbr[month_num] if 1 <= month_num <= 12 else ""
    return f"{day} {m} {year}" if m else ""


def _parse_dates_from_page(
    soup, text: str, fallback_start: str
) -> tuple[str, str, str]:
    time_tags = [
        t["datetime"][:10]
        for t in soup.find_all("time", attrs={"datetime": True})
        if re.match(r"^\d{4}-\d{2}-\d{2}", t.get("datetime", ""))
    ]
    if time_tags:
        time_tags.sort()
        ds = time_tags[0]
        de = time_tags[-1]
        label = ds if ds == de else f"{ds} – {de}"
        return label, ds, de

    try:
        result = parse_date_range(text[:600])
        if result and result[0]:
            ds, de = result
            label = ds if (not de or ds == de) else f"{ds} – {de}"
            return label, ds, de or ds
    except Exception:
        pass

    patterns = [
        r"(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})"
        r"\s*[–—\-]+\s*"
        r"(\d{1,2})\s+([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(\d{4})",
        r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+"
        r"(?:de\s+)?([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(?:de\s+)?(\d{4})",
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,})\s+(?:de\s+)?(\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if not m:
            continue
        g = m.groups()
        if len(g) == 5:
            d1, mo1, d2, mo2, y = g
            n1, n2 = _month_num(mo1), _month_num(mo2)
            if n1 and n2:
                ds = f"{y}-{n1:02d}-{int(d1):02d}"
                de = f"{y}-{n2:02d}-{int(d2):02d}"
                return f"{d1} {mo1} – {d2} {mo2} {y}", ds, de
        elif len(g) == 4:
            d1, d2, mo, y = g
            n = _month_num(mo)
            if n:
                ds = f"{y}-{n:02d}-{int(d1):02d}"
                de = f"{y}-{n:02d}-{int(d2):02d}"
                return f"{d1} – {d2} {mo} {y}", ds, de
        elif len(g) == 3:
            d, mo, y = g
            n = _month_num(mo)
            if n:
                ds = f"{y}-{n:02d}-{int(d):02d}"
                return f"{d} {mo} {y}", ds, ds

    return "", "", ""


# ═══════════════════════════════════════════════════════════════
# Helpers — horário
# ═══════════════════════════════════════════════════════════════

def _extract_schedule_from_page(text: str) -> str:
    times = []
    seen  = set()
    for m in re.finditer(r"\b(\d{1,2})[h:](\d{2})\b", text):
        hh, mm = int(m.group(1)), int(m.group(2))
        if 7 <= hh <= 23:
            t = f"{hh:02d}:{mm:02d}"
            if t not in seen:
                seen.add(t)
                times.append(t)
    return " | ".join(times[:4])


# ═══════════════════════════════════════════════════════════════
# Helpers — sinopse e subtítulo
# ═══════════════════════════════════════════════════════════════

_FICHA_LABELS = re.compile(
    r"^(texto|autor|encena[çc]|dramaturgia|dire[çc]|tradu[çc]|adapta[çc]|"
    r"cenografia|figurinos|luz|som|m[uú]sica|coreografia|interpreta[çc]|"
    r"produ[çc]|co.?produ[çc]|bilhetes|data|hora|pre[çc]|local|dura[çc]|"
    r"classif)",
    re.IGNORECASE,
)


def _extract_synopsis(soup) -> str:
    parts = []
    for p in soup.select("main p, article p, .entry-content p, .event-description p"):
        t = p.get_text(strip=True)
        if len(t) < 50:
            continue
        if _FICHA_LABELS.match(t):
            continue
        parts.append(t)
        if sum(len(x) for x in parts) > 800:
            break
    return " ".join(parts)


def _extract_subtitle(soup, text: str) -> str:
    h1 = soup.find("h1")
    if h1:
        nxt = h1.find_next_sibling()
        while nxt:
            tag = nxt.name
            if tag in ("h2", "h3", "p"):
                t = nxt.get_text(strip=True)
                if t and len(t) < 120 and not _FICHA_LABELS.match(t):
                    return t
            elif tag in ("div", "section", "article"):
                break
            nxt = nxt.find_next_sibling()
    return ""


# ═══════════════════════════════════════════════════════════════
# Helpers — bilhetes
# ═══════════════════════════════════════════════════════════════

_TICKET_PATTERNS = re.compile(
    r"tagv\.bol\.pt|bol\.pt/Comprar|ticketline|eventbrite|comprar[_-]bilhete|bilhete",
    re.IGNORECASE,
)


def _extract_ticket_url(soup) -> str:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _TICKET_PATTERNS.search(href):
            return href
    return ""


# ═══════════════════════════════════════════════════════════════
# Helpers — preço
# ═══════════════════════════════════════════════════════════════

def _parse_price(text: str) -> tuple[str, float | None, float | None]:
    if re.search(r"entrada\s+livre|gratuito|free", text, re.IGNORECASE):
        return "Entrada livre", 0.0, 0.0

    m = re.search(
        r"(\d+(?:[,\.]\d{1,2})?)\s*€\s*(?:[/\-–—]|a)\s*(\d+(?:[,\.]\d{1,2})?)\s*€",
        text,
    )
    if m:
        lo = float(m.group(1).replace(",", "."))
        hi = float(m.group(2).replace(",", "."))
        return f"{lo:.0f}€ – {hi:.0f}€", lo, hi

    m = re.search(r"(\d+(?:[,\.]\d{1,2})?)\s*€", text)
    if m:
        v = float(m.group(1).replace(",", "."))
        return f"{v:.0f}€", v, v

    return "", None, None


# ═══════════════════════════════════════════════════════════════
# Helpers — duração
# ═══════════════════════════════════════════════════════════════

def _parse_duration(text: str) -> tuple[str, int | None]:
    m = re.search(r"(\d+)\s*h\s*(\d+)\s*(?:min)?", text, re.IGNORECASE)
    if m:
        total = int(m.group(1)) * 60 + int(m.group(2))
        return f"{m.group(1)}h{m.group(2)}", total

    m = re.search(r"(\d+)\s*h(?:oras?)?\b", text, re.IGNORECASE)
    if m:
        total = int(m.group(1)) * 60
        return f"{m.group(1)}h", total

    m = re.search(r"(\d+)\s*min(?:utos?)?", text, re.IGNORECASE)
    if m:
        total = int(m.group(1))
        return f"{m.group(1)} min", total

    return "", None


# ═══════════════════════════════════════════════════════════════
# Helpers — classificação etária
# ═══════════════════════════════════════════════════════════════

def _parse_age(text: str) -> tuple[str, int | None]:
    m = re.search(r"M\s*/\s*(\d+)", text)
    if m:
        age = int(m.group(1))
        return f"M/{age}", age

    m = re.search(r"\+\s*(\d+)\s*anos?", text, re.IGNORECASE)
    if m:
        age = int(m.group(1))
        return f"+{age}", age

    if re.search(r"\blivre\b|\btodas\s+as\s+idades\b", text, re.IGNORECASE):
        return "Livre", 0

    return "", None


# ═══════════════════════════════════════════════════════════════
# Helpers — acessibilidade
# ═══════════════════════════════════════════════════════════════

def _parse_accessibility(text: str) -> list[str]:
    flags = {
        "Audiodescrição":         r"audiodescri[çc][aã]o",
        "LGP":                    r"\bLGP\b|l[íi]ngua\s+gestual",
        "Legendas em inglês":     r"legendas\s+em\s+ingl[eê]s",
        "Sobretitulação":         r"sobretitula[çc][aã]o",
        "Espetáculo relaxado":    r"espet[áa]culo\s+relaxado",
    }
    result = []
    for label, pattern in flags.items():
        if re.search(pattern, text, re.IGNORECASE):
            result.append(label)
    return result


# ═══════════════════════════════════════════════════════════════
# Helpers — ficha técnica
# ═══════════════════════════════════════════════════════════════

_FICHA_KEYS: list[tuple[str, str]] = [
    ("texto",          r"[Tt]exto\s*[:\s]\s*"),
    ("autor",          r"[Aa]utor[a]?\s*[:\s]\s*"),
    ("dramaturgia",    r"[Dd]ramaturgia\s*[:\s]\s*"),
    ("encenação",      r"[Ee]ncena[çc][aã]o\s*[:\s]\s*"),
    ("direção",        r"[Dd]ire[çc][aã]o\s*[:\s]\s*"),
    ("tradução",       r"[Tt]radu[çc][aã]o\s*[:\s]\s*"),
    ("adaptação",      r"[Aa]dapta[çc][aã]o\s*[:\s]\s*"),
    ("cenografia",     r"[Cc]enografia\s*[:\s]\s*"),
    ("figurinos",      r"[Ff]igurinos?\s*[:\s]\s*"),
    ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz\s*[:\s]\s*|[Ll]uz\s*[:\s]\s*|[Ii]lumina[çc][aã]o\s*[:\s]\s*"),
    ("som",            r"[Ss]om\s*[:\s]\s*|[Ss]onoplastia\s*[:\s]\s*"),
    ("música",         r"[Mm][úu]sica\s*[:\s]\s*|[Cc]omposi[çc][aã]o\s*[:\s]\s*"),
    ("coreografia",    r"[Cc]oreografia\s*[:\s]\s*"),
    ("interpretação",  r"[Ii]nterpreta[çc][aã]o\s*[:\s]\s*|[Aa]tores?\s*[:\s]\s*"),
    ("produção",       r"[Pp]rodu[çc][aã]o\s*[:\s]\s*"),
    ("coprodução",     r"[Cc]o[.\-]?produ[çc][aã]o\s*[:\s]\s*"),
    ("apoio",          r"[Aa]poio\s*[:\s]\s*"),
    ("companhia",      r"[Cc]ompanhia\s*[:\s]\s*"),
]


def _parse_ficha(soup, text: str) -> dict:
    ficha_text = text
    for sel in [".ficha-tecnica", ".technical-sheet", ".event-details", "[class*='ficha']"]:
        el = soup.select_one(sel)
        if el:
            ficha_text = el.get_text(" ", strip=True)
            break

    ficha: dict = {}
    positions: list[tuple[int, int, str]] = []

    for key, pattern in _FICHA_KEYS:
        for m in re.finditer(pattern, ficha_text):
            positions.append((m.start(), m.end(), key))

    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 400
        raw_value  = ficha_text[end:next_start].strip()
        value      = re.sub(r"\s+", " ", raw_value)[:300].strip()
        value = re.sub(r"[,;]+$", "", value).strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha


def _extract_list_from_ficha(ficha: dict, key: str) -> list[str]:
    val = ficha.get(key, "")
    if not val:
        return []
    parts = re.split(r"[,;]|\se\s", val)
    return [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]


def _extract_people(ficha: dict) -> list[str]:
    people_keys = ["texto", "autor", "encenação", "direção", "dramaturgia",
                   "tradução", "cenografia", "figurinos", "luz", "som",
                   "música", "coreografia", "interpretação", "produção"]
    seen = set()
    people = []
    for key in people_keys:
        for name in _extract_list_from_ficha(ficha, key):
            if name.lower() not in seen:
                seen.add(name.lower())
                people.append(name)
    return people
