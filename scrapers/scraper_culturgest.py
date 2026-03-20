"""
Scraper: Culturgest
Site: https://www.culturgest.pt

Estratégia:
  1. Chamar a API JSON interna (/pt/programacao/schedule/events/) que alimenta
     a listagem do site — devolve exactamente os eventos activos, com tipologia,
     datas, horário e URL canónico. Isto resolve o problema de contagem (37 vs 28)
     e o problema de categorias (vinhamos a ler atributos JS nunca populados).
  2. Para cada evento, visitar a página individual para recolher: og:image,
     sinopse, bilhetes, preço, duração, idade, ficha técnica.
  3. Todos os eventos são importados — sem filtro de tipologia neste scraper.
     A filtragem por categoria é da responsabilidade do harmonizer/validator.

Notas sobre a API:
  - A URL da API está embebida no HTML como:
      window.event_list_url="/pt/programacao/schedule/events/"
  - Pedimos sem parâmetro ?typology= para obter toda a programação.
  - A API pode devolver paginação; tratamos via ?page=N.
  - Se a API falhar (mudança de endpoint, auth, etc.), há fallback para o
    endpoint /pt/programacao/filtrar/ que devolve HTML com os cards.
"""
import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)
from scrapers.schema import normalize_category

logger = logging.getLogger(__name__)

BASE        = "https://www.culturgest.pt"
API_URL     = f"{BASE}/pt/programacao/schedule/events/"
LISTING_URL = f"{BASE}/pt/programacao/por-evento/"

THEATER = {
    "id":          "culturgest",
    "name":        "Culturgest",
    "short":       "Culturgest",
    "color":       "#6d4c97",
    "city":        "Lisboa",
    "address":     "Rua Arco do Cego, 50, 1000-020 Lisboa",
    "site":        "https://www.culturgest.pt",
    "programacao": "https://www.culturgest.pt/pt/programacao/por-evento/",
    "lat":         38.7316,
    "lng":         -9.1387,
    "salas":       ["Grande Auditório", "Pequeno Auditório"],
    "aliases":     ["culturgest", "fundação caixa geral de depósitos", "cgd culturgest"],
    "description": (
        "A Culturgest — Fundação Caixa Geral de Depósitos dedica-se à criação "
        "contemporânea, com programação regular de teatro, dança, música e artes "
        "visuais em Lisboa e Porto."
    ),
    "logo_url":    "https://www.culturgest.pt/static/site/images/logo_cgd.svg",
    "favicon_url": "https://www.culturgest.pt/static/site/images/favicon/favicon-96x96.png",
    "facade_url":  "https://www.culturgest.pt/media/filer_public/culturgest-fachada.jpg",
}
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

# Mapa tipologia_id → string raw para normalize_category()
# 1=Teatro  2=Dança  3=Performance  4=Artes Visuais
# 5=Cinema  6=Conferências e Debates  8=Música
_TYPOLOGY_TO_CATEGORY = {
    "1": "Teatro",
    "2": "Dança",
    "3": "Performance",
    "4": "Artes Visuais",
    "5": "Cinema",
    "6": "Conferências e Debates",
    "8": "Música",
}

_PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

_MONTHS_ABBR = [
    "", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
    "Jul", "Ago", "Set", "Out", "Nov", "Dez",
]

# Campos reconhecidos na ficha técnica
_TECH_FIELDS = [
    "texto", "encenação", "dramaturgia", "direção", "direção artística",
    "tradução", "cenografia", "figurinos", "luz", "iluminação", "som",
    "música", "interpretação", "elenco", "produção", "coprodução",
    "coreografia", "composição", "banda sonora", "desenho de luz",
    "desenho de som", "espaço", "adereços",
]


# ─────────────────────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    raw_events = _fetch_api_events()
    log(f"[{THEATER_NAME}] {len(raw_events)} eventos recebidos da API")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in raw_events:
        try:
            ev = _build_event(item)
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro a processar evento {item.get('url','?')}: {e}")
        time.sleep(0.4)

    log(f"[{THEATER_NAME}] {len(events)} eventos válidos após filtro de tipologia")
    return events


# ─────────────────────────────────────────────────────────────────────────────
# API interna
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_api_events() -> list[dict]:
    """
    Chama a API JSON interna do Culturgest com paginação.
    A API está em /pt/programacao/schedule/events/ e é a mesma que o
    frontend JS usa para popular a listagem.

    Se a API não devolver JSON válido, faz fallback para HTML.
    """
    results = []
    page    = 1

    while True:
        try:
            params = {"page": page}
            r = requests.get(
                API_URL,
                params=params,
                headers={**HEADERS, "Accept": "application/json, */*"},
                timeout=15,
            )
            r.raise_for_status()

            # A API pode devolver HTML se não reconhecer o pedido JSON
            ct = r.headers.get("content-type", "")
            try:
                data = r.json()
            except Exception:
                if page == 1:
                    log(f"[{THEATER_NAME}] API não devolveu JSON — fallback para HTML")
                    return _fallback_html_events()
                break

            # Normalizar estrutura da resposta
            if isinstance(data, list):
                batch = data
            elif isinstance(data, dict):
                batch = (
                    data.get("results")
                    or data.get("events")
                    or data.get("items")
                    or data.get("data")
                    or []
                )
            else:
                break

            if not batch:
                break

            results.extend(batch)

            # Paginação
            has_next = (
                isinstance(data, dict) and bool(data.get("next"))
            ) or (
                isinstance(data, list) and len(batch) >= 20  # heurística
            )
            if has_next:
                page += 1
            else:
                break

            time.sleep(0.3)

        except Exception as e:
            log(f"[{THEATER_NAME}] Erro na API (página {page}): {e}")
            if not results:
                return _fallback_html_events()
            break

    if not results:
        log(f"[{THEATER_NAME}] API devolveu lista vazia — fallback para HTML")
        return _fallback_html_events()

    return results


def _fallback_html_events() -> list[dict]:
    """
    Fallback: usa /pt/programacao/filtrar/ (fragmento HTML com cards activos)
    para obter URLs. Sem crawl progressivo cego — apenas a listagem directa.
    """
    log(f"[{THEATER_NAME}] A usar fallback HTML para descoberta de eventos")
    urls = set()

    for endpoint in (
        f"{BASE}/pt/programacao/filtrar/",
        f"{BASE}/pt/programacao/por-evento/",
    ):
        try:
            r = requests.get(endpoint, headers=HEADERS, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                full = a["href"] if a["href"].startswith("http") else urljoin(BASE, a["href"])
                if _is_event_url(full):
                    urls.add(full.rstrip("/") + "/")
            if urls:
                break
        except Exception as e:
            log(f"[{THEATER_NAME}] Fallback {endpoint} falhou: {e}")

    log(f"[{THEATER_NAME}] Fallback encontrou {len(urls)} URLs")
    return [{"url": u, "_from_fallback": True} for u in urls]


# ─────────────────────────────────────────────────────────────────────────────
# Construção do evento
# ─────────────────────────────────────────────────────────────────────────────

def _build_event(item: dict) -> dict | None:
    """Constrói o dict de evento a partir de um item da API + visita à página."""

    # URL canónico
    source_url = (
        item.get("url")
        or item.get("link")
        or item.get("absolute_url")
        or item.get("canonical_url")
        or ""
    )
    if source_url and not source_url.startswith("http"):
        source_url = urljoin(BASE, source_url)
    if not source_url:
        return None

    # Tipologia — apenas para mapear para categoria, sem filtro
    typology_id = str(item.get("typology_id") or item.get("typology") or "")

    # Dados estruturados da API
    title_api      = _clean_text(item.get("title") or item.get("name") or "")
    subtitle_api   = _clean_text(item.get("subtitle") or item.get("author") or "")
    date_start_api = _normalise_date(item.get("date_start") or item.get("start_date") or "")
    date_end_api   = _normalise_date(item.get("date_end")   or item.get("end_date")   or "")
    schedule_api   = _clean_text(item.get("schedule") or item.get("time") or "")

    # Visita à página individual (sempre obrigatória — para imagem e detalhes)
    page_data = _scrape_event_page(source_url)
    if not page_data:
        return None

    # Merge: API tem prioridade
    title      = title_api      or page_data.get("title", "")
    subtitle   = subtitle_api   or page_data.get("subtitle", "")
    date_start = date_start_api or page_data.get("date_start", "")
    date_end   = date_end_api   or page_data.get("date_end",   "")
    schedule   = schedule_api   or page_data.get("schedule",   "")

    if not title:
        log(f"[{THEATER_NAME}] Sem título: {source_url}")
        return None
    if not date_start:
        log(f"[{THEATER_NAME}] Sem data: {title!r} — {source_url}")
        return None

    # Categoria
    if typology_id in _TYPOLOGY_TO_CATEGORY:
        raw_category = _TYPOLOGY_TO_CATEGORY[typology_id]
    else:
        raw_category = page_data.get("category_raw") or "Teatro"
    category = normalize_category(raw_category)

    # Dates label
    dates_label = page_data.get("dates_label") or _make_dates_label(date_start, date_end)

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
        "synopsis":        page_data.get("synopsis", ""),
        "image":           page_data.get("image"),
        "source_url":      source_url,
        "ticket_url":      page_data.get("ticket_url", ""),
        "price_info":      page_data.get("price_info", ""),
        "price_min":       page_data.get("price_min"),
        "price_max":       page_data.get("price_max"),
        "duration":        page_data.get("duration", ""),
        "duration_min":    page_data.get("duration_min"),
        "age_rating":      page_data.get("age_rating", ""),
        "age_min":         page_data.get("age_min"),
        "accessibility":   page_data.get("accessibility", []),
        "technical_sheet": page_data.get("technical_sheet", {}),
        "sala":            page_data.get("sala", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scraping da página individual
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_event_page(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)
    main_el   = soup.find("main") or soup.find("article") or soup

    # ── Título ───────────────────────────────────────────────────────────────
    title = ""
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        title = re.sub(
            r"\s*[\|—]\s*Culturgest.*$", "", og_title["content"], flags=re.IGNORECASE
        ).strip()
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

    # ── Subtítulo ─────────────────────────────────────────────────────────────
    subtitle = ""
    for cls in ("subtitle", "event-subtitle", "author", "company"):
        el = soup.find(class_=re.compile(cls, re.IGNORECASE))
        if el:
            candidate = el.get_text(strip=True)
            if candidate and candidate != title:
                subtitle = candidate
                break
    if not subtitle:
        all_h1 = soup.find_all("h1")
        if len(all_h1) > 1:
            candidate = all_h1[1].get_text(strip=True)
            if candidate and candidate != title:
                subtitle = candidate
        if not subtitle:
            h2 = soup.find("h2")
            if h2:
                candidate = h2.get_text(strip=True)
                if candidate and len(candidate) < 100 and candidate != title:
                    subtitle = candidate

    # ── Imagem (sempre via og:image) ─────────────────────────────────────────
    image = None
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content", "").startswith("http"):
        image = build_image_object(og_img["content"], soup, THEATER_NAME, url)
    if not image:
        img_el = main_el.find("img", src=re.compile(r"/media/filer_public"))
        if img_el:
            image = build_image_object(urljoin(BASE, img_el["src"]), soup, THEATER_NAME, url)

    # ── Sinopse ───────────────────────────────────────────────────────────────
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        desc = og_desc.get("content", "").strip()
        # Rejeitar descrições genéricas
        if desc and len(desc) > 40 and desc not in ("Agenda | Culturgest", "Culturgest"):
            synopsis = desc
    if not synopsis:
        paras = [
            p.get_text(" ", strip=True)
            for p in main_el.find_all("p")
            if len(p.get_text(" ", strip=True)) > 80
        ]
        if paras:
            synopsis = " ".join(paras)[:2000]
    synopsis = truncate_synopsis(synopsis)

    # ── Datas ─────────────────────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates_from_page(soup, full_text)

    # ── Categoria raw (fallback quando tipologia não vem da API) ──────────────
    category_raw = ""
    typ_el = soup.select_one('li.type[data-property="typology"]')
    if typ_el:
        category_raw = typ_el.get_text(strip=True)
    if not category_raw:
        # Tentar links de filtro na nav da página
        for a in soup.select("ul li a[href*='typology']"):
            txt = a.get_text(strip=True)
            if txt:
                category_raw = txt
                break

    # ── Horário ───────────────────────────────────────────────────────────────
    schedule = ""
    m_sched = re.search(r"\b(\d{1,2}[h:]\d{2})\b", full_text)
    if m_sched:
        schedule = m_sched.group(1)

    # ── Sala ──────────────────────────────────────────────────────────────────
    sala = ""
    for sala_name in ("Grande Auditório", "Pequeno Auditório"):
        if sala_name.lower() in full_text.lower():
            sala = sala_name
            break

    # ── Bilhetes ──────────────────────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href   = a["href"].lower()
        text_a = a.get_text(strip=True).lower()
        if (
            any(x in href for x in ("ticketline", "bol.pt", "bilhete", "comprar"))
            or any(x in text_a for x in ("comprar bilhete", "bilheteira", "reservar"))
        ):
            full_href = a["href"] if a["href"].startswith("http") else urljoin(BASE, a["href"])
            ticket_url = full_href
            break

    # ── Preço ─────────────────────────────────────────────────────────────────
    price_info = ""
    price_min  = None
    price_max  = None
    if re.search(r"\bentrada\s+livre\b|\bgratuito\b|\bgratuita\b", full_text, re.IGNORECASE):
        price_info = "Entrada livre"
        price_min  = 0.0
        price_max  = 0.0
    else:
        vals = []
        for p_str in re.findall(r"(\d+(?:[.,]\d+)?)\s*€", full_text):
            try:
                vals.append(float(p_str.replace(",", ".")))
            except ValueError:
                pass
        if vals:
            price_min  = min(vals)
            price_max  = max(vals)
            price_info = (
                f"{price_min:.0f}€"
                if price_min == price_max
                else f"{price_min:.0f}€–{price_max:.0f}€"
            )

    # ── Duração ───────────────────────────────────────────────────────────────
    duration     = ""
    duration_min = None
    # Tenta "Duração: Xh Ymin", "Xh30min", "90 minutos", "1h30"
    patterns_dur = [
        r"[Dd]ura[çc][aã]o\s*[:\-]?\s*(\d+\s*h(?:oras?)?\s*\d*\s*(?:min(?:utos?)?)?)",
        r"(\d+)\s*h\s*(\d+)\s*min(?:utos?)?",
        r"(\d+)\s*min(?:utos?)?(?!\s*\d)",
        r"(\d+)\s*h(?:oras?)?(?!\d)",
    ]
    for pat in patterns_dur:
        m_dur = re.search(pat, full_text, re.IGNORECASE)
        if m_dur:
            duration = m_dur.group(0).strip()
            # Calcular minutos totais
            hm = re.search(r"(\d+)\s*h(?:oras?)?(?:\s*(\d+)\s*min)?", duration, re.IGNORECASE)
            om = re.search(r"^(\d+)\s*min", duration, re.IGNORECASE)
            if hm:
                h = int(hm.group(1))
                mn = int(hm.group(2)) if hm.group(2) else 0
                duration_min = h * 60 + mn
            elif om:
                duration_min = int(om.group(1))
            break

    # ── Classificação etária ──────────────────────────────────────────────────
    age_rating = ""
    age_min    = None
    m_age = re.search(r"M\s*/\s*(\d+)", full_text) or re.search(
        r"[Mm]aiores\s+de\s+(\d+)", full_text
    )
    if m_age:
        age_num    = int(m_age.group(1))
        age_min    = age_num
        age_rating = f"M/{age_num}"

    # ── Acessibilidade ────────────────────────────────────────────────────────
    accessibility = []
    for pat, label in (
        (r"audiodescri[çc][aã]o",          "Audiodescrição"),
        (r"LGP|l[íi]ngua\s+gestual",       "LGP"),
        (r"legendas\s+em\s+ingl[êe]s",     "Legendas EN"),
        (r"legendas\s+em\s+portugu[êe]s",  "Legendas PT"),
        (r"surtitula[çc][aã]o",            "Surtitulação"),
        (r"acesso\s+cadeira\s+de\s+rodas", "Acesso cadeira de rodas"),
    ):
        if re.search(pat, full_text, re.IGNORECASE):
            accessibility.append(label)

    # ── Ficha técnica ─────────────────────────────────────────────────────────
    technical_sheet = _extract_technical_sheet(main_el, full_text)

    return {
        "title":           title,
        "subtitle":        subtitle,
        "image":           image,
        "synopsis":        synopsis,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "schedule":        schedule,
        "sala":            sala,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "price_min":       price_min,
        "price_max":       price_max,
        "duration":        duration,
        "duration_min":    duration_min,
        "age_rating":      age_rating,
        "age_min":         age_min,
        "accessibility":   accessibility,
        "technical_sheet": technical_sheet,
        "category_raw":    category_raw,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────────────────────

def _extract_technical_sheet(main_el, full_text: str) -> dict:
    """
    Extrai ficha técnica como dict {papel: valor}.
    Estratégia em 3 níveis: <dl>, linhas "Campo: Valor" no texto, <li>/<p>.
    """
    sheet = {}

    # 1. <dl> estruturado
    for dl in main_el.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            key = dt.get_text(strip=True).rstrip(":").lower().strip()
            val = dd.get_text(" ", strip=True)
            if key and val:
                sheet[key] = val

    if sheet:
        return sheet

    # 2. Regex em texto livre: "Campo: Valor" (linha por linha)
    field_re = re.compile(
        r"^(" + "|".join(re.escape(f) for f in _TECH_FIELDS) + r")\s*[:–]\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    )
    for m in field_re.finditer(full_text):
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if key not in sheet:
            sheet[key] = val

    if sheet:
        return sheet

    # 3. Elementos inline <li>/<p> com "Palavra: texto"
    for el in main_el.find_all(["li", "p", "span"]):
        txt = el.get_text(" ", strip=True)
        m   = re.match(r"^([A-Za-zÀ-ÿ\s]{2,30})\s*[:–]\s*(.{2,200})$", txt)
        if m:
            key_raw = m.group(1).strip().lower()
            val     = m.group(2).strip()
            if key_raw in _TECH_FIELDS and key_raw not in sheet:
                sheet[key_raw] = val

    return sheet


# ─────────────────────────────────────────────────────────────────────────────
# Parse de datas
# ─────────────────────────────────────────────────────────────────────────────

def _parse_dates_from_page(soup, text: str) -> tuple[str, str, str]:
    # 1. Elementos <time datetime="YYYY-MM-DD">
    dates_iso = []
    for t in soup.find_all("time", attrs={"datetime": True}):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", t["datetime"])
        if m:
            dates_iso.append(m.group(1))
    if dates_iso:
        dates_iso.sort()
        d_s, d_e = dates_iso[0], dates_iso[-1]
        return _make_dates_label(d_s, d_e), d_s, d_e

    # 2. Texto livre
    return _parse_dates_from_text(text)


def _parse_dates_from_text(text: str) -> tuple[str, str, str]:
    # DD [de] MMMM [de] YYYY – DD [de] MMMM [de] YYYY
    m = re.search(
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zÀ-ÿ]+)\s+(?:de\s+)?(\d{4})"
        r"\s*[–—\-]+\s*"
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zÀ-ÿ]+)\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            ds = f"{y1}-{n1:02d}-{int(d1):02d}"
            de = f"{y2}-{n2:02d}-{int(d2):02d}"
            return _make_dates_label(ds, de), ds, de

    # DD–DD [de] MMMM [de] YYYY
    m = re.search(
        r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+(?:de\s+)?([A-Za-zÀ-ÿ]+)\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d1, d2, mo, y = m.groups()
        n = _mon(mo)
        if n:
            ds = f"{y}-{n:02d}-{int(d1):02d}"
            de = f"{y}-{n:02d}-{int(d2):02d}"
            return _make_dates_label(ds, de), ds, de

    # DD [de] MMMM [de] YYYY  (data única)
    m = re.search(
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zÀ-ÿ]+)\s+(?:de\s+)?(\d{4})",
        text,
    )
    if m:
        d, mo, y = m.groups()
        n = _mon(mo)
        if n:
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return _make_dates_label(ds, ds), ds, ds

    return "", "", ""


# ─────────────────────────────────────────────────────────────────────────────
# Utilitários
# ─────────────────────────────────────────────────────────────────────────────

def _is_event_url(url: str) -> bool:
    if not url.startswith(BASE):
        return False
    path  = url.replace(BASE, "").strip("/")
    if not path.startswith("pt/programacao/"):
        return False
    parts = [p for p in path.split("/") if p]
    if len(parts) < 3:
        return False
    skip = {
        "por-evento", "agenda-pdf", "archive", "schedule", "por-tipo",
        "participacao", "convite", "open-call", "temporada-2025-26",
        "temporada-2024-25", "concluido", "filtrar",
    }
    return parts[2] not in skip


def _clean_text(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalise_date(s: str) -> str:
    if not s:
        return ""
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    return ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower()[:3]) or _PT_MONTHS.get(s.lower())


def _make_dates_label(date_start: str, date_end: str) -> str:
    def fmt(d: str) -> str:
        try:
            y, mo, day = d.split("-")
            return f"{int(day)} {_MONTHS_ABBR[int(mo)]} {y}"
        except Exception:
            return d

    if not date_start:
        return ""
    if not date_end or date_end == date_start:
        return fmt(date_start)
    return f"{fmt(date_start)} – {fmt(date_end)}"
