"""
Scraper: Culturgest
Site: https://www.culturgest.pt

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ESTRATÉGIA DE DESCOBERTA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A listagem /pt/programacao/por-evento/ é carregada por JavaScript
(a div#events-section está sempre vazia no HTML). A API interna
/pt/programacao/schedule/events/ requer sessão Django e não é
acessível via requests simples.

Estratégia: crawl progressivo a partir de seeds, usando a secção
"Próximos Eventos" presente no HTML estático de cada página de evento.
Cada página lista 2–3 eventos futuros como links, o que permite
encadear toda a programação activa.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CACHE DE URLs  (data/culturgest_urls.json)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Para acelerar execuções diárias, o scraper mantém um ficheiro de cache
com os URLs já conhecidos e a sua date_end:

    {
      "https://.../polo-norte/": {"date_end": "2026-07-04"},
      "https://.../burn-burn-burn/": {"date_end": "2026-04-25"},
      ...
    }

Lógica de cache no arranque:
  1. Carregar cache existente (se existir)
  2. URLs com date_end >= hoje → incluir como seeds sem re-crawl de descoberta
     (mas a página individual É sempre visitada para dados actualizados)
  3. URLs com date_end < hoje → ignorar (evento terminado)
  4. Novos URLs descobertos durante o crawl → guardar na cache

O ficheiro é escrito em scrapers/data/culturgest_urls.json.
O build.py não precisa de o conhecer — é interno ao scraper.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FILTRAGEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Nenhuma. Todos os eventos encontrados são devolvidos.
A filtragem por categoria, data ou outra dimensão é da responsabilidade
do harmonizer/validator a jusante.
"""
import json
import os
import re
import time
import logging
from datetime import date, datetime
from pathlib import Path
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
LISTING_URL = f"{BASE}/pt/programacao/por-evento/"

# Cache de URLs conhecidos — relativo à raiz do projecto
_CACHE_PATH = Path(__file__).parent / "data" / "culturgest_urls.json"

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
    "salas":       ["Grande Auditório", "Pequeno Auditório", "Auditório Emílio Rui Vilar"],
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

# Seeds hardcoded de último recurso — só usadas se a cache estiver vazia.
# Actualizar quando o crawl ficar sem entrada (basta 1 evento activo).
_SEEDS_FALLBACK = [
    "https://www.culturgest.pt/pt/programacao/catarina-rolo-salgueiro-e-isabel-costa-os-possessos-burn-burn-burn-2026/",
    "https://www.culturgest.pt/pt/programacao/alex-cassal-ma-criacao-hotel-paradoxo/",
    "https://www.culturgest.pt/pt/programacao/mala-voadora-polo-norte/",
]

_SKIP_SLUGS = {
    "por-evento", "agenda-pdf", "archive", "schedule", "por-tipo",
    "participacao", "convite", "open-call", "temporada-2025-26",
    "temporada-2024-25", "concluido", "filtrar",
}

_PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

_MONTHS_ABBR = ["", "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                 "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

# Mapa nome-de-tipologia → string canónica (lida dos links ?typology= na página)
_TYPOLOGY_MAP = {
    "teatro":                 "Teatro",
    "dança":                  "Dança",
    "performance":            "Performance",
    "música":                 "Música",
    "artes visuais":          "Artes Visuais",
    "cinema":                 "Cinema",
    "conferências e debates": "Conferências e Debates",
    "conferências":           "Conferências e Debates",
    "escolas":                "Infanto-Juvenil",
}

# Campos reconhecidos na ficha técnica
_TECH_FIELDS = {
    "texto", "encenação", "dramaturgia", "direção", "direção artística",
    "tradução", "cenografia", "figurinos", "luz", "iluminação", "som",
    "música", "interpretação", "elenco", "produção", "coprodução",
    "coreografia", "composição", "banda sonora", "desenho de luz",
    "desenho de som", "espaço", "adereços", "direção técnica",
    "direção de produção", "com", "texto e encenação", "criação",
    "assistência de encenação", "assistência de direção",
}


# ─────────────────────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    today = date.today()

    # 1. Carregar cache de URLs
    cache = _load_cache()
    log(f"[{THEATER_NAME}] Cache: {len(cache)} URLs conhecidos")

    # 2. Seeds = URLs da cache ainda activos + seeds da listagem/hardcoded
    seeds: set[str] = set()

    # URLs da cache com date_end >= hoje (ou sem date_end conhecido)
    for url, meta in cache.items():
        de = meta.get("date_end", "")
        if not de or _parse_iso(de) >= today:
            seeds.add(url)

    # Tentar extrair da listagem (normalmente vazio por JS, mas tentamos)
    seeds.update(_seeds_from_listing())

    # Último recurso: seeds hardcoded
    if not seeds:
        log(f"[{THEATER_NAME}] Cache vazia + listagem vazia — a usar seeds hardcoded")
        seeds.update(_SEEDS_FALLBACK)

    log(f"[{THEATER_NAME}] {len(seeds)} seeds para este crawl")

    # 3. Crawl progressivo
    events:   list[dict] = []
    seen_ids: set[str]   = set()
    visited:  set[str]   = set()
    queue:    list[str]  = [_norm(u) for u in seeds]

    while queue:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        ev = _scrape_event_page(url)
        if ev:
            # Actualizar cache com date_end do evento
            cache[url] = {"date_end": ev.get("date_end", "")}

            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids.add(eid)
                # Remover campo interno antes de guardar
                next_urls = ev.pop("_next_urls", [])
                events.append(ev)
            else:
                next_urls = ev.pop("_next_urls", [])

            # Adicionar novos URLs ao crawl e à cache
            for nxt in next_urls:
                nxt = _norm(nxt)
                if nxt not in visited:
                    queue.append(nxt)
                if nxt not in cache:
                    cache[nxt] = {"date_end": ""}

        time.sleep(0.4)

    # 4. Guardar cache actualizada
    _save_cache(cache)

    log(f"[{THEATER_NAME}] {len(events)} eventos (visitadas {len(visited)} páginas, cache {len(cache)} URLs)")
    return events


# ─────────────────────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    """Carrega {url: {date_end: "YYYY-MM-DD"}} da cache persistente."""
    try:
        if _CACHE_PATH.exists():
            with open(_CACHE_PATH, encoding="utf-8") as f:
                data = json.load(f)
            # Normalizar — garantir que todas as chaves têm "/"  no fim
            return {_norm(k): v for k, v in data.items() if isinstance(v, dict)}
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro a carregar cache: {e}")
    return {}


def _save_cache(cache: dict) -> None:
    """Persiste a cache. Cria o directório se necessário."""
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro a guardar cache: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Seeds da listagem
# ─────────────────────────────────────────────────────────────────────────────

def _seeds_from_listing() -> set[str]:
    """Tenta extrair URLs da página de listagem (costuma estar vazia por JS)."""
    seeds = set()
    try:
        r = requests.get(LISTING_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            full = _abs(a["href"])
            if _is_event_url(full):
                seeds.add(_norm(full))
        if seeds:
            log(f"[{THEATER_NAME}] {len(seeds)} seeds extraídas da listagem")
    except Exception as e:
        log(f"[{THEATER_NAME}] Listagem inacessível: {e}")
    return seeds


# ─────────────────────────────────────────────────────────────────────────────
# Scraping de uma página de evento
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

    # ── Título ────────────────────────────────────────────────────────────────
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
    if not title or len(title) < 3:
        return None

    # ── Subtítulo ─────────────────────────────────────────────────────────────
    # O Culturgest usa dois h1 por evento (mobile + desktop) com o título,
    # e um h2 imediato para o subtítulo (ex: título="mala voadora", sub="Polo Norte").
    # Há também casos onde o padrão é invertido: h1=companhia, h2=título.
    subtitle = ""
    all_h1_texts = []
    seen_h1 = set()
    for h in soup.find_all("h1"):
        t = h.get_text(strip=True)
        if t and t not in seen_h1:
            seen_h1.add(t)
            all_h1_texts.append(t)
    # Se há dois h1 distintos, o segundo é o subtítulo
    if len(all_h1_texts) >= 2 and all_h1_texts[1] != title:
        subtitle = all_h1_texts[1]
    # Fallback: h2 curto que não seja secção
    if not subtitle:
        h2 = main_el.find("h2")
        if h2:
            candidate = h2.get_text(strip=True)
            if candidate and candidate != title and len(candidate) < 120:
                subtitle = candidate

    # ── Categoria ─────────────────────────────────────────────────────────────
    # Link de tipologia visível no header da página do evento:
    # ex: <a href="/pt/programacao/por-evento/?typology=1">Teatro</a>
    # precedido de "+ " nas versões mais antigas. Este link é HTML estático.
    raw_category = ""
    for a in soup.select("a[href*='typology=']"):
        txt = a.get_text(strip=True).lstrip("+ ").strip()
        if txt:
            raw_category = txt
            break
    category = normalize_category(
        _TYPOLOGY_MAP.get(raw_category.lower(), raw_category) or "Teatro"
    )

    # ── Datas ─────────────────────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates(soup, full_text)

    if not date_start:
        log(f"[{THEATER_NAME}] Sem data: {title!r} — {url}")
        return None

    # ── Imagem ────────────────────────────────────────────────────────────────
    # Sempre via og:image — as imagens inline são thumbnails de baixa resolução.
    image = None
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content", "").startswith("http"):
        image = build_image_object(og_img["content"], soup, THEATER_NAME, url)
    if not image:
        img_el = main_el.find("img", src=re.compile(r"/media/filer_public"))
        if img_el:
            image = build_image_object(_abs(img_el["src"]), soup, THEATER_NAME, url)

    # ── Sinopse ───────────────────────────────────────────────────────────────
    synopsis = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        desc = og_desc.get("content", "").strip()
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

    # ── Horário ───────────────────────────────────────────────────────────────
    # O Culturgest lista sessões individuais no formato "SÁB 19:00" ou "SEX 21:00".
    # Extrair a hora mais frequente como horário representativo.
    schedule = ""
    times_found = re.findall(r"\b(\d{1,2}[h:]\d{2})\b", full_text)
    if times_found:
        # A hora mais frequente é o horário principal
        from collections import Counter
        schedule = Counter(times_found).most_common(1)[0][0]

    # ── Sessões individuais ───────────────────────────────────────────────────
    sessions = build_sessions(date_start, date_end, schedule)

    # ── Sala ──────────────────────────────────────────────────────────────────
    sala = ""
    for sala_name in ("Auditório Emílio Rui Vilar", "Grande Auditório", "Pequeno Auditório"):
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
            or any(x in text_a for x in ("comprar bilhete", "comprar bilhetes", "bilheteira"))
        ):
            ticket_url = a["href"] if a["href"].startswith("http") else _abs(a["href"])
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
    # Padrão no Culturgest: "Duração 1h30 apróx." ou "90 minutos" ou "1h30"
    duration     = ""
    duration_min = None
    for pat in (
        r"[Dd]ura[çc][aã]o\s*[:\-]?\s*(\d+\s*h\d*\s*(?:min(?:utos?)?)?(?:\s*ap[ró]{2}x\.?)?)",
        r"(\d+)\s*h\s*(\d+)\s*min(?:utos?)?",
        r"(\d+)\s*min(?:utos?)?(?!\s*\d)",
        r"(\d+)\s*h(?:oras?)?(?!\d)",
    ):
        m_dur = re.search(pat, full_text, re.IGNORECASE)
        if m_dur:
            duration = m_dur.group(0).strip()
            hm = re.search(r"(\d+)\s*h(?:oras?)?(?:\s*(\d+))?", duration, re.IGNORECASE)
            om = re.search(r"(\d+)\s*min", duration, re.IGNORECASE)
            if hm:
                duration_min = int(hm.group(1)) * 60 + (int(hm.group(2)) if hm.group(2) else 0)
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
        (r"audiodescri[çc][aã]o",           "Audiodescrição"),
        (r"\bLGP\b|l[íi]ngua\s+gestual",    "LGP"),
        (r"legendas\s+em\s+ingl[êe]s",      "Legendas EN"),
        (r"legendas\s+em\s+portugu[êe]s",   "Legendas PT"),
        (r"surtitula[çc][aã]o",             "Surtitulação"),
        (r"acesso\s+cadeira\s+de\s+rodas",  "Acesso cadeira de rodas"),
        (r"reconhecimento\s+de\s+palco",    "Reconhecimento de palco"),
    ):
        if re.search(pat, full_text, re.IGNORECASE):
            accessibility.append(label)

    # ── Ficha técnica ─────────────────────────────────────────────────────────
    technical_sheet = _extract_technical_sheet(soup, main_el)

    # ── Links "Próximos Eventos" (para o crawl) ───────────────────────────────
    next_urls = _extract_next_event_urls(soup)

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "subtitle":        subtitle,
        "theater":         THEATER_NAME,
        "category":        category,
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        sessions,
        "schedule":        schedule,
        "synopsis":        synopsis,
        "image":           image,
        "source_url":      url,
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
        "sala":            sala,
        "_next_urls":      next_urls,   # campo interno — removido em scrape()
    }


# ─────────────────────────────────────────────────────────────────────────────
# Ficha técnica
# ─────────────────────────────────────────────────────────────────────────────

def _extract_technical_sheet(soup, main_el) -> dict:
    """
    Extrai a ficha técnica como dict {papel: valor}.

    O Culturgest usa consistentemente o padrão:
        <heading>Direção</heading>
        <p>Jorge Andrade</p>
    onde heading pode ser h4, h5, strong, ou b.

    Fallback para <dl> estruturado se existir.
    """
    sheet = {}

    # 1. <dl> estruturado (mais fiável quando existe)
    for dl in main_el.find_all("dl"):
        for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
            key = dt.get_text(strip=True).rstrip(":").lower().strip()
            val = dd.get_text(" ", strip=True)
            if key and val and key in _TECH_FIELDS:
                sheet[key] = val
    if sheet:
        return sheet

    # 2. Padrão Culturgest: heading seguido de parágrafo/span irmão
    # Percorrer todos os elementos que possam ser rótulos de ficha técnica
    for el in main_el.find_all(["h4", "h5", "strong", "b", "p", "span", "div"]):
        raw = el.get_text(strip=True).rstrip(":")
        key = raw.lower()
        if key not in _TECH_FIELDS:
            continue
        # Procurar o valor no próximo elemento irmão ou filho adjacente
        val_el = el.find_next_sibling()
        if not val_el:
            # Pode estar dentro do mesmo parágrafo com separador ":"
            parent_text = el.find_parent()
            if parent_text:
                full = parent_text.get_text(" ", strip=True)
                m = re.search(re.escape(raw) + r"\s*[:]\s*(.+)", full, re.IGNORECASE)
                if m:
                    val = m.group(1).strip()
                    if val and key not in sheet:
                        sheet[key] = val
            continue
        val = val_el.get_text(" ", strip=True)
        # Rejeitar se o valor for outro rótulo da ficha
        if val and val.lower().rstrip(":") not in _TECH_FIELDS and key not in sheet:
            sheet[key] = val

    if sheet:
        return sheet

    # 3. Regex de último recurso — "Rótulo\n\nValor" no texto corrido
    full_text = main_el.get_text("\n", strip=True)
    field_re = re.compile(
        r"^(" + "|".join(re.escape(f) for f in sorted(_TECH_FIELDS, key=len, reverse=True)) + r")\s*\n+(.+)$",
        re.IGNORECASE | re.MULTILINE,
    )
    for m in field_re.finditer(full_text):
        key = m.group(1).strip().lower()
        val = m.group(2).strip()
        if val and key not in sheet:
            sheet[key] = val

    return sheet


# ─────────────────────────────────────────────────────────────────────────────
# Links "Próximos Eventos"
# ─────────────────────────────────────────────────────────────────────────────

def _extract_next_event_urls(soup) -> list[str]:
    """
    Extrai os links da secção "Próximos Eventos" que aparece no rodapé de
    cada página de evento. É o mecanismo principal de descoberta do crawl.
    """
    next_urls = []
    for el in soup.find_all(string=re.compile(r"pr[óo]ximos\s+eventos", re.IGNORECASE)):
        container = el.find_parent()
        if not container:
            continue
        # Subir até encontrar um container com links de eventos
        for _ in range(5):
            links = [
                _norm(_abs(a["href"]))
                for a in container.find_all("a", href=True)
                if _is_event_url(_abs(a["href"]))
            ]
            if links:
                next_urls.extend(links)
                break
            container = container.find_parent()
            if not container:
                break
    return list(dict.fromkeys(next_urls))  # deduplicar mantendo ordem


# ─────────────────────────────────────────────────────────────────────────────
# Parse de datas
# ─────────────────────────────────────────────────────────────────────────────

def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    # 1. Elementos <time datetime="YYYY-MM-DD"> (mais fiáveis)
    dates_iso = sorted({
        m.group(1)
        for t in soup.find_all("time", attrs={"datetime": True})
        for m in [re.match(r"(\d{4}-\d{2}-\d{2})", t.get("datetime", ""))]
        if m
    })
    if dates_iso:
        d_s, d_e = dates_iso[0], dates_iso[-1]
        return _make_dates_label(d_s, d_e), d_s, d_e

    # 2. Strings de data no HTML: "26 JUN 2026", "23–25 Abr 2026", etc.
    # Recolher todos os nós de texto que contenham uma data
    date_strings = [
        el.strip()
        for el in soup.find_all(string=re.compile(r"\b\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}\b"))
    ]
    sources = date_strings + [text]

    for src in sources:
        # DD MMM [YYYY] – DD MMM YYYY  (meses distintos)
        m = re.search(
            r"(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,})(?:\s+(\d{4}))?"
            r"\s*[–—\-]+\s*"
            r"(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,})\s+(\d{4})",
            src,
        )
        if m:
            d1, mo1, y1_opt, d2, mo2, y2 = m.groups()
            n1, n2 = _mon(mo1), _mon(mo2)
            if n1 and n2:
                y1 = y1_opt or y2
                ds = f"{y1}-{n1:02d}-{int(d1):02d}"
                de = f"{y2}-{n2:02d}-{int(d2):02d}"
                return _make_dates_label(ds, de), ds, de

        # DD–DD MMM YYYY  (mesmo mês)
        m = re.search(r"(\d{1,2})\s*[–—\-]\s*(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,})\s+(\d{4})", src)
        if m:
            d1, d2, mo, y = m.groups()
            n = _mon(mo)
            if n:
                ds = f"{y}-{n:02d}-{int(d1):02d}"
                de = f"{y}-{n:02d}-{int(d2):02d}"
                return _make_dates_label(ds, de), ds, de

        # DD MMM YYYY  (data única)
        m = re.search(r"(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,})\s+(\d{4})", src)
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
    return len(parts) >= 3 and parts[2] not in _SKIP_SLUGS


def _norm(url: str) -> str:
    """Canonicaliza URL: garante trailing slash."""
    return url.rstrip("/") + "/"


def _abs(href: str) -> str:
    return href if href.startswith("http") else urljoin(BASE, href)


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower()[:3]) or _PT_MONTHS.get(s.lower())


def _parse_iso(s: str) -> date:
    """Converte "YYYY-MM-DD" para date. Devolve date.min se inválido."""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return date.min


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
