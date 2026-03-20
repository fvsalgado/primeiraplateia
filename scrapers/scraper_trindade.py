"""
Scraper: Teatro da Trindade INATEL
Fonte: https://teatrotrindade.inatel.pt/programacao/
Cidade: Lisboa

Estrutura do site (WordPress, HTML estático):
  - Listagem: /programacao/ — todos os espectáculos, sem filtro de categoria.
    Cada card é um <a href="/espetaculo/slug/"> com:
      • Imagem (<img> com srcset)
      • Nó de texto directo com intervalo de datas, ex: "30 Abr - 02 Ago 2026"
      • <h3> com título
      • <span class="bebas lighter"> com autor/companhia (subtitle)
  - Página de evento: /espetaculo/<slug>/
    O servidor bloqueia requests simples → usar Session com headers de browser.
  - Imagem: extrair URL sem sufixo de dimensão (-NNNxNNN) a partir do srcset
    do elemento hero <img>, que contém a imagem original em alta resolução.
    O og:image pode apontar para a versão reduzida (-300x169).
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object, build_sessions,
)
from scrapers.schema import normalize_category

# ─────────────────────────────────────────────────────────────
# Metadados do teatro — lidos pelo sync_scrapers.py
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "trindade",
    "name":        "Teatro da Trindade INATEL",
    "short":       "Trindade",
    "color":       "#8b0000",
    "city":        "Lisboa",
    "address":     "Rua Nova da Trindade, 9, 1200-301 Lisboa",
    "site":        "https://teatrotrindade.inatel.pt",
    "programacao": "https://teatrotrindade.inatel.pt/programacao/",
    "lat":         38.7107,
    "lng":         -9.1414,
    "salas":       ["Sala Carmen Dolores", "Sala Estúdio"],
    "logo_url":    "https://teatrotrindade.inatel.pt/wp-content/themes/teatrodatrindade/assets/img/logo.svg",
    "favicon_url": "https://teatrotrindade.inatel.pt/wp-content/uploads/2020/03/cropped-fav-tt-192x192.png",
    "facade_url":  "https://teatrotrindade.inatel.pt/wp-content/uploads/2020/03/fav-01.png",
    "aliases": [
        "teatro da trindade",
        "teatro da trindade inatel",
        "trindade",
        "trindade inatel",
    ],
    "description": (
        "O Teatro da Trindade INATEL, inaugurado em 1867, é um dos mais emblemáticos "
        "teatros de Lisboa. A sua Sala Carmen Dolores é um dos mais bem preservados "
        "exemplares de teatro à italiana do país, com capacidade para 485 espectadores."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://teatrotrindade.inatel.pt"
AGENDA       = f"{BASE}/programacao/"

# Headers de browser completos para contornar bloqueio do servidor
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language":           "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept-Encoding":           "gzip, deflate, br",
    "Connection":                "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer":                   BASE + "/",
}

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

# Regex para remover sufixos de dimensão WordPress: -300x169, -1920x980, etc.
_WP_SIZE_RE = re.compile(r"-\d+x\d+(?=\.[a-zA-Z]{3,4}$)")


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    session = requests.Session()
    session.headers.update(_BROWSER_HEADERS)

    # Visitar homepage para obter cookies de sessão
    try:
        session.get(BASE, timeout=15)
    except Exception:
        pass

    candidates = _collect_candidates(session)
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos na listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in candidates:
        try:
            ev = _scrape_event(session, item["url"], item["stub"])
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro em {item['url']}: {e}")
        time.sleep(0.5)

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates(session: requests.Session) -> list[dict]:
    """
    Lê /programacao/ e extrai URLs e stubs de todos os cards,
    sem filtrar por categoria.
    """
    try:
        r = session.get(AGENDA, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    for a in soup.find_all("a", href=re.compile(r"/espetaculo/")):
        href = a.get("href", "")
        url  = href if href.startswith("http") else urljoin(BASE, href)
        url  = url.rstrip("/") + "/"
        if url in seen_urls:
            continue
        seen_urls.add(url)

        stub = _extract_card_stub(a, url)
        if not stub.get("title"):
            continue

        candidates.append({"url": url, "stub": stub})

    return candidates


def _extract_card_stub(a_tag, url: str) -> dict:
    """
    Extrai dados do card da listagem.

    Título:   <h3> filho directo do <a>
    Subtitle: <span class="bebas lighter"> — autor/companhia/encenação
    Data:     nó de texto directo com padrão DD Mês [...] YYYY
    Schedule: texto horário na secção de datas, ex: "21h30", "21h00"
    Imagem:   primeiro <img> com srcset → URL original sem sufixo de dimensão
    """
    title     = ""
    subtitle  = ""
    dates_raw = ""
    schedule  = ""
    img_url   = ""

    # Título — h3 directo
    h3 = a_tag.find("h3")
    if h3:
        title = h3.get_text(strip=True)

    # Subtitle — span.bebas.lighter (autor / companhia / encenação)
    span = a_tag.find("span", class_=re.compile(r"bebas"))
    if span:
        subtitle = re.sub(r"\s+", " ", span.get_text(" ", strip=True))

    # Imagem — preferir srcset para obter URL original
    img = a_tag.find("img")
    if img:
        src = img.get("src", "")
        srcset = img.get("srcset", "")
        if src and "ticket.svg" not in src and "logo" not in src.lower():
            # Tentar extrair a URL de maior resolução do srcset
            best = _best_srcset_url(srcset) or src
            img_url = best if best.startswith("http") else urljoin(BASE, best)

    # Data e horário — percorrer APENAS nós de texto directos do <a>
    # (NavigableString sem tag.name), para não apanhar texto de h3/span
    for child in a_tag.children:
        if hasattr(child, "name"):
            continue
        text = str(child).strip()
        if not text:
            continue
        if re.search(r"\d{1,2}\s+[A-Za-z]{3}", text):
            dates_raw = text
        if re.search(r"\d{1,2}h\d{0,2}", text, re.IGNORECASE):
            schedule = text.strip()

    # Horário também pode estar num <p> dentro da secção .text-container
    if not schedule:
        text_container = a_tag.find(class_="text-container")
        if text_container:
            for p in text_container.find_all("p"):
                t = p.get_text(strip=True)
                if re.search(r"\d{1,2}h\d{0,2}", t):
                    schedule = t
                    break

    return {
        "title":    title,
        "subtitle": subtitle,
        "dates_raw": dates_raw,
        "schedule": schedule,
        "img_url":  img_url,
        "url":      url,
    }


def _best_srcset_url(srcset: str) -> str:
    """
    Extrai a URL de maior largura declarada num srcset WordPress.
    Ex: "img-300x169.jpg 300w, img-1920x980.jpg 1400w, img.jpg 1920w"
    → "img.jpg"  (maior w) ou URL original sem sufixo de dimensão.
    """
    if not srcset:
        return ""
    best_url = ""
    best_w   = 0
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        if not tokens:
            continue
        candidate_url = tokens[0]
        w = 0
        if len(tokens) > 1:
            wm = re.match(r"(\d+)w", tokens[1])
            if wm:
                w = int(wm.group(1))
        if w > best_w:
            best_w   = w
            best_url = candidate_url
    return best_url


def _strip_wp_size(url: str) -> str:
    """
    Remove sufixo de dimensão WordPress de uma URL de imagem.
    "https://…/clubepoetas-300x169.jpg" → "https://…/clubepoetas.jpg"
    """
    return _WP_SIZE_RE.sub("", url)


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(
    session: requests.Session,
    url: str,
    stub: dict,
) -> dict | None:
    soup      = None
    full_text = ""

    try:
        r = session.get(url, timeout=25)
        if r.status_code == 200:
            soup      = BeautifulSoup(r.text, "lxml")
            full_text = soup.get_text(" ", strip=True)
    except Exception as e:
        log(f"[{THEATER_NAME}] Timeout em {url}: {e} — usando stub")

    # ── Título ────────────────────────────────────────────────
    title = ""
    if soup:
        h1 = soup.find("h1")
        if not h1:
            # Algumas páginas usam h2.uppercase.headline no hero
            h2_hero = soup.find("h2", class_=re.compile(r"headline|uppercase"))
            if h2_hero:
                h1 = h2_hero
        if h1:
            title = h1.get_text(strip=True)
    if not title:
        title = stub.get("title", "")
    if not title or len(title) < 3:
        return None

    # ── Subtitle ──────────────────────────────────────────────
    # Preferir o stub da listagem; a página tem o mesmo conteúdo no hero
    subtitle = stub.get("subtitle", "")
    if not subtitle and soup:
        hero_span = soup.find("span", class_=re.compile(r"bebas"))
        if hero_span:
            subtitle = re.sub(r"\s+", " ", hero_span.get_text(" ", strip=True))

    # ── Categoria ─────────────────────────────────────────────
    # Tentar deduzir da página; fallback para normalize_category com título
    raw_cat = ""
    if soup:
        # Procurar no <head> ou em meta keywords
        kw_meta = soup.find("meta", attrs={"name": "keywords"})
        if kw_meta:
            raw_cat = kw_meta.get("content", "")
        # Procurar tags/labels de categoria no body
        if not raw_cat:
            for el in soup.find_all(class_=re.compile(r"categ|tag|genre|tipo", re.I)):
                t = el.get_text(strip=True)
                if t:
                    raw_cat = t
                    break
    category = normalize_category(raw_cat or title)

    # ── Datas — fonte primária: stub da listagem ──────────────
    dates_label, date_start, date_end = _parse_date_text(stub.get("dates_raw", ""))
    if not date_start and soup:
        dates_label, date_start, date_end = _parse_dates_from_page(soup)
    if not date_start:
        log(f"[{THEATER_NAME}] Sem data para '{title}' — descartado")
        return None

    # ── Schedule / horário ────────────────────────────────────
    schedule = stub.get("schedule", "")
    if not schedule and soup:
        # Procurar na secção .details da página (ex: "Qua a Sáb 21:00\nDom 16:30")
        details = soup.find(class_="details")
        if details:
            schedule_raw = details.get_text(" ", strip=True)
            hm = re.search(r"(\d{1,2}[h:]\d{0,2})", schedule_raw)
            if hm:
                schedule = hm.group(1)

    # ── Imagem — estratégia em camadas ────────────────────────
    #
    # 1. Hero <img srcset> na página → URL de maior resolução sem sufixo
    # 2. og:image da página → remover sufixo de dimensão WordPress
    # 3. Imagem do stub da listagem (srcset já processado)
    # 4. Primeira <img> de /wp-content/uploads/ sem sufixo de dimensão
    #
    raw_img = ""
    if soup:
        # 1. Hero com srcset
        hero_section = soup.find("section", class_="hero")
        if hero_section:
            hero_img = hero_section.find("img", srcset=True)
            if hero_img:
                best = _best_srcset_url(hero_img.get("srcset", ""))
                if best:
                    raw_img = best if best.startswith("http") else urljoin(BASE, best)

        # 2. og:image (pode conter sufixo -300x169 → limpar)
        if not raw_img:
            og = soup.find("meta", property="og:image")
            if og and og.get("content", "").startswith("http"):
                raw_img = _strip_wp_size(og["content"])

    # 3. Stub da listagem
    if not raw_img:
        raw_img = stub.get("img_url", "")

    # 4. Qualquer upload no body sem sufixo de dimensão
    if not raw_img and soup:
        for img in soup.find_all("img", src=re.compile(r"/wp-content/uploads/")):
            src = img.get("src", "")
            if src and "ticket" not in src and not _WP_SIZE_RE.search(src):
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break

    # Garantir que a URL final não tem sufixo de dimensão
    if raw_img:
        raw_img = _strip_wp_size(raw_img)

    image = build_image_object(raw_img, soup, THEATER_NAME, url) if raw_img else None

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    if soup:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href for kw in ("ticketline", "bol.pt", "bilhete", "comprar")):
                if href.startswith("http"):
                    ticket_url = href
                    break

    # ── Preço ─────────────────────────────────────────────────
    price_info = ""
    price_min: float | None = None
    price_max: float | None = None
    if full_text:
        pm = re.search(
            r"(Entrada\s+(?:livre|gratuita)|gratuito"
            r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[a-zA-Z]+\s*\d+(?:[,\.]\d+)?\s*€)?)",
            full_text, re.IGNORECASE,
        )
        if pm:
            price_info = pm.group(1).strip()
            # Extrair valores numéricos
            nums = re.findall(r"(\d+(?:[,\.]\d+)?)\s*€", price_info)
            if nums:
                vals = [float(n.replace(",", ".")) for n in nums]
                price_min = min(vals)
                price_max = max(vals)

    # ── Classificação etária ──────────────────────────────────
    age_rating = ""
    age_min: int | None = None
    if full_text:
        am = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
        if am:
            age_min    = int(am.group(1))
            age_rating = f"M/{age_min}"

    # ── Duração ───────────────────────────────────────────────
    duration    = ""
    duration_min: int | None = None
    if full_text:
        dm = re.search(r"[Dd]ura[çc][aã]o\s*[:\-–]?\s*(\d+)\s*min", full_text)
        if not dm:
            dm = re.search(r"(\d+)\s*min(?:utos?)?", full_text, re.IGNORECASE)
        if dm:
            duration_min = int(dm.group(1))
            duration     = f"{duration_min} min."

    # ── Sala ──────────────────────────────────────────────────
    sala = ""
    if full_text:
        sm = re.search(
            r"(Sala\s+Carmen\s+Dolores|Sala\s+Est[úu]dio|Sala\s+B)",
            full_text, re.IGNORECASE,
        )
        if sm:
            sala = sm.group(1)

    # ── Sinopse ───────────────────────────────────────────────
    synopsis = _extract_synopsis(soup) if soup else ""

    # ── Ficha técnica + elenco ────────────────────────────────
    technical_sheet, cast = _parse_ficha(soup, full_text)

    # ── Construir evento ──────────────────────────────────────
    ev: dict = {
        "id":          make_id(SOURCE_SLUG, title),
        "title":       title,
        "theater":     THEATER_NAME,
        "category":    category,
        "subtitle":    subtitle,
        "dates_label": dates_label,
        "date_start":  date_start,
        "date_end":    date_end,
        "sessions":    build_sessions(date_start, date_end, schedule),
        "schedule":    schedule,
        "synopsis":    truncate_synopsis(synopsis),
        "image":       image,
        "source_url":  url,
        "ticket_url":  ticket_url,
        "price_info":  price_info,
        "sala":        sala,
        "technical_sheet": technical_sheet,
        "cast":        cast,
    }

    # Campos numéricos — só incluir se tiverem valor
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max
    if age_rating:
        ev["age_rating"] = age_rating
    if age_min is not None:
        ev["age_min"] = age_min
    if duration:
        ev["duration"] = duration
    if duration_min is not None:
        ev["duration_min"] = duration_min

    return ev


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_dates_from_page(soup) -> tuple[str, str, str]:
    """Fallback: extrai datas da página do evento."""
    # Hero — texto da data (<p class="bebas"> no hero)
    hero = soup.find("section", class_="hero")
    if hero:
        for p in hero.find_all("p"):
            result = _parse_date_text(p.get_text(strip=True))
            if result[1]:
                return result

    # Elementos com classes relacionadas com data
    for el in soup.find_all(
        ["span", "div", "p", "time"],
        class_=re.compile(r"dat|period|time|when", re.I),
    ):
        result = _parse_date_text(el.get_text(strip=True))
        if result[1]:
            return result

    # Procurar padrão no início do texto da página
    text = soup.get_text(" ", strip=True)[:600]
    return _parse_date_text(text)


def _parse_date_text(text: str) -> tuple[str, str, str]:
    """
    Formatos do Trindade:
      "30 Abr - 02 Ago 2026"    (meses distintos, mesmo ano)
      "29 Jan - 05 Abr 2026"
      "30 ABR a 2 AGO"          (formato da página individual, sem ano)
      "26 Mai 2026"             (data única)
    """
    if not text:
        return "", "", ""
    text = text.strip()

    # Separadores possíveis: " - ", " – ", " a " (case-insensitive)
    SEP = r"\s*(?:[-–]|\ba\b)\s*"

    # "DD Mês [YYYY] SEP DD Mês YYYY"
    m = re.search(
        r"(\d{1,2})\s+([A-Za-záéíóúçã]{3,})(?:\s+(\d{4}))?"
        + SEP +
        r"(\d{1,2})\s+([A-Za-záéíóúçã]{3,})\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, y1_opt, d2, mo2, y2 = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            y2i = int(y2)
            y1i = int(y1_opt) if y1_opt else y2i
            return (
                f"{int(d1):02d} {mo1.capitalize()} – {int(d2):02d} {mo2.capitalize()} {y2}",
                f"{y1i}-{n1:02d}-{int(d1):02d}",
                f"{y2i}-{n2:02d}-{int(d2):02d}",
            )

    # "DD Mês YYYY" — data única
    m = re.search(
        r"(\d{1,2})\s+([A-Za-záéíóúçã]{3,})\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        d, mon_s, yr = m.groups()
        n = _mon(mon_s)
        if n:
            y  = int(yr)
            ds = f"{y}-{n:02d}-{int(d):02d}"
            return f"{int(d):02d} {mon_s.capitalize()} {yr}", ds, ds

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower().strip()[:3])


# ─────────────────────────────────────────────────────────────
# Extracção de sinopse
# ─────────────────────────────────────────────────────────────

def _extract_synopsis(soup) -> str:
    if not soup:
        return ""

    # og:description como base de comparação (pode ser truncado)
    og = soup.find("meta", property="og:description")
    og_text = og.get("content", "").strip() if og else ""

    synopsis = ""
    main = soup.find("main") or soup.find("article") or soup

    # Procurar parágrafos com conteúdo substantivo, ignorar boilerplate
    _SKIP_RE = re.compile(
        r"^(O PREÇ[AÁ]RIO|Consulte|CONVERSA|©|Saltar|Mapa do|Ajuda|"
        r"Teatro da Trindade|Fundação INATEL|Rua Nova|Contactos|Siga|"
        r"\+351|Made by|Política)",
        re.IGNORECASE,
    )

    # Ignorar secções de ficha artística e outros espectáculos
    skip_sections = set()
    for section in main.find_all("section"):
        cls = " ".join(section.get("class", []))
        if "events-section" in cls or "light" in cls:
            skip_sections.add(id(section))

    for p in main.find_all("p"):
        # Verificar se está dentro de uma secção a ignorar
        if any(id(ancestor) in skip_sections for ancestor in p.parents):
            continue
        t = p.get_text(strip=True)
        if len(t) < 60:
            continue
        if _SKIP_RE.match(t):
            continue
        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break

    return synopsis.strip() or og_text


# ─────────────────────────────────────────────────────────────
# Parsing da ficha técnica + elenco
# ─────────────────────────────────────────────────────────────

def _parse_ficha(soup, full_text: str) -> tuple[dict, list[str]]:
    """
    Extrai a ficha técnica e o elenco.

    Estratégia principal: encontrar a secção #ficha-artistica e parsear
    os <p> que contêm chave + <strong>valor</strong>.
    Fallback: regex sobre o texto completo.
    """
    ficha: dict       = {}
    cast:  list[str]  = []

    # ── Estratégia estruturada (HTML) ─────────────────────────
    if soup:
        ficha_section = None
        # Procurar âncora #ficha-artistica
        anchor = soup.find(id="ficha-artistica")
        if anchor:
            # A secção é normalmente o pai ou o seguinte irmão
            ficha_section = anchor.find_parent("section") or anchor.find_next_sibling()

        if not ficha_section:
            # Tentar por classe
            ficha_section = soup.find(class_=re.compile(r"ficha|artistica|technical", re.I))

        if ficha_section:
            for p in ficha_section.find_all("p"):
                # Padrão: texto antes do <strong> é a chave, <strong> é o valor
                # Ex: <p>De <strong>Tom Schulman</strong></p>
                # Ex: <p>Encenação <strong>Hélder Gamboa</strong></p>
                p_text = p.get_text(" ", strip=True)
                strongs = p.find_all("strong")
                if not strongs:
                    continue
                value = ", ".join(s.get_text(strip=True) for s in strongs if s.get_text(strip=True))
                if not value:
                    continue

                # Determinar a chave pelo texto fora dos <strong>
                raw_key = re.sub(r"<[^>]+>", "", str(p))  # strip tags
                raw_key = p_text
                for sv in strongs:
                    raw_key = raw_key.replace(sv.get_text(strip=True), "").strip()
                raw_key = raw_key.rstrip(":– ").lower()

                key = _normalise_ficha_key(raw_key)
                if not key:
                    continue

                if key in ("interpretação", "elenco", "com"):
                    # Elenco → lista separada
                    cast = [n.strip() for n in re.split(r",\s*|\s+e\s+", value) if n.strip()]
                    ficha["interpretação"] = value
                elif key not in ficha:
                    ficha[key] = value

    # ── Fallback: regex sobre texto completo ──────────────────
    if not ficha and full_text:
        known_keys = [
            ("texto",            r"[Tt]exto\s+(?:e\s+[Ee]ncena[çc][aã]o\s+)?(?:de\s+)?"),
            ("encenação",        r"[Ee]ncena[çc][aã]o\s+(?:de\s+)?|[Vv]ers[aã]o\s+e\s+[Ee]ncena[çc][aã]o\s+(?:de\s+)?"),
            ("autor",            r"[Dd]e\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
            ("dramaturgia",      r"[Dd]ramaturgia\s+(?:de\s+)?"),
            ("direção",          r"[Dd]ire[çc][aã]o\s+(?:de\s+)?"),
            ("tradução",         r"[Tt]radu[çc][aã]o\s+(?:de\s+)?"),
            ("adaptação",        r"[Aa]dapta[çc][aã]o\s+(?:de\s+)?"),
            ("coreografia",      r"[Cc]oreografia\s+(?:de\s+)?"),
            ("música",           r"[Mm][úu]sica\s+(?:de\s+)?"),
            ("cenografia",       r"[Cc]enografia\s+(?:de\s+)?"),
            ("figurinos",        r"[Ff]igurinos?\s+(?:de\s+)?"),
            ("luz",              r"[Dd]esenho\s+de\s+[Ll]uz\s+(?:de\s+)?|[Ll]uz\s+(?:de\s+)?"),
            ("som",              r"[Ss]onoplastia\s+(?:de\s+)?|[Ss]om\s+(?:de\s+)?"),
            ("interpretação",    r"[Ii]nterpreta[çc][aã]o\s+(?:de\s+)?|[Cc]om\s+(?=[A-Z])|[Ee]lenco\s+"),
            ("produção",         r"[Pp]rodu[çc][aã]o\s+(?:de\s+)?"),
            ("coprodução",       r"[Cc]o-?[Pp]rodu[çc][aã]o\s+(?:de\s+)?"),
        ]
        positions = []
        for key, pattern in known_keys:
            for match in re.finditer(pattern, full_text):
                positions.append((match.start(), match.end(), key))
        positions.sort()
        for i, (start, end, key) in enumerate(positions):
            next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 250
            value = re.sub(r"\s+", " ", full_text[end:next_start].strip())
            value = re.split(r"\n|(?:\s{2,})", value)[0]
            value = re.split(r"\s+(?:Apoio|©|CONVERSA)", value, flags=re.IGNORECASE)[0]
            value = value[:200].strip()
            if value and len(value) > 2 and key not in ficha:
                ficha[key] = value
                if key == "interpretação" and not cast:
                    cast = [n.strip() for n in re.split(r",\s*|\s+e\s+", value) if n.strip()]

    return ficha, cast


def _normalise_ficha_key(raw: str) -> str:
    """Normaliza uma chave de ficha técnica para slug canónico."""
    raw = raw.strip().lower()
    mapping = {
        "de":                "autor",
        "texto":             "texto",
        "encenação":         "encenação",
        "encenat":           "encenação",
        "versão e encenação":"encenação",
        "direção":           "direção",
        "dramaturgia":       "dramaturgia",
        "tradução":          "tradução",
        "adaptação":         "adaptação",
        "coreografia":       "coreografia",
        "música":            "música",
        "cenografia":        "cenografia",
        "figurinos":         "figurinos",
        "figurino":          "figurinos",
        "desenho de luz":    "luz",
        "luz":               "luz",
        "sonoplastia":       "som",
        "som":               "som",
        "com":               "interpretação",
        "interpretação":     "interpretação",
        "elenco":            "interpretação",
        "produção":          "produção",
        "produção executiva":"produção executiva",
        "coprodução":        "coprodução",
        "co-produção":       "coprodução",
        "comunicação":       "comunicação",
        "adereços":          "adereços",
        "cabelos":           "cabelos",
        "assistência de encenação": "assistência de encenação",
    }
    for k, v in mapping.items():
        if raw.startswith(k):
            return v
    return ""
