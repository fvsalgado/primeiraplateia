"""
Scraper: Theatro Circo
Fonte: https://theatrocirco.com/programa/
Cidade: Braga

Estrutura do site (HTML estático, WordPress):
  - Listagem: página única com todos os eventos, sem paginação.
    Cada evento é um bloco com link /event/<slug>/, imagem, data,
    categoria visível (→ Teatro), título e subtítulo.
    Inclui três secções na mesma página: programação geral,
    CTB (Companhia de Teatro de Braga) e "Mais programação" —
    todas com a mesma estrutura de URLs, tratadas da mesma forma.
  - Página de evento: HTML limpo com h1 (título), h2 (companhia),
    data/hora/sala em texto, preço, classificação etária, sinopse
    em parágrafos, ficha técnica em bold+texto, galeria de imagens.

Filtragem:
  - Aceitar: Teatro, Música+Teatro (dupla categoria)
  - Aceitar com categoria Infanto-Juvenil: se tag infantojuvenil presente
  - Rejeitar: Música, Dança, Cinema, Mediação, Multidisciplinar,
    Cineconcerto, Conversa, Oficina, OPEN CALL
"""

import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
    parse_date, MONTHS,
)

_BASE = "https://theatrocirco.com"


def _fetch_logo() -> str:
    """
    O logo do Theatro Circo é um SVG inline na homepage.
    Extrai-o e devolve como data URI (image/svg+xml; base64)
    para poder ser usado em <img src="..."> no portal.
    Devolve string vazia em caso de erro.
    """
    import base64
    try:
        r = requests.get(_BASE + "/", headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        logo_a = soup.find(
            "a",
            attrs={"aria-label": re.compile(r"página inicial", re.IGNORECASE)},
        )
        if logo_a:
            svg = logo_a.find("svg")
            if svg:
                svg_str = str(svg)
                encoded = base64.b64encode(svg_str.encode("utf-8")).decode("ascii")
                return f"data:image/svg+xml;base64,{encoded}"
    except Exception as e:
        log(f"[Theatro Circo] Não foi possível extrair logo: {e}")
    return ""


# ─────────────────────────────────────────────────────────────
# Metadados do teatro — lidos pelo sync_scrapers.py
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "theatrocirco",
    "name":        "Theatro Circo",
    "short":       "Theatro Circo",
    "color":       "#b71c1c",
    "city":        "Braga",
    "address":     "Av. da Liberdade, 697, 4710-251 Braga",
    "site":        "https://theatrocirco.com",
    "programacao": "https://theatrocirco.com/programa/?agenda_category=teatro-pt",
    "logo":        _fetch_logo(),   # SVG inline extraído da homepage
    "lat":         41.5454,
    "lng":         -8.4265,
    "salas":       ["Sala Principal", "Pequeno Auditório"],
    "aliases": [
        "theatro circo",
        "theatro circo braga",
        "teatro circo",
        "teatro circo braga",
    ],
    "description": (
        "O Theatro Circo é um dos mais emblemáticos teatros do norte de Portugal, "
        "inaugurado em 1915 em Braga. Com uma programação diversa de teatro, música, "
        "dança e cinema, é referência cultural da cidade e da região."
    ),
}

THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]
BASE         = "https://theatrocirco.com"
AGENDA       = f"{BASE}/programa/?agenda_category=teatro-pt"

# Categorias que resultam em Teatro no schema do Primeira Plateia
_ACCEPT_CATEGORIES = {"teatro"}

# Categorias a rejeitar sem visitar a página do evento
_REJECT_CATEGORIES = {
    "música", "musica", "dança", "danca", "cinema", "mediação", "mediacao",
    "multidisciplinar", "cineconcerto", "conversa", "oficina", "open call",
}

_PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    candidates = _collect_candidates()
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos de teatro na listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in candidates:
        try:
            ev = _scrape_event(item["url"], item["is_infantil"])
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro em {item['url']}: {e}")
        time.sleep(0.4)

    log(f"[{THEATER_NAME}] {len(events)} eventos de teatro")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates() -> list[dict]:
    """
    Percorre a listagem e devolve apenas eventos de Teatro.

    Estrutura HTML da listagem:
      Cada evento é um div.highlight-card com:
        p.highlight-meta  → "DD mês (dia) → Categoria[, Categoria]"
        div.highlight-image > a[href=/event/...]  → URL (contém só <img>, sem texto)
        p > em            → título (em itálico)
        p                 → subtítulo / companhia

    Bug anterior: o scraper iterava as tags <a href=/event/> e chamava
    a.get_text() — mas essas tags contêm apenas <img>, pelo que o texto
    era sempre vazio e _extract_category_from_block nunca encontrava "→".
    """
    try:
        r = requests.get(AGENDA, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    for card in soup.select("div.highlight-card"):
        # URL — está no <a> dentro de div.highlight-image
        a_tag = card.select_one("div.highlight-image a[href]")
        if not a_tag:
            continue
        href = a_tag["href"]
        url  = href if href.startswith("http") else urljoin(BASE, href)
        url  = url.rstrip("/") + "/"
        if url in seen_urls:
            continue

        # Categoria — p.highlight-meta: "DD mês → Categoria"
        # Usar a versão desktop (evitar duplicado mobile)
        meta_el = card.select_one("p.highlight-meta.desktop, p.highlight-meta")
        if not meta_el:
            continue
        meta_text = meta_el.get_text(" ", strip=True)

        category_raw = _extract_category_from_block(meta_text)
        if not category_raw:
            continue

        categories = [c.strip().lower() for c in category_raw.split(",")]
        has_teatro  = any(c in _ACCEPT_CATEGORIES for c in categories)
        all_rejected = all(c in _REJECT_CATEGORIES for c in categories)

        if not has_teatro:
            if all_rejected:
                continue
            continue  # categoria desconhecida — ignorar por defeito

        is_infantil = "infantojuvenil" in meta_text.lower()

        seen_urls.add(url)
        candidates.append({
            "url":         url,
            "is_infantil": is_infantil,
        })

    return candidates


def _extract_category_from_block(text: str) -> str:
    """
    Extrai categoria do texto do bloco.
    Formato na listagem: "27 março (sex) → Teatro" ou "→ Música, Teatro"
    """
    m = re.search(r"→\s*(.+?)(?:\n|$)", text)
    if m:
        return m.group(1).strip()
    # Fallback: último elemento de texto antes de título
    return ""


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(url: str, is_infantil: bool) -> dict | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # ── Título ────────────────────────────────────────────────
    h1 = soup.find("h1")
    if not h1:
        return None
    title = h1.get_text(strip=True)
    if not title or len(title) < 3:
        return None

    # ── Subtítulo / companhia ─────────────────────────────────
    subtitle = ""
    h2 = soup.find("h2")
    if h2:
        sub = h2.get_text(strip=True)
        # Ignorar h2 que sejam headings de secção (ex: "CTB - Companhia de Teatro de Braga")
        if sub and len(sub) < 120 and sub != title:
            subtitle = sub

    # ── Categoria ─────────────────────────────────────────────
    # Verificar tag infantojuvenil na página (mais fiável que na listagem)
    page_tags = [a.get_text(strip=True).lower() for a in soup.find_all("a", href=re.compile(r"/event_tag/"))]
    infantil  = is_infantil or any("infantojuvenil" in t for t in page_tags)
    category  = "Infanto-Juvenil" if infantil else "Teatro"

    # ── Datas ────────────────────────────────────────────────
    dates_label, date_start, date_end = _parse_dates(soup, full_text)
    if not date_start:
        return None

    # ── Hora e sala ───────────────────────────────────────────
    schedule = ""
    sala     = ""

    # O site apresenta hora e sala em texto simples após a data
    # Ex: "21h30\nSala Principal" ou "Sex 14h30 (escolas) | Sáb 11h (público geral)\nPequeno Auditório"
    hora_m = re.search(r"(\d{1,2}[h:]\d{2})", full_text)
    if hora_m:
        schedule = hora_m.group(1)

    sala_m = re.search(
        r"(Sala\s+Principal|Pequeno\s+Audit[oó]rio|Grande\s+Audit[oó]rio|Foyer)",
        full_text, re.IGNORECASE,
    )
    if sala_m:
        sala = sala_m.group(1)

    # ── Preço ─────────────────────────────────────────────────
    price_info = ""
    preco_m = re.search(
        r"(Entrada\s+(?:livre|gratuita)"
        r"|Gratuito"
        r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[-|]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if preco_m:
        price_info = preco_m.group(1).strip()

    # ── Classificação etária ───────────────────────────────────
    age_rating = ""
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if age_m:
        age_rating = f"M/{age_m.group(1)}"

    # ── Duração ───────────────────────────────────────────────
    duration = ""
    dur_m = re.search(r"[Dd]ura[çc][aã]o\s+(\d+)\s*minutos?", full_text)
    if not dur_m:
        dur_m = re.search(r"\b(\d{2,3})\s*min(?:utos?)?\b", full_text)
    if dur_m:
        duration = f"{dur_m.group(1)} min."

    # ── Imagem ────────────────────────────────────────────────
    image   = None
    raw_img = ""

    # og:image é o mais fiável
    og = soup.find("meta", property="og:image")
    if og:
        raw_img = og.get("content", "")

    # Fallback: primeira imagem da galeria (wp-content/uploads)
    if not raw_img or not raw_img.startswith("http"):
        for img in soup.find_all("img", src=re.compile(r"/wp-content/uploads/")):
            src = img.get("src", "")
            # Ignorar imagens de logo ou ícones (geralmente < 200 chars de URL)
            if src and len(src) > 40 and "logo" not in src.lower():
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break

    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "bol.pt" in href and "Comprar" in href:
            ticket_url = href if href.startswith("http") else urljoin(BASE, href)
            break

    # ── Sinopse ───────────────────────────────────────────────
    synopsis = _extract_synopsis(soup)

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_ficha(soup)

    # ── Acessibilidade ────────────────────────────────────────
    accessibility = ""
    if re.search(r"L[Gg][Pp]|L[íi]ngua\s+[Gg]estual", full_text):
        accessibility = "LGP"
    if re.search(r"[Aa]udiodes[ck]ri[çc][aã]o", full_text):
        accessibility = (accessibility + " + Audiodescrição").strip(" +")

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
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "sala":            sala,
        "accessibility":   accessibility,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_dates(soup, text: str) -> tuple[str, str, str]:
    """
    Formatos encontrados no Theatro Circo:
      "27 março (sex)"                     → data única
      "15 e 16 maio (sex e sáb)"           → intervalo mesmo mês
      "1, 2, 3 e 6 junho (seg a sáb)"      → múltiplas datas mesmo mês
      "17 a 24 abril"                       → intervalo
      "12 janeiro a 18 abril"              → intervalo meses distintos
    """
    # Tentar no h1 de data (o site tem um bloco de data antes do h1 principal)
    # Formato típico: "27 março (sex) → Teatro"
    date_candidates = []

    # Recolher todos os textos com padrão de data
    for el in soup.find_all(string=re.compile(
        r"\d{1,2}\s+(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)",
        re.IGNORECASE,
    )):
        date_candidates.append(el.strip())

    # Incluir texto completo como fallback
    date_candidates.append(text)

    for src in date_candidates:
        result = _parse_date_text(src)
        if result[1]:
            return result

    return "", "", ""


def _parse_date_text(text: str) -> tuple[str, str, str]:
    """
    Converte texto de data para (dates_label, date_start, date_end).
    """
    if not text:
        return "", "", ""

    text = text.strip()

    # "DD mês a DD mês [YYYY]" — intervalo meses distintos
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, d2, mo2, yr = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            y = int(yr) if yr else _infer_year(n2, int(d2))
            y1 = _infer_year(n1, int(d1)) if not yr else y
            return (
                f"{d1} {mo1} a {d2} {mo2}",
                f"{y1}-{n1:02d}-{int(d1):02d}",
                f"{y}-{n2:02d}-{int(d2):02d}",
            )

    # "DD e DD mês" ou "DD, DD e DD mês" — múltiplas datas mesmo mês
    m = re.search(
        r"(\d{1,2})(?:[,\s]+\d{1,2})*\s+e\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        # Extrair primeira e última data
        all_days = re.findall(r"\d{1,2}", text.split(m.group(3))[0])
        d1       = all_days[0] if all_days else m.group(1)
        d2       = m.group(2)
        mon_s    = m.group(3)
        yr       = m.group(4)
        n        = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            return (
                f"{d1} e {d2} {mon_s}",
                f"{y}-{n:02d}-{int(d1):02d}",
                f"{y}-{n:02d}-{int(d2):02d}",
            )

    # "DD a DD mês [YYYY]" — intervalo mesmo mês
    m = re.search(
        r"(\d{1,2})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
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

    # "DD mês [YYYY]" — data única
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
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
# Extracção de sinopse
# ─────────────────────────────────────────────────────────────

def _extract_synopsis(soup) -> str:
    """
    Sinopse: parágrafos substantivos antes da ficha técnica.
    O site separa sinopse e ficha com um <strong> "Ficha técnica".
    """
    synopsis = ""

    # Encontrar ponto de início da ficha técnica
    ficha_start = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            ficha_start = el
            break

    # Recolher parágrafos antes da ficha técnica
    for p in soup.find_all("p"):
        # Parar ao chegar à ficha técnica
        if ficha_start and ficha_start in p.descendants:
            break
        if ficha_start:
            # Verificar se este parágrafo está depois da ficha técnica
            try:
                p_pos    = list(soup.descendants).index(p)
                fic_pos  = list(soup.descendants).index(ficha_start)
                if p_pos > fic_pos:
                    break
            except ValueError:
                pass

        t = p.get_text(strip=True)
        # Ignorar parágrafos muito curtos ou claramente não-sinopse
        if len(t) < 40:
            continue
        if re.match(
            r"^(Este site|Inscreva|Pretende receber|campos de preenchimento|A reserva|Os seus dados)",
            t, re.IGNORECASE,
        ):
            continue
        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break

    # Fallback: og:description
    if not synopsis:
        og = soup.find("meta", property="og:description")
        if og:
            synopsis = og.get("content", "").strip()

    return synopsis


# ─────────────────────────────────────────────────────────────
# Parsing da ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(soup) -> dict:
    """
    A ficha técnica no Theatro Circo usa o padrão:
      <strong>Criação, texto e interpretação</strong> Sara Inês Gigante
    Os campos estão em <strong> seguidos de texto no mesmo nó ou nó irmão.
    """
    ficha      = {}
    known_keys = [
        ("criação",         r"[Cc]ria[çc][aã]o(?:[^<]{0,40}?)?"),
        ("texto",           r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?"),
        ("encenação",       r"[Ee]ncena[çc][aã]o"),
        ("direção",         r"[Dd]ire[çc][aã]o\s+[Aa]rt[íi]stica|[Dd]ire[çc][aã]o"),
        ("dramaturgia",     r"[Dd]ramaturgia"),
        ("interpretação",   r"[Ii]nterpreta[çc][aã]o"),
        ("tradução",        r"[Tt]radu[çc][aã]o"),
        ("adaptação",       r"[Aa]dapta[çc][aã]o"),
        ("cenografia",      r"[Cc]enografia|[Ee]spa[çc]o\s+[Cc][ée]nico|[Dd]esenho\s+de\s+[Ee]spa[çc]o"),
        ("figurinos",       r"[Ff]igurinos?"),
        ("luz",             r"[Dd]esenho\s+de\s+[Ll]uz|[Ii]lumina[çc][aã]o"),
        ("som",             r"[Ss]onoplastia|[Dd]esenho\s+[Ss]onoro|[Dd]esenho\s+de\s+[Ss]om"),
        ("música",          r"[Mm][úu]sica\s+[Oo]riginal|[Mm][úu]sica"),
        ("produção",        r"[Pp]rodu[çc][aã]o\s+[Ee]xecutiva|[Pp]rodu[çc][aã]o"),
        ("coprodução",      r"[Cc]o-?[Pp]rodu[çc][aã]o"),
        ("fotografia",      r"[Ff]otografia(?:\s+e\s+[Dd]esign\s+[Gg]ráfico)?"),
    ]

    # Usar texto completo da ficha para extracção posicional
    ficha_el = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            ficha_el = el.find_parent()
            break

    if not ficha_el:
        # Fallback: usar texto completo da página
        text = soup.get_text(" ", strip=True)
    else:
        text = ficha_el.get_text(" ", strip=True)

    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()

    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        # Parar antes de "Apoio", "Agradecimentos", "Duração", "©"
        value = re.split(
            r"\s+(?:Apoio|Agradecimentos|Duração|©|Parcerias|Residências)",
            value, flags=re.IGNORECASE,
        )[0]
        value = value[:200].strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha
