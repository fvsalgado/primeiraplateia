"""
Scraper: Teatro da Trindade INATEL
Fonte: https://teatrotrindade.inatel.pt/programacao/teatro
Cidade: Lisboa

Estrutura do site (WordPress, HTML estático):
  - Listagem: /programacao/teatro — filtra por Teatro no servidor.
    Cada card é um <a href="/espetaculo/slug/"> com:
      - Imagem (<img>)
      - Texto de data como nó de texto directo antes do <h3>
        (ex: "29 Jan - 05 Abr 2026")
      - <h3> com título
    IMPORTANTE: o filtro ?type=94 é aplicado por JS no cliente, não usar.
    A URL /programacao/teatro filtra correctamente no servidor.
  - Página de evento: /espetaculo/<slug>/
    O servidor bloqueia requests simples. Usar Session com headers de browser.
  - Filtragem secundária: verificar título/subtítulo na página de evento
    para garantir que não entram concertos ou outras categorias que possam
    aparecer na listagem.
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
    "programacao": "https://teatrotrindade.inatel.pt/programacao/teatro",
    "lat":         38.7107,
    "lng":         -9.1414,
    "salas":       ["Sala Carmen Dolores", "Sala Estúdio"],
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
AGENDA       = f"{BASE}/programacao/teatro"

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

# Padrões no título/subtítulo que indicam que NÃO é teatro
_REJECT_RE = re.compile(
    r"\bconcerto\b|\bciclo de concertos\b|\bfrequência 440\b|\bfrequencia 440\b"
    r"|\brecital\b|\bópera\b|\bopera\b",
    re.IGNORECASE,
)

_PT_MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}


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

    log(f"[{THEATER_NAME}] {len(events)} eventos de teatro")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates(session: requests.Session) -> list[dict]:
    """
    Lê /programacao/teatro e extrai URLs e stubs dos cards.

    A data está como nó de texto directo dentro do <a>, antes do <h3>.
    Usar children directos — get_text() do bloco inteiro apanha título
    e texto do rodapé da página, contaminando as datas.
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

        # Rejeitar imediatamente pela listagem se o título indica não-teatro
        if _REJECT_RE.search(stub["title"]):
            log(f"[{THEATER_NAME}] Rejeitado na listagem: '{stub['title']}'")
            continue

        candidates.append({"url": url, "stub": stub})

    return candidates


def _extract_card_stub(a_tag, url: str) -> dict:
    """
    Extrai dados do card com parsing estruturado.

    Data: nó de texto directo do <a> com padrão DD Mês - DD Mês YYYY.
    Título: texto do <h3> filho do <a>.
    Imagem: primeiro <img> filho do <a> que não seja ícone de bilhete.
    """
    title     = ""
    dates_raw = ""
    img_url   = ""

    # Título — h3 directo
    h3 = a_tag.find("h3")
    if h3:
        title = h3.get_text(strip=True)

    # Imagem — ignorar ticket.svg e logos
    img = a_tag.find("img")
    if img:
        src = img.get("src", "")
        if src and "ticket.svg" not in src and "logo" not in src.lower():
            img_url = src if src.startswith("http") else urljoin(BASE, src)

    # Data — percorrer APENAS filhos directos, ignorar elementos HTML
    # Os nós de texto directos são NavigableString
    for child in a_tag.children:
        if hasattr(child, "name"):
            # É um elemento HTML — ignorar (inclui <img>, <h3>, <strong>)
            continue
        text = str(child).strip()
        if not text:
            continue
        # Verificar se tem padrão de data: número + mês abreviado
        if re.search(r"\d{1,2}\s+[A-Za-z]{3}", text):
            dates_raw = text
            break

    return {"title": title, "dates_raw": dates_raw, "img_url": img_url, "url": url}


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

    # ── Verificação secundária de categoria na página ─────────
    if soup:
        h1_text = (soup.find("h1") or soup).get_text(strip=True)
        h2_el   = soup.find("h2")
        h2_text = h2_el.get_text(strip=True) if h2_el else ""
        if _REJECT_RE.search(h1_text + " " + h2_text):
            log(f"[{THEATER_NAME}] Rejeitado na página: '{stub.get('title')}'")
            return None

    # ── Título ────────────────────────────────────────────────
    title = ""
    if soup:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
    if not title:
        title = stub.get("title", "")
    if not title or len(title) < 3:
        return None

    # ── Datas — fonte primária: stub da listagem ──────────────
    # O stub tem a data limpa directamente do nó de texto do card.
    # A página de evento é usada apenas como fallback.
    dates_label, date_start, date_end = _parse_date_text(stub.get("dates_raw", ""))
    if not date_start and soup:
        dates_label, date_start, date_end = _parse_dates_from_page(soup)
    if not date_start:
        log(f"[{THEATER_NAME}] Sem data para '{title}' — descartado")
        return None

    # ── Imagem — preferir og:image da página (maior resolução) ─
    image   = None
    raw_img = ""
    if soup:
        og = soup.find("meta", property="og:image")
        if og and og.get("content", "").startswith("http"):
            raw_img = og["content"]
    if not raw_img:
        raw_img = stub.get("img_url", "")
    if not raw_img and soup:
        for img in soup.find_all("img", src=re.compile(r"/wp-content/uploads/")):
            src = img.get("src", "")
            if src and "ticket" not in src and len(src) > 40:
                raw_img = src if src.startswith("http") else urljoin(BASE, src)
                break
    if raw_img:
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Bilhetes ──────────────────────────────────────────────
    ticket_url = ""
    if soup:
        for a in soup.find_all("a", href=True):
            if "ticketline" in a["href"]:
                ticket_url = a["href"]
                break

    # ── Preço ─────────────────────────────────────────────────
    price_info = ""
    if full_text:
        pm = re.search(
            r"(Entrada\s+(?:livre|gratuita)|gratuito"
            r"|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
            full_text, re.IGNORECASE,
        )
        if pm:
            price_info = pm.group(1).strip()

    # ── Classificação etária ───────────────────────────────────
    age_rating = ""
    if full_text:
        am = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
        if am:
            age_rating = f"M/{am.group(1)}"

    # ── Duração ───────────────────────────────────────────────
    duration = ""
    if full_text:
        dm = re.search(r"(\d+)\s*min(?:utos?)?", full_text, re.IGNORECASE)
        if dm:
            duration = f"{dm.group(1)} min."

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

    # ── Ficha técnica ─────────────────────────────────────────
    technical_sheet = _parse_ficha(full_text) if full_text else {}

    return {
        "id":              make_id(SOURCE_SLUG, title),
        "title":           title,
        "theater":         THEATER_NAME,
        "category":        "Teatro",
        "dates_label":     dates_label,
        "date_start":      date_start,
        "date_end":        date_end,
        "sessions":        build_sessions(date_start, date_end, ""),
        "schedule":        "",
        "synopsis":        truncate_synopsis(synopsis),
        "image":           image,
        "source_url":      url,
        "ticket_url":      ticket_url,
        "price_info":      price_info,
        "duration":        duration,
        "age_rating":      age_rating,
        "sala":            sala,
        "technical_sheet": technical_sheet,
    }


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_dates_from_page(soup) -> tuple[str, str, str]:
    """Fallback: extrai datas da página do evento."""
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
    Formatos do Trindade na listagem:
      "29 Jan - 05 Abr 2026"   (meses distintos, separador " - ")
      "23 Abr - 07 Jun 2026"
      "26 Mai 2026"             (data única)
    """
    if not text:
        return "", "", ""
    text = text.strip()

    # "DD Mês - DD Mês YYYY" (com ou sem ano no início)
    m = re.search(
        r"(\d{1,2})\s+([A-Za-záéíóúçã]{3,})(?:\s+(\d{4}))?"
        r"\s*[-–]\s*"
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
                f"{d1} {mo1} – {d2} {mo2} {y2}",
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
            return f"{d} {mon_s} {yr}", ds, ds

    return "", "", ""


def _mon(s: str) -> int | None:
    return _PT_MONTHS.get(s.lower().strip()[:3])


# ─────────────────────────────────────────────────────────────
# Extracção de sinopse
# ─────────────────────────────────────────────────────────────

def _extract_synopsis(soup) -> str:
    og = soup.find("meta", property="og:description")
    og_text = og.get("content", "").strip() if og else ""
    synopsis = ""
    main = soup.find("main") or soup.find("article") or soup
    for p in main.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) < 60:
            continue
        if re.match(
            r"^(O PREÇ[AÁ]RIO|Consulte|CONVERSA|©|Saltar|Mapa do|Ajuda|"
            r"Teatro da Trindade|Fundação INATEL|Rua Nova)",
            t, re.IGNORECASE,
        ):
            continue
        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break
    return synopsis.strip() or og_text


# ─────────────────────────────────────────────────────────────
# Parsing da ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(text: str) -> dict:
    ficha      = {}
    known_keys = [
        ("texto",         r"[Tt]exto\s+(?:e\s+[Ee]ncena[çc][aã]o\s+)?(?:de\s+)?"),
        ("encenação",     r"[Ee]ncena[çc][aã]o\s+(?:de\s+)?|[Vv]ers[aã]o\s+e\s+[Ee]ncena[çc][aã]o\s+(?:de\s+)?"),
        ("autor",         r"[Dd]e\s+(?=[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇÑ])"),
        ("dramaturgia",   r"[Dd]ramaturgia\s+(?:de\s+)?"),
        ("direção",       r"[Dd]ire[çc][aã]o\s+(?:de\s+)?"),
        ("tradução",      r"[Tt]radu[çc][aã]o\s+(?:de\s+)?"),
        ("adaptação",     r"[Aa]dapta[çc][aã]o\s+(?:de\s+)?"),
        ("música",        r"[Mm][úu]sica\s+(?:de\s+)?"),
        ("cenografia",    r"[Cc]enografia\s+(?:de\s+)?"),
        ("figurinos",     r"[Ff]igurinos?\s+(?:de\s+)?"),
        ("interpretação", r"[Ii]nterpreta[çc][aã]o\s+(?:de\s+)?|[Ee]lenco\s+"),
        ("produção",      r"[Pp]rodu[çc][aã]o\s+(?:de\s+)?"),
        ("coprodução",    r"[Cc]o-?[Pp]rodu[çc][aã]o\s+(?:de\s+)?"),
    ]
    positions = []
    for key, pattern in known_keys:
        for match in re.finditer(pattern, text):
            positions.append((match.start(), match.end(), key))
    positions.sort()
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 250
        value      = re.sub(r"\s+", " ", text[end:next_start].strip())
        value      = re.split(r"\n|(?:\s{2,})", value)[0]
        value      = re.split(r"\s+(?:Apoio|©|CONVERSA)", value, flags=re.IGNORECASE)[0]
        value      = value[:200].strip()
        if value and len(value) > 2 and key not in ficha:
            ficha[key] = value
    return ficha
