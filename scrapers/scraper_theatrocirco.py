"""
Scraper: Theatro Circo
Fonte: https://theatrocirco.com/programa/
Cidade: Braga

Estrutura do site (HTML estático, WordPress / The Events Calendar):
  - Listagem: /programa/ — página única com TODOS os eventos (sem paginação).
    Cada evento é um div.highlight-card com:
      • p.highlight-meta  → "DD mês (dia) → Categoria[, Categoria]"
        (existe versão .desktop e versão mobile com a mesma info; usar só .desktop
        ou a primeira encontrada para evitar duplicados)
      • div.highlight-image > a[href]  → URL do evento (contém <img> ou <video>,
        NÃO texto — bug do scraper antigo: chamava a.get_text() aqui)
      • h3 > em  → título em itálico
      • h3 (sem em)  → título sem itálico
      • h3 seguinte → subtítulo / companhia
      • a.highlight-tag → tags (ex: Infantojuvenil, Acessibilidade, Aniversário)
      • a[href*="bol.pt"] → ticket_url directo da listagem

  - Página de evento individual (sempre visitada):
      • og:image        → imagem principal (mais fiável que a da listagem)
      • og:description  → synopsis de fallback
      • h1              → título
      • h2              → subtítulo / companhia
      • texto logo após h2: hora (21h30), sala (Sala Principal / Pequeno Auditório)
      • preço, M/X, duração  em texto livre antes/após h2
      • parágrafos de sinopse antes de "Ficha técnica"
      • ficha técnica: padrão <strong>Campo</strong> Valor

Estratégia de categoria:
  - Recolher a raw_category da listagem (ex: "Música, Teatro")
  - Passar para normalize_category() — a filtragem é feita a jusante
  - NÃO rejeitar nenhum evento na fase de scraping

Campos adicionais no THEATER:
  - logo_url   → URL absoluto do logo PNG/SVG no wp-content
  - favicon_url→ /favicon.ico do domínio
  - facade_url → imagem da fachada em og:image da homepage
"""

import re
import time
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
# Constantes
# ─────────────────────────────────────────────────────────────

_BASE    = "https://theatrocirco.com"
_AGENDA  = f"{_BASE}/programa/"

# Meses em português → número
_PT_MONTHS = {
    "janeiro": 1,  "fevereiro": 2,  "março": 3,   "marco": 3,
    "abril": 4,    "maio": 5,       "junho": 6,    "julho": 7,
    "agosto": 8,   "setembro": 9,   "outubro": 10,
    "novembro": 11, "dezembro": 12,
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

# ─────────────────────────────────────────────────────────────
# Metadados do teatro  (lidos pelo sync_scrapers.py)
# NOTA: não executar pedidos HTTP aqui — este dict é avaliado
#       no momento do import pelo sync_scrapers.py e pelo orquestrador.
# ─────────────────────────────────────────────────────────────

THEATER = {
    "id":          "theatrocirco",
    "name":        "Theatro Circo",
    "short":       "TC",
    "color":       "#b71c1c",
    "city":        "Braga",
    "address":     "Av. da Liberdade, 697, 4710-251 Braga",
    "site":        "https://theatrocirco.com",
    "programacao": "https://theatrocirco.com/programa/",
    "logo_url":    "https://theatrocirco.com/wp-content/themes/theatrocirco/assets/img/logo.svg",
    "favicon_url": "https://theatrocirco.com/favicon.ico",
    "facade_url":  "https://theatrocirco.com/wp-content/uploads/2019/09/fachada.jpg",
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


# ─────────────────────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    if not can_scrape(_BASE):
        log(f"[{THEATER_NAME}] robots.txt: scraping bloqueado para {_BASE}")
        return []

    candidates = _collect_candidates()
    log(f"[{THEATER_NAME}] {len(candidates)} candidatos na listagem")

    events:   list[dict] = []
    seen_ids: set[str]   = set()

    for item in candidates:
        try:
            ev = _scrape_event(item)
            if ev:
                eid = ev["id"]
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(ev)
        except Exception as e:
            log(f"[{THEATER_NAME}] Erro em {item['url']}: {e}")
        time.sleep(0.4)

    log(f"[{THEATER_NAME}] {len(events)} eventos recolhidos")
    return events


# ─────────────────────────────────────────────────────────────
# Recolha de candidatos da listagem
# ─────────────────────────────────────────────────────────────

def _collect_candidates() -> list[dict]:
    """
    Percorre /programa/ e devolve todos os eventos encontrados.

    Estrutura HTML de cada evento:
      <div class="highlight-card">
        <p class="highlight-meta desktop">DD mês (dia) → Categoria</p>
        <div class="highlight-image">
          <a href="/event/slug/">
            <img ...>  <!-- OU <video> — NÃO tem texto -->
          </a>
        </div>
        <!-- Tags opcionais antes do título -->
        <a class="highlight-tag" href="...">Infantojuvenil</a>
        <a href="https://www.bol.pt/...">Bilhetes</a>
        <!-- meta repetido (mobile) -->
        <p class="highlight-meta">DD mês (dia) → Categoria</p>
        <h3><em>Título em itálico</em></h3>  OU  <h3>Título simples</h3>
        <h3>Subtítulo / Companhia</h3>
        <!-- Tags opcionais depois do título -->
        <a class="highlight-tag" href="...">Tag</a>
      </div>

    Bug corrigido: o scraper anterior iterava <a href=/event/> e chamava
    .get_text() — essas tags só contêm <img>/<video>, texto sempre vazio.
    A solução é iterar div.highlight-card e ler os elementos filhos.
    """
    try:
        r = requests.get(_AGENDA, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro na listagem: {e}")
        return []

    soup       = BeautifulSoup(r.text, "lxml")
    candidates = []
    seen_urls  = set()

    for card in soup.select("div.highlight-card"):

        # ── URL ────────────────────────────────────────────────
        a_img = card.select_one("div.highlight-image a[href]")
        if not a_img:
            continue
        href = a_img["href"]
        url  = href if href.startswith("http") else urljoin(_BASE, href)
        url  = url.rstrip("/") + "/"
        if url in seen_urls:
            continue
        seen_urls.add(url)

        # ── Categoria raw + datas_label da listagem ────────────
        # Preferir a versão .desktop para evitar texto duplicado
        meta_el = card.select_one("p.highlight-meta.desktop") or card.select_one("p.highlight-meta")
        meta_text = meta_el.get_text(" ", strip=True) if meta_el else ""

        # Formato: "27 março (sex) → Teatro" ou "1, 2, 3 e 6 junho (seg a sáb) → Música, Teatro"
        raw_category = ""
        dates_raw    = ""
        if "→" in meta_text:
            parts        = meta_text.split("→", 1)
            dates_raw    = parts[0].strip()
            raw_category = parts[1].strip()

        # ── Título e subtítulo ─────────────────────────────────
        h3_list = card.select("h3")
        title    = ""
        subtitle = ""
        if h3_list:
            title    = h3_list[0].get_text(strip=True)
        if len(h3_list) >= 2:
            subtitle = h3_list[1].get_text(strip=True)

        # ── Tags ───────────────────────────────────────────────
        tags = [a.get_text(strip=True) for a in card.select("a.highlight-tag")]

        # ── Ticket URL da listagem ─────────────────────────────
        ticket_url = ""
        for a in card.select("a[href]"):
            h = a["href"]
            if "bol.pt" in h or "ticketline.pt" in h:
                ticket_url = h
                break

        # ── Imagem thumbnail da listagem ───────────────────────
        # Guardamos para fallback — a imagem definitiva vem da página individual
        thumb_url = ""
        img_el = card.select_one("div.highlight-image img[src]")
        if img_el:
            src = img_el.get("src", "")
            thumb_url = src if src.startswith("http") else urljoin(_BASE, src)

        # ── Parsing de datas ───────────────────────────────────
        dates_label, date_start, date_end = _parse_date_text(dates_raw or meta_text)

        candidates.append({
            "url":          url,
            "title":        title,
            "subtitle":     subtitle,
            "raw_category": raw_category,
            "tags":         tags,
            "ticket_url":   ticket_url,
            "thumb_url":    thumb_url,
            "dates_label":  dates_label,
            "date_start":   date_start,
            "date_end":     date_end,
            "dates_raw":    dates_raw,
        })

    return candidates


# ─────────────────────────────────────────────────────────────
# Scraping de página de evento individual
# ─────────────────────────────────────────────────────────────

def _scrape_event(item: dict) -> dict | None:
    url = item["url"]
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    full_text = soup.get_text(" ", strip=True)

    # ── Título ─────────────────────────────────────────────────
    # Preferir o h1 da página; fallback: título da listagem
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title:
        title = item.get("title", "")
    if not title or len(title) < 2:
        return None

    # ── Subtítulo / companhia ──────────────────────────────────
    subtitle = item.get("subtitle", "")
    h2 = soup.find("h2")
    if h2:
        h2_text = h2.get_text(strip=True)
        # Aceitar só se for curto e diferente do título (evitar headings de secção)
        if h2_text and len(h2_text) < 150 and h2_text != title:
            subtitle = h2_text

    # ── Categoria ──────────────────────────────────────────────
    raw_cat = item.get("raw_category", "")
    # Tags da página individual (mais completas)
    page_tags = [a.get_text(strip=True) for a in soup.find_all("a", href=re.compile(r"/event_tag/"))]
    all_tags  = list({t for t in (item.get("tags", []) + page_tags) if t})

    # normalize_category recebe a raw string; a filtragem é feita a jusante
    category = normalize_category(raw_cat) if raw_cat else "Outro"

    # Override: se tag Infantojuvenil presente e categoria não é já Infanto-Juvenil
    if any("infantojuvenil" in t.lower() for t in all_tags) and category not in ("Infanto-Juvenil",):
        # Manter a categoria original — o harmonizer pode decidir a classificação final
        pass

    # ── Datas ──────────────────────────────────────────────────
    dates_label = item.get("dates_label", "")
    date_start  = item.get("date_start", "")
    date_end    = item.get("date_end", "")

    # Se não vieram da listagem, tentar extrair da página
    if not date_start:
        # Procurar padrões de data no texto da página
        for el in soup.find_all(string=re.compile(
            r"\d{1,2}\s+(?:janeiro|fevereiro|março|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)",
            re.IGNORECASE,
        )):
            dl, ds, de = _parse_date_text(el.strip())
            if ds:
                dates_label, date_start, date_end = dl, ds, de
                break

    if not date_start:
        log(f"[{THEATER_NAME}] Sem data: {url}")
        return None

    # ── Hora e sala ────────────────────────────────────────────
    # Texto logo após h2: "21h30\nSala Principal"
    schedule = ""
    sala     = ""

    hora_m = re.search(r"\b(\d{1,2}[hH]\d{2})\b", full_text)
    if hora_m:
        schedule = hora_m.group(1).lower().replace("h", "h")  # normalizar para "21h30"

    sala_m = re.search(
        r"(Sala\s+Principal|Pequeno\s+Audit[oó]rio|Grande\s+Audit[oó]rio|Foyer)",
        full_text, re.IGNORECASE,
    )
    if sala_m:
        sala = sala_m.group(1)

    # ── Preço ──────────────────────────────────────────────────
    price_info = ""
    price_min  = None
    price_max  = None

    preco_m = re.search(
        r"(Entrada\s+(?:livre|gratuita)|Gratuito|\d+(?:[,\.]\d+)?\s*€(?:\s*[-–]\s*\d+(?:[,\.]\d+)?\s*€)?)",
        full_text, re.IGNORECASE,
    )
    if preco_m:
        price_info = preco_m.group(1).strip()
        # Extrair valores numéricos
        nums = re.findall(r"\d+(?:[,\.]\d+)?", price_info)
        floats = [float(n.replace(",", ".")) for n in nums]
        if floats:
            price_min = min(floats)
            price_max = max(floats)

    # ── Classificação etária ────────────────────────────────────
    age_rating = ""
    age_min    = None
    age_m = re.search(r"\bM\s*/\s*(\d+)\b", full_text)
    if age_m:
        age_min    = int(age_m.group(1))
        age_rating = f"M/{age_min}"

    # ── Duração ────────────────────────────────────────────────
    duration     = ""
    duration_min = None
    dur_m = re.search(r"[Dd]ura[çc][aã]o\D{0,10}?(\d+)\s*minutos?", full_text)
    if not dur_m:
        dur_m = re.search(r"\b(\d{2,3})\s*min(?:utos?)?\b", full_text)
    if dur_m:
        duration_min = int(dur_m.group(1))
        duration     = f"{duration_min} min."

    # ── Imagem ─────────────────────────────────────────────────
    image   = None
    raw_img = ""

    # og:image é o mais fiável
    og = soup.find("meta", property="og:image")
    if og:
        raw_img = og.get("content", "").strip()

    # Fallback: thumbnail da listagem
    if not raw_img:
        raw_img = item.get("thumb_url", "")

    # Fallback: primeira imagem wp-content sem ser logo
    if not raw_img:
        for img in soup.find_all("img", src=re.compile(r"/wp-content/uploads/")):
            src = img.get("src", "")
            if src and "logo" not in src.lower() and len(src) > 40:
                raw_img = src if src.startswith("http") else urljoin(_BASE, src)
                break

    if raw_img and raw_img.startswith("http"):
        image = build_image_object(raw_img, soup, THEATER_NAME, url)

    # ── Ticket URL ─────────────────────────────────────────────
    ticket_url = item.get("ticket_url", "")
    if not ticket_url:
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if any(k in h for k in ("bol.pt", "ticketline.pt", "bilhete", "comprar")):
                ticket_url = h if h.startswith("http") else urljoin(_BASE, h)
                break

    # ── Sinopse ────────────────────────────────────────────────
    synopsis = _extract_synopsis(soup)

    # ── Ficha técnica ───────────────────────────────────────────
    technical_sheet = _parse_ficha(soup, full_text)

    # ── Acessibilidade ──────────────────────────────────────────
    accessibility = []
    if re.search(r"L[Gg][Pp]|L[íi]ngua\s+[Gg]estual", full_text):
        accessibility.append("LGP")
    if re.search(r"[Aa]udiodes[ck]ri[çc][aã]o", full_text):
        accessibility.append("Audiodescrição")

    # ── Sessions ───────────────────────────────────────────────
    sessions = build_sessions(date_start, date_end, schedule)

    # ── Montar evento ───────────────────────────────────────────
    ev = {
        "id":               make_id(SOURCE_SLUG, title),
        "title":            title,
        "theater":          THEATER_NAME,
        "source_url":       url,
        "date_start":       date_start,
        # Campos recomendados
        "category":         category,
        "dates_label":      dates_label,
        "sessions":         sessions,
        "image":            image,
        "synopsis":         truncate_synopsis(synopsis),
        "ticket_url":       ticket_url,
        "price_info":       price_info,
        "technical_sheet":  technical_sheet,
        "tags":             all_tags,
        "accessibility":    accessibility,
    }

    # Campos opcionais — só incluir se tiverem valor
    if subtitle:
        ev["subtitle"] = subtitle
    if date_end and date_end != date_start:
        ev["date_end"] = date_end
    if schedule:
        ev["schedule"] = schedule
    if sala:
        ev["sala"] = sala
    if duration:
        ev["duration"]     = duration
        ev["duration_min"] = duration_min
    if age_rating:
        ev["age_rating"] = age_rating
        ev["age_min"]    = age_min
    if price_min is not None:
        ev["price_min"] = price_min
    if price_max is not None:
        ev["price_max"] = price_max

    return ev


# ─────────────────────────────────────────────────────────────
# Parsing de datas
# ─────────────────────────────────────────────────────────────

def _parse_date_text(text: str) -> tuple[str, str, str]:
    """
    Converte texto de data para (dates_label, date_start, date_end).

    Formatos suportados:
      "27 março (sex)"                          → data única
      "15 e 16 maio (sex e sáb)"               → duas datas mesmo mês
      "1, 2, 3 e 6 junho (seg a sáb)"          → múltiplas datas mesmo mês
      "17 a 24 abril"                           → intervalo mesmo mês
      "12 janeiro a 18 abril"                   → intervalo meses distintos
      "Sessões de 7 a 9 abril (ter a qui)"      → prefixo "Sessões de"
      "2 junho a 31 outubro"                    → intervalo longo
    """
    if not text:
        return "", "", ""

    text = text.strip()
    # Remover prefixos comuns
    text = re.sub(r"^[Ss]ess[õo]es\s+de\s+", "", text)

    # ── "DD mês a DD mês [YYYY]" — intervalo meses distintos ──
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, mo1, d2, mo2, yr = m.groups()
        n1, n2 = _mon(mo1), _mon(mo2)
        if n1 and n2:
            y2 = int(yr) if yr else _infer_year(n2, int(d2))
            y1 = _infer_year(n1, int(d1)) if not yr else y2
            label = f"{d1} {mo1.capitalize()} a {d2} {mo2.capitalize()}"
            return (label,
                    f"{y1}-{n1:02d}-{int(d1):02d}",
                    f"{y2}-{n2:02d}-{int(d2):02d}")

    # ── "DD a DD mês [YYYY]" — intervalo mesmo mês ─────────────
    m = re.search(
        r"(\d{1,2})\s+[aà]\s+(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d1, d2, mon_s, yr = m.groups()
        n = _mon(mon_s)
        if n:
            y = int(yr) if yr else _infer_year(n, int(d2))
            label = f"{d1} a {d2} {mon_s.capitalize()}"
            return (label,
                    f"{y}-{n:02d}-{int(d1):02d}",
                    f"{y}-{n:02d}-{int(d2):02d}")

    # ── "DD, DD e DD mês" ou "DD e DD mês" — múltiplas datas ───
    m = re.search(
        r"((?:\d{1,2}[,\s]+)+(?:e\s+)?\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        days_str, mon_s, yr = m.groups()
        all_days = [int(d) for d in re.findall(r"\d{1,2}", days_str)]
        n = _mon(mon_s)
        if n and all_days:
            d1, d2 = min(all_days), max(all_days)
            y = int(yr) if yr else _infer_year(n, d2)
            label = f"{d1} e {d2} {mon_s.capitalize()}"
            return (label,
                    f"{y}-{n:02d}-{d1:02d}",
                    f"{y}-{n:02d}-{d2:02d}")

    # ── "DD mês [YYYY]" — data única ────────────────────────────
    m = re.search(
        r"(\d{1,2})\s+([a-záéíóúçã]{3,})(?:\s+(\d{4}))?",
        text, re.IGNORECASE,
    )
    if m:
        d, mon_s, yr = m.groups()
        n = _mon(mon_s)
        if n:
            y  = int(yr) if yr else _infer_year(n, int(d))
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
# Extracção de sinopse
# ─────────────────────────────────────────────────────────────

def _extract_synopsis(soup) -> str:
    """
    Sinopse: parágrafos substantivos antes da ficha técnica.
    Fallback: og:description se não genérico.
    """
    synopsis = ""

    # Encontrar elemento de início da ficha técnica
    ficha_start = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            ficha_start = el
            break

    if ficha_start:
        # Pegar todos os elementos antes da ficha técnica
        all_descendants = list(soup.descendants)
        try:
            fic_pos = all_descendants.index(ficha_start)
        except ValueError:
            fic_pos = len(all_descendants)
    else:
        fic_pos = None

    for p in soup.find_all("p"):
        t = p.get_text(strip=True)
        if len(t) < 40:
            continue
        # Ignorar boilerplate
        if re.match(
            r"^(Este site|Inscreva|Pretende receber|campos de preenchimento"
            r"|A reserva|Os seus dados|Ao submeter)",
            t, re.IGNORECASE,
        ):
            continue
        # Parar ao chegar à zona da ficha técnica
        if fic_pos is not None:
            try:
                p_pos = all_descendants.index(p)
                if p_pos > fic_pos:
                    break
            except ValueError:
                pass

        synopsis += (" " if synopsis else "") + t
        if len(synopsis) > 800:
            break

    # Fallback og:description
    if not synopsis:
        og = soup.find("meta", property="og:description")
        if og:
            desc = og.get("content", "").strip()
            # Rejeitar descrições genéricas do site
            if desc and "theatrocirco.com" not in desc.lower() and len(desc) > 40:
                synopsis = desc

    return synopsis


# ─────────────────────────────────────────────────────────────
# Parsing da ficha técnica
# ─────────────────────────────────────────────────────────────

def _parse_ficha(soup, full_text: str) -> dict:
    """
    Ficha técnica no Theatro Circo:
      <strong>Campo</strong> Valor  (tudo dentro do mesmo bloco de texto)
    Estratégia: localizar o bloco após "Ficha técnica" e extrair
    pares chave→valor por posição dos <strong>.
    """
    # Campos conhecidos e os seus padrões de label
    _KNOWN = [
        ("criação",        r"[Cc]ria[çc][aã]o(?:\s*,\s*texto\s*e\s*interpreta[çc][aã]o)?(?:\s*e\s*\w+)*"),
        ("texto",          r"[Tt]exto(?:\s+e\s+[Ee]ncena[çc][aã]o)?"),
        ("encenação",      r"[Ee]ncena[çc][aã]o"),
        ("direção",        r"[Dd]ire[çc][aã]o(?:\s+[Aa]rt[íi]stica)?"),
        ("dramaturgia",    r"[Aa]poio\s+[àa]\s+cria[çc][aã]o\s+e\s+dramaturgia|[Dd]ramaturgia"),
        ("interpretação",  r"[Ii]nterpreta[çc][aã]o"),
        ("coreografia",    r"[Cc]oreografia"),
        ("tradução",       r"[Tt]radu[çc][aã]o"),
        ("adaptação",      r"[Aa]dapta[çc][aã]o"),
        ("cenografia",     r"[Cc]enografia|[Ee]spa[çc]o\s+[Cc][ée]nico|[Dd]esenho\s+de\s+[Ee]spa[çc]o"),
        ("figurinos",      r"[Ff]igurinos?"),
        ("luz",            r"[Dd]esenho\s+de\s+[Ll]uz|[Ii]lumina[çc][aã]o"),
        ("som",            r"[Ss]onoplastia|[Pp]rodu[çc][aã]o\s+[Mm]usical\s+e\s+[Ss]onoplastia|[Dd]esenho\s+[Ss]onoro|[Dd]esenho\s+de\s+[Ss]om"),
        ("música",         r"[Mm][úu]sica\s+[Oo]riginal|[Mm][úu]sica"),
        ("produção",       r"[Pp]rodu[çc][aã]o\s+[Ee]xecutiva|[Pp]rodu[çc][aã]o"),
        ("coprodução",     r"[Cc]o-?[Pp]rodu[çc][aã]o"),
        ("fotografia",     r"[Ff]otografia(?:\s+e\s+[Dd]esign\s+[Gg]ráfico)?"),
        ("design",         r"[Dd]esign\s+[Gg]ráfico"),
    ]

    ficha = {}

    # Tentar localizar o bloco da ficha técnica
    ficha_el = None
    for el in soup.find_all(["strong", "b"]):
        if "ficha técnica" in el.get_text(strip=True).lower():
            # Subir ao container que inclui toda a ficha
            ficha_el = el.find_parent(["div", "section", "article", "p"]) or el.find_parent()
            break

    text = ficha_el.get_text(" ", strip=True) if ficha_el else full_text

    # Encontrar posições de cada label no texto
    positions = []
    for key, pattern in _KNOWN:
        for m in re.finditer(pattern, text):
            positions.append((m.start(), m.end(), key))
    positions.sort()

    # Extrair valor entre cada label e o seguinte
    for i, (start, end, key) in enumerate(positions):
        next_start = positions[i + 1][0] if i + 1 < len(positions) else end + 300
        value      = re.sub(r"\s+", " ", text[end:next_start]).strip()
        # Cortar antes de palavras de paragem
        value = re.split(
            r"\s+(?:Apoio|Agradecimentos|Duração|©|Parcerias|Residências|Agradecimento)",
            value, flags=re.IGNORECASE,
        )[0]
        value = value[:200].strip()
        # Remover pontuação inicial
        value = re.sub(r"^[\s:,;]+", "", value).strip()
        if value and key not in ficha:
            ficha[key] = value

    return ficha
