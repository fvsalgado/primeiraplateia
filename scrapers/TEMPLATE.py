"""
scrapers/TEMPLATE.py
Primeira Plateia — Template para novos scrapers.

NÃO copiar e editar manualmente — usar o script de scaffolding:
    python scripts/new_scraper.py

Este ficheiro serve como referência para a estrutura obrigatória.
Cada secção marcada com "TODO" requer ajuste ao site do teatro.

Contrato mínimo que o orquestrador (scraper.py) espera:
    - Um dict THEATER no nível do módulo
    - Uma função def scrape() -> list[dict]

Campos obrigatórios em cada evento devolvido por scrape():
    id, title, theater, date_start, source_url

Campos recomendados (melhoram o completeness score):
    date_end, synopsis, image, ticket_url, price_info,
    category, subcategory, sessions
"""

import logging
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    HEADERS,             # User-Agent e Accept-Language padrão
    parse_date,          # "15 março 2025" → "2025-03-15"
    parse_date_range,    # "15 — 30 março" → ("2025-03-15", "2025-03-30")
    truncate_synopsis,   # corta sinopse no máximo de chars sem quebrar frase
    build_sessions,      # gera sessions[] a partir de datas + texto de horário
    build_image_object,  # cria dict {url, credit, source, theater}
    fetch_with_retry,    # GET com retry exponencial + rate limiting por domínio
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# THEATER dict — OBRIGATÓRIO
# Lido pelo sync_scrapers.py para registo automático.
# Todos os campos são usados para preencher theaters.json.
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":      "nome_teatro",          # slug único, sem espaços (ex: "tagv", "ccb")
    "name":    "Nome Completo do Teatro",
    "short":   "NomeBreve",            # para logs e UI
    "url":     "https://www.teatro.pt",
    "city":    "Lisboa",
    "address": "Rua Exemplo, 1, 1000-000 Lisboa",
    "lat":     38.7169,                # latitude (float)
    "lng":     -9.1395,                # longitude (float)
    "type":    "teatro",               # "teatro" | "centro_cultural" | "cine_teatro"
    "aliases": ["Teatro Exemplo", "Exemplo"],  # nomes alternativos para normalização
}

BASE_URL = THEATER["url"]

# URL da página de programação — pode ser diferente do BASE_URL
# TODO: ajustar ao site concreto
LIST_URL = f"{BASE_URL}/programacao"


# ─────────────────────────────────────────────────────────────
# Extracção de evento individual
# ─────────────────────────────────────────────────────────────

def _parse_event(item: BeautifulSoup, session: requests.Session) -> dict | None:
    """
    Extrai dados de um evento a partir de um elemento BeautifulSoup.

    Parâmetros:
        item:    elemento BS4 que representa um evento na listagem
        session: sessão HTTP partilhada (para pedidos de detalhe se necessário)

    Devolve:
        dict com campos do evento, ou None se os dados mínimos não estiverem presentes.

    Nota: não lançar excepções — retornar None em caso de erro.
    """
    try:
        # ── Título ────────────────────────────────────────────
        # TODO: ajustar seletor CSS ao HTML do site
        title_tag = item.select_one(".event-title, h2, h3")
        if not title_tag:
            return None
        title = title_tag.get_text(strip=True)
        if not title or len(title) < 2:
            return None

        # ── URL de detalhe ────────────────────────────────────
        # TODO: ajustar seletor
        link_tag = item.select_one("a[href]")
        source_url = ""
        if link_tag:
            href = link_tag.get("href", "")
            source_url = href if href.startswith("http") else BASE_URL.rstrip("/") + "/" + href.lstrip("/")
        if not source_url:
            source_url = LIST_URL

        # ── Datas ─────────────────────────────────────────────
        # TODO: ajustar seletor e formato de data
        date_tag = item.select_one(".event-date, .date, time")
        date_text = date_tag.get_text(strip=True) if date_tag else ""
        date_start, date_end = parse_date_range(date_text)
        if not date_start:
            logger.debug(f"[{THEATER['name']}] sem data para: {title!r}")
            return None

        # ── Imagem ────────────────────────────────────────────
        # TODO: ajustar seletor de imagem
        img_tag = item.select_one("img")
        img_url = ""
        if img_tag:
            img_url = (
                img_tag.get("src")
                or img_tag.get("data-src")
                or img_tag.get("data-lazy-src")
                or ""
            )
        # Resolver URLs relativas
        if img_url and not img_url.startswith("http"):
            img_url = BASE_URL.rstrip("/") + "/" + img_url.lstrip("/")
        image = build_image_object(img_url, None, THEATER["name"], source_url)

        # ── Sinopse ───────────────────────────────────────────
        # TODO: ajustar seletor
        synopsis_tag = item.select_one(".synopsis, .description, .intro, p")
        synopsis = ""
        if synopsis_tag:
            synopsis = truncate_synopsis(synopsis_tag.get_text(strip=True))

        # ── Preço ─────────────────────────────────────────────
        # TODO: ajustar seletor
        price_tag = item.select_one(".price, .ticket-price, .preco")
        price_info = price_tag.get_text(strip=True) if price_tag else ""

        # ── Categoria ─────────────────────────────────────────
        # TODO: extrair categoria do HTML ou definir uma padrão para este teatro
        category = ""

        # ── Sessões ───────────────────────────────────────────
        # build_sessions usa date_start + date_end + texto de horário
        # TODO: extrair texto de horário se disponível (ex: "Qua a Sex 21h00")
        schedule_text = ""
        sessions = build_sessions(date_start, date_end, schedule_text)

        return {
            "title":      title,
            "theater":    THEATER["name"],
            "date_start": date_start,
            "date_end":   date_end,
            "synopsis":   synopsis,
            "image":      image,
            "source_url": source_url,
            "ticket_url": source_url,   # TODO: separar URL de bilheteira se disponível
            "price_info": price_info,
            "category":   category,
            "sessions":   sessions,
        }

    except Exception as exc:
        logger.warning(f"[{THEATER['name']}] erro ao analisar evento: {exc}")
        return None


# ─────────────────────────────────────────────────────────────
# Ponto de entrada — OBRIGATÓRIO
# ─────────────────────────────────────────────────────────────

def scrape() -> list[dict]:
    """
    Recolhe e devolve todos os eventos futuros do teatro.

    Convenções:
      - Nunca lançar excepções para o orquestrador — tratar internamente.
      - Devolver lista vazia se nada for encontrado (o orquestrador usa cache).
      - Usar fetch_with_retry em vez de session.get() directamente.
      - Registar progresso com logger.info() para visibilidade no log.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    events: list[dict] = []

    try:
        # ── Obter listagem ────────────────────────────────────
        resp = fetch_with_retry(session, LIST_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # TODO: ajustar seletor para os itens de evento na página de listagem
        items = soup.select(".event-item, article.event, .programacao-item")
        logger.info(f"[{THEATER['name']}] {len(items)} candidatos na listagem")

        # ── Processar cada evento ─────────────────────────────
        for item in items:
            ev = _parse_event(item, session)
            if ev:
                events.append(ev)

    except Exception as exc:
        logger.error(f"[{THEATER['name']}] erro na listagem ({LIST_URL}): {exc}")

    logger.info(f"[{THEATER['name']}] {len(events)} eventos recolhidos")
    return events
