"""
scrapers/harmonizer.py
Primeira Plateia — Camada de harmonização de dados.

Corre APÓS deduplicação e ANTES de validação no pipeline principal:
    deduplicate() → harmonize() → validate()

Responsabilidades:
    - Limpeza de texto (HTML residual, espaços, encoding)
    - Normalização de nomes de teatros via registry
    - Normalização de categorias via vocabulário controlado
    - Geração de ID automático quando ausente
    - Garantia de campos de metadados (scraped_at)

Princípio: produz NOVOS dicts — nunca modifica os eventos in-place.
O evento original fica preservado para debug no validation_report.
"""

import html
import logging
import re
from datetime import datetime, timezone

from scrapers.schema import (
    CATEGORY_MAP,
    RECOMMENDED_FIELDS,
    generate_id,
    normalize_category,
)
from scrapers.theater_registry import build_theater_registry, get_canonical_name

logger = logging.getLogger(__name__)

# Registry carregado uma vez por run
_registry: dict[str, str] | None = None


def _get_registry() -> dict[str, str]:
    global _registry
    if _registry is None:
        _registry = build_theater_registry()
    return _registry


# ─────────────────────────────────────────────────────────────
# Funções de limpeza de texto
# ─────────────────────────────────────────────────────────────

def clean_text(text: str | None, max_chars: int | None = None) -> str:
    """
    Higienização completa de texto livre (sinopses, descrições, etc.):
    1. Decode HTML entities (&amp; → &, &nbsp; → espaço, etc.)
    2. Remove tags HTML residuais
    3. Normaliza espaços e saltos de linha
    4. Strip
    5. Trunca se max_chars fornecido (via truncate_synopsis de utils)
    """
    if not text:
        return ""
    # 1. HTML entities
    text = html.unescape(text)
    # 2. Tags HTML residuais
    text = re.sub(r"<[^>]+>", " ", text)
    # 3a. Normalizar espaços horizontais (tabs, múltiplos espaços)
    text = re.sub(r"[ \t]+", " ", text)
    # 3b. Normalizar saltos de linha (máx 2 consecutivos)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 3c. Espaços antes/depois de saltos de linha
    text = re.sub(r" *\n *", "\n", text)
    # 4. Strip
    text = text.strip()
    # 5. Truncar se pedido (importar aqui para evitar circular import)
    if max_chars and len(text) > max_chars:
        from scrapers.utils import truncate_synopsis
        text = truncate_synopsis(text, max_chars)
    return text


def clean_title(text: str | None) -> str:
    """
    Limpeza de título de espetáculo.
    - Aplica clean_text() base
    - Capitaliza só a primeira letra se o título vier todo em minúsculas
    - Nunca altera títulos com maiúsculas intermédias (ex: "CCB", "RTP")
    - Nunca usa title() — parte acrónimos e nomes próprios
    """
    if not text:
        return ""
    text = clean_text(text)
    if not text:
        return ""
    # Só capitalizar a primeira letra se o título vier todo em minúsculas
    # Títulos todo em maiúsculas são mantidos (podem ser estilísticos)
    if text == text.lower():
        text = text[0].upper() + text[1:]
    return text


# ─────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────

def harmonize(events: list[dict]) -> list[dict]:
    """
    Recebe lista de eventos (já deduplicados) e devolve nova lista
    com todos os campos harmonizados.

    Nunca modifica os dicts originais — produz novos dicts.
    Nunca rejeita eventos — harmoniza o que consegue e regista warnings.
    Rejeição é responsabilidade do validator.
    """
    registry = _get_registry()
    now_utc = datetime.now(timezone.utc).isoformat()
    harmonized = []
    warned = 0

    for ev in events:
        try:
            h = _harmonize_event(ev, registry, now_utc)
            harmonized.append(h)
        except Exception as e:
            # Nunca deixar um erro no harmonizer derrubar o pipeline
            logger.error(
                f"harmonizer: erro inesperado no evento "
                f"'{ev.get('id', '?')}' / '{ev.get('title', '?')}': {e}",
                exc_info=True,
            )
            # Incluir o evento original para não perder dados
            harmonized.append(ev)

    # Avisar sobre campos recomendados em falta (agrupado, não por evento)
    missing_counts: dict[str, int] = {f: 0 for f in RECOMMENDED_FIELDS}
    for h in harmonized:
        for field in RECOMMENDED_FIELDS:
            if not h.get(field):
                missing_counts[field] += 1

    for field, count in missing_counts.items():
        if count > 0:
            pct = round(count / len(harmonized) * 100) if harmonized else 0
            logger.warning(
                f"harmonizer: campo recomendado '{field}' ausente "
                f"em {count}/{len(harmonized)} eventos ({pct}%)"
            )

    logger.info(f"harmonizer: {len(harmonized)} eventos harmonizados")
    return harmonized


def _harmonize_event(ev: dict, registry: dict[str, str], now_utc: str) -> dict:
    """
    Harmoniza um único evento. Devolve novo dict.
    """
    h = dict(ev)  # cópia rasa — suficiente pois os valores são todos escalares ou dicts simples

    # ── Título ────────────────────────────────────────────────
    h["title"] = clean_title(h.get("title"))

    # ── Teatro ────────────────────────────────────────────────
    raw_theater = h.get("theater", "")
    h["theater"] = get_canonical_name(raw_theater, registry)
    if h["theater"] != raw_theater and raw_theater:
        logger.debug(f"harmonizer: teatro '{raw_theater}' → '{h['theater']}'")

    # ── Sinopse ───────────────────────────────────────────────
    if h.get("synopsis"):
        h["synopsis"] = clean_text(h["synopsis"], max_chars=300)
    elif h.get("description"):
        # Alguns scrapers usam "description" em vez de "synopsis"
        h["synopsis"] = clean_text(h.pop("description"), max_chars=300)

    # ── Categoria ─────────────────────────────────────────────
    h["category"] = normalize_category(h.get("category"))

    # ── Imagem ────────────────────────────────────────────────
    # Se image for ainda uma string (scraper antigo), converter para objecto mínimo
    if isinstance(h.get("image"), str):
        url = h["image"]
        if url and url.startswith("http"):
            h["image"] = {
                "url": url,
                "credit": None,
                "source": h.get("source_url", ""),
                "theater": h.get("theater", ""),
            }
        else:
            h["image"] = None

    # ── ID automático ─────────────────────────────────────────
    if not h.get("id"):
        h["id"] = generate_id(
            h.get("theater", ""),
            h.get("title", ""),
            h.get("date_start", ""),
        )
        logger.debug(f"harmonizer: ID gerado para '{h['title']}': {h['id']}")

    # ── Preço — normalizar nome do campo ──────────────────────
    # Scrapers legados usam "price"; schema define "price_info"
    if not h.get("price_info") and h.get("price"):
        h["price_info"] = h.pop("price")
    elif "price" in h:
        del h["price"]

    # ── synopsis_short — campo redundante, remover ────────────
    h.pop("synopsis_short", None)

    # ── source_url — normalizar campo ─────────────────────────
    for alt_field in ("url", "link", "href"):
        if not h.get("source_url") and h.get(alt_field):
            h["source_url"] = h.pop(alt_field)
            break

    # ── Metadados ─────────────────────────────────────────────
    if not h.get("scraped_at"):
        h["scraped_at"] = now_utc

    # ── Limpar campos de texto auxiliares ─────────────────────
    for field in ("price_info", "age_rating", "duration", "sala", "schedule"):
        if h.get(field):
            h[field] = clean_text(h[field])

    return h
