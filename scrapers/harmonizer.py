"""
scrapers/harmonizer.py
Primeira Plateia — Camada de harmonização de dados. v2.0

Corre APÓS deduplicação e ANTES de validação:
    deduplicate() → harmonize() → validate()

Responsabilidades v2.0:
    - Limpeza de texto (HTML residual, espaços, encoding)
    - Normalização de nomes de espaços culturais via registry
    - Normalização de categorias e subcategorias via vocabulário controlado
    - Inferência de subcategoria a partir do valor raw do scraper
    - Derivação automática de is_free a partir de price_info
    - Inferência de for_families a partir de age_min e texto
    - Inferência de is_festival a partir do título/subtitle
    - Sanitização de age_min (eliminar valores impossíveis)
    - Geração de ID automático quando ausente
    - Normalização de age_rating para formato canónico
    - Garantia de campos de metadados (scraped_at, schema_version)

Princípio: produz NOVOS dicts — nunca modifica os eventos in-place.
"""

import html
import logging
import re
from datetime import datetime, timezone

from scrapers.schema import (
    RECOMMENDED_FIELDS,
    SCHEMA_VERSION,
    generate_id,
    normalize_category,
    normalize_subcategory,
    infer_subcategory_from_raw,
    get_category_for_subcategory,
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
# Padrões de detecção (compilados uma vez)
# ─────────────────────────────────────────────────────────────

_FREE_RE = re.compile(
    r"entr[aã]da\s*livre|gratuito|grat[uú]ita|free\b|sem\s+custo|"
    r"\b0\s*[€e]\b|entrada\s+grat",
    re.IGNORECASE,
)
_FESTIVAL_RE = re.compile(r"\bfestival\b", re.IGNORECASE)
_FAMILIES_RE = re.compile(
    r"famíli[as]|para\s+crian[cç]as|para\s+toda\s+a\s+famíli|"
    r"espet[áa]culo\s+familiar|público\s+familiar",
    re.IGNORECASE,
)
_SCHOOLS_RE = re.compile(
    r"escola[s]?\b|programa\s+escolar|p[úu]blico\s+escolar|"
    r"sessão\s+escolar|visita\s+de\s+estudo",
    re.IGNORECASE,
)
_LSP_RE = re.compile(
    r"l[íi]ngua\s+gestual|LSP\b|intérprete\s+de\s+gestual|"
    r"sess[ãa]o\s+LSP|sess[ãa]o.*gestual",
    re.IGNORECASE,
)
_AGE_RATING_CLEAN_RE = re.compile(r"^M/0*(\d+)$", re.IGNORECASE)
_AGE_MIN_VALID = range(0, 22)  # 0–21, qualquer valor fora é lixo de parsing


# ─────────────────────────────────────────────────────────────
# Limpeza de texto
# ─────────────────────────────────────────────────────────────

def clean_text(text: str | None, max_chars: int | None = None) -> str:
    """
    Higienização completa de texto livre.
    1. Decode HTML entities
    2. Remove tags HTML residuais
    3. Normaliza espaços e saltos de linha
    4. Strip
    5. Trunca se max_chars fornecido
    """
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" *\n *", "\n", text)
    text = text.strip()
    if max_chars and len(text) > max_chars:
        from scrapers.utils import truncate_synopsis
        text = truncate_synopsis(text, max_chars)
    return text


def clean_title(text: str | None) -> str:
    """
    Limpeza de título.
    Capitaliza só a primeira letra se vier todo em minúsculas.
    Nunca usa title() — parte acrónimos e nomes próprios.
    """
    if not text:
        return ""
    text = clean_text(text)
    if not text:
        return ""
    if text == text.lower():
        text = text[0].upper() + text[1:]
    return text


# ─────────────────────────────────────────────────────────────
# Normalização de age_rating
# ─────────────────────────────────────────────────────────────

def _normalize_age_rating(raw: str | None) -> tuple[str, int | None]:
    """
    Normaliza age_rating para formato canónico e devolve (rating_str, age_min_int).
    Elimina valores impossíveis (+351, M/1770, M/120, etc.).
    Devolve ("", None) se inválido.
    """
    if not raw:
        return "", None

    text = raw.strip()

    # "Livre" → age_min = 0
    if re.match(r"^livre$", text, re.IGNORECASE):
        return "Livre", 0

    # M/NN ou M/0N
    m = re.match(r"^[Mm]/?(\d+)$", text)
    if m:
        val = int(m.group(1))
        if val in _AGE_MIN_VALID:
            return f"M/{val}", val
        # Valor fora do intervalo → lixo de parsing, descartar
        logger.debug(f"harmonizer: age_rating inválido descartado: {text!r} (val={val})")
        return "", None

    # "A classificar pela CCE" e variantes → manter como string, sem age_min
    if re.match(r"^A classificar", text, re.IGNORECASE):
        return text, None

    # Qualquer outra string não reconhecida com menos de 30 chars — manter
    if len(text) < 30:
        return text, None

    return "", None


# ─────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────

def harmonize(events: list[dict]) -> list[dict]:
    """
    Recebe lista de eventos (já deduplicados) e devolve nova lista harmonizada.
    Nunca modifica os dicts originais. Nunca rejeita eventos.
    """
    registry = _get_registry()
    now_utc = datetime.now(timezone.utc).isoformat()
    harmonized = []

    for ev in events:
        try:
            h = _harmonize_event(ev, registry, now_utc)
            harmonized.append(h)
        except Exception as e:
            logger.error(
                f"harmonizer: erro inesperado no evento "
                f"'{ev.get('id', '?')}' / '{ev.get('title', '?')}': {e}",
                exc_info=True,
            )
            harmonized.append(ev)

    # Sumário de campos recomendados em falta (agrupado)
    missing_counts: dict[str, int] = {f: 0 for f in RECOMMENDED_FIELDS}
    for h in harmonized:
        for field in RECOMMENDED_FIELDS:
            if not h.get(field):
                missing_counts[field] += 1

    for field, count in missing_counts.items():
        if count > 0:
            pct = round(count / len(harmonized) * 100) if harmonized else 0
            logger.warning(
                f"harmonizer: '{field}' ausente em {count}/{len(harmonized)} eventos ({pct}%)"
            )

    # Sumário de filtros booleanos inferidos
    free_count = sum(1 for h in harmonized if h.get("is_free") is True)
    families_count = sum(1 for h in harmonized if h.get("for_families") is True)
    festival_count = sum(1 for h in harmonized if h.get("is_festival") is True)
    schools_count = sum(1 for h in harmonized if h.get("for_schools") is True)
    lsp_count = sum(1 for h in harmonized if h.get("has_lsp") is True)
    logger.info(
        f"harmonizer: {len(harmonized)} eventos harmonizados | "
        f"is_free={free_count} for_families={families_count} "
        f"is_festival={festival_count} for_schools={schools_count} has_lsp={lsp_count}"
    )
    return harmonized


def _harmonize_event(ev: dict, registry: dict[str, str], now_utc: str) -> dict:
    """Harmoniza um único evento. Devolve novo dict."""
    h = dict(ev)

    # ── Schema version ────────────────────────────────────────
    h["schema_version"] = SCHEMA_VERSION

    # ── Título ────────────────────────────────────────────────
    h["title"] = clean_title(h.get("title"))

    # ── Espaço cultural ───────────────────────────────────────
    raw_theater = h.get("theater", "")
    h["theater"] = get_canonical_name(raw_theater, registry)
    if h["theater"] != raw_theater and raw_theater:
        logger.debug(f"harmonizer: espaço '{raw_theater}' → '{h['theater']}'")

    # ── Sinopse ───────────────────────────────────────────────
    if h.get("synopsis"):
        h["synopsis"] = clean_text(h["synopsis"], max_chars=300)
    elif h.get("description"):
        h["synopsis"] = clean_text(h.pop("description"), max_chars=300)

    # ── Categorias: guardar raw antes de normalizar ───────────
    raw_category = h.get("category", "")

    # ── Subcategoria: inferir do raw_category se não existir ──
    # (antes de normalizar category, enquanto ainda temos o valor raw do scraper)
    if not h.get("subcategory"):
        inferred_sub = infer_subcategory_from_raw(raw_category)
        if inferred_sub:
            h["subcategory"] = inferred_sub
            # Se a subcategoria tem uma categoria pai definida, usar essa
            parent_cat = get_category_for_subcategory(inferred_sub)
            if parent_cat:
                h["category"] = parent_cat
            else:
                h["category"] = normalize_category(raw_category)
        else:
            h["category"] = normalize_category(raw_category)
    else:
        # subcategory já fornecida pelo scraper — normalizar
        h["subcategory"] = normalize_subcategory(h["subcategory"]) or h["subcategory"]
        # Garantir que category é consistente com subcategory
        parent_cat = get_category_for_subcategory(h["subcategory"])
        if parent_cat and not h.get("category"):
            h["category"] = parent_cat
        elif not h.get("category"):
            h["category"] = normalize_category(raw_category)

    # ── Fallback de categoria ─────────────────────────────────
    if not h.get("category"):
        h["category"] = "Multidisciplinar"

    # ── Imagem ────────────────────────────────────────────────
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
    if not h.get("price_info") and h.get("price"):
        h["price_info"] = h.pop("price")
    elif "price" in h:
        del h["price"]

    # ── Campos redundantes — remover ─────────────────────────
    h.pop("synopsis_short", None)

    # ── source_url — normalizar campo ─────────────────────────
    for alt_field in ("url", "link", "href"):
        if not h.get("source_url") and h.get(alt_field):
            h["source_url"] = h.pop(alt_field)
            break

    # ── age_rating — sanitizar e normalizar ──────────────────
    raw_age = h.get("age_rating", "")
    if raw_age:
        clean_rating, clean_age_min = _normalize_age_rating(raw_age)
        h["age_rating"] = clean_rating
        # Só sobrescrever age_min se o valor actual for suspeito
        current_age_min = h.get("age_min")
        if current_age_min is not None and current_age_min not in _AGE_MIN_VALID:
            h["age_min"] = clean_age_min
            logger.debug(
                f"harmonizer: age_min inválido corrigido {current_age_min} → {clean_age_min} "
                f"para '{h.get('title', '?')}'"
            )
        elif current_age_min is None and clean_age_min is not None:
            h["age_min"] = clean_age_min

    # Sanitização extra: age_min fora do intervalo válido (de scrapers que não passam age_rating)
    if h.get("age_min") is not None and h["age_min"] not in _AGE_MIN_VALID:
        logger.debug(
            f"harmonizer: age_min={h['age_min']} impossível — descartado "
            f"para '{h.get('title', '?')}'"
        )
        h["age_min"] = None

    # ── Filtros booleanos: is_free ────────────────────────────
    price_info = h.get("price_info", "")
    if h.get("is_free") is None:
        if price_info and _FREE_RE.search(price_info):
            h["is_free"] = True
        else:
            h["is_free"] = None  # desconhecido, não false

    # ── Filtros booleanos: is_festival ────────────────────────
    if h.get("is_festival") is None:
        title_text = (h.get("title", "") + " " + h.get("subtitle", "")).strip()
        if _FESTIVAL_RE.search(title_text):
            h["is_festival"] = True

    # ── Filtros booleanos: for_families ───────────────────────
    if h.get("for_families") is None:
        age_min = h.get("age_min")
        synopsis_sub = (h.get("synopsis", "") + " " + h.get("subtitle", "")).strip()
        # age_min <= 6 é forte indicador de público infantil/familiar
        if (age_min is not None and age_min <= 6) or _FAMILIES_RE.search(synopsis_sub):
            h["for_families"] = True
        elif h.get("category") == "Infanto-Juvenil":
            h["for_families"] = True

    # ── Filtros booleanos: for_schools ────────────────────────
    if h.get("for_schools") is None:
        all_text = " ".join(filter(None, [
            h.get("synopsis", ""), h.get("subtitle", ""),
            h.get("title", ""), h.get("schedule", ""),
        ]))
        if _SCHOOLS_RE.search(all_text):
            h["for_schools"] = True

    # ── Filtros booleanos: has_lsp ────────────────────────────
    if h.get("has_lsp") is None:
        all_text = " ".join(filter(None, [
            h.get("synopsis", ""), h.get("subtitle", ""),
            h.get("schedule", ""), h.get("price_info", ""),
        ]))
        if _LSP_RE.search(all_text):
            h["has_lsp"] = True

    # ── Filtros booleanos: is_accessible ─────────────────────
    # Manter None — requer informação explícita do site do espaço
    if "is_accessible" not in h:
        h["is_accessible"] = None

    # ── Metadados ─────────────────────────────────────────────
    if not h.get("scraped_at"):
        h["scraped_at"] = now_utc

    # ── Limpar campos de texto auxiliares ─────────────────────
    for field in ("price_info", "age_rating", "duration", "sala", "schedule"):
        if h.get(field):
            h[field] = clean_text(h[field])

    return h
