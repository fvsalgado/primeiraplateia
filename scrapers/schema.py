"""
scrapers/schema.py
Primeira Plateia — Schema formal do evento.

Define o contrato de dados de cada evento no pipeline.
Usado pelo harmonizer (para produzir) e pelo validator (para verificar).

Campos obrigatórios (rejeitar se ausentes):
    id, title, theater, date_start, source_url

Campos recomendados (aviso se ausentes, nunca rejeitar):
    date_end, synopsis, category, image, ticket_url, price_info

Campos de metadados (preenchidos pelo orquestrador, não pelos scrapers):
    scraped_at
"""

import hashlib
import re
from typing import TypedDict, Required


# ─────────────────────────────────────────────────────────────
# Schema formal
# ─────────────────────────────────────────────────────────────

class ImageObject(TypedDict, total=False):
    url:     Required[str]   # URL da imagem (hotlink para o site do teatro)
    credit:  str | None      # Crédito fotográfico extraído da página (pode ser None)
    source:  Required[str]   # URL da página de origem do evento
    theater: Required[str]   # Nome canónico do teatro (para atribuição mínima)


class EventSchema(TypedDict, total=False):
    # ── Obrigatórios ──────────────────────────────────────────
    id:           Required[str]   # Identificador único. Gerado por SHA1 se ausente.
    title:        Required[str]   # Título limpo do espetáculo
    theater:      Required[str]   # Nome canónico (do theater_registry)
    date_start:   Required[str]   # ISO 8601 date: "YYYY-MM-DD"
    source_url:   Required[str]   # URL canónico do evento no site do teatro

    # ── Recomendados ──────────────────────────────────────────
    date_end:     str             # ISO 8601 date: "YYYY-MM-DD"
    synopsis:     str             # Excerto truncado, já limpo (max 300 chars)
    category:     str             # Do vocabulário controlado (ver CATEGORY_MAP)
    image:        ImageObject     # Objecto com url, credit, source, theater
    ticket_url:   str             # URL directo para compra de bilhetes
    price_info:   str             # Ex: "€10 – €18" ou "Entrada livre"

    # ── Metadados (preenchidos pelo orquestrador) ─────────────
    scraped_at:   str             # ISO 8601 datetime UTC: "2026-03-18T14:30:00Z"


# ─────────────────────────────────────────────────────────────
# Vocabulário controlado de categorias
# ─────────────────────────────────────────────────────────────

CATEGORY_MAP: dict[str, list[str]] = {
    "Teatro": [
        "teatro", "theatre", "peça", "peça de teatro", "teatro de texto",
        "teatro contemporâneo", "teatro clássico", "dramaturgia",
    ],
    "Dança": [
        "dança", "dance", "bailado", "ballet", "performance de dança",
        "dança contemporânea", "dança clássica",
    ],
    "Ópera": [
        "ópera", "opera", "lírica", "teatro lírico", "ópera contemporânea",
    ],
    "Teatro Musical": [
        "musical", "teatro musical", "musicado", "comédia musical",
    ],
    "Circo": [
        "circo", "novo circo", "circo contemporâneo", "acrobacia",
        "artes circenses",
    ],
    "Infanto-Juvenil": [
        "infantil", "para famílias", "para crianças", "kids", "teatro infantil",
        "teatro para a infância", "família",
    ],
    "Performance": [
        "performance", "performance art", "instalação performativa",
        "arte performativa", "artes performativas",
    ],
    "Música": [
        "música", "music", "concerto", "recital", "música ao vivo",
    ],
    "Outro": [],  # fallback explícito
}

# Lookup invertido: variante → categoria canónica (construído uma vez)
_CATEGORY_LOOKUP: dict[str, str] = {}
for _canonical, _variants in CATEGORY_MAP.items():
    _CATEGORY_LOOKUP[_canonical.lower()] = _canonical
    for _v in _variants:
        _CATEGORY_LOOKUP[_v.lower()] = _canonical


# ─────────────────────────────────────────────────────────────
# Geração de ID
# ─────────────────────────────────────────────────────────────

def generate_id(theater: str, title: str, date_start: str) -> str:
    """
    Gera um ID determinístico por SHA1 a partir de teatro + título + data.
    Formato: "auto-{8 hex chars}"
    Usado quando o scraper não fornece ID.

    Determinístico: o mesmo evento produz sempre o mesmo ID,
    mesmo em runs diferentes — evita duplicados no histórico do Git.
    """
    raw = f"{theater.lower().strip()}|{title.lower().strip()}|{date_start.strip()}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"auto-{digest}"


def normalize_category(raw: str | None) -> str:
    """
    Devolve a categoria canónica para um valor raw.
    Fallback: "Outro".
    """
    if not raw:
        return "Outro"
    key = raw.strip().lower()
    return _CATEGORY_LOOKUP.get(key, "Outro")


# ─────────────────────────────────────────────────────────────
# Campos obrigatórios — usados pelo validator
# ─────────────────────────────────────────────────────────────

REQUIRED_FIELDS: tuple[str, ...] = ("id", "title", "theater", "date_start", "source_url")
RECOMMENDED_FIELDS: tuple[str, ...] = ("date_end", "synopsis", "category", "image", "ticket_url")
