"""
scrapers/schema.py
Primeira Plateia — Schema formal do evento.
v2.0 — Agenda Cultural

Define o contrato de dados de cada evento no pipeline.
Usado pelo harmonizer (para produzir) e pelo validator (para verificar).

Campos obrigatórios (rejeitar se ausentes):
    id, title, theater, date_start, source_url

Campos recomendados (aviso se ausentes, nunca rejeitar):
    date_end, synopsis, category, subcategory, image, ticket_url, price_info

Filtros booleanos (null = informação desconhecida, não false):
    is_free, is_accessible, for_families, for_schools, has_lsp, is_festival

Campos de metadados (preenchidos pelo orquestrador):
    scraped_at, schema_version
"""

import hashlib
import re
from typing import TypedDict, Required

SCHEMA_VERSION = "2.0"


class ImageObject(TypedDict, total=False):
    url:     Required[str]
    credit:  str | None
    source:  Required[str]
    theater: Required[str]


class EventSchema(TypedDict, total=False):
    # Obrigatórios
    id:             Required[str]
    title:          Required[str]
    theater:        Required[str]
    date_start:     Required[str]
    source_url:     Required[str]
    # Recomendados
    date_end:       str
    synopsis:       str
    category:       str
    subcategory:    str
    image:          ImageObject
    ticket_url:     str
    price_info:     str
    # Filtros booleanos (None = desconhecido)
    is_free:        bool | None
    is_accessible:  bool | None
    for_families:   bool | None
    for_schools:    bool | None
    has_lsp:        bool | None
    is_festival:    bool | None
    # Metadados
    scraped_at:     str
    schema_version: str


# ─────────────────────────────────────────────────────────────
# Vocabulário controlado de categorias (10 categorias principais)
# ─────────────────────────────────────────────────────────────

CATEGORY_MAP: dict[str, list[str]] = {
    "Artes Performativas": [
        "teatro", "theatre", "peça", "peça de teatro", "teatro de texto",
        "teatro contemporâneo", "teatro clássico", "dramaturgia",
        "teatro de rua", "teatro de marionetas", "marionetas", "fantoches",
        "teatro físico", "teatro de objetos", "teatro de sombras",
        "teatro musical", "musical", "musicado", "comédia musical",
        "dança", "dance", "bailado", "ballet", "performance de dança",
        "dança contemporânea", "dança clássica", "dança urbana",
        "dança teatro", "tanztheater",
        "ópera", "opera", "lírica", "teatro lírico", "ópera contemporânea",
        "zarzuela", "opereta",
        "circo", "novo circo", "circo contemporâneo", "acrobacia",
        "artes circenses", "trapézio", "equilibrismo",
        "performance", "performance art", "instalação performativa",
        "arte performativa", "artes performativas", "happening",
        "comédia", "stand-up", "stand up", "comédia de improviso",
        "improviso", "improv",
        "monólogo", "revista", "variedades", "cabaret",
        "leitura encenada", "reading",
    ],
    "Música": [
        "música", "music", "concerto", "recital", "música ao vivo",
        "música clássica", "orquestra", "câmara", "sinfonia",
        "jazz", "blues", "soul", "funk",
        "fado", "fado tradicional", "fado contemporâneo",
        "rock", "pop", "indie", "alternativo",
        "eletrónica", "electronica", "dj set", "dj",
        "folk", "world music", "música do mundo", "música tradicional",
        "música experimental", "música contemporânea", "música de câmara",
        "coro", "coral", "canto", "ópera concerto",
        "jazz vocal", "a cappella",
    ],
    "Cinema & Audiovisual": [
        "cinema", "filme", "film", "projeção", "sessão de cinema",
        "documentário", "documentary",
        "curtas", "curtas-metragens", "short film",
        "videodança", "video arte", "videoarte", "instalação vídeo",
        "animação", "cinema de animação",
        "cinema mudo", "cinema experimental",
        "retrospetiva", "ciclo de cinema", "festival de cinema",
    ],
    "Artes Visuais & Exposições": [
        "exposição", "exposition", "exhibition",
        "instalação", "installation",
        "fotografia", "photography",
        "escultura", "sculpture",
        "pintura", "desenho", "gravura",
        "arte urbana", "graffiti", "street art",
        "arte contemporânea", "arte moderna",
        "design", "arquitectura",
        "visita guiada", "guided tour",
        "abertura de exposição", "vernissage",
        "open studio",
    ],
    "Literatura & Palavra": [
        "literatura", "leitura", "leitura dramatizada",
        "apresentação de livro", "lançamento de livro",
        "poesia", "poetry", "spoken word",
        "conto", "storytelling", "narrativa",
        "escrita criativa",
        "clube de leitura",
        "tradução literária",
    ],
    "Pensamento & Conversa": [
        "conferência", "conference",
        "debate", "colóquio", "simpósio",
        "mesa-redonda", "painel",
        "palestra", "talk", "lecture",
        "conversa", "diálogo", "encontro",
        "seminário",
        "ciclo de conversas", "ciclo de conferências",
        "apresentação", "comunicação",
    ],
    "Formação & Participação": [
        "workshop", "oficina", "atelier",
        "masterclass", "master class",
        "curso", "formação", "aula",
        "residência", "laboratório",
        "summer lab", "summer school",
        "bolsa", "candidatura",
        "participação", "co-criação",
    ],
    "Infanto-Juvenil": [
        "infantil", "para famílias", "para crianças", "kids",
        "teatro infantil", "teatro para a infância",
        "família", "para toda a família",
        "espetáculo infantil", "animação infantil",
        "conto infantil", "história para crianças",
        "concerto para crianças", "música para crianças",
        "cinema para crianças", "cinema infantil",
        "oficina infantil", "workshop infantil",
        "bebés", "para bebés",
    ],
    "Multidisciplinar": [
        "multidisciplinar", "interdisciplinar", "transdisciplinar",
        "festival", "festividade",
        "programação especial",
        "ciclo", "série",
    ],
    "Comunidade & Território": [
        "comunidade", "território",
        "participação comunitária", "mediação cultural",
        "educação artística",
        "projeto social", "inclusão",
    ],
}

# Lookup invertido: variante → categoria canónica
_CATEGORY_LOOKUP: dict[str, str] = {}
for _canonical, _variants in CATEGORY_MAP.items():
    _CATEGORY_LOOKUP[_canonical.lower()] = _canonical
    for _v in _variants:
        _CATEGORY_LOOKUP[_v.lower()] = _canonical


# ─────────────────────────────────────────────────────────────
# Vocabulário controlado de subcategorias
# ─────────────────────────────────────────────────────────────

SUBCATEGORY_MAP: dict[str, list[str]] = {
    "Teatro": [
        "teatro", "theatre", "peça", "peça de teatro", "teatro de texto",
        "teatro contemporâneo", "teatro clássico", "dramaturgia",
        "teatro de rua", "teatro de marionetas", "marionetas", "fantoches",
        "teatro físico", "teatro de objetos", "teatro de sombras",
        "leitura encenada", "reading",
    ],
    "Dança": [
        "dança", "dance", "bailado", "ballet",
        "dança contemporânea", "dança clássica", "dança urbana",
        "dança teatro", "tanztheater", "performance de dança",
    ],
    "Ópera": [
        "ópera", "opera", "lírica", "teatro lírico",
        "ópera contemporânea", "zarzuela", "opereta",
    ],
    "Teatro Musical": ["teatro musical", "musical", "musicado", "comédia musical"],
    "Circo": [
        "circo", "novo circo", "circo contemporâneo", "acrobacia",
        "artes circenses", "trapézio", "equilibrismo",
    ],
    "Performance": [
        "performance", "performance art", "instalação performativa",
        "arte performativa", "artes performativas", "happening",
    ],
    "Comédia": [
        "comédia", "stand-up", "stand up", "comédia de improviso",
        "improviso", "improv", "monólogo",
    ],
    "Revista & Variedades": ["revista", "variedades", "cabaret"],
    "Música Clássica": [
        "música clássica", "orquestra", "câmara", "sinfonia",
        "música de câmara", "coro", "coral", "recital",
    ],
    "Jazz & Blues": ["jazz", "blues", "soul", "funk", "jazz vocal"],
    "Fado": ["fado", "fado tradicional", "fado contemporâneo"],
    "Rock & Pop": ["rock", "pop", "indie", "alternativo"],
    "Música Eletrónica": ["eletrónica", "electronica", "dj set", "dj"],
    "Música do Mundo": ["folk", "world music", "música do mundo", "música tradicional"],
    "Música Experimental": ["música experimental", "música contemporânea"],
    "Concerto": ["concerto", "música ao vivo", "a cappella", "canto"],
    "Cinema": [
        "cinema", "filme", "film", "projeção", "sessão de cinema",
        "retrospetiva", "ciclo de cinema",
    ],
    "Documentário": ["documentário", "documentary"],
    "Curtas-Metragens": ["curtas", "curtas-metragens", "short film"],
    "Cinema de Animação": ["animação", "cinema de animação"],
    "Video Arte": [
        "videodança", "video arte", "videoarte", "instalação vídeo",
        "cinema experimental", "cinema mudo",
    ],
    "Exposição": [
        "exposição", "exposition", "exhibition",
        "abertura de exposição", "vernissage",
    ],
    "Instalação": ["instalação", "installation"],
    "Fotografia": ["fotografia", "photography"],
    "Arte Contemporânea": [
        "arte contemporânea", "arte moderna", "escultura",
        "pintura", "desenho", "gravura",
    ],
    "Arte Urbana": ["arte urbana", "graffiti", "street art"],
    "Design & Arquitectura": ["design", "arquitectura"],
    "Visita Guiada": ["visita guiada", "guided tour", "open studio"],
    "Leitura": ["leitura", "leitura dramatizada"],
    "Apresentação de Livro": ["apresentação de livro", "lançamento de livro"],
    "Poesia": ["poesia", "poetry", "spoken word"],
    "Conto & Narrativa": ["conto", "storytelling", "narrativa"],
    "Clube de Leitura": ["clube de leitura"],
    "Conferência": ["conferência", "conference", "palestra", "talk", "lecture"],
    "Debate": ["debate", "colóquio", "simpósio", "mesa-redonda", "painel"],
    "Conversa": ["conversa", "diálogo", "encontro", "ciclo de conversas"],
    "Seminário": ["seminário"],
    "Workshop": ["workshop", "oficina", "atelier"],
    "Masterclass": ["masterclass", "master class"],
    "Curso": ["curso", "formação", "aula", "summer lab", "summer school"],
    "Residência": ["residência", "laboratório"],
    "Teatro Infantil": [
        "teatro infantil", "teatro para a infância",
        "espetáculo infantil", "animação infantil",
    ],
    "Música para Crianças": ["concerto para crianças", "música para crianças"],
    "Cinema para Crianças": ["cinema para crianças", "cinema infantil"],
    "Conto Infantil": ["conto infantil", "história para crianças"],
    "Oficina Infantil": ["oficina infantil", "workshop infantil"],
    "Para Bebés": ["bebés", "para bebés"],
    "Festival": ["festival", "festividade", "programação especial"],
    "Ciclo": ["ciclo", "série"],
    "Mediação Cultural": [
        "mediação cultural", "participação comunitária",
        "educação artística", "projeto social", "inclusão",
    ],
}

# Lookup invertido: variante → subcategoria canónica
_SUBCATEGORY_LOOKUP: dict[str, str] = {}
for _sub_canonical, _sub_variants in SUBCATEGORY_MAP.items():
    _SUBCATEGORY_LOOKUP[_sub_canonical.lower()] = _sub_canonical
    for _sv in _sub_variants:
        _SUBCATEGORY_LOOKUP[_sv.lower()] = _sub_canonical

# Subcategoria → categoria pai
SUBCATEGORY_TO_CATEGORY: dict[str, str] = {
    "Teatro": "Artes Performativas",
    "Dança": "Artes Performativas",
    "Ópera": "Artes Performativas",
    "Teatro Musical": "Artes Performativas",
    "Circo": "Artes Performativas",
    "Performance": "Artes Performativas",
    "Comédia": "Artes Performativas",
    "Revista & Variedades": "Artes Performativas",
    "Música Clássica": "Música",
    "Jazz & Blues": "Música",
    "Fado": "Música",
    "Rock & Pop": "Música",
    "Música Eletrónica": "Música",
    "Música do Mundo": "Música",
    "Música Experimental": "Música",
    "Concerto": "Música",
    "Cinema": "Cinema & Audiovisual",
    "Documentário": "Cinema & Audiovisual",
    "Curtas-Metragens": "Cinema & Audiovisual",
    "Cinema de Animação": "Cinema & Audiovisual",
    "Video Arte": "Cinema & Audiovisual",
    "Exposição": "Artes Visuais & Exposições",
    "Instalação": "Artes Visuais & Exposições",
    "Fotografia": "Artes Visuais & Exposições",
    "Arte Contemporânea": "Artes Visuais & Exposições",
    "Arte Urbana": "Artes Visuais & Exposições",
    "Design & Arquitectura": "Artes Visuais & Exposições",
    "Visita Guiada": "Artes Visuais & Exposições",
    "Leitura": "Literatura & Palavra",
    "Apresentação de Livro": "Literatura & Palavra",
    "Poesia": "Literatura & Palavra",
    "Conto & Narrativa": "Literatura & Palavra",
    "Clube de Leitura": "Literatura & Palavra",
    "Conferência": "Pensamento & Conversa",
    "Debate": "Pensamento & Conversa",
    "Conversa": "Pensamento & Conversa",
    "Seminário": "Pensamento & Conversa",
    "Workshop": "Formação & Participação",
    "Masterclass": "Formação & Participação",
    "Curso": "Formação & Participação",
    "Residência": "Formação & Participação",
    "Teatro Infantil": "Infanto-Juvenil",
    "Música para Crianças": "Infanto-Juvenil",
    "Cinema para Crianças": "Infanto-Juvenil",
    "Conto Infantil": "Infanto-Juvenil",
    "Oficina Infantil": "Infanto-Juvenil",
    "Para Bebés": "Infanto-Juvenil",
    "Festival": "Multidisciplinar",
    "Ciclo": "Multidisciplinar",
    "Mediação Cultural": "Comunidade & Território",
}

# Retrocompatibilidade: categorias v1 → categoria v2
LEGACY_CATEGORY_MAP: dict[str, str] = {
    "Teatro":          "Artes Performativas",
    "Dança":           "Artes Performativas",
    "Ópera":           "Artes Performativas",
    "Teatro Musical":  "Artes Performativas",
    "Circo":           "Artes Performativas",
    "Performance":     "Artes Performativas",
    "Infanto-Juvenil": "Infanto-Juvenil",
    "Música":          "Música",
    "Outro":           "Multidisciplinar",
}


# ─────────────────────────────────────────────────────────────
# Geração de ID
# ─────────────────────────────────────────────────────────────

def generate_id(theater: str, title: str, date_start: str) -> str:
    """Gera ID determinístico SHA1. Formato: 'auto-{8 hex chars}'."""
    raw = f"{theater.lower().strip()}|{title.lower().strip()}|{date_start.strip()}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"auto-{digest}"


# ─────────────────────────────────────────────────────────────
# Normalização
# ─────────────────────────────────────────────────────────────

def normalize_category(raw: str | None) -> str:
    """
    Devolve categoria canónica v2.
    Trata retrocompatibilidade com categorias v1.
    Fallback: 'Multidisciplinar'.
    """
    if not raw:
        return "Multidisciplinar"
    stripped = raw.strip()
    key = stripped.lower()

    # 1. Lookup directo no novo mapa
    result = _CATEGORY_LOOKUP.get(key)
    if result:
        return result

    # 2. Retrocompatibilidade com categorias v1
    legacy = LEGACY_CATEGORY_MAP.get(stripped)
    if legacy is not None:
        return legacy

    return "Multidisciplinar"


def normalize_subcategory(raw: str | None) -> str | None:
    """Devolve subcategoria canónica, ou None se não reconhecida."""
    if not raw:
        return None
    return _SUBCATEGORY_LOOKUP.get(raw.strip().lower())


def infer_subcategory_from_raw(raw_category: str | None) -> str | None:
    """
    Tenta inferir subcategoria a partir do valor raw do scraper.
    Ex: scraper passa "dança contemporânea" → subcategory="Dança"
    """
    if not raw_category:
        return None
    return _SUBCATEGORY_LOOKUP.get(raw_category.strip().lower())


def get_category_for_subcategory(subcategory: str) -> str | None:
    """Devolve categoria pai de uma subcategoria canónica."""
    return SUBCATEGORY_TO_CATEGORY.get(subcategory)


# ─────────────────────────────────────────────────────────────
# Campos — usados pelo validator e harmonizer
# ─────────────────────────────────────────────────────────────

REQUIRED_FIELDS: tuple[str, ...] = (
    "id", "title", "theater", "date_start", "source_url",
)

RECOMMENDED_FIELDS: tuple[str, ...] = (
    "date_end", "synopsis", "category", "subcategory",
    "image", "ticket_url", "price_info",
)

FILTER_FIELDS: tuple[str, ...] = (
    "is_free", "is_accessible", "for_families",
    "for_schools", "has_lsp", "is_festival",
)

VALID_CATEGORIES: frozenset[str] = frozenset(CATEGORY_MAP.keys())
VALID_SUBCATEGORIES: frozenset[str] = frozenset(SUBCATEGORY_MAP.keys())
