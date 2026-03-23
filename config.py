"""
config.py
Primeira Plateia — Configuração central do pipeline.

Todas as constantes configuráveis estão aqui.
Sobrepor via variáveis de ambiente onde indicado.

Uso:
    from config import STALE_MAX_DAYS, SCRAPER_WORKERS, ...
"""

import os

# ─────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────

# Dias máximos de cache stale antes de descartar eventos de um teatro
# Sobrepor: STALE_MAX_DAYS=60
STALE_MAX_DAYS: int = int(os.environ.get("STALE_MAX_DAYS", 120))

# Scrapers em paralelo (ThreadPoolExecutor)
# Aumentar localmente se tiveres uma máquina rápida: SCRAPER_WORKERS=8
SCRAPER_WORKERS: int = int(os.environ.get("SCRAPER_WORKERS", 4))

# Comprimento máximo de sinopses (em caracteres)
SYNOPSIS_MAX_CHARS: int = int(os.environ.get("SYNOPSIS_MAX_CHARS", 300))

# Timeout HTTP padrão para pedidos nos scrapers (segundos)
# Sobrepor: SCRAPER_TIMEOUT=20
SCRAPER_TIMEOUT: int = int(os.environ.get("SCRAPER_TIMEOUT", 15))

# Número máximo de workers de detalhe por scraper (para scrapers com páginas individuais)
DETAIL_WORKERS: int = int(os.environ.get("DETAIL_WORKERS", 8))

# ─────────────────────────────────────────────────────────────
# Guarda de qualidade (usado no CI e no build)
# ─────────────────────────────────────────────────────────────

# Número mínimo de eventos válidos para o pipeline não abortar
MIN_EVENTS_THRESHOLD: int = int(os.environ.get("MIN_EVENTS_THRESHOLD", 50))

# Percentagem máxima de scrapers em stale antes de alertar
MAX_STALE_SCRAPERS_PCT: float = float(os.environ.get("MAX_STALE_SCRAPERS_PCT", 0.3))

# ─────────────────────────────────────────────────────────────
# Cache por teatro
# ─────────────────────────────────────────────────────────────

# Directoria onde guardar cache por teatro (relativo à raiz do repo)
CACHE_DIR: str = os.environ.get("CACHE_DIR", "scrapers/cache")

# ─────────────────────────────────────────────────────────────
# Arquivo histórico
# ─────────────────────────────────────────────────────────────

# Directoria de snapshots comprimidos (relativo à raiz do repo)
ARCHIVE_DIR: str = os.environ.get("ARCHIVE_DIR", "data/archive")

# Manter snapshots durante N dias (0 = guardar para sempre)
ARCHIVE_RETENTION_DAYS: int = int(os.environ.get("ARCHIVE_RETENTION_DAYS", 365))

# ─────────────────────────────────────────────────────────────
# Retry e resiliência HTTP
# ─────────────────────────────────────────────────────────────

# Número máximo de tentativas para pedidos HTTP
HTTP_MAX_RETRIES: int = int(os.environ.get("HTTP_MAX_RETRIES", 3))

# Backoff base em segundos (duplica a cada tentativa: 1s, 2s, 4s)
HTTP_RETRY_BACKOFF: float = float(os.environ.get("HTTP_RETRY_BACKOFF", 1.0))

# Códigos HTTP que devem ser retentados
HTTP_RETRY_CODES: tuple[int, ...] = (429, 500, 502, 503, 504)

# ─────────────────────────────────────────────────────────────
# Rate limiting por domínio
# ─────────────────────────────────────────────────────────────

# Número máximo de pedidos simultâneos ao mesmo domínio
DOMAIN_MAX_CONCURRENT: int = int(os.environ.get("DOMAIN_MAX_CONCURRENT", 3))

# ─────────────────────────────────────────────────────────────
# Notificações (ntfy.sh)
# ─────────────────────────────────────────────────────────────

# URL do tópico ntfy.sh (ex: https://ntfy.sh/primeiraplateia-alerts)
# Definir como variável de ambiente — não colocar aqui em claro
NTFY_URL: str = os.environ.get("NTFY_URL", "")

# ─────────────────────────────────────────────────────────────
# Completeness weights (usados em build.py)
# ─────────────────────────────────────────────────────────────

COMPLETENESS_WEIGHTS: dict[str, float] = {
    "sessions":        0.18,
    "synopsis":        0.14,
    "image":           0.10,
    "technical_sheet": 0.18,
    "people":          0.13,
    "price_info":      0.05,
    "duration":        0.05,
    "age_rating":      0.05,
    "ticket_url":      0.05,
    "subcategory":     0.04,
    "is_free":         0.02,
    "for_families":    0.01,
}
