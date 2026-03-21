#!/usr/bin/env python3
"""
Primeira Plateia — Orquestrador principal
Pipeline: Recolha → Deduplicação → Harmonização → Validação → events.json

Fallback de cache:
  - Se um scraper lançar excepção ou devolver 0 resultados, os eventos
    anteriores desse teatro são reutilizados do events.json da run anterior.
  - Eventos em cache são marcados com _stale: true e _stale_since (data
    do último scraping bem-sucedido).
  - Eventos em cache com _stale_since há mais de STALE_MAX_DAYS dias
    são descartados mesmo que ainda não tenham expirado.
  - Eventos expirados (date_end ou date_start < hoje) são sempre descartados
    do cache, independentemente de qualquer outro critério.
"""
import hashlib
import json
import logging
import sys
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# ─────────────────────────────────────────────────────────────
# Configuração de logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper_run.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger("primeiraplateia")

# ─────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────

STALE_MAX_DAYS = 120  # dias máximos de cache antes de descartar

# ─────────────────────────────────────────────────────────────
# Scrapers activos
# Gerido automaticamente pelo script scripts/sync_scrapers.py
# Não editar manualmente — adicionar scrapers via scraper_*.py
# ─────────────────────────────────────────────────────────────
from scrapers import (
    ccb,
    mariamatos,
    saoluiz,
    scraper_culturgest,
    scraper_teatro_capitolio,
    scraper_teatro_variedades,
    scraper_viriato,
    scraper_theatrocirco,
    scraper_teatrodobairro,
    scraper_trindade,
    scraper_teatrodasfiguras,
    scraper_tagv,
    scraper_cine_teatro_avenida,
)
from scrapers.harmonizer import harmonize
from scrapers.validator import validate

SCRAPERS: list[tuple[str, callable]] = [
    ("Teatro Variedades",               scraper_teatro_variedades.scrape),
    ("Capitólio",                       scraper_teatro_capitolio.scrape),
    ("São Luiz Teatro Municipal",       saoluiz.scrape),
    ("Teatro Maria Matos",              mariamatos.scrape),
    ("Culturgest",                      scraper_culturgest.scrape),
    ("CCB — Centro Cultural de Belém",  ccb.scrape),
    ("Teatro Viriato",                  scraper_viriato.scrape),
    ("Theatro Circo",                   scraper_theatrocirco.scrape),
    ("Teatro do Bairro",                scraper_teatrodobairro.scrape),
    ("Teatro da Trindade INATEL",       scraper_trindade.scrape),
    ("Teatro das Figuras",              scraper_teatrodasfiguras.scrape),
    ("TAGV — Teatro Académico Gil Vicente", scraper_tagv.scrape),
    ("Cine-Teatro Avenida",             scraper_cine_teatro_avenida.scrape),
]


# ─────────────────────────────────────────────────────────────
# Cache — carregamento do events.json anterior
# ─────────────────────────────────────────────────────────────

def load_previous_events() -> dict[str, list[dict]]:
    """
    Lê o events.json da run anterior e devolve um índice por nome de teatro.
    Retorna {} se o ficheiro não existir ou estiver corrompido.
    """
    path = Path("events.json")
    if not path.exists():
        logger.info("cache: events.json anterior não encontrado — primeira run")
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        events = raw.get("events", []) if isinstance(raw, dict) else raw
        index: dict[str, list[dict]] = {}
        for ev in events:
            theater = ev.get("theater", "")
            if theater:
                index.setdefault(theater, []).append(ev)
        logger.info(f"cache: {len(events)} eventos anteriores indexados ({len(index)} teatros)")
        return index
    except Exception as e:
        logger.warning(f"cache: erro ao ler events.json anterior — {e}")
        return {}


def filter_cache_for_theater(
    cached_events: list[dict],
    today: date,
) -> tuple[list[dict], int]:
    """
    Filtra eventos em cache:
      - Remove eventos expirados (date_end ou date_start < hoje)
      - Remove eventos com _stale_since há mais de STALE_MAX_DAYS dias
      - Marca/actualiza _stale: true nos restantes (preserva _stale_since original)

    Devolve (eventos_válidos, n_descartados).
    """
    kept   = []
    dropped = 0

    for ev in cached_events:
        # 1. Verificar TTL por data de evento
        date_end   = ev.get("date_end", "")
        date_start = ev.get("date_start", "")
        expiry     = date_end or date_start
        if expiry and expiry < today.isoformat():
            dropped += 1
            continue

        # 2. Verificar staleness máxima
        stale_since_str = ev.get("_stale_since", "")
        if stale_since_str:
            try:
                stale_since = date.fromisoformat(stale_since_str)
                if (today - stale_since).days > STALE_MAX_DAYS:
                    dropped += 1
                    continue
            except ValueError:
                pass  # formato inesperado — mantém o evento

        # 3. Marcar como stale (preserva _stale_since se já existir)
        ev = dict(ev)  # cópia para não mutar o original
        ev["_stale"] = True
        if not ev.get("_stale_since"):
            # Primeira vez que fica stale — regista hoje como último sucesso conhecido
            # (aproximação conservadora: não sabemos exactamente quando foi o último sucesso)
            ev["_stale_since"] = today.isoformat()

        kept.append(ev)

    return kept, dropped


# ─────────────────────────────────────────────────────────────
# Deduplicação
# ─────────────────────────────────────────────────────────────

def _fingerprint(ev: dict) -> str:
    title   = ev.get("title", "").lower().strip()
    theater = ev.get("theater", "").lower().strip()
    date    = ev.get("date_start", "")[:16]
    return f"{title}|{theater}|{date}"


def deduplicate(events: list[dict]) -> tuple[list[dict], list[dict]]:
    seen_ids: dict[str, str]          = {}
    seen_fingerprints: dict[str, str] = {}
    unique: list[dict]                = []
    removed: list[dict]               = []

    for ev in events:
        eid = ev.get("id", "").strip()
        fp  = _fingerprint(ev)

        if eid and eid in seen_ids:
            removed.append({
                "id":             eid,
                "title":          ev.get("title", "?"),
                "reason":         "duplicate_id",
                "original_title": seen_ids[eid],
            })
            continue

        if fp in seen_fingerprints:
            removed.append({
                "id":          eid or "(sem id)",
                "title":       ev.get("title", "?"),
                "reason":      "duplicate_fingerprint",
                "fingerprint": fp,
            })
            continue

        if eid:
            seen_ids[eid] = ev.get("title", "?")
        seen_fingerprints[fp] = eid or fp
        unique.append(ev)

    if removed:
        logger.info(
            f"deduplicação: {len(removed)} duplicados removidos "
            f"({len(unique)} únicos de {len(events)} raw)"
        )
    return unique, removed


# ─────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────

def run() -> None:
    t0    = time.time()
    today = datetime.now(timezone.utc).date()

    logger.info("=" * 55)
    logger.info("Primeira Plateia — início do scraping")
    logger.info("=" * 55)

    # ── 0. Carregar cache (events.json anterior) ───────────────
    previous_by_theater = load_previous_events()

    # ── 1. Recolha ────────────────────────────────────────────
    raw_events: list[dict] = []

    # Estado de saúde por scraper (para o relatório final)
    scraper_health: dict[str, dict] = {}
    scraper_stats:  dict[str, int]  = {}

    for name, fn in SCRAPERS:
        try:
            evs = fn()
        except Exception as e:
            logger.error(f"  ERRO {name}: {e}", exc_info=True)
            evs = []
            exception_msg = str(e)
        else:
            exception_msg = None

        if len(evs) > 0:
            # Scraper bem-sucedido — limpa flag stale nos eventos novos
            for ev in evs:
                ev.pop("_stale", None)
                ev.pop("_stale_since", None)
            raw_events.extend(evs)
            scraper_stats[name] = len(evs)
            scraper_health[name] = {
                "status":           "ok",
                "events_collected": len(evs),
            }
            logger.info(f"  OK   {name}: {len(evs)} eventos raw")

        else:
            # Scraper falhou ou devolveu 0 — tentar cache
            reason = "exception" if exception_msg else "zero_results"
            cached = previous_by_theater.get(name, [])

            if cached:
                kept, dropped = filter_cache_for_theater(cached, today)
                raw_events.extend(kept)
                scraper_stats[name] = len(kept)
                scraper_health[name] = {
                    "status":                  "stale",
                    "reason":                  reason,
                    "exception":               exception_msg,
                    "events_from_cache":       len(kept),
                    "events_expired_discarded": dropped,
                    "stale_since":             (
                        kept[0].get("_stale_since") if kept else None
                    ),
                }
                logger.warning(
                    f"  STALE {name}: {reason} — {len(kept)} eventos de cache "
                    f"({dropped} expirados descartados)"
                )
            else:
                scraper_stats[name] = 0
                scraper_health[name] = {
                    "status":    "empty",
                    "reason":    reason,
                    "exception": exception_msg,
                }
                logger.warning(f"  VAZIO {name}: {reason} — sem cache disponível")

    logger.info(f"\nTotal raw: {len(raw_events)}")

    # ── 2. Deduplicação ───────────────────────────────────────
    deduped, duplicates = deduplicate(raw_events)
    logger.info(f"Após deduplicação: {len(deduped)} eventos")

    # ── 3. Harmonização ───────────────────────────────────────
    harmonized = harmonize(deduped)
    logger.info(f"Após harmonização: {len(harmonized)} eventos")

    # ── 4. Validação ──────────────────────────────────────────
    valid_events, report = validate(harmonized)
    report["total_duplicates_removed"] = len(duplicates)
    report["duplicates_removed"]       = duplicates
    report["scraper_health"]           = scraper_health

    # ── 5. Ordenação ──────────────────────────────────────────
    valid_events.sort(key=lambda e: e.get("date_start", "9999"))

    # ── 6. Escrever events.json ───────────────────────────────
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total":      len(valid_events),
        "by_theater": scraper_stats,
        "meta": {
            "description":  "Primeira Plateia — Agregação pública de programação cultural em Portugal. Conteúdos © respetivos teatros.",
            "license_note": "Títulos, sinopses (excerto) e imagens são propriedade dos teatros indicados.",
            "contact":      "fabio@primeiraplateia.pt",
            "takedown_url": "mailto:fabio@primeiraplateia.pt",
            "takedown_sla": "Remoção em 48h mediante pedido.",
        },
        "events": valid_events,
    }
    Path("events.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # ── 7. Escrever validation_report.json ────────────────────
    Path("validation_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # ── 8. Sumário final ──────────────────────────────────────
    elapsed      = round(time.time() - t0, 1)
    stale_count  = sum(1 for h in scraper_health.values() if h["status"] == "stale")
    empty_count  = sum(1 for h in scraper_health.values() if h["status"] == "empty")

    logger.info(f"\nevents.json: {len(valid_events)} eventos válidos em {elapsed}s")
    logger.info("─" * 40)
    for name, count in scraper_stats.items():
        health  = scraper_health.get(name, {})
        status  = health.get("status", "ok")
        suffix  = f" [STALE — {health.get('reason', '')}]" if status == "stale" else \
                  f" [VAZIO — {health.get('reason', '')}]" if status == "empty" else ""
        logger.info(f"  {name:<40} {count:>3} eventos{suffix}")
    logger.info(f"\n  Scrapers OK:               {len(scraper_health) - stale_count - empty_count}")
    logger.info(f"  Scrapers em cache (stale): {stale_count}")
    logger.info(f"  Scrapers sem dados:        {empty_count}")
    logger.info(f"  Duplicados removidos:      {len(duplicates)}")
    logger.info(f"  Rejeitados pela validação: {report['total_rejected']}")
    logger.info(f"  Com avisos:                {report['total_warnings']}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()
