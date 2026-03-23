#!/usr/bin/env python3
"""
Primeira Plateia — Orquestrador principal v2.0
Agenda Cultural · "A cultura no melhor lugar"
Autor: Fábio Salgado <fabio@primeiraplateia.pt>

Pipeline: Recolha → Deduplicação → Harmonização → Validação → events.json

Fallback de cache:
  - Se um scraper lançar excepção ou devolver 0 resultados, os eventos
    anteriores desse espaço são reutilizados do events.json da run anterior.
  - Eventos em cache são marcados com _stale: true e _stale_since.
  - Eventos com _stale_since > STALE_MAX_DAYS são descartados.
  - Eventos expirados (date_end ou date_start < hoje) são sempre descartados.

Paralelismo:
  - Os scrapers correm em paralelo via ThreadPoolExecutor (max_workers=SCRAPER_WORKERS).
  - O acesso a raw_events é protegido por threading.Lock.
  - O logging é thread-safe por design (stdlib).

Modos de execução:
  python scraper.py                    # run completo
  python scraper.py --scraper ccb      # só um scraper (debug)
  python scraper.py --dry-run          # pipeline completo, não escreve ficheiros
"""
import argparse
import hashlib
import json
import logging
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# ─────────────────────────────────────────────────────────────
# Configuração de logging (texto + JSONL estruturado)
# ─────────────────────────────────────────────────────────────

class _JsonlHandler(logging.FileHandler):
    """Handler que escreve cada registo de log como linha JSON."""
    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = json.dumps({
                "ts":      self.formatter.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level":   record.levelname,
                "logger":  record.name,
                "msg":     record.getMessage(),
            }, ensure_ascii=False)
            self.stream.write(line + "\n")
            self.flush()
        except Exception:
            self.handleError(record)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper_run.log", mode="w", encoding="utf-8"),
    ],
)
# Adicionar handler JSONL
_jsonl_handler = _JsonlHandler("scraper_run.jsonl", mode="w", encoding="utf-8")
_jsonl_handler.setFormatter(logging.Formatter())
logging.getLogger().addHandler(_jsonl_handler)

logger = logging.getLogger("primeiraplateia")

# ─────────────────────────────────────────────────────────────
# Configuração central
# ─────────────────────────────────────────────────────────────

STALE_MAX_DAYS   = 120   # dias máximos de cache antes de descartar
SCRAPER_WORKERS  = 4     # scrapers em paralelo (conservador)
SYNOPSIS_MAX_CHARS = 300 # truncagem de sinopses

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
from scrapers.schema import SCHEMA_VERSION

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

# Lookup nome → módulo (para --scraper)
_SCRAPER_LOOKUP: dict[str, tuple[str, callable]] = {
    fn.__module__.split(".")[-1]: (name, fn)
    for name, fn in SCRAPERS
}


# ─────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────

def load_previous_events() -> dict[str, list[dict]]:
    """Lê o events.json da run anterior e devolve índice por nome de espaço."""
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
        logger.info(f"cache: {len(events)} eventos anteriores indexados ({len(index)} espaços)")
        return index
    except Exception as e:
        logger.warning(f"cache: erro ao ler events.json anterior — {e}")
        return {}


def filter_cache_for_theater(
    cached_events: list[dict],
    today: date,
) -> tuple[list[dict], int]:
    """Filtra eventos em cache: remove expirados e stale > STALE_MAX_DAYS."""
    kept = []
    dropped = 0

    for ev in cached_events:
        date_end   = ev.get("date_end", "")
        date_start = ev.get("date_start", "")
        expiry     = date_end or date_start
        if expiry and expiry < today.isoformat():
            dropped += 1
            continue

        stale_since_str = ev.get("_stale_since", "")
        if stale_since_str:
            try:
                stale_since = date.fromisoformat(stale_since_str)
                if (today - stale_since).days > STALE_MAX_DAYS:
                    dropped += 1
                    continue
            except ValueError:
                pass

        ev = dict(ev)
        ev["_stale"] = True
        if not ev.get("_stale_since"):
            ev["_stale_since"] = today.isoformat()
        kept.append(ev)

    return kept, dropped


# ─────────────────────────────────────────────────────────────
# Deduplicação
# ─────────────────────────────────────────────────────────────

def _fingerprint(ev: dict) -> str:
    title   = ev.get("title", "").lower().strip()
    theater = ev.get("theater", "").lower().strip()
    dt      = ev.get("date_start", "")[:16]
    return f"{title}|{theater}|{dt}"


def deduplicate(events: list[dict]) -> tuple[list[dict], list[dict]]:
    seen_ids: dict[str, str] = {}
    seen_fingerprints: dict[str, str] = {}
    unique: list[dict] = []
    removed: list[dict] = []

    for ev in events:
        eid = ev.get("id", "").strip()
        fp  = _fingerprint(ev)

        if eid and eid in seen_ids:
            removed.append({
                "id": eid, "title": ev.get("title", "?"),
                "reason": "duplicate_id", "original_title": seen_ids[eid],
            })
            continue

        if fp in seen_fingerprints:
            removed.append({
                "id": eid or "(sem id)", "title": ev.get("title", "?"),
                "reason": "duplicate_fingerprint", "fingerprint": fp,
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
# Execução de um scraper individual
# ─────────────────────────────────────────────────────────────

def _run_scraper(
    name: str,
    fn: callable,
    previous_by_theater: dict[str, list[dict]],
    today: date,
) -> tuple[str, list[dict], dict]:
    """Corre um scraper e devolve (name, eventos, health_dict). Nunca lança excepção."""
    try:
        evs = fn()
    except Exception as e:
        logger.error(f"  ERRO {name}: {e}", exc_info=True)
        evs = []
        exception_msg = str(e)
    else:
        exception_msg = None

    if len(evs) > 0:
        for ev in evs:
            ev.pop("_stale", None)
            ev.pop("_stale_since", None)
        health = {"status": "ok", "events_collected": len(evs)}
        logger.info(f"  OK   {name}: {len(evs)} eventos raw")
        return name, evs, health

    reason = "exception" if exception_msg else "zero_results"
    cached = previous_by_theater.get(name, [])

    if cached:
        kept, dropped = filter_cache_for_theater(cached, today)
        health = {
            "status":                   "stale",
            "reason":                   reason,
            "exception":                exception_msg,
            "events_from_cache":        len(kept),
            "events_expired_discarded": dropped,
            "stale_since":              kept[0].get("_stale_since") if kept else None,
        }
        logger.warning(
            f"  STALE {name}: {reason} — {len(kept)} eventos de cache "
            f"({dropped} expirados descartados)"
        )
        return name, kept, health
    else:
        health = {"status": "empty", "reason": reason, "exception": exception_msg}
        logger.warning(f"  VAZIO {name}: {reason} — sem cache disponível")
        return name, [], health


# ─────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────

def run(scraper_filter: str | None = None, dry_run: bool = False) -> None:
    t0    = time.time()
    today = datetime.now(timezone.utc).date()

    logger.info("=" * 55)
    logger.info("Primeira Plateia — Agenda Cultural")
    logger.info(f"  Schema v{SCHEMA_VERSION} | Scrapers: {len(SCRAPERS)} | Workers: {SCRAPER_WORKERS}")
    if dry_run:
        logger.info("  MODO DRY-RUN — sem escrita de ficheiros")
    if scraper_filter:
        logger.info(f"  FILTRO — só scraper: {scraper_filter}")
    logger.info("=" * 55)

    # Seleccionar scrapers a correr
    if scraper_filter:
        entry = _SCRAPER_LOOKUP.get(scraper_filter)
        if not entry:
            logger.error(f"Scraper '{scraper_filter}' não encontrado. Disponíveis: {list(_SCRAPER_LOOKUP.keys())}")
            sys.exit(1)
        scrapers_to_run = [entry]
    else:
        scrapers_to_run = SCRAPERS

    # ── 0. Cache ───────────────────────────────────────────────
    previous_by_theater = load_previous_events()

    # ── 1. Recolha em paralelo ────────────────────────────────
    raw_events: list[dict] = []
    scraper_health: dict[str, dict] = {}
    scraper_stats:  dict[str, int] = {}
    lock = threading.Lock()
    results_by_name: dict[str, tuple[list[dict], dict]] = {}

    with ThreadPoolExecutor(max_workers=SCRAPER_WORKERS) as executor:
        futures = {
            executor.submit(_run_scraper, name, fn, previous_by_theater, today): name
            for name, fn in scrapers_to_run
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                returned_name, evs, health = future.result()
            except Exception as e:
                logger.error(f"  ERRO INESPERADO {name}: {e}", exc_info=True)
                evs = []
                health = {"status": "empty", "reason": "unexpected_exception", "exception": str(e)}
                returned_name = name
            with lock:
                results_by_name[returned_name] = (evs, health)

    for name, fn in scrapers_to_run:
        evs, health = results_by_name.get(name, ([], {"status": "empty", "reason": "missing"}))
        raw_events.extend(evs)
        scraper_health[name] = health
        scraper_stats[name]  = len(evs)

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

    if dry_run:
        elapsed = round(time.time() - t0, 1)
        logger.info(f"\n[DRY-RUN] Pipeline concluído em {elapsed}s — {len(valid_events)} eventos válidos")
        logger.info("[DRY-RUN] Nenhum ficheiro escrito.")
        return

    # ── 6. Escrever events.json ───────────────────────────────
    output = {
        "schema_version": SCHEMA_VERSION,
        "updated_at":     datetime.now(timezone.utc).isoformat(),
        "total":          len(valid_events),
        "by_theater":     scraper_stats,
        "scraper_health": scraper_health,
        "meta": {
            "description":  "Primeira Plateia — Agenda cultural em Portugal. A cultura no melhor lugar.",
            "author":       "Fábio Salgado",
            "contact":      "fabio@primeiraplateia.pt",
            "site":         "https://www.primeiraplateia.pt",
            "license_note": "Títulos, sinopses (excerto) e imagens são propriedade dos espaços culturais indicados.",
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
    elapsed     = round(time.time() - t0, 1)
    stale_count = sum(1 for h in scraper_health.values() if h["status"] == "stale")
    empty_count = sum(1 for h in scraper_health.values() if h["status"] == "empty")
    free_count  = sum(1 for e in valid_events if e.get("is_free") is True)
    fam_count   = sum(1 for e in valid_events if e.get("for_families") is True)
    fest_count  = sum(1 for e in valid_events if e.get("is_festival") is True)

    logger.info(f"\nevents.json: {len(valid_events)} eventos válidos em {elapsed}s")
    logger.info("─" * 40)
    for name, count in scraper_stats.items():
        health = scraper_health.get(name, {})
        status = health.get("status", "ok")
        suffix = (
            f" [STALE — {health.get('reason', '')}]" if status == "stale" else
            f" [VAZIO — {health.get('reason', '')}]" if status == "empty" else ""
        )
        logger.info(f"  {name:<42} {count:>3} eventos{suffix}")
    logger.info(f"\n  Scrapers OK:               {len(scraper_health) - stale_count - empty_count}")
    logger.info(f"  Scrapers em cache (stale): {stale_count}")
    logger.info(f"  Scrapers sem dados:        {empty_count}")
    logger.info(f"  Duplicados removidos:      {len(duplicates)}")
    logger.info(f"  Rejeitados pela validação: {report['total_rejected']}")
    logger.info(f"  Com avisos:                {report['total_warnings']}")
    logger.info(f"  Gratuitos (is_free):       {free_count}")
    logger.info(f"  Para famílias:             {fam_count}")
    logger.info(f"  Festivais:                 {fest_count}")
    logger.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Primeira Plateia — Orquestrador de scraping v2.0"
    )
    parser.add_argument(
        "--scraper", metavar="ID",
        help="Correr só um scraper (ex: ccb, saoluiz, tagv). Para debug local.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pipeline completo mas sem escrever events.json nem validation_report.json.",
    )
    args = parser.parse_args()
    run(scraper_filter=args.scraper, dry_run=args.dry_run)
