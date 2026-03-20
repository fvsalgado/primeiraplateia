#!/usr/bin/env python3
"""
Primeira Plateia — Orquestrador principal
Pipeline: Recolha → Deduplicação → Harmonização → Validação → events.json
"""
import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# ─────────────────────────────────────────────────────────────
# Configuração de logging
# Feita AQUI, antes de qualquer import de scrapers,
# para garantir que os handlers estão prontos quando
# os módulos filhos chamarem logging.getLogger(__name__).
# FileHandler em modo 'w' (overwrite) — um ficheiro por run.
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
    ("Theatro Circo",                       scraper_theatrocirco.scrape),
    ("Teatro do Bairro",                    scraper_teatrodobairro.scrape),
    ("Teatro da Trindade INATEL",           scraper_trindade.scrape),
    ("Teatro das Figuras",                  scraper_teatrodasfiguras.scrape),
]


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
    t0 = time.time()
    logger.info("=" * 55)
    logger.info("Primeira Plateia — início do scraping")
    logger.info("=" * 55)

    # ── 1. Recolha ────────────────────────────────────────────
    raw_events: list[dict]     = []
    scraper_stats: dict[str, int] = {}

    for name, fn in SCRAPERS:
        try:
            evs = fn()
            raw_events.extend(evs)
            scraper_stats[name] = len(evs)
            logger.info(f"  OK   {name}: {len(evs)} eventos raw")
        except Exception as e:
            logger.error(f"  ERRO {name}: {e}", exc_info=True)
            scraper_stats[name] = 0

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
    elapsed = round(time.time() - t0, 1)
    logger.info(f"\nevents.json: {len(valid_events)} eventos válidos em {elapsed}s")
    logger.info("─" * 40)
    for name, count in scraper_stats.items():
        logger.info(f"  {name:<35} {count:>3} raw")
    logger.info(f"\n  Duplicados removidos:      {len(duplicates)}")
    logger.info(f"  Rejeitados pela validação: {report['total_rejected']}")
    logger.info(f"  Com avisos:                {report['total_warnings']}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()
