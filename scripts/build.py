#!/usr/bin/env python3
"""
scripts/build.py
Primeira Plateia — Gerador de artefactos de dados. v2.0

Lê events.json + theaters.json e produz:
  data/meta.json
  data/health.json
  data/events.slim.json
  data/events.json          (cópia com path consistente)
  data/search.json
  data/by-theater/<id>.json (um por espaço cultural)

Corre APÓS scraper.py no pipeline:
  sync_scrapers.py → scraper.py → build.py → deploy

Uso:
  python scripts/build.py
  python scripts/build.py --events path/to/events.json
"""

import json
import logging
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("build")

ROOT           = Path(__file__).parent.parent
DATA_DIR       = ROOT / "data"
EVENTS_PATH    = ROOT / "events.json"
THEATERS_PATH  = ROOT / "theaters.json"
BY_THEATER_DIR = DATA_DIR / "by-theater"


# ─────────────────────────────────────────────────────────────
# Completeness weights v2.0
# ─────────────────────────────────────────────────────────────

COMPLETENESS_WEIGHTS = {
    "sessions":        0.18,
    "synopsis":        0.14,
    "image":           0.10,
    "technical_sheet": 0.18,
    "people":          0.13,
    "price_info":      0.05,
    "duration":        0.05,
    "age_rating":      0.05,
    "ticket_url":      0.05,
    "subcategory":     0.04,  # novo v2.0
    "is_free":         0.02,  # novo v2.0 (derivado automaticamente)
    "for_families":    0.01,  # novo v2.0
}


def compute_completeness(ev: dict) -> float:
    score = 0.0
    for field, weight in COMPLETENESS_WEIGHTS.items():
        val = ev.get(field)
        if field == "sessions":
            if val and len(val) > 0:
                score += weight
        elif field == "technical_sheet":
            if val and isinstance(val, dict) and len(val) > 0:
                score += weight * min(len(val) / 10, 1.0)
        elif field == "people":
            if val and len(val) > 0:
                score += weight * min(len(val) / 5, 1.0)
        elif field == "image":
            if val and (isinstance(val, dict) and val.get("url") or isinstance(val, str)):
                score += weight
        elif field in ("is_free", "for_families"):
            # Booleanos: só conta se explicitamente True (not None)
            if val is True:
                score += weight
        else:
            if val:
                score += weight
    return round(score, 3)


# ─────────────────────────────────────────────────────────────
# Sessão mais próxima
# ─────────────────────────────────────────────────────────────

def get_next_session(ev: dict, today: str) -> str | None:
    sessions = ev.get("sessions", [])
    if sessions:
        future = [s for s in sessions if s.get("date", "") >= today]
        if future:
            return min(future, key=lambda s: s["date"])["date"]
    date_start = ev.get("date_start", "")
    return date_start if date_start >= today else None


# ─────────────────────────────────────────────────────────────
# Filtro de eventos futuros/activos
# ─────────────────────────────────────────────────────────────

def is_active(ev: dict, today: str) -> bool:
    date_end   = ev.get("date_end", "")
    date_start = ev.get("date_start", "")
    if date_end:
        return date_end >= today
    return date_start >= today


# ─────────────────────────────────────────────────────────────
# Contagens por data
# ─────────────────────────────────────────────────────────────

def count_events_on_date(events: list[dict], date_str: str) -> int:
    count = 0
    for ev in events:
        ds = ev.get("date_start", "")
        de = ev.get("date_end", "") or ds
        if ds <= date_str <= de:
            count += 1
    return count


def get_next_weekend(today: datetime) -> tuple[str, str]:
    days_until_sat = (5 - today.weekday()) % 7
    if days_until_sat == 0 and today.hour >= 20:
        days_until_sat = 7
    sat = today + timedelta(days=days_until_sat)
    sun = sat + timedelta(days=1)
    return sat.strftime("%Y-%m-%d"), sun.strftime("%Y-%m-%d")


def count_events_in_date_range(events: list[dict], date_from: str, date_to: str) -> int:
    count = 0
    for ev in events:
        ds = ev.get("date_start", "")
        de = ev.get("date_end", "") or ds
        if ds <= date_to and de >= date_from:
            count += 1
    return count


# ─────────────────────────────────────────────────────────────
# Slim fields (para o frontend)
# ─────────────────────────────────────────────────────────────

SLIM_FIELDS = {
    "id", "title", "subtitle", "theater", "theater_id", "city",
    "category", "subcategory",               # v2.0: subcategory
    "date_start", "date_end", "dates_label",
    "next_session", "has_sessions",
    "image_url",                              # extraído de image.url
    "source_url", "ticket_url",
    "price_info", "price_min",
    "age_rating", "age_min",
    "accessibility", "sessions",
    # Filtros booleanos v2.0
    "is_free", "is_accessible", "for_families",
    "for_schools", "has_lsp", "is_festival",
    "_stale", "_stale_since",
}


def to_slim(ev: dict, today: str) -> dict:
    slim = {}
    for f in SLIM_FIELDS:
        if f in ev:
            slim[f] = ev[f]

    # image_url extraído do objecto image
    if "image_url" not in slim:
        img = ev.get("image")
        if isinstance(img, dict):
            slim["image_url"] = img.get("url", "")
        elif isinstance(img, str):
            slim["image_url"] = img
        else:
            slim["image_url"] = ""

    slim["next_session"] = get_next_session(ev, today)
    slim["has_sessions"] = bool(ev.get("sessions"))

    return slim


# ─────────────────────────────────────────────────────────────
# Search index v2.0
# ─────────────────────────────────────────────────────────────

def to_search(ev: dict, today: str) -> dict:
    return {
        "id":            ev.get("id", ""),
        "title":         (ev.get("title") or "").lower(),
        "theater":       (ev.get("theater") or "").lower(),
        "theater_id":    ev.get("theater_id", ""),
        "city":          (ev.get("city") or "").lower(),
        "category":      (ev.get("category") or "").lower(),
        "subcategory":   (ev.get("subcategory") or "").lower(),   # v2.0
        "date_start":    ev.get("date_start", ""),
        "date_end":      ev.get("date_end", ""),
        "next_session":  get_next_session(ev, today),
        "has_sessions":  bool(ev.get("sessions")),
        "director":      (ev.get("director") or "").lower(),
        "author":        (ev.get("author") or "").lower(),
        "cast":          [n.lower() for n in (ev.get("cast") or [])],
        "people":        [n.lower() for n in (ev.get("people") or [])],
        "price_min":     ev.get("price_min"),
        "duration_min":  ev.get("duration_min"),
        "age_min":       ev.get("age_min"),
        "accessibility": ev.get("accessibility") or [],
        "tags":          ev.get("tags") or [],
        # Filtros booleanos v2.0
        "is_free":       ev.get("is_free"),
        "is_accessible": ev.get("is_accessible"),
        "for_families":  ev.get("for_families"),
        "for_schools":   ev.get("for_schools"),
        "has_lsp":       ev.get("has_lsp"),
        "is_festival":   ev.get("is_festival"),
    }


# ─────────────────────────────────────────────────────────────
# Anomaly detection
# ─────────────────────────────────────────────────────────────

def detect_anomalies(current_by_theater: dict[str, int], prev_meta_path: Path) -> list[str]:
    """Detecta espaços onde eventos caíram >50% face ao build anterior."""
    anomalies = []
    if not prev_meta_path.exists():
        return anomalies
    try:
        prev = json.loads(prev_meta_path.read_text(encoding="utf-8"))
        prev_by_theater = prev.get("by_theater", {})
        for theater, count in current_by_theater.items():
            prev_count = prev_by_theater.get(theater, 0)
            if prev_count > 0 and count < prev_count * 0.5:
                anomalies.append(
                    f"{theater}: {prev_count} → {count} eventos (queda de {round((1-count/prev_count)*100)}%)"
                )
    except Exception as e:
        logger.warning(f"Não foi possível ler meta.json anterior: {e}")
    return anomalies


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def build(events_path: Path = EVENTS_PATH) -> None:
    t0       = datetime.now(timezone.utc)
    today    = t0.date().isoformat()
    today_dt = datetime.now(timezone.utc)

    logger.info("=" * 55)
    logger.info("Primeira Plateia — build.py v2.0")
    logger.info("=" * 55)

    DATA_DIR.mkdir(exist_ok=True)
    BY_THEATER_DIR.mkdir(exist_ok=True)

    # ── Ler events.json ───────────────────────────────────────
    logger.info(f"A ler {events_path}…")
    scraper_health: dict = {}
    schema_version = "2.0"
    try:
        raw = json.loads(events_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning(f"events.json não encontrado — a gerar data/ vazio")
        raw = []
    except json.JSONDecodeError as e:
        logger.warning(f"JSON inválido: {e} — a gerar data/ vazio")
        raw = []

    if isinstance(raw, list):
        events = raw
    elif isinstance(raw, dict):
        events         = raw.get("events", [])
        scraper_health = raw.get("scraper_health", {})
        schema_version = raw.get("schema_version", "2.0")
    else:
        events = []

    logger.info(f"  {len(events)} eventos lidos (schema v{schema_version})")

    # ── Ler theaters.json ─────────────────────────────────────
    theater_id_map:   dict[str, str] = {}
    theater_city_map: dict[str, str] = {}
    theater_type_map: dict[str, str] = {}
    try:
        th_data = json.loads(THEATERS_PATH.read_text(encoding="utf-8"))
        for t in th_data.get("theaters", []):
            name = t.get("name", "")
            tid  = t.get("id", "")
            city = t.get("city", "")
            ttype = t.get("type", "")
            if name and tid:
                theater_id_map[name]   = tid
                theater_city_map[name] = city
                theater_type_map[name] = ttype
    except Exception as e:
        logger.warning(f"Não foi possível ler theaters.json: {e}")

    # ── Enriquecer eventos ────────────────────────────────────
    for ev in events:
        theater = ev.get("theater", "")
        if not ev.get("theater_id") and theater in theater_id_map:
            ev["theater_id"] = theater_id_map[theater]
        if not ev.get("city") and theater in theater_city_map:
            ev["city"] = theater_city_map[theater]
        if not ev.get("venue_type") and theater in theater_type_map:
            ev["venue_type"] = theater_type_map[theater]
        if "_meta" not in ev:
            ev["_meta"] = {}
        ev["_meta"]["completeness"] = compute_completeness(ev)

    # ── Filtrar eventos futuros/activos ───────────────────────
    future_events = [ev for ev in events if is_active(ev, today)]
    stale_count   = sum(1 for ev in future_events if ev.get("_stale"))
    logger.info(f"  {len(future_events)} eventos futuros/activos ({stale_count} em cache stale)")

    # ── data/events.json ──────────────────────────────────────
    out_events = DATA_DIR / "events.json"
    out_events.write_text(
        json.dumps(future_events, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"  → {out_events} ({len(future_events)} eventos)")

    # ── data/events.slim.json ─────────────────────────────────
    slim_events = [to_slim(ev, today) for ev in future_events]
    out_slim = DATA_DIR / "events.slim.json"
    out_slim.write_text(
        json.dumps(slim_events, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    logger.info(f"  → {out_slim} ({len(slim_events)} eventos, {out_slim.stat().st_size//1024}KB)")

    # ── data/search.json ──────────────────────────────────────
    search_index = [to_search(ev, today) for ev in future_events]
    out_search = DATA_DIR / "search.json"
    out_search.write_text(
        json.dumps(search_index, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    logger.info(f"  → {out_search} ({len(search_index)} entradas, {out_search.stat().st_size//1024}KB)")

    # ── data/by-theater/<id>.json ─────────────────────────────
    by_theater: dict[str, list] = {}
    for ev in future_events:
        tid = ev.get("theater_id") or ev.get("theater", "unknown").lower().replace(" ", "-")
        by_theater.setdefault(tid, []).append(ev)

    for tid, evs in by_theater.items():
        out_th = BY_THEATER_DIR / f"{tid}.json"
        out_th.write_text(
            json.dumps(evs, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    logger.info(f"  → {BY_THEATER_DIR}/ ({len(by_theater)} ficheiros)")

    # ── Estatísticas ──────────────────────────────────────────
    by_theater_count = {tid: len(evs) for tid, evs in by_theater.items()}
    id_to_name       = {v: k for k, v in theater_id_map.items()}
    by_theater_named = {
        id_to_name.get(tid, tid): count
        for tid, count in by_theater_count.items()
    }

    completeness_avg = 0.0
    if future_events:
        completeness_avg = round(
            sum(ev.get("_meta", {}).get("completeness", 0) for ev in future_events)
            / len(future_events),
            3,
        )

    prev_meta  = DATA_DIR / "meta.json"
    anomalies  = detect_anomalies(by_theater_named, prev_meta)
    if anomalies:
        logger.warning(f"  ⚠️  {len(anomalies)} anomalia(s) detectada(s):")
        for a in anomalies:
            logger.warning(f"     • {a}")

    health_summary = {
        name: {"status": h.get("status"), "stale_since": h.get("stale_since")}
        for name, h in scraper_health.items()
        if h.get("status") != "ok"
    }

    events_today        = count_events_on_date(future_events, today)
    weekend_sat, weekend_sun = get_next_weekend(today_dt)
    events_this_weekend = count_events_in_date_range(future_events, weekend_sat, weekend_sun)

    # Contagens por categoria v2.0
    by_category: dict[str, int] = {}
    by_subcategory: dict[str, int] = {}
    for ev in future_events:
        cat = ev.get("category") or "Multidisciplinar"
        by_category[cat] = by_category.get(cat, 0) + 1
        sub = ev.get("subcategory")
        if sub:
            by_subcategory[sub] = by_subcategory.get(sub, 0) + 1

    by_category    = dict(sorted(by_category.items(), key=lambda x: x[1], reverse=True))
    by_subcategory = dict(sorted(by_subcategory.items(), key=lambda x: x[1], reverse=True))

    # Contagens de filtros booleanos v2.0
    filter_counts = {
        "is_free":       sum(1 for e in future_events if e.get("is_free") is True),
        "is_accessible": sum(1 for e in future_events if e.get("is_accessible") is True),
        "for_families":  sum(1 for e in future_events if e.get("for_families") is True),
        "for_schools":   sum(1 for e in future_events if e.get("for_schools") is True),
        "has_lsp":       sum(1 for e in future_events if e.get("has_lsp") is True),
        "is_festival":   sum(1 for e in future_events if e.get("is_festival") is True),
    }

    # Cidades com espaços activos
    active_theater_names = {ev.get("theater", "") for ev in future_events}
    cities = sorted({
        theater_city_map[name]
        for name in active_theater_names
        if name in theater_city_map and theater_city_map[name]
    })

    logger.info(f"  Hoje ({today}): {events_today} eventos")
    logger.info(f"  Fim-de-semana ({weekend_sat}/{weekend_sun}): {events_this_weekend} eventos")
    logger.info(f"  Cidades activas: {', '.join(cities)}")
    logger.info(f"  Categorias: {', '.join(f'{k}:{v}' for k,v in by_category.items())}")
    logger.info(f"  Filtros: {filter_counts}")

    build_version = t0.strftime("%Y%m%d-%H%M")

    # ── data/meta.json ────────────────────────────────────────
    meta = {
        "updated_at":           t0.isoformat(),
        "build_version":        build_version,
        "schema_version":       schema_version,
        "total_events":         len(future_events),
        "total_theaters":       len(by_theater),
        "events_today":         events_today,
        "events_this_weekend":  events_this_weekend,
        "cities":               cities,
        "by_category":          by_category,
        "by_subcategory":       by_subcategory,      # novo v2.0
        "filter_counts":        filter_counts,       # novo v2.0
        "stale_theaters":       stale_count,
        "by_theater":           by_theater_named,
        "completeness_avg":     completeness_avg,
        "anomalies":            anomalies,
        "scraper_health":       health_summary,
    }
    out_meta = DATA_DIR / "meta.json"
    out_meta.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"  → {out_meta}")

    # ── data/health.json (endpoint de monitorização) ──────────
    health_ok     = sum(1 for h in scraper_health.values() if h.get("status") == "ok")
    health_stale  = sum(1 for h in scraper_health.values() if h.get("status") == "stale")
    health_empty  = sum(1 for h in scraper_health.values() if h.get("status") == "empty")
    health_data = {
        "updated_at":       t0.isoformat(),
        "status":           "ok" if health_empty == 0 and len(anomalies) == 0 else "degraded",
        "total_events":     len(future_events),
        "total_scrapers":   len(scraper_health) if scraper_health else None,
        "scrapers_ok":      health_ok,
        "scrapers_stale":   health_stale,
        "scrapers_empty":   health_empty,
        "is_free_count":    filter_counts["is_free"],
        "anomalies":        len(anomalies),
        "last_build":       build_version,
    }
    out_health = DATA_DIR / "health.json"
    out_health.write_text(
        json.dumps(health_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"  → {out_health}")

    # ── Sumário ───────────────────────────────────────────────
    elapsed = round((datetime.now(timezone.utc) - t0).total_seconds(), 2)
    logger.info("=" * 55)
    logger.info(f"Build concluído em {elapsed}s")
    logger.info(f"  Espaços culturais: {len(by_theater)}")
    logger.info(f"  Eventos:           {len(future_events)}")
    logger.info(f"  Hoje:              {events_today}")
    logger.info(f"  Fim-de-semana:     {events_this_weekend}")
    logger.info(f"  Cidades:           {len(cities)}")
    logger.info(f"  Em cache (stale):  {stale_count}")
    logger.info(f"  Completeness avg:  {completeness_avg}")
    logger.info(f"  Anomalias:         {len(anomalies)}")
    logger.info(f"  Gratuitos:         {filter_counts['is_free']}")
    logger.info(f"  Para famílias:     {filter_counts['for_families']}")
    logger.info("=" * 55)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Primeira Plateia — build de artefactos v2.0")
    parser.add_argument("--events", type=Path, default=EVENTS_PATH, help="Caminho para events.json")
    args = parser.parse_args()
    build(events_path=args.events)
