#!/usr/bin/env python3
"""
scripts/build.py
Primeira Plateia — Gerador de artefactos de dados.

Lê events.json + theaters.json e produz:
  data/meta.json
  data/events.slim.json
  data/events.json          (cópia com path consistente)
  data/search.json
  data/by-theater/<id>.json (um por teatro)

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
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("build")

ROOT         = Path(__file__).parent.parent
DATA_DIR     = ROOT / "data"
EVENTS_PATH  = ROOT / "events.json"
THEATERS_PATH = ROOT / "theaters.json"
BY_THEATER_DIR = DATA_DIR / "by-theater"


# ─────────────────────────────────────────────────────────────
# Completeness weights (Architecture §7.3)
# ─────────────────────────────────────────────────────────────

COMPLETENESS_WEIGHTS = {
    "sessions":        0.20,
    "synopsis":        0.15,
    "image":           0.10,
    "technical_sheet": 0.20,
    "people":          0.15,
    "price_info":      0.05,
    "duration":        0.05,
    "age_rating":      0.05,
    "ticket_url":      0.05,
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
                # Proporcional ao nº de chaves (max útil: 10)
                score += weight * min(len(val) / 10, 1.0)
        elif field == "people":
            if val and len(val) > 0:
                score += weight * min(len(val) / 5, 1.0)
        elif field == "image":
            if val and (isinstance(val, dict) and val.get("url") or isinstance(val, str)):
                score += weight
        else:
            if val:
                score += weight
    return round(score, 3)


# ─────────────────────────────────────────────────────────────
# next_session — sessão futura mais próxima
# ─────────────────────────────────────────────────────────────

def get_next_session(ev: dict, today: str) -> str | None:
    sessions = ev.get("sessions", [])
    if sessions:
        future = [s for s in sessions if s.get("date", "") >= today]
        if future:
            return min(future, key=lambda s: s["date"])["date"]
    return ev.get("date_start") if ev.get("date_start", "") >= today else None


# ─────────────────────────────────────────────────────────────
# Slim fields (Architecture §8)
# ─────────────────────────────────────────────────────────────

SLIM_FIELDS = {
    "id", "title", "subtitle", "theater", "theater_id", "city",
    "category", "date_start", "date_end", "dates_label",
    "next_session", "has_sessions",
    "image_url",   # extraído de image.url
    "source_url", "ticket_url",
    "price_info", "price_min",
    "age_rating", "accessibility", "sessions",
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

    # next_session calculado
    slim["next_session"] = get_next_session(ev, today)

    # has_sessions
    slim["has_sessions"] = bool(ev.get("sessions"))

    return slim


# ─────────────────────────────────────────────────────────────
# Search index fields (Architecture §8)
# ─────────────────────────────────────────────────────────────

def to_search(ev: dict, today: str) -> dict:
    return {
        "id":           ev.get("id", ""),
        "title":        (ev.get("title") or "").lower(),
        "theater":      (ev.get("theater") or "").lower(),
        "theater_id":   ev.get("theater_id", ""),
        "city":         (ev.get("city") or "").lower(),
        "category":     (ev.get("category") or "").lower(),
        "date_start":   ev.get("date_start", ""),
        "date_end":     ev.get("date_end", ""),
        "next_session": get_next_session(ev, today),
        "has_sessions": bool(ev.get("sessions")),
        "director":     (ev.get("director") or "").lower(),
        "author":       (ev.get("author") or "").lower(),
        "cast":         [n.lower() for n in (ev.get("cast") or [])],
        "people":       [n.lower() for n in (ev.get("people") or [])],
        "price_min":    ev.get("price_min"),
        "duration_min": ev.get("duration_min"),
        "age_min":      ev.get("age_min"),
        "accessibility": ev.get("accessibility") or [],
        "tags":         ev.get("tags") or [],
    }


# ─────────────────────────────────────────────────────────────
# Anomaly detection
# ─────────────────────────────────────────────────────────────

def detect_anomalies(current_by_theater: dict[str, int], prev_meta_path: Path) -> list[str]:
    """Detecta teatros onde eventos caíram >50% face ao build anterior."""
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
    t0 = datetime.now(timezone.utc)
    today = t0.date().isoformat()

    logger.info("=" * 55)
    logger.info("Primeira Plateia — build.py")
    logger.info("=" * 55)

    # ── Criar directório data/ ────────────────────────────────
    DATA_DIR.mkdir(exist_ok=True)
    BY_THEATER_DIR.mkdir(exist_ok=True)

    # ── Ler events.json ───────────────────────────────────────
    logger.info(f"A ler {events_path}…")
    try:
        raw = json.loads(events_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning(f"events.json não encontrado em {events_path} — a gerar data/ vazio")
        raw = []
    except json.JSONDecodeError as e:
        logger.warning(f"JSON inválido: {e} — a gerar data/ vazio")
        raw = []

    # events.json pode ser lista ou dict com chave "events"
    if isinstance(raw, list):
        events = raw
    elif isinstance(raw, dict):
        events = raw.get("events", [])
    else:
        logger.warning("Formato inesperado de events.json — a continuar com lista vazia")
        events = []

    logger.info(f"  {len(events)} eventos lidos")

    # ── Ler theaters.json ─────────────────────────────────────
    theater_id_map: dict[str, str] = {}  # name → id
    theater_city_map: dict[str, str] = {}
    try:
        th_data = json.loads(THEATERS_PATH.read_text(encoding="utf-8"))
        for t in th_data.get("theaters", []):
            name = t.get("name", "")
            tid  = t.get("id", "")
            city = t.get("city", "")
            if name and tid:
                theater_id_map[name] = tid
                theater_city_map[name] = city
    except Exception as e:
        logger.warning(f"Não foi possível ler theaters.json: {e}")

    # ── Enriquecer eventos com theater_id e city ──────────────
    for ev in events:
        theater = ev.get("theater", "")
        if not ev.get("theater_id") and theater in theater_id_map:
            ev["theater_id"] = theater_id_map[theater]
        if not ev.get("city") and theater in theater_city_map:
            ev["city"] = theater_city_map[theater]

        # Calcular completeness
        if "_meta" not in ev:
            ev["_meta"] = {}
        ev["_meta"]["completeness"] = compute_completeness(ev)

    # ── Filtrar eventos futuros para artefactos slim/search ───
    future_events = [
        ev for ev in events
        if (ev.get("next_session") or ev.get("date_start", "")) >= today
        or (ev.get("date_end", "") >= today)
    ]
    logger.info(f"  {len(future_events)} eventos futuros/activos")

    # ── data/events.json (cópia completa) ────────────────────
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
        if tid not in by_theater:
            by_theater[tid] = []
        by_theater[tid].append(ev)

    for tid, evs in by_theater.items():
        out_th = BY_THEATER_DIR / f"{tid}.json"
        out_th.write_text(
            json.dumps(evs, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    logger.info(f"  → {BY_THEATER_DIR}/ ({len(by_theater)} ficheiros)")

    # ── Estatísticas by_theater para meta.json ────────────────
    by_theater_count = {
        tid: len(evs) for tid, evs in by_theater.items()
    }

    # Nome do teatro em vez de ID para leitura humana
    by_theater_named = {}
    id_to_name = {v: k for k, v in theater_id_map.items()}
    for tid, count in by_theater_count.items():
        name = id_to_name.get(tid, tid)
        by_theater_named[name] = count

    # ── Completeness médio ────────────────────────────────────
    completeness_avg = 0.0
    if future_events:
        completeness_avg = round(
            sum(ev.get("_meta", {}).get("completeness", 0) for ev in future_events)
            / len(future_events),
            3,
        )

    # ── Detectar anomalias ────────────────────────────────────
    prev_meta = DATA_DIR / "meta.json"
    anomalies = detect_anomalies(by_theater_named, prev_meta)
    if anomalies:
        logger.warning(f"  ⚠️  {len(anomalies)} anomalia(s) detectada(s):")
        for a in anomalies:
            logger.warning(f"     • {a}")

    # ── data/meta.json ────────────────────────────────────────
    build_version = t0.strftime("%Y%m%d-%H%M")
    meta = {
        "updated_at":       t0.isoformat(),
        "build_version":    build_version,
        "total_events":     len(future_events),
        "total_theaters":   len(by_theater),
        "by_theater":       by_theater_named,
        "completeness_avg": completeness_avg,
        "anomalies":        anomalies,
    }
    out_meta = DATA_DIR / "meta.json"
    out_meta.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"  → {out_meta}")

    # ── Sumário ───────────────────────────────────────────────
    elapsed = round((datetime.now(timezone.utc) - t0).total_seconds(), 2)
    logger.info("=" * 55)
    logger.info(f"Build concluído em {elapsed}s")
    logger.info(f"  Teatros:          {len(by_theater)}")
    logger.info(f"  Espectáculos:     {len(future_events)}")
    logger.info(f"  Completeness avg: {completeness_avg}")
    logger.info(f"  Anomalias:        {len(anomalies)}")
    logger.info("=" * 55)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Primeira Plateia — build de artefactos de dados")
    parser.add_argument("--events", type=Path, default=EVENTS_PATH, help="Caminho para events.json")
    args = parser.parse_args()
    build(events_path=args.events)
