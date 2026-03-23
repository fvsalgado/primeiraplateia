#!/usr/bin/env python3
"""
scripts/test_scrapers.py
Primeira Plateia — Smoke tests por scraper.

Corre cada scraper e verifica:
  (a) devolveu pelo menos 1 evento
  (b) todos os eventos têm os campos obrigatórios (id, title, theater, date_start, source_url)
  (c) as datas estão no futuro (ou são válidas)
  (d) não há IDs duplicados dentro do scraper

Não faz commits nem escreve events.json — apenas reporta.

Uso:
    python scripts/test_scrapers.py                   # todos os scrapers
    python scripts/test_scrapers.py --scraper ccb     # só um scraper
    python scripts/test_scrapers.py --fast            # para ao primeiro erro
    python scripts/test_scrapers.py --no-network      # salta scrapers que precisam de rede
"""

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from scraper import SCRAPERS, _SCRAPER_LOOKUP  # noqa: E402

REQUIRED_FIELDS = ("id", "title", "theater", "date_start", "source_url")

# Cores ANSI
OK      = "\033[92m✓\033[0m"
FAIL    = "\033[91m✗\033[0m"
WARN    = "\033[93m⚠\033[0m"
BOLD    = "\033[1m"
RESET   = "\033[0m"
DIM     = "\033[2m"


def check_events(name: str, events: list[dict]) -> tuple[bool, list[str]]:
    """Verifica lista de eventos. Devolve (passed, lista_de_erros)."""
    errors = []
    today = date.today().isoformat()

    if not events:
        errors.append("devolveu 0 eventos")
        return False, errors

    # Verificar campos obrigatórios
    missing_counts = {f: 0 for f in REQUIRED_FIELDS}
    ids_seen = set()
    duplicate_ids = []

    for i, ev in enumerate(events):
        for field in REQUIRED_FIELDS:
            if not ev.get(field):
                missing_counts[field] += 1

        eid = ev.get("id", "")
        if eid:
            if eid in ids_seen:
                duplicate_ids.append(eid)
            ids_seen.add(eid)

        # Verificar que date_start parece válida
        ds = ev.get("date_start", "")
        if ds and len(ds) >= 10:
            try:
                # Não obrigar a ser no futuro — scrapers podem ter eventos de hoje
                date.fromisoformat(ds[:10])
            except ValueError:
                if i < 3:  # reportar só os primeiros 3 para não encher output
                    errors.append(f"ev[{i}] date_start inválida: {ds!r}")

    for field, count in missing_counts.items():
        if count > 0:
            pct = round(count / len(events) * 100)
            errors.append(f"campo '{field}' ausente em {count}/{len(events)} eventos ({pct}%)")

    if duplicate_ids:
        errors.append(f"{len(duplicate_ids)} IDs duplicados: {duplicate_ids[:3]}")

    # source_url deve começar com http
    bad_urls = sum(
        1 for ev in events
        if ev.get("source_url") and not ev["source_url"].startswith("http")
    )
    if bad_urls:
        errors.append(f"source_url inválida em {bad_urls} eventos")

    return len(errors) == 0, errors


def run_scraper_test(name: str, fn, timeout_s: int = 120) -> dict:
    """Corre um scraper e devolve resultado do teste."""
    result = {
        "name":    name,
        "passed":  False,
        "events":  0,
        "errors":  [],
        "elapsed": 0.0,
        "exception": None,
    }

    t0 = time.time()
    try:
        events = fn()
        result["elapsed"] = round(time.time() - t0, 1)
        result["events"]  = len(events) if events else 0
        passed, errors    = check_events(name, events or [])
        result["passed"]  = passed
        result["errors"]  = errors
    except Exception as exc:
        result["elapsed"]   = round(time.time() - t0, 1)
        result["exception"] = str(exc)
        result["errors"]    = [f"excepção: {exc}"]

    return result


def print_result(result: dict) -> None:
    name    = result["name"]
    passed  = result["passed"]
    events  = result["events"]
    elapsed = result["elapsed"]
    errors  = result["errors"]

    icon = OK if passed else FAIL
    time_str = f"{DIM}({elapsed}s){RESET}"

    if passed:
        print(f"  {icon} {BOLD}{name}{RESET}  {events} eventos  {time_str}")
    else:
        print(f"  {icon} {BOLD}{name}{RESET}  {events} eventos  {time_str}")
        for err in errors:
            print(f"       {FAIL} {err}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Primeira Plateia — Smoke tests por scraper")
    parser.add_argument("--scraper", metavar="ID", help="Só testar um scraper (ex: ccb)")
    parser.add_argument("--fast",    action="store_true", help="Parar ao primeiro scraper com erro")
    parser.add_argument("--json",    action="store_true", help="Output em JSON (para CI)")
    args = parser.parse_args()

    if args.scraper:
        entry = _SCRAPER_LOOKUP.get(args.scraper)
        if not entry:
            print(f"Scraper '{args.scraper}' não encontrado. Disponíveis: {list(_SCRAPER_LOOKUP.keys())}")
            return 1
        scrapers_to_test = [entry]
    else:
        scrapers_to_test = SCRAPERS

    print(f"\n{BOLD}Primeira Plateia — Smoke Tests{RESET}")
    print(f"{DIM}A testar {len(scrapers_to_test)} scraper(s)…{RESET}\n")

    results = []
    failed_count = 0

    for name, fn in scrapers_to_test:
        result = run_scraper_test(name, fn)
        results.append(result)
        if not args.json:
            print_result(result)
        if not result["passed"]:
            failed_count += 1
            if args.fast:
                print(f"\n{FAIL} Parado após primeiro erro (--fast).")
                break

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 0 if failed_count == 0 else 1

    total   = len(results)
    passed  = total - failed_count
    total_events = sum(r["events"] for r in results)
    total_time   = sum(r["elapsed"] for r in results)

    print(f"\n{'─' * 50}")
    status_color = "\033[92m" if failed_count == 0 else "\033[91m"
    print(
        f"  {status_color}{BOLD}{passed}/{total} scrapers OK{RESET}  "
        f"| {total_events} eventos  "
        f"| {total_time:.1f}s total"
    )
    if failed_count:
        print(f"\n  {FAIL} {failed_count} scraper(s) com problemas:")
        for r in results:
            if not r["passed"]:
                print(f"     • {r['name']}: {'; '.join(r['errors'][:2])}")
    print()

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
