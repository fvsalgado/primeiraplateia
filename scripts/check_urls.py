#!/usr/bin/env python3
"""
scripts/check_urls.py
Primeira Plateia — Verificador de URLs de eventos.

Verifica periodicamente (não a cada run) se as source_url e ticket_url
dos eventos ainda respondem com 200. Regista URLs mortas em data/broken_urls.json.

Corre em CI numa job separada (semanal) ou localmente a pedido:
    python scripts/check_urls.py
    python scripts/check_urls.py --limit 50          # verificar só 50 URLs
    python scripts/check_urls.py --field source_url  # só um campo
    python scripts/check_urls.py --workers 10        # paralelismo
    python scripts/check_urls.py --timeout 8         # timeout por pedido
"""

import argparse
import json
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    import requests
except ImportError:
    print("ERRO: requests não instalado. Corre: pip install requests")
    sys.exit(1)

from scrapers.utils import HEADERS

DEFAULT_TIMEOUT = 8
DEFAULT_WORKERS = 8
FIELDS_TO_CHECK = ("source_url", "ticket_url")


def check_url(session: requests.Session, url: str, timeout: int) -> tuple[str, int, str]:
    """
    Verifica se uma URL responde. Devolve (url, status_code, erro).
    status_code = 0 se houver excepção de rede.
    """
    if not url or not url.startswith("http"):
        return url, 0, "URL inválida"
    try:
        resp = session.head(url, timeout=timeout, allow_redirects=True)
        # Alguns sites não suportam HEAD — tentar GET com stream
        if resp.status_code in (405, 501):
            resp = session.get(url, timeout=timeout, stream=True)
            resp.close()
        return url, resp.status_code, ""
    except requests.exceptions.Timeout:
        return url, 0, "timeout"
    except requests.exceptions.TooManyRedirects:
        return url, 0, "too many redirects"
    except requests.exceptions.ConnectionError as e:
        return url, 0, f"connection error: {e}"
    except Exception as e:
        return url, 0, str(e)


def load_events() -> list[dict]:
    events_path = ROOT / "events.json"
    if not events_path.exists():
        # Tentar data/events.json
        events_path = ROOT / "data" / "events.json"
    if not events_path.exists():
        print("ERRO: events.json não encontrado.", file=sys.stderr)
        sys.exit(1)
    raw = json.loads(events_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return raw.get("events", [])
    return raw if isinstance(raw, list) else []


def main() -> int:
    parser = argparse.ArgumentParser(description="Primeira Plateia — Verificador de URLs")
    parser.add_argument("--limit",   type=int, default=0,       help="Máx. de URLs a verificar (0 = todas)")
    parser.add_argument("--field",   default="",                help="Só verificar um campo (source_url ou ticket_url)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Paralelismo (default: {DEFAULT_WORKERS})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help=f"Timeout em segundos (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--no-save", action="store_true",       help="Não gravar data/broken_urls.json")
    args = parser.parse_args()

    fields = [args.field] if args.field else list(FIELDS_TO_CHECK)
    events = load_events()

    # Recolher URLs únicas a verificar
    urls_to_check: dict[str, dict] = {}  # url → {theater, event_id, field}
    for ev in events:
        for field in fields:
            url = ev.get(field, "")
            if url and url not in urls_to_check:
                urls_to_check[url] = {
                    "theater":  ev.get("theater", ""),
                    "event_id": ev.get("id", ""),
                    "field":    field,
                }

    total = len(urls_to_check)
    if args.limit and args.limit < total:
        urls_to_check = dict(list(urls_to_check.items())[:args.limit])

    print(f"\nPrimeira Plateia — Verificação de URLs")
    print(f"─────────────────────────────────────")
    print(f"  {len(urls_to_check)} URLs únicas ({total} no total, {len(events)} eventos)")
    print(f"  Workers: {args.workers} | Timeout: {args.timeout}s\n")

    session = requests.Session()
    session.headers.update(HEADERS)

    broken: list[dict] = []
    ok_count = 0
    err_count = 0
    lock = threading.Lock()
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(check_url, session, url, args.timeout): (url, meta)
            for url, meta in urls_to_check.items()
        }
        done = 0
        for future in as_completed(futures):
            url, meta = futures[future]
            checked_url, status, error = future.result()
            done += 1

            if status == 200 or (300 <= status < 400):
                ok_count += 1
            else:
                err_count += 1
                broken_entry = {
                    "url":      url,
                    "status":   status,
                    "error":    error,
                    "theater":  meta["theater"],
                    "event_id": meta["event_id"],
                    "field":    meta["field"],
                }
                with lock:
                    broken.append(broken_entry)
                domain = urlparse(url).netloc
                print(f"  ✗ [{status or 'ERR'}] {domain} — {error or ''}")

            # Progresso simples
            if done % 20 == 0 or done == len(urls_to_check):
                elapsed = time.time() - t0
                print(f"  … {done}/{len(urls_to_check)} ({elapsed:.0f}s)", end="\r", flush=True)

    elapsed = round(time.time() - t0, 1)
    print(f"\n\n  ✓ OK:    {ok_count}")
    print(f"  ✗ Erros: {err_count}")
    print(f"  Tempo:   {elapsed}s\n")

    if broken:
        # Agrupar por teatro
        by_theater: dict[str, list] = {}
        for entry in broken:
            by_theater.setdefault(entry["theater"], []).append(entry)
        print("URLs com problemas por espaço cultural:")
        for theater, entries in sorted(by_theater.items()):
            print(f"  {theater}: {len(entries)} URL(s)")
            for e in entries[:3]:  # mostrar só as primeiras 3
                print(f"    [{e['status'] or 'ERR'}] {e['url'][:80]}")

    if not args.no_save:
        data_dir = ROOT / "data"
        data_dir.mkdir(exist_ok=True)
        report = {
            "checked_at":   datetime.now(timezone.utc).isoformat(),
            "total_checked": len(urls_to_check),
            "ok":            ok_count,
            "broken":        err_count,
            "urls":          broken,
        }
        out = data_dir / "broken_urls.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n  → Relatório guardado em {out}")

    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
