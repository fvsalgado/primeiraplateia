#!/usr/bin/env python3
"""
scripts/log_viewer.py
Primeira Plateia — Visualizador de logs estruturados (scraper_run.jsonl).

Filtra e formata o JSONL de forma legível no terminal.
Usa cores ANSI para destacar níveis de log.

Uso:
    python scripts/log_viewer.py                      # todas as linhas
    python scripts/log_viewer.py --errors             # só ERROR e WARNING
    python scripts/log_viewer.py --scraper saoluiz    # só linhas desse scraper
    python scripts/log_viewer.py --level WARNING      # filtro por nível mínimo
    python scripts/log_viewer.py --tail 50            # últimas N linhas
    python scripts/log_viewer.py --file caminho.jsonl # ficheiro alternativo
    python scripts/log_viewer.py --summary            # resumo estatístico da run
"""

import argparse
import json
import sys
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# Cores ANSI
# ─────────────────────────────────────────────────────────────

_NO_COLOR = not sys.stdout.isatty()

COLORS = {
    "ERROR":   "\033[91m",   # vermelho
    "WARNING": "\033[93m",   # amarelo
    "INFO":    "\033[97m",   # branco
    "DEBUG":   "\033[90m",   # cinzento
    "RESET":   "\033[0m",
    "BOLD":    "\033[1m",
    "DIM":     "\033[2m",
    "GREEN":   "\033[92m",
    "CYAN":    "\033[96m",
}


def c(text: str, color: str) -> str:
    if _NO_COLOR:
        return text
    return f"{COLORS.get(color, '')}{text}{COLORS['RESET']}"


# ─────────────────────────────────────────────────────────────
# Parsing
# ─────────────────────────────────────────────────────────────

LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}


def parse_line(line: str) -> dict | None:
    try:
        return json.loads(line.strip())
    except json.JSONDecodeError:
        return None


def load_lines(path: Path) -> list[dict]:
    if not path.exists():
        print(c(f"Ficheiro não encontrado: {path}", "ERROR"), file=sys.stderr)
        sys.exit(1)
    lines = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        parsed = parse_line(raw)
        if parsed:
            lines.append(parsed)
    return lines


# ─────────────────────────────────────────────────────────────
# Formatação
# ─────────────────────────────────────────────────────────────

def format_line(entry: dict, show_logger: bool = True) -> str:
    ts      = entry.get("ts", "?")
    level   = entry.get("level", "INFO")
    logger  = entry.get("logger", "")
    msg     = entry.get("msg", "")

    # Simplificar nome do logger
    short_logger = logger.split(".")[-1] if logger else ""

    level_str = f"[{level:<7}]"
    level_col = {
        "ERROR":   "ERROR",
        "WARNING": "WARNING",
        "INFO":    "INFO",
        "DEBUG":   "DEBUG",
    }.get(level, "INFO")

    ts_str     = c(ts, "DIM")
    level_str  = c(level_str, level_col)
    logger_str = c(f"{short_logger}:", "CYAN") if show_logger and short_logger else ""
    msg_str    = c(msg, level_col) if level in ("ERROR", "WARNING") else msg

    parts = [ts_str, level_str]
    if logger_str:
        parts.append(logger_str)
    parts.append(msg_str)

    return "  ".join(parts)


# ─────────────────────────────────────────────────────────────
# Resumo estatístico
# ─────────────────────────────────────────────────────────────

def print_summary(lines: list[dict]) -> None:
    errors   = [l for l in lines if l.get("level") == "ERROR"]
    warnings = [l for l in lines if l.get("level") == "WARNING"]
    infos    = [l for l in lines if l.get("level") == "INFO"]

    print(c("─" * 55, "DIM"))
    print(c("RESUMO DA RUN", "BOLD"))
    print(c("─" * 55, "DIM"))
    print(f"  Total de linhas:  {len(lines)}")
    print(f"  {c('INFO:', 'INFO')}            {len(infos)}")
    print(f"  {c('WARNING:', 'WARNING')}         {len(warnings)}")
    print(f"  {c('ERROR:', 'ERROR')}           {len(errors)}")

    if lines:
        first_ts = lines[0].get("ts", "?")
        last_ts  = lines[-1].get("ts", "?")
        print(f"  Início:          {first_ts}")
        print(f"  Fim:             {last_ts}")

    if errors:
        print()
        print(c("ERROS:", "ERROR"))
        for e in errors:
            print(f"  • {e.get('msg', '')}")

    if warnings:
        print()
        print(c("AVISOS:", "WARNING"))
        for w in warnings:
            msg = w.get("msg", "")
            # Mostrar só avisos relevantes (STALE, VAZIO, REJEITADO)
            if any(kw in msg for kw in ("STALE", "VAZIO", "REJEITADO", "ausente", "stale")):
                print(f"  • {msg}")

    # Procurar linha de sumário final
    for line in reversed(lines):
        msg = line.get("msg", "")
        if "eventos válidos em" in msg:
            print()
            print(c(f"RESULTADO: {msg}", "GREEN"))
            break

    print(c("─" * 55, "DIM"))


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Primeira Plateia — Visualizador de logs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--file",    default="scraper_run.jsonl", help="Ficheiro JSONL (default: scraper_run.jsonl)")
    parser.add_argument("--scraper", metavar="ID",  help="Filtrar por nome de scraper (ex: saoluiz, ccb)")
    parser.add_argument("--level",   metavar="LVL", default="INFO", help="Nível mínimo: DEBUG, INFO, WARNING, ERROR")
    parser.add_argument("--errors",  action="store_true", help="Atalho para --level WARNING")
    parser.add_argument("--tail",    type=int, metavar="N", help="Mostrar só as últimas N linhas")
    parser.add_argument("--summary", action="store_true", help="Mostrar resumo estatístico da run")
    parser.add_argument("--no-color", action="store_true", help="Desactivar cores ANSI")
    args = parser.parse_args()

    global _NO_COLOR
    if args.no_color:
        _NO_COLOR = True

    log_path = Path(args.file)
    lines = load_lines(log_path)

    if args.summary:
        print_summary(lines)
        return

    # Filtro por nível
    min_level_str = "WARNING" if args.errors else args.level.upper()
    min_level_val = LEVEL_ORDER.get(min_level_str, 1)
    lines = [l for l in lines if LEVEL_ORDER.get(l.get("level", "INFO"), 1) >= min_level_val]

    # Filtro por scraper/logger
    if args.scraper:
        needle = args.scraper.lower()
        lines = [l for l in lines if needle in l.get("logger", "").lower() or needle in l.get("msg", "").lower()]

    # Tail
    if args.tail:
        lines = lines[-args.tail:]

    if not lines:
        print(c("Nenhuma linha corresponde aos filtros.", "DIM"))
        return

    for entry in lines:
        print(format_line(entry))


if __name__ == "__main__":
    main()
