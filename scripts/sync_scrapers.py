#!/usr/bin/env python3
"""
scripts/sync_scrapers.py
Primeira Plateia — Sincronização automática de scrapers.

Corre no GitHub Actions (sync.yml) e também localmente.

Fluxo:
  1. Lê scrapers/ e identifica módulos com def scrape() + dict THEATER
  2. Compara com scraper.py (scrapers registados) e theaters.json (teatros conhecidos)
  3. Para scrapers novos:
     a. Adiciona entrada a theaters.json (merge inteligente com overrides)
     b. Regista import e entrada SCRAPERS em scraper.py
  4. Para scrapers existentes com THEATER actualizado:
     a. Actualiza campos não-override em theaters.json
  5. Commita se houve alterações (em CI) ou imprime diff (local)
  6. Define GitHub Actions outputs: updated, new_scrapers

Uso local:
    python scripts/sync_scrapers.py
    python scripts/sync_scrapers.py --dry-run
"""

import importlib.util
import json
import os
import re
import sys
from pathlib import Path

ROOT          = Path(__file__).parent.parent
SCRAPERS_DIR  = ROOT / "scrapers"
SCRAPER_PY    = ROOT / "scraper.py"
THEATERS_JSON = ROOT / "theaters.json"

# Adicionar raiz do repositório ao sys.path ANTES de qualquer import
# dinâmico de scrapers, para que "from scrapers.utils import ..." funcione
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ─────────────────────────────────────────────────────────────
# Módulos da pasta scrapers/ que NÃO são scrapers de teatro
# ─────────────────────────────────────────────────────────────
_NON_SCRAPER_MODULES = {
    "base_variedades",
    "harmonizer",
    "theater_registry",
    "schema",
    "utils",
    "validator",
    "__init__",
}


# ─────────────────────────────────────────────────────────────
# 1. Descoberta de scrapers na pasta
# ─────────────────────────────────────────────────────────────

def discover_scrapers() -> dict[str, dict]:
    """
    Lê scrapers/ e devolve {module_name: THEATER_dict}
    para todos os módulos que têm def scrape() E dict THEATER.
    """
    found = {}

    for path in sorted(SCRAPERS_DIR.glob("*.py")):
        name = path.stem
        if name in _NON_SCRAPER_MODULES:
            continue

        source = path.read_text(encoding="utf-8")

        # Verificar presença de def scrape()
        if not re.search(r"^def scrape\s*\(", source, re.MULTILINE):
            continue

        # Verificar presença de THEATER = {...}
        if "THEATER" not in source:
            print(f"  AVISO: {name}.py tem def scrape() mas não tem dict THEATER — ignorado pelo sync.")
            print(f"         Adiciona o bloco THEATER ao scraper para registo automático.")
            continue

        # Importar o módulo dinamicamente para ler THEATER
        try:
            spec   = importlib.util.spec_from_file_location(f"scrapers.{name}", path)
            mod    = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            theater = getattr(mod, "THEATER", None)
            if not isinstance(theater, dict):
                print(f"  AVISO: {name}.py — THEATER não é um dict válido.")
                continue
            # Validar campos obrigatórios
            required = {"id", "name", "short", "color", "city", "site"}
            missing  = required - theater.keys()
            if missing:
                print(f"  AVISO: {name}.py — THEATER em falta campos: {missing}")
                continue
            found[name] = theater
        except Exception as e:
            print(f"  ERRO ao importar {name}.py: {e}")

    return found


# ─────────────────────────────────────────────────────────────
# 2. Ler estado actual
# ─────────────────────────────────────────────────────────────

def read_theaters_json() -> dict:
    """Lê theaters.json e devolve {theater_id: entry_dict}."""
    if not THEATERS_JSON.exists():
        return {}
    data = json.loads(THEATERS_JSON.read_text(encoding="utf-8"))
    return {t["id"]: t for t in data.get("theaters", [])}


def read_registered_scrapers() -> set[str]:
    """
    Lê scraper.py e devolve o conjunto de nomes de módulo
    já registados na lista SCRAPERS.
    Ex: {"scraper_viriato", "ccb", "saoluiz", ...}
    """
    if not SCRAPER_PY.exists():
        return set()
    source     = SCRAPER_PY.read_text(encoding="utf-8")
    registered = set()

    # Encontrar bloco "from scrapers import (...)" — pode ser multi-linha
    m = re.search(
        r"from\s+scrapers\s+import\s+\((.*?)\)",
        source,
        re.DOTALL,
    )
    if m:
        block = m.group(1)
        for name in re.findall(r"\b(\w+)\b", block):
            if name not in _NON_SCRAPER_MODULES and name != "scrapers":
                registered.add(name)

    # Também apanhar imports simples: "from scrapers import X"
    for line in source.splitlines():
        lm = re.match(r"from\s+scrapers\s+import\s+(\w+)", line.strip())
        if lm:
            name = lm.group(1)
            if name not in _NON_SCRAPER_MODULES and name != "scrapers":
                registered.add(name)

    return registered


# ─────────────────────────────────────────────────────────────
# 3. Merge inteligente theaters.json
# ─────────────────────────────────────────────────────────────

def merge_theater(existing: dict | None, theater: dict) -> tuple[dict, bool]:
    """
    Faz merge entre entrada existente no JSON e dados do scraper.

    Regras:
    - Campos em existing["_overrides"] → JSON ganha (não sobrescrever)
    - Campos não em _overrides → scraper ganha (actualizar)
    - Campos novos do scraper → sempre adicionar
    - Se existing é None → criar entrada nova

    Devolve (merged_dict, changed: bool).
    """
    if existing is None:
        entry = dict(theater)
        entry["_overrides"] = []
        return entry, True

    overrides = set(existing.get("_overrides", []))
    merged    = dict(existing)
    changed   = False

    for key, value in theater.items():
        if key == "_overrides":
            continue
        if key in overrides:
            # Campo com override manual — manter o que está no JSON
            continue
        if merged.get(key) != value:
            merged[key] = value
            changed     = True

    return merged, changed


def write_theaters_json(theaters_by_id: dict) -> None:
    """Escreve theaters.json preservando a ordem original + novos no fim."""
    # Ler ordem original para preservar
    if THEATERS_JSON.exists():
        original = json.loads(THEATERS_JSON.read_text(encoding="utf-8"))
        original_ids = [t["id"] for t in original.get("theaters", [])]
    else:
        original_ids = []

    # Ordenar: originais primeiro, novos no fim
    all_ids = original_ids + [i for i in theaters_by_id if i not in original_ids]
    ordered = [theaters_by_id[i] for i in all_ids if i in theaters_by_id]

    data = {"theaters": ordered}
    THEATERS_JSON.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ─────────────────────────────────────────────────────────────
# 4. Actualizar scraper.py
# ─────────────────────────────────────────────────────────────

def register_in_scraper_py(module_name: str, theater_name: str) -> bool:
    """
    Adiciona import e entrada SCRAPERS ao scraper.py.
    Devolve True se houve alteração.
    """
    source = SCRAPER_PY.read_text(encoding="utf-8")

    # Verificar se já está registado
    if module_name in source:
        return False

    # ── Adicionar import ──────────────────────────────────────
    # Encontrar o bloco "from scrapers import (...)" e adicionar no fim
    import_pattern = re.compile(
        r"(from\s+scrapers\s+import\s+\()(.*?)(\))",
        re.DOTALL,
    )
    m = import_pattern.search(source)
    if m:
        imports_block = m.group(2)
        # Adicionar o novo módulo no fim do bloco de imports, ordenado
        new_import    = f"    {module_name},"
        new_block     = imports_block.rstrip() + f"\n{new_import}\n"
        source        = source[:m.start(2)] + new_block + source[m.end(2):]
    else:
        # Fallback: adicionar import simples antes da linha "from scrapers.harmonizer"
        source = source.replace(
            "from scrapers.harmonizer import",
            f"from scrapers import {module_name}  # auto-registered\nfrom scrapers.harmonizer import",
        )

    # ── Adicionar à lista SCRAPERS ────────────────────────────
    scrapers_pattern = re.compile(
        r"(SCRAPERS\s*:\s*list\[.*?\]\s*=\s*\[)(.*?)(\])",
        re.DOTALL,
    )
    m2 = scrapers_pattern.search(source)
    if m2:
        scrapers_block = m2.group(2)
        new_entry      = f'    ("{theater_name}",{" " * max(1, 36 - len(theater_name))}{module_name}.scrape),'
        new_scrapers   = scrapers_block.rstrip() + f"\n{new_entry}\n"
        source         = source[:m2.start(2)] + new_scrapers + source[m2.end(2):]

    SCRAPER_PY.write_text(source, encoding="utf-8")
    return True


# ─────────────────────────────────────────────────────────────
# 5. GitHub Actions output
# ─────────────────────────────────────────────────────────────

def set_gha_output(key: str, value: str) -> None:
    """Escreve output para GitHub Actions via GITHUB_OUTPUT."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"{key}={value}\n")
    else:
        print(f"  [output] {key}={value}")


# ─────────────────────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────────────────────

def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 55)
    print("Primeira Plateia — Sync Scrapers")
    print("=" * 55)

    # Descobrir scrapers na pasta
    scrapers_found    = discover_scrapers()
    registered        = read_registered_scrapers()
    theaters_by_id    = read_theaters_json()

    print(f"\nScrapers encontrados em scrapers/: {len(scrapers_found)}")
    print(f"Scrapers registados em scraper.py: {len(registered)}")
    print(f"Teatros em theaters.json:          {len(theaters_by_id)}")

    new_scrapers      = []
    updated_theaters  = []
    scraper_py_changed = False

    for module_name, theater in scrapers_found.items():
        theater_id   = theater["id"]
        theater_name = theater["name"]
        existing     = theaters_by_id.get(theater_id)

        # Merge theaters.json
        merged, changed = merge_theater(existing, theater)
        if changed:
            theaters_by_id[theater_id] = merged
            if existing is None:
                print(f"\n  + NOVO teatro: {theater_name} (id: {theater_id})")
                new_scrapers.append(module_name)
            else:
                print(f"\n  ~ ACTUALIZADO: {theater_name} (campos do scraper)")
                updated_theaters.append(module_name)

        # Registar em scraper.py
        if module_name not in registered:
            print(f"  + Registar em scraper.py: {module_name}")
            if not dry_run:
                changed_py = register_in_scraper_py(module_name, theater_name)
                if changed_py:
                    scraper_py_changed = True
            else:
                print(f"    [dry-run] Seria adicionado: {module_name}")

    # Escrever theaters.json se houve alterações
    if (new_scrapers or updated_theaters) and not dry_run:
        write_theaters_json(theaters_by_id)
        print(f"\nteatros.json actualizado.")

    if scraper_py_changed:
        print(f"scraper.py actualizado.")

    # Sumário
    total_changed = len(new_scrapers) + len(updated_theaters)
    print(f"\n{'─' * 40}")
    print(f"  Novos teatros registados:   {len(new_scrapers)}")
    print(f"  Teatros actualizados:       {len(updated_theaters)}")
    print(f"  scraper.py modificado:      {'sim' if scraper_py_changed else 'não'}")
    if dry_run:
        print(f"  [dry-run] Nenhum ficheiro foi escrito.")
    print(f"{'─' * 40}")

    # GitHub Actions outputs
    updated = total_changed > 0 or scraper_py_changed
    set_gha_output("updated",      "true" if updated else "false")
    set_gha_output("new_scrapers", ",".join(new_scrapers) if new_scrapers else "")

    return 0


if __name__ == "__main__":
    sys.exit(main())
