"""
scrapers/theater_registry.py
Primeira Plateia — Registry de teatros.

Lê theaters.json e constrói um dict de lookup em memória:
    alias_normalizado → nome_canónico

Uso:
    from scrapers.theater_registry import build_theater_registry
    registry = build_theater_registry()
    canonical = registry.get("ccb")  # → "CCB — Centro Cultural de Belém"

Para adicionar um novo teatro ou alias, editar theaters.json — não este ficheiro.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Caminho padrão — relativo à raiz do repositório
_DEFAULT_PATH = Path(__file__).parent.parent / "theaters.json"


def build_theater_registry(theaters_path: Path | str | None = None) -> dict[str, str]:
    """
    Constrói e devolve um dict de lookup:
        alias (str, lowercase, stripped) → nome canónico (str)

    O nome canónico é o campo "name" de cada teatro em theaters.json.
    São automaticamente incluídos como aliases:
        - o próprio "name"
        - o campo "short"
        - todos os valores em "aliases"

    Se theaters.json não for encontrado ou estiver malformado,
    devolve dict vazio e regista o erro — o pipeline continua sem normalização.
    """
    path = Path(theaters_path) if theaters_path else _DEFAULT_PATH

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.error(f"theater_registry: ficheiro não encontrado em {path}")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"theater_registry: JSON inválido em {path}: {e}")
        return {}

    registry: dict[str, str] = {}
    theaters = data.get("theaters", [])

    for theater in theaters:
        canonical = theater.get("name", "").strip()
        if not canonical:
            logger.warning(f"theater_registry: entrada sem 'name' — ignorada: {theater}")
            continue

        # Fontes de aliases: name + short + aliases[]
        raw_aliases = [
            canonical,
            theater.get("short", ""),
            *theater.get("aliases", []),
        ]

        for alias in raw_aliases:
            if not alias:
                continue
            key = alias.strip().lower()
            if key in registry and registry[key] != canonical:
                logger.warning(
                    f"theater_registry: alias '{key}' já mapeado para "
                    f"'{registry[key]}' — ignorado para '{canonical}'"
                )
                continue
            registry[key] = canonical

    logger.info(f"theater_registry: {len(theaters)} teatros, {len(registry)} aliases carregados")
    return registry


def get_canonical_name(raw: str, registry: dict[str, str]) -> str:
    """
    Devolve o nome canónico para um valor raw de teatro.
    Se não reconhecido, devolve o valor original limpo (sem alterar).
    Útil para o harmonizer.
    """
    if not raw:
        return ""
    key = raw.strip().lower()
    canonical = registry.get(key)
    if canonical:
        return canonical
    # Tentativa de correspondência parcial — útil para "CCB Lisboa" não listado
    for alias, name in registry.items():
        if alias in key or key in alias:
            logger.debug(f"theater_registry: correspondência parcial '{raw}' → '{name}'")
            return name
    logger.debug(f"theater_registry: teatro não reconhecido '{raw}' — mantido original")
    return raw.strip()
