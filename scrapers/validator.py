"""
scrapers/validator.py
Primeira Plateia — Validador de eventos. v2.0

Corre após harmonização e antes de escrever o events.json.
Produz validation_report.json com estatísticas e erros.

v2.0:
- Validação de subcategory contra VALID_SUBCATEGORIES (aviso, não rejeição)
- Validação de filtros booleanos (só true/false/None)
- category obrigatória: Multidisciplinar como fallback (não rejeita)
- age_min: verifica intervalo [0–21]
"""
import re
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from scrapers.schema import (
    REQUIRED_FIELDS,
    VALID_CATEGORIES,
    VALID_SUBCATEGORIES,
    FILTER_FIELDS,
)
from scrapers.theater_registry import build_theater_registry

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_registry: dict[str, str] | None = None


def _get_valid_theaters() -> set[str]:
    global _registry
    if _registry is None:
        _registry = build_theater_registry()
    return set(_registry.values())


def validate(events: list[dict]) -> tuple[list[dict], dict]:
    """Recebe lista de eventos harmonizados, devolve (eventos_válidos, relatório)."""
    today = date.today().isoformat()
    valid_theaters = _get_valid_theaters()

    accepted = []
    rejected = []
    warnings = []

    for ev in events:
        errors = []
        warns = []

        # ── Campos obrigatórios ───────────────────────────────
        for field in REQUIRED_FIELDS:
            if not ev.get(field):
                errors.append(f"'{field}' ausente ou vazio")

        # ── Título: comprimento mínimo ────────────────────────
        title = ev.get("title", "")
        if title and len(title.strip()) < 3:
            errors.append(f"'title' demasiado curto: {title!r}")

        # ── Datas: formato e lógica ───────────────────────────
        ds = ev.get("date_start", "")
        de = ev.get("date_end", "")

        if ds and not DATE_RE.match(ds):
            errors.append(f"'date_start' formato inválido: {ds!r}")
        elif ds:
            end = de if (de and DATE_RE.match(de)) else ds
            if end < today:
                warns.append(f"evento já terminou ({end})")

        if de and DATE_RE.match(de) and ds and DATE_RE.match(ds):
            if de < ds:
                warns.append(f"'date_end' ({de}) < 'date_start' ({ds}) — corrigido")
                ev = dict(ev)
                ev["date_end"] = ds

        # ── Espaço cultural: verificar contra registry ────────
        theater = ev.get("theater", "")
        if theater and theater not in valid_theaters:
            warns.append(f"espaço cultural não reconhecido no registry: {theater!r}")

        # ── Categoria: verificar vocabulário controlado ───────
        category = ev.get("category", "")
        if category and category not in VALID_CATEGORIES:
            warns.append(f"'category' não reconhecida: {category!r}")

        # ── Subcategoria: verificar vocabulário controlado ────
        subcategory = ev.get("subcategory")
        if subcategory and subcategory not in VALID_SUBCATEGORIES:
            warns.append(f"'subcategory' não reconhecida: {subcategory!r}")

        # ── Filtros booleanos: verificar tipos ────────────────
        for field in FILTER_FIELDS:
            val = ev.get(field)
            if val is not None and not isinstance(val, bool):
                warns.append(f"'{field}' deve ser true/false/null, recebido: {val!r}")

        # ── age_min: verificar intervalo ──────────────────────
        age_min = ev.get("age_min")
        if age_min is not None and (not isinstance(age_min, int) or age_min < 0 or age_min > 21):
            warns.append(f"'age_min' fora do intervalo válido [0–21]: {age_min!r}")

        # ── Imagem: validar como objecto ──────────────────────
        image = ev.get("image")
        if image is not None:
            if isinstance(image, str):
                if image and not image.startswith("http"):
                    warns.append("'image' (string) com URL inválida — ignorada")
                    ev = dict(ev)
                    ev["image"] = None
                else:
                    warns.append("'image' ainda como string — harmonizer não correu?")
            elif isinstance(image, dict):
                img_url = image.get("url", "")
                if img_url and not img_url.startswith("http"):
                    warns.append(f"'image.url' inválida: {img_url!r} — ignorada")
                    ev = dict(ev)
                    ev["image"] = None
                if not image.get("theater"):
                    warns.append("'image.theater' ausente — crédito mínimo em falta")

        # ── source_url: formato ───────────────────────────────
        source_url = ev.get("source_url", "")
        if source_url and not source_url.startswith("http"):
            errors.append(f"'source_url' inválida: {source_url!r}")

        # ── Resultado ─────────────────────────────────────────
        ev_id = ev.get("id", "?")
        ev_title = ev.get("title", "?")

        if errors:
            rejected.append({"id": ev_id, "title": ev_title, "errors": errors})
        else:
            accepted.append(ev)
            if warns:
                warnings.append({"id": ev_id, "title": ev_title, "warnings": warns})

    report = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "schema_version": "2.0",
        "total_raw":      len(events),
        "total_accepted": len(accepted),
        "total_rejected": len(rejected),
        "total_warnings": len(warnings),
        "rejected":       rejected,
        "warnings":       warnings,
    }

    logger.info(
        f"validação: {len(accepted)} aceites | "
        f"{len(rejected)} rejeitados | {len(warnings)} com avisos"
    )
    for r in rejected:
        logger.warning(f"  REJEITADO {r['id']!r}: {r['errors']}")

    return accepted, report
