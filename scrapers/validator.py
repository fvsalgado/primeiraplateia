"""
Validador de eventos Primeira Plateia.
Corre após harmonização e antes de escrever o events.json.
Produz um validation_report.json com estatísticas e erros.

Mudanças face à versão anterior:
- VALID_THEATERS substituída por leitura dinâmica de theaters.json via registry
- Validação de 'image' actualizada para objecto {url, credit, source, theater}
- Logging via logging.getLogger (sem configurar basicConfig aqui)
- Campo 'source_url' agora obrigatório (rejeita se ausente)
- Campos obrigatórios lidos de schema.REQUIRED_FIELDS
"""
import re
import logging
from datetime import date, datetime
from pathlib import Path

from scrapers.schema import REQUIRED_FIELDS
from scrapers.theater_registry import build_theater_registry

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Registry carregado uma vez — partilhado com o harmonizer via módulo
_registry: dict[str, str] | None = None


def _get_valid_theaters() -> set[str]:
    """Devolve o conjunto de nomes canónicos a partir de theaters.json."""
    global _registry
    if _registry is None:
        _registry = build_theater_registry()
    return set(_registry.values())


def validate(events: list[dict]) -> tuple[list[dict], dict]:
    """
    Recebe lista de eventos harmonizados, devolve (eventos_válidos, relatório).
    """
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
                ev["date_end"] = ds  # correcção não-destrutiva (ev já é cópia do harmonizer)

        # ── Teatro: verificar contra registry ─────────────────
        theater = ev.get("theater", "")
        if theater and theater not in valid_theaters:
            # Aviso, não erro — o harmonizer já tentou normalizar
            # Se chegou aqui não normalizado, é um teatro genuinamente novo
            warns.append(f"teatro não reconhecido no registry: {theater!r}")

        # ── Imagem: validar como objecto ──────────────────────
        image = ev.get("image")
        if image is not None:
            if isinstance(image, str):
                # Scraper não foi ainda actualizado — aceitar mas avisar
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
            rejected.append({
                "id":     ev_id,
                "title":  ev_title,
                "errors": errors,
            })
        else:
            accepted.append(ev)
            if warns:
                warnings.append({
                    "id":       ev_id,
                    "title":    ev_title,
                    "warnings": warns,
                })

    report = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "total_raw":      len(events),
        "total_accepted": len(accepted),
        "total_rejected": len(rejected),
        "total_warnings": len(warnings),
        "rejected":       rejected,
        "warnings":       warnings,
    }

    logger.info(
        f"validação: {len(accepted)} aceites | "
        f"{len(rejected)} rejeitados | "
        f"{len(warnings)} com avisos"
    )
    for r in rejected:
        logger.warning(f"  REJEITADO {r['id']!r}: {r['errors']}")

    return accepted, report
