"""
tests/test_harmonizer_validator.py
Primeira Plateia — Testes unitários para harmonizer.py e validator.py

Uso:
    pytest tests/test_harmonizer_validator.py -v
"""

import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from scrapers.harmonizer import harmonize, clean_title, clean_text
from scrapers.validator import validate
from scrapers.schema import normalize_category, normalize_subcategory, generate_id


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def make_event(**overrides) -> dict:
    """Cria evento mínimo válido com overrides opcionais."""
    base = {
        "id":         "test-abc12345",
        "title":      "Peça de Teste",
        "theater":    "CCB — Centro Cultural de Belém",
        "date_start": "2099-06-15",
        "source_url": "https://example.com/peca",
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────
# clean_title
# ─────────────────────────────────────────────────────────────

class TestCleanTitle:
    def test_remove_html(self):
        result = clean_title("<b>Título</b> em <i>itálico</i>")
        assert "<" not in result
        assert "Título" in result

    def test_normaliza_espacos(self):
        result = clean_title("  Título   com   espaços  ")
        assert result == "Título com espaços"

    def test_preserva_acronimos(self):
        result = clean_title("CCB — Concerto de Verão")
        assert "CCB" in result

    def test_strip(self):
        result = clean_title("\n\nTítulo\n\n")
        assert result == "Título"

    def test_vazio(self):
        assert clean_title("") == ""
        assert clean_title(None) == ""  # type: ignore


# ─────────────────────────────────────────────────────────────
# clean_text
# ─────────────────────────────────────────────────────────────

class TestCleanText:
    def test_decode_html_entities(self):
        result = clean_text("caf&eacute; &amp; bar")
        assert "&eacute;" not in result
        assert "café" in result

    def test_remove_tags(self):
        result = clean_text("<p>Parágrafo <strong>importante</strong>.</p>")
        assert "<" not in result

    def test_max_chars(self):
        text = "A" * 500
        result = clean_text(text, max_chars=100)
        assert len(result) <= 105  # margem para reticências


# ─────────────────────────────────────────────────────────────
# normalize_category
# ─────────────────────────────────────────────────────────────

class TestNormalizeCategory:
    def test_categoria_exacta(self):
        assert normalize_category("Música") == "Música"

    def test_variante_minuscula(self):
        assert normalize_category("teatro") == "Artes Performativas"

    def test_variante_danca(self):
        assert normalize_category("dança") == "Artes Performativas"

    def test_fallback_vazio(self):
        assert normalize_category("") == "Multidisciplinar"

    def test_fallback_none(self):
        assert normalize_category(None) == "Multidisciplinar"  # type: ignore

    def test_fallback_desconhecida(self):
        assert normalize_category("categoria_inventada_xpto") == "Multidisciplinar"

    def test_retrocompat_v1(self):
        # "Outro" do schema v1 → "Multidisciplinar"
        assert normalize_category("Outro") == "Multidisciplinar"
        # "Teatro" v1 → "Artes Performativas"
        assert normalize_category("Teatro") == "Artes Performativas"


# ─────────────────────────────────────────────────────────────
# normalize_subcategory
# ─────────────────────────────────────────────────────────────

class TestNormalizeSubcategory:
    def test_subcategoria_exacta(self):
        assert normalize_subcategory("Teatro") == "Teatro"

    def test_variante(self):
        assert normalize_subcategory("dança contemporânea") == "Dança"

    def test_desconhecida_devolve_none(self):
        assert normalize_subcategory("subcategoria_inexistente") is None

    def test_vazio_devolve_none(self):
        assert normalize_subcategory("") is None
        assert normalize_subcategory(None) is None  # type: ignore


# ─────────────────────────────────────────────────────────────
# generate_id
# ─────────────────────────────────────────────────────────────

class TestGenerateId:
    def test_formato(self):
        result = generate_id("CCB", "Peça X", "2025-03-15")
        assert result.startswith("auto-")
        assert len(result) == 13  # "auto-" + 8 hex chars

    def test_determinístico(self):
        a = generate_id("CCB", "Peça X", "2025-03-15")
        b = generate_id("CCB", "Peça X", "2025-03-15")
        assert a == b

    def test_diferente_para_diferentes_inputs(self):
        a = generate_id("CCB", "Peça X", "2025-03-15")
        b = generate_id("CCB", "Peça Y", "2025-03-15")
        assert a != b


# ─────────────────────────────────────────────────────────────
# harmonize
# ─────────────────────────────────────────────────────────────

class TestHarmonize:
    def test_harmonize_lista_vazia(self):
        assert harmonize([]) == []

    def test_gera_id_automatico(self):
        ev = make_event()
        del ev["id"]
        result = harmonize([ev])
        assert len(result) == 1
        assert result[0]["id"].startswith("auto-")

    def test_normaliza_categoria(self):
        ev = make_event(category="teatro")
        result = harmonize([ev])
        assert result[0]["category"] == "Artes Performativas"

    def test_is_free_inferido_de_price_info(self):
        ev = make_event(price_info="Entrada Livre", is_free=None)
        result = harmonize([ev])
        assert result[0]["is_free"] is True

    def test_is_free_nao_inferido_quando_pago(self):
        ev = make_event(price_info="15€ / 10€", is_free=None)
        result = harmonize([ev])
        assert result[0]["is_free"] is None  # desconhecido, não False

    def test_for_families_inferido_de_age_min(self):
        ev = make_event(age_min=3)
        result = harmonize([ev])
        assert result[0]["for_families"] is True

    def test_is_festival_inferido_do_titulo(self):
        ev = make_event(title="Festival de Verão 2025")
        result = harmonize([ev])
        assert result[0]["is_festival"] is True

    def test_image_string_converte_para_dict(self):
        ev = make_event(image="https://example.com/foto.jpg")
        result = harmonize([ev])
        assert isinstance(result[0]["image"], dict)
        assert result[0]["image"]["url"] == "https://example.com/foto.jpg"

    def test_image_invalida_vira_none(self):
        ev = make_event(image="/foto-relativa.jpg")
        result = harmonize([ev])
        assert result[0]["image"] is None

    def test_schema_version_adicionado(self):
        ev = make_event()
        result = harmonize([ev])
        assert result[0].get("schema_version") == "2.0"

    def test_nao_modifica_input(self):
        ev = make_event(category="teatro")
        original_category = ev["category"]
        harmonize([ev])
        assert ev["category"] == original_category  # não modificou in-place

    def test_age_min_invalido_descartado(self):
        ev = make_event(age_min=351)  # valor impossível
        result = harmonize([ev])
        assert result[0].get("age_min") is None


# ─────────────────────────────────────────────────────────────
# validate
# ─────────────────────────────────────────────────────────────

class TestValidate:
    def test_evento_valido_aceite(self):
        events = [make_event()]
        accepted, report = validate(events)
        assert len(accepted) == 1
        assert report["total_accepted"] == 1
        assert report["total_rejected"] == 0

    def test_sem_titulo_rejeitado(self):
        events = [make_event(title="")]
        accepted, report = validate(events)
        assert len(accepted) == 0
        assert report["total_rejected"] == 1

    def test_sem_date_start_rejeitado(self):
        events = [make_event(date_start="")]
        accepted, report = validate(events)
        assert len(accepted) == 0

    def test_sem_source_url_rejeitado(self):
        events = [make_event(source_url="")]
        accepted, report = validate(events)
        assert len(accepted) == 0

    def test_source_url_invalida_rejeitado(self):
        events = [make_event(source_url="nao-e-um-url")]
        accepted, report = validate(events)
        assert len(accepted) == 0

    def test_date_start_formato_invalido(self):
        events = [make_event(date_start="15-03-2025")]  # formato errado
        accepted, report = validate(events)
        assert len(accepted) == 0

    def test_multiplos_eventos(self):
        events = [make_event(id=f"test-{i:08x}") for i in range(5)]
        accepted, report = validate(events)
        assert len(accepted) == 5
        assert report["total_raw"] == 5

    def test_relatorio_gerado(self):
        events = [make_event()]
        _, report = validate(events)
        assert "generated_at" in report
        assert "total_raw" in report
        assert "total_accepted" in report
        assert "total_rejected" in report
        assert "rejected" in report
        assert "warnings" in report

    def test_event_expirado_gera_aviso(self):
        events = [make_event(date_start="2000-01-01", date_end="2000-01-31")]
        accepted, report = validate(events)
        # Não é rejeitado, mas gera aviso de evento expirado
        assert len(accepted) == 1
        assert report["total_warnings"] > 0

    def test_filtros_booleanos_invalidos_geram_aviso(self):
        # is_free deve ser bool ou None, não string
        events = [make_event(is_free="sim")]
        accepted, report = validate(events)
        # Aceite (não é campo obrigatório), mas com aviso
        assert len(accepted) == 1
        assert report["total_warnings"] > 0
