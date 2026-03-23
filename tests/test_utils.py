"""
tests/test_utils.py
Primeira Plateia — Testes unitários para scrapers/utils.py

Cobre: parse_date, parse_date_range, truncate_synopsis, build_sessions, make_id, sanitize_age_min

Uso:
    pytest tests/test_utils.py -v
    pytest tests/test_utils.py -v --tb=short
"""

import sys
from pathlib import Path

# Garantir que a raiz do repo está no sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from scrapers.utils import (
    parse_date,
    parse_date_range,
    truncate_synopsis,
    build_sessions,
    make_id,
    sanitize_age_min,
)


# ─────────────────────────────────────────────────────────────
# parse_date
# ─────────────────────────────────────────────────────────────

class TestParseDate:
    def test_dd_mm_yyyy_slash(self):
        assert parse_date("15/03/2025") == "2025-03-15"

    def test_dd_mm_yyyy_dot(self):
        assert parse_date("15.03.2025") == "2025-03-15"

    def test_dd_mes_yyyy_pt(self):
        assert parse_date("15 março 2025") == "2025-03-15"

    def test_dd_de_mes_yyyy(self):
        assert parse_date("15 de março de 2025") == "2025-03-15"

    def test_dd_mes_abreviado(self):
        assert parse_date("15 mar 2025") == "2025-03-15"

    def test_dd_mes_en(self):
        assert parse_date("15 march 2025") == "2025-03-15"

    def test_force_year(self):
        result = parse_date("15 março", force_year=2026)
        assert result == "2026-03-15"

    def test_empty_string(self):
        assert parse_date("") == ""

    def test_none_like(self):
        assert parse_date(None) == ""  # type: ignore

    def test_invalid_date(self):
        # 31 de fevereiro não existe
        assert parse_date("31/02/2025") == ""

    def test_leading_zero_day(self):
        assert parse_date("05/01/2025") == "2025-01-05"

    def test_todos_os_meses_pt(self):
        meses = [
            ("janeiro", "01"), ("fevereiro", "02"), ("março", "03"),
            ("abril", "04"), ("maio", "05"), ("junho", "06"),
            ("julho", "07"), ("agosto", "08"), ("setembro", "09"),
            ("outubro", "10"), ("novembro", "11"), ("dezembro", "12"),
        ]
        for nome, num in meses:
            result = parse_date(f"10 {nome} 2025")
            assert result == f"2025-{num}-10", f"Falhou para mês: {nome}"


# ─────────────────────────────────────────────────────────────
# parse_date_range
# ─────────────────────────────────────────────────────────────

class TestParseDateRange:
    def test_intervalo_completo(self):
        start, end = parse_date_range("15 março 2025 — 30 março 2025")
        assert start == "2025-03-15"
        assert end   == "2025-03-30"

    def test_intervalo_dia_mes_partilhado(self):
        # "15 — 30 março 2025": o dia inicial não tem mês nem ano
        start, end = parse_date_range("15 — 30 março 2025")
        assert start == "2025-03-15"
        assert end   == "2025-03-30"

    def test_data_unica(self):
        start, end = parse_date_range("15/03/2025")
        assert start == "2025-03-15"
        assert end   == "2025-03-15"

    def test_intervalo_dd_mm(self):
        # "15.03 — 30.03.2025"
        start, end = parse_date_range("15.03 — 30.03.2025")
        assert start == "2025-03-15"
        assert end   == "2025-03-30"

    def test_separador_a(self):
        start, end = parse_date_range("15 março a 30 março 2025")
        assert start == "2025-03-15"
        assert end   == "2025-03-30"

    def test_vazio(self):
        start, end = parse_date_range("")
        assert start == ""
        assert end   == ""

    def test_end_before_start_nao_crasha(self):
        # parse_date_range não garante ordem — isso é o validator que corrige
        start, end = parse_date_range("30 março 2025 — 15 março 2025")
        assert start != "" or end != ""  # pelo menos um deve ser válido


# ─────────────────────────────────────────────────────────────
# truncate_synopsis
# ─────────────────────────────────────────────────────────────

class TestTruncateSynopsis:
    def test_texto_curto_nao_trunca(self):
        text = "Uma peça sobre amor e liberdade."
        assert truncate_synopsis(text) == text

    def test_trunca_no_limite(self):
        # 350 chars → deve truncar a 300 ou na última frase antes
        text = "A" * 350
        result = truncate_synopsis(text)
        assert len(result) <= 301  # 300 + reticências

    def test_trunca_na_frase(self):
        text = ("Esta é a primeira frase. " * 10) + "Esta última não deve aparecer."
        result = truncate_synopsis(text, max_chars=100)
        assert result.endswith(".")  # cortou na frase
        assert "…" not in result  # não precisou de reticências

    def test_adiciona_reticencias(self):
        # Sem frase completa antes do limite → usa reticências
        text = "A" * 400
        result = truncate_synopsis(text, max_chars=300)
        assert result.endswith("…")

    def test_vazio(self):
        assert truncate_synopsis("") == ""
        assert truncate_synopsis(None) == ""  # type: ignore

    def test_max_chars_custom(self):
        text = "Palavra " * 50  # ~400 chars
        result = truncate_synopsis(text, max_chars=50)
        assert len(result) <= 55  # 50 + margem para reticências


# ─────────────────────────────────────────────────────────────
# build_sessions
# ─────────────────────────────────────────────────────────────

class TestBuildSessions:
    def test_data_unica(self):
        sessions = build_sessions("2025-03-15", "2025-03-15")
        assert len(sessions) == 1
        assert sessions[0]["date"] == "2025-03-15"

    def test_sem_data_devolve_vazio(self):
        sessions = build_sessions("", "")
        assert sessions == []

    def test_intervalo_curto(self):
        # 3 dias → 3 sessões
        sessions = build_sessions("2025-03-15", "2025-03-17")
        assert len(sessions) == 3
        assert sessions[0]["date"] == "2025-03-15"
        assert sessions[-1]["date"] == "2025-03-17"

    def test_tem_weekday(self):
        sessions = build_sessions("2025-03-15", "2025-03-15")  # sábado
        assert "weekday" in sessions[0]
        assert sessions[0]["weekday"] == "Sáb"

    def test_horario_extraido(self):
        sessions = build_sessions("2025-03-15", "2025-03-15", "21h00")
        assert sessions[0]["time"] == "21:00"

    def test_horario_dias_semana(self):
        # "Qua a Sex 21h00" → só gera sessões nas quartas, quintas, sextas
        sessions = build_sessions("2025-03-17", "2025-03-21", "Qua a Sex 21h00")
        weekdays = {s["weekday"] for s in sessions}
        assert "Qua" in weekdays or "Qui" in weekdays or "Sex" in weekdays
        assert "Sáb" not in weekdays
        assert "Dom" not in weekdays

    def test_limite_90_sessoes(self):
        # Intervalo muito longo sem padrão de dias não deve gerar > 2 sessões (só start/end)
        sessions = build_sessions("2025-01-01", "2025-12-31")
        assert len(sessions) <= 90

    def test_end_anterior_a_start(self):
        # Não deve crashar
        sessions = build_sessions("2025-03-20", "2025-03-15")
        assert isinstance(sessions, list)


# ─────────────────────────────────────────────────────────────
# make_id
# ─────────────────────────────────────────────────────────────

class TestMakeId:
    def test_formato_basico(self):
        result = make_id("ccb", "O Sonho de Uma Noite de Verão")
        assert result.startswith("ccb-")
        assert len(result) < 60

    def test_caracteres_especiais(self):
        result = make_id("tagv", "É uma peça! Com pontuação?")
        assert " " not in result
        assert "!" not in result
        assert "?" not in result

    def test_slug_truncado(self):
        result = make_id("tagv", "A" * 200)
        # O slug deve estar truncado a ~50 chars
        assert len(result) < 60


# ─────────────────────────────────────────────────────────────
# sanitize_age_min
# ─────────────────────────────────────────────────────────────

class TestSanitizeAgeMin:
    def test_valores_validos(self):
        for v in (0, 3, 6, 12, 16, 18, 21):
            assert sanitize_age_min(v) == v

    def test_valores_invalidos(self):
        for v in (-1, 22, 100, 351, 1770):
            assert sanitize_age_min(v) is None, f"Esperava None para {v}"

    def test_none_devolve_none(self):
        assert sanitize_age_min(None) is None

    def test_zero_valido(self):
        assert sanitize_age_min(0) == 0
