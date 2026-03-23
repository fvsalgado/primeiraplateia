# Primeira Plateia — Makefile
# Uso: make <target>
#
# Targets disponíveis:
#   make scrape              corre o pipeline completo de scraping
#   make scrape-dry          simula o pipeline sem escrever ficheiros
#   make scrape-scraper ID=ccb  corre só um scraper (debug)
#   make build               gera artefactos de dados (build.py)
#   make test                corre testes unitários (pytest)
#   make smoke               corre smoke tests em todos os scrapers (com rede)
#   make smoke-scraper ID=ccb   smoke test de um scraper específico
#   make sync                sincroniza scrapers com theaters.json e scraper.py
#   make new-scraper         assistente interactivo para criar novo scraper
#   make logs                visualiza o último log de scraping
#   make logs-errors         visualiza só erros e avisos do último log
#   make logs-summary        resumo estatístico do último log
#   make check-urls          verifica se URLs dos eventos estão activas
#   make quality             gera relatório de qualidade por teatro
#   make install             instala dependências de scraping
#   make install-all         instala todas as dependências (scraping + stats)
#   make clean               remove ficheiros temporários e cache

.PHONY: scrape scrape-dry scrape-scraper build test smoke smoke-scraper \
        sync new-scraper logs logs-errors logs-summary check-urls quality \
        install install-all clean help

# ─────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────
PYTHON  := python3
PIP     := pip3
ID      ?= ccb  # ID padrão para targets que precisam de um scraper

# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────

scrape:
	@echo "→ A correr pipeline completo..."
	$(PYTHON) scraper.py

scrape-dry:
	@echo "→ Dry-run (sem escrever ficheiros)..."
	$(PYTHON) scraper.py --dry-run

scrape-scraper:
	@echo "→ A correr só o scraper: $(ID)"
	$(PYTHON) scraper.py --scraper $(ID)

build:
	@echo "→ A gerar artefactos de dados..."
	$(PYTHON) scripts/build.py

# ─────────────────────────────────────────────────────────────
# Qualidade e testes
# ─────────────────────────────────────────────────────────────

test:
	@echo "→ A correr testes unitários..."
	$(PYTHON) -m pytest tests/ -v --tb=short

smoke:
	@echo "→ A correr smoke tests (requer ligação à internet)..."
	$(PYTHON) scripts/test_scrapers.py

smoke-scraper:
	@echo "→ Smoke test para: $(ID)"
	$(PYTHON) scripts/test_scrapers.py --scraper $(ID)

quality:
	@echo "→ A gerar relatório de qualidade..."
	$(PYTHON) scripts/build.py
	@echo "Relatório em: data/quality_report.json"

check-urls:
	@echo "→ A verificar URLs de eventos..."
	$(PYTHON) scripts/check_urls.py

# ─────────────────────────────────────────────────────────────
# Logs
# ─────────────────────────────────────────────────────────────

logs:
	$(PYTHON) scripts/log_viewer.py

logs-errors:
	$(PYTHON) scripts/log_viewer.py --errors

logs-summary:
	$(PYTHON) scripts/log_viewer.py --summary

# ─────────────────────────────────────────────────────────────
# Scaffolding
# ─────────────────────────────────────────────────────────────

sync:
	@echo "→ A sincronizar scrapers..."
	$(PYTHON) scripts/sync_scrapers.py

new-scraper:
	@echo "→ Criar novo scraper (interactivo)..."
	$(PYTHON) scripts/new_scraper.py

# ─────────────────────────────────────────────────────────────
# Dependências
# ─────────────────────────────────────────────────────────────

install:
	$(PIP) install -r requirements.txt

install-all:
	$(PIP) install -r requirements.txt -r requirements-stats.txt

# ─────────────────────────────────────────────────────────────
# Limpeza
# ─────────────────────────────────────────────────────────────

clean:
	@echo "→ A limpar ficheiros temporários..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -f scraper_run.log scraper_run.jsonl
	@echo "  Limpo."

# ─────────────────────────────────────────────────────────────
# Ajuda
# ─────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "Primeira Plateia — Comandos disponíveis"
	@echo "─────────────────────────────────────────"
	@echo "  make scrape              Pipeline completo"
	@echo "  make scrape-dry          Dry-run (sem escrever)"
	@echo "  make scrape-scraper ID=ccb  Só um scraper"
	@echo "  make build               Gerar artefactos (build.py)"
	@echo "  make test                Testes unitários (pytest)"
	@echo "  make smoke               Smoke tests (com rede)"
	@echo "  make smoke-scraper ID=ccb  Smoke test de um scraper"
	@echo "  make sync                Sync scrapers ↔ theaters.json"
	@echo "  make new-scraper         Criar novo scraper (interactivo)"
	@echo "  make logs                Ver último log"
	@echo "  make logs-errors         Ver só erros/avisos"
	@echo "  make logs-summary        Resumo do log"
	@echo "  make check-urls          Verificar URLs activas"
	@echo "  make install             Instalar dependências"
	@echo "  make clean               Limpar ficheiros temp"
	@echo ""
