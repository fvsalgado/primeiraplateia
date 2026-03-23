# Arquitectura — Primeira Plateia

Documentação técnica do pipeline de dados para contribuidores de código.  
Para informação sobre o site em si, ver o [README.md](README.md).

---

## Visão geral

A Primeira Plateia é um agregador de agenda cultural em Portugal. O pipeline corre diariamente no GitHub Actions, recolhe eventos de 13+ teatros, normaliza os dados e publica um site estático em GitHub Pages.

```
scrapers/          →  scraper.py         →  events.json
(13 módulos)          (orquestrador)         (contrato de dados)
                          ↓
                       build.py
                          ↓
                        data/
                   ├── meta.json
                   ├── health.json
                   ├── events.slim.json
                   ├── search.json
                   ├── by-theater/<id>.json
                   ├── quality_report.json
                   └── archive/YYYY-MM-DD.json.gz
                          ↓
                     GitHub Pages
                    (site estático)
```

---

## Componentes

### `config.py`
Configuração central do pipeline. Todas as constantes configuráveis estão aqui e podem ser sobrepostas por variáveis de ambiente. Ver o ficheiro para a lista completa.

### `scraper.py` — Orquestrador principal
Coordena a execução dos scrapers em paralelo via `ThreadPoolExecutor`. Responsável por:
- Carregar cache do `events.json` anterior (fallback se um scraper falhar)
- Correr os scrapers em paralelo (`SCRAPER_WORKERS` workers)
- Marcar eventos como `_stale` quando um scraper falha (cache com data de validade)
- Escrever `events.json` e `validation_report.json`
- Produzir logs em texto (`scraper_run.log`) e JSONL estruturado (`scraper_run.jsonl`)

**Pipeline interno:**
```
load_previous_events()   →  cache por teatro
    ↓
_run_scraper() × N       →  recolha em paralelo
    ↓
deduplicate()            →  remoção de duplicados (por ID e por fingerprint)
    ↓
harmonize()              →  normalização (categorias, imagens, filtros booleanos)
    ↓
validate()               →  verificação de campos obrigatórios e formatos
    ↓
events.json              →  output final
```

### `scrapers/` — Módulos de scraping

Cada scraper é um módulo Python com:
- Um dict `THEATER` no nível do módulo (lido pelo `sync_scrapers.py`)
- Uma função `def scrape() -> list[dict]`

Ver `scrapers/TEMPLATE.py` para a estrutura completa e `CONTRIBUTING.md` para o guia de adição de novos scrapers.

Módulos de suporte (não são scrapers):
- `utils.py` — funções partilhadas: `parse_date`, `parse_date_range`, `truncate_synopsis`, `build_sessions`, `fetch_with_retry`, rate limiting por domínio
- `harmonizer.py` — normalização de dados: categorias, imagens, filtros booleanos, IDs automáticos
- `validator.py` — validação de campos obrigatórios e formatos
- `schema.py` — vocabulário controlado de categorias/subcategorias, geração de IDs
- `theater_registry.py` — lookup de nomes canónicos de espaços culturais
- `base_variedades.py` — classe base partilhada pelos scrapers do Teatro Variedades e Capitólio

### `scripts/build.py` — Gerador de artefactos
Lê `events.json` + `theaters.json` e produz os ficheiros em `data/` que o frontend consome. Calcula completeness scores, deteta anomalias (queda > 50% de eventos por teatro), gera snapshots comprimidos em `data/archive/`, e o relatório de qualidade por teatro.

### `scripts/sync_scrapers.py` — Sincronização automática
Corre no CI antes do scraping. Descobre novos scrapers em `scrapers/` (que tenham `def scrape()` e `THEATER`), atualiza `theaters.json` e regista os imports em `scraper.py`. Permite adicionar um novo teatro sem editar manualmente esses ficheiros.

### `scripts/fetch_stats.py` — Métricas de analytics
Recolhe dados do Google Analytics 4 e Google Search Console (via secrets do CI). Corre com `continue-on-error: true` — a falha não bloqueia o deploy.

---

## Contrato de dados

### Evento (`events.json`)

Campos obrigatórios — a ausência causa rejeição:
```json
{
  "id":         "auto-a1b2c3d4",
  "title":      "Nome do espectáculo",
  "theater":    "CCB — Centro Cultural de Belém",
  "date_start": "2025-06-15",
  "source_url": "https://ccb.pt/evento/..."
}
```

Campos recomendados — ausência gera aviso, nunca rejeição:
```
date_end, synopsis, category, subcategory, image,
ticket_url, price_info, duration, age_rating, sessions
```

Filtros booleanos — `null` significa "desconhecido", não `false`:
```
is_free, is_accessible, for_families, for_schools, has_lsp, is_festival
```

Campos de metadados (preenchidos automaticamente):
```
scraped_at, schema_version, _stale, _stale_since, _meta.completeness
```

### `data/health.json`
Endpoint de monitorização. Pode ser pingado por sistemas externos para verificar se o pipeline correu correctamente.
```json
{
  "updated_at": "2025-03-15T07:30:00+00:00",
  "status": "ok",
  "total_events": 409,
  "scrapers_ok": 12,
  "scrapers_stale": 0,
  "scrapers_empty": 1
}
```

---

## Cache e resiliência

Se um scraper falhar (excepção ou 0 resultados), o orquestrador reutiliza os eventos anteriores desse teatro do `events.json` da run anterior, marcando-os como `_stale: true`. Eventos stale são descartados após `STALE_MAX_DAYS` dias (default: 120).

Cache por teatro: cada teatro tem um ficheiro `scrapers/cache/<id>.json` para cache granular (invalidar um teatro sem afectar os outros).

### Retry HTTP
A função `fetch_with_retry()` em `utils.py` implementa retry com backoff exponencial (1s, 2s, 4s) para erros transientes (timeout, 429, 503). Rate limiting por domínio via semáforo (`DOMAIN_MAX_CONCURRENT` pedidos simultâneos ao mesmo servidor).

---

## CI/CD

O workflow `scrape.yml` corre diariamente às 07:00 UTC com estes steps principais:

1. **Testes unitários** (`pytest tests/`) — falha aborta o pipeline
2. **Sync scrapers** — regista novos scrapers automaticamente
3. **Scraping** — `python scraper.py`
4. **Guarda de integridade** — aborta se total de eventos < `MIN_EVENTS_THRESHOLD`
5. **Build** — `python scripts/build.py` (gera `data/`, snapshot de arquivo)
6. **Commit** — `events.json` para `main`
7. **Deploy** — `_site/` para `gh-pages`
8. **Notificação** — ntfy.sh (se `NTFY_URL` secret configurado)

Logs guardados como artefact por 30 dias. Job separado `check-urls` para verificação semanal de URLs (activado via `workflow_dispatch`).

---

## Variáveis de ambiente e secrets

### Variáveis (Settings → Variables)
| Variável | Default | Descrição |
|---|---|---|
| `SCRAPER_WORKERS` | 4 | Scrapers em paralelo |
| `MIN_EVENTS_THRESHOLD` | 50 | Mínimo de eventos para não abortar |

### Secrets (Settings → Secrets)
| Secret | Obrigatório | Descrição |
|---|---|---|
| `NTFY_URL` | Não | URL ntfy.sh para notificações push |
| `GA4_PROPERTY_ID` | Não | Google Analytics 4 |
| `GOOGLE_SERVICE_ACCOUNT` | Não | Credenciais Google (JSON em base64) |
| `GSC_SITE_URL` | Não | Google Search Console |
| `META_PAGE_ACCESS_TOKEN` | Não | Meta (Facebook/Instagram) |
| `META_PAGE_ID` | Não | ID da página Facebook |
| `META_IG_USER_ID` | Não | ID do utilizador Instagram |

---

## Comandos úteis

```bash
# Correr pipeline completo localmente
make scrape

# Correr só um scraper (debug rápido)
make scrape-scraper ID=ccb

# Simular sem escrever ficheiros
make scrape-dry

# Ver log da última run
make logs-summary
make logs-errors

# Testes unitários
make test

# Smoke tests (com rede)
make smoke
make smoke-scraper ID=saoluiz

# Criar novo scraper
make new-scraper
```

---

## Decisões de design

**Por que JSON em vez de base de dados?**  
O site é estático (GitHub Pages). O `events.json` é o "estado da aplicação" — simples, versionado no Git, sem dependências externas. Os artefactos em `data/` são derivados deste ficheiro.

**Por que SHA1 para IDs?**  
`id = sha1(teatro + título + data)[:8]` — determinístico (mesma run gera o mesmo ID) e estável entre runs (permite detetar eventos repetidos). O prefixo `auto-` distingue IDs gerados automaticamente de IDs fornecidos pelo scraper.

**Por que `_stale` em vez de remover eventos de scrapers com falha?**  
Um teatro pode ter o site em baixo temporariamente. Remover eventos imediatamente degradaria a experiência do utilizador. O cache com prazo de validade dá margem para o site voltar ao normal antes de os eventos desaparecerem.

**Por que `null` em vez de `false` para filtros booleanos?**  
`is_free: false` significaria "confirmo que não é gratuito". `null` significa "não tenho informação". A distinção é importante para o frontend não mostrar incorrectamente "não gratuito" quando simplesmente não sabemos.
