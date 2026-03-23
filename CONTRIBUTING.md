# Guia de Contribuição — Primeira Plateia

## Adicionar um novo teatro

O processo é maioritariamente automático. Segue estes passos:

### 1. Gerar o ficheiro de scaffolding

```bash
make new-scraper
# ou diretamente:
python scripts/new_scraper.py
```

O assistente pergunta o nome, ID, URL, cidade e coordenadas, e cria o ficheiro `scrapers/scraper_<id>.py` com toda a estrutura necessária.

Alternativamente, copia `scrapers/TEMPLATE.py` manualmente e renomeia para `scrapers/scraper_<id>.py`.

### 2. Implementar o scraper

Abre o ficheiro gerado e substitui os `# TODO` pelos seletores CSS correctos para o site do teatro.

Funções úteis disponíveis em `scrapers/utils.py`:

| Função | Descrição |
|---|---|
| `fetch_with_retry(session, url)` | GET com retry exponencial + rate limiting |
| `parse_date(text)` | "15 março 2025" → "2025-03-15" |
| `parse_date_range(text)` | "15 — 30 março" → ("2025-03-15", "2025-03-30") |
| `truncate_synopsis(text)` | Corta sinopse no limite sem quebrar frases |
| `build_sessions(date_start, date_end, schedule)` | Gera array `sessions[]` |
| `build_image_object(url, soup, theater, source_url)` | Cria dict `{url, credit, …}` |
| `HEADERS` | User-Agent e Accept-Language padrão |

### 3. Testar localmente

```bash
# Correr só o novo scraper
make scrape-scraper ID=scraper_<id>

# Smoke test (verifica campos obrigatórios, datas, etc.)
make smoke-scraper ID=scraper_<id>

# Ver o log da run
make logs-summary
```

Verificar:
- [ ] O scraper devolve pelo menos 1 evento
- [ ] Todos os eventos têm `title`, `theater`, `date_start`, `source_url`
- [ ] As datas estão no formato `YYYY-MM-DD`
- [ ] As sinopses não têm HTML residual
- [ ] As imagens têm URLs absolutas (começam com `http`)
- [ ] O `THEATER` dict tem `id`, `name`, `url`, `city`, `lat`, `lng`

### 4. O registo é automático

O `sync_scrapers.py` (que corre no CI antes do scraping) detecta automaticamente o novo módulo e:
- Adiciona o teatro a `theaters.json`
- Regista o import e a entrada em `SCRAPERS` no `scraper.py`

Não é necessário editar estes ficheiros manualmente.

### 5. Abrir Pull Request

O PR deve incluir apenas o ficheiro `scrapers/scraper_<id>.py`. O `theaters.json` e `scraper.py` são actualizados automaticamente pelo CI.

---

## Modificar um scraper existente

### Quando modificar

Um scraper precisa de atenção quando:
- O teatro redesenhou o site (os seletores CSS mudaram)
- Eventos estão a ser marcados como `stale` repetidamente
- O `validation_report.json` mostra erros consistentes para aquele teatro
- O log mostra `VAZIO` ou `STALE` para aquele teatro

### Como diagnosticar

```bash
# Ver o log da última run para um teatro específico
make logs-errors | grep -i "saoluiz"

# Correr só aquele scraper para ver o output
make scrape-scraper ID=saoluiz

# Ver resumo completo
make logs-summary
```

### Regras importantes ao modificar scrapers

1. **Não alterar o campo `THEATER["id"]`** — é a chave primária que liga ao `theaters.json` e ao cache. Alterá-lo cria um novo teatro "fantasma".

2. **Não alterar o nome do ficheiro** — `sync_scrapers.py` usa o nome do ficheiro como chave.

3. **Nunca lançar excepções para o orquestrador** — o `scraper.py` trata erros, mas uma excepção não capturada dentro do scraper pode interromper a thread. Usar `try/except` internamente e devolver lista vazia em caso de falha.

4. **Usar `fetch_with_retry()` em vez de `session.get()`** — garante retry automático e rate limiting correcto.

5. **Testar com `--dry-run`** antes de fazer push:
   ```bash
   make scrape-dry
   ```

---

## Estrutura de um evento válido

### Campos obrigatórios (rejeição se ausentes)
```python
{
    "id":         "scraper-slug-titulo",  # ou gerado automaticamente
    "title":      "Nome do espectáculo",
    "theater":    "Nome Canónico do Teatro",  # deve corresponder a theaters.json
    "date_start": "YYYY-MM-DD",
    "source_url": "https://...",
}
```

### Campos recomendados (aviso se ausentes)
```python
{
    "date_end":   "YYYY-MM-DD",
    "synopsis":   "Texto curto (máx. 300 chars)",
    "category":   "Artes Performativas",   # ver schema.py para vocabulário
    "subcategory": "Teatro",
    "image":      {"url": "https://...", "credit": None, "source": "...", "theater": "..."},
    "ticket_url": "https://...",
    "price_info": "10€ / 8€ (reduzido)",
    "sessions":   [{"date": "2025-03-15", "time": "21:00", "weekday": "Sáb"}],
    "duration":   "1h30",
    "age_rating": "M/6",
}
```

### Filtros booleanos
```python
{
    "is_free":      True,   # ou None se desconhecido
    "for_families": True,
    "is_festival":  None,   # null = não sabemos, não false
    "for_schools":  None,
    "has_lsp":      None,
    "is_accessible": None,
}
```

> **Importante:** `null`/`None` significa "informação desconhecida", não `false`. Só usar `True` ou `False` quando a informação está explícita no site.

---

## Vocabulário de categorias

Definido em `scrapers/schema.py`. Resumo:

| Categoria | Exemplos de subcategorias |
|---|---|
| Artes Performativas | Teatro, Dança, Ópera, Circo, Performance, Comédia |
| Música | Música Clássica, Jazz & Blues, Fado, Concerto |
| Cinema & Audiovisual | Cinema, Documentário, Curtas-Metragens |
| Artes Visuais & Exposições | Exposição, Instalação, Fotografia |
| Literatura & Palavra | Poesia, Apresentação de Livro, Conto & Narrativa |
| Pensamento & Conversa | Conferência, Debate, Conversa |
| Formação & Participação | Workshop, Masterclass, Curso |
| Infanto-Juvenil | Teatro Infantil, Para Bebés, Conto Infantil |
| Multidisciplinar | Festival, Ciclo |
| Comunidade & Território | Mediação Cultural |

O harmonizador normaliza automaticamente variantes (ex: "dança contemporânea" → categoria "Artes Performativas", subcategoria "Dança").

---

## Testes

```bash
# Testes unitários (rápidos, sem rede)
make test

# Smoke tests (lentos, precisam de rede)
make smoke

# Smoke test de um único scraper
make smoke-scraper ID=ccb
```

Os testes unitários em `tests/` cobrem `utils.py`, `harmonizer.py` e `validator.py`. Para cada novo scraper, é possível (mas não obrigatório) adicionar fixtures HTML em `tests/fixtures/<id>.html`.

---

## Checklist antes de fazer push

- [ ] `make test` passa sem erros
- [ ] `make scrape-scraper ID=<id>` devolve eventos
- [ ] `make smoke-scraper ID=<id>` passa
- [ ] `make logs-summary` não mostra erros inesperados
- [ ] O `THEATER` dict está preenchido com coordenadas correctas
- [ ] A sinopse não contém HTML residual
- [ ] As imagens têm URLs absolutas
