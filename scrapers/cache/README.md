# scrapers/cache/

Directoria de cache por teatro.

Cada ficheiro `<id>.json` contém os eventos em cache do respectivo espaço cultural,
permitindo invalidação granular (um teatro específico) sem afectar os outros.

## Formato

```json
{
  "theater": "CCB — Centro Cultural de Belém",
  "cached_at": "2025-03-15T07:30:00+00:00",
  "events": [ ... ]
}
```

## Regras

- Os ficheiros são geridos automaticamente pelo `scraper.py` — não editar manualmente.
- Para forçar re-scraping de um teatro específico, apagar o ficheiro `<id>.json`.
- Ficheiros de cache são ignorados pelo Git (ver `.gitignore`).
- A directoria em si está incluída no Git (via este README).
