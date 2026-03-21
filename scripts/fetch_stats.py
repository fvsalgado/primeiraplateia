"""
fetch_stats.py — corre no CI (GitHub Actions) e escreve data/stats.json
Requer os seguintes GitHub Secrets:
  GA4_PROPERTY_ID          ex: "properties/123456789"
  GOOGLE_SERVICE_ACCOUNT   JSON da service account (base64 ou raw)
  GSC_SITE_URL             ex: "https://www.primeiraplateia.pt/"
  META_PAGE_ACCESS_TOKEN   token de página do Facebook/Instagram
  META_PAGE_ID             ID da página Facebook
  META_IG_USER_ID          ID do utilizador Instagram (Business)
"""

import os, json, base64, datetime, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

TODAY = datetime.date.today()
DATE_28 = (TODAY - datetime.timedelta(days=28)).isoformat()
DATE_TODAY = TODAY.isoformat()
DATE_7 = (TODAY - datetime.timedelta(days=7)).isoformat()

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "stats.json"

stats = {
    "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
    "period": {"start": DATE_28, "end": DATE_TODAY},
    "ga4": {},
    "gsc": {},
    "instagram": {},
    "facebook": {},
}


# ── Google credentials ──────────────────────────────────────────────────────
def get_google_credentials():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")
    if not raw:
        log.warning("GOOGLE_SERVICE_ACCOUNT não definido")
        return None
    try:
        # tenta base64 primeiro, senão assume JSON raw
        try:
            decoded = base64.b64decode(raw).decode()
        except Exception:
            decoded = raw
        info = json.loads(decoded)
        from google.oauth2 import service_account
        scopes = [
            "https://www.googleapis.com/auth/analytics.readonly",
            "https://www.googleapis.com/auth/webmasters.readonly",
        ]
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    except Exception as e:
        log.error("Erro ao criar credenciais Google: %s", e)
        return None


# ── Google Analytics 4 ──────────────────────────────────────────────────────
def fetch_ga4(creds):
    if not creds:
        return
    prop = os.environ.get("GA4_PROPERTY_ID", "")
    if not prop:
        log.warning("GA4_PROPERTY_ID não definido")
        return
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest, OrderBy
        )
        client = BetaAnalyticsDataClient(credentials=creds)

        def run(dimensions, metrics, order=None, limit=10):
            req = RunReportRequest(
                property=prop,
                date_ranges=[DateRange(start_date=DATE_28, end_date=DATE_TODAY)],
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                limit=limit,
            )
            if order:
                req.order_bys = order
            return client.run_report(req)

        # Totais
        r = run([], ["sessions", "totalUsers", "screenPageViews", "bounceRate", "averageSessionDuration"])
        row = r.rows[0] if r.rows else None
        stats["ga4"] = {
            "sessions":    int(row.metric_values[0].value) if row else 0,
            "users":       int(row.metric_values[1].value) if row else 0,
            "pageviews":   int(row.metric_values[2].value) if row else 0,
            "bounce_rate": round(float(row.metric_values[3].value) * 100, 1) if row else 0,
            "avg_session": round(float(row.metric_values[4].value)),
        }

        # Páginas mais vistas
        r2 = run(["pagePath", "pageTitle"], ["screenPageViews"],
                 order=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
                 limit=8)
        stats["ga4"]["top_pages"] = [
            {"path": r.dimension_values[0].value,
             "title": r.dimension_values[1].value,
             "views": int(r.metric_values[0].value)} for r in r2.rows
        ]

        # Países
        r3 = run(["country"], ["sessions"],
                 order=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
                 limit=6)
        stats["ga4"]["countries"] = [
            {"country": r.dimension_values[0].value,
             "sessions": int(r.metric_values[0].value)} for r in r3.rows
        ]

        # Dispositivos
        r4 = run(["deviceCategory"], ["sessions"])
        stats["ga4"]["devices"] = {r.dimension_values[0].value: int(r.metric_values[0].value) for r in r4.rows}

        # Sessões por dia (últimos 28 dias)
        r5 = run(["date"], ["sessions"], limit=28)
        stats["ga4"]["sessions_by_day"] = sorted(
            [{"date": r.dimension_values[0].value, "sessions": int(r.metric_values[0].value)} for r in r5.rows],
            key=lambda x: x["date"]
        )

        log.info("GA4 OK: %s sessões", stats["ga4"].get("sessions"))
    except Exception as e:
        log.error("GA4 erro: %s", e)


# ── Google Search Console ───────────────────────────────────────────────────
def fetch_gsc(creds):
    if not creds:
        return
    site = os.environ.get("GSC_SITE_URL", "")
    if not site:
        log.warning("GSC_SITE_URL não definido")
        return
    try:
        from googleapiclient.discovery import build
        service = build("searchconsole", "v1", credentials=creds)

        def query(dimensions, rows=10, start=DATE_28):
            return service.searchanalytics().query(
                siteUrl=site,
                body={
                    "startDate": start, "endDate": DATE_TODAY,
                    "dimensions": dimensions, "rowLimit": rows,
                    "dataState": "final",
                }
            ).execute()

        # Totais
        r = query([])
        row = (r.get("rows") or [{}])[0]
        stats["gsc"] = {
            "clicks":      int(row.get("clicks", 0)),
            "impressions": int(row.get("impressions", 0)),
            "ctr":         round(row.get("ctr", 0) * 100, 1),
            "position":    round(row.get("position", 0), 1),
        }

        # Top queries
        r2 = query(["query"], rows=10)
        stats["gsc"]["top_queries"] = [
            {"query":       r["keys"][0],
             "clicks":      int(r.get("clicks", 0)),
             "impressions": int(r.get("impressions", 0)),
             "ctr":         round(r.get("ctr", 0) * 100, 1),
             "position":    round(r.get("position", 0), 1)} for r in (r2.get("rows") or [])
        ]

        # Top páginas
        r3 = query(["page"], rows=8)
        stats["gsc"]["top_pages"] = [
            {"page":        r["keys"][0],
             "clicks":      int(r.get("clicks", 0)),
             "impressions": int(r.get("impressions", 0))} for r in (r3.get("rows") or [])
        ]

        # Cliques por dia
        r4 = query(["date"], rows=28)
        stats["gsc"]["clicks_by_day"] = sorted(
            [{"date": r["keys"][0], "clicks": int(r.get("clicks", 0)), "impressions": int(r.get("impressions", 0))} for r in (r4.get("rows") or [])],
            key=lambda x: x["date"]
        )

        log.info("GSC OK: %s cliques, %s impressões", stats["gsc"].get("clicks"), stats["gsc"].get("impressions"))
    except Exception as e:
        log.error("GSC erro: %s", e)


# ── Instagram (Meta Graph API) ──────────────────────────────────────────────
def fetch_instagram():
    token = os.environ.get("META_PAGE_ACCESS_TOKEN", "")
    ig_id = os.environ.get("META_IG_USER_ID", "")
    if not token or not ig_id:
        log.warning("META_PAGE_ACCESS_TOKEN ou META_IG_USER_ID não definido")
        return
    try:
        import urllib.request, urllib.parse

        def get(path, params={}):
            params["access_token"] = token
            url = f"https://graph.facebook.com/v19.0{path}?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())

        # Conta básica
        acc = get(f"/{ig_id}", {"fields": "followers_count,media_count,name,username,biography"})
        stats["instagram"] = {
            "username":       acc.get("username", ""),
            "followers":      acc.get("followers_count", 0),
            "media_count":    acc.get("media_count", 0),
        }

        # Insights da conta (últimos 28 dias)
        ins = get(f"/{ig_id}/insights", {
            "metric": "impressions,reach,profile_views,website_clicks,follower_count",
            "period": "day",
            "since": int((TODAY - datetime.timedelta(days=28)).strftime("%s") if hasattr(TODAY, 'strftime') else 0),
        })
        # Agrega totais dos últimos 28 dias
        by_metric = {}
        for series in (ins.get("data") or []):
            name = series["name"]
            total = sum(v.get("value", 0) for v in series.get("values", []))
            by_metric[name] = total
        stats["instagram"]["impressions_28d"]    = by_metric.get("impressions", 0)
        stats["instagram"]["reach_28d"]          = by_metric.get("reach", 0)
        stats["instagram"]["profile_views_28d"]  = by_metric.get("profile_views", 0)
        stats["instagram"]["website_clicks_28d"] = by_metric.get("website_clicks", 0)

        # Posts recentes
        media = get(f"/{ig_id}/media", {
            "fields": "id,caption,media_type,timestamp,like_count,comments_count,thumbnail_url,media_url",
            "limit": "6"
        })
        stats["instagram"]["recent_posts"] = [
            {"id": m.get("id"), "type": m.get("media_type"),
             "caption": (m.get("caption") or "")[:80],
             "likes": m.get("like_count", 0), "comments": m.get("comments_count", 0),
             "date": m.get("timestamp", "")[:10]} for m in (media.get("data") or [])
        ]

        log.info("Instagram OK: %s seguidores", stats["instagram"].get("followers"))
    except Exception as e:
        log.error("Instagram erro: %s", e)


# ── Facebook (Meta Graph API) ───────────────────────────────────────────────
def fetch_facebook():
    token = os.environ.get("META_PAGE_ACCESS_TOKEN", "")
    page_id = os.environ.get("META_PAGE_ID", "")
    if not token or not page_id:
        log.warning("META_PAGE_ACCESS_TOKEN ou META_PAGE_ID não definido")
        return
    try:
        import urllib.request, urllib.parse

        def get(path, params={}):
            params["access_token"] = token
            url = f"https://graph.facebook.com/v19.0{path}?{urllib.parse.urlencode(params)}"
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())

        # Info básica
        page = get(f"/{page_id}", {"fields": "name,fan_count,followers_count,talking_about_count"})
        stats["facebook"] = {
            "name":            page.get("name", ""),
            "fans":            page.get("fan_count", 0),
            "followers":       page.get("followers_count", 0),
            "talking_about":   page.get("talking_about_count", 0),
        }

        # Page insights
        ins = get(f"/{page_id}/insights", {
            "metric": "page_impressions,page_reach,page_engaged_users,page_post_engagements",
            "period": "day",
            "since":  DATE_28, "until": DATE_TODAY,
        })
        by_metric = {}
        for series in (ins.get("data") or []):
            name = series["name"]
            total = sum(v.get("value", 0) for v in series.get("values", []))
            by_metric[name] = total
        stats["facebook"]["impressions_28d"]  = by_metric.get("page_impressions", 0)
        stats["facebook"]["reach_28d"]        = by_metric.get("page_reach", 0)
        stats["facebook"]["engaged_users_28d"]= by_metric.get("page_engaged_users", 0)
        stats["facebook"]["engagements_28d"]  = by_metric.get("page_post_engagements", 0)

        log.info("Facebook OK: %s fãs", stats["facebook"].get("fans"))
    except Exception as e:
        log.error("Facebook erro: %s", e)


# ── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    creds = get_google_credentials()
    fetch_ga4(creds)
    fetch_gsc(creds)
    fetch_instagram()
    fetch_facebook()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    log.info("Escrito em %s", OUTPUT_PATH)
