"""Utilitários partilhados pelos scrapers."""
import re
import logging
import urllib.robotparser
from datetime import datetime, date

# ─────────────────────────────────────────────────────────────
# NOTA: logging.basicConfig() foi removido deste módulo.
# A configuração do logging (handlers, formato, ficheiro de log)
# é agora responsabilidade exclusiva do orquestrador (scraper.py).
# Desta forma evita-se conflito de configuração quando o módulo
# é importado antes do orquestrador ter configurado os seus handlers.
# ─────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    """
    Compatibilidade com scrapers que ainda usam log().
    Encaminha para o logger do módulo.
    Mantido para não partir os scrapers individuais antes de serem actualizados.
    """
    logger.info(msg)


def make_id(prefix: str, title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower().strip()).strip("-")[:50]
    return f"{prefix}-{slug}"


MONTHS = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "feb": 2, "apr": 4, "aug": 8, "sep": 9, "oct": 10, "dec": 12,
}


def parse_date(s: str, force_year: int | None = None) -> str:
    """
    Converte string de data para 'YYYY-MM-DD'.
    force_year: se fornecido, usa esse ano em vez de inferir.
    """
    if not s:
        return ""
    s = s.strip()
    # DD/MM/YYYY ou DD.MM.YYYY
    m = re.match(r"(\d{1,2})[/.](\d{1,2})[/.](\d{4})", s)
    if m:
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
        try:
            date(y, mo, d)
            return f"{y:04d}-{mo:02d}-{d:02d}"
        except ValueError:
            return ""
    # DD [de] MES [YYYY]
    m = re.match(
        r"(\d{1,2})\s+(?:de\s+)?([A-Za-zçãáéíóúàèìòùÇÃÁÉÍÓÚ]{3,})(?:\s+(\d{4}))?",
        s, re.IGNORECASE,
    )
    if m:
        d = int(m[1])
        mon = MONTHS.get(m[2].lower()) or MONTHS.get(m[2].lower()[:3])
        if not mon:
            return ""
        if m[3]:
            y = int(m[3])
        elif force_year:
            y = force_year
        else:
            now = datetime.now()
            y = now.year
            if mon < now.month or (mon == now.month and d < now.day):
                y = now.year + 1
        try:
            date(y, mon, d)
            return f"{y:04d}-{mon:02d}-{d:02d}"
        except ValueError:
            return ""
    return ""


def parse_date_range(s: str) -> tuple[str, str]:
    """Converte intervalo para (date_start, date_end). Aceita variados formatos."""
    if not s:
        return "", ""
    s = s.strip()
    parts = re.split(r"\s*[–—]\s*|\s+[aA]\s+", s, maxsplit=1)
    if len(parts) == 2:
        start_s, end_s = parts[0].strip(), parts[1].strip()
        date_end = parse_date(end_s)
        year_end = re.search(r"\d{4}", end_s)
        year_end_val = int(year_end.group()) if year_end else None
        if not year_end_val and date_end:
            year_end_val = int(date_end[:4])
        if re.match(r"^\d{2}\.\d{2}$", start_s) and year_end_val:
            start_s += f".{year_end_val}"
            date_start = parse_date(start_s)
        elif re.match(r"^\d{1,2}$", start_s):
            month_m = re.search(r"[A-Za-zçãáéíóúÇÃÁÉÍÓÚ]{3,}", end_s)
            if month_m:
                start_s = f"{start_s} {month_m.group()}"
                if year_end_val:
                    start_s += f" {year_end_val}"
            date_start = parse_date(start_s, force_year=year_end_val)
        elif not re.search(r"\d{4}", start_s) and year_end_val:
            date_start = parse_date(start_s, force_year=year_end_val)
        else:
            date_start = parse_date(start_s)
        return date_start, date_end
    d = parse_date(s)
    return d, d


# ─────────────────────────────────────────────────────────────
# CONFORMIDADE — funções de scraping ético
# ─────────────────────────────────────────────────────────────

# User-Agent e headers padrão para todos os scrapers
HEADERS = {
    "User-Agent": "PrimeiraPlateiaBot/1.0 (+https://www.primeiraplateia.pt)",
    "Accept-Language": "pt-PT,pt;q=0.9",
}


def truncate_synopsis(text: str, max_chars: int = 300) -> str:
    """
    Devolve excerto máximo de max_chars caracteres.
    Corta na última frase completa (., !, ?) antes do limite.
    Só corta em frase se resultado tiver mais de 150 chars.
    Adiciona '…' no final se truncado.
    """
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_sentence = max(
        truncated.rfind("."),
        truncated.rfind("!"),
        truncated.rfind("?"),
    )
    if last_sentence > 150:
        return truncated[:last_sentence + 1] + "…"
    return truncated + "…"


def build_image_object(
    url: str,
    page_soup,
    theater_name: str,
    source_url: str,
) -> dict | None:
    """
    Tenta extrair crédito fotográfico da página BeautifulSoup.
    Devolve dict {url, credit, source, theater} ou None se url vazio.
    """
    if not url:
        return None
    credit = None
    if page_soup:
        try:
            for fig in (page_soup.find_all("figure") or []):
                img_tag = fig.find("img")
                if img_tag and img_tag.get("src", "") == url:
                    cap = fig.find("figcaption")
                    if cap and cap.get_text(strip=True):
                        credit = cap.get_text(strip=True)[:120]
                        break
            if not credit:
                for img_tag in (page_soup.find_all("img") or []):
                    if img_tag.get("src", "") == url:
                        alt = (img_tag.get("alt") or "").strip()
                        if len(alt) >= 10:
                            credit = alt[:120]
                        break
            if not credit:
                page_text = page_soup.get_text(" ", strip=True)
                m = re.search(
                    r"(?:Foto:|Fotografia:|©\s*|Crédito:)\s*(.{5,80})",
                    page_text,
                    re.IGNORECASE,
                )
                if m:
                    credit = m.group(1).strip()[:80]
        except Exception:
            pass
    return {
        "url": url,
        "credit": credit,
        "source": source_url,
        "theater": theater_name,
    }


def can_scrape(base_url: str, path: str = "/") -> bool:
    """
    Verifica robots.txt para o user-agent PrimeiraPlateiaBot.
    Se inacessível, assume True e faz log.
    """
    rp = urllib.robotparser.RobotFileParser()
    try:
        robots_url = base_url.rstrip("/") + "/robots.txt"
        rp.set_url(robots_url)
        import socket
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(5)
        try:
            rp.read()
        finally:
            socket.setdefaulttimeout(old_timeout)
        return rp.can_fetch("PrimeiraPlateiaBot", base_url.rstrip("/") + path)
    except Exception as e:
        logger.warning(
            f"can_scrape: não foi possível verificar robots.txt "
            f"de {base_url} ({e}). A assumir permitido."
        )
        return True


# ─────────────────────────────────────────────────────────────
# SESSÕES — construção de sessions[] (Nível 2, Architecture §6)
# ─────────────────────────────────────────────────────────────

_WEEKDAYS_PT = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


def _weekday_pt(iso_date: str) -> str:
    """Devolve abreviatura PT do dia da semana para uma data ISO."""
    try:
        from datetime import date as _date
        d = _date.fromisoformat(iso_date)
        return _WEEKDAYS_PT[d.weekday()]
    except Exception:
        return ""


def build_sessions(
    date_start: str,
    date_end: str,
    schedule_text: str = "",
) -> list[dict]:
    """
    Constrói sessions[] a partir de date_start, date_end e texto de horário.

    Estratégia:
      1. Tenta extrair padrões de dia-da-semana + hora do schedule_text.
         Ex: "Qua a Sex 21h00 / Sáb e Dom 17h00"
      2. Se não conseguir, gera uma sessão por dia entre date_start e date_end
         com hora extraída do texto (ou sem hora).
      3. Limita a 90 sessões (evitar eventos com centenas de datas).

    Formato de saída (Architecture §6):
      [{"date": "YYYY-MM-DD", "time": "HH:MM", "weekday": "Qui"}, ...]
    """
    from datetime import date as _date, timedelta
    import re as _re

    if not date_start:
        return []

    try:
        ds = _date.fromisoformat(date_start)
    except ValueError:
        return []

    try:
        de = _date.fromisoformat(date_end) if date_end else ds
    except ValueError:
        de = ds

    # ── Extrair padrões "Weekday[s] HH:MM" do schedule_text ──
    # Ex: "Quarta a Sexta, 21h00 | Sábado e Domingo, 17h00"
    #     "Sex 21h | Sáb e Dom 17h00"

    _WDAY_MAP = {
        "seg": 0, "segunda": 0, "2ª": 0,
        "ter": 1, "terça": 1, "terca": 1, "3ª": 1,
        "qua": 2, "quarta": 2, "4ª": 2,
        "qui": 3, "quinta": 3, "5ª": 3,
        "sex": 4, "sexta": 4, "6ª": 4,
        "sáb": 5, "sab": 5, "sabado": 5, "sábado": 5,
        "dom": 6, "domingo": 6,
    }

    # Normalizar texto
    txt = (schedule_text or "").lower().strip()

    # Extrair slots: list of (set_of_weekday_ints, time_str)
    slots: list[tuple[set, str]] = []

    # Padrão: "dia [a|e] dia[, ]HHhMM" ou "dia[, ]HHhMM"
    seg_pat = _re.compile(
        r"((?:seg|ter|qua|qui|sex|s[aá]b|dom|segunda|terça|quarta|quinta|sexta|s[aá]bado|domingo)"
        r"(?:\s+(?:a|e|,|/)\s*(?:seg|ter|qua|qui|sex|s[aá]b|dom|segunda|terça|quarta|quinta|sexta|s[aá]bado|domingo))*)"
        r"[,\s]+(\d{1,2})[h:](\d{0,2})",
        _re.IGNORECASE,
    )

    for m in seg_pat.finditer(txt):
        days_str = m.group(1).lower()
        hh = int(m.group(2))
        mm = int(m.group(3)) if m.group(3) else 0
        time_str = f"{hh:02d}:{mm:02d}"

        # Expandir range: "qua a sex" → {2,3,4}
        parts = _re.split(r"\s+(?:a|e|,|/)\s*|\s*,\s*|\s*/\s*", days_str)
        found_days: set[int] = set()
        part_nums = []
        for p in parts:
            p = p.strip()
            for key, num in _WDAY_MAP.items():
                if p.startswith(key):
                    part_nums.append(num)
                    break

        if len(part_nums) >= 2:
            # Range: do primeiro ao último
            lo, hi = min(part_nums), max(part_nums)
            found_days = set(range(lo, hi + 1))
        elif len(part_nums) == 1:
            found_days = {part_nums[0]}

        if found_days:
            slots.append((found_days, time_str))

    # ── Se extraiu slots, gerar datas concretas ───────────────
    if slots:
        sessions = []
        cur = ds
        while cur <= de and len(sessions) < 90:
            wday = cur.weekday()
            for day_set, time_str in slots:
                if wday in day_set:
                    sessions.append({
                        "date":    cur.isoformat(),
                        "time":    time_str,
                        "weekday": _WEEKDAYS_PT[wday],
                    })
            cur += timedelta(days=1)
        return sessions

    # ── Fallback: tentar extrair hora única do texto ──────────
    time_str = ""
    hour_m = _re.search(r"(\d{1,2})[h:](\d{0,2})", txt)
    if hour_m:
        hh = int(hour_m.group(1))
        mm = int(hour_m.group(2)) if hour_m.group(2) else 0
        if 8 <= hh <= 23:
            time_str = f"{hh:02d}:{mm:02d}"

    # ── Se date_start == date_end, uma única sessão ───────────
    if ds == de:
        return [{"date": ds.isoformat(), "time": time_str, "weekday": _WEEKDAYS_PT[ds.weekday()]}]

    # ── Intervalo sem dias específicos: gerar todas as datas ──
    # Só se o intervalo for curto (≤ 14 dias) para não gerar centenas
    delta = (de - ds).days
    if delta <= 14:
        sessions = []
        cur = ds
        while cur <= de:
            sessions.append({
                "date":    cur.isoformat(),
                "time":    time_str,
                "weekday": _WEEKDAYS_PT[cur.weekday()],
            })
            cur += timedelta(days=1)
        return sessions

    # ── Intervalo longo sem info de dias: devolver só start/end ─
    result = [{"date": ds.isoformat(), "time": time_str, "weekday": _WEEKDAYS_PT[ds.weekday()]}]
    if de != ds:
        result.append({"date": de.isoformat(), "time": time_str, "weekday": _WEEKDAYS_PT[de.weekday()]})
    return result
