import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


TZ = ZoneInfo("Asia/Taipei")


@dataclass(frozen=True)
class Table:
    fields: list[str]
    rows: list[list[str]]


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; twse-scraper/1.0; +https://github.com/)",
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        }
    )
    return session


def _get_json(session: requests.Session, url: str, timeout_s: int = 30) -> dict:
    resp = session.get(url, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


def _get_html(session: requests.Session, url: str, timeout_s: int = 30) -> str:
    resp = session.get(url, timeout=timeout_s)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _parse_twse_json_table(payload: dict) -> Table:
    fields = payload.get("fields") or payload.get("fields1")
    data = payload.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        raise ValueError("Unexpected TWSE JSON shape")

    def norm_cell(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    return Table(fields=[str(f).strip() for f in fields], rows=[[norm_cell(c) for c in row] for row in data])


def _parse_html_first_table(html: str) -> Table:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        raise ValueError("No <table> found")

    thead = table.find("thead")
    if thead is None:
        raise ValueError("No <thead> found")

    header_rows = thead.find_all("tr")
    if not header_rows:
        raise ValueError("No header rows")

    header_cells = header_rows[-1].find_all(["th", "td"])
    fields = [c.get_text(strip=True) for c in header_cells]

    tbody = table.find("tbody")
    body_rows = tbody.find_all("tr") if tbody else []

    rows: list[list[str]] = []
    for tr in body_rows:
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)

    if not rows:
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [c.get_text(strip=True) for c in tds]
            if cells:
                rows.append(cells)

    return Table(fields=fields, rows=rows)


def fetch_bfi84u(session: requests.Session, base_date_yyyymmdd: str) -> tuple[Table, str]:
    candidates = [
        "https://www.twse.com.tw/rwd/zh/marginTrading/BFI84U?response=json",
        f"https://www.twse.com.tw/rwd/zh/marginTrading/BFI84U?response=json&date={base_date_yyyymmdd}",
        "https://www.twse.com.tw/exchangeReport/BFI84U?response=json",
        f"https://www.twse.com.tw/exchangeReport/BFI84U?response=json&date={base_date_yyyymmdd}",
    ]

    last_err: Exception | None = None
    for url in candidates:
        try:
            payload = _get_json(session, url)
            stat = str(payload.get("stat", "OK")).upper()
            if "OK" not in stat:
                raise ValueError(f"TWSE stat not OK: {payload.get('stat')}")
            table = _parse_twse_json_table(payload)
            if not table.rows:
                raise ValueError("No rows")
            return table, url
        except Exception as e:
            last_err = e

    url = "https://www.twse.com.tw/zh/trading/margin/bfi84u.html"
    try:
        html = _get_html(session, url)
        table = _parse_html_first_table(html)
        if not table.rows:
            raise ValueError("No rows")
        return table, url
    except Exception as e:
        raise RuntimeError(f"Failed to fetch BFI84U: {last_err or e}")


def fetch_twt93u(session: requests.Session, yyyymmdd: str) -> tuple[Table, str]:
    candidates = [
        f"https://www.twse.com.tw/rwd/zh/marginTrading/TWT93U?response=json&date={yyyymmdd}",
        f"https://www.twse.com.tw/exchangeReport/TWT93U?response=json&date={yyyymmdd}",
    ]

    last_err: Exception | None = None
    for url in candidates:
        try:
            payload = _get_json(session, url)
            stat = str(payload.get("stat", "OK")).upper()
            if "OK" not in stat:
                raise ValueError(f"TWSE stat not OK: {payload.get('stat')}")
            table = _parse_twse_json_table(payload)
            if not table.rows:
                raise ValueError("No rows")
            return table, url
        except Exception as e:
            last_err = e

    url = "https://www.twse.com.tw/zh/trading/margin/twt93u.html"
    try:
        html = _get_html(session, url)
        table = _parse_html_first_table(html)
        if not table.rows:
            raise ValueError("No rows")
        return table, url
    except Exception as e:
        raise RuntimeError(f"Failed to fetch TWT93U for {yyyymmdd}: {last_err or e}")


def _find_stock_code_col(fields: list[str]) -> int:
    for i, f in enumerate(fields):
        if re.search(r"代號|證券代號|股票代號", f):
            return i
    return 0


def _pick_index(fields: list[str], target: str, score_fn) -> int:
    candidates = [i for i, f in enumerate(fields) if target in f]
    if not candidates:
        raise ValueError(f"Missing column: {target}")
    scored = sorted(((score_fn(i), i) for i in candidates), reverse=True)
    return scored[0][1]


def _twt93u_indices(fields: list[str]) -> tuple[int, int, int]:
    code_idx = _find_stock_code_col(fields)

    def score_short(i: int) -> int:
        score = 0
        for back, pts in [(1, 2), (2, 1), (3, 1)]:
            if i - back >= 0 and "現券" in fields[i - back]:
                score += pts
        return score

    def score_borrow(i: int) -> int:
        score = 0
        if i + 1 < len(fields) and ("次一" in fields[i + 1] or "限額" in fields[i + 1]):
            score += 2
        if i - 1 >= 0 and "當日調整" in fields[i - 1]:
            score += 1
        return score

    short_idx = _pick_index(fields, "今日餘額", score_short)
    borrow_idx = _pick_index(fields, "當日餘額", score_borrow)
    return code_idx, short_idx, borrow_idx


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def compute_trading_dates(session: requests.Session, base: date, count: int = 5, max_lookback_days: int = 45):
    trading: list[date] = []
    tables: dict[str, tuple[Table, str]] = {}

    cursor = base
    for _ in range(max_lookback_days):
        ds = _yyyymmdd(cursor)
        try:
            table, url = fetch_twt93u(session, ds)
            trading.append(cursor)
            tables[ds] = (table, url)
            if len(trading) >= count:
                break
        except Exception:
            pass
        cursor = cursor - timedelta(days=1)

    if len(trading) < count:
        raise RuntimeError(f"Only found {len(trading)} trading days within lookback window")

    return trading, tables


def build_twt93u_maps(trading_dates: list[date], tables_by_date: dict[str, tuple[Table, str]]):
    maps: dict[str, dict[str, tuple[str, str]]] = {}
    source_urls: dict[str, str] = {}

    for d in trading_dates:
        ds = _yyyymmdd(d)
        table, url = tables_by_date[ds]
        source_urls[ds] = url

        code_idx, short_idx, borrow_idx = _twt93u_indices(table.fields)
        date_map: dict[str, tuple[str, str]] = {}

        for row in table.rows:
            if code_idx >= len(row):
                continue
            code = row[code_idx].strip()
            if not code:
                continue
            short_val = row[short_idx].strip() if short_idx < len(row) else ""
            borrow_val = row[borrow_idx].strip() if borrow_idx < len(row) else ""
            date_map[code] = (short_val, borrow_val)

        maps[ds] = date_map

    return maps, source_urls


def main() -> int:
    now = datetime.now(TZ)
    base_date = now.date()
    base_date_iso = base_date.isoformat()
    base_date_yyyymmdd = _yyyymmdd(base_date)

    session = _session()

    print(f"Base date (Taipei): {base_date_iso}")
    print("Finding last 5 trading dates via TWT93U...")
    trading_dates, tables_by_date = compute_trading_dates(session, base_date, count=5)

    trading_dates_iso = [d.isoformat() for d in trading_dates]
    labels = [f"D{-i}" if i else "D0" for i in range(len(trading_dates))]

    twt_maps, twt_urls = build_twt93u_maps(trading_dates, tables_by_date)

    print("Fetching BFI84U...")
    bfi_table, bfi_url = fetch_bfi84u(session, base_date_yyyymmdd)

    code_idx = _find_stock_code_col(bfi_table.fields)

    appended_fields: list[str] = []
    for label in labels:
        appended_fields.append(f"融券_今日餘額_{label}")
        appended_fields.append(f"借券_當日餘額_{label}")

    combined_fields = list(bfi_table.fields) + appended_fields
    combined_rows: list[list[str]] = []

    for row in bfi_table.rows:
        code = row[code_idx].strip() if code_idx < len(row) else ""
        extras: list[str] = []
        for d in trading_dates:
            ds = _yyyymmdd(d)
            short_val, borrow_val = ("", "")
            found = twt_maps.get(ds, {}).get(code)
            if found:
                short_val, borrow_val = found
            extras.extend([short_val or "—", borrow_val or "—"])
        combined_rows.append(list(row) + extras)

    out_dir = os.path.join("docs", "data")
    os.makedirs(out_dir, exist_ok=True)

    for name in os.listdir(out_dir):
        if re.match(r"^latest-\d{4}-\d{2}-\d{2}\.csv$", name):
            os.remove(os.path.join(out_dir, name))

    csv_name = f"latest-{base_date_iso}.csv"
    csv_path = os.path.join(out_dir, csv_name)
    json_path = os.path.join(out_dir, "latest.json")

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(combined_fields)
        writer.writerows(combined_rows)

    payload = {
        "base_date": base_date_iso,
        "generated_at": now.isoformat(),
        "trading_dates": trading_dates_iso,
        "labels": labels,
        "source": {"bfi84u": bfi_url, "twt93u_by_date": twt_urls},
        "csv": {"file": csv_name},
        "fields": combined_fields,
        "rows": combined_rows,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(f"Wrote {json_path}")
    print(f"Wrote {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())