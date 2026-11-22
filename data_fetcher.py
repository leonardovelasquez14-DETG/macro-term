"""
data_fetcher.py — Real-time market & macroeconomic data engine for MACRO-TERM v2.4.

Data sources (100% free, zero API keys required):
  - Yahoo Finance (yfinance): Intraday & historical OHLCV for Equities, Volatility,
    Rates, FX, and Commodities.
  - FRED (Federal Reserve Economic Data) public CSV endpoints:
    CPIAUCSL (US CPI), PCEPILFE (Core PCE), FEDFUNDS (Fed Funds Rate),
    DGS10 / DGS2 / T10Y2Y (US Treasury Curve), BAMLEMCBPIOAS (EM Credit OAS).
  - BCRP (Banco Central de Reserva del Perú) public JSON API:
    EMBI+ Perú (PD04709XD) & EMBI+ LatAm (PD04710XD) in basis points.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import io
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd
import requests
import yfinance as yf


# ---------------------------------------------------------------------------
# Lightweight Thread-Safe In-Memory TTL Cache (No external dependencies)
# ---------------------------------------------------------------------------
_CACHES: list[dict[Any, tuple[float, Any]]] = []
_CACHE_LOCK = threading.Lock()


def _ttl_cache(ttl_seconds: int):
    store: dict[Any, tuple[float, Any]] = {}
    _CACHES.append(store)

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.monotonic()
            with _CACHE_LOCK:
                entry = store.get(key)
                if entry is not None and (now - entry[0]) < ttl_seconds:
                    return entry[1]
            result = fn(*args, **kwargs)
            with _CACHE_LOCK:
                store[key] = (time.monotonic(), result)
            return result

        return wrapper

    return decorator


def clear_all_caches() -> None:
    """Clear all in-memory market & macro TTL caches."""
    with _CACHE_LOCK:
        for store in _CACHES:
            store.clear()


_cache_market = _ttl_cache(25)
_cache_macro = _ttl_cache(3600)


# ---------------------------------------------------------------------------
# Asset Universe Definition
# ---------------------------------------------------------------------------
ASSET_CATALOG: dict[str, list[dict[str, Any]]] = {
    "1. RENTA VARIABLE & VOLATILIDAD": [
        {
            "ticker": "^GSPC", "short": "SPX", "name": "S&P 500 Index", "unit": "pts", "decimals": 2,
            "desc": "500 corporaciones líderes de EE. UU. Termómetro principal de apetito por riesgo global.",
            "sensitivity": "TASAS REALES, GANANCIAS CORPORATIVAS, LIQUIDEZ FED",
        },
        {
            "ticker": "^VIX", "short": "VIX", "name": "CBOE Volatility Index", "unit": "pts", "decimals": 2,
            "desc": "Volatilidad implícita a 30 días en opciones del S&P 500. >25 indica estrés de mercado.",
            "sensitivity": "SHOCKS GEOPOLÍTICOS, SORPRESAS INFLACIONARIAS",
        },
        {
            "ticker": "EEM", "short": "MSCI-EM", "name": "MSCI Emerging Markets ETF", "unit": "$", "decimals": 2,
            "desc": "Renta variable de economías emergentes (China, India, Brasil, México, Perú).",
            "sensitivity": "ÍNDICE DÓLAR (DXY), CRECIMIENTO ASIA, COMMODITIES",
        },
        {
            "ticker": "^GDAXI", "short": "DAX", "name": "DAX Performance (Europa)", "unit": "pts", "decimals": 2,
            "desc": "Principal índice bursátil de Alemania y termómetro industrial de la Eurozona.",
            "sensitivity": "DECISIONES BCE, COSTOS ENERGÉTICOS, PMI EUROZONA",
        },
        {
            "ticker": "^HSI", "short": "HSI", "name": "Hang Seng Index (China/HK)", "unit": "pts", "decimals": 2,
            "desc": "Índice bursátil de Hong Kong con alta exposición a tecnológicas y bancos chinos.",
            "sensitivity": "ESTÍMULOS PBOC, SECTOR INMOBILIARIO, PMI CHINA",
        },
    ],
    "2. TASAS Y DIVISAS": [
        {
            "ticker": "^TNX", "short": "US10Y", "name": "Bono Tesoro EEUU 10 Años", "unit": "%", "decimals": 3,
            "desc": "Rendimiento soberano a 10 años de EE. UU. Tasa libre de riesgo de referencia mundial.",
            "sensitivity": "POLÍTICA MONETARIA FED, CORE PCE, CPI, NFP",
        },
        {
            "ticker": "2YY=F", "short": "US2Y", "name": "Bono Tesoro EEUU 2 Años", "unit": "%", "decimals": 3,
            "desc": "Rendimiento soberano a 2 años. Refleja directamente las expectativas de tasas de la Fed.",
            "sensitivity": "PROYECCIONES FOMC, EMPLEO EEUU, INFLACIÓN SUBYACENTE",
        },
        {
            "ticker": "^IRX", "short": "US3M", "name": "Letras Tesoro EEUU 13 Sem.", "unit": "%", "decimals": 3,
            "desc": "Tasa corta T-Bill a 3 meses, anclada a la tasa efectiva de fondos federales.",
            "sensitivity": "TASA FED FUNDS, LIQUIDEZ INTERBANCARIA",
        },
        {
            "ticker": "DX-Y.NYB", "short": "DXY", "name": "Índice Dólar EEUU (DXY)", "unit": "pts", "decimals": 2,
            "desc": "Fortaleza del dólar frente a la canasta de 6 divisas principales (EUR, JPY, GBP...).",
            "sensitivity": "DIFERENCIAL DE TASAS EEUU VS RESTO DEL MUNDO",
        },
        {
            "ticker": "PEN=X", "short": "USD/PEN", "name": "Dólar / Sol Peruano", "unit": "PEN", "decimals": 4,
            "desc": "Tipo de cambio interbancario USD/PEN. Sensible al cobre, DXY y flujo de capitales.",
            "sensitivity": "PRECIO DEL COBRE, DXY, INTERVENCIÓN BCRP",
        },
        {
            "ticker": "EURUSD=X", "short": "EUR/USD", "name": "Euro / Dólar EEUU", "unit": "$", "decimals": 4,
            "desc": "Par de divisas más líquido del mundo; mide divergencia monetaria BCE vs Fed.",
            "sensitivity": "DIFERENCIAL DE TASAS BCE-FED, INFLACIÓN EUROZONA",
        },
    ],
    "3. MATERIAS PRIMAS": [
        {
            "ticker": "HG=F", "short": "COPPER", "name": "Cobre COMEX (Dr. Copper)", "unit": "$", "decimals": 4,
            "desc": '"Dr. Copper": Barómetro industrial global y principal exportación de Perú y Chile.',
            "sensitivity": "DEMANDA INDUSTRIAL CHINA, TRANSICIÓN ENERGÉTICA",
        },
        {
            "ticker": "CL=F", "short": "WTI", "name": "Petróleo WTI (USD/bbl)", "unit": "$", "decimals": 2,
            "desc": "Crudo West Texas Intermediate. Referente clave de energía e inflación global.",
            "sensitivity": "CUOTAS OPEP+, TENSIONES MEDIO ORIENTE, INVENTARIOS",
        },
        {
            "ticker": "BZ=F", "short": "BRENT", "name": "Petróleo Brent (USD/bbl)", "unit": "$", "decimals": 2,
            "desc": "Crudo de referencia internacional (Mar del Norte) para comercio marítimo global.",
            "sensitivity": "OFERTA OPEP+, GEOPOLÍTICA, DEMANDA GLOBAL",
        },
        {
            "ticker": "GC=F", "short": "GOLD", "name": "Oro Futuros (COMEX USD/oz)", "unit": "$", "decimals": 2,
            "desc": "Activo refugio monetario frente a incertidumbre geopolítica y tasas reales bajas.",
            "sensitivity": "TASAS REALES EEUU, COMPRAS BANCOS CENTRALES, DXY",
        },
    ],
}

ALL_ASSETS: dict[str, dict[str, Any]] = {
    item["ticker"]: {**item, "category": cat}
    for cat, items in ASSET_CATALOG.items()
    for item in items
}

TIMEFRAME_MAP: dict[str, dict[str, str]] = {
    "1D": {"label": "1D - 1m", "period": "1d", "interval": "1m"},
    "5D": {"label": "5D - 15m", "period": "5d", "interval": "15m"},
    "1M": {"label": "1M - 1d", "period": "1mo", "interval": "1d"},
    "1Y": {"label": "1Y - 1d", "period": "1y", "interval": "1d"},
}

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}


# ---------------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------------
def _extract_ticker_df(raw: pd.DataFrame, ticker: str, single: bool = False) -> pd.DataFrame:
    """Safely extract a single-ticker OHLCV DataFrame from yf.download output."""
    if raw is None or raw.empty:
        return pd.DataFrame()
    if single or not isinstance(raw.columns, pd.MultiIndex):
        df = raw.copy()
    else:
        level0 = raw.columns.get_level_values(0)
        level1 = raw.columns.get_level_values(1)
        if ticker in level0:
            df = raw[ticker].copy()
        elif ticker in level1:
            df = raw.xs(ticker, axis=1, level=1).copy()
        else:
            return pd.DataFrame()

    # Flatten any residual MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["Close"])
    return df


def _is_market_closed(last_ts: pd.Timestamp | None) -> bool:
    """Determine if the latest bar is stale (> 25 min old) indicating closed market."""
    if last_ts is None:
        return True
    try:
        ts_utc = last_ts.tz_convert("UTC") if last_ts.tzinfo is not None else last_ts.tz_localize("UTC")
    except Exception:
        ts_utc = pd.Timestamp(last_ts, tz="UTC")
    now_utc = pd.Timestamp.now(tz="UTC")
    age_minutes = (now_utc - ts_utc).total_seconds() / 60.0
    return age_minutes > 25.0


def _to_utc_str(ts: pd.Timestamp | None) -> str:
    if ts is None:
        return "N/A"
    try:
        ts_utc = ts.tz_convert("UTC") if ts.tzinfo is not None else ts.tz_localize("UTC")
        return ts_utc.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)[:16]


# ---------------------------------------------------------------------------
# Watchlist Snapshot Fetcher
# ---------------------------------------------------------------------------
@_cache_market
def fetch_watchlist_snapshot() -> dict[str, Any]:
    """
    Fetch latest intraday quotes, previous closes, day highs/lows, and sparklines
    for all configured assets in parallel via yfinance.
    """
    tickers = list(ALL_ASSETS.keys())
    logs: list[str] = []
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    logs.append(f"[{now_str}] INIT YFINANCE BATCH SYNC ({len(tickers)} INSTRUMENTS)...")

    intraday_raw = pd.DataFrame()
    daily_raw = pd.DataFrame()

    try:
        intraday_raw = yf.download(
            tickers=tickers,
            period="5d",
            interval="15m",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        logs.append(f"[{now_str}] YFINANCE INTRADAY (5d/15m): OK")
    except Exception as exc:
        logs.append(f"[{now_str}] YFINANCE INTRADAY ERR: {exc}")

    try:
        daily_raw = yf.download(
            tickers=tickers,
            period="10d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        logs.append(f"[{now_str}] YFINANCE DAILY REF (10d/1d): OK")
    except Exception as exc:
        logs.append(f"[{now_str}] YFINANCE DAILY ERR: {exc}")

    quotes: dict[str, dict[str, Any]] = {}
    ok_count = 0
    closed_count = 0

    for ticker, meta in ALL_ASSETS.items():
        intra_df = _extract_ticker_df(intraday_raw, ticker)
        daily_df = _extract_ticker_df(daily_raw, ticker)

        # Fallback to individual Ticker.history if batch missed it
        if intra_df.empty and daily_df.empty:
            try:
                t_obj = yf.Ticker(ticker)
                daily_df = t_obj.history(period="10d", interval="1d").dropna(subset=["Close"])
            except Exception:
                pass

        if intra_df.empty and daily_df.empty:
            quotes[ticker] = {
                **meta,
                "price": None,
                "prev_close": None,
                "change": None,
                "change_pct": None,
                "day_high": None,
                "day_low": None,
                "sparkline": [],
                "last_ts": "N/A",
                "market_closed": True,
                "status": "OFFLINE",
            }
            continue

        # Determine latest price and timestamp
        if not intra_df.empty:
            last_price = float(intra_df["Close"].iloc[-1])
            last_ts = intra_df.index[-1]
            market_closed = _is_market_closed(last_ts)

            # Restrict day high/low to the last available calendar date in intraday data
            last_date = intra_df.index[-1].date()
            session_df = intra_df[intra_df.index.date == last_date]
            day_high = float(session_df["High"].max()) if "High" in session_df.columns and not session_df.empty else last_price
            day_low = float(session_df["Low"].min()) if "Low" in session_df.columns and not session_df.empty else last_price

            # Sparkline: last 24 intraday points
            spark = [float(v) for v in intra_df["Close"].tail(24).tolist()]
        else:
            last_price = float(daily_df["Close"].iloc[-1])
            last_ts = daily_df.index[-1]
            market_closed = True
            day_high = float(daily_df["High"].iloc[-1]) if "High" in daily_df.columns else last_price
            day_low = float(daily_df["Low"].iloc[-1]) if "Low" in daily_df.columns else last_price
            spark = [float(v) for v in daily_df["Close"].tail(10).tolist()]

        # Determine accurate previous trading session close
        prev_close = None
        if not daily_df.empty:
            if len(daily_df) >= 2:
                # If last daily row matches last intraday date, prev_close is iloc[-2]
                if not intra_df.empty and daily_df.index[-1].date() == intra_df.index[-1].date():
                    prev_close = float(daily_df["Close"].iloc[-2])
                elif intra_df.empty:
                    prev_close = float(daily_df["Close"].iloc[-2])
                else:
                    # Intraday is a newer session than daily_df[-1]
                    prev_close = float(daily_df["Close"].iloc[-1])
            else:
                prev_close = float(daily_df["Close"].iloc[-1])
        elif not intra_df.empty:
            prev_close = float(intra_df["Close"].iloc[0])

        if prev_close and prev_close != 0:
            change = last_price - prev_close
            change_pct = (change / prev_close) * 100.0
        else:
            change = 0.0
            change_pct = 0.0

        ok_count += 1
        if market_closed:
            closed_count += 1

        quotes[ticker] = {
            **meta,
            "price": last_price,
            "prev_close": prev_close,
            "change": change,
            "change_pct": change_pct,
            "day_high": day_high,
            "day_low": day_low,
            "sparkline": spark,
            "last_ts": _to_utc_str(last_ts),
            "market_closed": market_closed,
            "status": "OK",
        }

    # Build volatility alerts for assets moving >= 1.5%
    alerts: list[dict[str, Any]] = []
    for ticker, q in quotes.items():
        if q["change_pct"] is not None and abs(q["change_pct"]) >= 1.5:
            alerts.append({
                "ticker": ticker,
                "short": q["short"],
                "name": q["name"],
                "change_pct": q["change_pct"],
                "price": q["price"],
                "decimals": q["decimals"],
            })
    alerts.sort(key=lambda x: abs(x["change_pct"]), reverse=True)

    all_closed = ok_count > 0 and closed_count == ok_count
    logs.append(
        f"[{now_str}] SYNC COMPLETE: {ok_count}/{len(tickers)} ACTIVE | "
        f"SESSION: {'CLOSED / LAST CLOSE' if all_closed else 'LIVE / MIXED'}"
    )

    return {
        "quotes": quotes,
        "alerts": alerts,
        "logs": logs,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "all_closed": all_closed,
    }


# ---------------------------------------------------------------------------
# Detailed Chart Data Fetcher
# ---------------------------------------------------------------------------
@_cache_market
def fetch_chart_data(ticker: str, timeframe_key: str = "1D") -> dict[str, Any]:
    """
    Fetch OHLCV series for the main interactive retro chart.
    Gracefully falls back to last available trading session when market is closed.
    """
    cfg = TIMEFRAME_MAP.get(timeframe_key, TIMEFRAME_MAP["1D"])
    period = cfg["period"]
    interval = cfg["interval"]
    effective_label = cfg["label"]
    note = ""

    df = pd.DataFrame()
    prev_close = None

    try:
        t_obj = yf.Ticker(ticker)
        df = t_obj.history(period=period, interval=interval, auto_adjust=False)
        if df is not None and not df.empty:
            df = df.dropna(subset=["Close"])

        # Fallback for 1D when market is closed (weekend / pre-market / holiday)
        if (df is None or df.empty or len(df) < 3) and timeframe_key == "1D":
            fallback = t_obj.history(period="5d", interval="5m", auto_adjust=False)
            if fallback is not None and not fallback.empty:
                fallback = fallback.dropna(subset=["Close"])
                last_day = fallback.index[-1].date()
                df = fallback[fallback.index.date == last_day].copy()
                effective_label = "LAST SESSION - 5m"
                note = "MARKET CLOSED / SHOWING LAST TRADING SESSION"

        # Fetch daily reference for accurate previous close line
        daily = t_obj.history(period="10d", interval="1d", auto_adjust=False)
        if daily is not None and not daily.empty:
            daily = daily.dropna(subset=["Close"])
            if len(daily) >= 2:
                if timeframe_key in ("1D", "5D"):
                    prev_close = float(daily["Close"].iloc[-2])
                else:
                    prev_close = float(df["Close"].iloc[0]) if not df.empty else float(daily["Close"].iloc[-2])
            elif len(daily) == 1:
                prev_close = float(daily["Close"].iloc[0])
    except Exception as exc:
        note = f"DATA FETCH WARNING: {exc}"

    if df is None or df.empty:
        return {
            "df": pd.DataFrame(),
            "prev_close": prev_close,
            "market_closed": True,
            "effective_label": effective_label,
            "note": note or "NO DATA AVAILABLE FOR THIS INSTRUMENT",
        }

    market_closed = _is_market_closed(df.index[-1])
    if market_closed and not note:
        note = "MARKET CLOSED / LAST CLOSE"

    # Normalize index to timezone-naive UTC timestamps for clean Plotly rendering
    plot_df = df.copy()
    if plot_df.index.tz is not None:
        plot_df.index = plot_df.index.tz_convert("UTC").tz_localize(None)

    return {
        "df": plot_df,
        "prev_close": prev_close,
        "market_closed": market_closed,
        "effective_label": effective_label,
        "note": note,
    }


# ---------------------------------------------------------------------------
# Macroeconomic & Sovereign Risk Data (FRED Public CSV + BCRP Public JSON)
# ---------------------------------------------------------------------------
def _fetch_fred_series(
    series_id: str,
    lookback_days: int = 450,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Download a public FRED series as CSV without requiring an API key."""
    start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start_date}"
    client = session or requests
    resp = client.get(url, headers=HTTP_HEADERS, timeout=10)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    if df.shape[1] < 2:
        return pd.DataFrame()
    date_col, val_col = df.columns[0], df.columns[1]
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    df = df.dropna(subset=[val_col]).reset_index(drop=True)
    df.columns = ["date", "value"]
    return df


def _fetch_bcrp_embi(series_code: str, session: requests.Session | None = None) -> tuple[float | None, str]:
    """Fetch latest EMBI+ spread (bps) from BCRP public statistics API."""
    url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{series_code}/json"
    client = session or requests
    resp = client.get(url, headers=HTTP_HEADERS, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    periods = data.get("periods", [])
    for entry in reversed(periods):
        vals = entry.get("values", [])
        if vals and vals[0] not in (None, "", "n.d."):
            try:
                return float(vals[0]), str(entry.get("name", "N/A"))
            except ValueError:
                continue
    return None, "N/A"


def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs), None
    except Exception as exc:
        return None, str(exc)


def _fetch_all_fred() -> dict[str, tuple[pd.DataFrame | None, str | None]]:
    """Fetch FRED series sequentially over one keep-alive session (avoids FRED burst throttling)."""
    fred_specs = [
        ("CPIAUCSL", 500),
        ("PCEPILFE", 500),
        ("DFEDTARU", 120),
        ("DGS10", 60),
        ("DGS2", 60),
        ("IPMAN", 500),
        ("CHNCPIALLMINMEI", 1200),
    ]
    results: dict[str, tuple[pd.DataFrame | None, str | None]] = {}
    with requests.Session() as sess:
        for sid, days in fred_specs:
            results[sid] = _safe_call(_fetch_fred_series, sid, days, sess)
    return results


def _fetch_all_bcrp() -> dict[str, tuple[Any, str | None]]:
    """Fetch BCRP EMBI+ sovereign risk series over one keep-alive session."""
    results: dict[str, tuple[Any, str | None]] = {}
    with requests.Session() as sess:
        results["PD04709XD"] = _safe_call(_fetch_bcrp_embi, "PD04709XD", sess)
        results["PD04710XD"] = _safe_call(_fetch_bcrp_embi, "PD04710XD", sess)
    return results


@_cache_macro
def fetch_macro_indicators() -> dict[str, Any]:
    """
    Fetch key global macro indicators from free public endpoints (FRED CSV & BCRP JSON):
      - US CPI YoY (%)
      - US Core PCE YoY (%)
      - US Fed Funds Target (%)
      - US 10Y & 2Y Treasury Yields + 10Y-2Y Spread (FRED DGS10 / DGS2)
      - US Manufacturing Output (YoY) & China CPI (YoY)
      - EMBI+ Country Risk (LatAm & Peru in bps, with FRED EM OAS fallback)
    """
    logs: list[str] = []
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    indicators: list[dict[str, Any]] = []
    curve_data: dict[str, Any] = {
        "us10y": None,
        "us2y": None,
        "spread_10y_2y": None,
        "date": "N/A",
        "source": "FRED",
    }

    # Run FRED session and BCRP session concurrently (2 threads = fast & rate-limit safe)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_fred = pool.submit(_fetch_all_fred)
        f_bcrp = pool.submit(_fetch_all_bcrp)
        fred_res = f_fred.result()
        bcrp_res = f_bcrp.result()

    # 1. US CPI YoY
    cpi_df, err = fred_res["CPIAUCSL"]
    if cpi_df is not None and len(cpi_df) >= 13:
        yoy = ((float(cpi_df["value"].iloc[-1]) / float(cpi_df["value"].iloc[-13])) - 1.0) * 100.0
        indicators.append({
            "code": "US_CPI_YOY", "label": "US CPI (YoY)", "value": f"{yoy:.2f}%",
            "raw": yoy, "date": str(cpi_df["date"].iloc[-1]), "source": "FRED/BLS", "status": "OK",
        })
        logs.append(f"[{now_str}] FRED CPIAUCSL: OK ({yoy:.2f}%)")
    else:
        logs.append(f"[{now_str}] FRED CPIAUCSL ERR: {err or 'INSUFFICIENT DATA'}")
        indicators.append({
            "code": "US_CPI_YOY", "label": "US CPI (YoY)", "value": "N/A",
            "raw": None, "date": "OFFLINE", "source": "FRED/BLS", "status": "ERR",
        })

    # 2. US Core PCE YoY
    pce_df, err = fred_res["PCEPILFE"]
    if pce_df is not None and len(pce_df) >= 13:
        yoy = ((float(pce_df["value"].iloc[-1]) / float(pce_df["value"].iloc[-13])) - 1.0) * 100.0
        indicators.append({
            "code": "US_CORE_PCE", "label": "US Core PCE (YoY)", "value": f"{yoy:.2f}%",
            "raw": yoy, "date": str(pce_df["date"].iloc[-1]), "source": "FRED/BEA", "status": "OK",
        })
        logs.append(f"[{now_str}] FRED PCEPILFE: OK ({yoy:.2f}%)")
    else:
        logs.append(f"[{now_str}] FRED PCEPILFE ERR: {err or 'INSUFFICIENT DATA'}")
        indicators.append({
            "code": "US_CORE_PCE", "label": "US Core PCE (YoY)", "value": "N/A",
            "raw": None, "date": "OFFLINE", "source": "FRED/BEA", "status": "ERR",
        })

    # 3. Fed Funds Rate
    ff_df, err = fred_res["DFEDTARU"]
    if ff_df is not None and not ff_df.empty:
        val = float(ff_df["value"].iloc[-1])
        indicators.append({
            "code": "FED_FUNDS", "label": "Fed Funds Target", "value": f"{val:.2f}%",
            "raw": val, "date": str(ff_df["date"].iloc[-1]), "source": "FRED/FED", "status": "OK",
        })
        logs.append(f"[{now_str}] FRED DFEDTARU: OK ({val:.2f}%)")
    else:
        logs.append(f"[{now_str}] FRED DFEDTARU ERR: {err or 'EMPTY'}")
        indicators.append({
            "code": "FED_FUNDS", "label": "Fed Funds Target", "value": "N/A",
            "raw": None, "date": "OFFLINE", "source": "FRED/FED", "status": "ERR",
        })

    # 4. US Treasury 10Y & 2Y Curve (FRED DGS10 / DGS2)
    dgs10, err10 = fred_res["DGS10"]
    dgs2, err2 = fred_res["DGS2"]
    if dgs10 is not None and not dgs10.empty and dgs2 is not None and not dgs2.empty:
        y10 = float(dgs10["value"].iloc[-1])
        y2 = float(dgs2["value"].iloc[-1])
        spread = y10 - y2
        curve_data = {
            "us10y": y10, "us2y": y2, "spread_10y_2y": spread,
            "date": str(dgs10["date"].iloc[-1]), "source": "FRED DGS10/DGS2",
        }
        logs.append(f"[{now_str}] FRED CURVE: 10Y={y10:.2f}% 2Y={y2:.2f}% SPREAD={spread:+.2f}%")
    else:
        logs.append(f"[{now_str}] FRED CURVE ERR: {err10 or err2 or 'EMPTY'}")

    # 5. US Manufacturing Activity (FRED: IPMAN YoY)
    ip_df, err = fred_res["IPMAN"]
    if ip_df is not None and len(ip_df) >= 13:
        yoy = ((float(ip_df["value"].iloc[-1]) / float(ip_df["value"].iloc[-13])) - 1.0) * 100.0
        indicators.append({
            "code": "US_MFG_IP", "label": "US Mfg Output (YoY)", "value": f"{yoy:+.2f}%",
            "raw": yoy, "date": str(ip_df["date"].iloc[-1]), "source": "FRED/IPMAN", "status": "OK",
        })
        logs.append(f"[{now_str}] FRED IPMAN: OK ({yoy:+.2f}%)")
    else:
        logs.append(f"[{now_str}] FRED IPMAN ERR: {err or 'INSUFFICIENT DATA'}")
        indicators.append({
            "code": "US_MFG_IP", "label": "US Mfg Output (YoY)", "value": "N/A",
            "raw": None, "date": "OFFLINE", "source": "FRED/IPMAN", "status": "ERR",
        })

    # 6. China CPI YoY (FRED/OECD: CHNCPIALLMINMEI)
    cn_df, err = fred_res["CHNCPIALLMINMEI"]
    if cn_df is not None and len(cn_df) >= 13:
        yoy = ((float(cn_df["value"].iloc[-1]) / float(cn_df["value"].iloc[-13])) - 1.0) * 100.0
        indicators.append({
            "code": "CN_CPI_YOY", "label": "China CPI (YoY)", "value": f"{yoy:+.2f}%",
            "raw": yoy, "date": str(cn_df["date"].iloc[-1]), "source": "FRED/OECD", "status": "OK",
        })
        logs.append(f"[{now_str}] FRED CHINA CPI: OK ({yoy:+.2f}%)")
    else:
        logs.append(f"[{now_str}] FRED CHINA CPI ERR: {err or 'INSUFFICIENT DATA'}")
        indicators.append({
            "code": "CN_CPI_YOY", "label": "China CPI (YoY)", "value": "N/A",
            "raw": None, "date": "OFFLINE", "source": "FRED/OECD", "status": "ERR",
        })

    # 7. EMBI+ Sovereign Risk (BCRP -> fallback FRED EM OAS)
    embi_loaded = False
    res_pe, err_pe = bcrp_res["PD04709XD"]
    res_la, err_la = bcrp_res["PD04710XD"]
    if res_pe and res_pe[0] is not None:
        indicators.append({
            "code": "EMBI_PERU", "label": "EMBI+ Peru Risk", "value": f"{res_pe[0]:.0f} bps",
            "raw": res_pe[0], "date": res_pe[1], "source": "BCRP", "status": "OK",
        })
        embi_loaded = True
        logs.append(f"[{now_str}] BCRP EMBI+ PERU: OK ({res_pe[0]:.0f} bps)")
    if res_la and res_la[0] is not None:
        indicators.append({
            "code": "EMBI_LATAM", "label": "EMBI+ LatAm Risk", "value": f"{res_la[0]:.0f} bps",
            "raw": res_la[0], "date": res_la[1], "source": "BCRP", "status": "OK",
        })
        embi_loaded = True
        logs.append(f"[{now_str}] BCRP EMBI+ LATAM: OK ({res_la[0]:.0f} bps)")

    if not embi_loaded:
        oas_df, err_oas = _safe_call(_fetch_fred_series, "BAMLEMCBPIOAS", 90)
        if oas_df is not None and not oas_df.empty:
            oas_bps = float(oas_df["value"].iloc[-1]) * 100.0
            indicators.append({
                "code": "EM_OAS", "label": "EM Credit Spread (OAS)", "value": f"{oas_bps:.0f} bps",
                "raw": oas_bps, "date": str(oas_df["date"].iloc[-1]), "source": "FRED/ICE", "status": "OK",
            })
            logs.append(f"[{now_str}] FRED EM OAS FALLBACK: OK ({oas_bps:.0f} bps)")
        else:
            logs.append(f"[{now_str}] EMBI/OAS ERR: {err_pe or err_la or err_oas}")
            indicators.append({
                "code": "EMBI_RISK", "label": "EMBI+ Country Risk", "value": "N/A",
                "raw": None, "date": "OFFLINE", "source": "BCRP/FRED", "status": "ERR",
            })

    return {
        "indicators": indicators,
        "curve": curve_data,
        "logs": logs,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def compute_live_yield_spread(
    quotes: dict[str, dict[str, Any]],
    macro_curve: dict[str, Any],
) -> dict[str, Any]:
    """
    Combine real-time Yahoo Finance yields (^TNX and 2YY=F) with FRED DGS10/DGS2
    to compute the most accurate 10Y - 2Y Treasury Yield Curve Spread.
    """
    q10 = quotes.get("^TNX", {})
    q2 = quotes.get("2YY=F", {})

    y10 = q10.get("price") if q10.get("status") == "OK" and q10.get("price") is not None else macro_curve.get("us10y")
    y2 = q2.get("price") if q2.get("status") == "OK" and q2.get("price") is not None else macro_curve.get("us2y")

    source = "LIVE YF (^TNX / 2YY=F)"
    if q2.get("status") != "OK" or q2.get("price") is None:
        source = "HYBRID (^TNX + FRED DGS2)"
    if q10.get("status") != "OK" or q10.get("price") is None:
        source = macro_curve.get("source", "FRED")

    # Sanity check: yields should be in percentage points (0..20)
    if y10 is not None and y10 > 20:
        y10 = y10 / 10.0
    if y2 is not None and y2 > 20:
        y2 = y2 / 10.0

    spread = (y10 - y2) if (y10 is not None and y2 is not None) else macro_curve.get("spread_10y_2y")
    inverted = (spread is not None) and (spread < 0.0)

    return {
        "us10y": y10,
        "us2y": y2,
        "spread": spread,
        "spread_bps": (spread * 100.0) if spread is not None else None,
        "inverted": inverted,
        "source": source,
    }


def fetch_chart_series_json(ticker: str, timeframe_key: str = "1D") -> dict[str, Any]:
    """Return JSON-serializable OHLC series for the HTML5 Canvas retro chart."""
    res = fetch_chart_data(ticker, timeframe_key)
    df: pd.DataFrame = res["df"]
    candles: list[dict[str, Any]] = []

    if not df.empty:
        # Downsample very dense 1m series if > 240 points so Canvas stays razor sharp
        step = max(1, len(df) // 240) if len(df) > 260 else 1
        sampled = df.iloc[::step]
        if sampled.index[-1] != df.index[-1]:
            sampled = pd.concat([sampled, df.iloc[[-1]]])

        for ts, row in sampled.iterrows():
            c_val = float(row["Close"])
            o_val = float(row["Open"]) if "Open" in row and pd.notna(row["Open"]) else c_val
            h_val = float(row["High"]) if "High" in row and pd.notna(row["High"]) else max(o_val, c_val)
            l_val = float(row["Low"]) if "Low" in row and pd.notna(row["Low"]) else min(o_val, c_val)
            epoch_ms = int(pd.Timestamp(ts).tz_localize("UTC").timestamp() * 1000)
            candles.append({
                "time": epoch_ms,
                "open": o_val,
                "high": h_val,
                "low": l_val,
                "close": c_val,
            })

    return {
        "ticker": ticker,
        "timeframe": timeframe_key,
        "effective_label": res["effective_label"],
        "prev_close": res["prev_close"],
        "market_closed": res["market_closed"],
        "note": res["note"],
        "candles": candles,
    }
