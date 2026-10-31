#!/usr/bin/env python3
"""
binance_dashboard.py — Streamlit frontend for ``binancedata.py``.

Views:
  * Scanner       — live 24h gainers, losers and volume leaders.
  * Past records  — scans are stored in a local SQLite database; review,
                    overlay and chart recorded prices with moving averages.
  * Moving average — OHLCV candles with configurable SMA/EMA overlays.
  * Alerts        — MA cross, RSI level and volume-spike rules evaluated on
                    every refresh, with a persisted trigger log.
  * Account       — signed balance lookup for the selected market/environment.

Run:
    python -m streamlit run binance_dashboard.py

Public market data needs no API key. The history database defaults to
``data/binance_history.sqlite3``; override it with ``BINANCE_HISTORY_DB``.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

from binancedata import (
    BASE_URLS,
    DEFAULT_ENVIRONMENT,
    DEFAULT_MARKET,
    SUPPORTED_INTERVALS,
    BinanceDataError,
    active_dns_overrides,
    build_snapshot,
    fetch_account_balance,
    fetch_klines,
    fetch_tradable_symbols,
    resolve_base_url,
)

APP_DIR = Path(__file__).resolve().parent
DB_PATH = Path(
    os.environ.get(
        "BINANCE_HISTORY_DB", APP_DIR / "data" / "binance_history.sqlite3"
    )
)

MAX_RECORDS_PER_SCAN = 250
QUOTES = ("USDT", "USDC", "FDUSD", "BTC", "ETH", "BNB", "TRY", "EUR")
FALLBACK_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT")
MA_COLORS = ("#f59e0b", "#8b5cf6", "#0ea5e9", "#ec4899", "#22c55e")
LINE_COLORS = (
    "#0ea5e9",
    "#f59e0b",
    "#8b5cf6",
    "#ec4899",
    "#22c55e",
    "#ef4444",
    "#14b8a6",
    "#64748b",
)
HISTORY_WINDOWS = {
    "Last hour": timedelta(hours=1),
    "Last 24 hours": timedelta(days=1),
    "Last 7 days": timedelta(days=7),
    "Last 30 days": timedelta(days=30),
    "All time": None,
}
ALERT_TYPES = ("MA cross", "RSI level", "Volume spike")

UP_COLOR = "#16a34a"
DOWN_COLOR = "#dc2626"


# --------------------------------------------------------------------------- #
# History + alerts store (SQLite)
# --------------------------------------------------------------------------- #
def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def init_db() -> None:
    """Create the database tables if they do not exist yet."""
    with closing(_connect()) as connection, connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                market TEXT NOT NULL,
                environment TEXT NOT NULL,
                quote TEXT NOT NULL,
                symbol TEXT NOT NULL,
                last_price REAL,
                change_pct REAL,
                quote_volume REAL,
                high_price REAL,
                low_price REAL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_snapshot_symbol_time
            ON snapshot_records (symbol, captured_at)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                environment TEXT NOT NULL,
                rule_type TEXT NOT NULL,
                direction TEXT NOT NULL,
                interval TEXT NOT NULL,
                window INTEGER NOT NULL,
                threshold REAL,
                last_candle TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER,
                symbol TEXT NOT NULL,
                rule_type TEXT NOT NULL,
                message TEXT NOT NULL,
                price REAL,
                candle_time TEXT,
                created_at TEXT NOT NULL
            )
            """
        )


def _to_number(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if pd.isna(number) else number


def record_snapshot(
    df: pd.DataFrame, market: str, environment: str, quote: str
) -> int:
    """Persist the most liquid rows of a scan and return how many were written."""
    captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    top = df.sort_values("quoteVolume", ascending=False).head(MAX_RECORDS_PER_SCAN)

    rows = [
        (
            captured_at,
            market,
            environment,
            quote,
            str(row["symbol"]),
            _to_number(row.get("lastPrice")),
            _to_number(row.get("priceChangePercent")),
            _to_number(row.get("quoteVolume")),
            _to_number(row.get("highPrice")),
            _to_number(row.get("lowPrice")),
        )
        for _, row in top.iterrows()
    ]
    if not rows:
        return 0

    with closing(_connect()) as connection, connection:
        connection.executemany(
            """
            INSERT INTO snapshot_records (
                captured_at, market, environment, quote, symbol,
                last_price, change_pct, quote_volume, high_price, low_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def load_history(
    quote: str | None = None,
    symbols: list[str] | None = None,
    since: datetime | None = None,
    limit: int = 5000,
) -> pd.DataFrame:
    """Read recorded scans, newest first."""
    query = [
        "SELECT captured_at, market, environment, quote, symbol,",
        "last_price, change_pct, quote_volume, high_price, low_price",
        "FROM snapshot_records",
    ]
    clauses: list[str] = []
    params: list[object] = []

    if quote and quote != "All":
        clauses.append("quote = ?")
        params.append(quote)
    if symbols:
        clauses.append(
            "symbol IN (" + ", ".join("?" for _ in symbols) + ")"
        )
        params.extend(symbols)
    if since is not None:
        clauses.append("captured_at >= ?")
        params.append(since.isoformat())

    if clauses:
        query.append("WHERE " + " AND ".join(clauses))
    query.append("ORDER BY captured_at DESC, id DESC LIMIT ?")
    params.append(limit)

    with closing(_connect()) as connection:
        df = pd.read_sql_query(" ".join(query), connection, params=params)

    if not df.empty:
        df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True)
    return df


def history_stats() -> tuple[int, str | None]:
    """Return total records and the timestamp of the latest capture."""
    with closing(_connect()) as connection:
        row = connection.execute(
            "SELECT COUNT(*), MAX(captured_at) FROM snapshot_records"
        ).fetchone()
    return int(row[0] or 0), row[1]


def history_symbols() -> list[str]:
    with closing(_connect()) as connection:
        rows = connection.execute(
            "SELECT DISTINCT symbol FROM snapshot_records ORDER BY symbol"
        ).fetchall()
    return [row[0] for row in rows]


def history_quotes() -> list[str]:
    with closing(_connect()) as connection:
        rows = connection.execute(
            "SELECT DISTINCT quote FROM snapshot_records ORDER BY quote"
        ).fetchall()
    return [row[0] for row in rows]


def seconds_since_last_save(
    market: str, environment: str, quote: str
) -> float | None:
    """How long ago this scan selection was last recorded, if ever."""
    with closing(_connect()) as connection:
        row = connection.execute(
            """
            SELECT MAX(captured_at) FROM snapshot_records
            WHERE market = ? AND environment = ? AND quote = ?
            """,
            (market, environment, quote),
        ).fetchone()
    if not row or not row[0]:
        return None
    try:
        last = datetime.fromisoformat(row[0])
    except ValueError:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds()


def delete_history() -> None:
    with closing(_connect()) as connection, connection:
        connection.execute("DELETE FROM snapshot_records")


# --------------------------------------------------------------------------- #
# Alert rule storage
# --------------------------------------------------------------------------- #
def add_alert_rule(
    symbol: str,
    market: str,
    environment: str,
    rule_type: str,
    direction: str,
    interval: str,
    window: int,
    threshold: float | None,
) -> int:
    with closing(_connect()) as connection, connection:
        cursor = connection.execute(
            """
            INSERT INTO alert_rules (
                created_at, symbol, market, environment, rule_type,
                direction, interval, window, threshold
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                symbol,
                market,
                environment,
                rule_type,
                direction,
                interval,
                int(window),
                threshold,
            ),
        )
        return int(cursor.lastrowid or 0)


def load_alert_rules() -> list[sqlite3.Row]:
    with closing(_connect()) as connection:
        return connection.execute(
            "SELECT * FROM alert_rules ORDER BY id"
        ).fetchall()


def delete_alert_rules(rule_ids: list[int]) -> None:
    if not rule_ids:
        return
    placeholders = ", ".join("?" for _ in rule_ids)
    with closing(_connect()) as connection, connection:
        connection.execute(
            f"DELETE FROM alert_rules WHERE id IN ({placeholders})", rule_ids
        )
        connection.execute(
            f"DELETE FROM alert_events WHERE rule_id IN ({placeholders})", rule_ids
        )


def mark_rule_candle(rule_id: int, candle_time: str) -> None:
    with closing(_connect()) as connection, connection:
        connection.execute(
            "UPDATE alert_rules SET last_candle = ? WHERE id = ?",
            (candle_time, rule_id),
        )


def add_alert_event(
    rule_id: int,
    symbol: str,
    rule_type: str,
    message: str,
    price: float,
    candle_time: str,
) -> None:
    with closing(_connect()) as connection, connection:
        connection.execute(
            """
            INSERT INTO alert_events (
                rule_id, symbol, rule_type, message, price, candle_time, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_id,
                symbol,
                rule_type,
                message,
                price,
                candle_time,
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )


def load_alert_events(limit: int = 50) -> pd.DataFrame:
    with closing(_connect()) as connection:
        df = pd.read_sql_query(
            """
            SELECT created_at, symbol, rule_type, message, price
            FROM alert_events ORDER BY id DESC LIMIT ?
            """,
            connection,
            params=[limit],
        )
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df


def clear_alert_events() -> None:
    with closing(_connect()) as connection, connection:
        connection.execute("DELETE FROM alert_events")


# --------------------------------------------------------------------------- #
# Cached network calls
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=900, show_spinner=False)
def load_tradable_symbols(
    market: str, environment: str, quote: str
) -> frozenset[str]:
    """exchangeInfo changes rarely, so cache the symbol list for 15 minutes."""
    base_url = resolve_base_url(market, environment, None)
    with requests.Session() as session:
        return frozenset(fetch_tradable_symbols(base_url, market, quote, session))


def _build_snapshot(
    market: str,
    environment: str,
    quote: str,
    min_volume: float,
    symbols: frozenset[str],
) -> pd.DataFrame:
    base_url = resolve_base_url(market, environment, None)
    with requests.Session() as session:
        return build_snapshot(
            base_url, market, quote, min_volume, session, tradable=symbols
        )


@st.cache_data(ttl=45, max_entries=8, show_spinner=False)
def load_snapshot(
    market: str,
    environment: str,
    quote: str,
    min_volume: float,
    nonce: int,
) -> tuple[pd.DataFrame, str]:
    """Fetch a snapshot, falling back to live public data if demo fails."""
    try:
        symbols = load_tradable_symbols(market, environment, quote)
        return _build_snapshot(market, environment, quote, min_volume, symbols), environment
    except BinanceDataError:
        if environment == "live":
            raise
        symbols = load_tradable_symbols(market, "live", quote)
        return (
            _build_snapshot(market, "live", quote, min_volume, symbols),
            "live (demo fallback)",
        )


@st.cache_data(ttl=60, show_spinner=False)
def load_klines(
    market: str, environment: str, symbol: str, interval: str, limit: int
) -> tuple[pd.DataFrame, str]:
    """Fetch candles, falling back to live data if the demo API refuses."""
    base_url = resolve_base_url(market, environment, None)
    with requests.Session() as session:
        try:
            return (
                fetch_klines(base_url, market, symbol, interval, limit, session),
                environment,
            )
        except BinanceDataError:
            if environment == "live":
                raise
            live_url = BASE_URLS[(market, "live")]
            return (
                fetch_klines(live_url, market, symbol, interval, limit, session),
                "live (demo fallback)",
            )


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #
def format_price(value: object) -> str:
    number = _to_number(value)
    if number is None:
        return "—"
    if number == 0:
        return "0"
    absolute = abs(number)
    if absolute >= 1000:
        return f"{number:,.2f}"
    if absolute >= 1:
        return f"{number:,.4f}"
    return f"{number:,.8f}".rstrip("0").rstrip(".")


def format_volume(value: object) -> str:
    number = _to_number(value)
    if number is None:
        return "—"
    for threshold, suffix in (
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ):
        if abs(number) >= threshold:
            return f"{number / threshold:,.2f}{suffix}"
    return f"{number:,.2f}"


def dataframe_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def should_auto_refresh(
    previous_tick: float | None, now: float, interval_seconds: int
) -> bool:
    """True when a fragment timer run is old enough to count as a real tick.

    The first render of the fragment only arms the timer, and every full app
    rerun re-creates it, so recent timestamps must not trigger a refresh.
    """
    if previous_tick is None:
        return False
    return (now - previous_tick) >= interval_seconds * 0.6


# --------------------------------------------------------------------------- #
# Indicators
# --------------------------------------------------------------------------- #
def simple_moving_average(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=1).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI."""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    values = 100 - (100 / (1 + avg_gain / avg_loss))
    return values.where(avg_loss != 0, 100.0)


def add_moving_averages(
    df: pd.DataFrame, windows: list[int], method: str
) -> tuple[pd.DataFrame, list[str]]:
    enriched = df.copy()
    columns: list[str] = []
    for window in sorted(set(windows)):
        column = f"{method}{window}"
        if method == "EMA":
            enriched[column] = enriched["close"].ewm(span=window, adjust=False).mean()
        else:
            enriched[column] = simple_moving_average(enriched["close"], window)
        columns.append(column)
    return enriched, columns


def trend_readout(close: float, averages: dict[str, float]) -> tuple[str, str]:
    """Describe whether price trades above or below each average."""
    if not averages:
        return "No averages selected", "info"
    above = [name for name, value in averages.items() if close >= value]
    if len(above) == len(averages):
        return "Price above every selected average — bullish bias", "success"
    if not above:
        return "Price below every selected average — bearish bias", "warning"
    return (
        f"Mixed: above {', '.join(above)}; below "
        f"{', '.join(name for name in averages if name not in above)}",
        "info",
    )


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #
def make_movers_chart(df: pd.DataFrame, rows: int = 10) -> go.Figure:
    gainers = df.nlargest(rows, "priceChangePercent")
    losers = df.nsmallest(rows, "priceChangePercent")
    movers = (
        pd.concat([gainers, losers])
        .drop_duplicates("symbol")
        .sort_values("priceChangePercent")
    )
    colors = [
        DOWN_COLOR if value < 0 else UP_COLOR
        for value in movers["priceChangePercent"]
    ]
    fig = go.Figure(
        go.Bar(
            x=movers["priceChangePercent"],
            y=movers["symbol"],
            orientation="h",
            marker_color=colors,
            hovertemplate="%{y}: %{x:.2f}%<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_white",
        height=max(320, 26 * len(movers)),
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="24h change (%)",
        yaxis_title=None,
    )
    return fig


def make_candles_chart(
    df: pd.DataFrame,
    symbol: str,
    ma_columns: list[str],
) -> go.Figure:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.76, 0.24],
        vertical_spacing=0.04,
    )
    fig.add_trace(
        go.Candlestick(
            x=df["openTime"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name=symbol,
            increasing_line_color=UP_COLOR,
            decreasing_line_color=DOWN_COLOR,
        ),
        row=1,
        col=1,
    )
    for column, color in zip(ma_columns, MA_COLORS):
        fig.add_trace(
            go.Scatter(
                x=df["openTime"],
                y=df[column],
                mode="lines",
                name=column,
                line=dict(width=1.6, color=color),
                hovertemplate=f"{column}: %{{y:.6f}}<extra></extra>",
            ),
            row=1,
            col=1,
        )
    fig.add_trace(
        go.Bar(
            x=df["openTime"],
            y=df["volume"],
            name="Volume",
            marker_color="#94a3b8",
            opacity=0.55,
            hovertemplate="%{y:,.4s}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        template="plotly_white",
        height=640,
        margin=dict(l=10, r=10, t=34, b=10),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hovermode="x unified",
        bargap=0.15,
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    return fig


def make_records_chart(
    history: pd.DataFrame,
    symbols: list[str],
    ma_window: int,
    normalize: bool,
) -> go.Figure:
    fig = go.Figure()
    show_ma_legend = len(symbols) <= 2
    for index, symbol in enumerate(symbols):
        series = (
            history[history["symbol"] == symbol]
            .sort_values("captured_at")
            .dropna(subset=["last_price"])
        )
        if series.empty:
            continue

        color = LINE_COLORS[index % len(LINE_COLORS)]
        prices = series["last_price"]
        base = float(prices.iloc[0]) or 1.0
        y_values = (prices / base - 1) * 100 if normalize else prices

        fig.add_trace(
            go.Scatter(
                x=series["captured_at"],
                y=y_values,
                mode="lines+markers",
                name=symbol,
                legendgroup=symbol,
                line=dict(color=color, width=2),
                marker=dict(size=6),
                hovertemplate=f"{symbol}: %{{y:,.4f}}<extra></extra>",
            )
        )

        if ma_window > 1:
            average = simple_moving_average(prices, ma_window)
            ma_values = (average / base - 1) * 100 if normalize else average
            fig.add_trace(
                go.Scatter(
                    x=series["captured_at"],
                    y=ma_values,
                    mode="lines",
                    name=f"{symbol} MA({ma_window})",
                    legendgroup=symbol,
                    showlegend=show_ma_legend,
                    line=dict(color=color, width=1.4, dash="dot"),
                    opacity=0.9,
                    hovertemplate=f"{symbol} MA: %{{y:,.4f}}<extra></extra>",
                )
            )

    fig.update_layout(
        title="Recorded prices" + (" — % change" if normalize else ""),
        template="plotly_white",
        height=460,
        margin=dict(l=10, r=10, t=48, b=10),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        yaxis_title="% change since first record" if normalize else "Price",
    )
    return fig


# --------------------------------------------------------------------------- #
# Alert evaluation
# --------------------------------------------------------------------------- #
def evaluate_rule(rule: sqlite3.Row) -> tuple[str, float, str] | None:
    """Return ``(message, price, candle_time)`` when a rule fires."""
    window = max(2, int(rule["window"] or 14))
    limit = min(1000, max(200, window * 4))
    candles, _source = load_klines(
        rule["market"],
        rule["environment"],
        rule["symbol"],
        rule["interval"],
        limit,
    )
    if len(candles) < 3:
        return None

    candle_time = candles["openTime"].iloc[-1].isoformat()
    if rule["last_candle"] == candle_time:
        return None

    close = candles["close"]
    price = float(close.iloc[-1])
    symbol = rule["symbol"]
    interval = rule["interval"]
    direction = rule["direction"].lower()

    if rule["rule_type"] == "MA cross":
        if len(candles) < window + 2:
            return None
        average = close.rolling(window).mean()
        previous_close, current_close = close.iloc[-2], close.iloc[-1]
        previous_ma, current_ma = average.iloc[-2], average.iloc[-1]
        if pd.isna(previous_ma) or pd.isna(current_ma):
            return None

        crossed_above = previous_close <= previous_ma and current_close > current_ma
        crossed_below = previous_close >= previous_ma and current_close < current_ma
        if direction == "above" and crossed_above:
            return (
                f"{symbol} crossed above SMA{window} ({interval}) "
                f"at {format_price(price)}",
                price,
                candle_time,
            )
        if direction == "below" and crossed_below:
            return (
                f"{symbol} crossed below SMA{window} ({interval}) "
                f"at {format_price(price)}",
                price,
                candle_time,
            )
        return None

    if rule["rule_type"] == "RSI level":
        values = rsi(close, window)
        previous, current = values.iloc[-2], values.iloc[-1]
        if pd.isna(previous) or pd.isna(current):
            return None
        if direction == "above" and previous <= 70 < current:
            return (
                f"{symbol} RSI{window} crossed above 70 (overbought) on "
                f"{interval} at {format_price(price)}",
                price,
                candle_time,
            )
        if direction == "below" and previous >= 30 > current:
            return (
                f"{symbol} RSI{window} crossed below 30 (oversold) on "
                f"{interval} at {format_price(price)}",
                price,
                candle_time,
            )
        return None

    if rule["rule_type"] == "Volume spike":
        multiplier = float(rule["threshold"] or 2.0)
        average = candles["volume"].rolling(window).mean().iloc[-1]
        volume = float(candles["volume"].iloc[-1])
        if pd.isna(average) or average <= 0:
            return None
        ratio = volume / average
        if direction == "above" and ratio >= multiplier:
            return (
                f"{symbol} volume is {ratio:.1f}× its {window}-candle average "
                f"({interval}) at {format_price(price)}",
                price,
                candle_time,
            )
        return None

    return None


def check_alerts() -> list[dict[str, object]]:
    """Evaluate every active rule and persist (and return) new triggers."""
    triggered: list[dict[str, object]] = []
    for rule in load_alert_rules():
        try:
            result = evaluate_rule(rule)
        except BinanceDataError:
            continue
        except Exception:  # noqa: BLE001 — a broken rule must not break the app
            continue
        if result is None:
            continue

        message, price, candle_time = result
        try:
            add_alert_event(
                int(rule["id"]),
                str(rule["symbol"]),
                str(rule["rule_type"]),
                message,
                price,
                candle_time,
            )
        except sqlite3.Error:
            continue
        mark_rule_candle(int(rule["id"]), candle_time)
        triggered.append(
            {
                "message": message,
                "symbol": rule["symbol"],
                "rule_type": rule["rule_type"],
            }
        )
    return triggered


# --------------------------------------------------------------------------- #
# UI helpers
# --------------------------------------------------------------------------- #
def _prices(df: pd.DataFrame, column: str) -> pd.Series:
    """Format a price column, tolerating tickers that omit high/low."""
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[column].map(format_price)


def snapshot_table(df: pd.DataFrame) -> None:
    view = pd.DataFrame(
        {
            "symbol": df["symbol"],
            "price": _prices(df, "lastPrice"),
            "change_pct": df["priceChangePercent"],
            "volume": df["quoteVolume"],
            "high": _prices(df, "highPrice"),
            "low": _prices(df, "lowPrice"),
        }
    )
    st.dataframe(
        view,
        width="stretch",
        height=min(38 * (len(view) + 1), 460),
        hide_index=True,
        column_config={
            "symbol": st.column_config.TextColumn("Symbol"),
            "price": st.column_config.TextColumn("Price"),
            "change_pct": st.column_config.NumberColumn("24h %", format="%.2f"),
            "volume": st.column_config.NumberColumn(
                "24h volume", format="compact"
            ),
            "high": st.column_config.TextColumn("High"),
            "low": st.column_config.TextColumn("Low"),
        },
    )


def records_table(history: pd.DataFrame) -> None:
    view = history.copy()
    view["price"] = view["last_price"].map(format_price)
    view["high"] = view["high_price"].map(format_price)
    view["low"] = view["low_price"].map(format_price)
    view["volume"] = view["quote_volume"].map(format_volume)
    view = view[
        [
            "captured_at",
            "symbol",
            "market",
            "environment",
            "price",
            "change_pct",
            "volume",
            "high",
            "low",
        ]
    ]
    st.dataframe(
        view,
        width="stretch",
        height=420,
        hide_index=True,
        column_config={
            "captured_at": st.column_config.DatetimeColumn(
                "Captured (UTC)", format="YYYY-MM-DD HH:mm:ss"
            ),
            "symbol": st.column_config.TextColumn("Symbol"),
            "market": st.column_config.TextColumn("Market"),
            "environment": st.column_config.TextColumn("Env"),
            "price": st.column_config.TextColumn("Price"),
            "change_pct": st.column_config.NumberColumn("24h %", format="%.2f"),
            "volume": st.column_config.TextColumn("24h volume"),
            "high": st.column_config.TextColumn("High"),
            "low": st.column_config.TextColumn("Low"),
        },
    )


def busiest_symbols(history: pd.DataFrame, limit: int = 5) -> list[str]:
    counts = history["symbol"].value_counts()
    return list(counts.head(limit).index)


def records_summary(
    history: pd.DataFrame, symbols: list[str], ma_window: int
) -> pd.DataFrame:
    rows = []
    for symbol in symbols:
        series = (
            history[history["symbol"] == symbol]
            .sort_values("captured_at")
            .dropna(subset=["last_price"])
        )
        if series.empty:
            continue
        first = float(series["last_price"].iloc[0])
        latest = float(series["last_price"].iloc[-1])
        average = float(
            simple_moving_average(series["last_price"], ma_window).iloc[-1]
        )
        rows.append(
            {
                "symbol": symbol,
                "observations": len(series),
                "first": format_price(first),
                "latest": format_price(latest),
                "change_pct": (latest / first - 1) * 100 if first else 0.0,
                "ma": format_price(average),
                "position": "Above MA" if latest >= average else "Below MA",
                "last_seen": series["captured_at"].iloc[-1],
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Tabs
# --------------------------------------------------------------------------- #
def render_scanner(snapshot: pd.DataFrame, top_n: int, quote: str) -> None:
    changes = snapshot["priceChangePercent"]
    total_volume = snapshot["quoteVolume"].sum()

    metrics = st.columns(4)
    metrics[0].metric("Pairs matched", f"{len(snapshot):,}")
    metrics[1].metric("24h volume", format_volume(total_volume))
    metrics[2].metric("Median change", f"{changes.median():.2f}%")
    metrics[3].metric("Advancing", f"{(changes > 0).sum():,} / {len(snapshot):,}")

    gainers, losers = st.columns(2)
    with gainers:
        st.subheader("Top gainers")
        snapshot_table(
            snapshot.nlargest(top_n, "priceChangePercent").reset_index(drop=True)
        )
    with losers:
        st.subheader("Top losers")
        snapshot_table(
            snapshot.nsmallest(top_n, "priceChangePercent").reset_index(drop=True)
        )

    st.subheader("Biggest moves")
    st.plotly_chart(
        make_movers_chart(snapshot, rows=min(top_n, 12)),
        width="stretch",
        theme=None,
    )

    st.subheader("Highest volume")
    leaders = snapshot.nlargest(top_n, "quoteVolume").reset_index(drop=True)
    snapshot_table(leaders)
    st.download_button(
        f"Download {len(snapshot):,} {quote} pairs as CSV",
        data=dataframe_csv(snapshot),
        file_name=f"binance_{quote.lower()}_snapshot.csv",
        mime="text/csv",
    )


def render_records(
    quote: str,
    market: str,
    environment: str,
    auto_save: bool,
    snapshot: pd.DataFrame | None,
) -> None:
    total, latest = history_stats()

    header = st.columns([2, 1, 1])
    with header[0]:
        st.caption(f"History file: `{DB_PATH}`")
    header[1].metric("Saved records", f"{total:,}")
    header[2].metric(
        "Latest capture (UTC)",
        pd.to_datetime(latest).strftime("%H:%M:%S") if latest else "—",
    )

    if total == 0:
        st.info(
            "No records yet. Run a scan with **Save scans to past records** "
            "enabled and the snapshot will be stored here."
        )

    options = history_quotes()
    filters = st.columns([1, 2, 1])
    quote_filter = filters[0].selectbox(
        "Quote", ["All"] + options, index=0, key="history_quote"
    )
    symbol_filter = filters[1].multiselect(
        "Symbols",
        history_symbols(),
        default=[],
        key="history_symbols",
        placeholder="All symbols",
    )
    window_label = filters[2].selectbox(
        "Time range", list(HISTORY_WINDOWS), index=1, key="history_window"
    )

    delta = HISTORY_WINDOWS[window_label]
    since = datetime.now(timezone.utc) - delta if delta else None
    history = load_history(
        quote=None if quote_filter == "All" else quote_filter,
        symbols=symbol_filter or None,
        since=since,
    )

    if history.empty:
        if total:
            st.warning("No records match these filters.")
    else:
        records_table(history)

        st.divider()
        st.subheader("Recorded prices & moving average")
        recorded_symbols = sorted(history["symbol"].unique())
        chart_symbols = symbol_filter or busiest_symbols(history)
        chart_symbols = [s for s in chart_symbols if s in recorded_symbols]

        if len(chart_symbols) > 1:
            st.caption(
                "Overlaying several symbols? Switch on **Normalize to % change** "
                "so different price scales stay comparable."
            )

        chart_controls = st.columns([1, 1, 2])
        ma_window = chart_controls[0].number_input(
            "MA window (records)",
            min_value=1,
            max_value=50,
            value=3,
            step=1,
            key="history_ma_window",
        )
        normalize = chart_controls[1].toggle(
            "Normalize to % change",
            value=len(chart_symbols) > 1,
            key="history_normalize",
        )
        chart_controls[2].caption(
            f"{len(chart_symbols)} symbol(s) · {len(history):,} stored observations"
        )

        if len(history) < 2:
            st.info("Scan at least twice to see a trend.")
        else:
            st.plotly_chart(
                make_records_chart(
                    history, chart_symbols, int(ma_window), bool(normalize)
                ),
                width="stretch",
                theme=None,
            )

            summary = records_summary(history, chart_symbols, int(ma_window))
            if not summary.empty:
                st.dataframe(
                    summary,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "observations": st.column_config.NumberColumn(
                            "Records", format="%d"
                        ),
                        "first": st.column_config.TextColumn("First"),
                        "latest": st.column_config.TextColumn("Latest"),
                        "change_pct": st.column_config.NumberColumn(
                            "Change %", format="%.2f"
                        ),
                        "ma": st.column_config.TextColumn("MA"),
                        "position": st.column_config.TextColumn("Position"),
                        "last_seen": st.column_config.DatetimeColumn(
                            "Last seen (UTC)", format="YYYY-MM-DD HH:mm:ss"
                        ),
                    },
                )

        actions = st.columns([1, 1, 2])
        actions[0].download_button(
            "Export records (CSV)",
            data=dataframe_csv(history),
            file_name="binance_past_records.csv",
            mime="text/csv",
        )
        if snapshot is not None:
            if actions[1].button("Save current scan", width="stretch"):
                saved = record_snapshot(snapshot, market, environment, quote)
                st.toast(f"Saved {saved} records", icon="💾")
                st.rerun()

    with st.expander("Danger zone"):
        st.write("Deleting clears every stored record. This cannot be undone.")
        confirm = st.checkbox("I understand", key="history_delete_confirm")
        if st.button("Delete all records", disabled=not confirm):
            delete_history()
            st.toast("History cleared", icon="🗑️")
            st.rerun()

    if not auto_save:
        st.caption("Automatic saving is off — only manual saves are stored.")


def render_moving_average(
    snapshot: pd.DataFrame | None, market: str, environment: str, quote: str
) -> None:
    if snapshot is not None and not snapshot.empty:
        symbols = sorted(snapshot["symbol"].tolist())
    else:
        symbols = [symbol for symbol in FALLBACK_SYMBOLS if symbol.endswith(quote)]
        symbols = symbols or list(FALLBACK_SYMBOLS)

    controls = st.columns([2, 1, 1, 1])
    symbol = controls[0].selectbox(
        "Symbol",
        symbols,
        index=0,
        key="ma_symbol",
        help="Type to search the scanned pairs.",
    )
    interval = controls[1].selectbox(
        "Interval",
        list(SUPPORTED_INTERVALS),
        index=list(SUPPORTED_INTERVALS).index("1h"),
        key="ma_interval",
    )
    candle_count = controls[2].slider(
        "Candles", min_value=50, max_value=1000, value=300, step=50,
        key="ma_candles",
    )
    method = controls[3].radio(
        "Average", ("SMA", "EMA"), horizontal=True, key="ma_method"
    )

    windows = st.multiselect(
        "Moving average windows",
        options=[7, 10, 20, 30, 50, 100, 200],
        default=[20, 50],
        key="ma_windows",
    )

    try:
        with st.spinner(f"Loading {symbol} candles…"):
            candles, source = load_klines(
                market, environment, symbol, interval, candle_count
            )
    except BinanceDataError as exc:
        st.error(f"Could not load candles for {symbol}: {exc}")
        return

    if candles.empty:
        st.warning("Binance returned no candles for this symbol.")
        return

    enriched, ma_columns = add_moving_averages(candles, windows, method)
    latest = enriched.iloc[-1]
    window_change = (
        (latest["close"] / enriched.iloc[0]["close"] - 1) * 100
        if enriched.iloc[0]["close"]
        else 0.0
    )

    metrics = st.columns(2 + len(ma_columns))
    metrics[0].metric(symbol, format_price(latest["close"]), f"{window_change:+.2f}%")
    metrics[1].metric("Candles", f"{len(enriched):,}")
    averages: dict[str, float] = {}
    for index, column in enumerate(ma_columns, start=2):
        average = float(latest[column])
        averages[column] = average
        distance = (latest["close"] / average - 1) * 100 if average else 0.0
        metrics[index].metric(column, format_price(average), f"{distance:+.2f}%")

    st.caption(f"Data source: {source} · interval {interval} · {len(enriched)} candles")
    st.plotly_chart(
        make_candles_chart(enriched, symbol, ma_columns),
        width="stretch",
        theme=None,
    )

    message, level = trend_readout(float(latest["close"]), averages)
    if level == "success":
        st.success(message, icon="📈")
    elif level == "warning":
        st.warning(message, icon="📉")
    else:
        st.info(message, icon="ℹ️")

    with st.expander("Raw candles"):
        display_columns = ["openTime", "open", "high", "low", "close", "volume"]
        display_columns += ma_columns
        st.dataframe(
            enriched[display_columns].sort_values("openTime", ascending=False),
            width="stretch",
            hide_index=True,
        )
        st.download_button(
            "Download candles (CSV)",
            data=dataframe_csv(enriched[display_columns]),
            file_name=f"{symbol}_{interval}_candles.csv",
            mime="text/csv",
        )


def render_alerts(
    snapshot: pd.DataFrame | None, market: str, environment: str, quote: str
) -> None:
    st.subheader("Alert rules")
    st.caption(
        "Rules are checked whenever the dashboard refreshes and are stored in "
        f"the local database (`{DB_PATH.name}`)."
    )

    if snapshot is not None and not snapshot.empty:
        symbol_options = sorted(snapshot["symbol"].tolist())
    else:
        symbol_options = list(FALLBACK_SYMBOLS)

    with st.form("new_alert_rule"):
        st.caption(f"New rules apply to **{market}** / **{environment}**.")
        columns = st.columns([2, 1, 1])
        symbol = columns[0].selectbox(
            "Symbol",
            symbol_options,
            index=(
                symbol_options.index("BTCUSDT")
                if "BTCUSDT" in symbol_options
                else 0
            ),
            key="alert_symbol",
        )
        rule_type = columns[1].selectbox(
            "Condition", ALERT_TYPES, key="alert_type"
        )
        direction = columns[2].selectbox(
            "Direction",
            ["Above", "Below"],
            key="alert_direction",
            help="'Above' = cross/spike up, 'Below' = cross down. "
            "Volume spikes only trigger on 'Above'.",
        )
        columns = st.columns([1, 1, 2])
        interval = columns[0].selectbox(
            "Interval",
            list(SUPPORTED_INTERVALS),
            index=list(SUPPORTED_INTERVALS).index("1h"),
            key="alert_interval",
        )
        window = columns[1].number_input(
            "Window / period",
            min_value=2,
            max_value=200,
            value=50,
            step=1,
            key="alert_window",
            help="MA window, RSI period, or volume lookback.",
        )
        threshold = columns[2].number_input(
            "Volume multiplier",
            min_value=1.0,
            max_value=20.0,
            value=2.0,
            step=0.1,
            key="alert_threshold",
            help="Only used by Volume spike rules (e.g. 2.0 = twice the average).",
        )
        submitted = st.form_submit_button("Add alert", type="primary")

    if submitted:
        symbol = symbol.strip().upper()
        rule_id = add_alert_rule(
            symbol=symbol,
            market=market,
            environment=environment,
            rule_type=rule_type,
            direction=direction.lower(),
            interval=interval,
            window=int(window),
            threshold=float(threshold) if rule_type == "Volume spike" else None,
        )
        st.toast(f"Alert #{rule_id} added for {symbol}", icon="🔔")

    rules = load_alert_rules()
    if rules:
        st.divider()
        st.subheader("Active rules")
        rules_table = pd.DataFrame(
            [
                {
                    "id": rule["id"],
                    "symbol": rule["symbol"],
                    "condition": rule["rule_type"],
                    "direction": rule["direction"].title(),
                    "interval": rule["interval"],
                    "window": rule["window"],
                    "threshold": rule["threshold"],
                    "market": rule["market"],
                    "env": rule["environment"],
                    "created": rule["created_at"],
                }
                for rule in rules
            ]
        )
        st.dataframe(rules_table, width="stretch", hide_index=True)

        labels = {
            f"#{rule['id']} · {rule['symbol']} · {rule['rule_type']} "
            f"{rule['direction']} ({rule['interval']})": int(rule["id"])
            for rule in rules
        }
        columns = st.columns([3, 1])
        selected = columns[0].multiselect(
            "Select rules to delete", list(labels), key="alert_delete_selection"
        )
        if columns[1].button("Delete selected", width="stretch"):
            delete_alert_rules([labels[label] for label in selected])
            st.toast(f"Deleted {len(selected)} rule(s)", icon="🗑️")
            st.rerun()
    else:
        st.info("No alert rules yet — create one above.")

    st.divider()
    st.subheader("Recent triggers")
    events = load_alert_events()
    if events.empty:
        st.info("No alerts have fired yet.")
    else:
        view = events.copy()
        view["price"] = view["price"].map(format_price)
        st.dataframe(
            view,
            width="stretch",
            hide_index=True,
            column_config={
                "created_at": st.column_config.DatetimeColumn(
                    "Triggered (UTC)", format="YYYY-MM-DD HH:mm:ss"
                ),
                "symbol": st.column_config.TextColumn("Symbol"),
                "rule_type": st.column_config.TextColumn("Condition"),
                "message": st.column_config.TextColumn("Message", width="large"),
                "price": st.column_config.TextColumn("Price"),
            },
        )
        actions = st.columns([1, 3])
        actions[0].download_button(
            "Export triggers (CSV)",
            data=dataframe_csv(events),
            file_name="binance_alert_triggers.csv",
            mime="text/csv",
        )
        if actions[1].button("Clear trigger log"):
            clear_alert_events()
            st.toast("Trigger log cleared", icon="🧹")
            st.rerun()


def render_account(
    market: str, environment: str, quote: str, snapshot: pd.DataFrame | None
) -> None:
    st.subheader("Account balances")
    st.caption(
        "Signed request to the selected market/environment. Keys are read from "
        "`BINANCE_API_KEY` / `BINANCE_API_SECRET` when present and are never "
        "written to disk."
    )

    with st.form("account_balances"):
        key_column, secret_column = st.columns(2)
        api_key = key_column.text_input(
            "API key",
            value=os.environ.get("BINANCE_API_KEY", ""),
            type="password",
        )
        api_secret = secret_column.text_input(
            "API secret",
            value=os.environ.get("BINANCE_API_SECRET", ""),
            type="password",
        )
        submitted = st.form_submit_button("Load balances", type="primary")

    if submitted:
        if not api_key or not api_secret:
            st.session_state.account_result = {
                "error": "An API key and secret are required for this environment."
            }
        else:
            try:
                base_url = resolve_base_url(market, environment, None)
                with requests.Session() as session:
                    balances = fetch_account_balance(
                        base_url, market, api_key, api_secret, session
                    )
                st.session_state.account_result = {
                    "rows": balances.to_dict("records"),
                    "market": market,
                    "environment": environment,
                    "fetched_at": datetime.now(timezone.utc),
                }
            except BinanceDataError as exc:
                st.session_state.account_result = {"error": str(exc)}

    result = st.session_state.get("account_result")
    if not result:
        st.info("Load balances to see holdings for this market/environment.")
        return

    if "error" in result:
        st.error(f"Could not load balances: {result['error']}")
        return

    balances = pd.DataFrame(result["rows"])
    if balances.empty:
        st.info("No non-zero balances on this account.")
        return

    st.caption(
        f"Loaded for {result.get('market')} / {result.get('environment')}."
    )

    prices: dict[str, float] = {}
    if snapshot is not None and not snapshot.empty:
        prices = {
            str(symbol): float(price)
            for symbol, price in zip(snapshot["symbol"], snapshot["lastPrice"])
        }

    rows = []
    for _, row in balances.iterrows():
        asset = str(row.get("asset") or "")
        amount = 0.0
        for column in ("free", "locked", "balance"):
            if column in balances.columns:
                amount += _to_number(row.get(column)) or 0.0
        if asset == quote:
            price = 1.0
        else:
            price = prices.get(f"{asset}{quote}")
        rows.append(
            {
                "asset": asset,
                "amount": amount,
                "price": price,
                "value": amount * price if price is not None else None,
            }
        )

    holdings = pd.DataFrame(rows)
    holdings["value"] = pd.to_numeric(holdings["value"], errors="coerce")
    holdings = holdings.sort_values(
        "value", ascending=False, na_position="last"
    ).reset_index(drop=True)
    total_value = float(holdings["value"].fillna(0).sum())

    metrics = st.columns(3)
    metrics[0].metric("Assets held", f"{len(holdings):,}")
    metrics[1].metric(f"Estimated value ({quote})", format_price(total_value))
    metrics[2].metric(
        "Fetched (UTC)",
        result["fetched_at"].strftime("%H:%M:%S")
        if isinstance(result.get("fetched_at"), datetime)
        else "—",
    )

    view = holdings.copy()
    view["amount"] = view["amount"].map(format_price)
    view["price"] = view["price"].map(format_price)
    view["value"] = view["value"].map(format_price)
    st.dataframe(
        view,
        width="stretch",
        hide_index=True,
        column_config={
            "asset": st.column_config.TextColumn("Asset"),
            "amount": st.column_config.TextColumn("Amount"),
            "price": st.column_config.TextColumn(f"Price ({quote})"),
            "value": st.column_config.TextColumn(f"Value ({quote})"),
        },
    )
    st.caption(
        "Values use 24h ticker prices from the current scan; assets without a "
        f"{quote} pair show no estimate."
    )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    st.set_page_config(
        page_title="Binance Market Dashboard",
        page_icon="📈",
        layout="wide",
    )
    init_db()

    for key, default in (
        ("scan_nonce", 0),
        ("force_save", False),
        ("account_result", None),
        ("last_scan_at", None),
        ("last_snapshot", None),
        ("tick_last", None),
    ):
        if key not in st.session_state:
            st.session_state[key] = default

    st.title("Binance Market Dashboard")

    with st.sidebar:
        st.header("Controls")
        markets = ["futures", "spot"]
        market = st.selectbox(
            "Market",
            markets,
            index=markets.index(DEFAULT_MARKET) if DEFAULT_MARKET in markets else 0,
        )
        environments = ["demo", "live"]
        environment = st.selectbox(
            "Environment",
            environments,
            index=(
                environments.index(DEFAULT_ENVIRONMENT)
                if DEFAULT_ENVIRONMENT in environments
                else 0
            ),
            help="Public market data is identical on both for spot/futures.",
        )
        quote = st.selectbox("Quote asset", QUOTES, index=0)
        min_volume = st.number_input(
            "Minimum 24h volume",
            min_value=0.0,
            value=1_000_000.0,
            step=500_000.0,
            format="%.0f",
        )
        top_n = st.slider("Rows per table", min_value=5, max_value=50, value=15, step=5)

        auto_save = st.toggle(
            "Save scans to past records",
            value=True,
            help="Each scan stores its most liquid pairs in the local database.",
        )
        save_minutes = st.number_input(
            "Record at most every (minutes)",
            min_value=1,
            max_value=60,
            value=5,
            step=1,
            disabled=not auto_save,
            help="Prevents auto-refresh from flooding the history database.",
        )

        st.divider()
        auto_refresh = st.toggle(
            "Auto-refresh",
            value=True,
            help="Refresh market data automatically while this tab stays open.",
        )
        refresh_seconds = st.slider(
            "Refresh interval (seconds)",
            min_value=30,
            max_value=45,
            value=30,
            step=5,
            disabled=not auto_refresh,
        )

        st.divider()
        scan_clicked = st.button("Scan market", type="primary", width="stretch")
        if st.button("Refresh cached data", width="stretch"):
            load_snapshot.clear()
            load_klines.clear()
            st.rerun()

        st.divider()
        st.caption(
            f"DB: `{DB_PATH.name}` · {MAX_RECORDS_PER_SCAN} rows max per scan"
        )

    if scan_clicked:
        st.session_state.scan_nonce += 1
        st.session_state.force_save = True

    if auto_refresh:
        interval_seconds = int(refresh_seconds)

        @st.fragment(run_every=interval_seconds)
        def _auto_refresh_tick() -> None:
            now = time.time()
            previous = st.session_state.tick_last
            st.session_state.tick_last = now
            # The first render (and every full rerun) only arms the timer;
            # only a timed fragment run triggers a real refresh.
            if not should_auto_refresh(previous, now, interval_seconds):
                return
            st.session_state.scan_nonce += 1
            st.rerun()

        _auto_refresh_tick()

    nonce = st.session_state.scan_nonce
    snapshot: pd.DataFrame | None = None
    snapshot_source: str | None = None
    scan_error: str | None = None
    used_stale = False

    try:
        with st.spinner("Fetching Binance market data…"):
            snapshot, snapshot_source = load_snapshot(
                market, environment, quote, min_volume, nonce
            )
    except BinanceDataError as exc:
        scan_error = str(exc)

    if snapshot is not None:
        captured = datetime.now(timezone.utc)
        st.session_state.last_scan_at = captured
        st.session_state.last_snapshot = {
            "market": market,
            "environment": environment,
            "quote": quote,
            "min_volume": float(min_volume),
            "frame": snapshot,
            "source": snapshot_source,
            "captured_at": captured,
        }

        if auto_save:
            since = seconds_since_last_save(market, environment, quote)
            due = since is None or since >= float(save_minutes) * 60
            if st.session_state.force_save or due:
                try:
                    saved = record_snapshot(snapshot, market, environment, quote)
                    st.session_state.force_save = False
                    st.toast(f"Saved {saved} records to past records", icon="💾")
                except sqlite3.Error as exc:
                    st.warning(f"Could not save history: {exc}")
    elif scan_error:
        cached = st.session_state.last_snapshot
        if (
            cached
            and cached["market"] == market
            and cached["environment"] == environment
            and cached["quote"] == quote
            and cached["min_volume"] == float(min_volume)
        ):
            snapshot = cached["frame"]
            snapshot_source = cached["source"]
            used_stale = True
            st.warning(
                f"Live refresh failed: {scan_error} Showing the last successful "
                f"scan from {cached['captured_at']:%H:%M:%S} UTC."
            )
        else:
            st.error(f"Binance request failed: {scan_error}")

        if st.button("Retry now", key="retry_scan"):
            load_snapshot.clear()
            st.session_state.scan_nonce += 1
            st.rerun()

    triggered = check_alerts()
    for alert in triggered[:3]:
        st.toast(str(alert["message"]), icon="🚨")
    if triggered:
        with st.container(border=True):
            st.markdown("#### 🚨 New alerts")
            for alert in triggered:
                st.markdown(f"- {alert['message']}")

    status = []
    if st.session_state.last_scan_at is not None and not used_stale:
        status.append(
            f"Updated {st.session_state.last_scan_at:%H:%M:%S} UTC"
        )
    if used_stale:
        status.append("showing cached snapshot")
    if snapshot_source and snapshot_source != environment:
        status.append(f"source: {snapshot_source}")
    status.append(
        f"auto-refresh every {int(refresh_seconds)}s"
        if auto_refresh
        else "auto-refresh off"
    )
    st.caption(" · ".join(status))

    overrides = active_dns_overrides()
    if overrides:
        st.caption(
            "DNS fallback in use for "
            + ", ".join(sorted(overrides))
            + " (resolved over HTTPS)"
        )

    scanner_tab, records_tab, average_tab, alerts_tab, account_tab = st.tabs(
        ["Scanner", "Past records", "Moving average", "Alerts", "Account"]
    )

    with scanner_tab:
        if snapshot is None:
            st.info("Scan data unavailable — see the error above.")
        else:
            render_scanner(snapshot, top_n, quote)

    with records_tab:
        render_records(quote, market, environment, auto_save, snapshot)

    with average_tab:
        render_moving_average(snapshot, market, environment, quote)

    with alerts_tab:
        render_alerts(snapshot, market, environment, quote)

    with account_tab:
        render_account(market, environment, quote, snapshot)


if __name__ == "__main__":
    main()
