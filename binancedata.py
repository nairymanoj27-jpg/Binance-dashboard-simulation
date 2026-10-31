#!/usr/bin/env python3
"""
binancedata.py — Binance market scanner.

Fetches rolling 24-hour ticker statistics from Binance market data, filters
pairs by quote asset, and reports:
  * Top gainers
  * Top losers
  * Highest quote-volume pairs

Public market data needs no API key. Private account data (``--account``)
requires a Binance API key/secret for the selected environment:





   
Demo trading keys are created at <https://demo.binance.com> (they are separate
from live keys). Live keys are created at
<https://www.binance.com/en/my/settings/api-management>.

Install:
    python -m pip install requests pandas python-dotenv

If Binance requests time out because your ISP returns unusable DNS answers,
requests are retried through DNS-over-HTTPS. Control that with
``BINANCE_DNS_MODE`` (``auto`` | ``doh`` | ``system``) and ``BINANCE_DOH_URL``.

Examples:
    python binancedata.py
    python binancedata.py --top 30 --min-volume 5000000
    python binancedata.py --market spot --env live --top 20
    python binancedata.py --top 20 --csv binance_markets.csv
    python binancedata.py --quote BTC --top 10
    python binancedata.py --account

Frontend (Streamlit dashboard with past records and moving averages):
    python -m streamlit run binance_dashboard.py
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import socket
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

CONNECT_TIMEOUT_SECONDS = 4
READ_TIMEOUT_SECONDS = 45
REQUEST_ATTEMPTS = 3
RECV_WINDOW_MS = 5000

# Some networks/ISPs answer DNS for Binance domains with unreachable addresses.
# "auto" retries through DNS-over-HTTPS after a failed connection, "doh" starts
# with DNS-over-HTTPS (fastest on a poisoned resolver), "system" disables it.
DNS_MODE = os.environ.get("BINANCE_DNS_MODE", "auto").strip().lower()
DNS_FALLBACK_ENABLED = DNS_MODE in {"auto", "doh"} and os.environ.get(
    "BINANCE_DNS_FALLBACK", "1"
).strip().lower() not in {"0", "false", "no", "off"}
DOH_FIRST = DNS_FALLBACK_ENABLED and DNS_MODE == "doh"
DOH_URL = os.environ.get(
    "BINANCE_DOH_URL", "https://cloudflare-dns.com/dns-query"
).strip()
DOH_FALLBACK_URL = "https://dns.google/resolve"

DOH_TIMEOUT_SECONDS = 4

_dns_cache: dict[str, tuple[str, ...]] = {}
_pinned_hosts: dict[str, tuple[str, ...]] = {}
_dns_lock = threading.RLock()

BASE_URLS = {
    ("spot", "live"): "https://api.binance.com",
    ("spot", "demo"): "https://demo-api.binance.com",
    ("futures", "live"): "https://fapi.binance.com",
    ("futures", "demo"): "https://demo-fapi.binance.com",
}

TICKER_24HR_PATHS = {
    "spot": "/api/v3/ticker/24hr",
    "futures": "/fapi/v1/ticker/24hr",
}

EXCHANGE_INFO_PATHS = {
    "spot": "/api/v3/exchangeInfo",
    "futures": "/fapi/v1/exchangeInfo",
}

KLINES_PATHS = {
    "spot": "/api/v3/klines",
    "futures": "/fapi/v1/klines",
}

SUPPORTED_INTERVALS = (
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
)

KLINE_COLUMNS = (
    "openTime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "closeTime",
    "quoteVolume",
    "trades",
    "takerBaseVolume",
    "takerQuoteVolume",
    "ignore",
)

ACCOUNT_PATHS = {
    "spot": "/api/v3/account",
    "futures": "/fapi/v2/balance",
}

DEFAULT_MARKET = os.environ.get("BINANCE_MARKET", "futures")
DEFAULT_ENVIRONMENT = os.environ.get("BINANCE_ENV", "demo")

NUMERIC_COLUMNS = (
    "lastPrice",
    "priceChange",
    "priceChangePercent",
    "weightedAvgPrice",
    "prevClosePrice",
    "lastQty",
    "bidPrice",
    "askPrice",
    "openPrice",
    "highPrice",
    "lowPrice",
    "volume",
    "quoteVolume",
    "openTime",
    "closeTime",
    "firstId",
    "lastId",
    "count",
)

LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

DISPLAY_COLUMNS = (
    "symbol",
    "lastPrice",
    "priceChangePercent",
    "quoteVolume",
    "highPrice",
    "lowPrice",
)


class BinanceDataError(RuntimeError):
    """Raised when Binance market data cannot be retrieved or parsed."""


def resolve_base_url(market: str, environment: str, override: str | None) -> str:
    """Return the REST base URL for a market/environment pair."""
    if override:
        return override.rstrip("/")
    try:
        return BASE_URLS[(market, environment)]
    except KeyError as exc:
        raise BinanceDataError(
            f"Unsupported market/environment: {market}/{environment}"
        ) from exc


def _doh_lookup(host: str) -> tuple[str, ...]:
    """Resolve ``host`` over DNS-over-HTTPS, bypassing a broken local resolver."""
    cached = _dns_cache.get(host)
    if cached:
        return cached

    for endpoint in (DOH_URL, DOH_FALLBACK_URL):
        if not endpoint:
            continue
        try:
            response = requests.get(
                endpoint,
                params={"name": host, "type": "A"},
                headers={"accept": "application/dns-json"},
                timeout=DOH_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            continue

        answers = payload.get("Answer") if isinstance(payload, dict) else None
        if not isinstance(answers, list):
            continue
        addresses = tuple(
            str(answer["data"])
            for answer in answers
            if isinstance(answer, dict)
            and answer.get("type") == 1
            and answer.get("data")
        )
        if addresses:
            _dns_cache[host] = addresses
            return addresses

    return ()


@contextmanager
def _pin_host(host: str, addresses: Sequence[str]) -> Iterator[None]:
    """Temporarily resolve ``host`` to ``addresses`` at the socket layer.

    Only the address lookup is changed; the URL, Host header and TLS SNI are
    untouched, so certificate validation still targets the real hostname.
    """
    with _dns_lock:
        original = socket.getaddrinfo

        def patched(
            hostname: str,
            port: int,
            family: int = 0,
            socktype: int = 0,
            proto: int = 0,
            flags: int = 0,
        ) -> list[Any]:
            if hostname == host:
                resolved: list[Any] = []
                for address in addresses:
                    resolved.extend(
                        original(address, port, family, socktype, proto, flags)
                    )
                return resolved
            return original(hostname, port, family, socktype, proto, flags)

        socket.getaddrinfo = patched  # type: ignore[assignment]
        try:
            yield
        finally:
            socket.getaddrinfo = original  # type: ignore[assignment]


def active_dns_overrides() -> dict[str, tuple[str, ...]]:
    """Hosts currently bypassing the local resolver, for diagnostics."""
    return dict(_pinned_hosts)


def _request_once(
    client: requests.Session,
    url: str,
    headers: dict[str, str] | None,
) -> Any:
    response = client.get(
        url,
        headers=headers,
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
    )
    response.raise_for_status()
    return response.json()


def _request_json(
    client: requests.Session,
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> Any:
    """GET a Binance endpoint, retrying through DoH when DNS is unreliable."""
    host = urlparse(url).hostname or ""
    pinned = _pinned_hosts.get(host, ())
    tried_doh = False
    network_failure = False
    last_error = "unknown error"

    if not pinned and DOH_FIRST and host:
        addresses = _doh_lookup(host)
        if addresses:
            _pinned_hosts[host] = addresses
            pinned = addresses
            tried_doh = True

    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            if pinned:
                with _pin_host(host, pinned):
                    return _request_once(client, url, headers)
            return _request_once(client, url, headers)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            body = exc.response.text[:300] if exc.response is not None else ""
            last_error = f"Binance returned HTTP {status}. {body}".strip()
            if (
                status is not None
                and (status == 418 or status == 429 or status >= 500)
                and attempt < REQUEST_ATTEMPTS
            ):
                time.sleep(float(attempt))
                continue
            raise BinanceDataError(last_error) from exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            network_failure = True
            last_error = (
                "Binance request timed out."
                if isinstance(exc, requests.Timeout)
                else f"Network error while contacting Binance: {exc}"
            )
            if pinned:
                _pinned_hosts.pop(host, None)
                pinned = ()
            if DNS_FALLBACK_ENABLED and host and not tried_doh:
                tried_doh = True
                addresses = _doh_lookup(host)
                if addresses:
                    _pinned_hosts[host] = addresses
                    pinned = addresses
                    continue
            if attempt < REQUEST_ATTEMPTS:
                time.sleep(0.5 * attempt)
                continue
        except ValueError as exc:
            raise BinanceDataError("Binance returned invalid JSON.") from exc
        except requests.RequestException as exc:
            network_failure = True
            last_error = f"Network error while contacting Binance: {exc}"
            break

    hint = ""
    if network_failure and DNS_FALLBACK_ENABLED and host:
        hint = (
            " DNS fallback did not resolve it either; check your internet "
            "connection, VPN or firewall."
        )
    raise BinanceDataError(f"{last_error}{hint}")


def fetch_24hr_tickers(
    base_url: str,
    market: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch 24-hour ticker statistics for all symbols on a market."""
    client = session or requests.Session()
    url = f"{base_url}{TICKER_24HR_PATHS[market]}"
    payload = _request_json(client, url)

    if not isinstance(payload, list) or not payload:
        raise BinanceDataError("Binance returned an empty or unexpected response.")

    df = pd.DataFrame(payload)
    required = {"symbol", "lastPrice", "priceChangePercent", "quoteVolume"}
    missing = required.difference(df.columns)
    if missing:
        raise BinanceDataError(
            "Binance ticker response is missing columns: " + ", ".join(sorted(missing))
        )

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def fetch_klines(
    base_url: str,
    market: str,
    symbol: str,
    interval: str = "1h",
    limit: int = 500,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch OHLCV candles for ``symbol`` as a time-indexed DataFrame."""
    if interval not in SUPPORTED_INTERVALS:
        raise BinanceDataError(
            f"Unsupported interval {interval!r}; choose from "
            + ", ".join(SUPPORTED_INTERVALS)
        )
    if market not in KLINES_PATHS:
        raise BinanceDataError(f"Unsupported market: {market}")

    limit = max(1, min(int(limit), 1000))
    query = urlencode({"symbol": symbol.upper(), "interval": interval, "limit": limit})
    url = f"{base_url}{KLINES_PATHS[market]}?{query}"
    client = session or requests.Session()
    payload = _request_json(client, url)

    if not isinstance(payload, list) or not payload:
        raise BinanceDataError(f"Binance returned no candles for {symbol}.")

    try:
        df = pd.DataFrame(payload, columns=KLINE_COLUMNS)
    except ValueError as exc:
        raise BinanceDataError(
            f"Unexpected kline response for {symbol}."
        ) from exc

    numeric = (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quoteVolume",
        "trades",
        "takerBaseVolume",
        "takerQuoteVolume",
    )
    for column in numeric:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["openTime"] = pd.to_datetime(df["openTime"], unit="ms", utc=True)
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms", utc=True)
    df = df.drop(columns=["ignore"]).sort_values("openTime").reset_index(drop=True)
    return df


def fetch_tradable_symbols(
    base_url: str,
    market: str,
    quote: str,
    session: requests.Session | None = None,
) -> set[str]:
    """Return symbols currently trading and quoted in ``quote``."""
    client = session or requests.Session()
    url = f"{base_url}{EXCHANGE_INFO_PATHS[market]}"
    payload = _request_json(client, url)

    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list):
        raise BinanceDataError("Binance exchangeInfo response is missing symbols.")

    tradable: set[str] = set()
    for item in symbols:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("quoteAsset") != quote:
            continue
        symbol = item.get("symbol")
        if not symbol or "_" in symbol:
            continue
        if market == "spot" and _is_leveraged_token(symbol, quote):
            continue
        tradable.add(symbol)

    if not tradable:
        raise BinanceDataError(f"No trading symbols found for quote asset {quote}.")

    return tradable


def _is_leveraged_token(symbol: str, quote: str) -> bool:
    base = symbol[: -len(quote)] if symbol.endswith(quote) else symbol
    return any(base.endswith(suffix) for suffix in LEVERAGED_SUFFIXES)


def build_snapshot(
    base_url: str,
    market: str,
    quote: str,
    min_volume: float,
    session: requests.Session | None = None,
    tradable: set[str] | frozenset[str] | None = None,
) -> pd.DataFrame:
    """Fetch tickers and keep only liquid, tradable pairs for ``quote``.

    Pass ``tradable`` to reuse an already-fetched symbol list (e.g. a cached
    exchangeInfo response) instead of requesting it again.
    """
    client = session or requests.Session()
    tickers = fetch_24hr_tickers(base_url, market, client)
    symbols = (
        tradable
        if tradable is not None
        else fetch_tradable_symbols(base_url, market, quote, client)
    )

    df = tickers[tickers["symbol"].isin(symbols)].copy()
    df = df[df["quoteVolume"] >= min_volume]
    df = df.dropna(subset=["lastPrice", "priceChangePercent", "quoteVolume"])

    if df.empty:
        raise BinanceDataError(
            f"No {quote} pairs matched the filters "
            f"(min-volume={min_volume:,.0f})."
        )

    return df


def _signed_query(api_secret: str, params: dict[str, Any]) -> str:
    query = urlencode(params)
    signature = hmac.new(
        api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"{query}&signature={signature}"


def fetch_account_balance(
    base_url: str,
    market: str,
    api_key: str,
    api_secret: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch non-zero account balances using a signed request."""
    client = session or requests.Session()
    params = {"timestamp": int(time.time() * 1000), "recvWindow": RECV_WINDOW_MS}
    url = f"{base_url}{ACCOUNT_PATHS[market]}?{_signed_query(api_secret, params)}"

    try:
        payload = _request_json(client, url, headers={"X-MBX-APIKEY": api_key})
    except BinanceDataError as exc:
        raise BinanceDataError(
            f"{exc} (check BINANCE_API_KEY/BINANCE_API_SECRET and that the keys "
            f"belong to the selected {market} environment)"
        ) from exc

    if market == "spot":
        balances = payload.get("balances", []) if isinstance(payload, dict) else []
        rows = [
            {"asset": item.get("asset"), "free": item.get("free"), "locked": item.get("locked")}
            for item in balances
        ]
    else:
        rows = [
            {"asset": item.get("asset"), "balance": item.get("balance")}
            for item in payload
            if isinstance(item, dict)
        ] if isinstance(payload, list) else []

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for column in ("free", "locked", "balance"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    value_columns = [c for c in ("free", "locked", "balance") if c in df.columns]
    return df[(df[value_columns] != 0).any(axis=1)].reset_index(drop=True)


def _print_table(df: pd.DataFrame, title: str, top: int) -> None:
    columns = [c for c in DISPLAY_COLUMNS if c in df.columns]
    table = df[columns].head(top).copy()
    table = table.rename(
        columns={
            "lastPrice": "price",
            "priceChangePercent": "change_%",
            "quoteVolume": "volume",
            "highPrice": "high",
            "lowPrice": "low",
        }
    )
    print(f"\n{title}")
    with pd.option_context("display.width", 120, "display.max_columns", None):
        print(table.to_string(index=False, float_format=lambda value: f"{value:,.4f}"))


def report(df: pd.DataFrame, top: int, quote: str, min_volume: float) -> None:
    """Print gainers, losers and volume leaders."""
    liquid = df[df["quoteVolume"] >= min_volume]
    print(
        f"\n{len(df)} {quote} pairs | min 24h volume {min_volume:,.0f} "
        f"| {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC"
    )
    _print_table(
        liquid.sort_values("priceChangePercent", ascending=False), "Top gainers", top
    )
    _print_table(
        liquid.sort_values("priceChangePercent", ascending=True), "Top losers", top
    )
    _print_table(
        liquid.sort_values("quoteVolume", ascending=False), "Top volume", top
    )


def export_csv(df: pd.DataFrame, path: Path) -> None:
    columns = [c for c in DISPLAY_COLUMNS if c in df.columns]
    df.sort_values("quoteVolume", ascending=False)[columns].to_csv(
        path, index=False
    )
    print(f"\nWrote {len(df)} rows to {path}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Binance 24h tickers for gainers, losers and volume."
    )
    parser.add_argument("--top", type=int, default=20, help="rows per table (default: 20)")
    parser.add_argument(
        "--min-volume",
        type=float,
        default=1_000_000,
        help="minimum 24h quote volume (default: 1000000)",
    )
    parser.add_argument("--quote", default="USDT", help="quote asset (default: USDT)")
    parser.add_argument(
        "--market",
        choices=("spot", "futures"),
        default=DEFAULT_MARKET,
        help=f"market type (default: {DEFAULT_MARKET})",
    )
    parser.add_argument(
        "--env",
        dest="environment",
        choices=("demo", "live"),
        default=DEFAULT_ENVIRONMENT,
        help=f"Binance environment (default: {DEFAULT_ENVIRONMENT})",
    )
    parser.add_argument("--base-url", default=None, help="override the REST base URL")
    parser.add_argument("--csv", type=Path, default=None, help="export filtered data to CSV")
    parser.add_argument(
        "--account",
        action="store_true",
        help="also print account balances (requires API key/secret)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.top < 1:
        print("error: --top must be at least 1", file=sys.stderr)
        return 2
    if args.min_volume < 0:
        print("error: --min-volume must be >= 0", file=sys.stderr)
        return 2

    quote = args.quote.upper()
    base_url = resolve_base_url(args.market, args.environment, args.base_url)
    print(f"Binance {args.market} / {args.environment} -> {base_url}")

    try:
        with requests.Session() as session:
            df = build_snapshot(
                base_url, args.market, quote, args.min_volume, session
            )
            report(df, args.top, quote, args.min_volume)

            if args.csv:
                export_csv(df, args.csv)

            if args.account:
                api_key = os.environ.get("BINANCE_API_KEY")
                api_secret = os.environ.get("BINANCE_API_SECRET")
                if not api_key or not api_secret:
                    raise BinanceDataError(
                        "BINANCE_API_KEY and BINANCE_API_SECRET must be set for --account."
                    )
                balances = fetch_account_balance(
                    base_url, args.market, api_key, api_secret, session
                )
                print("\nAccount balances")
                if balances.empty:
                    print("No non-zero balances.")
                else:
                    print(balances.to_string(index=False))
    except BinanceDataError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
