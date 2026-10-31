# Binance Market Dashboard

A Python Streamlit dashboard for scanning Binance markets and analyzing candlestick data. It reads live ticker data through `binancedata.py`, saves scan history to a local SQLite database (`data/binance_history.sqlite3`), and offers moving-average charts plus persistent alert rules.

## Requirements

- Python 3.9+
- Python packages listed in `requirements.txt` (pandas, streamlit, plotly, requests, certifi, python-dotenv)

## Setup

### 1. Clone or open the repository

```bash
cd project1
```

### 2. Create and activate a virtual environment

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows (PowerShell):

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure

Copy the example environment file and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Binance (public market data needs no key; --account needs demo/live keys)
BINANCE_API_KEY=your-binance-key-here
BINANCE_API_SECRET=your-binance-secret-here
BINANCE_ENV=demo
BINANCE_MARKET=futures

# DNS handling for Binance domains: auto (retry over DoH after a failed connection),
# doh (start with DoH), or system (never use DoH).
# BINANCE_DNS_MODE=auto
# BINANCE_DOH_URL=https://cloudflare-dns.com/dns-query

# Optional: where the dashboard stores its past-records database
# BINANCE_HISTORY_DB=data/binance_history.sqlite3
```

Keep `.env` secret and never commit it to Git.

## Usage

Run the dashboard:

```bash
python -m streamlit run binance_dashboard.py
```

The browser opens at `http://localhost:8501`.

`binance_dashboard.py` is a Python web frontend for `binancedata.py`. It has five tabs:

| Tab | What it shows |
| --- | --- |
| **Scanner** | Live 24h gainers, losers, biggest moves and highest-volume pairs, with CSV export. |
| **Past records** | Every scan is saved to a local SQLite database (`data/binance_history.sqlite3`). Filter by quote, symbol and time range, overlay several symbols (absolute or % change) with a moving average, and export to CSV. |
| **Moving average** | Candlestick chart for any symbol with configurable SMA/EMA overlays (7–200 periods), volume bars, distance-to-average metrics and a bullish/bearish read-out. |
| **Alerts** | Persistent MA-cross, RSI-level and volume-spike rules, checked on every refresh; each trigger is timestamped in a log you can export. |
| **Account** | Signed balance lookup for the selected market/environment, with an estimated value in the quote asset. Keys come from `BINANCE_API_KEY` / `BINANCE_API_SECRET` or the form. |

Public market data needs no API key; use the sidebar to pick market, environment, quote asset, minimum volume and how many rows to display.

## Notes

- **Auto-refresh** reruns the data every 30–45 seconds (sidebar slider) while the tab stays open. Use *Scan market* for an immediate refresh.
- Each scan stores up to 250 of the most liquid pairs, but auto-refresh only records at most every few minutes (configurable, default 5) so the history database does not flood. Turning off *Save scans to past records* stops saving entirely.
- Alert rules are evaluated on each refresh and stored in the same database; triggers are de-duplicated per candle. *Volume spike* rules only fire in the *Above* direction.
- History location can be changed with `BINANCE_HISTORY_DB`.
- The moving-average chart downloads candles from Binance directly; the demo environment is used by default and falls back to live public candles if demo data is unavailable.

## Troubleshooting

**`SSL: CERTIFICATE_VERIFY_FAILED`** — the scripts load CA certificates from `certifi`, so make sure dependencies are installed in the active virtual environment. If it still fails, reinstall:

```bash
pip install --force-reinstall certifi
```

**`Binance request timed out` / `Network error while contacting Binance`** — some ISPs answer DNS for Binance domains with dead addresses, so connections hang until the timeout. `binancedata.py` detects that and re-resolves the host over DNS-over-HTTPS automatically. Choose the behaviour with `BINANCE_DNS_MODE`:

| Value | Behaviour |
| --- | --- |
| `auto` (default) | Use the system resolver first; retry over DoH when a connection fails. |
| `doh` | Resolve over DoH first — fastest when your ISP blocks/poisons Binance DNS. |
| `system` | Never use DoH (`BINANCE_DNS_FALLBACK=0` disables it too). |

```env
BINANCE_DNS_MODE=doh
# BINANCE_DOH_URL=https://cloudflare-dns.com/dns-query
```

The dashboard shows a “DNS fallback in use” note when this happens, keeps the last successful scan on screen if a refresh fails, and offers a **Retry now** button.
