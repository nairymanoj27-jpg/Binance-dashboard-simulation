# OBSIDIAN · Market Terminal

A premium dark-themed market terminal built on the existing **Python / Streamlit / Plotly / SQLite** stack. It reads live Binance ticker and candle data through `binancedata.py`, stores scan history locally, and adds a Gemini-powered AI assistant.

The interface follows the Obsidian Chrome identity:

- **Obsidian Black** `#080808` — application and navigation surfaces.
- **Chrome Silver** `#CDCBCA` — typography, borders and secondary UI.
- **Laser Crimson** `#D41514` — active navigation, primary actions and accents.

Positive/negative market values use conventional green/red indicators, separate from the brand accent.

## Requirements

- Python 3.9+
- Packages in `requirements.txt` (pandas, streamlit, plotly, requests, certifi, python-dotenv)

## Setup

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1      # Windows PowerShell
pip install -r requirements.txt
```

Copy the environment template and fill in your values:

```bash
cp .env.example .env
```

```env
BINANCE_API_KEY=your-binance-key-here
BINANCE_API_SECRET=your-binance-secret-here
BINANCE_ENV=demo
BINANCE_MARKET=futures

# Optional: Gemini API key for the AI Trading Assistant
GEMINI_API_KEY=your-gemini-key-here
# Model override; defaults to gemini-3.5-flash with automatic fallbacks
GEMINI_MODEL=gemini-3.5-flash

# Optional DNS handling
# BINANCE_DNS_MODE=auto
# BINANCE_DOH_URL=https://cloudflare-dns.com/dns-query
# BINANCE_HISTORY_DB=data/binance_history.sqlite3
```

Keep `.env` secret and never commit it.

## Run

```bash
python -m streamlit run binance_dashboard.py
```

The browser opens at `http://localhost:8501`.

## Navigation

The app uses a horizontal top navigation bar with a sticky Obsidian header:

| Page | What it shows |
| --- | --- |
| **Dashboard** | Portfolio summary cards, market breadth, volume leaders, top gainers/losers, watchlist, open positions and recent scans. |
| **Markets** | Live 24h gainers, losers, biggest moves and highest-volume pairs, with CSV export. |
| **Trade** | Candlestick chart with SMA/EMA overlays, volume, trend readout, market snapshot and a clearly labelled order-ticket preview. |
| **Watchlist** | Session watchlist with live prices, 24h changes and a comparison chart. |
| **Portfolio** | Signed balance lookup for the selected market/environment, with estimated values in the quote asset. |
| **Orders** | Honest empty state — no order execution backend is connected in this build. |
| **Records** | Every scan is saved to a local SQLite database (`data/binance_history.sqlite3`). Filter, overlay, chart and export historical prices. |
| **Alerts** | Persistent MA-cross, RSI-level and volume-spike rules with a timestamped trigger log. |
| **News** | Honest empty state — no news feed is connected in this build. |
| **Chat** | Gemini-powered AI Trading Assistant with conversation history and an editable system prompt. |

The top-right icons provide global symbol search, notifications, the AI assistant, profile/environment info and a dark/light theme switcher.

## Notes

- **Auto-refresh** reruns the data every 30–45 seconds (set in the top **Refresh** control) while the page stays open. Use **Scan Market** for an immediate refresh.
- Each scan stores up to 250 of the most liquid pairs, but auto-refresh records at most every few minutes (configurable, default 5).
- Alert rules are evaluated on each refresh and stored in the same database; triggers are de-duplicated per candle.
- The moving-average chart downloads candles from Binance directly; the demo environment is used by default and falls back to live public candles if demo data is unavailable.
- Portfolio values populate only after balances are loaded in **Portfolio**; the dashboard does not invent balances or positions.
- Order placement and news are not implemented because the existing backend does not provide those APIs. Their pages show clear empty states rather than simulated data.

## Troubleshooting

**`SSL: CERTIFICATE_VERIFY_FAILED`** — make sure dependencies are installed in the active virtual environment, then reinstall:

```bash
pip install --force-reinstall certifi
```

**`Binance request timed out` / `Network error while contacting Binance`** — some ISPs return dead DNS answers for Binance domains. `binancedata.py` detects this and retries over DNS-over-HTTPS. Choose the behaviour with `BINANCE_DNS_MODE`:

| Value | Behaviour |
| --- | --- |
| `auto` (default) | Use the system resolver first; retry over DoH when a connection fails. |
| `doh` | Resolve over DoH first — fastest when your ISP blocks/poisons Binance DNS. |
| `system` | Never use DoH (`BINANCE_DNS_FALLBACK=0` disables it too). |

```env
BINANCE_DNS_MODE=doh
# BINANCE_DOH_URL=https://cloudflare-dns.com/dns-query
```
