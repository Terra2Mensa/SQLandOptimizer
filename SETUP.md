# Terra Mensa — Setup Guide

## Prerequisites

- **Python 3.10+**
- **PostgreSQL 14+** (any recent version works)

## Quick Start

### 1. Install Python dependencies

```bash
pip3 install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:
- `MARS_API_KEY` — get from [USDA MARS portal](https://marsapi.ams.usda.gov)
- `DB_USER` — your PostgreSQL username
- `DB_PASSWORD` — your PostgreSQL password (leave blank if using peer/trust auth)

### 3. Set up the database

```bash
chmod +x setup_db.sh
./setup_db.sh
```

This creates the `terra_mensa` database and initializes all tables. Safe to run multiple times.

### 4. Test a valuation

```bash
cd src
python3 cattle_valuation.py --all-grades
```

You should see a valuation report printed and an Excel file saved to `reports/`.

### 5. Daily automation (optional)

```bash
chmod +x setup_schedule.sh
./setup_schedule.sh
```

- **macOS**: installs a launchd agent that runs at 12:30 PM daily
- **Linux**: offers to add a cron entry

Edit `run_daily.sh` to control which species run and what options are used.

## Species Overview

| Species | Script | Data Source |
|---------|--------|-------------|
| Cattle | `cattle_valuation.py` | USDA DataMart + MARS (automatic) |
| Pork | `pork_valuation.py` | USDA DataMart (automatic) |
| Lamb | `lamb_valuation.py` | USDA DataMart (automatic) |
| Chicken | `chicken_valuation.py` | Manual entry only |
| Goat | `goat_valuation.py` | Manual entry only |

For chicken and goat, enter prices first:

```bash
cd src
python3 manual_entry.py
```

## Project Structure

```
├── src/            Python source (all scripts run from here)
├── sql/            SQL migration scripts
├── data/           JSON data files (buyers, manual prices)
├── reports/        Excel output (gitignored)
├── logs/           Daily run logs (auto-cleaned after 30 days)
├── run_daily.sh    Daily runner (edit to configure)
├── setup_db.sh     Database setup
├── setup_schedule.sh  Automation setup
├── .env.example    Environment template
└── requirements.txt
```

## Troubleshooting

**"psycopg2 not installed"** — Run `pip3 install psycopg2-binary`

**"connection refused"** — Make sure PostgreSQL is running:
- macOS: `brew services start postgresql@17`
- Linux: `sudo systemctl start postgresql`

**"database does not exist"** — Run `./setup_db.sh`

**"MARS API key missing"** — Cattle valuations still work (MARS is only for Indiana auction data). Set the key in `.env` if you need auction prices.
