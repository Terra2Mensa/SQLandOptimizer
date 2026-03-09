# Setup — Multi-Species Valuation Engine

## Prerequisites

- Python 3.10+
- PostgreSQL (any recent version — 14, 15, 16, 17)
- macOS or Linux

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/TerraMensa95/Business-Files.git cattle-valuation
cd cattle-valuation
```

### 2. Install Python dependencies

```bash
pip3 install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:
- `MARS_API_KEY` — your USDA MARS API key
- `DB_USER` — your PostgreSQL username (usually your system username)
- `DB_PASSWORD` — your PostgreSQL password (leave blank if using local trust auth)

### 4. Set up the database

```bash
./setup_db.sh
```

This creates the `cattle_valuation` database and initializes all tables.

### 5. Run a valuation

```bash
cd src
python3 cattle_valuation.py --all-grades --save-db
python3 pork_valuation.py --save-db
python3 lamb_valuation.py --save-db
```

### 6. (Optional) Set up daily automation

```bash
./setup_schedule.sh
```

This installs a daily 12:30 PM scheduled run (launchd on macOS, cron on Linux).

## Manual-Entry Species

Chicken and goat have no USDA API — enter prices manually:

```bash
cd src
python3 manual_entry.py chicken
python3 manual_entry.py goat
```

## Budget Workbook

Generate a flexible budget Excel workbook seeded with latest DB prices:

```bash
cd src
python3 budget_workbook.py
python3 budget_workbook.py --start-month 2026-04
python3 budget_workbook.py --output ~/Desktop/budget.xlsx
```

Output goes to `reports/` by default.

## Project Structure

```
src/            Python source (all scripts run from here)
sql/            SQL migration scripts
data/           JSON data files (buyers, manual prices)
reports/        Generated Excel reports (gitignored)
logs/           Daily run logs (gitignored, 30-day retention)
```
