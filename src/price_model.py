"""
Terra Mensa — Price Model

Computes D2C prices from USDA market data using cost-up chain:
  auction → farmer premium → processing → platform margin → customer price

Reads: weekly_market_prices, processor_costs, optimizer_config
Writes: price_custom (with new effective_date)

Usage: python3 price_model.py [--supabase] [--dry-run]
"""

import sys
from datetime import date
from collections import defaultdict

from optimizer_config import (
    get_connection, load_optimizer_config, get_config,
    get_dress_pct, get_typical_live_weight,
    SHARE_FRACTIONS, SPECIES_LIST,
)


SHARE_CONFIGS = {
    'cattle': ['whole', 'half', 'quarter', 'eighth'],
    'pork':   ['whole', 'half', 'quarter'],
    'lamb':   ['whole', 'half', 'uncut'],
    'goat':   ['whole', 'half', 'uncut'],
}


def get_latest_market_data(conn, species):
    """Get the most recent weekly_market_prices row for a species."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT report_date, quality_grade, live_price_cwt, dressed_price_cwt,
                   cutout_value_cwt, processor_cost_est,
                   typical_live_weight, typical_hanging_weight
            FROM weekly_market_prices
            WHERE species = %s
            ORDER BY report_date DESC, quality_grade
        """, (species,))
        rows = cur.fetchall()
        if not rows:
            return None

        # Group by grade, take most recent
        result = {}
        latest_date = None
        for row in rows:
            rd, grade, live_cwt, dress_cwt, cutout_cwt, proc_cost, live_wt, hang_wt = row
            if latest_date is None:
                latest_date = rd
            if rd != latest_date:
                break
            result[grade] = {
                'report_date': rd,
                'live_price_cwt': float(live_cwt) if live_cwt else None,
                'dressed_price_cwt': float(dress_cwt) if dress_cwt else None,
                'cutout_value_cwt': float(cutout_cwt) if cutout_cwt else None,
                'processor_cost_est': float(proc_cost) if proc_cost else None,
                'typical_live_weight': float(live_wt) if live_wt else None,
                'typical_hanging_weight': float(hang_wt) if hang_wt else None,
            }
        return result


def get_avg_processor_cost(conn, species):
    """Get average processing cost across all processors for a species."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(kill_fee), AVG(fab_cost_per_lb)
            FROM processor_costs
            WHERE species = %s AND kill_fee IS NOT NULL
        """, (species,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0]), float(row[1])
    return None, None


def get_current_prices(conn, species):
    """Get current price_custom values for comparison (guard rail)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (share) share, price
            FROM price_custom
            WHERE species = %s AND effective_date <= CURRENT_DATE
            ORDER BY share, effective_date DESC
        """, (species,))
        return {row[0]: float(row[1]) for row in cur.fetchall()}


def compute_prices(species, market_data, config, avg_kill_fee, avg_fab_per_lb):
    """Compute D2C prices using cost-up chain.

    Returns dict of {share: total_price} and a breakdown dict.
    """
    # Config values
    farmer_premium_pct = get_config(config, 'farmer_premium_pct', 0.14)
    platform_margin_pct = get_config(config, 'platform_fee_pct', 0.25)
    cutout_yield = get_config(config, 'cutout_yield', 0.65)
    dress_pct = get_dress_pct(config, species)
    typical_live = get_typical_live_weight(config, species)

    # Use 'choice' grade for cattle, 'standard' for others
    grade = 'choice' if species == 'cattle' else list(market_data.keys())[0]
    data = market_data.get(grade, list(market_data.values())[0])

    live_cwt = data.get('live_price_cwt')
    hanging_wt = typical_live * dress_pct
    take_home_wt = hanging_wt * cutout_yield

    # Processing cost per lb hanging
    proc_per_lb = (avg_kill_fee + avg_fab_per_lb * hanging_wt) / hanging_wt if hanging_wt > 0 else 0

    # Cost-up chain
    if live_cwt and live_cwt > 0:
        auction_per_lb = live_cwt / 100 / dress_pct
    elif data.get('dressed_price_cwt'):
        auction_per_lb = float(data['dressed_price_cwt']) / 100
    else:
        return None, None  # No price data available

    farmer_per_lb = auction_per_lb * (1 + farmer_premium_pct)
    cost_basis = farmer_per_lb + proc_per_lb
    platform_total = cost_basis * (1 + platform_margin_pct)
    take_home_per_lb = platform_total / cutout_yield if cutout_yield > 0 else 0

    # Store as one $/lb per species (share modifiers applied separately by website)
    prices = {'per_lb': round(take_home_per_lb, 2)}

    breakdown = {
        'species': species,
        'grade': grade,
        'report_date': str(data.get('report_date', '')),
        'live_cwt': live_cwt,
        'auction_per_lb_hanging': round(auction_per_lb, 4),
        'farmer_premium_pct': farmer_premium_pct,
        'farmer_per_lb_hanging': round(farmer_per_lb, 4),
        'proc_per_lb_hanging': round(proc_per_lb, 4),
        'cost_basis_per_lb': round(cost_basis, 4),
        'platform_margin_pct': platform_margin_pct,
        'platform_total_per_lb': round(platform_total, 4),
        'cutout_yield': cutout_yield,
        'take_home_per_lb': round(take_home_per_lb, 2),
        'typical_live_weight': typical_live,
        'typical_hanging_weight': hanging_wt,
        'typical_take_home_weight': take_home_wt,
        'avg_kill_fee': avg_kill_fee,
        'avg_fab_per_lb': avg_fab_per_lb,
    }

    return prices, breakdown


def write_price_custom(conn, species, prices, effective_date, dry_run=False):
    """Write new price_custom rows. Returns count written."""
    written = 0
    with conn.cursor() as cur:
        for share, price in prices.items():
            if dry_run:
                print(f"    [DRY RUN] {species}/{share}: ${price:,.2f}")
            else:
                cur.execute("""
                    INSERT INTO price_custom (species, share, price, effective_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (species, share, effective_date) DO UPDATE SET price = EXCLUDED.price
                """, (species, share, price, effective_date))
            written += 1
    if not dry_run:
        conn.commit()
    return written


def check_guard_rail(current_prices, new_prices, threshold=0.15):
    """Check if any price changed more than threshold (15%). Returns list of warnings."""
    warnings = []
    for share, new_price in new_prices.items():
        old_price = current_prices.get(share)
        if old_price and old_price > 0:
            change = abs(new_price - old_price) / old_price
            if change > threshold:
                warnings.append(
                    f"{share}: ${old_price:,.2f} → ${new_price:,.2f} ({change*100:+.1f}%)"
                )
    return warnings


def run_price_model(use_supabase=False, dry_run=False):
    """Main entry point."""
    conn = get_connection(use_supabase=use_supabase)
    config = load_optimizer_config(conn)

    print("═══ Terra Mensa Price Model ═══")
    print(f"  Farmer premium: {get_config(config, 'farmer_premium_pct', 0.14)*100:.0f}%")
    print(f"  Platform margin: {get_config(config, 'platform_fee_pct', 0.25)*100:.0f}%")
    print(f"  Cutout yield: {get_config(config, 'cutout_yield', 0.65)*100:.0f}%")
    if dry_run:
        print("  *** DRY RUN ***")
    print()

    today = date.today()
    total_written = 0

    for species in SPECIES_LIST:
        print(f"── {species.upper()} ──")

        # Get market data
        market = get_latest_market_data(conn, species)
        if not market:
            print(f"  No market data in weekly_market_prices — skipping")
            continue

        # Get processor costs
        kill_fee, fab_per_lb = get_avg_processor_cost(conn, species)
        if kill_fee is None:
            print(f"  No processor costs — skipping")
            continue

        # Compute prices
        prices, breakdown = compute_prices(species, market, config, kill_fee, fab_per_lb)
        if not prices:
            print(f"  Could not compute prices (missing live/dressed data) — skipping")
            continue

        # Display breakdown
        print(f"  Market data: {breakdown['report_date']} ({breakdown['grade']})")
        print(f"  Auction: ${breakdown['live_cwt']}/cwt → ${breakdown['auction_per_lb_hanging']:.2f}/lb hanging")
        print(f"  + Farmer premium ({breakdown['farmer_premium_pct']*100:.0f}%): ${breakdown['farmer_per_lb_hanging']:.2f}/lb")
        print(f"  + Processing: ${breakdown['proc_per_lb_hanging']:.2f}/lb (kill ${breakdown['avg_kill_fee']:.0f} + fab ${breakdown['avg_fab_per_lb']:.2f}/lb)")
        print(f"  = Cost basis: ${breakdown['cost_basis_per_lb']:.2f}/lb hanging")
        print(f"  + Platform ({breakdown['platform_margin_pct']*100:.0f}%): ${breakdown['platform_total_per_lb']:.2f}/lb hanging")
        print(f"  ÷ Cutout yield ({breakdown['cutout_yield']*100:.0f}%): ${breakdown['take_home_per_lb']:.2f}/lb take-home")
        print()

        # Show per-lb price and estimated share totals
        per_lb = prices['per_lb']
        print(f"  ──> price_custom: ${per_lb}/lb take-home")
        print(f"  Estimated totals (for reference):")
        for share in SHARE_CONFIGS.get(species, ['whole', 'half']):
            fraction = SHARE_FRACTIONS.get(share, 1.0)
            take_home = breakdown['typical_take_home_weight'] * fraction
            modifier = 1.0  # modifier applied by website, not here
            total = per_lb * take_home
            print(f"    {share:<10} {take_home:>6.0f} lbs × ${per_lb}/lb = ${total:>8,.2f}")

        # Guard rail check
        current = get_current_prices(conn, species)
        warnings = check_guard_rail(current, prices)
        if warnings:
            print(f"\n  ⚠️  GUARD RAIL — changes exceed 15%:")
            for w in warnings:
                print(f"    {w}")

        # Write
        written = write_price_custom(conn, species, prices, today, dry_run=dry_run)
        total_written += written
        print()

    print(f"═══ Done: {total_written} price_custom rows {'would be ' if dry_run else ''}written ═══")
    conn.close()


if __name__ == '__main__':
    use_supabase = '--supabase' in sys.argv
    dry_run = '--dry-run' in sys.argv
    run_price_model(use_supabase=use_supabase, dry_run=dry_run)
