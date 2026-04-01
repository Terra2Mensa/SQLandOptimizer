"""
Terra Mensa Pricing Engine
Computes dynamic customer prices using:
  1. Base price (price_custom)
  2. Share-size modifier (price_modifier)
  3. Seasonal adjustment (seasonal_pricing)
  4. Batch-fill dynamic pricing (batch_pricing_rules)

Also computes farmer payment breakdowns.

Usage:
  from pricing_engine import PricingEngine
  engine = PricingEngine(conn)
  price = engine.compute_customer_price('cattle', 'quarter')
  farmer = engine.compute_farmer_payment(slaughter_order_data)
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, date
from decimal import Decimal


class PricingEngine:
    """Stateful pricing engine that loads rules from the database."""

    def __init__(self, conn):
        self.conn = conn
        self._load_base_prices()
        self._load_modifiers()
        self._load_seasonal()
        self._load_batch_rules()
        self._load_commodity_base()
        self._load_config()

    def _load_base_prices(self):
        """Load current base prices (most recent effective_date)."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (species, share) species, share, price
                FROM price_custom
                WHERE effective_date <= CURRENT_DATE
                ORDER BY species, share, effective_date DESC
            """)
            self.base_prices = {(r['species'], r['share']): float(r['price']) for r in cur.fetchall()}

    def _load_modifiers(self):
        """Load current share-size modifiers."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (species, share) species, share, modifier
                FROM price_modifier
                WHERE effective_date <= CURRENT_DATE
                ORDER BY species, share, effective_date DESC
            """)
            self.modifiers = {(r['species'], r['share']): float(r['modifier']) for r in cur.fetchall()}

    def _load_seasonal(self):
        """Load seasonal pricing adjustments."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute("SELECT species, month_start, month_end, adjustment, label FROM seasonal_pricing")
                self.seasonal = [dict(r) for r in cur.fetchall()]
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
                self.seasonal = []

    def _load_batch_rules(self):
        """Load batch-fill pricing rules."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute("""
                    SELECT fill_min, fill_max, adjustment, label, stale_days, stale_adjustment
                    FROM batch_pricing_rules ORDER BY fill_min
                """)
                self.batch_rules = [dict(r) for r in cur.fetchall()]
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
                self.batch_rules = []

    def _load_commodity_base(self):
        """Load commodity base prices for farmer payment."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute("""
                    SELECT DISTINCT ON (species) species, price_per_lb
                    FROM commodity_base_prices
                    WHERE effective_date <= CURRENT_DATE
                    ORDER BY species, effective_date DESC
                """)
                self.commodity_base = {r['species']: float(r['price_per_lb']) for r in cur.fetchall()}
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
                self.commodity_base = {}

    def _load_config(self):
        """Load optimizer_config values."""
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT key, value FROM optimizer_config")
            self.config = {r['key']: float(r['value']) for r in cur.fetchall()}

    # ─── Customer Pricing ────────────────────────────────────────────────

    def get_seasonal_adjustment(self, species, month=None):
        """Get seasonal price adjustment for a species and month."""
        if month is None:
            month = datetime.now().month

        for rule in self.seasonal:
            if rule['species'] != species:
                continue
            ms = int(rule['month_start'])
            me = int(rule['month_end'])
            # Handle wrap-around (e.g., Nov-Feb)
            if ms <= me:
                if ms <= month <= me:
                    return float(rule['adjustment'])
            else:
                if month >= ms or month <= me:
                    return float(rule['adjustment'])

        return 1.0  # no seasonal adjustment

    def get_batch_fill_adjustment(self, fill_fraction, batch_age_days=0):
        """Get batch-fill dynamic pricing adjustment.

        Args:
            fill_fraction: 0.0 to 1.0, how full the batch is
            batch_age_days: days since batch was opened

        Returns:
            multiplier (e.g., 0.96 for early-bird, 1.02 for last-share)
        """
        for rule in self.batch_rules:
            fmin = float(rule['fill_min'])
            fmax = float(rule['fill_max'])
            if fmin <= fill_fraction < fmax:
                # Check stale batch
                stale_days = rule.get('stale_days')
                if stale_days and batch_age_days > int(stale_days):
                    stale_adj = rule.get('stale_adjustment')
                    if stale_adj:
                        return float(stale_adj)
                return float(rule['adjustment'])

        return 1.0  # default: no adjustment

    def compute_customer_price(self, species, share, fill_fraction=None,
                                batch_age_days=0, month=None):
        """Compute the full customer price with all adjustments.

        Price = base_price × share_modifier × seasonal × batch_fill

        Returns: dict with breakdown
        """
        base = self.base_prices.get((species, share), 0)
        modifier = self.modifiers.get((species, share), 1.0)
        seasonal = self.get_seasonal_adjustment(species, month)

        batch_fill = 1.0
        if fill_fraction is not None:
            batch_fill = self.get_batch_fill_adjustment(fill_fraction, batch_age_days)

        final_price = round(base * modifier * seasonal * batch_fill, 2)

        return {
            'base_price': base,
            'share_modifier': modifier,
            'seasonal_adjustment': seasonal,
            'batch_fill_adjustment': batch_fill,
            'final_price': final_price,
            'species': species,
            'share': share,
            'per_lb_est': round(final_price / self._est_hanging_weight(species, share), 2)
                          if self._est_hanging_weight(species, share) > 0 else None,
        }

    def _est_hanging_weight(self, species, share):
        """Estimate hanging weight for a share (for per-lb display)."""
        from optimizer_config import SHARE_FRACTIONS, DRESS_PCT
        fraction = SHARE_FRACTIONS.get(share, 0)
        # Typical live weights
        typical_live = {'cattle': 1200, 'pork': 275, 'lamb': 115, 'goat': 90}
        live = typical_live.get(species, 0)
        dress = DRESS_PCT.get(species, 0.60)
        return live * dress * fraction

    # ─── Farmer Payment ──────────────────────────────────────────────────

    def compute_farmer_payment(self, species, hanging_weight, processor_cost,
                                farmer_transport=0):
        """Compute farmer payment breakdown using Nash bargaining model.

        Revenue model:
          gross_revenue = dtc_price_per_lb × hanging_weight
          platform_fee = gross_revenue × platform_fee_pct
          farmer_gross = gross_revenue - platform_fee - processor_cost - farmer_transport
          premium = dtc_price_per_lb - commodity_base_per_lb (per lb)

        Milestone payments:
          milestone_1 = 90% of farmer_gross (on delivery to processor)
          milestone_2 = 10% of farmer_gross (on hanging weight confirmation)
        """
        platform_fee_pct = self.config.get('platform_fee_pct', 0.10)
        m1_pct = self.config.get('farmer_milestone_1_pct', 0.90)
        m2_pct = self.config.get('farmer_milestone_2_pct', 0.10)

        # DTC price per lb: use half-share base as the reference
        base_half = self.base_prices.get((species, 'half'), 0)
        modifier_half = self.modifiers.get((species, 'half'), 1.0)
        # Estimate per-lb from half price ÷ half hanging weight
        from optimizer_config import DRESS_PCT
        typical_live = {'cattle': 1200, 'pork': 275, 'lamb': 115, 'goat': 90}
        half_hw = typical_live.get(species, 0) * DRESS_PCT.get(species, 0.60) * 0.5
        dtc_per_lb = (base_half * modifier_half / half_hw) if half_hw > 0 else 0

        commodity_per_lb = self.commodity_base.get(species, 0)

        gross_revenue = round(dtc_per_lb * hanging_weight, 2)
        platform_fee = round(gross_revenue * platform_fee_pct, 2)
        farmer_gross = round(gross_revenue - platform_fee - processor_cost - farmer_transport, 2)
        farmer_gross = max(farmer_gross, 0)  # floor at zero

        premium_per_lb = round(dtc_per_lb - commodity_per_lb, 2)
        premium_total = round(premium_per_lb * hanging_weight, 2)

        milestone_1 = round(farmer_gross * m1_pct, 2)
        milestone_2 = round(farmer_gross * m2_pct, 2)

        return {
            'species': species,
            'hanging_weight': hanging_weight,
            'dtc_price_per_lb': round(dtc_per_lb, 2),
            'commodity_base_per_lb': commodity_per_lb,
            'premium_per_lb': premium_per_lb,
            'gross_revenue': gross_revenue,
            'platform_fee': platform_fee,
            'platform_fee_pct': platform_fee_pct,
            'processor_cost': processor_cost,
            'farmer_transport': farmer_transport,
            'farmer_gross': farmer_gross,
            'milestone_1': milestone_1,
            'milestone_1_pct': m1_pct,
            'milestone_2': milestone_2,
            'milestone_2_pct': m2_pct,
            'farmer_pct_of_retail': round(farmer_gross / gross_revenue * 100, 1) if gross_revenue > 0 else 0,
        }

    def compute_payment_processing_fee(self, amount, method='card'):
        """Compute payment processing fee by method.

        Card (Stripe): 2.9% + $0.30
        ACH (Stripe):  0.8%, capped at $5.00
        Check:         $0.00
        """
        if method == 'card':
            return round(amount * 0.029 + 0.30, 2)
        elif method == 'ach':
            return min(round(amount * 0.008, 2), 5.00)
        elif method == 'check':
            return 0.00
        return 0.00

    # ─── Price Display Helpers ───────────────────────────────────────────

    def get_all_prices(self, species, fill_fraction=None, batch_age_days=0):
        """Get prices for all share sizes of a species. Used by the website."""
        shares = {
            'cattle': ['whole', 'half', 'quarter', 'eighth'],
            'pork': ['whole', 'half', 'quarter'],
            'lamb': ['whole', 'half', 'uncut'],
            'goat': ['whole', 'half', 'uncut'],
        }
        results = {}
        for share in shares.get(species, []):
            results[share] = self.compute_customer_price(
                species, share, fill_fraction, batch_age_days
            )
        return results

    def get_price_comparison(self, species, share):
        """Show price across different payment methods and batch states."""
        base = self.compute_customer_price(species, share)
        early = self.compute_customer_price(species, share, fill_fraction=0.10)
        standard = self.compute_customer_price(species, share, fill_fraction=0.50)
        last = self.compute_customer_price(species, share, fill_fraction=0.80)
        stale = self.compute_customer_price(species, share, fill_fraction=0.50, batch_age_days=25)

        price = base['final_price']
        return {
            'base': base,
            'early_bird': early,
            'standard': standard,
            'last_share': last,
            'stale_close_out': stale,
            'card_fee': self.compute_payment_processing_fee(price, 'card'),
            'ach_fee': self.compute_payment_processing_fee(price, 'ach'),
            'card_savings_vs_ach': round(
                self.compute_payment_processing_fee(price, 'card') -
                self.compute_payment_processing_fee(price, 'ach'), 2
            ),
        }


# ─── CLI Demo ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '.')
    from optimizer_config import get_connection

    use_supabase = '--supabase' in sys.argv
    conn = get_connection(use_supabase=use_supabase)

    try:
        engine = PricingEngine(conn)
    except Exception as e:
        print(f"Error loading pricing (tables may not exist yet): {e}")
        print("Run sql/019_phase3_economics.sql first.")
        conn.close()
        sys.exit(1)

    print("═══ Terra Mensa Pricing Engine ═══\n")

    # Show all prices
    for species in ['cattle', 'pork', 'lamb', 'goat']:
        print(f"── {species.upper()} ──")
        prices = engine.get_all_prices(species)
        for share, data in prices.items():
            seasonal = f" × {data['seasonal_adjustment']:.2f} seasonal" if data['seasonal_adjustment'] != 1.0 else ""
            print(f"  {share:<10} ${data['final_price']:>8,.2f}  "
                  f"(base ${data['base_price']:,.0f} × {data['share_modifier']:.2f}{seasonal})"
                  f"  ~${data['per_lb_est']}/lb" if data['per_lb_est'] else "")

        # Show batch-fill dynamics for the most common share
        test_share = 'quarter' if species == 'cattle' else 'half'
        comp = engine.get_price_comparison(species, test_share)
        print(f"\n  Batch-fill dynamics for {test_share}:")
        print(f"    Early-bird (10% full):  ${comp['early_bird']['final_price']:>8,.2f}  ({comp['early_bird']['batch_fill_adjustment']:.2f}x)")
        print(f"    Standard (50% full):    ${comp['standard']['final_price']:>8,.2f}  ({comp['standard']['batch_fill_adjustment']:.2f}x)")
        print(f"    Last-share (80% full):  ${comp['last_share']['final_price']:>8,.2f}  ({comp['last_share']['batch_fill_adjustment']:.2f}x)")
        print(f"    Stale close-out (>21d): ${comp['stale_close_out']['final_price']:>8,.2f}  ({comp['stale_close_out']['batch_fill_adjustment']:.2f}x)")

        print(f"\n  Payment fees on ${comp['base']['final_price']:,.2f}:")
        print(f"    Card (Stripe 2.9%):  ${comp['card_fee']:>6,.2f}")
        print(f"    ACH  (Stripe 0.8%):  ${comp['ach_fee']:>6,.2f}")
        print(f"    Savings with ACH:    ${comp['card_savings_vs_ach']:>6,.2f}")
        print()

    # Farmer payment example
    print("── FARMER PAYMENT (Cattle Example) ──")
    farmer = engine.compute_farmer_payment(
        species='cattle',
        hanging_weight=720,  # typical beef
        processor_cost=712,  # kill fee + fab
        farmer_transport=40,
    )
    print(f"  Hanging weight:    {farmer['hanging_weight']} lbs")
    print(f"  DTC price/lb:      ${farmer['dtc_price_per_lb']}")
    print(f"  Commodity base/lb: ${farmer['commodity_base_per_lb']}")
    print(f"  Premium/lb:        ${farmer['premium_per_lb']}")
    print(f"  Gross revenue:     ${farmer['gross_revenue']:,.2f}")
    print(f"  Platform fee:      -${farmer['platform_fee']:,.2f} ({farmer['platform_fee_pct']*100:.0f}%)")
    print(f"  Processor cost:    -${farmer['processor_cost']:,.2f}")
    print(f"  Farmer transport:  -${farmer['farmer_transport']:,.2f}")
    print(f"  ─────────────────────────────")
    print(f"  Farmer gross:      ${farmer['farmer_gross']:,.2f} ({farmer['farmer_pct_of_retail']}% of retail)")
    print(f"  Milestone 1 (90%): ${farmer['milestone_1']:,.2f} (on delivery to processor)")
    print(f"  Milestone 2 (10%): ${farmer['milestone_2']:,.2f} (on hanging weight confirm)")

    conn.close()
