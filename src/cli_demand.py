#!/usr/bin/env python3
"""CLI entry point for demand-side commands: buyer management, demand analysis, allocation."""
import argparse
import sys

from config import (
    BUYER_TYPES, SUBPRIMAL_YIELDS, GROUND_BEEF_PRODUCTS,
    DEFAULT_LIVE_WEIGHT, DEFAULT_YIELD_GRADE, DEFAULT_QUALITY_GRADE,
    DEFAULT_PROCESSOR, DEFAULT_REGION,
    GRADE_RANK,
)
from config_loader import (
    load_processors, load_regions, load_grassfed_premium_cwt,
)
from buyers import (
    BuyerProfile, create_buyer_from_template, compute_all_buyer_prices,
    load_buyers_json, save_buyers_json,
)
from demand import (
    aggregate_demand, compute_animals_needed, allocate_carcass,
    print_demand_report, print_allocation_report, print_buyer_list,
    build_income_statement, build_income_statement_with_buyers,
    print_income_statement, print_income_comparison,
)
from cattle_valuation import (
    fetch_boxed_beef, fetch_slaughter_cattle, fetch_premiums_discounts,
    build_grade_cuts, compute_carcass_value, compute_purchase_prices,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_ground_beef_prices(usda_data: dict) -> dict:
    """Extract ground beef prices from USDA data, keyed by our pseudo-IMPS codes."""
    prices = {}
    for gb in usda_data.get("ground_beef", []):
        desc = gb.get("description", "").lower()
        cwt = gb.get("weighted_avg_cwt", 0)
        if cwt <= 0:
            continue
        if "80" in desc:
            prices["ground_80_20"] = cwt
        elif "73" in desc:
            prices["ground_73_27"] = cwt
        elif "90" in desc:
            prices["ground_90_10"] = cwt
    # Fallback: if we got any price, fill missing with average
    if prices:
        avg = sum(prices.values()) / len(prices)
        for code in GROUND_BEEF_PRODUCTS:
            if code not in prices:
                prices[code] = avg
    return prices


def _load_usda_and_prices(grade: str):
    """Fetch USDA data and build price dicts for the given grade."""
    usda_data = fetch_boxed_beef()
    grassfed_prem = load_grassfed_premium_cwt() if grade.lower() == "grassfed" else 0.0
    usda_prices = build_grade_cuts(usda_data, grade, grassfed_prem)
    ground_prices = _get_ground_beef_prices(usda_data)
    return usda_data, usda_prices, ground_prices


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_list_buyers(args):
    buyers = load_buyers_json()
    if not args.all:
        buyers = [b for b in buyers if b.active]
    if not buyers:
        print("No buyers found. Use 'add-buyer' to create one.")
        return
    print_buyer_list(buyers)


def cmd_add_buyer(args):
    if args.template not in BUYER_TYPES:
        print(f"Unknown template: {args.template}")
        print(f"Available: {', '.join(BUYER_TYPES.keys())}")
        sys.exit(1)
    regions = load_regions()
    if args.region not in regions:
        print(f"Unknown region: {args.region}")
        print(f"Available: {', '.join(regions.keys())}")
        sys.exit(1)

    buyers = load_buyers_json()
    buyer_id = args.id or args.name.lower().replace(" ", "_").replace("'", "")

    # Check for duplicate
    if any(b.buyer_id == buyer_id for b in buyers):
        print(f"Buyer '{buyer_id}' already exists. Use a different --id.")
        sys.exit(1)

    extra_kwargs = {}
    if args.contact_name:
        extra_kwargs["contact_name"] = args.contact_name
    if args.contact_email:
        extra_kwargs["contact_email"] = args.contact_email
    if args.contact_phone:
        extra_kwargs["contact_phone"] = args.contact_phone
    if args.delivery_day:
        extra_kwargs["delivery_day"] = args.delivery_day

    new_buyer = create_buyer_from_template(
        buyer_id=buyer_id,
        name=args.name,
        buyer_type=args.template,
        region=args.region,
        volume_multiplier=args.volume_multiplier,
        **extra_kwargs,
    )
    buyers.append(new_buyer)
    save_buyers_json(buyers)
    print(f"Added buyer: {new_buyer.name} ({new_buyer.buyer_type}) in {regions[args.region]['label']}")
    print(f"  ID: {buyer_id} | Cuts: {len(new_buyer.cut_preferences)} | "
          f"Min grade: {new_buyer.min_quality_grade}")


def _validate_processor_region(args):
    """Validate processor and region args at runtime. Exit on error."""
    processors = load_processors()
    regions = load_regions()
    if hasattr(args, 'processor') and args.processor not in processors:
        print(f"Unknown processor '{args.processor}'. Available: {list(processors.keys())}")
        sys.exit(1)
    if hasattr(args, 'region') and args.region not in regions:
        print(f"Unknown region '{args.region}'. Available: {list(regions.keys())}")
        sys.exit(1)


def cmd_demand_report(args):
    _validate_processor_region(args)
    buyers = load_buyers_json()
    active_buyers = [b for b in buyers if b.active]
    if not active_buyers:
        print("No active buyers. Use 'add-buyer' first.")
        return

    usda_data, usda_prices, ground_prices = _load_usda_and_prices(args.grade)

    valuation = compute_carcass_value(
        usda_data, args.live_weight, args.yield_grade, args.grade,
    )

    demand_lines = aggregate_demand(
        active_buyers, usda_prices, ground_prices,
        valuation.hot_carcass_weight, args.region, args.grade,
    )
    animals = compute_animals_needed(demand_lines)

    print_demand_report(demand_lines, animals, args.grade, args.region)

    if args.output:
        print(f"  Excel output not yet reimplemented (--output ignored)")


def cmd_allocate(args):
    _validate_processor_region(args)
    buyers = load_buyers_json()
    active_buyers = [b for b in buyers if b.active]
    if not active_buyers:
        print("No active buyers. Use 'add-buyer' first.")
        return

    usda_data, usda_prices, ground_prices = _load_usda_and_prices(args.grade)

    valuation = compute_carcass_value(
        usda_data, args.live_weight, args.yield_grade, args.grade,
    )

    # Get purchase price for margin calc
    slaughter_data = fetch_slaughter_cattle()
    premiums_data = fetch_premiums_discounts()
    processor = load_processors()[args.processor]

    pp = compute_purchase_prices(
        valuation, slaughter_data, premiums_data,
        {"slaughter": [], "feeder": []},  # auction_data placeholder
        processor, args.live_weight, args.yield_grade, args.grade,
    )
    # Use cutout-minus-margin as purchase price
    purchase_cwt = pp.cutout_minus_margin_cwt if pp.cutout_minus_margin_cwt > 0 else pp.live_basis_cwt

    allocation = allocate_carcass(
        active_buyers, usda_prices, ground_prices,
        valuation, purchase_cwt, args.processor, args.region,
    )

    # Also show demand context
    demand_lines = aggregate_demand(
        active_buyers, usda_prices, ground_prices,
        valuation.hot_carcass_weight, args.region, args.grade,
    )
    animals = compute_animals_needed(demand_lines)
    print_demand_report(demand_lines, animals, args.grade, args.region)
    print_allocation_report(allocation)

    if args.output:
        print(f"  Excel output not yet reimplemented (--output ignored)")


def cmd_buyer_pricing(args):
    buyers = load_buyers_json()
    buyer = next((b for b in buyers if b.buyer_id == args.buyer_id), None)
    if not buyer:
        print(f"Buyer '{args.buyer_id}' not found.")
        sys.exit(1)

    usda_data, usda_prices, ground_prices = _load_usda_and_prices(args.grade)
    results = compute_all_buyer_prices(buyer, usda_prices, ground_prices, buyer.region)

    region_label = load_regions().get(buyer.region, {}).get("label", buyer.region)
    print(f"\nPRICING — {buyer.name} ({buyer.buyer_type}) | {args.grade.title()} | {region_label}")
    print("=" * 85)
    print(f"{'Cut':<14} {'Description':<26} {'USDA $/cwt':>10} {'Markup':>7} {'Buyer $/lb':>10} {'Vol/wk':>8} {'Rev/wk':>10}")
    print("-" * 85)

    total_rev = 0.0
    for r in results:
        total_rev += r["weekly_revenue"]
        print(f"{r['cut_code']:<14} {r['description']:<26} ${r['base_cwt']:>8.2f} "
              f"{r['markup_pct']:>6.0%} ${r['buyer_price_lb']:>8.2f} "
              f"{r['volume_lbs_week']:>6.0f}lb ${r['weekly_revenue']:>9,.2f}")

    print("-" * 85)
    print(f"{'Total weekly revenue:':<58} ${total_rev:>9,.2f}")
    print()


def cmd_income(args):
    _validate_processor_region(args)
    buyers = load_buyers_json()
    active_buyers = [b for b in buyers if b.active]
    if not active_buyers:
        print("No active buyers. Use 'add-buyer' first.")
        return

    grades_to_run = ["prime", "choice", "select", "grassfed"] if args.all_grades else [args.grade]
    statements = {}

    # Fetch all external data once
    usda_data = fetch_boxed_beef()
    slaughter_data = fetch_slaughter_cattle()
    premiums_data = fetch_premiums_discounts()
    ground_prices = _get_ground_beef_prices(usda_data)
    processor = load_processors()[args.processor]

    for grade in grades_to_run:
        grassfed_prem = load_grassfed_premium_cwt() if grade == "grassfed" else 0.0
        usda_prices = build_grade_cuts(usda_data, grade, grassfed_prem)

        valuation = compute_carcass_value(
            usda_data, args.live_weight, args.yield_grade, grade,
        )

        pp = compute_purchase_prices(
            valuation, slaughter_data, premiums_data,
            {"slaughter": [], "feeder": []},
            processor, args.live_weight, args.yield_grade, grade,
        )
        purchase_cwt = pp.cutout_minus_margin_cwt if pp.cutout_minus_margin_cwt > 0 else pp.live_basis_cwt

        inc = build_income_statement_with_buyers(
            active_buyers, usda_prices, ground_prices,
            valuation, purchase_cwt, args.processor, args.region,
        )
        statements[grade] = inc

    # Print results
    if len(statements) == 1:
        inc = next(iter(statements.values()))
        print_income_statement(inc)
    else:
        # Print detail for each, then comparison
        for grade in grades_to_run:
            if grade in statements:
                print_income_statement(statements[grade])
        print_income_comparison(statements)

    if args.output:
        print(f"  Excel output not yet reimplemented (--output ignored)")


def cmd_edit_buyer(args):
    buyers = load_buyers_json()
    buyer = next((b for b in buyers if b.buyer_id == args.buyer_id), None)
    if not buyer:
        print(f"Buyer '{args.buyer_id}' not found.")
        sys.exit(1)

    editable_fields = [
        "name", "region", "contact_name", "contact_email", "contact_phone",
        "business_name", "address_line1", "address_line2", "zip_code",
        "license_number", "delivery_zone", "delivery_day", "credit_limit",
        "notes", "payment_terms_days",
    ]
    changed = []
    for fld in editable_fields:
        val = getattr(args, fld, None)
        if val is not None:
            if fld in ("credit_limit",):
                val = float(val)
            elif fld in ("payment_terms_days",):
                val = int(val)
            setattr(buyer, fld, val)
            changed.append(fld)

    if not changed:
        print("No fields specified. Use --name, --contact-name, --delivery-day, etc.")
        return

    save_buyers_json(buyers)
    print(f"Updated buyer '{buyer.buyer_id}': {', '.join(changed)}")


def cmd_deactivate_buyer(args):
    buyers = load_buyers_json()
    buyer = next((b for b in buyers if b.buyer_id == args.buyer_id), None)
    if not buyer:
        print(f"Buyer '{args.buyer_id}' not found.")
        sys.exit(1)

    buyer.active = False
    save_buyers_json(buyers)
    print(f"Deactivated buyer: {buyer.name} ({buyer.buyer_id})")


# ---------------------------------------------------------------------------
# CLI setup
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Cattle Demand-Side Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # list-buyers
    p_list = sub.add_parser("list-buyers", help="List buyer profiles")
    p_list.add_argument("--all", action="store_true", help="Include inactive buyers")

    # add-buyer
    p_add = sub.add_parser("add-buyer", help="Add a buyer from template")
    p_add.add_argument("--template", required=True, choices=BUYER_TYPES.keys())
    p_add.add_argument("--name", required=True)
    p_add.add_argument("--id", default=None, help="Custom buyer ID (default: derived from name)")
    p_add.add_argument("--region", default=DEFAULT_REGION)
    p_add.add_argument("--volume-multiplier", type=float, default=1.0,
                       help="Scale default volume (e.g. 2.0 for large buyer)")
    p_add.add_argument("--contact-name", default="")
    p_add.add_argument("--contact-email", default="")
    p_add.add_argument("--contact-phone", default="")
    p_add.add_argument("--delivery-day", default="")

    # demand-report
    p_demand = sub.add_parser("demand-report", help="Aggregate demand analysis")
    p_demand.add_argument("--grade", default=DEFAULT_QUALITY_GRADE,
                          choices=["prime", "choice", "select", "grassfed"])
    p_demand.add_argument("--region", default=DEFAULT_REGION)
    p_demand.add_argument("--live-weight", type=float, default=DEFAULT_LIVE_WEIGHT)
    p_demand.add_argument("--yield-grade", type=int, default=DEFAULT_YIELD_GRADE)
    p_demand.add_argument("--processor", default=DEFAULT_PROCESSOR)
    p_demand.add_argument("--output", default=None, help="Excel output filepath")

    # allocate
    p_alloc = sub.add_parser("allocate", help="Full carcass allocation with margin")
    p_alloc.add_argument("--grade", default=DEFAULT_QUALITY_GRADE,
                         choices=["prime", "choice", "select", "grassfed"])
    p_alloc.add_argument("--region", default=DEFAULT_REGION)
    p_alloc.add_argument("--live-weight", type=float, default=DEFAULT_LIVE_WEIGHT)
    p_alloc.add_argument("--yield-grade", type=int, default=DEFAULT_YIELD_GRADE)
    p_alloc.add_argument("--processor", default=DEFAULT_PROCESSOR)
    p_alloc.add_argument("--output", default=None, help="Excel output filepath")

    # buyer-pricing
    p_price = sub.add_parser("buyer-pricing", help="Show pricing for a specific buyer")
    p_price.add_argument("buyer_id", help="Buyer ID to look up")
    p_price.add_argument("--grade", default=DEFAULT_QUALITY_GRADE,
                         choices=["prime", "choice", "select", "grassfed"])

    # income
    p_income = sub.add_parser("income", help="Income statement (single or all grades)")
    p_income.add_argument("--grade", default=DEFAULT_QUALITY_GRADE,
                          choices=["prime", "choice", "select", "grassfed"])
    p_income.add_argument("--all-grades", action="store_true",
                          help="Run all 4 grades and show comparison")
    p_income.add_argument("--region", default=DEFAULT_REGION)
    p_income.add_argument("--live-weight", type=float, default=DEFAULT_LIVE_WEIGHT)
    p_income.add_argument("--yield-grade", type=int, default=DEFAULT_YIELD_GRADE)
    p_income.add_argument("--processor", default=DEFAULT_PROCESSOR)
    p_income.add_argument("--output", default=None, help="Excel output filepath")

    # edit-buyer
    p_edit = sub.add_parser("edit-buyer", help="Edit buyer profile fields")
    p_edit.add_argument("buyer_id", help="Buyer ID to edit")
    p_edit.add_argument("--name", default=None)
    p_edit.add_argument("--region", default=None)
    p_edit.add_argument("--contact-name", default=None)
    p_edit.add_argument("--contact-email", default=None)
    p_edit.add_argument("--contact-phone", default=None)
    p_edit.add_argument("--business-name", default=None)
    p_edit.add_argument("--address-line1", default=None)
    p_edit.add_argument("--address-line2", default=None)
    p_edit.add_argument("--zip-code", default=None)
    p_edit.add_argument("--license-number", default=None)
    p_edit.add_argument("--delivery-zone", default=None)
    p_edit.add_argument("--delivery-day", default=None)
    p_edit.add_argument("--credit-limit", default=None)
    p_edit.add_argument("--notes", default=None)
    p_edit.add_argument("--payment-terms-days", default=None)

    # deactivate-buyer
    p_deact = sub.add_parser("deactivate-buyer", help="Deactivate a buyer")
    p_deact.add_argument("buyer_id", help="Buyer ID to deactivate")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "list-buyers": cmd_list_buyers,
        "add-buyer": cmd_add_buyer,
        "demand-report": cmd_demand_report,
        "allocate": cmd_allocate,
        "buyer-pricing": cmd_buyer_pricing,
        "income": cmd_income,
        "edit-buyer": cmd_edit_buyer,
        "deactivate-buyer": cmd_deactivate_buyer,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
