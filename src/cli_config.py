#!/usr/bin/env python3
"""CLI for managing DB-backed business configuration."""
import argparse
import sys
from datetime import date

from config_loader import (
    load_processors, load_regions, load_param, seed_config_to_db,
    _PARAM_DEFAULTS, _PARAM_DESCRIPTIONS,
)


def _get_conn():
    from db import get_connection
    return get_connection()


# ------------------------------------------------------------------
# Subcommands
# ------------------------------------------------------------------

def cmd_seed(args):
    seed_config_to_db()


def cmd_list_processors(args):
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    procs = load_processors(as_of)
    label = f" (as of {args.as_of})" if args.as_of else ""
    print(f"\nPROCESSORS{label}")
    print("=" * 78)
    print(f"{'Key':<18} {'Name':<18} {'Kill Fee':>9} {'Fab $/lb':>9} {'Shrink':>7} {'Terms':>6}")
    print("-" * 78)
    for key, p in sorted(procs.items()):
        print(f"{key:<18} {p['name']:<18} ${p['kill_fee']:>7,.2f} "
              f"${p['fab_cost_per_lb']:>7.4f} {p['shrink_pct']:>6.1%} "
              f"{p['payment_terms_days']:>4}d")
    print()


def cmd_update_processor(args):
    eff = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    procs = load_processors()
    if args.key not in procs:
        print(f"Unknown processor '{args.key}'. Available: {list(procs.keys())}")
        sys.exit(1)

    current = procs[args.key]
    kill_fee = args.kill_fee if args.kill_fee is not None else current["kill_fee"]
    fab_cost = args.fab_cost if args.fab_cost is not None else current["fab_cost_per_lb"]
    shrink = args.shrink_pct if args.shrink_pct is not None else current["shrink_pct"]
    terms = args.payment_terms if args.payment_terms is not None else current["payment_terms_days"]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_processors
                    (processor_key, name, kill_fee, fab_cost_per_lb,
                     shrink_pct, payment_terms_days, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (processor_key, effective_date) DO UPDATE SET
                    kill_fee = EXCLUDED.kill_fee,
                    fab_cost_per_lb = EXCLUDED.fab_cost_per_lb,
                    shrink_pct = EXCLUDED.shrink_pct,
                    payment_terms_days = EXCLUDED.payment_terms_days
            """, (args.key, current["name"], kill_fee, fab_cost,
                  shrink, terms, eff))
        conn.commit()
        print(f"Updated processor '{args.key}' effective {eff}")
    finally:
        conn.close()


def cmd_add_processor(args):
    eff = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_processors
                    (processor_key, name, kill_fee, fab_cost_per_lb,
                     shrink_pct, payment_terms_days, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (processor_key, effective_date) DO UPDATE SET
                    name = EXCLUDED.name,
                    kill_fee = EXCLUDED.kill_fee,
                    fab_cost_per_lb = EXCLUDED.fab_cost_per_lb,
                    shrink_pct = EXCLUDED.shrink_pct,
                    payment_terms_days = EXCLUDED.payment_terms_days
            """, (args.key, args.name, args.kill_fee, args.fab_cost,
                  args.shrink_pct, args.payment_terms, eff))
        conn.commit()
        print(f"Added processor '{args.key}' effective {eff}")
    finally:
        conn.close()


def cmd_list_regions(args):
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    regions = load_regions(as_of)
    label = f" (as of {args.as_of})" if args.as_of else ""
    print(f"\nREGIONS{label}")
    print("=" * 65)
    print(f"{'Key':<22} {'Label':<22} {'City':<14} {'St':<4} {'Adj':>7}")
    print("-" * 65)
    for key, r in sorted(regions.items()):
        print(f"{key:<22} {r['label']:<22} {r['city']:<14} {r['state']:<4} "
              f"{r['pricing_adjustment']:>6.2%}")
    print()


def cmd_update_region(args):
    eff = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    regions = load_regions()
    if args.key not in regions:
        print(f"Unknown region '{args.key}'. Available: {list(regions.keys())}")
        sys.exit(1)

    current = regions[args.key]
    adj = args.pricing_adjustment if args.pricing_adjustment is not None else current["pricing_adjustment"]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_regions
                    (region_key, label, city, state,
                     pricing_adjustment, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (region_key, effective_date) DO UPDATE SET
                    pricing_adjustment = EXCLUDED.pricing_adjustment
            """, (args.key, current["label"], current["city"],
                  current["state"], adj, eff))
        conn.commit()
        print(f"Updated region '{args.key}' effective {eff}")
    finally:
        conn.close()


def cmd_add_region(args):
    eff = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_regions
                    (region_key, label, city, state,
                     pricing_adjustment, effective_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (region_key, effective_date) DO UPDATE SET
                    label = EXCLUDED.label, city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    pricing_adjustment = EXCLUDED.pricing_adjustment
            """, (args.key, args.label, args.city, args.state,
                  args.pricing_adjustment, eff))
        conn.commit()
        print(f"Added region '{args.key}' effective {eff}")
    finally:
        conn.close()


def cmd_list_params(args):
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    label = f" (as of {args.as_of})" if args.as_of else ""
    print(f"\nPARAMETERS{label}")
    print("=" * 65)
    print(f"{'Key':<28} {'Value':>12} {'Description'}")
    print("-" * 65)
    for key in sorted(_PARAM_DEFAULTS):
        val = load_param(key, as_of_date=as_of)
        desc = _PARAM_DESCRIPTIONS.get(key, "")
        print(f"{key:<28} {val:>12.4f}   {desc}")
    print()


def cmd_set_param(args):
    eff = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    desc = _PARAM_DESCRIPTIONS.get(args.key, "")
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config_parameters
                    (param_key, param_value, description, effective_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (param_key, effective_date) DO UPDATE SET
                    param_value = EXCLUDED.param_value,
                    description = EXCLUDED.description
            """, (args.key, args.value, desc, eff))
        conn.commit()
        print(f"Set {args.key} = {args.value} effective {eff}")
    finally:
        conn.close()


def cmd_history(args):
    type_map = {
        "processor": ("config_processors", "processor_key",
                       ["name", "kill_fee", "fab_cost_per_lb", "shrink_pct",
                        "payment_terms_days", "effective_date"]),
        "region": ("config_regions", "region_key",
                    ["label", "city", "state", "pricing_adjustment",
                     "effective_date"]),
        "param": ("config_parameters", "param_key",
                   ["param_value", "description", "effective_date"]),
    }
    if args.type not in type_map:
        print(f"Unknown type '{args.type}'. Use: processor, region, param")
        sys.exit(1)

    table, key_col, cols = type_map[args.type]
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            col_list = ", ".join(cols)
            cur.execute(
                f"SELECT {col_list} FROM {table} "
                f"WHERE {key_col} = %s ORDER BY effective_date DESC LIMIT %s",
                (args.key, args.limit),
            )
            rows = cur.fetchall()
            if not rows:
                print(f"No history for {args.type} '{args.key}'")
                return
            print(f"\nHISTORY — {args.type} '{args.key}' (newest first)")
            print("=" * 70)
            header = "  ".join(f"{c:<16}" for c in cols)
            print(header)
            print("-" * 70)
            for row in rows:
                vals = "  ".join(f"{str(v):<16}" for v in row)
                print(vals)
            print()
    finally:
        conn.close()


# ------------------------------------------------------------------
# CLI setup
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Manage DB-backed business configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # seed
    sub.add_parser("seed", help="Seed DB from config.py defaults")

    # list-processors
    p = sub.add_parser("list-processors", help="List processors")
    p.add_argument("--as-of", default=None, help="Effective date (YYYY-MM-DD)")

    # update-processor
    p = sub.add_parser("update-processor", help="Update processor config")
    p.add_argument("key", help="Processor key")
    p.add_argument("--kill-fee", type=float, default=None)
    p.add_argument("--fab-cost", type=float, default=None)
    p.add_argument("--shrink-pct", type=float, default=None)
    p.add_argument("--payment-terms", type=int, default=None)
    p.add_argument("--effective-date", default=None, help="YYYY-MM-DD")

    # add-processor
    p = sub.add_parser("add-processor", help="Add a new processor")
    p.add_argument("key", help="Processor key")
    p.add_argument("--name", required=True)
    p.add_argument("--kill-fee", type=float, required=True)
    p.add_argument("--fab-cost", type=float, required=True)
    p.add_argument("--shrink-pct", type=float, required=True)
    p.add_argument("--payment-terms", type=int, required=True)
    p.add_argument("--effective-date", default=None, help="YYYY-MM-DD")

    # list-regions
    p = sub.add_parser("list-regions", help="List regions")
    p.add_argument("--as-of", default=None, help="Effective date (YYYY-MM-DD)")

    # update-region
    p = sub.add_parser("update-region", help="Update region config")
    p.add_argument("key", help="Region key")
    p.add_argument("--pricing-adjustment", type=float, default=None)
    p.add_argument("--effective-date", default=None, help="YYYY-MM-DD")

    # add-region
    p = sub.add_parser("add-region", help="Add a new region")
    p.add_argument("key", help="Region key")
    p.add_argument("--label", required=True)
    p.add_argument("--city", required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--pricing-adjustment", type=float, required=True)
    p.add_argument("--effective-date", default=None, help="YYYY-MM-DD")

    # list-params
    p = sub.add_parser("list-params", help="List scalar parameters")
    p.add_argument("--as-of", default=None, help="Effective date (YYYY-MM-DD)")

    # set-param
    p = sub.add_parser("set-param", help="Set a scalar parameter")
    p.add_argument("key", help="Parameter key")
    p.add_argument("value", type=float, help="New value")
    p.add_argument("--effective-date", default=None, help="YYYY-MM-DD")

    # history
    p = sub.add_parser("history", help="Show effective-date history")
    p.add_argument("type", choices=["processor", "region", "param"])
    p.add_argument("key", help="Key to look up")
    p.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "seed": cmd_seed,
        "list-processors": cmd_list_processors,
        "update-processor": cmd_update_processor,
        "add-processor": cmd_add_processor,
        "list-regions": cmd_list_regions,
        "update-region": cmd_update_region,
        "add-region": cmd_add_region,
        "list-params": cmd_list_params,
        "set-param": cmd_set_param,
        "history": cmd_history,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
