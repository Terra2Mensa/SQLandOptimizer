#!/usr/bin/env python3
"""CLI for managing business configuration.

With the schema simplification, processors are now in profiles + processor_costs,
regions and scalar params come from config.py constants.
"""
import argparse
import sys
from datetime import date

from config_loader import (
    load_processors, load_regions, load_param,
    _PARAM_DEFAULTS,
)


def _get_conn():
    from db import get_connection
    return get_connection()


# ------------------------------------------------------------------
# Subcommands
# ------------------------------------------------------------------

def cmd_list_processors(args):
    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    procs = load_processors(as_of)
    label = f" (as of {args.as_of})" if args.as_of else ""
    print(f"\nPROCESSORS{label}")
    print("=" * 68)
    print(f"{'Key':<20} {'Name':<20} {'Kill Fee':>9} {'Fab $/lb':>9} {'Shrink':>7}")
    print("-" * 68)
    for key, p in sorted(procs.items()):
        print(f"{key:<20} {p['name']:<20} ${p['kill_fee']:>7,.2f} "
              f"${p['fab_cost_per_lb']:>7.4f} {p['shrink_pct']:>6.1%}")
    print()


def cmd_list_regions(args):
    regions = load_regions()
    print(f"\nREGIONS")
    print("=" * 65)
    print(f"{'Key':<22} {'Label':<22} {'City':<14} {'St':<4} {'Adj':>7}")
    print("-" * 65)
    for key, r in sorted(regions.items()):
        print(f"{key:<22} {r['label']:<22} {r['city']:<14} {r['state']:<4} "
              f"{r['pricing_adjustment']:>6.2%}")
    print()


def cmd_list_params(args):
    print(f"\nPARAMETERS (from config.py)")
    print("=" * 55)
    print(f"{'Key':<28} {'Value':>12}")
    print("-" * 55)
    for key in sorted(_PARAM_DEFAULTS):
        val = load_param(key)
        print(f"{key:<28} {val:>12.4f}")
    print()


# ------------------------------------------------------------------
# CLI setup
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="View business configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", help="Subcommand")

    p = sub.add_parser("list-processors", help="List processors")
    p.add_argument("--as-of", default=None, help="Effective date (YYYY-MM-DD)")

    sub.add_parser("list-regions", help="List regions")

    sub.add_parser("list-params", help="List scalar parameters")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "list-processors": cmd_list_processors,
        "list-regions": cmd_list_regions,
        "list-params": cmd_list_params,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
