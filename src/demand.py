"""Demand aggregation, carcass allocation, margin calculation, and output."""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from config import (
    SUBPRIMAL_YIELDS, GROUND_BEEF_PRODUCTS, PRIMAL_ORDER,
    GRADE_RANK, DEFAULT_REGION, DEFAULT_PROCESSOR, REPORTS_DIR,
)
from config_loader import (
    load_processors, load_regions, load_trim_yield_pct, load_broker_fee_pct,
)
from buyers import (
    BuyerProfile, compute_buyer_price, _get_base_price_cwt,
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DemandLine:
    cut_code: str
    description: str
    primal: str
    total_weekly_demand_lbs: float
    num_buyers: int
    highest_price_lb: float
    lowest_price_lb: float
    avg_price_lb: float
    carcass_yield_lbs: float
    supply_ratio: float  # demand / yield (how many animals needed)
    buyer_details: list = field(default_factory=list)


@dataclass
class AllocationLine:
    buyer_id: str
    buyer_name: str
    cut_code: str
    description: str
    lbs_allocated: float
    price_per_lb: float
    line_revenue: float


@dataclass
class AllocationResult:
    quality_grade: str
    hcw: float
    live_weight: float
    allocations: List[AllocationLine] = field(default_factory=list)
    unallocated: list = field(default_factory=list)
    total_revenue: float = 0.0
    unallocated_value: float = 0.0
    farmer_cost: float = 0.0
    processing_cost: float = 0.0
    gross_margin: float = 0.0
    margin_pct: float = 0.0
    byproduct_revenue: float = 0.0
    kill_fee: float = 0.0
    fabrication_cost: float = 0.0
    shrink_cost: float = 0.0
    broker_fee: float = 0.0


@dataclass
class IncomeStatement:
    quality_grade: str
    yield_grade: int
    hcw: float
    live_weight: float
    report_date: str
    region: str

    # Revenue
    allocated_revenue: float = 0.0
    unallocated_revenue: float = 0.0
    byproduct_revenue: float = 0.0
    total_revenue: float = 0.0

    # Revenue detail
    revenue_by_primal: Dict[str, float] = field(default_factory=dict)
    revenue_by_channel: Dict[str, float] = field(default_factory=dict)

    # COGS
    animal_cost: float = 0.0
    kill_fee: float = 0.0
    fabrication_cost: float = 0.0
    shrink_cost: float = 0.0
    total_cogs: float = 0.0

    # Gross profit
    gross_profit: float = 0.0
    gross_margin_pct: float = 0.0

    # Operating expenses
    broker_fee: float = 0.0
    total_opex: float = 0.0

    # Net margin
    net_margin: float = 0.0
    net_margin_pct: float = 0.0

    # For DB persistence
    allocation_detail: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Demand aggregation
# ---------------------------------------------------------------------------

def _cut_description(code: str) -> tuple:
    if code in SUBPRIMAL_YIELDS:
        desc, _, primal = SUBPRIMAL_YIELDS[code]
        return desc, primal
    if code in GROUND_BEEF_PRODUCTS:
        return GROUND_BEEF_PRODUCTS[code]["description"], "Trim"
    return code, "Other"


def aggregate_demand(
    buyers: List[BuyerProfile],
    usda_prices: dict,
    ground_beef_prices: dict,
    hcw: float,
    region: str = DEFAULT_REGION,
    quality_grade: str = "choice",
) -> List[DemandLine]:
    regions = load_regions()
    regional_adj = regions.get(region, regions[DEFAULT_REGION])["pricing_adjustment"]
    grade_rank = GRADE_RANK.get(quality_grade.lower(), 2)
    trim_yield_pct = load_trim_yield_pct()

    # Collect demand per cut across all active, grade-eligible buyers
    cut_demand = {}  # code -> list of {buyer_id, name, volume, price}
    for buyer in buyers:
        if not buyer.active:
            continue
        buyer_grade_rank = GRADE_RANK.get(buyer.min_quality_grade.lower(), 1)
        if grade_rank < buyer_grade_rank:
            continue  # this grade doesn't meet buyer's minimum

        for pref in buyer.cut_preferences:
            base_cwt = _get_base_price_cwt(pref.cut_code, usda_prices, ground_beef_prices)
            if base_cwt <= 0:
                continue
            price_lb = compute_buyer_price(pref.cut_code, pref, base_cwt, regional_adj)

            if pref.cut_code not in cut_demand:
                cut_demand[pref.cut_code] = []
            cut_demand[pref.cut_code].append({
                "buyer_id": buyer.buyer_id,
                "buyer_name": buyer.name,
                "volume_lbs_week": pref.volume_lbs_week,
                "price_lb": price_lb,
            })

    # Build DemandLine objects
    lines = []
    for code, details in cut_demand.items():
        desc, primal = _cut_description(code)
        total_demand = sum(d["volume_lbs_week"] for d in details)
        prices = [d["price_lb"] for d in details]

        # Carcass yield for this cut
        if code in SUBPRIMAL_YIELDS:
            yield_pct = SUBPRIMAL_YIELDS[code][1] / 100.0
            carcass_yield = hcw * yield_pct
        elif code in GROUND_BEEF_PRODUCTS:
            carcass_yield = hcw * trim_yield_pct
        else:
            carcass_yield = 0.0

        supply_ratio = total_demand / carcass_yield if carcass_yield > 0 else 999.0

        lines.append(DemandLine(
            cut_code=code,
            description=desc,
            primal=primal,
            total_weekly_demand_lbs=round(total_demand, 1),
            num_buyers=len(details),
            highest_price_lb=round(max(prices), 2),
            lowest_price_lb=round(min(prices), 2),
            avg_price_lb=round(sum(prices) / len(prices), 2),
            carcass_yield_lbs=round(carcass_yield, 1),
            supply_ratio=round(supply_ratio, 1),
            buyer_details=details,
        ))

    lines.sort(key=lambda x: -x.supply_ratio)
    return lines


def compute_animals_needed(demand_lines: List[DemandLine]) -> dict:
    if not demand_lines:
        return {"animals_needed": 0, "bottleneck": "N/A", "excess": {}}
    worst = max(demand_lines, key=lambda x: x.supply_ratio)
    animals = worst.supply_ratio
    excess = {}
    for line in demand_lines:
        if line.carcass_yield_lbs > 0 and animals > 0:
            produced = line.carcass_yield_lbs * animals
            surplus = produced - line.total_weekly_demand_lbs
            if surplus > 0:
                excess[line.cut_code] = round(surplus, 1)
    return {
        "animals_needed": round(animals, 1),
        "bottleneck": worst.cut_code,
        "bottleneck_desc": worst.description,
        "excess": excess,
    }


# ---------------------------------------------------------------------------
# Carcass allocation
# ---------------------------------------------------------------------------

def allocate_carcass(
    buyers: List[BuyerProfile],
    usda_prices: dict,
    ground_beef_prices: dict,
    valuation,
    purchase_price_cwt: float,
    processor_key: str = DEFAULT_PROCESSOR,
    region: str = DEFAULT_REGION,
) -> AllocationResult:
    processors = load_processors()
    processor = processors[processor_key]
    regions = load_regions()
    regional_adj = regions.get(region, regions[DEFAULT_REGION])["pricing_adjustment"]
    hcw = valuation.hot_carcass_weight
    live_weight = valuation.live_weight
    quality_grade = valuation.quality_grade.lower()
    grade_rank = GRADE_RANK.get(quality_grade, 2)

    allocations = []
    unallocated = []
    total_revenue = 0.0
    unallocated_value = 0.0

    # Build all allocatable cuts: IMPS subprimals + ground products
    all_cuts = {}
    for code, (desc, yield_pct, primal) in SUBPRIMAL_YIELDS.items():
        cut_weight = hcw * (yield_pct / 100.0)
        base_cwt = _get_base_price_cwt(code, usda_prices, ground_beef_prices)
        all_cuts[code] = {
            "desc": desc, "weight": cut_weight, "base_cwt": base_cwt, "primal": primal,
        }

    # Ground beef: split trim proportionally among ground products with demand
    ground_total_weight = hcw * load_trim_yield_pct()
    ground_codes_with_demand = []
    for gcode in GROUND_BEEF_PRODUCTS:
        if _get_base_price_cwt(gcode, usda_prices, ground_beef_prices) > 0:
            ground_codes_with_demand.append(gcode)
    if not ground_codes_with_demand:
        ground_codes_with_demand = list(GROUND_BEEF_PRODUCTS.keys())

    per_ground = ground_total_weight / len(ground_codes_with_demand) if ground_codes_with_demand else 0
    for gcode in ground_codes_with_demand:
        base_cwt = _get_base_price_cwt(gcode, usda_prices, ground_beef_prices)
        all_cuts[gcode] = {
            "desc": GROUND_BEEF_PRODUCTS[gcode]["description"],
            "weight": per_ground,
            "base_cwt": base_cwt,
            "primal": "Trim",
        }

    # For each cut, allocate to buyers sorted by price (highest first)
    for code, info in all_cuts.items():
        remaining = info["weight"]
        base_cwt = info["base_cwt"]
        desc = info["desc"]
        if remaining <= 0 or base_cwt <= 0:
            continue

        # Find eligible buyers for this cut
        eligible = []
        for buyer in buyers:
            if not buyer.active:
                continue
            buyer_grade_rank = GRADE_RANK.get(buyer.min_quality_grade.lower(), 1)
            if grade_rank < buyer_grade_rank:
                continue
            for pref in buyer.cut_preferences:
                if pref.cut_code == code:
                    price_lb = compute_buyer_price(code, pref, base_cwt, regional_adj)
                    eligible.append({
                        "buyer": buyer,
                        "pref": pref,
                        "price_lb": price_lb,
                    })
                    break

        eligible.sort(key=lambda x: -x["price_lb"])

        for e in eligible:
            if remaining <= 0:
                break
            alloc_lbs = min(e["pref"].volume_lbs_week, remaining)
            line_rev = alloc_lbs * e["price_lb"]
            allocations.append(AllocationLine(
                buyer_id=e["buyer"].buyer_id,
                buyer_name=e["buyer"].name,
                cut_code=code,
                description=desc,
                lbs_allocated=round(alloc_lbs, 1),
                price_per_lb=round(e["price_lb"], 2),
                line_revenue=round(line_rev, 2),
            ))
            total_revenue += line_rev
            remaining -= alloc_lbs

        # Remainder at wholesale
        if remaining > 0.1:
            wholesale_lb = (base_cwt / 100.0) * regional_adj
            uv = remaining * wholesale_lb
            unallocated.append({
                "cut_code": code,
                "description": desc,
                "lbs": round(remaining, 1),
                "wholesale_lb": round(wholesale_lb, 2),
                "value": round(uv, 2),
            })
            unallocated_value += uv

    # Margin calculation
    farmer_cost = purchase_price_cwt * live_weight / 100.0
    kill_fee_val = processor["kill_fee"]
    fab_total = processor["fab_cost_per_lb"] * hcw
    shrink_cost_val = processor["shrink_pct"] * valuation.total_cut_value
    processing_cost = kill_fee_val + fab_total + shrink_cost_val
    byproduct_rev = valuation.byproduct_value
    broker_fee_val = valuation.processing_cost
    gross_margin = total_revenue + unallocated_value + byproduct_rev - farmer_cost - processing_cost
    margin_pct = (gross_margin / (total_revenue + unallocated_value + byproduct_rev) * 100) if (total_revenue + unallocated_value + byproduct_rev) > 0 else 0

    return AllocationResult(
        quality_grade=valuation.quality_grade,
        hcw=hcw,
        live_weight=live_weight,
        allocations=allocations,
        unallocated=unallocated,
        total_revenue=round(total_revenue, 2),
        unallocated_value=round(unallocated_value, 2),
        farmer_cost=round(farmer_cost, 2),
        processing_cost=round(processing_cost, 2),
        gross_margin=round(gross_margin, 2),
        margin_pct=round(margin_pct, 1),
        byproduct_revenue=round(byproduct_rev, 2),
        kill_fee=round(kill_fee_val, 2),
        fabrication_cost=round(fab_total, 2),
        shrink_cost=round(shrink_cost_val, 2),
        broker_fee=round(broker_fee_val, 2),
    )


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_demand_report(demand_lines: List[DemandLine], animals_needed: dict, quality_grade: str, region: str):
    regions = load_regions()
    region_label = regions.get(region, regions[DEFAULT_REGION])["label"]
    print(f"\nDEMAND ANALYSIS — {quality_grade.title()} | Region: {region_label}")
    print("=" * 90)
    print(f"{'IMPS':<14} {'Cut':<26} {'Primal':<8} {'Demand/wk':>10} {'Buyers':>7} {'Avg$/lb':>8} {'Yield':>7} {'Ratio':>7}")
    print("-" * 90)

    for line in demand_lines:
        flag = " \u25b2" if line.supply_ratio > 1.0 else ""
        print(f"{line.cut_code:<14} {line.description:<26} {line.primal:<8} "
              f"{line.total_weekly_demand_lbs:>8,.0f} lb {line.num_buyers:>6} "
              f"${line.avg_price_lb:>6.2f} {line.carcass_yield_lbs:>6.1f} "
              f"{line.supply_ratio:>5.1f}x{flag}")

    print("-" * 90)
    an = animals_needed
    print(f"Animals/week to meet all demand: {an['animals_needed']} "
          f"(bottleneck: {an.get('bottleneck_desc', an['bottleneck'])})")
    if an.get("excess"):
        top_excess = sorted(an["excess"].items(), key=lambda x: -x[1])[:5]
        excess_str = ", ".join(f"{c}: {lbs:.0f} lb" for c, lbs in top_excess)
        print(f"Top excess production: {excess_str}")
    print()


def print_allocation_report(result: AllocationResult):
    print(f"\nCARCASS ALLOCATION — {result.quality_grade}, {result.hcw:.0f} lb carcass")
    print("=" * 78)
    print(f"{'Buyer':<22} {'Cut':<18} {'Lbs':>6} {'$/lb':>7} {'Revenue':>10}")
    print("-" * 78)

    for a in sorted(result.allocations, key=lambda x: -x.line_revenue):
        code_desc = f"{a.cut_code} {a.description}"
        if len(code_desc) > 17:
            code_desc = code_desc[:17]
        print(f"{a.buyer_name:<22} {code_desc:<18} {a.lbs_allocated:>5.1f} "
              f"${a.price_per_lb:>6.2f} ${a.line_revenue:>9,.2f}")

    if result.unallocated:
        print("-" * 78)
        for u in result.unallocated:
            print(f"{'Unallocated':<22} {u['cut_code']:<18} {u['lbs']:>5.1f} "
                  f"${u['wholesale_lb']:>6.2f} ${u['value']:>9,.2f}")

    total_rev = result.total_revenue + result.unallocated_value
    print("-" * 78)
    print(f"{'Total Revenue:':<42} ${total_rev:>12,.2f}")
    print(f"{'Farmer Cost:':<42} -${result.farmer_cost:>11,.2f}")
    print(f"{'Processing:':<42} -${result.processing_cost:>11,.2f}")
    margin_sign = "" if result.gross_margin >= 0 else "-"
    print(f"{'MARGIN:':<42} {margin_sign}${abs(result.gross_margin):>11,.2f}  ({result.margin_pct:.1f}%)")
    print()


def print_buyer_list(buyers: List[BuyerProfile]):
    regions = load_regions()
    print(f"\nBUYER DIRECTORY — {len(buyers)} profiles")
    print("=" * 95)
    print(f"{'ID':<16} {'Name':<24} {'Type':<18} {'Region':<16} {'Grade':<8} {'Active':<7} {'Cuts':>5}")
    print("-" * 95)
    for b in buyers:
        region_label = regions.get(b.region, {}).get("label", b.region)
        print(f"{b.buyer_id:<16} {b.name:<24} {b.buyer_type:<18} {region_label:<16} "
              f"{b.min_quality_grade:<8} {'Yes' if b.active else 'No':<7} {len(b.cut_preferences):>5}")
    print()


# ---------------------------------------------------------------------------
# Income statement
# ---------------------------------------------------------------------------

def build_income_statement(
    allocation: AllocationResult,
    valuation,
    purchase_price_cwt: float,
    processor_key: str = DEFAULT_PROCESSOR,
    region: str = DEFAULT_REGION,
) -> IncomeStatement:
    processors = load_processors()
    processor = processors[processor_key]
    hcw = valuation.hot_carcass_weight
    live_weight = valuation.live_weight

    # Revenue
    allocated_rev = allocation.total_revenue
    unallocated_rev = allocation.unallocated_value
    byproduct_rev = valuation.byproduct_value
    total_rev = allocated_rev + unallocated_rev + byproduct_rev

    # Revenue detail by primal
    rev_by_primal = {}
    for a in allocation.allocations:
        code = a.cut_code
        if code in SUBPRIMAL_YIELDS:
            primal = SUBPRIMAL_YIELDS[code][2]
        elif code in GROUND_BEEF_PRODUCTS:
            primal = "Trim"
        else:
            primal = "Other"
        rev_by_primal[primal] = rev_by_primal.get(primal, 0.0) + a.line_revenue
    for u in allocation.unallocated:
        code = u["cut_code"]
        if code in SUBPRIMAL_YIELDS:
            primal = SUBPRIMAL_YIELDS[code][2]
        elif code in GROUND_BEEF_PRODUCTS:
            primal = "Trim"
        else:
            primal = "Other"
        rev_by_primal[primal] = rev_by_primal.get(primal, 0.0) + u["value"]

    # Revenue detail by channel (buyer_type)
    rev_by_channel = {}
    for a in allocation.allocations:
        # Look up buyer_type from buyer_id — we use buyer_name as fallback label
        channel = "buyer"  # default
        rev_by_channel[channel] = rev_by_channel.get(channel, 0.0) + a.line_revenue
    if unallocated_rev > 0:
        rev_by_channel["wholesale"] = unallocated_rev
    if byproduct_rev > 0:
        rev_by_channel["byproduct"] = byproduct_rev

    # COGS
    animal_cost = purchase_price_cwt * live_weight / 100.0
    kill_fee = processor["kill_fee"]
    fab_cost = processor["fab_cost_per_lb"] * hcw
    shrink_cost = processor["shrink_pct"] * valuation.total_cut_value
    total_cogs = animal_cost + kill_fee + fab_cost + shrink_cost

    # Gross profit
    gross_profit = total_rev - total_cogs
    gross_margin_pct = (gross_profit / total_rev * 100) if total_rev > 0 else 0.0

    # Operating expenses
    broker_fee = valuation.processing_cost
    total_opex = broker_fee

    # Net margin
    net_margin = gross_profit - total_opex
    net_margin_pct = (net_margin / total_rev * 100) if total_rev > 0 else 0.0

    # Allocation detail for DB
    alloc_detail = [
        {"buyer_id": a.buyer_id, "buyer_name": a.buyer_name,
         "cut_code": a.cut_code, "lbs": a.lbs_allocated,
         "price_lb": a.price_per_lb, "revenue": a.line_revenue}
        for a in allocation.allocations
    ]

    return IncomeStatement(
        quality_grade=valuation.quality_grade,
        yield_grade=valuation.yield_grade,
        hcw=round(hcw, 1),
        live_weight=round(live_weight, 1),
        report_date=valuation.report_date,
        region=region,
        allocated_revenue=round(allocated_rev, 2),
        unallocated_revenue=round(unallocated_rev, 2),
        byproduct_revenue=round(byproduct_rev, 2),
        total_revenue=round(total_rev, 2),
        revenue_by_primal={k: round(v, 2) for k, v in rev_by_primal.items()},
        revenue_by_channel={k: round(v, 2) for k, v in rev_by_channel.items()},
        animal_cost=round(animal_cost, 2),
        kill_fee=round(kill_fee, 2),
        fabrication_cost=round(fab_cost, 2),
        shrink_cost=round(shrink_cost, 2),
        total_cogs=round(total_cogs, 2),
        gross_profit=round(gross_profit, 2),
        gross_margin_pct=round(gross_margin_pct, 1),
        broker_fee=round(broker_fee, 2),
        total_opex=round(total_opex, 2),
        net_margin=round(net_margin, 2),
        net_margin_pct=round(net_margin_pct, 1),
        allocation_detail=alloc_detail,
    )


def build_income_statement_with_buyers(
    buyers: List[BuyerProfile],
    usda_prices: dict,
    ground_beef_prices: dict,
    valuation,
    purchase_price_cwt: float,
    processor_key: str = DEFAULT_PROCESSOR,
    region: str = DEFAULT_REGION,
) -> IncomeStatement:
    allocation = allocate_carcass(
        buyers, usda_prices, ground_beef_prices,
        valuation, purchase_price_cwt, processor_key, region,
    )

    inc = build_income_statement(
        allocation, valuation, purchase_price_cwt, processor_key, region,
    )

    # Enrich revenue_by_channel with actual buyer types
    rev_by_channel = {}
    buyer_map = {b.buyer_id: b.buyer_type for b in buyers}
    for a in allocation.allocations:
        channel = buyer_map.get(a.buyer_id, "unknown")
        rev_by_channel[channel] = rev_by_channel.get(channel, 0.0) + a.line_revenue
    if allocation.unallocated_value > 0:
        rev_by_channel["wholesale"] = allocation.unallocated_value
    if valuation.byproduct_value > 0:
        rev_by_channel["byproduct"] = valuation.byproduct_value
    inc.revenue_by_channel = {k: round(v, 2) for k, v in rev_by_channel.items()}

    return inc


def print_income_statement(inc: IncomeStatement):
    regions = load_regions()
    region_label = regions.get(inc.region, regions[DEFAULT_REGION])["label"]
    print(f"\nINCOME STATEMENT — {inc.quality_grade.title()} YG{inc.yield_grade} | "
          f"{inc.hcw:.0f} lb carcass | {region_label}")
    print("=" * 64)

    print("REVENUE")
    print(f"  Allocated Sales (buyer-priced)     ${inc.allocated_revenue:>10,.2f}")
    print(f"  Wholesale Remainder                ${inc.unallocated_revenue:>10,.2f}")
    print(f"  Byproduct (hide/offal)             ${inc.byproduct_revenue:>10,.2f}")
    print(f"                                     {'----------':>10}")
    print(f"  TOTAL REVENUE                      ${inc.total_revenue:>10,.2f}")
    print()

    print("COST OF GOODS SOLD")
    print(f"  Animal Purchase                   (${inc.animal_cost:>10,.2f})")
    print(f"  Kill Fee                          (${inc.kill_fee:>10,.2f})")
    print(f"  Fabrication                       (${inc.fabrication_cost:>10,.2f})")
    print(f"  Cooler Shrink                     (${inc.shrink_cost:>10,.2f})")
    print(f"                                     {'----------':>10}")
    print(f"  TOTAL COGS                        (${inc.total_cogs:>10,.2f})")
    print()

    gp_sign = "" if inc.gross_profit >= 0 else "-"
    print(f"GROSS PROFIT                         {gp_sign}${abs(inc.gross_profit):>9,.2f}   ({inc.gross_margin_pct:.1f}%)")
    print()

    print("OPERATING EXPENSES")
    print(f"  Broker Fee ({load_broker_fee_pct():.0%})                   (${inc.broker_fee:>10,.2f})")
    print(f"                                     {'----------':>10}")

    nm_sign = "" if inc.net_margin >= 0 else "-"
    print(f"NET MARGIN                           {nm_sign}${abs(inc.net_margin):>9,.2f}   ({inc.net_margin_pct:.1f}%)")
    print()


def print_income_comparison(statements: dict):
    grades = ["prime", "choice", "select", "grassfed"]
    available = [g for g in grades if g in statements]
    if not available:
        print("No income statements to compare.")
        return

    col_w = 11
    header = f"{'':>28}" + "".join(f"{g.title():>{col_w}}" for g in available)
    print(f"\nINCOME STATEMENT COMPARISON — All Grades")
    print("=" * (28 + col_w * len(available)))
    print(header)

    def row(label, accessor, fmt="money"):
        vals = []
        for g in available:
            v = accessor(statements[g])
            if fmt == "money":
                vals.append(f"${v:>{col_w - 1},.0f}")
            elif fmt == "pct":
                vals.append(f"{v:>{col_w - 1}.1f}%")
            elif fmt == "money_neg":
                vals.append(f"(${abs(v):>{col_w - 3},.0f})")
        print(f"  {label:<26}" + "".join(vals))

    print("REVENUE")
    row("Allocated Sales", lambda s: s.allocated_revenue)
    row("Wholesale", lambda s: s.unallocated_revenue)
    row("Byproduct", lambda s: s.byproduct_revenue)
    row("TOTAL", lambda s: s.total_revenue)

    print("COGS")
    row("Animal Purchase", lambda s: s.animal_cost, "money_neg")
    row("Processing", lambda s: s.kill_fee + s.fabrication_cost + s.shrink_cost, "money_neg")
    row("TOTAL COGS", lambda s: s.total_cogs, "money_neg")

    print("GROSS PROFIT")
    row("", lambda s: s.gross_profit)
    row("Margin %", lambda s: s.gross_margin_pct, "pct")

    print("NET MARGIN")
    row("", lambda s: s.net_margin)
    row("Margin %", lambda s: s.net_margin_pct, "pct")
    print()


