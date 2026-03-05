"""Buyer profiles, pricing engine, and JSON persistence."""
import json
import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional

from config import (
    BUYER_TYPES, REGIONS, GROUND_BEEF_PRODUCTS, GRADE_RANK,
    SUBPRIMAL_YIELDS, DEFAULT_REGION,
)

BUYERS_JSON = os.path.join(os.path.dirname(__file__), "buyers.json")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CutPreference:
    cut_code: str
    form: str  # whole_subprimal, steak_cut, ground, etc.
    markup_pct: float = 0.0
    fixed_premium_per_lb: float = 0.0
    volume_lbs_week: float = 0.0
    use_fixed_premium: bool = False


@dataclass
class BuyerProfile:
    buyer_id: str
    name: str
    buyer_type: str
    city: str = ""
    state: str = ""
    region: str = DEFAULT_REGION
    cut_preferences: List[CutPreference] = field(default_factory=list)
    min_quality_grade: str = "select"
    payment_terms_days: int = 30
    active: bool = True
    contact_name: str = ""
    contact_email: str = ""
    contact_phone: str = ""


# ---------------------------------------------------------------------------
# Template-based buyer creation
# ---------------------------------------------------------------------------

def create_buyer_from_template(
    buyer_id: str,
    name: str,
    buyer_type: str,
    region: str = DEFAULT_REGION,
    volume_multiplier: float = 1.0,
    **kwargs,
) -> BuyerProfile:
    template = BUYER_TYPES[buyer_type]
    region_info = REGIONS.get(region, REGIONS[DEFAULT_REGION])
    base_volume = template["weekly_volume_lbs"]

    prefs = []
    num_cuts = len(template["cuts"])
    vol_per_cut = (base_volume * volume_multiplier) / num_cuts if num_cuts else 0

    for code, cut_info in template["cuts"].items():
        prefs.append(CutPreference(
            cut_code=code,
            form=cut_info["form"],
            markup_pct=cut_info["markup_pct"],
            volume_lbs_week=round(vol_per_cut, 1),
        ))

    return BuyerProfile(
        buyer_id=buyer_id,
        name=name,
        buyer_type=buyer_type,
        city=region_info.get("city", ""),
        state=region_info.get("state", ""),
        region=region,
        cut_preferences=prefs,
        min_quality_grade=template["min_grade"],
        payment_terms_days=template["payment_terms_days"],
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Pricing engine
# ---------------------------------------------------------------------------

def compute_buyer_price(
    cut_code: str,
    preference: CutPreference,
    base_price_cwt: float,
    regional_adjustment: float = 1.0,
) -> float:
    base_per_lb = (base_price_cwt / 100.0) * regional_adjustment
    if preference.use_fixed_premium:
        return base_per_lb + preference.fixed_premium_per_lb
    return base_per_lb * (1.0 + preference.markup_pct)


def _get_base_price_cwt(cut_code: str, usda_prices: dict, ground_beef_prices: dict) -> float:
    if cut_code in usda_prices:
        return usda_prices[cut_code].weighted_avg_cwt
    if cut_code in ground_beef_prices:
        return ground_beef_prices[cut_code]
    return 0.0


def compute_all_buyer_prices(
    buyer: BuyerProfile,
    usda_prices: dict,
    ground_beef_prices: dict,
    region: str = DEFAULT_REGION,
) -> list:
    regional_adj = REGIONS.get(region, REGIONS[DEFAULT_REGION])["pricing_adjustment"]
    results = []
    for pref in buyer.cut_preferences:
        base_cwt = _get_base_price_cwt(pref.cut_code, usda_prices, ground_beef_prices)
        if base_cwt <= 0:
            continue
        buyer_price_lb = compute_buyer_price(pref.cut_code, pref, base_cwt, regional_adj)
        weekly_rev = buyer_price_lb * pref.volume_lbs_week

        # Description
        if pref.cut_code in SUBPRIMAL_YIELDS:
            desc = SUBPRIMAL_YIELDS[pref.cut_code][0]
        elif pref.cut_code in GROUND_BEEF_PRODUCTS:
            desc = GROUND_BEEF_PRODUCTS[pref.cut_code]["description"]
        else:
            desc = pref.cut_code

        results.append({
            "cut_code": pref.cut_code,
            "description": desc,
            "form": pref.form,
            "base_cwt": round(base_cwt, 2),
            "markup_pct": pref.markup_pct,
            "buyer_price_lb": round(buyer_price_lb, 2),
            "volume_lbs_week": pref.volume_lbs_week,
            "weekly_revenue": round(weekly_rev, 2),
        })
    return results


# ---------------------------------------------------------------------------
# JSON persistence
# ---------------------------------------------------------------------------

def save_buyers_json(buyers: List[BuyerProfile], filepath: str = BUYERS_JSON):
    data = [asdict(b) for b in buyers]
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def load_buyers_json(filepath: str = BUYERS_JSON) -> List[BuyerProfile]:
    if not os.path.exists(filepath):
        return []
    with open(filepath) as f:
        data = json.load(f)
    buyers = []
    for d in data:
        prefs = [CutPreference(**p) for p in d.pop("cut_preferences", [])]
        buyers.append(BuyerProfile(cut_preferences=prefs, **d))
    return buyers
