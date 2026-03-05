"""Configuration for cattle valuation engine."""
import os
from dotenv import load_dotenv

load_dotenv()

# MARS API (Indiana auction data)
MARS_API_KEY = os.getenv("MARS_API_KEY", "")
MARS_BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"

# USDA DataMart (boxed beef, slaughter cattle)
DATAMART_BASE_URL = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports"

# Report IDs
REPORT_CHOICE_SELECT = 2461   # Weekly Boxed Beef Cutout & Cuts (Choice + Select)
REPORT_PRIME = 2460            # Weekly Boxed Beef Cuts for Prime Product
REPORT_5AREA_WEEKLY = 2477     # 5 Area Weekly Weighted Average Direct Slaughter Cattle
REPORT_IAMN_DAILY = 2672       # IA-MN Daily Direct Slaughter Cattle Summary
REPORT_PREMIUMS = 2482         # National Weekly Premiums and Discounts
REPORT_IN_AUCTION = 1976       # Indiana Weekly Auction Summary (MARS)

# PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "cattle_valuation"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Default valuation parameters
DEFAULT_LIVE_WEIGHT = 1350.0
DEFAULT_YIELD_GRADE = 3
DEFAULT_QUALITY_GRADE = "choice"
DEFAULT_BROKER_FEE_PCT = 0.02
DEFAULT_BYPRODUCT_PCT = 0.08
DEFAULT_BYPRODUCT_VALUE_PER_LB = 0.30
DEFAULT_GRASSFED_PREMIUM_CWT = 45.0  # $/cwt over Choice

# Dressing percentages by USDA Yield Grade
DRESS_PCT_BY_YG = {
    1: 0.635,
    2: 0.620,
    3: 0.600,
    4: 0.580,
    5: 0.560,
}

# IMPS code -> (description, % of carcass weight, primal)
SUBPRIMAL_YIELDS = {
    "109E": ("Ribeye, lip-on, bone-in", 3.50, "Rib"),
    "112A": ("Ribeye, boneless", 2.80, "Rib"),
    "113C": ("Chuck, semi-bnls, neck/off", 5.50, "Chuck"),
    "114":  ("Shoulder clod", 3.80, "Chuck"),
    "114A": ("Shoulder clod, trimmed", 3.20, "Chuck"),
    "114D": ("Clod, top blade (flat iron)", 0.80, "Chuck"),
    "114E": ("Clod, arm roast", 1.60, "Chuck"),
    "114F": ("Clod tender", 0.35, "Chuck"),
    "116A": ("Chuck roll, lxl, neck/off", 6.00, "Chuck"),
    "116B": ("Chuck tender", 1.20, "Chuck"),
    "916A": ("Chuck roll, retail ready", 1.80, "Chuck"),
    "116G": ("Chuck flap", 0.90, "Chuck"),
    "120":  ("Brisket, deckle-off, bnls", 2.80, "Brisket"),
    "120A": ("Brisket, point/off, bnls", 1.00, "Brisket"),
    "123A": ("Short plate, short rib", 1.50, "Plate"),
    "130":  ("Chuck short rib", 2.00, "Chuck"),
    "160":  ("Round, bone-in", 0.30, "Round"),
    "161":  ("Round, boneless", 0.20, "Round"),
    "167A": ("Knuckle, peeled", 2.80, "Round"),
    "168":  ("Top inside round", 4.50, "Round"),
    "169":  ("Top inside, denuded", 2.50, "Round"),
    "169A": ("Top inside, cap off", 1.00, "Round"),
    "170":  ("Bottom gooseneck", 1.50, "Round"),
    "171B": ("Outside round (flat)", 3.50, "Round"),
    "171C": ("Eye of round", 1.80, "Round"),
    "174":  ("Short loin, 0x1", 3.50, "Loin"),
    "175":  ("Strip loin, 1x1", 2.80, "Loin"),
    "180":  ("Strip, bnls, 0x1", 2.50, "Loin"),
    "184":  ("Top butt, boneless", 3.00, "Loin"),
    "184B": ("Top butt, cap/collar", 0.60, "Loin"),
    "185A": ("Sirloin flap (bavette)", 1.50, "Loin"),
    "185B": ("Ball-tip, boneless", 1.20, "Loin"),
    "185C": ("Sirloin tri-tip", 1.00, "Loin"),
    "185D": ("Tri-tip, peeled", 0.80, "Loin"),
    "189A": ("Tenderloin, trimmed", 1.20, "Loin"),
    "191A": ("Butt tender, trimmed", 0.40, "Loin"),
    "193":  ("Flank steak", 1.00, "Flank"),
}

PRIMAL_ORDER = ["Rib", "Chuck", "Loin", "Round", "Brisket", "Plate", "Flank"]

# Processor profiles (kill/fab costs for purchase price calculation)
PROCESSORS = {
    "processor_a": {
        "name": "Processor A",
        "kill_fee": 175.00,           # $/head slaughter fee
        "fab_cost_per_lb": 0.22,      # $/lb carcass fabrication
        "shrink_pct": 0.025,          # cooler shrink (2.5%)
        "payment_terms_days": 30,     # net payment days
    },
    "processor_b": {
        "name": "Processor B",
        "kill_fee": 200.00,
        "fab_cost_per_lb": 0.25,
        "shrink_pct": 0.030,
        "payment_terms_days": 45,
    },
}
DEFAULT_PROCESSOR = "processor_a"

# ---------------------------------------------------------------------------
# Demand-side configuration
# ---------------------------------------------------------------------------

# Grade hierarchy for buyer filtering (higher rank accepts lower grades too)
GRADE_RANK = {
    "prime": 4,
    "grassfed": 3,
    "choice": 2,
    "select": 1,
}

# 18% of carcass weight becomes trim -> ground beef
TRIM_YIELD_PCT = 0.18

# Regional pricing adjustments (multiplier on national USDA prices)
REGIONS = {
    "south_bend_in": {
        "label": "South Bend, IN",
        "city": "South Bend",
        "state": "IN",
        "pricing_adjustment": 1.0,
    },
    "chicago_il": {
        "label": "Chicago, IL",
        "city": "Chicago",
        "state": "IL",
        "pricing_adjustment": 1.05,
    },
    "indianapolis_in": {
        "label": "Indianapolis, IN",
        "city": "Indianapolis",
        "state": "IN",
        "pricing_adjustment": 0.98,
    },
}
DEFAULT_REGION = "south_bend_in"

# Ground beef products (pseudo-IMPS codes, not in SUBPRIMAL_YIELDS)
GROUND_BEEF_PRODUCTS = {
    "ground_80_20": {
        "description": "Ground Beef 80/20",
        "lean_pct": 80,
        "fat_pct": 20,
    },
    "ground_73_27": {
        "description": "Ground Beef 73/27",
        "lean_pct": 73,
        "fat_pct": 27,
    },
    "ground_90_10": {
        "description": "Ground Beef 90/10",
        "lean_pct": 90,
        "fat_pct": 10,
    },
}

# Buyer type templates: key cuts, markup ranges, min grade, defaults
BUYER_TYPES = {
    "fine_dining": {
        "label": "Fine Dining",
        "min_grade": "choice",
        "payment_terms_days": 30,
        "weekly_volume_lbs": 150,
        "cuts": {
            "112A": {"form": "steak_cut",    "markup_pct": 0.55},
            "180":  {"form": "steak_cut",    "markup_pct": 0.50},
            "189A": {"form": "steak_cut",    "markup_pct": 0.60},
            "185C": {"form": "steak_cut",    "markup_pct": 0.45},
            "185A": {"form": "steak_cut",    "markup_pct": 0.40},
            "193":  {"form": "whole_subprimal", "markup_pct": 0.40},
        },
    },
    "casual_restaurant": {
        "label": "Casual Restaurant",
        "min_grade": "select",
        "payment_terms_days": 30,
        "weekly_volume_lbs": 300,
        "cuts": {
            "112A": {"form": "steak_cut",       "markup_pct": 0.25},
            "180":  {"form": "steak_cut",       "markup_pct": 0.25},
            "184":  {"form": "steak_cut",       "markup_pct": 0.20},
            "120":  {"form": "whole_subprimal", "markup_pct": 0.25},
            "116A": {"form": "whole_subprimal", "markup_pct": 0.20},
            "ground_80_20": {"form": "ground",  "markup_pct": 0.20},
        },
    },
    "fast_casual": {
        "label": "Fast Casual / Pizza",
        "min_grade": "select",
        "payment_terms_days": 14,
        "weekly_volume_lbs": 500,
        "cuts": {
            "ground_80_20": {"form": "ground",          "markup_pct": 0.18},
            "ground_73_27": {"form": "ground",          "markup_pct": 0.15},
            "120":          {"form": "whole_subprimal",  "markup_pct": 0.20},
        },
    },
    "institution": {
        "label": "Institution (Schools/Hospitals)",
        "min_grade": "select",
        "payment_terms_days": 45,
        "weekly_volume_lbs": 800,
        "cuts": {
            "116A":         {"form": "whole_subprimal", "markup_pct": 0.10},
            "168":          {"form": "whole_subprimal", "markup_pct": 0.10},
            "167A":         {"form": "whole_subprimal", "markup_pct": 0.10},
            "ground_80_20": {"form": "ground",          "markup_pct": 0.12},
            "ground_73_27": {"form": "ground",          "markup_pct": 0.10},
        },
    },
    "dtc": {
        "label": "Direct-to-Consumer",
        "min_grade": "choice",
        "payment_terms_days": 0,
        "weekly_volume_lbs": 100,
        "cuts": {
            "112A": {"form": "steak_cut",       "markup_pct": 0.90},
            "180":  {"form": "steak_cut",       "markup_pct": 0.80},
            "189A": {"form": "steak_cut",       "markup_pct": 0.85},
            "120":  {"form": "whole_subprimal", "markup_pct": 0.60},
            "ground_80_20": {"form": "ground",  "markup_pct": 0.50},
        },
    },
    "butcher_shop": {
        "label": "Butcher Shop",
        "min_grade": "choice",
        "payment_terms_days": 14,
        "weekly_volume_lbs": 400,
        "cuts": {
            "112A": {"form": "whole_subprimal", "markup_pct": 0.35},
            "180":  {"form": "whole_subprimal", "markup_pct": 0.30},
            "189A": {"form": "whole_subprimal", "markup_pct": 0.35},
            "184":  {"form": "whole_subprimal", "markup_pct": 0.25},
            "116A": {"form": "whole_subprimal", "markup_pct": 0.25},
            "120":  {"form": "whole_subprimal", "markup_pct": 0.30},
            "185C": {"form": "whole_subprimal", "markup_pct": 0.30},
            "193":  {"form": "whole_subprimal", "markup_pct": 0.30},
            "167A": {"form": "whole_subprimal", "markup_pct": 0.25},
            "171C": {"form": "whole_subprimal", "markup_pct": 0.25},
            "ground_80_20": {"form": "ground",  "markup_pct": 0.40},
            "ground_90_10": {"form": "ground",  "markup_pct": 0.40},
        },
    },
}
