"""Configuration for cattle valuation engine."""
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

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
# Order/invoice status workflows
ORDER_STATUSES = ["pending", "confirmed", "fulfilled", "invoiced", "paid", "cancelled"]
INVOICE_STATUSES = ["draft", "sent", "partial", "paid", "overdue"]

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

# ---------------------------------------------------------------------------
# Pork configuration
# ---------------------------------------------------------------------------

REPORT_PORK_DAILY = 2498        # National Daily Pork FOB Plant (cut-level)
REPORT_PORK_WEEKLY = 2500       # National Weekly Pork FOB Plant
REPORT_PORK_COMPREHENSIVE = 2680  # National Weekly Pork Comprehensive
REPORT_PORK_LIVE = 2510         # National Daily Direct Purchased Swine

PORK_PRIMAL_ORDER = ["Loin", "Butt", "Picnic", "Rib", "Ham", "Belly", "Sparerib", "Jowl", "Trim", "Variety"]

DEFAULT_PORK_LIVE_WEIGHT = 270.0    # typical market hog lbs
DEFAULT_PORK_DRESS_PCT = 0.74

PORK_PROCESSORS = {
    "processor_a": {
        "name": "Processor A",
        "kill_fee": 45.00,
        "fab_cost_per_lb": 0.12,
        "shrink_pct": 0.020,
        "payment_terms_days": 30,
    },
    "processor_b": {
        "name": "Processor B",
        "kill_fee": 55.00,
        "fab_cost_per_lb": 0.14,
        "shrink_pct": 0.025,
        "payment_terms_days": 45,
    },
}

# Key pork cuts for valuation (description, approx yield % of carcass, primal)
# IMPS Series 400: (description, yield % of carcass, primal)
# Codes verified against USDA IMPS 400 spec
PORK_CUT_YIELDS = {
    "410":  ("Pork Loin", 10.5, "Loin"),
    "413":  ("Pork Loin, Boneless", 7.0, "Loin"),
    "415":  ("Pork Tenderloin", 1.2, "Loin"),
    "413D": ("Pork Sirloin, Boneless", 3.5, "Loin"),
    "412B": ("Pork Loin, Center-Cut, 8 Ribs, Boneless", 5.0, "Loin"),
    "406":  ("Pork Shoulder, Butt, Bone-In", 8.5, "Butt"),
    "406A": ("Pork Shoulder, Butt, Boneless", 6.5, "Butt"),
    "405":  ("Pork Shoulder, Picnic", 5.5, "Picnic"),
    "405A": ("Pork Shoulder, Picnic, Boneless", 3.5, "Picnic"),
    "402":  ("Pork Leg, Skinned", 16.0, "Ham"),
    "402B": ("Pork Leg, Boneless", 10.5, "Ham"),
    "409":  ("Pork Belly, Skinless", 12.0, "Belly"),
    "416":  ("Pork Spareribs", 3.5, "Sparerib"),
    "416A": ("Pork Spareribs, St. Louis Style", 2.5, "Sparerib"),
    "419":  ("Pork Jowl", 2.0, "Jowl"),
}

# ---------------------------------------------------------------------------
# Lamb configuration
# ---------------------------------------------------------------------------

REPORT_LAMB_CUTOUT = 2649       # National Estimated Lamb Carcass Cutout
REPORT_LAMB_BOXED = 2648        # National 5-Day Rolling Average Boxed Lamb
REPORT_LAMB_COMPREHENSIVE = 2651  # National Weekly Comprehensive Lamb Carcass

LAMB_PRIMAL_ORDER = ["Rack", "Shoulder", "Loin", "Leg", "Breast/Shank", "Other"]

DEFAULT_LAMB_LIVE_WEIGHT = 135.0
DEFAULT_LAMB_DRESS_PCT = 0.50

# IMPS Series 200: (description, yield % of carcass, primal)
# These are fallback values; report 2649 provides percentage_carcass live
LAMB_SUBPRIMAL_YIELDS = {
    "204":  ("Rack, 8-Rib", 5.65, "Rack"),
    "204C": ("Rack, Roast-Ready, Frenched", 3.07, "Rack"),
    "207":  ("Shoulders, Square-Cut", 23.10, "Shoulder"),
    "209":  ("Breast", 8.45, "Breast/Shank"),
    "210":  ("Foreshank", 3.25, "Breast/Shank"),
    "232":  ("Loin, Trimmed", 5.56, "Loin"),
    "232E": ("Flank, Untrimmed", 3.22, "Loin"),
    "233A": ("Leg, Trotter Off", 15.81, "Leg"),
    "234":  ("Leg, Boneless, Tied", 10.24, "Leg"),
    "296":  ("Ground Lamb", 3.91, "Other"),
}

LAMB_PROCESSORS = {
    "processor_a": {
        "name": "Processor A",
        "kill_fee": 35.00,
        "fab_cost_per_lb": 0.15,
        "shrink_pct": 0.020,
        "payment_terms_days": 30,
    },
}

# ---------------------------------------------------------------------------
# Chicken configuration
# ---------------------------------------------------------------------------

DEFAULT_CHICKEN_LIVE_WEIGHT = 6.5   # typical broiler lbs
DEFAULT_CHICKEN_DRESS_PCT = 0.72

# No USDA API available — manual entry only
# Codes TBD — NAMP Poultry Buyers Guide needed for official P-series codes
# (description, yield % of live weight after dressing, category)
CHICKEN_CUT_YIELDS = {
    "breast_bnls":  ("Boneless Skinless Breast", 24.0, "White Meat"),
    "breast_bone":  ("Bone-In Breast", 28.0, "White Meat"),
    "tender":       ("Tenderloins", 4.0, "White Meat"),
    "thigh_bnls":   ("Boneless Thigh", 10.0, "Dark Meat"),
    "thigh_bone":   ("Bone-In Thigh", 12.0, "Dark Meat"),
    "drumstick":    ("Drumstick", 10.0, "Dark Meat"),
    "wing_whole":   ("Whole Wing", 8.0, "Wing"),
    "wing_flat":    ("Wing Flat", 3.5, "Wing"),
    "wing_drum":    ("Wing Drumette", 3.0, "Wing"),
    "back_neck":    ("Back & Neck", 15.0, "Other"),
    "giblets":      ("Giblets", 5.0, "Other"),
}

# ---------------------------------------------------------------------------
# Goat configuration
# ---------------------------------------------------------------------------

DEFAULT_GOAT_LIVE_WEIGHT = 80.0
DEFAULT_GOAT_DRESS_PCT = 0.45

# No USDA API available — manual entry only
# Codes TBD — official IMPS Series 11 uses 11-X-## format, needs mapping
# (description, yield % of carcass weight, primal)
GOAT_CUT_YIELDS = {
    "leg":          ("Leg", 28.0, "Leg"),
    "loin":         ("Loin/Rack", 12.0, "Loin"),
    "shoulder":     ("Shoulder", 24.0, "Shoulder"),
    "shank":        ("Shank/Foreshank", 8.0, "Shank"),
    "breast_rib":   ("Breast & Ribs", 14.0, "Breast"),
    "neck":         ("Neck", 6.0, "Other"),
    "ground":       ("Ground Goat", 8.0, "Other"),
}
