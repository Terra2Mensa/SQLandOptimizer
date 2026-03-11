#!/usr/bin/env python3
"""Generate realistic test data for the Michiana region (NW Indiana / SW Michigan).

Uses real farms, real processors, and population-weighted customer distribution
concentrated around the South Bend-Elkhart corridor.

Distributes orders evenly across 12 months, runs the optimizer monthly,
replenishes animal supply between runs, and tracks fulfillment time.

Usage:
    python3 simulate_michiana.py              # generate all data + run monthly optimizer
    python3 simulate_michiana.py --clean      # wipe first, then generate
    python3 simulate_michiana.py --clean-only # wipe and exit
"""
import argparse
import random
from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import mean

from db import (get_connection, save_farmer, save_animal,
                save_dtc_customer, save_purchase_order,
                save_processor_capability)
from optimizer_db import save_processor
from optimizer import run_optimizer
from seed_optimizer import seed_all

random.seed(2026)

# ============================================================================
# REAL PROCESSORS (researched)
# ============================================================================

PROCESSORS = [
    {
        'processor_key': 'martins_butchering',
        'company_name': "Martin's Custom Butchering",
        'address_line1': '27570 County Road 42',
        'city': 'Wakarusa', 'state': 'IN', 'zip_code': '46573',
        'latitude': 41.5356, 'longitude': -86.0208,
        'phone': '574-862-2982',
        'is_buyer_of_last_resort': True,
        'species': ['cattle', 'pork', 'lamb', 'goat'],
    },
    {
        'processor_key': 'garys_custom_meats',
        'company_name': "Gary's Custom Meats",
        'address_line1': '16237 Mason St',
        'city': 'Union', 'state': 'MI', 'zip_code': '49130',
        'latitude': 41.7814, 'longitude': -85.8572,
        'phone': '269-641-5683',
        'is_buyer_of_last_resort': True,
        'species': ['cattle', 'pork', 'lamb', 'goat', 'chicken'],
    },
    {
        'processor_key': 'sims_meat_processing',
        'company_name': 'Sims Meat Processing',
        'address_line1': '6961 S 3rd Road',
        'city': 'La Porte', 'state': 'IN', 'zip_code': '46350',
        'latitude': 41.5494, 'longitude': -86.7225,
        'phone': '219-393-1000',
        'is_buyer_of_last_resort': False,
        'species': ['cattle', 'pork', 'lamb'],
    },
]

PROCESSOR_CAPACITIES = {
    # processor_key: {species: daily_head}
    'martins_butchering': {'cattle': 12, 'pork': 25, 'lamb': 15, 'goat': 8},
    'garys_custom_meats': {'cattle': 10, 'pork': 20, 'lamb': 12, 'goat': 6, 'chicken': 100},
    'sims_meat_processing': {'cattle': 30, 'pork': 50, 'lamb': 20},
}

# ============================================================================
# REAL FARMS (researched — NW Indiana / SW Michigan)
# ============================================================================

FARMS = [
    # --- Elkhart County, IN ---
    {
        'farmer_id': 'vintage-meadows',
        'company_name': 'Vintage Meadows Farm',
        'contact_name': 'Schrock Family',
        'city': 'Goshen', 'state': 'IN', 'zip_code': '46528',
        'latitude': 41.5923, 'longitude': -85.8645,
        'species': ['cattle', 'pork'],
        'primary': 'cattle',
        'notes': '100% grass-fed beef, pastured non-GMO pork',
    },
    {
        'farmer_id': 'dmf-shorthorns',
        'company_name': 'DMF Shorthorns',
        'contact_name': None,
        'contact_phone': '574-642-4466',
        'city': 'Goshen', 'state': 'IN', 'zip_code': '46526',
        'latitude': 41.5750, 'longitude': -85.8350,
        'species': ['cattle'],
        'primary': 'cattle',
        'notes': 'Shorthorn beef cattle, Elkhart County',
    },
    {
        'farmer_id': 'woodsbrook-farm',
        'company_name': 'Woodsbrook Farm',
        'contact_phone': '574-238-1816',
        'city': 'Goshen', 'state': 'IN', 'zip_code': '46526',
        'latitude': 41.6050, 'longitude': -85.8100,
        'species': ['cattle'],
        'primary': 'cattle',
        'notes': '57446 CR 29, Goshen — fresh beef',
    },
    {
        'farmer_id': 'hertsel-berkshire',
        'company_name': 'Hertsel Berkshire Farm',
        'city': 'Nappanee', 'state': 'IN', 'zip_code': '46550',
        'latitude': 41.4528, 'longitude': -85.9689,
        'species': ['pork'],
        'primary': 'pork',
        'notes': '25638 CR 52, Nappanee — Berkshire pork',
    },
    # --- Marshall County, IN ---
    {
        'farmer_id': 'third-day-farm',
        'company_name': 'Third Day Farm LLC',
        'city': 'Walkerton', 'state': 'IN', 'zip_code': '46574',
        'latitude': 41.4664, 'longitude': -86.4892,
        'species': ['cattle', 'pork', 'lamb', 'chicken'],
        'primary': 'cattle',
        'notes': '2814 Plymouth Laporte Trail — grass-fed beef, pastured pork, lamb, poultry',
    },
    {
        'farmer_id': 'cooper-angus',
        'company_name': 'Cooper Angus',
        'contact_phone': '574-274-0001',
        'city': 'Tippecanoe', 'state': 'IN', 'zip_code': '46570',
        'latitude': 41.3630, 'longitude': -86.1070,
        'species': ['cattle'],
        'primary': 'cattle',
        'notes': 'Angus beef, Marshall County',
    },
    # --- St. Joseph County, IN ---
    {
        'farmer_id': 'laidig-family',
        'company_name': 'Laidig Family Farms',
        'contact_phone': '574-261-0015',
        'city': 'Mishawaka', 'state': 'IN', 'zip_code': '46544',
        'latitude': 41.6620, 'longitude': -86.1586,
        'species': ['pork'],
        'primary': 'pork',
        'notes': '63200 Madison Trail, Mishawaka — pastured pork',
    },
    {
        'farmer_id': 'palmer-show-lambs',
        'company_name': 'Palmer Show Lambs',
        'contact_phone': '574-532-9316',
        'city': 'New Carlisle', 'state': 'IN', 'zip_code': '46552',
        'latitude': 41.7003, 'longitude': -86.5097,
        'species': ['lamb'],
        'primary': 'lamb',
        'notes': '33978 Prairie Knolls Dr, New Carlisle — sheep',
    },
    # --- LaPorte County, IN ---
    {
        'farmer_id': 'clay-hill-ranch',
        'company_name': 'Clay Hill Ranch',
        'contact_phone': '574-323-3506',
        'city': 'La Porte', 'state': 'IN', 'zip_code': '46350',
        'latitude': 41.6403, 'longitude': -86.7225,
        'species': ['cattle', 'lamb', 'pork'],
        'primary': 'cattle',
        'notes': '6881 N 300 E, LaPorte — beef, lamb, pork',
    },
    {
        'farmer_id': 'mitzner-meats',
        'company_name': 'Mitzner Meats LLC',
        'contact_phone': '219-716-2721',
        'city': 'Wanatah', 'state': 'IN', 'zip_code': '46390',
        'latitude': 41.4306, 'longitude': -86.8986,
        'species': ['cattle', 'pork'],
        'primary': 'cattle',
        'notes': '8252 W 1775 S, Wanatah — beef & pork',
    },
    # --- Kosciusko County, IN ---
    {
        'farmer_id': 'crestview-farms',
        'company_name': 'Crestview Farms',
        'contact_phone': '574-457-3033',
        'city': 'Syracuse', 'state': 'IN', 'zip_code': '46567',
        'latitude': 41.4269, 'longitude': -85.7503,
        'species': ['cattle'],
        'primary': 'cattle',
        'notes': '10379 N Syracuse-Webster Rd, Syracuse',
    },
    {
        'farmer_id': 'sands-farms',
        'company_name': 'Sands Farms Inc',
        'contact_phone': '260-352-2278',
        'city': 'Silver Lake', 'state': 'IN', 'zip_code': '46982',
        'latitude': 41.0714, 'longitude': -85.8608,
        'species': ['cattle', 'pork'],
        'primary': 'cattle',
        'notes': '3063 W 950 S, Silver Lake — beef & pork',
    },
    # --- Starke County, IN ---
    {
        'farmer_id': 'troike-farms',
        'company_name': 'Troike Farms',
        'city': 'North Judson', 'state': 'IN', 'zip_code': '46366',
        'latitude': 41.2134, 'longitude': -86.7739,
        'species': ['cattle'],
        'primary': 'cattle',
        'notes': 'North Judson, Starke County — beef',
    },
    # --- Berrien / Cass County, MI ---
    {
        'farmer_id': 'roseland-organic',
        'company_name': 'Roseland Organic Farms',
        'city': 'Dowagiac', 'state': 'MI', 'zip_code': '49047',
        'latitude': 41.9842, 'longitude': -86.1086,
        'species': ['cattle', 'pork'],
        'primary': 'cattle',
        'notes': '500-acre certified organic, grass-fed beef, pastured pork',
    },
    {
        'farmer_id': 'bennett-farms',
        'company_name': 'Bennett Farms',
        'city': 'Edwardsburg', 'state': 'MI', 'zip_code': '49112',
        'latitude': 41.7953, 'longitude': -86.0803,
        'species': ['pork', 'chicken'],
        'primary': 'pork',
        'notes': 'Pastured pork & poultry, Cass County MI',
    },
    {
        'farmer_id': 'robinson-farm',
        'company_name': 'Robinson Farm',
        'contact_name': 'Nate & Lou Ann Robinson',
        'city': 'Cassopolis', 'state': 'MI', 'zip_code': '49031',
        'latitude': 41.9117, 'longitude': -86.0103,
        'species': ['cattle', 'pork', 'lamb'],
        'primary': 'cattle',
        'notes': '6th generation Cass County MI farm, 40+ years',
    },
]

# ============================================================================
# CUSTOMER METRO AREA — South Bend / Elkhart corridor
# ============================================================================

CUSTOMER_TOWNS = [
    # (city, state, lat, lng, population) — weight by real population density
    ("South Bend", "IN", 41.6764, -86.2520, 103713),
    ("Elkhart",    "IN", 41.6820, -85.9767, 53690),
    ("Mishawaka",  "IN", 41.6620, -86.1586, 51021),
    ("Goshen",     "IN", 41.5823, -85.8345, 34458),
    ("Granger",    "IN", 41.7519, -86.1100, 30249),
    ("La Porte",   "IN", 41.6103, -86.7225, 22444),
    ("Niles",      "MI", 41.8297, -86.2542, 11738),
    ("Plymouth",   "IN", 41.3434, -86.3092, 11042),
    ("Nappanee",   "IN", 41.4428, -85.9989, 6700),
    ("Bremen",     "IN", 41.4464, -86.1486, 4603),
    ("Buchanan",   "MI", 41.8272, -86.3611, 4500),
    ("Middlebury", "IN", 41.6753, -85.7092, 3500),
    ("Osceola",    "IN", 41.6653, -86.0764, 2600),
    ("Wakarusa",   "IN", 41.5356, -86.0208, 2006),
]

FIRST_NAMES = [
    "James", "Robert", "John", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Christopher", "Mary", "Patricia", "Jennifer",
    "Linda", "Sarah", "Karen", "Nancy", "Lisa", "Betty", "Margaret",
    "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul",
    "Andrew", "Joshua", "Kenneth", "Emily", "Jessica", "Ashley", "Amanda",
    "Stephanie", "Nicole", "Melissa", "Rebecca", "Laura", "Heather",
    "Brian", "Kevin", "Timothy", "Ronald", "Jason", "Jeff", "Ryan",
    "Gary", "Nicholas", "Eric", "Michelle", "Kimberly", "Amy", "Angela",
    "Donna", "Dorothy", "Carol", "Ruth", "Sharon", "Diane",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Hill",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Campbell", "Mitchell",
    "Carter", "Roberts", "Turner", "Phillips", "Evans", "Edwards",
    "Collins", "Stewart", "Morris", "Reed", "Cook", "Morgan", "Bell",
    "Murphy", "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard",
    "Yoder", "Miller", "Schrock", "Bontrager", "Hochstetler", "Troyer",
]

# Breed distributions by species (realistic for region)
BREED_CONFIG = {
    'cattle': {
        'breeds': [("Angus", 0.35), ("Red Angus", 0.10), ("Hereford", 0.15),
                   ("Shorthorn", 0.10), ("Simmental", 0.10), ("Charolais", 0.08),
                   ("Angus Cross", 0.12)],
        'weight': (1100, 1500),
        'grades': [("choice", 0.50), ("select", 0.25), ("prime", 0.10), ("grassfed", 0.15)],
        'yg': (2, 4), 'dress': (0.58, 0.64),
        'sex': [("steer", 0.60), ("heifer", 0.35), ("cow", 0.05)],
        'age': (14, 24), 'price_lb': (1.70, 2.30), 'price_head': (1900, 3100),
    },
    'pork': {
        'breeds': [("Berkshire", 0.30), ("Duroc", 0.25), ("Hampshire", 0.15),
                   ("Yorkshire", 0.15), ("Heritage Cross", 0.15)],
        'weight': (250, 300),
        'grades': [("standard", 0.60), ("premium", 0.40)],
        'yg': (1, 3), 'dress': (0.72, 0.76),
        'sex': [("barrow", 0.55), ("gilt", 0.45)],
        'age': (5, 7), 'price_lb': (0.65, 0.95), 'price_head': (170, 260),
    },
    'lamb': {
        'breeds': [("Suffolk", 0.30), ("Dorper", 0.20), ("Katahdin", 0.20),
                   ("Hampshire", 0.15), ("Dorset", 0.15)],
        'weight': (110, 155),
        'grades': [("choice", 0.55), ("prime", 0.25), ("good", 0.20)],
        'yg': (1, 3), 'dress': (0.48, 0.53),
        'sex': [("wether", 0.50), ("ewe", 0.30), ("ram", 0.20)],
        'age': (6, 12), 'price_lb': (2.20, 3.40), 'price_head': (260, 480),
    },
    'goat': {
        'breeds': [("Boer", 0.45), ("Kiko", 0.25), ("Spanish", 0.15),
                   ("Savanna", 0.15)],
        'weight': (65, 105),
        'grades': [("standard", 0.60), ("premium", 0.40)],
        'yg': (1, 2), 'dress': (0.43, 0.48),
        'sex': [("wether", 0.50), ("doe", 0.30), ("buck", 0.20)],
        'age': (6, 14), 'price_lb': (3.50, 5.00), 'price_head': (220, 480),
    },
    'chicken': {
        'breeds': [("Cornish Cross", 0.60), ("Freedom Ranger", 0.25),
                   ("Red Ranger", 0.15)],
        'weight': (5, 9),
        'grades': [("grade_a", 0.85), ("standard", 0.15)],
        'yg': (1, 1), 'dress': (0.70, 0.75),
        'sex': [("rooster", 0.50), ("hen", 0.50)],
        'age': (2, 4), 'price_lb': (1.80, 3.00), 'price_head': (9, 24),
    },
}


def _jitter(lat, lng, miles=2.0):
    deg = miles / 69.0
    return (
        round(lat + random.uniform(-deg, deg), 6),
        round(lng + random.uniform(-deg, deg), 6),
    )


def _wchoice(options):
    """Weighted choice from list of (value, weight) tuples."""
    vals, wts = zip(*options)
    return random.choices(vals, weights=wts, k=1)[0]


def _random_phone():
    area = random.choice(["574", "574", "574", "219", "269"])
    return f"{area}-{random.randint(200,999)}-{random.randint(1000,9999)}"


def _random_email(first, last):
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com",
                            "icloud.com", "hotmail.com"])
    sep = random.choice([".", "", "_"])
    num = random.randint(1, 99) if random.random() > 0.4 else ""
    return f"{first.lower()}{sep}{last.lower()}{num}@{domain}"


def _random_zip(state):
    if state == "MI":
        return f"4{random.randint(9000,9199)}"
    return f"4{random.randint(6300,6599)}"


# ============================================================================
# Generators
# ============================================================================

def create_processors():
    """Save the 3 real processors + capabilities."""
    for p in PROCESSORS:
        proc_data = {k: v for k, v in p.items() if k != 'species'}
        proc_data['active'] = True
        save_processor(proc_data)

        for sp in p['species']:
            cap = PROCESSOR_CAPACITIES[p['processor_key']].get(sp, 10)
            save_processor_capability(
                p['processor_key'], sp,
                daily_capacity_head=cap,
                city=p['city'], state=p['state'],
                latitude=p['latitude'], longitude=p['longitude'],
                usda_inspected=True,
                organic_certified=(p['processor_key'] == 'garys_custom_meats'),
            )
    print(f"  Created {len(PROCESSORS)} processors with capabilities")


def create_farms():
    """Save the 16 real farms."""
    for f in FARMS:
        farmer_data = {
            'farmer_id': f['farmer_id'],
            'company_name': f['company_name'],
            'contact_name': f.get('contact_name'),
            'contact_phone': f.get('contact_phone'),
            'city': f['city'], 'state': f['state'],
            'zip_code': f.get('zip_code'),
            'latitude': f['latitude'], 'longitude': f['longitude'],
            'active': True,
            'notes': f.get('notes'),
        }
        save_farmer(farmer_data)
    print(f"  Created {len(FARMS)} farmers")


# Global counter for unique animal IDs across replenishment cycles
_animal_counter = 0


def create_animals(n=200, ref_date=None, prefix="A", quiet=False):
    """Generate animals distributed across real farms based on their species.

    Args:
        n: number of animals to create
        ref_date: reference date for finish date calculation
        prefix: ID prefix (use different prefix per replenishment cycle)
        quiet: suppress output
    """
    global _animal_counter
    animals = []
    ref = ref_date or date.today()

    for _ in range(n):
        _animal_counter += 1
        farm = random.choice(FARMS)
        sp = random.choice(farm['species'])
        cfg = BREED_CONFIG[sp]

        breed = _wchoice(cfg['breeds'])
        grade = _wchoice(cfg['grades'])
        sex = _wchoice(cfg['sex'])
        live_wt = round(random.uniform(*cfg['weight']), 0)
        dress_pct = round(random.uniform(*cfg['dress']), 3)
        finish_offset = random.randint(0, 45)

        animal = {
            'animal_id': f"{prefix}-{_animal_counter:05d}",
            'farmer_id': farm['farmer_id'],
            'species': sp,
            'breed': breed,
            'lot_number': f"LOT-{farm['farmer_id'][-4:]}-{random.randint(1,15):02d}",
            'live_weight_est': live_wt,
            'quality_grade_est': grade,
            'yield_grade_est': random.randint(*cfg['yg']),
            'dressing_pct_est': dress_pct,
            'age_months': random.randint(*cfg['age']),
            'sex': sex,
            'frame_score': random.randint(4, 7) if sp == 'cattle' else None,
            'expected_finish_date': (ref + timedelta(days=finish_offset)).isoformat(),
            'asking_price_per_lb': round(random.uniform(*cfg['price_lb']), 4),
            'asking_price_head': round(random.uniform(*cfg['price_head']), 2),
            'status': 'available',
        }
        animals.append(animal)
        save_animal(animal)

    counts = Counter(a['species'] for a in animals)
    if not quiet:
        print(f"  Created {len(animals)} animals:")
        for sp, cnt in sorted(counts.items()):
            print(f"    {sp:10s}  {cnt:4d}")
    return animals


def create_customers(n=150):
    """Generate customers concentrated in S. Bend–Elkhart metro."""
    customers = []
    used_emails = set()
    town_names, _, town_lats, town_lngs, town_wts = zip(*CUSTOMER_TOWNS)

    conn = get_connection()
    try:
        for i in range(n):
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            town_idx = random.choices(range(len(CUSTOMER_TOWNS)), weights=town_wts, k=1)[0]
            town = CUSTOMER_TOWNS[town_idx]
            lat, lng = _jitter(town[2], town[3], miles=3.0)

            email = _random_email(first, last)
            while email in used_emails:
                email = _random_email(first, last)
            used_emails.add(email)

            street_num = random.randint(100, 9999)
            street = random.choice([
                "Main St", "Oak Ave", "Elm St", "Maple Dr", "Lincoln Way",
                "Jefferson Blvd", "Ironwood Dr", "Grape Rd", "McKinley Ave",
                "Mishawaka Ave", "Western Ave", "Miami St", "Sample St",
                "Lincolnway E", "US-20", "SR-19", "CR-6", "Cassopolis St",
            ])

            customer = {
                'customer_id': f"C-{i+1:04d}",
                'first_name': first,
                'last_name': last,
                'email': email,
                'phone': _random_phone(),
                'zip_code': _random_zip(town[1]),
                'address_line1': f"{street_num} {street}",
                'city': town[0],
                'state': town[1],
            }
            save_dtc_customer(customer)

            # Set lat/lng
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE dtc_customers SET latitude = %s, longitude = %s
                    WHERE customer_id = %s
                """, (lat, lng, customer['customer_id']))

            customer['_lat'] = lat
            customer['_lng'] = lng
            customers.append(customer)

        conn.commit()
    finally:
        conn.close()

    print(f"  Created {len(customers)} customers")
    return customers


SPECIES_OPTS = [("cattle", 0.50), ("pork", 0.25), ("lamb", 0.15),
                ("goat", 0.05), ("chicken", 0.05)]
# Cattle can use all 4 portions; pork/lamb/goat only whole or half
PORTION_OPTS_CATTLE = [("whole", 0.08), ("half", 0.32),
                       ("quarter_hind", 0.35), ("quarter_front", 0.25)]
PORTION_OPTS_SIDE = [("whole", 0.20), ("half", 0.80)]
GRADE_BY_SPECIES = {
    'cattle':  [("choice", 0.50), ("prime", 0.15), ("select", 0.20), ("grassfed", 0.15)],
    'pork':    [("standard", 0.60), ("premium", 0.40)],
    'lamb':    [("choice", 0.55), ("prime", 0.25), ("good", 0.20)],
    'goat':    [("standard", 0.60), ("premium", 0.40)],
    'chicken': [("grade_a", 0.85), ("standard", 0.15)],
}

# Global PO counter
_po_counter = 0


def create_purchase_orders(customers, n=100, order_date=None, quiet=False):
    """Generate POs with realistic species/portion distribution.

    Args:
        customers: list of customer dicts
        n: number of POs to create
        order_date: date to set on the PO (default: today)
        quiet: suppress output
    Returns:
        (created_count, list_of_po_numbers)
    """
    global _po_counter
    ord_date = order_date or date.today()
    delivery_base = ord_date + timedelta(days=30)

    created = 0
    skipped = 0
    po_numbers = []

    for _ in range(n):
        _po_counter += 1
        sp = _wchoice(SPECIES_OPTS)
        portion = _wchoice(PORTION_OPTS_CATTLE if sp == 'cattle'
                           else PORTION_OPTS_SIDE)
        grade = _wchoice(GRADE_BY_SPECIES[sp])
        customer = random.choice(customers)
        delivery = (delivery_base + timedelta(days=random.randint(0, 45))).isoformat()

        deposit_map = {'whole': 0.30, 'half': 0.25, 'quarter_hind': 0.20, 'quarter_front': 0.20}
        deposit = round(random.uniform(80, 600) * deposit_map[portion], 2)
        po_num = f"PO-{_po_counter:04d}"

        try:
            save_purchase_order(
                po_num, customer['customer_id'], sp, grade, portion,
                requested_delivery_date=delivery, deposit_amount=deposit,
            )
            # Update order_date to the simulated date
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE purchase_orders SET order_date = %s
                        WHERE po_number = %s
                    """, (ord_date, po_num))
                conn.commit()
            finally:
                conn.close()

            created += 1
            po_numbers.append(po_num)
        except ValueError:
            skipped += 1

    if not quiet:
        print(f"  Created {created} POs for {ord_date} ({skipped} skipped)")
    return created, po_numbers


# ============================================================================
# Supply replenishment
# ============================================================================

def replenish_animals(target=200, ref_date=None):
    """Top up animal supply to target count (assume supply always available)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM farmer_inventory WHERE status = 'available'")
            current = cur.fetchone()[0]
    finally:
        conn.close()

    needed = max(0, target - current)
    if needed > 0:
        create_animals(needed, ref_date=ref_date, prefix="R", quiet=True)
    return needed


# ============================================================================
# Fulfillment time tracking
# ============================================================================

def compute_fulfillment_times():
    """Compute days between order_date and slaughter_order created_at for fulfilled POs."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT po.po_number, po.species, po.order_date,
                       MIN(so.created_at) AS first_fulfillment
                FROM purchase_orders po
                JOIN po_lines pl ON pl.po_number = po.po_number
                JOIN slaughter_order_lines sol ON sol.po_line_id = pl.id
                JOIN slaughter_orders so ON so.id = sol.slaughter_order_id
                WHERE po.status != 'cancelled'
                GROUP BY po.po_number, po.species, po.order_date
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for po_number, species, order_date, first_fulfillment in rows:
        if order_date and first_fulfillment:
            delta = (first_fulfillment.date() - order_date.date()
                     if hasattr(order_date, 'date') else
                     first_fulfillment.date() - order_date)
            results.append({
                'po_number': po_number,
                'species': species,
                'order_date': order_date,
                'fulfilled_date': first_fulfillment,
                'days_to_fulfill': delta.days,
            })
    return results


def print_fulfillment_report(fulfillments):
    """Print a summary of fulfillment times."""
    if not fulfillments:
        print("\n  No fulfillment data available.")
        return

    print(f"\n{'='*60}")
    print("  FULFILLMENT TIME REPORT")
    print(f"{'='*60}")

    # Overall stats
    all_days = [f['days_to_fulfill'] for f in fulfillments]
    print(f"\n  Total POs fulfilled: {len(fulfillments)}")
    print(f"  Avg fulfillment time: {mean(all_days):.1f} days")
    print(f"  Min: {min(all_days)} days   Max: {max(all_days)} days")

    # By species
    by_species = defaultdict(list)
    for f in fulfillments:
        by_species[f['species']].append(f['days_to_fulfill'])

    print(f"\n  {'Species':10s} {'Count':>6s} {'Avg Days':>10s} {'Min':>6s} {'Max':>6s}")
    print(f"  {'-'*40}")
    for sp in sorted(by_species):
        days = by_species[sp]
        print(f"  {sp:10s} {len(days):6d} {mean(days):10.1f} {min(days):6d} {max(days):6d}")

    # By month
    by_month = defaultdict(list)
    for f in fulfillments:
        od = f['order_date']
        month_key = od.strftime('%Y-%m') if hasattr(od, 'strftime') else str(od)[:7]
        by_month[month_key].append(f['days_to_fulfill'])

    print(f"\n  {'Month':10s} {'Orders':>7s} {'Avg Days':>10s}")
    print(f"  {'-'*30}")
    for month in sorted(by_month):
        days = by_month[month]
        print(f"  {month:10s} {len(days):7d} {mean(days):10.1f}")

    print()


# ============================================================================
# Cleanup
# ============================================================================

def clean_all():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM slaughter_order_lines")
            cur.execute("DELETE FROM slaughter_orders")
            cur.execute("DELETE FROM po_lines")
            cur.execute("DELETE FROM purchase_orders")
            cur.execute("DELETE FROM dtc_customers")
            cur.execute("DELETE FROM farmer_inventory")
            cur.execute("DELETE FROM farmers")
            cur.execute("DELETE FROM config_processor_capabilities")
            cur.execute("DELETE FROM processors")
        conn.commit()
        print("Cleaned all simulation data.")
    finally:
        conn.close()


# ============================================================================
# Main — Monthly simulation loop
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate Michiana region test data')
    parser.add_argument('--clean', action='store_true',
                        help='Clean existing data before generating')
    parser.add_argument('--clean-only', action='store_true',
                        help='Only clean data, do not generate')
    parser.add_argument('--months', type=int, default=12,
                        help='Number of months to simulate (default: 12)')
    parser.add_argument('--orders-per-month', type=int, default=9,
                        help='POs per month (default: 9, ~108/year)')
    parser.add_argument('--animal-target', type=int, default=200,
                        help='Target available animal count (default: 200)')
    args = parser.parse_args()

    if args.clean or args.clean_only:
        clean_all()
        if args.clean_only:
            return

    # Ensure reference tables seeded
    seed_all()

    print("\nGenerating Michiana region simulation data...\n")

    # Static setup
    create_processors()
    create_farms()
    customers = create_customers(150)

    # Initial animal inventory
    start_date = date(2026, 1, 1)
    create_animals(args.animal_target, ref_date=start_date)

    # Monthly loop
    print(f"\n{'='*60}")
    print(f"  MONTHLY SIMULATION — {args.months} months, {args.orders_per_month} POs/month")
    print(f"{'='*60}\n")

    monthly_results = []
    all_species = ['cattle', 'pork', 'lamb']  # optimizable species

    for month_idx in range(args.months):
        month_num = 1 + month_idx % 12
        year = 2026 + month_idx // 12

        # Orders placed throughout the month (days 1-25)
        order_date = date(year, month_num, random.randint(1, 25))
        # Optimizer runs at end of month (28th)
        run_date = date(year, month_num, 28)

        print(f"\n  --- Month {month_idx+1}: {order_date.strftime('%B %Y')} ---")

        # 1. Replenish animals to target
        replenished = replenish_animals(args.animal_target, ref_date=order_date)
        if replenished > 0:
            print(f"  Replenished {replenished} animals")

        # 2. Generate this month's POs (spread across the month)
        total_created = 0
        all_po_nums = []
        orders_this_month = args.orders_per_month
        for j in range(orders_this_month):
            # Stagger order dates across the month
            day = min(28, 1 + int((j / orders_this_month) * 25))
            po_date = date(year, month_num, day)
            cnt, nums = create_purchase_orders(
                customers, n=1, order_date=po_date, quiet=True
            )
            total_created += cnt
            all_po_nums.extend(nums)
        print(f"  Created {total_created} POs across {order_date.strftime('%B')}")

        # 3. Run optimizer for each species (commit mode) — runs on the 28th
        month_summary = {'month': date(year, month_num, 1), 'orders_created': total_created,
                         'held_po_count': 0}
        new_so_ids = []
        for sp in all_species:
            result = run_optimizer(sp, dry_run=False, run_date=run_date)
            if result['status'] in ('success', 'held'):
                month_summary[f'{sp}_animals'] = result.get('animals_selected', 0)
                month_summary[f'{sp}_hw'] = result.get('total_hanging_weight', 0)
                month_summary[f'{sp}_util'] = result.get('avg_utilization_pct', 0)
                month_summary['held_po_count'] += result.get('held_po_count', 0)
                for r in result.get('results', []):
                    new_so_ids.append(r['order']['order_number'])

        # 4. Backdate slaughter_orders.created_at to simulated run date (28th)
        if new_so_ids:
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE slaughter_orders SET created_at = %s
                        WHERE order_number = ANY(%s)
                    """, (run_date, new_so_ids))
                conn.commit()
            finally:
                conn.close()

        monthly_results.append(month_summary)

    # Final summary
    print(f"\n{'='*60}")
    print("  ANNUAL SIMULATION SUMMARY")
    print(f"{'='*60}")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM purchase_orders")
            total_pos = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM po_lines")
            total_lines = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM slaughter_orders")
            total_sos = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM farmer_inventory WHERE status = 'reserved'")
            reserved = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM farmer_inventory WHERE status = 'available'")
            available = cur.fetchone()[0]
    finally:
        conn.close()

    print(f"\n  Total POs created:        {total_pos}")
    print(f"  Total PO lines:           {total_lines}")
    print(f"  Slaughter orders created: {total_sos}")
    print(f"  Animals reserved:         {reserved}")
    print(f"  Animals still available:  {available}")

    # Monthly breakdown
    print(f"\n  {'Month':12s} {'POs':>5s} {'Cattle':>8s} {'Pork':>8s} {'Lamb':>8s} {'Held':>6s}")
    print(f"  {'-'*53}")
    for ms in monthly_results:
        m = ms['month'].strftime('%Y-%m')
        cattle = ms.get('cattle_animals', 0)
        pork = ms.get('pork_animals', 0)
        lamb = ms.get('lamb_animals', 0)
        held = ms.get('held_po_count', 0)
        print(f"  {m:12s} {ms['orders_created']:5d} {cattle:8d} {pork:8d} {lamb:8d} {held:6d}")

    # Fulfillment time report
    fulfillments = compute_fulfillment_times()
    print_fulfillment_report(fulfillments)

    print(f"\n  Simulation complete!\n")


if __name__ == '__main__':
    main()
