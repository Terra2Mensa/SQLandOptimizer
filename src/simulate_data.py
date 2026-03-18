#!/usr/bin/env python3
"""Generate realistic test data for the carcass optimizer.

Creates:
  - 50 farmers as profiles (type='farmer')
  - 500 animals in farmer_inventory
  - 12 processors as profiles (type='processor') + processor_costs
  - 300 customers as profiles (type='customer')
  - 200 share-based purchase orders

Usage:
    python3 simulate_data.py              # generate all data
    python3 simulate_data.py --clean      # wipe simulated data first
    python3 simulate_data.py --clean-only # wipe and exit
"""
import argparse
import random
from datetime import date, timedelta

from db import (get_connection, save_profile, save_farmer_animal,
                save_purchase_order, save_processor_cost)
from seed_optimizer import seed_all

random.seed(42)

# ---------------------------------------------------------------------------
# Geographic reference data
# ---------------------------------------------------------------------------

IN_TOWNS = [
    ("South Bend", "IN", 41.6764, -86.2520),
    ("Mishawaka", "IN", 41.6620, -86.1586),
    ("Elkhart", "IN", 41.6820, -85.9767),
    ("Goshen", "IN", 41.5823, -85.8345),
    ("Plymouth", "IN", 41.3434, -86.3092),
    ("Warsaw", "IN", 41.2381, -85.8530),
    ("Ligonier", "IN", 41.4656, -85.5875),
    ("Nappanee", "IN", 41.4428, -85.9989),
    ("Bremen", "IN", 41.4464, -86.1486),
    ("Wakarusa", "IN", 41.5356, -86.0208),
    ("Middlebury", "IN", 41.6753, -85.7092),
    ("LaGrange", "IN", 41.6417, -85.4164),
    ("Angola", "IN", 41.6347, -84.9994),
    ("Kendallville", "IN", 41.4414, -85.2647),
    ("Columbia City", "IN", 41.1572, -85.4883),
    ("Huntington", "IN", 40.8831, -85.4975),
    ("Fort Wayne", "IN", 41.0793, -85.1394),
    ("Decatur", "IN", 40.8306, -84.9294),
    ("Bluffton", "IN", 40.7381, -85.1717),
    ("Wabash", "IN", 40.7978, -85.8206),
    ("Peru", "IN", 40.7536, -86.0689),
    ("Logansport", "IN", 40.7545, -86.3567),
    ("Rochester", "IN", 41.0647, -86.2156),
    ("Knox", "IN", 41.2956, -86.6250),
    ("Valparaiso", "IN", 41.4731, -87.0611),
    ("LaPorte", "IN", 41.6103, -86.7225),
    ("Michigan City", "IN", 41.7075, -86.8950),
    ("Kokomo", "IN", 40.4864, -86.1336),
    ("Marion", "IN", 40.5583, -85.6592),
    ("Muncie", "IN", 40.1934, -85.3864),
    ("Indianapolis", "IN", 39.7684, -86.1581),
    ("Lafayette", "IN", 40.4167, -86.8753),
    ("Terre Haute", "IN", 39.4667, -87.4139),
]

MI_TOWNS = [
    ("Niles", "MI", 41.8297, -86.2542),
    ("Buchanan", "MI", 41.8272, -86.3611),
    ("Dowagiac", "MI", 41.9842, -86.1086),
    ("Three Rivers", "MI", 41.9439, -85.6322),
    ("Sturgis", "MI", 41.7992, -85.4192),
    ("Coldwater", "MI", 41.9403, -85.0006),
    ("Hillsdale", "MI", 41.9200, -84.6306),
    ("Constantine", "MI", 41.8414, -85.6686),
    ("Cassopolis", "MI", 41.9117, -86.0103),
    ("Kalamazoo", "MI", 42.2917, -85.5872),
    ("Battle Creek", "MI", 42.3211, -85.1797),
]

OH_TOWNS = [
    ("Defiance", "OH", 41.2845, -84.3558),
    ("Van Wert", "OH", 40.8695, -84.5842),
    ("Bryan", "OH", 41.4748, -84.5525),
    ("Archbold", "OH", 41.5217, -84.3072),
    ("Napoleon", "OH", 41.3922, -84.1253),
    ("Celina", "OH", 40.5489, -84.5703),
]

ALL_TOWNS = IN_TOWNS + MI_TOWNS + OH_TOWNS

METRO_TOWNS = [t for t in ALL_TOWNS if t[0] in (
    "South Bend", "Mishawaka", "Elkhart", "Goshen", "Plymouth",
    "Nappanee", "Bremen", "Wakarusa", "Middlebury", "Niles",
    "Buchanan", "LaPorte", "Michigan City",
)]

# ---------------------------------------------------------------------------
# Name generators
# ---------------------------------------------------------------------------

FARM_PREFIXES = [
    "Heritage", "Prairie", "Meadowbrook", "Sunrise", "Heartland",
    "Golden", "Oak Ridge", "Cedar", "Maple", "Riverbend",
    "Rolling Hills", "Stoney Creek", "Wildwood", "Green Acres", "Timber",
    "Valley View", "Shady Lane", "Harvest", "Iron Gate", "Blue Sky",
    "Clover", "Windmill", "Lakeside", "Pine", "Whispering",
    "Pleasant", "Crystal", "Northwind", "Summit", "Country",
    "Hickory", "Walnut", "Birchwood", "Creekside", "Eagle",
    "Fox Run", "Hawk", "Frontier", "Pioneer", "Trailside",
    "Rustic", "Homestead", "Cornerstone", "Fieldstone", "Ridgeview",
    "Fairview", "Hillcrest", "Brookside", "Millstone", "Sycamore",
]

FARM_SUFFIXES = [
    "Farm", "Ranch", "Acres", "Farms", "Holdings",
    "Land & Cattle", "Livestock", "Family Farm", "Angus", "Meats",
]

FIRST_NAMES = [
    "James", "Robert", "John", "Michael", "David", "William", "Richard",
    "Joseph", "Thomas", "Christopher", "Mary", "Patricia", "Jennifer",
    "Linda", "Sarah", "Karen", "Nancy", "Lisa", "Betty", "Margaret",
    "Daniel", "Matthew", "Anthony", "Mark", "Donald", "Steven", "Paul",
    "Andrew", "Joshua", "Kenneth", "Emily", "Jessica", "Ashley", "Amanda",
    "Stephanie", "Nicole", "Melissa", "Rebecca", "Laura", "Heather",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter",
]

CATTLE_BREEDS = ["Angus", "Hereford", "Charolais", "Simmental", "Limousin",
                 "Red Angus", "Shorthorn", "Wagyu", "Brahman"]
PORK_BREEDS = ["Berkshire", "Duroc", "Hampshire", "Yorkshire", "Landrace",
               "Chester White", "Spotted"]
LAMB_BREEDS = ["Suffolk", "Dorper", "Hampshire", "Katahdin", "Texel",
               "Dorset", "Merino"]
GOAT_BREEDS = ["Boer", "Kiko", "Spanish", "Savanna", "Myotonic"]
CHICKEN_BREEDS = ["Cornish Cross", "Freedom Ranger", "Red Ranger",
                  "Jersey Giant", "Orpington"]

PROCESSOR_NAMES = [
    ("ligonier_meats", "Ligonier Custom Meats", "Ligonier", "IN", 41.4656, -85.5875),
    ("elkhart_pack", "Elkhart Packing Co", "Elkhart", "IN", 41.6820, -85.9767),
    ("bremen_proc", "Bremen Processing", "Bremen", "IN", 41.4464, -86.1486),
    ("nappanee_meats", "Nappanee Meats", "Nappanee", "IN", 41.4428, -85.9989),
    ("plymouth_abattoir", "Plymouth Abattoir", "Plymouth", "IN", 41.3434, -86.3092),
    ("warsaw_custom", "Warsaw Custom Cuts", "Warsaw", "IN", 41.2381, -85.8530),
    ("fort_wayne_pack", "Fort Wayne Packing", "Fort Wayne", "IN", 41.0793, -85.1394),
    ("huntington_proc", "Huntington Processing", "Huntington", "IN", 40.8831, -85.4975),
    ("niles_meats", "Niles Meat Processing", "Niles", "MI", 41.8297, -86.2542),
    ("sturgis_pack", "Sturgis Packing", "Sturgis", "MI", 41.7992, -85.4192),
    ("coldwater_proc", "Coldwater Processing", "Coldwater", "MI", 41.9403, -85.0006),
    ("defiance_meats", "Defiance Meats", "Defiance", "OH", 41.2845, -84.3558),
]


def _jitter(lat, lng, miles=3.0):
    deg = miles / 69.0
    return (
        lat + random.uniform(-deg, deg),
        lng + random.uniform(-deg, deg),
    )


def _random_phone():
    area = random.choice(["574", "260", "765", "219", "269", "517", "419"])
    return f"{area}-{random.randint(200,999)}-{random.randint(1000,9999)}"


def _random_email(first, last):
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com",
                            "icloud.com", "hotmail.com", "aol.com"])
    sep = random.choice([".", "", "_"])
    num = random.randint(1, 99) if random.random() > 0.5 else ""
    return f"{first.lower()}{sep}{last.lower()}{num}@{domain}"


def _random_zip(state):
    if state == "IN":
        return f"4{random.randint(6000,6999)}"
    elif state == "MI":
        return f"4{random.randint(9000,9499)}"
    else:
        return f"4{random.randint(3500,3999)}"


# ---------------------------------------------------------------------------
# Generator functions
# ---------------------------------------------------------------------------

def generate_farmers(n=50):
    """Generate n farmer profiles."""
    farmers = []
    used_names = set()
    for i in range(n):
        while True:
            prefix = random.choice(FARM_PREFIXES)
            suffix = random.choice(FARM_SUFFIXES)
            name = f"{prefix} {suffix}"
            if name not in used_names:
                used_names.add(name)
                break
        town = random.choice(ALL_TOWNS)
        lat, lng = _jitter(town[2], town[3], miles=5.0)
        contact = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        first, last = contact.split(' ', 1)
        farmer = {
            'profile_id': f"F-{i+1:04d}",
            'type': 'farmer',
            'first': first,
            'last': last,
            'email': f"info@{name.lower().replace(' ', '')}.com",
            'phone': _random_phone(),
            'address': f"{town[0]}, {town[1]} {_random_zip(town[1])}",
            'latitude': round(lat, 6),
            'longitude': round(lng, 6),
            'company_name': name,
            'active': True,
        }
        farmers.append(farmer)
    return farmers


def generate_animals(farmers, n=500):
    """Generate n animal records in farmer_inventory.

    Species mix: 60% cattle, 20% pork, 10% lamb, 5% goat, 5% chicken.
    """
    species_names = ['cattle', 'pork', 'lamb', 'goat', 'chicken']
    species_weights = [0.60, 0.20, 0.10, 0.05, 0.05]

    species_config = {
        'cattle': {
            'weight_range': (1100, 1500),
            'grades': [('choice', 0.50), ('select', 0.25), ('prime', 0.15), ('grassfed', 0.10)],
        },
        'pork': {
            'weight_range': (240, 310),
            'grades': [('standard', 0.70), ('premium', 0.30)],
        },
        'lamb': {
            'weight_range': (110, 160),
            'grades': [('choice', 0.55), ('prime', 0.25), ('good', 0.20)],
        },
        'goat': {
            'weight_range': (60, 110),
            'grades': [('standard', 0.65), ('premium', 0.35)],
        },
        'chicken': {
            'weight_range': (5, 9),
            'grades': [('grade_a', 0.85), ('standard', 0.15)],
        },
    }

    animals = []
    today = date.today()
    for i in range(n):
        sp = random.choices(species_names, weights=species_weights, k=1)[0]
        cfg = species_config[sp]
        farmer = random.choice(farmers)

        grade_names = [g[0] for g in cfg['grades']]
        grade_weights = [g[1] for g in cfg['grades']]
        grade = random.choices(grade_names, weights=grade_weights, k=1)[0]

        live_wt = round(random.uniform(*cfg['weight_range']), 0)
        finish_offset = random.randint(0, 90)

        animal = {
            'animal_id': f"A-{i+1:05d}",
            'profile_id': farmer['profile_id'],
            'species': sp,
            'live_weight_est': live_wt,
            'expected_grade': grade,
            'expected_finish_date': (today + timedelta(days=finish_offset)).isoformat(),
            'status': 'available',
        }
        animals.append(animal)
    return animals


def generate_processors():
    """Generate 12 processors as profiles + processor_costs."""
    capability_map = {
        'ligonier_meats':    ['cattle', 'pork', 'lamb'],
        'elkhart_pack':      ['cattle', 'pork'],
        'bremen_proc':       ['cattle', 'pork', 'lamb', 'goat'],
        'nappanee_meats':    ['cattle', 'pork', 'lamb'],
        'plymouth_abattoir': ['cattle', 'pork', 'lamb', 'goat', 'chicken'],
        'warsaw_custom':     ['cattle', 'pork'],
        'fort_wayne_pack':   ['cattle', 'pork', 'lamb'],
        'huntington_proc':   ['cattle'],
        'niles_meats':       ['cattle', 'pork', 'lamb'],
        'sturgis_pack':      ['cattle', 'pork'],
        'coldwater_proc':    ['cattle', 'pork', 'lamb', 'goat'],
        'defiance_meats':    ['cattle', 'pork'],
    }

    capacity_ranges = {
        'cattle':  (15, 50),
        'pork':    (30, 100),
        'lamb':    (20, 60),
        'goat':    (10, 30),
        'chicken': (100, 500),
    }

    # Species-level cost defaults
    from config import PROCESSING_RATES

    processors = []
    for key, name, city, state, lat, lng in PROCESSOR_NAMES:
        lat_j, lng_j = _jitter(lat, lng, miles=1.0)
        profile = {
            'profile_id': key,
            'type': 'processor',
            'company_name': name,
            'address': f"{city}, {state}",
            'latitude': round(lat_j, 6),
            'longitude': round(lng_j, 6),
            'phone': _random_phone(),
            'active': True,
        }
        save_profile(profile)
        processors.append(profile)

        for sp in capability_map.get(key, ['cattle']):
            rates = PROCESSING_RATES.get(sp, {'kill_fee': 150, 'fab_cost_per_lb': 0.85, 'shrink_pct': 0.025})
            cap_range = capacity_ranges.get(sp, (10, 30))
            save_processor_cost({
                'profile_id': key,
                'species': sp,
                'kill_fee': rates['kill_fee'],
                'fab_cost_per_lb': rates['fab_cost_per_lb'],
                'shrink_pct': rates['shrink_pct'],
                'daily_capacity_head': random.randint(*cap_range),
                'effective_date': date(2020, 1, 1),
            })

    return processors


def generate_customers(n=300):
    """Generate n customer profiles."""
    customers = []
    used_emails = set()
    for i in range(n):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        if random.random() < 0.70:
            town = random.choice(METRO_TOWNS)
        else:
            town = random.choice(ALL_TOWNS)

        lat, lng = _jitter(town[2], town[3], miles=4.0)

        email = _random_email(first, last)
        while email in used_emails:
            email = _random_email(first, last)
        used_emails.add(email)

        customer = {
            'profile_id': f"C-{i+1:05d}",
            'type': 'customer',
            'first': first,
            'last': last,
            'email': email,
            'phone': _random_phone(),
            'address': f"{random.randint(100,9999)} {random.choice(['Main','Oak','Elm','Maple','Cedar','Pine','Lincoln','Washington','Jefferson'])} {random.choice(['St','Ave','Rd','Dr','Ln','Ct'])}, {town[0]}, {town[1]} {_random_zip(town[1])}",
            'latitude': round(lat, 6),
            'longitude': round(lng, 6),
            'active': True,
        }
        save_profile(customer)
        customers.append(customer)

    return customers


def generate_purchase_orders(customers, n=200):
    """Generate n share-based purchase orders.

    Species mix: 55% cattle, 25% pork, 15% lamb, 3% goat, 2% chicken
    Share mix: 10% whole, 30% half, 35% quarter, 25% eighth
    """
    species_choices = ['cattle', 'pork', 'lamb', 'goat', 'chicken']
    species_weights = [0.55, 0.25, 0.15, 0.03, 0.02]

    share_choices = ['whole', 'half', 'quarter', 'eighth']
    share_weights = [0.10, 0.30, 0.35, 0.25]

    today = date.today()
    orders = []
    for i in range(n):
        sp = random.choices(species_choices, weights=species_weights, k=1)[0]
        share = random.choices(share_choices, weights=share_weights, k=1)[0]
        customer = random.choice(customers)

        deposit_map = {'whole': 0.30, 'half': 0.25, 'quarter': 0.20, 'eighth': 0.15}
        deposit = round(random.uniform(100, 800) * deposit_map[share], 2)

        orders.append({
            'po_number': f"PO-{i+1:05d}",
            'profile_id': customer['profile_id'],
            'species': sp,
            'share': share,
            'deposit': deposit,
            'status': 'pending',
        })
    return orders


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def clean_simulated_data():
    """Remove all simulated data (leaves reference/seed tables intact)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM slaughter_order_allocations")
            cur.execute("DELETE FROM slaughter_orders")
            cur.execute("DELETE FROM po_cut_instructions")
            cur.execute("DELETE FROM purchase_orders")
            cur.execute("DELETE FROM farmer_inventory")
            cur.execute("DELETE FROM processor_costs")
            cur.execute("DELETE FROM profiles")
        conn.commit()
        print("Cleaned all simulated data.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Generate test data for optimizer')
    parser.add_argument('--clean', action='store_true',
                        help='Clean existing data before generating')
    parser.add_argument('--clean-only', action='store_true',
                        help='Only clean data, do not generate')
    args = parser.parse_args()

    if args.clean or args.clean_only:
        clean_simulated_data()
        if args.clean_only:
            return

    # Ensure reference data is seeded
    seed_all()

    print("\nGenerating simulated data...")

    # 1. Farmers
    farmers = generate_farmers(50)
    for f in farmers:
        save_profile(f)
    print(f"  Created {len(farmers)} farmers")

    # 2. Animals
    animals = generate_animals(farmers, 500)
    for a in animals:
        save_farmer_animal(a)
    print(f"  Created {len(animals)} animals")

    from collections import Counter
    species_counts = Counter(a['species'] for a in animals)
    for sp, cnt in sorted(species_counts.items()):
        print(f"    {sp:10s}  {cnt:4d}")

    # 3. Processors
    processors = generate_processors()
    print(f"  Created {len(processors)} processors")

    # 4. Customers
    customers = generate_customers(300)
    print(f"  Created {len(customers)} customers")

    # 5. Purchase Orders
    orders = generate_purchase_orders(customers, 200)
    for o in orders:
        save_purchase_order(o)
    print(f"  Created {len(orders)} purchase orders")

    po_species = Counter(o['species'] for o in orders)
    for sp, cnt in sorted(po_species.items()):
        print(f"    {sp:10s}  {cnt:4d} POs")

    print(f"\n  Simulation complete!")


if __name__ == '__main__':
    main()
