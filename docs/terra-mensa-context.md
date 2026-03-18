# Terra Mensa — System Context Document

> Canonical reference for the Terra Mensa data model, business logic, and
> website/backend boundary. Every numeric value is sourced from code; every
> table/field is sourced from `sql/supabase_init.sql`. See "Source Files" at
> the end for exact file:line references.

---

## 1. Business Model Overview

### What Terra Mensa Does

Terra Mensa is a **demand-aggregation and logistics service** for locally
sourced livestock in the Michiana (South Bend, IN) area. It connects farmers
who raise animals with end buyers who want specific portions (whole, half, or quarter, or eight (for cattle) of specific species, optimizing the
matching, processing coordination, and invoicing in between.

The core value proposition: farmers get fair, USDA-anchored prices for whole
animals instead of selling at auction; buyers get locally sourced cuts at
Below grocery price without having to buy a whole carcass.

Terra Mensa automates the creation of slaughter orders for farmers, and cut sheets for processors. 

### Four Actors
##Four Actor numerics is floating with real market rates. Many numbers here are current as of 17 MAR 26 /placeholders

| Actor | Role | System Entry Point |
|-------|------|--------------------|
| **Farmers** | List live animals for sale | Website marketplace / farmer dashboard |
| **D2C Customers** ("Families") | Buy shares (whole/half/quarter) of an animal | Website marketplace / family dashboard |
| **B2B Buyers** | Restaurants, institutions, butcher shops — buy specific cuts | Python backend only (not on website) |
| **Processors** | Slaughter + fabrication — kill fee + fab $/lb | Directory on website (read-only); optimizer selects |

### Revenue Model

TM earns the **spread** between:
- **Farmer purchase price** — derived from USDA live/dressed/grid basis
- **Buyer sell price** — USDA cutout prices + buyer-type markup

The markup varies by buyer type (from `config.py` BUYER_TYPES):

| Buyer Type | Label | Markup Range | Min Grade | Payment Terms | Weekly Vol (lbs) |
|------------|-------|-------------|-----------|---------------|------------------|
| `dtc` | Direct-to-Consumer | 50-90% | Choice | Prepaid (0 days) | 100 |
| `fine_dining` | Fine Dining | 40-60% | Choice | 30 days | 150 |
| `butcher_shop` | Butcher Shop | 25-40% | Choice | 14 days | 400 |
| `casual_restaurant` | Casual Restaurant | 20-25% | Select | 30 days | 300 |
| `fast_casual` | Fast Casual / Pizza | 15-20% | Select | 14 days | 500 |
| `institution` | Institution (Schools/Hospitals) | 10-12% | Select | 45 days | 800 |

### Species Coverage

| Species | Typical Live Wt | Dressing % | USDA Data Source | Pricing |
|---------|-----------------|------------|------------------|---------|
| Cattle | 1,350 lbs | 56-63.5% (by YG 1-5) | DataMart 2461,2460,2477,2672,2482 + MARS 1976 | API auto |
| Pork | 270 lbs | 74% | DataMart 2498 (cuts), 2510 (live hogs) | API auto |
| Lamb | 135 lbs | 50% | DataMart 2649 (IMPS cutout), 2648 (boxed) | API auto |
| Chicken | 6.5 lbs | 72% | None | Manual entry |
| Goat | 80 lbs | 45% | None | Manual entry |

**Cattle dressing % by USDA Yield Grade:**

| YG | Dress % |
|----|---------|
| 1 | 63.5% |
| 2 | 62.0% |
| 3 | 60.0% |
| 4 | 58.0% |
| 5 | 56.0% |

### Default Processing Rates

Per-species rates (2025-26 Michiana-area market rates):

| Species | Kill Fee ($/head) | Fab Cost ($/lb HW) | Shrink % |
|---------|-------------------|---------------------|----------|
| Cattle | $150.00 | $0.85 | 2.5% |
| Pork | $75.00 | $0.80 | 2.0% |
| Lamb | $100.00 | $0.90 | 2.0% |
| Goat | $100.00 | $0.90 | 2.0% |
| Chicken | $5.00 | $0.50 | 1.0% |

### Other Revenue/Cost Parameters

| Parameter | Value | Source |
|-----------|-------|--------|
| Byproduct revenue | 8% of carcass weight @ $0.30/lb | `DEFAULT_BYPRODUCT_PCT`, `DEFAULT_BYPRODUCT_VALUE_PER_LB` |
| Broker fee | 2% of gross | `DEFAULT_BROKER_FEE_PCT` | **remove 2% broker fee
| Grassfed premium | $45/cwt over Choice | `DEFAULT_GRASSFED_PREMIUM_CWT` |
| Trim yield | 18% of carcass -> ground beef | `TRIM_YIELD_PCT` |

### Grade Hierarchy

| Grade | Rank |
|-------|------|
| Prime | 4 |
| Grassfed | 3 |
| Choice | 2 |
| Select | 1 |

Higher rank accepts lower grades. A buyer requesting Choice will accept Choice
or higher (Grassfed, Prime). The optimizer prefers exact-match grades to avoid
wasting premium animals on lower-grade orders.

---

## 2. Data Model & Requirements

Two-schema layout:
- **`public`** — Website reads/writes via Supabase REST API + RLS
- **`engine`** — Python backend only, invisible to REST API (no grants to anon/authenticated)

### 2.1 Farmers

**Table: `public.farmers`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `farmer_id` | VARCHAR(50) | PK, UNIQUE | Yes — FK from inventory | Maps to Supabase auth UID |
| `company_name` | VARCHAR(150) | NOT NULL | Display | |
| `contact_name` | VARCHAR(100) | Optional | No | |
| `contact_email` | VARCHAR(100) | Optional | No | |
| `contact_phone` | VARCHAR(30) | Optional | No | |
| `address_line1` | VARCHAR(150) | Optional | No | |
| `address_line2` | VARCHAR(150) | Optional | No | |
| `city` | VARCHAR(50) | Optional | No | |
| `state` | VARCHAR(10) | Optional | No | Indexed |
| `zip_code` | VARCHAR(20) | Optional | No | |
| `latitude` | NUMERIC(9,6) | Optional | **Critical** | Used for distance scoring in `match_animal()` and `select_processor()`. Schema allows NULL but optimizer degrades to 9999-mile fallback without it. |
| `longitude` | NUMERIC(9,6) | Optional | **Critical** | Same as latitude |
| `active` | BOOLEAN | Default TRUE | Indexed filter | |
| `notes` | TEXT | Optional | No | |
| `created_at` | TIMESTAMP | Auto | No | |
| `updated_at` | TIMESTAMP | Auto | No | |

**Table: `public.farmer_inventory`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `animal_id` | VARCHAR(50) | PK, UNIQUE | Yes — FK from slaughter_orders | |
| `farmer_id` | VARCHAR(50) | FK NOT NULL | Yes — farmer location lookup | |
| `species` | VARCHAR(20) | NOT NULL | Yes — species routing | |
| `breed` | VARCHAR(50) | Optional | No | |
| `lot_number` | VARCHAR(50) | Optional | No | |
| `live_weight_est` | NUMERIC(10,1) | Optional | **Critical** — hanging weight calc | If NULL/0, animal skipped (`estimate_hanging_weight` returns 0) |
| `quality_grade_est` | VARCHAR(20) | Optional | Yes — grade filtering | Hard filter: animal grade must >= assembly strictest grade |
| `yield_grade_est` | INTEGER | Optional | Cattle only — dress % lookup | Falls back to YG 3 (60%) |
| `dressing_pct_est` | NUMERIC(5,3) | Optional | Overrides species default | If set, used instead of YG/species lookup |
| `age_months` | INTEGER | Optional | No | |
| `sex` | VARCHAR(10) | Optional | No | Options: steer, heifer, cow, bull |
| `frame_score` | INTEGER | Optional | No | |
| `expected_finish_date` | DATE | Optional | No | Indexed when status='available' |
| `asking_price_per_lb` | NUMERIC(10,4) | Optional | No | |
| `asking_price_head` | NUMERIC(10,2) | Optional | No | |
| `status` | VARCHAR(20) | NOT NULL, default 'available' | Yes — only 'available' animals matched | States: available, reserved, sold, processing, complete |
| `notes` | TEXT | Optional | No | |
| `created_at` | TIMESTAMP | Auto | No | |
| `updated_at` | TIMESTAMP | Auto | No | |

**Indexes:** species+status, species+grade+status, farmer_id, finish_date (where available)

### 2.2 D2C Customers

**Table: `public.dtc_customers`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `customer_id` | VARCHAR(50) | PK, UNIQUE | Yes — FK from POs | Maps to Supabase auth UID |
| `first_name` | VARCHAR(50) | NOT NULL | Display | |
| `last_name` | VARCHAR(50) | NOT NULL | Display | |
| `email` | VARCHAR(100) | NOT NULL | Indexed | |
| `phone` | VARCHAR(30) | NOT NULL | | |
| `zip_code` | VARCHAR(20) | NOT NULL | | |
| `address_line1` | VARCHAR(150) | Optional | | |
| `address_line2` | VARCHAR(150) | Optional | | |
| `city` | VARCHAR(50) | Optional | | |
| `state` | VARCHAR(10) | Optional | | |
| `latitude` | NUMERIC(9,6) | Optional | **Critical** | Added by migration 003. Used for assembly geographic clustering and processor selection. **Currently never populated from ZIP — known gap.** |
| `longitude` | NUMERIC(9,6) | Optional | **Critical** | Same as latitude |
| `notes` | TEXT | Optional | No | |
| `created_at` | TIMESTAMP | Auto | No | |
| `updated_at` | TIMESTAMP | Auto | No | |

> **Known gap:** `latitude`/`longitude` columns exist (migration 003) but the
> website never geocodes the customer's ZIP code to populate them. The optimizer
> queries `dc.latitude AS cust_lat, dc.longitude AS cust_lng` in
> `get_pending_pos_for_assembly()` — NULLs degrade distance scoring.

**Table: `public.purchase_orders`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `po_number` | VARCHAR(50) | PK, UNIQUE | Yes — assembly grouping | Format: `PO-{base36_timestamp}` |
| `customer_id` | VARCHAR(50) | FK NOT NULL | Yes — customer location JOIN | |
| `species` | VARCHAR(20) | NOT NULL | Yes — species routing | |
| `quality_grade` | VARCHAR(20) | Optional | Yes — strictest grade calc | |
| `carcass_portion` | VARCHAR(20) | NOT NULL | Yes — slot allocation | whole, half, quarter_front, quarter_hind |
| `order_date` | TIMESTAMP | Auto | Yes — age-based triggers | |
| `requested_delivery_date` | DATE | Optional | No (future) | |
| `status` | VARCHAR(20) | NOT NULL, default 'pending' | Yes — only 'pending' assembled | States: pending, planned, processing, fulfilled, cancelled |
| `deposit_amount` | NUMERIC(10,2) | Default 0 | No | 15% of estimated total |
| `total_estimated` | NUMERIC(12,2) | Optional | Invoicing baseline | |
| `total_final` | NUMERIC(12,2) | Optional | Set during reconciliation | Based on actual weights |
| `notes` | TEXT | Optional | No | |
| `confirmed_delivery_date` | DATE | Optional | No | |
| `customer_preferences` | TEXT | Optional | No | Free-text from order form |
| `created_at` | TIMESTAMP | Auto | No | |
| `updated_at` | TIMESTAMP | Auto | No | |

**Table: `public.po_lines`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `id` | SERIAL | PK | Yes — po_line_id in SO lines | |
| `po_number` | VARCHAR(50) | FK NOT NULL | Yes — grouped by PO | CASCADE delete |
| `cut_code` | VARCHAR(20) | NOT NULL | Yes — yield matching | IMPS code or pseudo-code (ground_80_20) |
| `description` | VARCHAR(100) | Optional | Display | |
| `primal` | VARCHAR(20) | Optional | Display | |
| `quantity_lbs` | NUMERIC(10,1) | NOT NULL | Yes — demand calculation | |
| `price_per_lb` | NUMERIC(8,4) | NOT NULL | Invoicing | |
| `line_total` | NUMERIC(10,2) | NOT NULL | Display | |
| `fulfilled_lbs` | NUMERIC(10,1) | Default 0 | Yes — remaining = qty - fulfilled | |
| `actual_lbs` | NUMERIC(10,1) | Optional | Reconciliation | Set during `finalize_slaughter_order` |
| `status` | VARCHAR(20) | NOT NULL, default 'pending' | Yes — only pending/partial used | States: pending, partial, fulfilled, cancelled |

### 2.3 B2B Buyers

**Table: `engine.buyers`** (engine schema — invisible to website REST API)

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `buyer_id` | VARCHAR(50) | PK, UNIQUE | Yes | |
| `name` | VARCHAR(100) | NOT NULL | Display | |
| `buyer_type` | VARCHAR(30) | NOT NULL | Markup lookup from BUYER_TYPES config | fine_dining, casual_restaurant, fast_casual, institution, dtc, butcher_shop |
| `city` | VARCHAR(50) | Optional | | |
| `state` | VARCHAR(10) | Optional | | |
| `region` | VARCHAR(30) | Optional | Regional pricing adjustment | |
| `min_quality_grade` | VARCHAR(20) | Optional | Grade filtering | |
| `payment_terms_days` | INTEGER | Optional | Invoicing | |
| `active` | BOOLEAN | Default TRUE | | |
| `contact_name` | VARCHAR(100) | Optional | | |
| `contact_email` | VARCHAR(100) | Optional | | |
| `contact_phone` | VARCHAR(30) | Optional | | |
| `business_name` | VARCHAR(150) | Optional | | |
| `address_line1` | VARCHAR(150) | Optional | | |
| `address_line2` | VARCHAR(150) | Optional | | |
| `zip_code` | VARCHAR(20) | Optional | | |
| `license_number` | VARCHAR(50) | Optional | | |
| `delivery_zone` | VARCHAR(50) | Optional | | |
| `delivery_day` | VARCHAR(20) | Optional | | |
| `credit_limit` | NUMERIC(12,2) | Default 0 | | |
| `notes` | TEXT | Optional | | |

**Table: `engine.buyer_cut_preferences`**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `buyer_id` | VARCHAR(50) | FK NOT NULL | CASCADE delete |
| `cut_code` | VARCHAR(20) | NOT NULL | IMPS or pseudo-code |
| `form` | VARCHAR(30) | Optional | steak_cut, whole_subprimal, ground |
| `markup_pct` | NUMERIC(6,4) | Optional | Per-cut markup override |
| `fixed_premium_per_lb` | NUMERIC(8,4) | Optional | Alternative to % markup |
| `volume_lbs_week` | NUMERIC(10,1) | Optional | Weekly demand |
| `use_fixed_premium` | BOOLEAN | Default FALSE | |

**Table: `engine.orders`** (B2B orders)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `order_id` | VARCHAR(50) | PK, UNIQUE | |
| `buyer_id` | VARCHAR(50) | FK NOT NULL | |
| `order_date` | TIMESTAMP | Auto | |
| `delivery_date` | DATE | Optional | |
| `status` | VARCHAR(20) | Default 'pending' | States: pending, confirmed, fulfilled, invoiced, paid, cancelled |
| `quality_grade` | VARCHAR(20) | Optional | |
| `region` | VARCHAR(30) | Optional | |

**Table: `engine.order_lines`** (B2B order line items)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `order_id` | VARCHAR(50) | FK NOT NULL | CASCADE delete |
| `cut_code` | VARCHAR(20) | NOT NULL | |
| `form` | VARCHAR(30) | Optional | |
| `quantity_lbs` | NUMERIC(10,1) | NOT NULL | |
| `price_per_lb` | NUMERIC(8,2) | NOT NULL | |
| `usda_base_cwt` | NUMERIC(10,2) | Optional | USDA reference price |
| `markup_pct` | NUMERIC(6,4) | Optional | |
| `line_total` | NUMERIC(10,2) | NOT NULL | |
| `fulfilled_lbs` | NUMERIC(10,1) | Default 0 | |
| `source_animal_id` | VARCHAR(50) | Optional | Traceability link |

### 2.4 Processors

Three tables work together:

1. **`public.processors`** — Directory (name, location, LOR flag). Website-visible, read-only.
2. **`engine.config_processor_capabilities`** — Per-species capacity, certifications. Effective-date versioned. Engine-only.
3. **`public.config_processors`** — Cost rates (kill fee, fab $/lb, shrink). Effective-date versioned. Website-visible, read-only.

**Table: `public.processors`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `processor_key` | VARCHAR(50) | PK, UNIQUE | Yes — FK everywhere | |
| `company_name` | VARCHAR(100) | NOT NULL | Display | |
| `address_line1` | VARCHAR(200) | Optional | | |
| `city` | VARCHAR(80) | Optional | | |
| `state` | VARCHAR(10) | Optional | | |
| `zip_code` | VARCHAR(20) | Optional | | |
| `latitude` | NUMERIC(9,6) | Optional | **Critical** — distance scoring | |
| `longitude` | NUMERIC(9,6) | Optional | **Critical** — distance scoring | |
| `phone` | VARCHAR(30) | Optional | | |
| `is_buyer_of_last_resort` | BOOLEAN | Default FALSE | Yes — LOR processor lookup | |
| `active` | BOOLEAN | Default TRUE | Yes — eligibility filter | |
| `notes` | TEXT | Optional | | |

**Table: `engine.config_processor_capabilities`**

| Field | Type | Required | Optimizer Uses | Notes |
|-------|------|----------|----------------|-------|
| `processor_key` | VARCHAR(50) | NOT NULL | JOIN to processors | |
| `species` | VARCHAR(20) | NOT NULL | Species eligibility | |
| `daily_capacity_head` | INTEGER | Optional | Capacity enforcement | Optimizer skips if day is full |
| `city` | VARCHAR(50) | Optional | | |
| `state` | VARCHAR(10) | Optional | | |
| `latitude` | NUMERIC(9,6) | Optional | | |
| `longitude` | NUMERIC(9,6) | Optional | | |
| `organic_certified` | BOOLEAN | Default FALSE | Returned in eligibility query | |
| `usda_inspected` | BOOLEAN | Default TRUE | Returned in eligibility query | |
| `effective_date` | DATE | Default CURRENT_DATE | Most-recent effective wins | UNIQUE (processor_key, species, effective_date) |

**Table: `public.config_processors`** (cost rates)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `processor_key` | VARCHAR(50) | NOT NULL | |
| `name` | VARCHAR(100) | NOT NULL | |
| `kill_fee` | NUMERIC(10,2) | NOT NULL | $/head |
| `fab_cost_per_lb` | NUMERIC(10,4) | NOT NULL | $/lb hanging weight |
| `shrink_pct` | NUMERIC(6,4) | NOT NULL | |
| `payment_terms_days` | INTEGER | Default 30 | |
| `effective_date` | DATE | Default CURRENT_DATE | UNIQUE (processor_key, effective_date) |

### 2.5 Config Tables

**`public.config_cut_specs`** — Species cut definitions (IMPS codes, yield %, primal, grade requirements). Read-only from website. Seeded from `config.py` constants per species. UNIQUE (species, cut_code).

**`engine.config_grade_hierarchy`** — Grade ranking per species (grade_code, rank_order). Used by optimizer for hard grade filtering and preference scoring. UNIQUE (species, grade_code).

**`engine.config_regions`** — Regional pricing adjustments (pricing_adjustment multiplier). Effective-date versioned. UNIQUE (region_key, effective_date).

**`engine.config_parameters`** — Generic key-value config with effective-date versioning. Stores broker_fee_pct, byproduct_pct, etc. UNIQUE (param_key, effective_date).

### 2.6 Slaughter Orders (Optimizer Output)

**Table: `public.slaughter_orders`**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `order_number` | VARCHAR(50) | PK, UNIQUE | Format: `SO-{run_id[:8]}-{index:03d}` |
| `status` | VARCHAR(20) | Default 'planned' | States: planned, confirmed, in_progress, completed, cancelled |
| `species` | VARCHAR(20) | NOT NULL | |
| `animal_id` | VARCHAR(50) | FK to farmer_inventory | |
| `processor_key` | VARCHAR(50) | FK to processors | |
| `optimizer_run_id` | VARCHAR(50) | Optional | UUID grouping all SOs from one run |
| `estimated_hanging_weight` | NUMERIC(10,2) | Optional | Sum of yield vector |
| `actual_hanging_weight` | NUMERIC(10,2) | Optional | Set during weight recording |
| `processing_cost_total` | NUMERIC(10,2) | Optional | kill_fee + (fab_cost * HW) |
| `farmer_to_proc_distance` | NUMERIC(8,2) | Optional | Miles |
| `avg_cust_to_proc_distance` | NUMERIC(8,2) | Optional | Miles |
| `pct_allocated_to_orders` | NUMERIC(5,2) | Optional | Utilization % |
| `pct_to_last_resort` | NUMERIC(5,2) | Optional | 100 - utilization |
| `optimizer_score` | NUMERIC(10,4) | Optional | Lower is better |
| `completed_at` | TIMESTAMP | Optional | Set during finalization |

**Table: `public.slaughter_order_lines`**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `slaughter_order_id` | INTEGER | FK NOT NULL | CASCADE delete |
| `cut_code` | VARCHAR(30) | NOT NULL | |
| `total_lbs` | NUMERIC(8,2) | NOT NULL | From yield vector |
| `allocated_to_po` | NUMERIC(8,2) | Default 0 | Estimated allocation |
| `allocated_to_lor` | NUMERIC(8,2) | Default 0 | Last-of-resort estimated |
| `actual_lbs` | NUMERIC(8,2) | Optional | Post-processing actual |
| `actual_allocated_to_po` | NUMERIC(8,2) | Optional | Set during reconciliation |
| `actual_allocated_to_lor` | NUMERIC(8,2) | Optional | Set during reconciliation |
| `po_number` | VARCHAR(50) | Optional | NULL for LOR lines |
| `po_line_id` | INTEGER | Optional | NULL for LOR lines |

**Constraint:** `allocated_to_po + allocated_to_lor <= total_lbs + 0.01`

### 2.7 Invoices

**Table: `public.invoices`**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `invoice_id` | VARCHAR(50) | PK, UNIQUE | Format: `INV-{po_number}` |
| `order_id` | VARCHAR(50) | Optional | B2B order reference |
| `po_number` | VARCHAR(50) | FK Optional | D2C PO reference |
| `buyer_id` | VARCHAR(50) | Optional | B2B buyer |
| `customer_id` | VARCHAR(50) | FK Optional | D2C customer |
| `invoice_date` | DATE | Default CURRENT_DATE | |
| `due_date` | DATE | NOT NULL | invoice_date + payment_terms_days |
| `total_amount` | NUMERIC(12,2) | NOT NULL | Based on actual weights where available |
| `paid_amount` | NUMERIC(12,2) | Default 0 | |
| `status` | VARCHAR(20) | Default 'draft' | States: draft, sent, partial, paid, overdue |

### 2.8 Engine-Only Market Data Tables

These tables store USDA price data fetched by the valuation scripts. All in `engine` schema.

| Table | Purpose |
|-------|---------|
| `engine.usda_subprimal_prices` | Boxed beef cut-level prices (Choice/Select/Prime) |
| `engine.usda_composites` | Primal composite prices (Choice/Select) |
| `engine.slaughter_cattle_prices` | Live/dressed cattle prices |
| `engine.premiums_discounts` | Grade premiums and discounts |
| `engine.indiana_auction` | MARS Indiana weekly auction data |
| `engine.valuations` | Computed valuations (all species) |
| `engine.purchase_price_analysis` | Purchase price basis analysis |
| `engine.pork_cutout_prices` | Pork FOB cut-level prices |
| `engine.pork_primal_values` | Pork primal composite values |
| `engine.pork_live_prices` | Direct-purchased swine prices |
| `engine.lamb_cutout_prices` | Lamb IMPS cutout prices |
| `engine.lamb_carcass_summary` | Lamb carcass composite prices |
| `engine.manual_species_prices` | Chicken/goat manual price entries |
| `engine.demand_snapshots` | Demand analysis snapshots |
| `engine.allocations` | Allocation analysis results |
| `engine.income_snapshots` | Revenue/margin analysis snapshots |
| `engine.actual_cuts` | Post-processing weight reconciliation |

---

## 3. Order Lifecycle

### 10-Step Lifecycle

```
1. FARMER LISTS ANIMAL
   Farmer creates entry in farmer_inventory (status: available)
   via website farmer dashboard or direct DB insert.

2. CUSTOMER PLACES ORDER
   D2C customer selects animal + portion on website marketplace.
   Creates purchase_order (status: pending) + po_lines (status: pending).
   Deposit = 15% of total_estimated.

3. OPTIMIZER ASSEMBLES POs
   optimizer.py groups pending POs into Assemblies that tile a carcass.
   Sort: largest portion first, then oldest order.
   Place each PO in the best-fit assembly by geographic proximity.

4. TRIGGER EVALUATION
   Each assembly checked for trigger conditions:
   - "ready": fullness >= 90%
   - "forced" (cattle only): fullness >= 75% AND oldest PO > 30 days old
   - "hold": otherwise (keep accumulating)

5. ANIMAL MATCHING
   For triggered assemblies, find best available animal:
   - Hard filter: grade >= strictest in assembly
   - Prefer exact grade match (don't waste prime on choice orders)
   - Score: farmer_distance + size_penalty (age-based relaxation for non-cattle)
   - Yield sufficiency check: animal must cover >= 80% of demand

6. PROCESSOR SELECTION
   Score eligible processors by:
   customer_distance × 1.0 + farmer_distance × 0.7 +
   processing_cost × 0.5 + waste_penalty × 0.3
   Skip processors at daily capacity.

7. SLAUGHTER ORDER CREATED
   Creates slaughter_order (status: planned) + slaughter_order_lines.
   Allocates yield vector to PO lines; remainder becomes LOR lines.
   Updates: animal -> reserved, POs -> planned.
   Fulfills po_lines with allocated lbs.

8. PROCESSING + WEIGHT RECORDING
   Processor slaughters animal, fabricates cuts.
   Actual weights recorded via record_actual_cuts().
   Updates actual_lbs on slaughter_order_lines.
   SO status: planned -> in_progress.

9. RECONCILIATION
   finalize_slaughter_order() reconciles actuals vs estimates:
   - actual_allocated_to_po = min(actual_lbs, customer ordered qty)
   - Remainder -> actual_allocated_to_lor
   - Updates po_lines.actual_lbs
   - Recalculates purchase_orders.total_final
   - SO status -> completed

10. INVOICING + PAYMENT
    generate_po_invoice() creates invoice based on actual weights.
    Invoice status: draft -> sent -> partial -> paid
    PO status -> fulfilled when all lines have actual_lbs.
```

### Status State Machines

**farmer_inventory.status:**
```
available ──> reserved ──> processing ──> complete
    │              │
    └──────────────┴──> sold
```

**purchase_orders.status:**
```
pending ──> planned ──> processing ──> fulfilled ──> cancelled
    │                                                    ^
    └────────────────────────────────────────────────────┘
```

**po_lines.status:**
```
pending ──> partial ──> fulfilled
    │                       ^
    └───────────────────────┘ (if first allocation fills completely)
    │
    └──> cancelled
```

**slaughter_orders.status:**
```
planned ──> confirmed ──> in_progress ──> completed
    │                                         ^
    │         (weight recording auto-moves     │
    │          planned -> in_progress)         │
    │                                          │
    └──> cancelled                             │
              └────────────────────────────────┘
```

**invoices.status:**
```
draft ──> sent ──> partial ──> paid
                      │
                      └──> overdue
```

### Assembly Model

**Cattle — 4-slot model (2 front + 2 hind quarters):**
```
┌─────────────────────────────┐
│          CARCASS             │
│  ┌──────────┬──────────┐    │
│  │ FRONT 1  │ FRONT 2  │    │  Primals: Rib, Chuck, Brisket, Plate
│  ├──────────┼──────────┤    │
│  │  HIND 1  │  HIND 2  │    │  Primals: Loin, Round, Flank
│  └──────────┴──────────┘    │
└─────────────────────────────┘

Allowed portions:
  whole         = 2F + 2H (fills carcass)
  half          = 1F + 1H
  quarter_front = 1F + 0H
  quarter_hind  = 0F + 1H

Example tiling:
  1 half (1F+1H) + 1 quarter_front (1F) + 1 quarter_hind (1H) = 100%
  2 halves = 100%
  4 quarters (2F + 2H) = 100%
```

**Pork / Lamb / Goat — 2-slot model (2 sides):**
```
┌─────────────────────────────┐
│          CARCASS             │
│  ┌──────────┬──────────┐    │
│  │  LEFT    │  RIGHT   │    │
│  │  SIDE    │  SIDE    │    │
│  └──────────┴──────────┘    │
└─────────────────────────────┘

Allowed portions:
  whole = 2 sides (fills carcass)
  half  = 1 side

Primals per species:
  Pork:  front [Butt, Picnic, Sparerib, Jowl], hind [Loin, Ham, Belly]
  Lamb:  front [Rack, Shoulder, Breast/Shank], hind [Loin, Leg]
```

### Optimizer Scoring

**Assembly → Processor scoring** (`OPTIMIZER_WEIGHTS`):

| Weight | Value | Description |
|--------|-------|-------------|
| `customer_distance` | 1.0 | $/mile equivalent — avg customer-to-processor distance |
| `farmer_distance` | 0.7 | Farmer-to-processor distance |
| `processing_cost` | 0.5 | Normalized $/head processing cost |
| `waste_penalty` | 0.3 | Penalty per % of carcass going to LOR |

**Score formula:**
```
score = (1.0 × avg_cust_dist) + (0.7 × farmer_dist) +
        (0.5 × proc_cost) + (0.3 × lor_pct)
```
Lower score wins.

**Animal matching** (`MATCH_WEIGHTS`):

| Weight | Value | Description |
|--------|-------|-------------|
| `farmer_distance` | 1.0 | Farmer-to-assembly centroid distance |
| `size_penalty` | 0.8 | Deviation from target weight range |

Size penalty is asymmetric: too small penalized 2x more than too large (customer gets less product).

**Target weight ranges:**

| Species | Low (lbs) | High (lbs) |
|---------|-----------|------------|
| Cattle | 1,200 | 1,400 |
| Pork | 260 | 290 |
| Lamb | 120 | 145 |
| Goat | 75 | 95 |

### Trigger Thresholds

| Parameter | Value | Notes |
|-----------|-------|-------|
| Default fullness threshold | 90% | Assembly triggers at this fullness |
| Forced trigger (cattle only) | 75% + 30 days | If oldest PO > 30 days AND >= 75% full |
| Non-cattle stale PO handling | Age-based proximity relaxation | Instead of forced triggers |

**Age-based proximity relaxation** (pork/lamb/goat):

| PO Age (days) | farmer_distance weight multiplier |
|---------------|-----------------------------------|
| 0-29 | 1.00 (normal) |
| 30-44 | 0.50 (relaxed) |
| 45+ | 0.25 (very relaxed) |

This means stale POs accept animals from farther away rather than forcing
an incomplete carcass to processing.

---

## 4. Website Role & Boundaries

### Website Capabilities

| Route | Page | Tables Read | Tables Written |
|-------|------|-------------|----------------|
| `/` | Home | — | — |
| `/marketplace` | Marketplace | farmers, farmer_inventory, config_cut_specs | — |
| `/animals/:id` | Animal Detail | farmer_inventory, farmers, config_cut_specs | purchase_orders, po_lines (via RPC) |
| `/farms/:id` | Farm Profile | farmers, farmer_inventory | — |
| `/dashboard/family` | Family Dashboard | dtc_customers, purchase_orders, po_lines | dtc_customers |
| `/dashboard/farmer` | Farmer Dashboard | farmers, farmer_inventory | farmers, farmer_inventory |
| `/dashboard/admin` | Admin Dashboard | All public tables, waitlist_requests, contact_requests | waitlist_requests, contact_requests, processors |
| `/processors` | Processor Directory | processors, config_processors | — |
| `/processors/:id` | Processor Detail | processors, config_processors, config_processor_capabilities | — |
| `/service-area` | Service Area | — | — |
| `/for-farmers` | Farmer Guide | — | — |
| `/about` | About | — | — |
| `/contact` | Contact | — | contact_requests |
| `/login` | Login | — | — (auth) |
| `/signup` | Signup | — | dtc_customers or farmers (during onboarding) |

### Website API Operations

From `marketplaceApi.js`:

| Function | Action | Tables |
|----------|--------|--------|
| `getMarketplaceSnapshot()` | Reads all marketplace data | dtc_customers, farmers, farmer_inventory, purchase_orders, po_lines, processors, config_processors, config_cut_specs, waitlist_requests, contact_requests, latest_valuations (view) |
| `createPurchaseOrder()` | Place D2C order | purchase_orders, po_lines (via `create_purchase_order` RPC) |
| `createAnimalListing()` | Farmer lists animal | farmer_inventory |
| `upsertFarmerProfile()` | Create/update farmer | farmers |
| `upsertCustomerProfile()` | Create/update customer | dtc_customers |
| `createWaitlistRequest()` | Join waitlist | waitlist_requests |
| `createContactRequest()` | Submit contact form | contact_requests |
| `upsertProcessorProfile()` | Admin manages processors | processors |
| `updateWaitlistRequestStatus()` | Admin updates waitlist | waitlist_requests |
| `updateContactRequestStatus()` | Admin updates contacts | contact_requests |

### What the Website Does NOT Do

The website has **no involvement** in:

- **Optimizer logic** — PO assembly, animal matching, processor selection, trigger evaluation
- **Slaughter order creation** — entirely driven by `optimizer.py`
- **Weight recording** — `record_actual_cuts()` in `optimizer_db.py`
- **Reconciliation** — `finalize_slaughter_order()` in `optimizer_db.py`
- **Invoicing** — `generate_po_invoice()` in `optimizer_db.py`
- **B2B orders** — `engine.buyers`, `engine.orders`, `engine.order_lines` are invisible to REST
- **USDA data fetching** — `cattle_valuation.py`, `pork_valuation.py`, `lamb_valuation.py`
- **Manual price entry** — `manual_entry.py` (chicken/goat)
- **Valuation calculations** — all species valuation scripts
- **Config management** — `config_loader.py`, `cli_config.py` (DB-backed config)

### Interface Contract

**Website writes → Optimizer reads:**

| Data | Written By | Read By |
|------|-----------|---------|
| `farmers` (profiles, lat/lng) | Website / farmer dashboard | `optimizer.py` `_get_farmer_location()` |
| `farmer_inventory` (animal listings) | Website / farmer dashboard | `optimizer.py` via `db.get_available_animals()` |
| `dtc_customers` (lat/lng) | Website / signup | `optimizer.py` `get_pending_pos_for_assembly()` JOIN |
| `purchase_orders` + `po_lines` | Website / marketplace | `optimizer.py` `get_pending_pos_for_assembly()` |

**Optimizer writes → Website reads:**

| Data | Written By | Read By |
|------|-----------|---------|
| `slaughter_orders` (status, dates) | `optimizer.py` / `optimizer_db.py` | Website (family dashboard — order tracking) |
| `slaughter_order_lines` | `optimizer.py` | Website (order detail) |
| `invoices` | `optimizer_db.py` `generate_po_invoice()` | Website (family dashboard) |
| `farmer_inventory.status` changes | `optimizer.py` (available → reserved) | Website (marketplace availability) |
| `purchase_orders.status` changes | `optimizer.py` (pending → planned → fulfilled) | Website (family dashboard) |
| `purchase_orders.total_final` | `optimizer_db.py` reconciliation | Website (order total display) |

### Two-Schema Layout

| Schema | Access | Purpose |
|--------|--------|---------|
| `public` | REST API (anon + authenticated via RLS) | Marketplace, customer/farmer data, order tracking, processor directory, config lookup |
| `engine` | Python direct connection only | USDA market data, valuations, B2B buyers/orders, demand analysis, actual_cuts, backend config |

### RLS Summary

| Table | anon (unauthenticated) | authenticated | Policy |
|-------|----------------------|---------------|--------|
| `farmers` | SELECT | SELECT, INSERT (own), UPDATE (own) | Anyone reads; farmer_id = auth.uid() for writes |
| `farmer_inventory` | SELECT | SELECT, INSERT/UPDATE/DELETE (own farm) | Anyone reads; farmer FK check for writes |
| `dtc_customers` | — | SELECT/INSERT/UPDATE (own row) | customer_id = auth.uid() |
| `purchase_orders` | — | SELECT/INSERT/UPDATE (own) | customer_id = auth.uid() |
| `po_lines` | — | SELECT/INSERT (own PO's) | po_number IN own POs |
| `config_processors` | SELECT | SELECT | Read-only for everyone |
| `config_cut_specs` | SELECT | SELECT | Read-only for everyone |
| `processors` | SELECT | SELECT | Read-only for everyone |
| `slaughter_orders` | — | SELECT | Any authenticated user (future: restrict to relevant parties) |
| `slaughter_order_lines` | — | SELECT | Via slaughter_orders access |
| `invoices` | — | SELECT (own) | customer_id = auth.uid() |
| `engine.*` | — | — | No grants — invisible to REST API |

### Grants

```
anon:          SELECT on farmers, farmer_inventory, config_processors,
               config_cut_specs, processors
authenticated: SELECT, INSERT, UPDATE on ALL public tables (RLS enforces row-level)
engine.*:      NO grants to anon/authenticated (postgres role has full access)
```

---

## Source Files

| Content | Source |
|---------|--------|
| Buyer type markups, species config, processing rates, grade hierarchy, status constants | `src/config.py` |
| Assembly logic (SPECIES_PORTION_CONFIG, Assembly class), scoring weights (OPTIMIZER_WEIGHTS, MATCH_WEIGHTS), trigger thresholds, age-based relaxation | `src/optimizer.py` |
| Weight recording, reconciliation, invoicing, processor CRUD, cut specs CRUD | `src/optimizer_db.py` |
| All table schemas, indexes, RLS policies, grants | `sql/supabase_init.sql` |
| D2C lat/lng migration | `sql/003_add_dtc_geo.sql` |
| Website routes | `TerraMensa-Website/src/App.jsx` |
| Website API operations | `TerraMensa-Website/src/services/marketplaceApi.js` |
