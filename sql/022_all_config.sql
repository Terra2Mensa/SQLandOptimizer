-- 022_all_config.sql
-- Move ALL hardcoded values to optimizer_config.
-- Every tunable number in the optimizer is now database-configurable.

INSERT INTO optimizer_config (key, value) VALUES
-- Dressing percentages (biological — species-specific)
('dress_pct_cattle',        '0.60'),
('dress_pct_pork',          '0.72'),
('dress_pct_lamb',          '0.50'),
('dress_pct_goat',          '0.50'),

-- Typical live weights (reference for per-lb price estimates)
('typical_live_weight_cattle', '1200'),
('typical_live_weight_pork',   '275'),
('typical_live_weight_lamb',   '115'),
('typical_live_weight_goat',   '90'),

-- Quality scoring (Phase 4 advanced)
('quality_score_premium',    '1.0'),
('quality_score_standard',   '0.7'),
('quality_score_economy',    '0.4'),
('quality_bonus_grass',      '0.1'),
('quality_bonus_breed',      '0.05'),

-- Processor reliability (Phase 4 advanced)
('default_reliability_score',    '0.8'),
('reliability_weight_ontime',    '0.6'),
('reliability_weight_quality',   '0.4'),

-- Hold-back logic (rolling horizon)
('hold_dispatch_threshold',  '0.85'),
('hold_min_fill',            '0.60'),
('hold_age_threshold',       '7'),
('hold_min_age',             '3'),

-- Payment processing (external — Stripe rates)
('stripe_card_pct',          '0.029'),
('stripe_card_flat',         '0.30'),
('stripe_ach_pct',           '0.008'),
('stripe_ach_cap',           '5.00'),

-- MIP solver tuning
('mip_assignment_bonus',     '100000'),
('mip_time_limit_seconds',   '60')

ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
