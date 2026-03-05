CREATE TABLE IF NOT EXISTS technique_knowledge (
    id TEXT PRIMARY KEY,
    technique_slug TEXT NOT NULL,
    sector TEXT NOT NULL,
    article_markdown TEXT,
    real_world_tolerances TEXT,
    real_world_materials TEXT,
    real_world_equipment TEXT,
    real_world_surface_finishes TEXT,
    typical_batch_sizes TEXT,
    tips_and_insights TEXT,
    common_applications TEXT,
    supplier_count INTEGER DEFAULT 0,
    source_company_ids TEXT,
    generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    pushed_at DATETIME
);
