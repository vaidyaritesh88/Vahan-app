"""Database schema creation and management."""
import sqlite3
import os
from config.settings import DB_PATH, DATA_DIR


def get_connection():
    """Get a SQLite connection with WAL mode enabled."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    """Create all database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
    -- Vehicle categories (PV, 2W, 3W, etc.)
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        parent_code TEXT,
        is_subsegment INTEGER DEFAULT 0,
        base_category_code TEXT,
        display_order INTEGER DEFAULT 0
    );

    -- Normalized OEM master list
    CREATE TABLE IF NOT EXISTS oems (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        normalized_name TEXT UNIQUE NOT NULL,
        display_name TEXT NOT NULL
    );

    -- OEM aliases: raw names -> normalized OEM
    CREATE TABLE IF NOT EXISTS oem_aliases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_name TEXT NOT NULL,
        oem_id INTEGER NOT NULL,
        source TEXT NOT NULL DEFAULT 'excel',
        FOREIGN KEY (oem_id) REFERENCES oems(id),
        UNIQUE(raw_name, source)
    );

    -- Indian states/UTs
    CREATE TABLE IF NOT EXISTS states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        region TEXT
    );

    -- National monthly OEM volumes (from Excel + Vahan national)
    CREATE TABLE IF NOT EXISTS national_monthly (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_code TEXT NOT NULL,
        oem_name TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        volume REAL DEFAULT 0,
        source TEXT DEFAULT 'excel',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(category_code, oem_name, year, month)
    );

    -- State monthly OEM volumes (from Vahan scraping)
    CREATE TABLE IF NOT EXISTS state_monthly (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_code TEXT NOT NULL,
        oem_name TEXT NOT NULL,
        state TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        volume REAL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(category_code, oem_name, state, year, month)
    );

    -- Weekly trend data (from Excel weekly sheets)
    CREATE TABLE IF NOT EXISTS weekly_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_code TEXT NOT NULL,
        oem_name TEXT NOT NULL,
        week_ending DATE NOT NULL,
        cumulative_volume REAL DEFAULT 0,
        period_volume REAL,
        num_days INTEGER,
        UNIQUE(category_code, oem_name, week_ending)
    );

    -- Scrape job log
    CREATE TABLE IF NOT EXISTS scrape_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_code TEXT NOT NULL,
        state TEXT,
        year INTEGER NOT NULL,
        month INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        error_message TEXT,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        rows_inserted INTEGER DEFAULT 0
    );

    -- Data load log
    CREATE TABLE IF NOT EXISTS load_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        load_type TEXT NOT NULL,
        status TEXT NOT NULL,
        records_loaded INTEGER DEFAULT 0,
        error_message TEXT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- National OEM x Vehicle Category (monthly)
    CREATE TABLE IF NOT EXISTS national_oem_vehcat (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oem_name TEXT NOT NULL,
        oem_raw TEXT NOT NULL,
        veh_category TEXT NOT NULL,
        category_group TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        volume INTEGER DEFAULT 0,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(oem_raw, veh_category, year, month)
    );

    -- National OEM x Fuel (monthly)
    CREATE TABLE IF NOT EXISTS national_oem_fuel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oem_name TEXT NOT NULL,
        oem_raw TEXT NOT NULL,
        fuel_type TEXT NOT NULL,
        fuel_group TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        volume INTEGER DEFAULT 0,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(oem_raw, fuel_type, year, month)
    );

    -- National OEM x Vehicle Class (monthly) for Tractor/CE/Bus detail
    CREATE TABLE IF NOT EXISTS national_oem_vehclass (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oem_name TEXT NOT NULL,
        oem_raw TEXT NOT NULL,
        veh_class TEXT NOT NULL,
        class_group TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        volume INTEGER DEFAULT 0,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(oem_raw, veh_class, year, month)
    );

    -- OEM hierarchy: sub-entity to parent mapping with business metadata
    CREATE TABLE IF NOT EXISTS oem_hierarchy (
        oem_raw TEXT PRIMARY KEY,
        oem_normalized TEXT NOT NULL,
        parent_oem TEXT NOT NULL,
        sub_brand TEXT,
        business_category TEXT,
        fuel_profile TEXT,
        notes TEXT
    );

    -- Scrape metadata: tracks national scrape runs
    CREATE TABLE IF NOT EXISTS scrape_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrape_type TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER,
        state TEXT,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        rows_upserted INTEGER DEFAULT 0,
        status TEXT DEFAULT 'running',
        UNIQUE(scrape_type, year, month, state)
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_national_cat_ym ON national_monthly(category_code, year, month);
    CREATE INDEX IF NOT EXISTS idx_national_oem ON national_monthly(oem_name);
    CREATE INDEX IF NOT EXISTS idx_state_cat_state ON state_monthly(category_code, state, year, month);
    CREATE INDEX IF NOT EXISTS idx_state_oem ON state_monthly(oem_name, state);
    CREATE INDEX IF NOT EXISTS idx_weekly_cat ON weekly_trends(category_code, week_ending);

    CREATE INDEX IF NOT EXISTS idx_novc_oem ON national_oem_vehcat(oem_name, year, month);
    CREATE INDEX IF NOT EXISTS idx_novc_cat ON national_oem_vehcat(category_group, year, month);
    CREATE INDEX IF NOT EXISTS idx_nof_oem ON national_oem_fuel(oem_name, year, month);
    CREATE INDEX IF NOT EXISTS idx_nof_fuel ON national_oem_fuel(fuel_group, year, month);
    CREATE INDEX IF NOT EXISTS idx_novcl_oem ON national_oem_vehclass(oem_name, year, month);
    CREATE INDEX IF NOT EXISTS idx_novcl_class ON national_oem_vehclass(class_group, year, month);
    CREATE INDEX IF NOT EXISTS idx_hier_parent ON oem_hierarchy(parent_oem);
    CREATE INDEX IF NOT EXISTS idx_hier_norm ON oem_hierarchy(oem_normalized);
    CREATE INDEX IF NOT EXISTS idx_smeta_type ON scrape_metadata(scrape_type, year, month);
    """)

    conn.commit()
    conn.close()


def seed_categories():
    """Seed the categories table with configured categories."""
    from config.settings import CATEGORY_CONFIG
    conn = get_connection()
    cursor = conn.cursor()
    for code, cfg in CATEGORY_CONFIG.items():
        cursor.execute("""
            INSERT OR IGNORE INTO categories (code, name, parent_code, is_subsegment, base_category_code, display_order)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (code, cfg["name"], cfg["parent"], 1 if cfg["is_subsegment"] else 0, cfg["base"], cfg["order"]))
    conn.commit()
    conn.close()


def seed_states():
    """Seed the states table."""
    from config.settings import STATE_TO_REGION
    conn = get_connection()
    cursor = conn.cursor()
    for state, region in STATE_TO_REGION.items():
        cursor.execute("INSERT OR IGNORE INTO states (name, region) VALUES (?, ?)", (state, region))
    conn.commit()
    conn.close()


def init_db():
    """Initialize the database: create tables and seed reference data."""
    create_tables()
    seed_categories()
    seed_states()
    return True


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
