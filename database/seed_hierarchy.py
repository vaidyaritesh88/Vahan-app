"""Seed oem_hierarchy table with known OEM sub-entity mappings.

This maps raw Vahan maker names to:
- oem_normalized: display name (e.g., "Mahindra")
- parent_oem: group parent (for consolidation)
- sub_brand: optional sub-brand viewable separately (e.g., "Swaraj", "Triumph")
- business_category: what they make (PV, 2W, 3W, CV, TRACTOR, etc.)
- fuel_profile: ICE, EV, or MIXED
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import get_connection


HIERARCHY = [
    # (oem_raw, oem_normalized, parent_oem, sub_brand, business_category, fuel_profile, notes)

    # === Mahindra Group (Auto) ===
    ("MAHINDRA & MAHINDRA LIMITED", "Mahindra", "Mahindra", None, "PV,CV,LCV", "ICE", None),
    ("MAHINDRA & MAHINDRA LTD", "Mahindra", "Mahindra", None, "PV,CV,LCV", "ICE", None),
    ("MAHINDRA ELECTRIC AUTOMOBILE LTD", "Mahindra", "Mahindra", None, "PV", "EV", "PV EV arm (BE 6e, XUV400)"),
    ("MAHINDRA ELECTRIC MOBILITY LIMITED", "Mahindra", "Mahindra", None, "PV,3W", "EV", "EV predecessor entity"),
    ("MAHINDRA REVA ELECTRIC VEHICLES PVT LTD", "Mahindra", "Mahindra", None, "PV", "EV", "Old EV entity (e2o)"),
    ("MAHINDRA LAST MILE MOBILITY LTD", "Mahindra", "Mahindra", None, "3W", "MIXED", "3W arm (Treo, Zor Grand)"),
    ("MAHINDRA VEHICLE MANUFACTURER LIMITED", "Mahindra", "Mahindra", None, "PV,CV", "ICE", "Contract mfg (Chakan)"),
    ("MAHINDRA TWO WHEELERS LTD", "Mahindra", "Mahindra", None, "2W", "ICE", "Discontinued (sold to Classic Legends)"),
    ("MAHINDRA DEFENCE SYSTEMS LTD", "Mahindra", "Mahindra", None, "CV", "ICE", "Defence vehicles (tiny)"),

    # === Mahindra Tractors (separate display, same parent group) ===
    ("MAHINDRA & MAHINDRA LIMITED (TRACTOR)", "Mahindra Tractors", "Mahindra", None, "TRACTOR", "ICE", None),
    ("MAHINDRA & MAHINDRA LTD FARM MACHINERY DIVISION", "Mahindra Tractors", "Mahindra", None, "TRACTOR", "ICE", None),
    ("MAHINDRA GUJARAT TRACTOR LIMITED", "Mahindra Tractors", "Mahindra", None, "TRACTOR", "ICE", None),
    ("MAHINDRA & MAHINDRA LIMITED (SWARAJ DIVISION)", "Mahindra Tractors", "Mahindra", "Swaraj", "TRACTOR", "ICE", "Viewable separately"),
    ("SWARAJ AUTOMOTIVES LTD", "Mahindra Tractors", "Mahindra", "Swaraj", "TRACTOR", "ICE", "Viewable separately"),

    # === Tata Motors ===
    ("TATA MOTORS LTD", "Tata Motors", "Tata Motors", None, "PV,CV,LCV,MHCV", "ICE", None),
    ("TATA MOTORS LIMITED", "Tata Motors", "Tata Motors", None, "PV,CV,LCV,MHCV", "ICE", None),
    ("TATA MOTORS PASSENGER VEHICLES LTD", "Tata Motors", "Tata Motors", None, "PV", "ICE", "PV subsidiary"),
    ("TATA PASSENGER ELECTRIC MOBILITY LTD", "Tata Motors", "Tata Motors", None, "PV", "EV", "PV EV arm (Nexon EV etc.)"),

    # === Bajaj Auto (includes Chetak, Triumph, Yulu) ===
    ("BAJAJ AUTO LTD", "Bajaj Auto", "Bajaj Auto", None, "2W,3W", "ICE", None),
    ("BAJAJ AUTO LIMITED", "Bajaj Auto", "Bajaj Auto", None, "2W,3W", "ICE", None),
    ("CHETAK TECHNOLOGY LTD", "Bajaj Auto", "Bajaj Auto", "Chetak", "2W", "EV", "EV scooter brand"),
    ("CHETAK TECHNOLOGY LIMITED", "Bajaj Auto", "Bajaj Auto", "Chetak", "2W", "EV", "EV scooter brand"),
    ("TRIUMPH MOTORCYCLES (INDIA) PVT LTD", "Bajaj Auto", "Bajaj Auto", "Triumph", "2W", "ICE", "Viewable separately"),
    ("TRIUMPH UK", "Bajaj Auto", "Bajaj Auto", "Triumph", "2W", "ICE", "CBU imports"),
    ("YADEA TECHNOLOGY (IMPORTER: YULU BIKES)", "Bajaj Auto", "Bajaj Auto", "Yulu", "2W", "EV", "Viewable separately"),

    # === Hero MotoCorp (includes Harley) ===
    ("HERO MOTOCORP LTD", "Hero MotoCorp", "Hero MotoCorp", None, "2W", "ICE", None),
    ("HERO MOTOCORP LIMITED", "Hero MotoCorp", "Hero MotoCorp", None, "2W", "ICE", None),
    ("HARLEY DAVIDSON (IMPORTER: HERO MOTOCORP)", "Hero MotoCorp", "Hero MotoCorp", "Harley", "2W", "ICE", "Viewable separately (X440)"),

    # === Maruti Suzuki ===
    ("MARUTI SUZUKI INDIA LTD", "Maruti Suzuki", "Maruti Suzuki", None, "PV,LCV", "ICE", None),
    ("MARUTI SUZUKI INDIA LIMITED", "Maruti Suzuki", "Maruti Suzuki", None, "PV,LCV", "ICE", None),

    # === Hyundai ===
    ("HYUNDAI MOTOR INDIA LTD", "Hyundai", "Hyundai", None, "PV", "MIXED", None),
    ("HYUNDAI MOTOR INDIA LIMITED", "Hyundai", "Hyundai", None, "PV", "MIXED", None),
    ("HYUNDAI MOTORS LTD, SOUTH KOREA", "Hyundai", "Hyundai", None, "PV", "MIXED", "CBU imports (Ioniq, Nexo)"),

    # === Kia ===
    ("KIA INDIA PRIVATE LIMITED", "Kia", "Kia", None, "PV", "MIXED", None),
    ("KIA INDIA PVT LTD", "Kia", "Kia", None, "PV", "MIXED", None),
    ("KIA MOTORS INDIA PVT LTD", "Kia", "Kia", None, "PV", "MIXED", "Old name pre-rebrand"),

    # === Toyota ===
    ("TOYOTA KIRLOSKAR MOTOR PVT LTD", "Toyota", "Toyota", None, "PV", "MIXED", None),
    ("TOYOTA KIRLOSKAR MOTOR PRIVATE LIMITED", "Toyota", "Toyota", None, "PV", "MIXED", None),

    # === Honda Cars ===
    ("HONDA CARS INDIA LTD", "Honda Cars", "Honda Cars", None, "PV", "ICE", None),
    ("HONDA CARS INDIA LIMITED", "Honda Cars", "Honda Cars", None, "PV", "ICE", None),

    # === Honda 2W ===
    ("HONDA MOTORCYCLE AND SCOOTER INDIA (P) LTD", "Honda 2W", "Honda 2W", None, "2W", "MIXED", None),
    ("HONDA MOTORCYCLE AND SCOOTER INDIA PVT LTD", "Honda 2W", "Honda 2W", None, "2W", "MIXED", None),

    # === Skoda / VW ===
    ("SKODA AUTO INDIA PVT LTD", "Skoda", "Skoda VW", "Skoda", "PV", "ICE", "Imports (Kodiaq, Superb CBU)"),
    ("SKODA AUTO VOLKSWAGEN INDIA PVT LTD", "Skoda VW", "Skoda VW", None, "PV", "ICE", "Combined mfg entity"),
    ("VOLKSWAGEN INDIA PVT LTD", "Volkswagen", "Skoda VW", "VW", "PV", "ICE", None),

    # === Ashok Leyland ===
    ("ASHOK LEYLAND LTD", "Ashok Leyland", "Ashok Leyland", None, "CV,LCV,MHCV,BUS", "ICE", None),
    ("ASHOK LEYLAND LIMITED", "Ashok Leyland", "Ashok Leyland", None, "CV,LCV,MHCV,BUS", "ICE", None),

    # === Daimler / BharatBenz ===
    ("DAIMLER INDIA COMMERCIAL VEHICLES PVT LTD", "Daimler", "Daimler", None, "MHCV,BUS", "ICE", None),
    ("DAIMLER INDIA COMMERCIAL VEHICLES PVT. LTD", "Daimler", "Daimler", None, "MHCV,BUS", "ICE", None),

    # === VECV (Eicher CV arm) ===
    ("VE COMMERCIAL VEHICLES LTD", "VECV", "VECV", None, "CV,LCV,MHCV,BUS", "ICE", None),
    ("VE COMMERCIAL VEHICLES LIMITED", "VECV", "VECV", None, "CV,LCV,MHCV,BUS", "ICE", None),

    # === TAFE/Eicher (Tractors) ===
    ("EICHER MOTORS LTD", "TAFE/Eicher", "TAFE/Eicher", None, "TRACTOR", "ICE", None),
    ("EICHER TRACTORS", "TAFE/Eicher", "TAFE/Eicher", None, "TRACTOR", "ICE", None),

    # === TVS Motor ===
    ("TVS MOTOR COMPANY LTD", "TVS Motor", "TVS Motor", None, "2W,3W", "MIXED", None),
    ("TVS MOTOR COMPANY LIMITED", "TVS Motor", "TVS Motor", None, "2W,3W", "MIXED", None),

    # === Yamaha ===
    ("INDIA YAMAHA MOTOR PVT LTD", "Yamaha", "Yamaha", None, "2W", "ICE", None),

    # === Royal Enfield ===
    ("ROYAL ENFIELD", "Royal Enfield", "Royal Enfield", None, "2W", "ICE", None),
    ("ROYAL-ENFIELD (UNIT OF EICHER LTD)", "Royal Enfield", "Royal Enfield", None, "2W", "ICE", None),

    # === Suzuki 2W ===
    ("SUZUKI MOTORCYCLE INDIA PVT LTD", "Suzuki 2W", "Suzuki 2W", None, "2W", "ICE", None),

    # === Ather Energy ===
    ("ATHER ENERGY PVT LTD", "Ather Energy", "Ather Energy", None, "2W", "EV", None),
    ("ATHER ENERGY LTD", "Ather Energy", "Ather Energy", None, "2W", "EV", None),

    # === Ola Electric ===
    ("OLA ELECTRIC TECHNOLOGIES PVT LTD", "Ola Electric", "Ola Electric", None, "2W", "EV", None),
]


def seed_hierarchy():
    """Insert/update OEM hierarchy rows."""
    conn = get_connection()
    cursor = conn.cursor()
    rows = 0
    for row in HIERARCHY:
        cursor.execute("""
            INSERT INTO oem_hierarchy
                (oem_raw, oem_normalized, parent_oem, sub_brand,
                 business_category, fuel_profile, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(oem_raw) DO UPDATE SET
                oem_normalized=excluded.oem_normalized,
                parent_oem=excluded.parent_oem,
                sub_brand=excluded.sub_brand,
                business_category=excluded.business_category,
                fuel_profile=excluded.fuel_profile,
                notes=excluded.notes
        """, row)
        rows += 1
    conn.commit()
    conn.close()
    return rows


if __name__ == "__main__":
    rows = seed_hierarchy()
    print(f"Seeded {rows} OEM hierarchy rows")

    # Verify sub-brands
    conn = get_connection()
    subs = conn.execute(
        "SELECT parent_oem, sub_brand FROM oem_hierarchy WHERE sub_brand IS NOT NULL ORDER BY parent_oem"
    ).fetchall()
    print(f"\nSub-brands ({len(subs)}):")
    for r in subs:
        print(f"  {r['parent_oem']} -> {r['sub_brand']}")
    conn.close()
