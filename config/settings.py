import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "vahan_tracker.db")
DB_URL = f"sqlite:///{DB_PATH}"

VAHAN_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

# Indian fiscal year: April-March
# FY26 = April 2025 - March 2026
# Quarters: 1Q = Apr-Jun, 2Q = Jul-Sep, 3Q = Oct-Dec, 4Q = Jan-Mar

REGIONS = {
    "North": ["Delhi", "Haryana", "Himachal Pradesh", "Jammu and Kashmir",
              "Ladakh", "Punjab", "Rajasthan", "Uttarakhand", "Uttar Pradesh",
              "Chandigarh"],
    "South": ["Andhra Pradesh", "Karnataka", "Kerala", "Tamil Nadu",
              "Telangana", "Puducherry", "Lakshadweep",
              "Andaman and Nicobar Islands"],
    "East": ["Bihar", "Jharkhand", "Odisha", "West Bengal", "Sikkim"],
    "West": ["Goa", "Gujarat", "Maharashtra", "Dadra and Nagar Haveli and Daman and Diu"],
    "Central": ["Chhattisgarh", "Madhya Pradesh"],
    "NE": ["Arunachal Pradesh", "Assam", "Manipur", "Meghalaya", "Mizoram",
           "Nagaland", "Tripura"],
}

# Map state to region for quick lookup
STATE_TO_REGION = {}
for region, states in REGIONS.items():
    for state in states:
        STATE_TO_REGION[state] = region

# All Indian states/UTs
ALL_STATES = sorted(STATE_TO_REGION.keys())

# Category definitions - map our codes to display names and hierarchy
CATEGORY_CONFIG = {
    "PV": {"name": "Passenger Vehicles", "parent": None, "is_subsegment": False, "base": None, "order": 1},
    "2W": {"name": "Two Wheelers", "parent": None, "is_subsegment": False, "base": None, "order": 2},
    "3W": {"name": "Three Wheelers", "parent": None, "is_subsegment": False, "base": None, "order": 3},
    "CV": {"name": "Commercial Vehicles", "parent": None, "is_subsegment": False, "base": None, "order": 4},
    "LCV": {"name": "Light Commercial Vehicles", "parent": "CV", "is_subsegment": False, "base": None, "order": 5},
    "MHCV": {"name": "Medium & Heavy Commercial Vehicles", "parent": "CV", "is_subsegment": False, "base": None, "order": 6},
    "TRACTORS": {"name": "Tractors", "parent": None, "is_subsegment": False, "base": None, "order": 7},
    "EV_PV": {"name": "Electric PV", "parent": None, "is_subsegment": True, "base": "PV", "order": 8},
    "EV_2W": {"name": "Electric 2W", "parent": None, "is_subsegment": True, "base": "2W", "order": 9},
    "EV_3W": {"name": "Electric 3W", "parent": None, "is_subsegment": True, "base": "3W", "order": 10},
    "PV_CNG": {"name": "CNG PV", "parent": None, "is_subsegment": True, "base": "PV", "order": 11},
    "PV_HYBRID": {"name": "Strong Hybrid PV", "parent": None, "is_subsegment": True, "base": "PV", "order": 12},
}

# Map Excel sheet names to category codes
SHEET_TO_CATEGORY = {
    "PV": "PV",
    "Electric PV": "EV_PV",
    "PV CNG": "PV_CNG",
    "PV Strong Hybrid": "PV_HYBRID",
    "2W": "2W",
    "Electric 2W": "EV_2W",
    "3W": "3W",
    "Electric 3W": "EV_3W",
    "CV": "CV",  # Special: has LCV + MHCV sub-sections
    "Tractors": "TRACTORS",
}

WEEKLY_SHEET_TO_CATEGORY = {
    "2W Weekly trends": "2W",
    "Electric 2W Weekly trends": "EV_2W",
    "PV Weekly trends": "PV",
    "Electric PV Weekly trends": "EV_PV",
}

# Mapping from Vahan portal VhClass labels -> our category codes.
# Used when scraping with Y-axis="Vehicle Class" to compute category totals.
# Labels not listed here are ignored (e.g. AMBULANCE, ADAPTED VEHICLE).
VHCLASS_TO_CATEGORY = {
    # PV
    "MOTOR CAR": "PV",
    "MOTOR CAB": "PV",
    # 2W
    "M-CYCLE/SCOOTER": "2W",
    "M-CYCLE/SCOOTER-WITH SIDE CAR": "2W",
    "MOPED": "2W",
    "MOTORISED CYCLE (CC > 25CC)": "2W",
    "MOTOR CYCLE/SCOOTER-SIDECAR(T)": "2W",
    "MOTOR CYCLE/SCOOTER-WITH TRAILER": "2W",
    "MOTOR CYCLE/SCOOTER-USED FOR HIRE": "2W",
    # 3W
    "THREE WHEELER (GOODS)": "3W",
    "THREE WHEELER (PASSENGER)": "3W",
    "THREE WHEELER (PERSONAL)": "3W",
    "E-RICKSHAW WITH CART (G)": "3W",
    "E-RICKSHAW(P)": "3W",
    # CV
    "GOODS CARRIER": "CV",
    "BUS": "CV",
    "MAXI CAB": "CV",
    # Tractors
    "AGRICULTURAL TRACTOR": "TRACTORS",
    "TRACTOR (COMMERCIAL)": "TRACTORS",
}

# Mapping from Vahan portal fuel labels -> our fuel-based sub-category codes.
# Used when scraping with Y-axis="Fuel" to compute EV/CNG/Hybrid totals.
FUEL_TO_SUBCATEGORY = {
    "PURE EV": "EV",
    "ELECTRIC(BOV)": "EV",
    "CNG ONLY": "CNG",
    "PETROL/CNG": "CNG",
    "PETROL(E20)/CNG": "CNG",
    "PLUG-IN HYBRID EV": "HYBRID",
    "STRONG HYBRID EV": "HYBRID",
}

# Vahan scraping filter definitions
#
# IMPORTANT: The Vahan portal exposes only THREE SelectManyCheckbox panels:
#   VhClass (76 vehicle-class options), fuel (36 fuel-type options), norms.
#   There is NO "VhCatg" (vehicle-category) checkbox panel — it only exists
#   as an AJAX update container.  ALL filters must use vehicle_class (→ VhClass)
#   and/or fuel.
#
# LCV and MHCV are NOT scrapable because VhClass has a single "GOODS CARRIER"
# entry that covers both light and heavy goods vehicles — they cannot be split.
# State-level LCV/MHCV data must come from the Excel tracker instead.
VAHAN_SCRAPE_CONFIGS = {
    "PV": {
        "vehicle_class": ["MOTOR CAB", "MOTOR CAR"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "2W": {
        "vehicle_class": [
            "M-CYCLE/SCOOTER", "M-CYCLE/SCOOTER-WITH SIDE CAR", "MOPED",
            "MOTORISED CYCLE (CC > 25CC)",
            "MOTOR CYCLE/SCOOTER-SIDECAR(T)",
            "MOTOR CYCLE/SCOOTER-WITH TRAILER",
            "MOTOR CYCLE/SCOOTER-USED FOR HIRE",
        ],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "3W": {
        "vehicle_class": [
            "THREE WHEELER (GOODS)", "THREE WHEELER (PASSENGER)",
            "THREE WHEELER (PERSONAL)", "E-RICKSHAW WITH CART (G)", "E-RICKSHAW(P)",
        ],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "TRACTORS": {
        "vehicle_class": ["AGRICULTURAL TRACTOR", "TRACTOR (COMMERCIAL)"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "EV_PV": {
        "vehicle_class": ["MOTOR CAB", "MOTOR CAR"],
        "fuel": ["ELECTRIC(BOV)", "PURE EV"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "EV_2W": {
        "vehicle_class": [
            "M-CYCLE/SCOOTER", "M-CYCLE/SCOOTER-WITH SIDE CAR", "MOPED",
            "MOTORISED CYCLE (CC > 25CC)",
            "MOTOR CYCLE/SCOOTER-SIDECAR(T)",
            "MOTOR CYCLE/SCOOTER-WITH TRAILER",
            "MOTOR CYCLE/SCOOTER-USED FOR HIRE",
        ],
        "fuel": ["ELECTRIC(BOV)", "PURE EV"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "EV_3W": {
        "vehicle_class": [
            "THREE WHEELER (GOODS)", "THREE WHEELER (PASSENGER)",
            "THREE WHEELER (PERSONAL)", "E-RICKSHAW WITH CART (G)", "E-RICKSHAW(P)",
        ],
        "fuel": ["ELECTRIC(BOV)", "PURE EV"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "PV_CNG": {
        "vehicle_class": ["MOTOR CAB", "MOTOR CAR"],
        "fuel": ["CNG ONLY", "PETROL/CNG", "PETROL(E20)/CNG"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
    "PV_HYBRID": {
        "vehicle_class": ["MOTOR CAB", "MOTOR CAR"],
        "fuel": ["PLUG-IN HYBRID EV", "STRONG HYBRID EV"],
        "y_axis": "Maker",
        "x_axis": "Month Wise",
    },
}


# ── National scrape: Vehicle Category code → consolidated category ──
# From Vahan X-axis = "Vehicle Category" (17 codes)
VEHCAT_TO_CATEGORY = {
    "2WN": "2W", "2WT": "2W", "2WIC": "2W",
    "3WN": "3W", "3WT": "3W", "3WIC": "3W",
    "LMV": "PV", "LPV": "PV", "4WIC": "PV",
    "LGV": "LCV",
    "MGV": "MHCV", "MMV": "MHCV", "MPV": "MHCV",
    "HGV": "MHCV", "HMV": "MHCV",
    "HPV": "BUS",
    "OTH": "OTHERS",
}

# Sub-groups within MHCV for optional drill-down
VEHCAT_MHCV_DETAIL = {
    "MGV": "MCV", "MMV": "MCV", "MPV": "MCV",
    "HGV": "HCV", "HMV": "HCV",
}

# ── National scrape: Vehicle Class label → consolidated category ──
# From Vahan X-axis = "Vehicle Class" (75+ types)
# Used for Tractor/CE/Bus breakdown that Vehicle Category lumps into OTH/HPV
VEHCLASS_TO_NATIONAL = {
    # 2W
    "M-Cycle/Scooter": "2W",
    "M-Cycle/Scooter-With Side Car": "2W",
    "Moped": "2W",
    "Motorised Cycle (CC > 25cc)": "2W",
    "Motor Cycle/Scooter-SideCar(T)": "2W",
    "Motor Cycle/Scooter-With Trailer": "2W",
    "Motor Cycle/Scooter-Used For Hire": "2W",
    # 3W
    "Three Wheeler (Personal)": "3W",
    "Three Wheeler (Passenger)": "3W",
    "Three Wheeler (Goods)": "3W",
    "e-Rickshaw with Cart (G)": "3W",
    "e-Rickshaw(P)": "3W",
    # PV
    "Motor Car": "PV",
    "Luxury Cab": "PV",
    "Motor Cab": "PV",
    "Maxi Cab": "PV",
    "Adapted Vehicle": "PV",
    "Motor Caravan": "PV",
    "Quadricycle (Private)": "PV",
    "Quadricycle (Commercial)": "PV",
    "Private Service Vehicle (Individual Use)": "PV",
    # CV Goods (combined - use Vehicle Category for LCV/MCV/HCV split)
    "Goods Carrier": "CV_GOODS",
    # Bus
    "Bus": "BUS",
    "School Bus": "BUS",
    "Educational Institution Bus": "BUS",
    "Omni Bus": "BUS",
    "Omni Bus (Private Use)": "BUS",
    "Private Service Vehicle": "BUS",
    # Tractors
    "Agricultural Tractor": "TRACTOR",
    "Power Tiller": "TRACTOR",
    "Tractor (Commercial)": "TRACTOR",
    "Tractor-Trolley(Commercial)": "TRACTOR",
    "Power Tiller (Commercial)": "TRACTOR",
    "Puller Tractor": "TRACTOR",
    # Construction Equipment
    "Construction Equipment Vehicle": "CE",
    "Construction Equipment Vehicle (Commercial)": "CE",
    "Road Roller": "CE",
    "Excavator (NT)": "CE",
    "Excavator (Commercial)": "CE",
    "Bulldozer": "CE",
    "Earth Moving Equipment": "CE",
    "Dumper": "CE",
    "Fork Lift": "CE",
    "Crane Mounted Vehicle": "CE",
}
# Anything not in the map -> "OTHERS"

# ── National scrape: Fuel label → consolidated fuel group ──
# From Vahan X-axis = "Fuel" (36+ types)
FUEL_TO_GROUP = {
    # Petrol (includes E20/ethanol variants — same base fuel)
    "PETROL": "Petrol", "PETROL(E10)": "Petrol",
    "PETROL(E20)": "Petrol", "PETROL(E100)": "Petrol",
    "PETROL/ETHANOL": "Petrol", "ETHANOL": "Petrol",
    # Petrol mild hybrids (MHEV — 48V belt-starter, not self-propelling EV)
    "PETROL/HYBRID": "Petrol",
    "PETROL(E20)/HYBRID": "Petrol",
    # Diesel
    "DIESEL": "Diesel",
    "DUAL DIESEL/LNG": "Diesel",
    # CNG (includes dual-fuel petrol/CNG)
    "CNG ONLY": "CNG", "PETROL/CNG": "CNG",
    "PETROL(E20)/CNG": "CNG", "DUAL DIESEL/BIO CNG": "CNG",
    "PETROL/HYBRID/CNG": "CNG", "PETROL(E20)/HYBRID/CNG": "CNG",
    "DUAL DIESEL/CNG": "CNG",
    # EV
    "PURE EV": "EV", "ELECTRIC(BOV)": "EV",
    # Strong Hybrid (full HEV — Toyota etc.)
    "STRONG HYBRID EV": "Strong Hybrid",
    # Plug-in Hybrid
    "PLUG-IN HYBRID EV": "Plug-in Hybrid",
    # Diesel Hybrid
    "DIESEL/HYBRID": "Diesel Hybrid",
    # LPG
    "LPG ONLY": "LPG", "PETROL/LPG": "LPG",
    "PETROL(E20)/LPG": "LPG",
    "LNG": "CNG",
    # Remaining unmapped -> "Others" (NOT APPLICABLE, SOLAR, HYDROGEN, etc.)
}
# Anything not in the map -> "Others"

# ── Vahan FY dropdown values ──
# Year Type "Financial Year" uses format "YYYY-YYYY" in the dropdown
FY_DROPDOWN_VALUES = {
    2017: "2017-2018", 2018: "2018-2019",
    2019: "2019-2020", 2020: "2020-2021", 2021: "2021-2022",
    2022: "2022-2023", 2023: "2023-2024", 2024: "2024-2025",
    2025: "2025-2026", 2026: "2026-2027",
}

# Month dropdown values on Vahan data table
MONTH_DROPDOWN_MAP = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR",
    5: "MAY", 6: "JUN", 7: "JUL", 8: "AUG",
    9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}
