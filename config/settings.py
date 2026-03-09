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
