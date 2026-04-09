"""
OEM name normalization: maps raw names from Excel/Vahan to canonical display names.
Multiple legal entities that belong to the same company are merged.
"""

# Raw name -> Canonical display name
# Covers names from both the Excel raw OEM rows AND the grouped OEM section
OEM_NORMALIZATION = {
    # === PV OEMs ===
    "MARUTI SUZUKI INDIA LTD": "Maruti Suzuki",
    "MARUTI SUZUKI INDIA LIMITED": "Maruti Suzuki",
    "Maruti": "Maruti Suzuki",
    "Maruti Suzuki": "Maruti Suzuki",
    "HYUNDAI MOTOR INDIA LTD": "Hyundai",
    "HYUNDAI MOTOR INDIA LIMITED": "Hyundai",
    "Hyundai": "Hyundai",
    "TATA MOTORS LTD": "Tata Motors",
    "TATA MOTORS LIMITED": "Tata Motors",
    "TATA MOTORS PASSENGER VEHICLES LTD": "Tata Motors",
    "TATA PASSENGER ELECTRIC MOBILITY LTD": "Tata Motors",
    "Tata Motors": "Tata Motors",
    "MAHINDRA & MAHINDRA LIMITED": "Mahindra",
    "MAHINDRA & MAHINDRA LTD": "Mahindra",
    "M&M": "Mahindra",
    "Mahindra": "Mahindra",
    "TOYOTA KIRLOSKAR MOTOR PVT LTD": "Toyota",
    "TOYOTA KIRLOSKAR MOTOR PRIVATE LIMITED": "Toyota",
    "Toyota": "Toyota",
    "KIA MOTORS INDIA PVT LTD": "Kia",
    "KIA INDIA PRIVATE LIMITED": "Kia",
    "KIA INDIA PVT LTD": "Kia",
    "Kia": "Kia",
    "HONDA CARS INDIA LTD": "Honda Cars",
    "HONDA CARS INDIA LIMITED": "Honda Cars",
    "Honda Cars": "Honda Cars",
    "Honda": "Honda Cars",  # PV context
    "MG MOTOR INDIA PVT LTD": "MG Motor",
    "MG MOTOR INDIA PRIVATE LIMITED": "MG Motor",
    "MG": "MG Motor",
    "SKODA AUTO INDIA PVT LTD": "Skoda",
    "SKODA AUTO VOLKSWAGEN INDIA PVT LTD": "Skoda VW",
    "Skoda": "Skoda",
    "VOLKSWAGEN INDIA PVT LTD": "Volkswagen",
    "Volkswagen": "Volkswagen",
    "RENAULT INDIA PVT LTD": "Renault",
    "Renault": "Renault",
    "NISSAN MOTOR INDIA PVT LTD": "Nissan",
    "Nissan": "Nissan",
    "BMW INDIA PVT LTD": "BMW",
    "BMW": "BMW",
    "MERCEDES-BENZ INDIA PVT LTD": "Mercedes-Benz",
    "MERCEDES -BENZ AG": "Mercedes-Benz",
    "Mercedes-Benz": "Mercedes-Benz",
    "AUDI AG": "Audi",
    "Audi": "Audi",
    "JAGUAR LAND ROVER INDIA LIMITED": "JLR",
    "JLR": "JLR",
    "FORD INDIA PVT LTD": "Ford",
    "Ford": "Ford",
    "FIAT INDIA AUTOMOBILES PVT LTD": "Fiat",
    "Fiat": "Fiat",
    "FORCE MOTORS LTD": "Force Motors",
    "FORCE MOTORS LIMITED, A FIRODIA ENTERPRISE": "Force Motors",
    "Force Motors": "Force Motors",
    "PCA AUTOMOBILES INDIA PVT LTD": "Citroen",
    "Citroen": "Citroen",
    "BYD INDIA PVT LTD": "BYD",
    "BYD INDIA PRIVATE LIMITED": "BYD",
    "BYD": "BYD",
    "PORSCHE INDIA": "Porsche",
    "Porsche": "Porsche",
    "VOLVO AUTO INDIA PVT LTD": "Volvo",
    "Volvo": "Volvo",
    "JSW MG MOTOR INDIA PVT LTD": "MG Motor",
    "MG MOTOR INDIA Electric PVT LTD": "MG Motor",
    "MG MOTOR INDIA ELECTRIC PVT LTD": "MG Motor",
    "HYUNDAI MOTORS LTD, SOUTH KOREA": "Hyundai",
    "TATA PASSENGER VEHICLES LTD": "Tata Motors",  # distinct from TATA MOTORS PASSENGER VEHICLES LTD

    # === 2W OEMs ===
    "HERO MOTOCORP LTD": "Hero MotoCorp",
    "HERO MOTOCORP LIMITED": "Hero MotoCorp",
    "Hero MotoCorp": "Hero MotoCorp",
    "HONDA MOTORCYCLE AND SCOOTER INDIA (P) LTD": "Honda 2W",
    "HONDA MOTORCYCLE AND SCOOTER INDIA PVT LTD": "Honda 2W",
    "Honda ": "Honda 2W",  # Note: Excel has trailing space
    "Honda 2W": "Honda 2W",
    "TVS MOTOR COMPANY LTD": "TVS Motor",
    "TVS MOTOR COMPANY LIMITED": "TVS Motor",
    "TVS": "TVS Motor",
    "TVS Motor": "TVS Motor",
    "BAJAJ AUTO LTD": "Bajaj Auto",
    "BAJAJ AUTO LIMITED": "Bajaj Auto",
    "Bajaj": "Bajaj Auto",
    "Bajaj Auto": "Bajaj Auto",
    "INDIA YAMAHA MOTOR PVT LTD": "Yamaha",
    "Yamaha": "Yamaha",
    "ROYAL-ENFIELD (UNIT OF EICHER LTD)": "Royal Enfield",
    "ROYAL ENFIELD": "Royal Enfield",
    "Royal Enfield": "Royal Enfield",
    "SUZUKI MOTORCYCLE INDIA PVT LTD": "Suzuki 2W",
    "Suzuki ": "Suzuki 2W",  # Note: Excel has trailing space
    "Suzuki 2W": "Suzuki 2W",

    # === EV 2W OEMs ===
    "OLA ELECTRIC TECHNOLOGIES PVT LTD": "Ola Electric",
    "Ola Electric": "Ola Electric",
    "ATHER ENERGY PVT LTD": "Ather Energy",
    "Ather Energy": "Ather Energy",
    "HERO ELECTRIC VEHICLES PVT LTD": "Hero Electric",
    "HERO ELECTRIC VEHICLES PVT. LTD": "Hero Electric",
    "HERO ELECTRIC VEHICLE PVT LTD": "Hero Electric",
    "Hero Electric": "Hero Electric",
    "OKINAWA AUTOTECH PVT LTD": "Okinawa",
    "Okinawa": "Okinawa",
    "AMPERE VEHICLES PVT LTD": "Ampere",
    "AMPERE VEHICLES PRIVATE LIMITED": "Ampere",
    "Ampere": "Ampere",
    "REVOLT INTELLICORP PVT LTD": "Revolt",
    "Revolt": "Revolt",
    "PUR ENERGY PVT LTD": "Pur Energy",
    "CHETAK TECHNOLOGY LTD": "Bajaj Auto",  # Chetak is Bajaj brand
    "CHETAK TECHNOLOGY LIMITED": "Bajaj Auto",
    "BAJAJ/CHETAK": "Bajaj Auto",
    "CLASSIC LEGENDS PVT LTD": "Classic Legends",
    "ULTRAVIOLETTE AUTOMOTIVE PVT LTD": "Ultraviolette",

    # === 3W OEMs ===
    "ATUL AUTO LTD": "Atul Auto",
    "ATUL AUTO LIMITED": "Atul Auto",
    "Atul Auto": "Atul Auto",
    "ATUL GREENTECH PVT LTD": "Atul Auto",
    "ATUL GREENTECH PRIVATE LIMITED": "Atul Auto",
    "BAJAJ AUTO LTD ": "Bajaj Auto",  # trailing space variant
    "PIAGGIO VEHICLES PVT LTD": "Piaggio",
    "Piaggio": "Piaggio",
    "MAHINDRA LAST MILE MOBILITY LTD": "Mahindra",
    "MAHINDRA REVA ELECTRIC VEHICLES PVT LTD": "Mahindra",
    "OMEGA SEIKI MOBILITY PVT LTD": "Omega Seiki",
    "OMEGA SEIKI PVT LTD": "Omega Seiki",
    "Omega Seiki": "Omega Seiki",
    "TVS MOTOR COMPANY LTD ": "TVS Motor",  # trailing space variant
    "CONTINENTAL ENGINES PVT LTD": "Continental",
    "ALTIGREEN PROPULSION LABS PVT LTD": "Altigreen",

    # === CV OEMs ===
    "ASHOK LEYLAND LTD": "Ashok Leyland",
    "ASHOK LEYLAND LIMITED": "Ashok Leyland",
    "Ashok Leyland": "Ashok Leyland",
    "VECV": "VECV",
    "VE COMMERCIAL VEHICLES LTD": "VECV",
    "VE COMMERCIAL VEHICLES LIMITED": "VECV",
    "DAIMLER INDIA COMMERCIAL VEHICLES PVT LTD": "Daimler",
    "DAIMLER INDIA COMMERCIAL VEHICLES PVT. LTD": "Daimler",
    "Daimler": "Daimler",
    "VOLVO INDIA PVT LTD": "Volvo CV",
    "SML ISUZU LTD": "SML Isuzu",
    "SML ISUZU LIMITED": "SML Isuzu",
    "ISUZU MOTORS INDIA PVT LTD": "Isuzu",

    # === Tractor OEMs ===
    "MAHINDRA & MAHINDRA LIMITED (TRACTOR)": "Mahindra Tractors",
    "MAHINDRA & MAHINDRA LTD FARM MACHINERY DIVISION": "Mahindra Tractors",
    "MAHINDRA GUJARAT TRACTOR LIMITED": "Mahindra Tractors",
    "MAHINDRA & MAHINDRA LIMITED (SWARAJ DIVISION)": "Mahindra Tractors",
    "ESCORTS LIMITED": "Escorts",
    "ESCORTS KUBOTA LIMITED": "Escorts",
    "ESCORTS LIMITED (AGRI MACHINERY GROUP)": "Escorts",
    "ESCORTS KUBOTA LIMITED (AGRI MACHINERY GROUP)": "Escorts",
    "ESCORTS CONSTRUCTION EQUIPMENT LTD": "Escorts",
    "ESCORTS R&D CENTRE": "Escorts",
    "ESCORTS KUBOTA LIMITED (CONSTRUCTION EQUIPMENT)": "Escorts",
    "ADICO ESCORTS AGRI EQUIPMENTS PVT. LTD.": "Escorts",
    "ESCORTS LTD": "Escorts",
    "ESCORTS AUTOMOTIVE LTD": "Escorts",
    "ESCORTS TRACTORS LTD": "Escorts",
    "Escorts": "Escorts",
    "JOHN DEERE INDIA PVT LTD": "John Deere",
    "JOHN DEERE INDIA  PVT LTD(TRACTOR DEVISION)": "John Deere",
    "JOHN DEERE INDIA PVT LTD(CROP SOLUTION DIV)": "John Deere",
    "John Deere": "John Deere",
    "KUBOTA AGRICULTURAL MACHINERY INDIA PVT LTD": "Kubota",
    "KUBOTA AGRICULTURAL MACHINERY INDIA PVT.LTD.": "Kubota",
    "Kubota": "Kubota",
    "SONALIKA INTERNATIONAL TRACTORS LTD": "Sonalika",
    "SONALIKA INTERNATIONAL TRACTORS LIMITED": "Sonalika",
    "INTERNATIONAL TRACTORS LIMITED": "Sonalika",
    "SONALIKA INDUSTRIES": "Sonalika",
    "Sonalika": "Sonalika",
    "TAFE MOTORS AND TRACTORS LTD": "TAFE",
    "TRACTORS AND FARM EQUIPMENT LTD": "TAFE",
    "EICHER MOTORS LTD": "TAFE/Eicher",
    "EICHER TRACTORS": "TAFE/Eicher",
    "TAFE/Eicher": "TAFE/Eicher",
    "VST TILLERS TRACTORS LTD": "VST Tillers",
    "VST Tillers": "VST Tillers",
    "CASE NEW HOLLAND CONSTRUCTION EQUIPMENT (INDIA) PVT LTD": "Case New Holland",
    "CASE NEW HOLLAND CONSTRUCTION EQUIPMENT(I) PVT LTD": "Case New Holland",
    "CNH INDUSTRIAL (INDIA) PVT LTD": "Case New Holland",
    "L&T-CASE EQUIPMENT PVT LTD": "Case New Holland",
    "L & T CASE EQUIPMENT PVT LTD": "Case New Holland",
    "NEW HOLLAND FIAT (INDIA) PVT LTD": "Case New Holland",
    "NEW HOLLAND FIAT INDIA PVT. LTD.": "Case New Holland",
    "NEW HOLLAND CONSTRUCTION EQUIPMENT(I) PVT.LTD.": "Case New Holland",
    "Case New Holland": "Case New Holland",
    "CAPTAIN TRACTORS PVT LTD": "Captain",
    "CAPTAIN TRACTORS PVT. LTD.": "Captain",
    "PREET GROUP": "Preet",
    "PREET TRACTORS PVT LTD": "Preet",
    "SWARAJ AUTOMOTIVES LTD": "Mahindra Tractors",
    "TAFE LIMITED": "TAFE",
    "V.S.T. TILLERS TRACTORS LIMITED": "VST Tillers",

    # === EV 3W / Small OEMs ===
    "EULER MOTORS PVT LTD": "Euler Motors",
    "KINETIC GREEN ENERGY & POWER SOLUTIONS LTD": "Kinetic Green",
    "TI CLEAN MOBILITY PVT LTD": "TI Clean Mobility",
    "DILLI ELECTRIC AUTO PVT LTD": "Dilli Electric",
    "E ROYCE MOTORS INDIA PVT LTD": "E Royce",
    "REEP INDUSTRIES PVT LTD": "Reep Industries",
    "ALTIGREEN PROPULSION LABS PVT LTD": "Altigreen",

    # === EV 2W Small OEMs ===
    "BEING INDIA ENERGY AND TECHNOLOGY PVT LTD": "Being",
    "JITENDRA NEW EV-TECH PVT. LTD": "Jitendra EV",
    "LECTRIX EV PVT LTD": "Lectrix EV",
    "GOREEN E-MOBILITY PVT LTD": "Goreen",
    "GREAVES ELECTRIC MOBILITY PVT LTD": "Greaves Electric",
    "OKAYA EV PVT LTD": "Okaya EV",
    "MEW ELECTRICALS LIMITED": "MEW Electric",
    "ATHER ENERGY LTD": "Ather Energy",
    "MAHINDRA ELECTRIC AUTOMOBILE LTD": "Mahindra",
    "MAHINDRA DEFENCE SYSTEMS LTD": "Mahindra",  # defence division


    # === Cross-brand / partnership mappings ===
    "MAHINDRA ELECTRIC MOBILITY LIMITED": "Mahindra",
    "MAHINDRA TWO WHEELERS LTD": "Mahindra",
    "MAHINDRA VEHICLE MANUFACTURER LIMITED": "Mahindra",
    "TRIUMPH MOTORCYCLES (INDIA) PVT LTD": "Bajaj Auto",
    "TRIUMPH UK": "Bajaj Auto",
    "YADEA TECHNOLOGY (IMPORTER: YULU BIKES)": "Bajaj Auto",
    "HARLEY DAVIDSON (IMPORTER: HERO MOTOCORP)": "Hero MotoCorp",
}

# Category-specific overrides: when the same raw name means different OEMs
# depending on which category sheet it comes from
CATEGORY_SPECIFIC_OVERRIDES = {
    # "Honda" in PV context = Honda Cars, in 2W = Honda 2W
    ("Honda", "PV"): "Honda Cars",
    ("Honda", "EV_PV"): "Honda Cars",
    ("Honda", "PV_CNG"): "Honda Cars",
    ("Honda", "PV_HYBRID"): "Honda Cars",
    ("Honda", "2W"): "Honda 2W",
    ("Honda", "EV_2W"): "Honda 2W",
    ("Honda ", "2W"): "Honda 2W",
    ("Honda ", "EV_2W"): "Honda 2W",
    # "Suzuki" only in 2W
    ("Suzuki ", "2W"): "Suzuki 2W",
    # Mahindra in tractors context
    ("Mahindra", "TRACTORS"): "Mahindra Tractors",
    ("M&M", "TRACTORS"): "Mahindra Tractors",
    # M&M in CV context = Mahindra (auto)
    ("M&M", "LCV"): "Mahindra",
    ("M&M", "MHCV"): "Mahindra",
    ("M&M", "CV"): "Mahindra",
    ("M&M", "3W"): "Mahindra",
    ("M&M", "EV_3W"): "Mahindra",
    # Escorts R&D Centre is tractor-related
    ("ESCORTS R&D CENTRE", "TRACTORS"): "Escorts",
}


def normalize_oem(raw_name, category_code=None):
    """Normalize an OEM name, optionally with category context."""
    if raw_name is None:
        return None
    name = str(raw_name).strip()
    if not name or name.upper() in ("OTHERS", "TOTAL", "GRAND TOTAL"):
        if name.upper() == "OTHERS":
            return "Others"
        return None

    # Check category-specific override first
    if category_code:
        key = (name, category_code)
        if key in CATEGORY_SPECIFIC_OVERRIDES:
            return CATEGORY_SPECIFIC_OVERRIDES[key]

    # Check global normalization map
    if name in OEM_NORMALIZATION:
        return OEM_NORMALIZATION[name]

    # Try case-insensitive match
    name_upper = name.upper()
    for raw, canonical in OEM_NORMALIZATION.items():
        if raw.upper() == name_upper:
            return canonical

    # Return cleaned name if no mapping found
    return name.strip()
