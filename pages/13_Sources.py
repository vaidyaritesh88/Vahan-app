"""Page 13: Sources & Definitions - Data provenance, methodology, and key terms."""
import streamlit as st
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.title("Sources & Definitions")

# ══════════════════════════════════════
# DATA SOURCES
# ══════════════════════════════════════
st.header("Data Sources")

st.subheader("1. Retail / Sell-out Sales (Vahan Portal)")
st.markdown("""
| Attribute | Detail |
|-----------|--------|
| **Source** | [Vahan Portal](https://vahan.parivahan.gov.in/vahan4dashboard/) — Ministry of Road Transport & Highways, Govt. of India |
| **What it measures** | Vehicle **registrations** at RTOs across India. This represents retail sales (sell-out) — the point at which a vehicle is sold to the end customer and registered. **Domestic only** — vehicles registered in India (exports are never included since they are never registered at Indian RTOs). |
| **Granularity** | Monthly, by State, by OEM (Maker), by Vehicle Category, by Fuel Type |
| **Coverage** | All states and UTs **except Telangana and Odisha** (these states do not report to Vahan consistently) |
| **History** | Available from April 2017 onwards in our database |
| **Update frequency** | Can be next day, but for best results scrape with at least a 7-day lag after month-end |
| **Scraping method** | HTTP scraper (state-level data) + Selenium scraper (national subsegment data with checkbox filters) |

**Important caveat:** Since Telangana and Odisha data is missing, absolute retail volumes are ~5-8% lower than actual national totals. This is why we use YoY growth rates (not absolute volumes) when comparing with primary sales.
""")

st.subheader("2. Primary / Sell-in Sales (OEM Filings)")
st.markdown("""
| Attribute | Detail |
|-----------|--------|
| **Source** | Monthly sales data reported by OEMs (company filings, SIAM, press releases) |
| **What it measures** | Factory **dispatches** (wholesale) from manufacturer to dealer. This represents primary sales (sell-in) — vehicles shipped from factory to dealership, not yet sold to end customer. **Domestic only** — exports are excluded. |
| **Granularity** | Monthly, by OEM, by Model, by Sub-segment (Entry HB, Compact SUV, etc.) |
| **Coverage** | All India (complete national coverage, domestic dispatches only) |
| **History** | PV: October 2014 onwards; 2W: June 2008 onwards |
| **Update frequency** | Monthly, typically within 15-20 days of month-end |
| **Import method** | Excel upload via Data Management page |
""")

st.divider()

# ══════════════════════════════════════
# KEY DEFINITIONS
# ══════════════════════════════════════
st.header("Key Definitions")

st.subheader("Sales Types")
st.markdown("""
| Term | Definition |
|------|-----------|
| **Primary Sales (Sell-in)** | Factory dispatches — vehicles shipped from OEM factory to dealerships. Reflects production/supply decisions. |
| **Retail Sales (Sell-out)** | Vehicle registrations at RTOs — vehicles sold to end customers. Reflects actual consumer demand. |
| **Channel Inventory** | The gap between primary and retail. If primary > retail consistently, inventory is building at dealerships. If retail > primary, destocking is occurring. |
""")

st.subheader("Vehicle Categories")
st.markdown("""
| Category | Full Name | Examples |
|----------|-----------|----------|
| **PV** | Passenger Vehicles | Cars, SUVs, MPVs, Vans |
| **2W** | Two Wheelers | Motorcycles, Scooters, Mopeds, Electric 2Ws |
| **3W** | Three Wheelers | Auto-rickshaws, e-rickshaws, goods carriers |
| **CV** | Commercial Vehicles | Trucks (LCV, MHCV), Buses |
| **TRACTORS** | Tractors | Farm tractors (registered under LMV on Vahan portal) |
""")

st.subheader("PV Sub-segments (Primary Sales)")
st.markdown("""
**PC (Passenger Cars)** — traditional body styles:

| Sub-segment | Examples |
|-------------|----------|
| Entry Hatchback | Maruti Alto, Renault Kwid |
| Compact Hatchback | Maruti WagonR, Tata Tiago |
| Premium Hatchback | Maruti Swift, Hyundai Grand i10 |
| Super Premium Hatchback | Maruti Baleno, Hyundai i20, Tata Altroz |
| Compact Sedan | Maruti Dzire, Honda Amaze |
| Upper Sedan | Honda City, Hyundai Verna |
| Vans | Maruti Eeco |

**UV (Utility Vehicles)** — SUVs and MPVs:

| Sub-segment | Examples |
|-------------|----------|
| Sub-compact SUV | Tata Punch, Hyundai Exter |
| Compact SUV | Tata Nexon, Maruti Brezza, Hyundai Venue |
| Mid-SUV | Hyundai Creta, Maruti Grand Vitara, Kia Seltos |
| Premium SUV | Mahindra XUV700, Tata Harrier, MG Hector |
| MUV | Maruti Ertiga, Toyota Innova, Kia Carens |

**PV = PC + UV**
""")

st.subheader("2W Sub-segments (Primary Sales)")
st.markdown("""
**Motorcycle** sub-segments (by price/positioning):

| Sub-segment | Examples |
|-------------|----------|
| Economy | Hero HF Deluxe, Bajaj CT100 |
| Entry Executive | Hero Splendor, Bajaj Platina |
| Executive | Hero Glamour, Honda Shine, Bajaj Pulsar 125 |
| Premium | Honda Unicorn, Bajaj Pulsar 150, TVS Apache |
| Sports | KTM Duke 200/390, Bajaj Dominar |
| Sports Super Premium | Honda CBR, Kawasaki, Suzuki Hayabusa |
| Classic Premium | Royal Enfield Classic/Meteor/Hunter/Himalayan |
| Classic Super Premium | Harley-Davidson, Triumph |

**Other 2W types:** Scooter (Honda Activa, TVS Jupiter), Moped (TVS XL), EV (Ola, Ather, TVS iQube)

**2W = Motorcycle + Scooter + Moped + EV**
""")

st.subheader("Powertrain / Fuel Types (Retail Data)")
st.markdown("""
| Type | Definition |
|------|-----------|
| **ICE** | Internal Combustion Engine — petrol or diesel. Computed as: Base Category Total minus all subsegments. |
| **EV** | Battery Electric Vehicle — pure electric, no ICE. Tracked separately as EV_PV, EV_2W, EV_3W. |
| **CNG** | Compressed Natural Gas — factory-fitted CNG. Tracked as PV_CNG. |
| **Hybrid** | Strong Hybrid — parallel hybrid with significant electric-only range. Tracked as PV_HYBRID. Does not include mild hybrids. |
""")

st.divider()

# ══════════════════════════════════════
# METRICS & METHODOLOGY
# ══════════════════════════════════════
st.header("Metrics & Methodology")

st.subheader("Time Periods")
st.markdown("""
| Term | Definition |
|------|-----------|
| **FY (Financial Year)** | April to March. FY26 = April 2025 to March 2026. |
| **YTDFY (Year-to-Date FY)** | April through the reference month. YTDFY26 as of Feb\'26 = Apr\'25 to Feb\'26 (11 months). |
| **Quarter** | 1Q = Apr-Jun, 2Q = Jul-Sep, 3Q = Oct-Dec, 4Q = Jan-Mar. |
| **Reference Month** | The latest month included in the analysis. Defaults to the last fully completed month (current partial month excluded). |
""")

st.subheader("Growth Rates")
st.markdown("""
| Metric | Formula | Note |
|--------|---------|------|
| **YoY %** | (Current Period / Same Period Last Year - 1) × 100 | Most reliable for seasonality. Shows "—" if prior year data unavailable. |
| **MoM %** | (Current Month / Previous Month - 1) × 100 | Affected by seasonality (e.g., festive months always higher). |
| **QoQ %** | (Current Quarter / Previous Quarter - 1) × 100 | Less seasonal noise than MoM. |
""")

st.subheader("Incomplete Period Handling")
st.markdown("""
When viewing **Quarterly** or **Financial Year** frequency:
- If a quarter has fewer than 3 months of data, it is marked as incomplete
- If a FY has fewer than 12 months, it is labeled as **YTDFY** (e.g., YTDFY26)
- YoY growth is **suppressed** ("—") for incomplete periods and for periods whose prior-year comparator is incomplete
- This prevents unfair comparisons (e.g., 2 months vs 3 months)
""")

st.subheader("Rebased Volume Index (Primary vs Retail)")
st.markdown("""
Since retail data is missing Telangana and Odisha, absolute volumes cannot be compared directly. Instead, we use a **rebased index**:

**Indexₜ = (Volumeₜ / Volume₀) × 100**

where Volume₀ is the volume in the user-selected base month.

Both Primary and Retail indices start at 100 in the base month. Divergence between the two lines indicates cumulative inventory buildup (Primary > Retail) or destocking (Retail > Primary).
""")

st.divider()

# ══════════════════════════════════════
# DATA CAVEATS
# ══════════════════════════════════════
st.header("Data Caveats & Known Limitations")

st.markdown("""
1. **Domestic sales only:** Both datasets reflect **domestic Indian sales only**. Primary (sell-in) data from OEM filings excludes exports — only factory dispatches to Indian dealerships are captured. Retail (sell-out) data from Vahan is inherently domestic since it tracks registrations at Indian RTOs. Export volumes are NOT reflected anywhere in this dashboard.

2. **Missing states in retail data:** Telangana and Odisha do not report to the Vahan portal consistently. Retail (sell-out) absolute volumes are therefore ~5-8% lower than actual national totals.

3. **Tractor classification:** On the Vahan portal, tractors are registered under vehicle category LMV (Light Motor Vehicle). We exclude known tractor OEMs (Mahindra Tractors, Sonalika, Escorts, TAFE, John Deere, etc.) from the PV category and show them separately as TRACTORS.

4. **OEM name normalization:** The Vahan portal and OEM filings use different legal entity names (e.g., "TATA MOTORS LTD", "TATA MOTORS PASSENGER VEHICLES LTD", "TATA PASSENGER ELECTRIC MOBILITY LTD" all map to "Tata Motors"). Our normalization maps these to canonical names for consistent analysis.

5. **Primary vs Retail OEM matching:** Not all OEMs in primary data have exact matches in retail data (and vice versa). The comparison works best for major OEMs where both datasets have consistent coverage.

6. **COVID base effects:** April-June 2020 had near-zero volumes due to lockdowns, causing April-June 2021 to show absurdly high YoY growth (+1000%+). Charts clip the YoY axis to -80% to +120% and mark outliers with triangle markers.

7. **Data freshness:** Retail data depends on Vahan portal scraping (run locally). Primary data depends on Excel file uploads. Check the Data Management page for last update timestamps.
""")
