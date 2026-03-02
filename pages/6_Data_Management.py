"""Page 6: Data Management - Excel upload, Vahan scraper, data freshness."""
import streamlit as st
import pandas as pd
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import get_data_freshness, get_load_history, get_record_counts
from components.formatters import format_month, MONTH_NAMES
from config.settings import DATA_DIR

init_db()

st.set_page_config(page_title="Data Management", page_icon="⚙️", layout="wide")
st.title("Data Management")

tab1, tab2, tab3 = st.tabs(["Excel Import", "Vahan Scraper", "Data Status"])

# ── Tab 1: Excel Import ──
with tab1:
    st.subheader("Import Excel Data")
    st.markdown("""
    Upload the weekly Vahan retail volumes tracker Excel file.
    The parser will extract OEM-level monthly data for all vehicle categories.
    """)

    uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx", "xls"])

    # Also allow loading from the existing file in the directory
    existing_files = [f for f in os.listdir(os.path.dirname(DATA_DIR))
                      if f.endswith((".xlsx", ".xls")) and "Vahan" in f]

    if existing_files:
        st.markdown("**Or load from existing file in app directory:**")
        selected_existing = st.selectbox("Existing file", ["-- Select --"] + existing_files)

        if selected_existing != "-- Select --" and st.button("Load Existing File"):
            with st.spinner("Parsing Excel file..."):
                try:
                    from data_pipeline.excel_parser import parse_and_load_excel
                    file_path = os.path.join(os.path.dirname(DATA_DIR), selected_existing)
                    stats = parse_and_load_excel(file_path)
                    st.success(f"Loaded {stats['national']:,} national records, {stats['weekly']:,} weekly records.")
                    if stats["errors"]:
                        st.warning(f"Warnings: {', '.join(stats['errors'])}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")

    if uploaded_file:
        if st.button("Parse and Load"):
            with st.spinner("Parsing uploaded Excel file..."):
                try:
                    from data_pipeline.excel_parser import parse_and_load_excel
                    # Save uploaded file temporarily
                    tmp_path = os.path.join(DATA_DIR, uploaded_file.name)
                    os.makedirs(DATA_DIR, exist_ok=True)
                    with open(tmp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    stats = parse_and_load_excel(tmp_path)
                    st.success(f"Loaded {stats['national']:,} national records, {stats['weekly']:,} weekly records.")
                    if stats["errors"]:
                        st.warning(f"Warnings: {', '.join(stats['errors'])}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")

# ── Tab 2: Vahan Scraper ──
with tab2:
    st.subheader("Vahan Portal Scraper")
    st.markdown("""
    Scrape state-level registration data directly from the Vahan portal.
    This requires Chrome/Chromium and the Selenium driver.
    """)

    from config.settings import ALL_STATES, VAHAN_SCRAPE_CONFIGS

    col1, col2, col3 = st.columns(3)
    with col1:
        scrape_categories = st.multiselect(
            "Categories to scrape",
            list(VAHAN_SCRAPE_CONFIGS.keys()),
            default=["PV", "2W"],
        )
    with col2:
        scrape_states = st.multiselect(
            "States to scrape",
            ALL_STATES,
            default=["Maharashtra", "Tamil Nadu", "Karnataka", "Gujarat",
                     "Uttar Pradesh", "Rajasthan", "Delhi", "Haryana",
                     "Kerala", "Madhya Pradesh"],
        )
    with col3:
        scrape_year = st.selectbox("Year", list(range(2026, 2018, -1)))

    if st.button("Start Scraping", type="primary"):
        st.warning("Scraping will take a while depending on the number of state/category combinations.")
        progress_bar = st.progress(0)
        status_text = st.empty()

        total_jobs = len(scrape_categories) * len(scrape_states)
        completed = 0

        try:
            from scraper.vahan_scraper import VahanScraper
            scraper = VahanScraper(headless=True)

            for cat_code in scrape_categories:
                for state in scrape_states:
                    status_text.text(f"Scraping {cat_code} - {state} ({scrape_year})...")
                    try:
                        scraper.scrape_and_store(cat_code, state, scrape_year)
                    except Exception as e:
                        st.warning(f"Failed {cat_code}/{state}: {str(e)}")
                    completed += 1
                    progress_bar.progress(completed / total_jobs)

            scraper.close()
            st.success(f"Scraping complete! Processed {completed} jobs.")
            st.rerun()
        except ImportError:
            st.error("Selenium not installed. Run: `pip install selenium webdriver-manager`")
        except Exception as e:
            st.error(f"Scraper error: {str(e)}")

# ── Tab 3: Data Status ──
with tab3:
    st.subheader("Data Freshness")

    counts = get_record_counts()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("National Monthly Records", f"{counts['national_monthly']:,}")
    with col2:
        st.metric("State Monthly Records", f"{counts['state_monthly']:,}")
    with col3:
        st.metric("Weekly Trend Records", f"{counts['weekly_trends']:,}")

    st.divider()

    freshness = get_data_freshness()
    if not freshness.empty:
        st.markdown("**Latest Data by Category:**")
        display = freshness.copy()
        display["latest_month"] = display["latest_ym"].apply(
            lambda ym: format_month(ym // 100, ym % 100) if pd.notna(ym) else "N/A"
        )
        st.dataframe(
            display[["category_code", "latest_month", "oem_count", "total_records"]].rename(columns={
                "category_code": "Category",
                "latest_month": "Latest Month",
                "oem_count": "OEM Count",
                "total_records": "Total Volume",
            }),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    st.markdown("**Load History:**")
    history = get_load_history()
    if not history.empty:
        st.dataframe(history, use_container_width=True, hide_index=True)
    else:
        st.info("No data loads recorded yet.")
