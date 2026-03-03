"""Page 6: Data Management - Excel upload, Vahan scraper, data freshness."""
import streamlit as st
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_data_freshness, get_load_history, get_record_counts,
    get_scrape_log_summary, get_state_data_freshness,
)
from components.formatters import format_month, format_units
from config.settings import DATA_DIR, ALL_STATES, VAHAN_SCRAPE_CONFIGS

init_db()

st.set_page_config(page_title="Data Management", page_icon="⚙️", layout="wide")
st.title("Data Management")

tab1, tab2, tab3 = st.tabs(["Excel Import", "State Data Scraper", "Data Status"])

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

# ── Tab 2: State Data Scraper ──
with tab2:
    st.subheader("State-Level Data Scraper")
    st.markdown("""
    Scrape state-level registration data from the Vahan portal.
    Data is stored with **upsert logic** — re-scraping the same combination
    will update existing records without creating duplicates.
    """)

    # Connection test
    col_test, col_info = st.columns([1, 2])
    with col_test:
        if st.button("Test Connection"):
            with st.spinner("Testing connection to Vahan portal..."):
                try:
                    from scraper.vahan_http_scraper import VahanHttpScraper
                    scraper = VahanHttpScraper()
                    ok, msg = scraper.test_connection()
                    if ok:
                        st.success(msg)
                        # Remember SSL setting for scraping session
                        if not scraper.verify_ssl:
                            st.session_state["vahan_verify_ssl"] = False
                    else:
                        st.error(msg)
                except Exception as e:
                    st.error(f"Error: {str(e)}")
    with col_info:
        st.info("The scraper uses HTTP requests — no Chrome/Selenium needed.")

    st.divider()

    # ── Scrape Configuration ──
    st.markdown("### Configure Scrape Job")

    # Key categories for investment analysis
    key_categories = ["2W", "PV", "3W", "LCV", "MHCV", "EV_2W", "EV_PV", "EV_3W", "TRACTORS"]
    # Key states (top 15 by vehicle registrations)
    key_states = [
        "Maharashtra", "Tamil Nadu", "Karnataka", "Gujarat", "Uttar Pradesh",
        "Rajasthan", "Delhi", "Haryana", "Kerala", "Madhya Pradesh",
        "Andhra Pradesh", "Telangana", "West Bengal", "Punjab", "Bihar",
    ]

    col1, col2 = st.columns(2)
    with col1:
        scrape_categories = st.multiselect(
            "Categories to scrape",
            list(VAHAN_SCRAPE_CONFIGS.keys()),
            default=key_categories,
            key="scrape_cats",
        )
    with col2:
        scrape_states = st.multiselect(
            "States to scrape",
            ALL_STATES,
            default=key_states,
            key="scrape_states",
        )

    col3, col4, col5 = st.columns(3)
    with col3:
        year_range = st.slider(
            "Year Range",
            min_value=2019, max_value=2026,
            value=(2020, 2026),
            key="year_range",
        )
        scrape_years = list(range(year_range[0], year_range[1] + 1))
    with col4:
        skip_existing = st.checkbox("Skip already scraped", value=True, key="skip_existing",
                                    help="Skip state/category/year combos that were already successfully scraped")
    with col5:
        delay_between = st.slider("Delay between requests (sec)", 1, 10, 2, key="delay",
                                  help="Pause between scrape calls to avoid rate limiting")

    # Calculate job size
    total_combos = len(scrape_categories) * len(scrape_states) * len(scrape_years)

    pending_combos = total_combos
    if skip_existing:
        try:
            from scraper.vahan_http_scraper import get_pending_scrapes
            pending = get_pending_scrapes(scrape_categories, scrape_states, scrape_years)
            pending_combos = len(pending)
        except Exception:
            pending_combos = total_combos

    st.markdown(
        f"**Job size:** {total_combos} total combinations "
        f"({'**' + str(pending_combos) + ' pending**' if skip_existing else 'all will be scraped'})"
    )

    if pending_combos > 100:
        est_minutes = (pending_combos * (delay_between + 3)) / 60
        st.caption(f"Estimated time: ~{est_minutes:.0f} minutes at {delay_between}s delay")

    st.divider()

    # ── Start Scraping ──
    if st.button("Start Scraping", type="primary", disabled=(pending_combos == 0)):
        import time

        progress_bar = st.progress(0)
        status_text = st.empty()
        results_container = st.container()

        success_count = 0
        fail_count = 0
        total_rows = 0

        try:
            from scraper.vahan_http_scraper import VahanHttpScraper, get_pending_scrapes

            # Determine what to scrape
            if skip_existing:
                jobs = get_pending_scrapes(scrape_categories, scrape_states, scrape_years)
            else:
                jobs = [(c, s, y) for c in scrape_categories
                        for s in scrape_states for y in scrape_years]

            if not jobs:
                st.success("All combinations already scraped! Uncheck 'Skip already scraped' to re-fetch.")
            else:
                verify_ssl = st.session_state.get("vahan_verify_ssl", True)
                scraper = VahanHttpScraper(verify_ssl=verify_ssl)

                for i, (cat, state, year) in enumerate(jobs):
                    status_text.text(f"Scraping {cat} / {state} / {year}  ({i+1}/{len(jobs)})")
                    try:
                        rows = scraper.scrape_and_store(cat, state, year)
                        total_rows += rows
                        success_count += 1
                    except Exception as e:
                        fail_count += 1
                        with results_container:
                            st.warning(f"Failed: {cat}/{state}/{year} — {str(e)[:100]}")

                    progress_bar.progress((i + 1) / len(jobs))
                    time.sleep(delay_between)

                status_text.empty()
                st.success(
                    f"Scraping complete! "
                    f"{success_count} succeeded, {fail_count} failed, "
                    f"{total_rows:,} total records upserted."
                )
                if fail_count > 0:
                    st.info("Failed jobs can be retried by running the scraper again with the same settings.")
                st.rerun()

        except ImportError as e:
            st.error(f"Import error: {str(e)}")
        except Exception as e:
            st.error(f"Scraper error: {str(e)}")

    st.divider()

    # ── Scrape History ──
    st.markdown("### Scrape History")
    scrape_log = get_scrape_log_summary()
    if not scrape_log.empty:
        # Summary stats
        success_log = scrape_log[scrape_log["status"] == "success"]
        failed_log = scrape_log[scrape_log["status"] == "failed"]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Successful Scrapes", len(success_log))
        with col2:
            st.metric("Failed Scrapes", len(failed_log))
        with col3:
            st.metric("Total Rows Scraped", format_units(success_log["total_rows"].sum()) if not success_log.empty else "0")

        with st.expander("Full Scrape Log"):
            st.dataframe(scrape_log, use_container_width=True, hide_index=True)
    else:
        st.info("No scrape history yet. Run the scraper above to populate state-level data.")

# ── Tab 3: Data Status ──
with tab3:
    st.subheader("Data Overview")

    counts = get_record_counts()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("National Monthly Records", f"{counts['national_monthly']:,}")
    with col2:
        st.metric("State Monthly Records", f"{counts['state_monthly']:,}")
    with col3:
        st.metric("Weekly Trend Records", f"{counts['weekly_trends']:,}")

    st.divider()

    # National data freshness
    st.markdown("**National Data (from Excel):**")
    freshness = get_data_freshness()
    if not freshness.empty:
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

    # State data freshness
    st.markdown("**State-Level Data (from Scraper):**")
    state_freshness = get_state_data_freshness()
    if not state_freshness.empty:
        display_state = state_freshness.copy()
        display_state["latest_month"] = display_state["latest_ym"].apply(
            lambda ym: format_month(ym // 100, ym % 100) if pd.notna(ym) else "N/A"
        )
        # Pivot to show categories as columns
        summary = display_state.groupby("category_code").agg(
            states=("state", "nunique"),
            latest_month=("latest_ym", lambda x: format_month(x.max() // 100, x.max() % 100)),
            total_volume=("total_volume", "sum"),
        ).reset_index()
        summary.columns = ["Category", "States Covered", "Latest Month", "Total Volume"]
        summary["Total Volume"] = summary["Total Volume"].apply(format_units)
        st.dataframe(summary, use_container_width=True, hide_index=True)

        with st.expander("Detailed State Coverage"):
            st.dataframe(
                display_state[["category_code", "state", "latest_month", "oem_count", "total_volume"]].rename(columns={
                    "category_code": "Category",
                    "state": "State",
                    "latest_month": "Latest Month",
                    "oem_count": "OEMs",
                    "total_volume": "Total Volume",
                }),
                use_container_width=True, hide_index=True,
            )
    else:
        st.info("No state-level data yet. Use the **State Data Scraper** tab to populate.")

    st.divider()

    st.markdown("**Load History:**")
    history = get_load_history()
    if not history.empty:
        st.dataframe(history, use_container_width=True, hide_index=True)
    else:
        st.info("No data loads recorded yet.")
