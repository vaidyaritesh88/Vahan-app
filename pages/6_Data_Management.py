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
    cleanup_corrupt_state_data, aggregate_state_to_national,
)
from components.formatters import format_month, format_units
from config.settings import DATA_DIR, ALL_STATES, VAHAN_SCRAPE_CONFIGS

init_db()

st.set_page_config(page_title="Data Management", page_icon="⚙️", layout="wide")
st.title("Data Management")

tab1, tab2, tab3, tab4 = st.tabs(["Excel Import", "State Data Scraper", "Data Status", "Local Setup Guide"])

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
    st.markdown(
        "Scrape state-level registration data from the Vahan portal. "
        "The scraper runs as a **background process** — you can close this tab "
        "or browser and it keeps running. Data is saved in real-time after each combo."
    )

    # Cloud environment detection
    import platform as _plat
    _is_cloud = _plat.system() == "Linux" and "/home/appuser" in os.environ.get("HOME", "")
    if _is_cloud:
        st.warning(
            "**Note:** The Vahan portal blocks cloud server IPs. "
            "Run the scraper **locally** (`streamlit run app.py`)."
        )

    # ── Live Status: check if scraper is running ──
    from scraper.run_background import is_scraper_running, request_stop, CONTROL_FILE

    scraper_running, ctrl_info = is_scraper_running()

    if scraper_running and ctrl_info:
        st.markdown("---")
        status_label = ctrl_info.get("status", "running")
        if status_label == "stopping":
            st.warning("Scraper is **stopping** (finishing current job)...")
        else:
            st.success("Scraper is **running** in the background")

        # Progress metrics
        total_jobs = ctrl_info.get("total_jobs", 0)
        completed = ctrl_info.get("completed", 0)
        bg_success = ctrl_info.get("success", 0)
        bg_failed = ctrl_info.get("failed", 0)
        bg_rows = ctrl_info.get("total_rows", 0)
        current_job = ctrl_info.get("current_job", "")
        started_at = ctrl_info.get("started_at", "")

        # Progress bar
        if total_jobs > 0:
            pct = completed / total_jobs
            st.progress(pct, text=f"{completed} / {total_jobs} combinations ({pct:.0%})")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Completed", f"{completed:,}")
        with col2:
            st.metric("Succeeded", f"{bg_success:,}")
        with col3:
            st.metric("Failed", f"{bg_failed:,}")
        with col4:
            st.metric("Rows Scraped", format_units(bg_rows))

        if current_job:
            st.caption(f"Current: {current_job}")
        if started_at:
            st.caption(f"Started: {started_at[:19]}")

        # Stop buttons -- always visible (no more hiding after "stopping")
        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
        with btn_col1:
            if st.button("Stop Scraping", type="secondary", key="btn_stop"):
                request_stop()
                st.rerun()
        with btn_col2:
            if st.button("Force Kill", type="secondary", key="btn_force_kill",
                          help="Forcefully terminate the scraper process and clean up"):
                import signal
                pid = ctrl_info.get("pid")
                if pid:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except (ProcessLookupError, OSError):
                        pass
                # Clean up control file
                try:
                    os.remove(CONTROL_FILE)
                except FileNotFoundError:
                    pass
                st.success("Scraper process killed and control file cleaned up.")
                import time; time.sleep(1)
                st.rerun()
        with btn_col3:
            st.caption("Use **Force Kill** if Stop doesn't work within 30 seconds.")

        # Non-blocking auto-refresh: just show a refresh button instead of sleeping
        st.caption("Page auto-refreshes every 15 seconds while scraper is running.")
        import time as _time
        _time.sleep(15)
        st.rerun()

    else:
        # ── Scrape Configuration (only show when not running) ──
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
                        else:
                            st.error(msg)
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        with col_info:
            st.info("The scraper uses HTTP requests — no Chrome/Selenium needed.")

        st.divider()
        st.markdown("### Configure Scrape Job")

        key_categories = ["2W", "PV", "3W", "EV_2W", "EV_PV", "EV_3W", "TRACTORS"]
        key_states = [
            "Maharashtra", "Tamil Nadu", "Karnataka", "Gujarat", "Uttar Pradesh",
            "Rajasthan", "Delhi", "Haryana", "Kerala", "Madhya Pradesh",
            "Andhra Pradesh", "Telangana", "West Bengal", "Punjab", "Bihar",
        ]

        all_categories = list(VAHAN_SCRAPE_CONFIGS.keys())

        def _toggle_all_cats():
            if st.session_state.all_cats:
                st.session_state.scrape_cats = all_categories
            else:
                st.session_state.scrape_cats = key_categories

        def _toggle_all_states():
            if st.session_state.all_states:
                st.session_state.scrape_states = list(ALL_STATES)
            else:
                st.session_state.scrape_states = key_states

        col1, col2 = st.columns(2)
        with col1:
            st.checkbox("Select all", key="all_cats", on_change=_toggle_all_cats)
            scrape_categories = st.multiselect(
                "Categories to scrape",
                all_categories,
                default=key_categories,
                key="scrape_cats",
            )
        with col2:
            st.checkbox("Select all", key="all_states", on_change=_toggle_all_states)
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
                                        help="Skip combos already successfully scraped")
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

        # ── Start Scraping (launches background process) ──
        if st.button("Start Scraping", type="primary", disabled=(pending_combos == 0)):
            import subprocess

            cmd = [
                sys.executable, "-m", "scraper.run_background",
                "--categories", *scrape_categories,
                "--years", *[str(y) for y in scrape_years],
                "--states", *scrape_states,
                "--delay", str(delay_between),
            ]
            if not skip_existing:
                cmd.append("--rescrape")

            # Launch detached background process
            try:
                # On Windows, CREATE_NEW_PROCESS_GROUP + DETACHED_PROCESS
                # ensures the process survives if Streamlit exits.
                import platform
                if platform.system() == "Windows":
                    CREATE_NEW_PROCESS_GROUP = 0x00000200
                    DETACHED_PROCESS = 0x00000008
                    subprocess.Popen(
                        cmd,
                        creationflags=CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    )
                else:
                    subprocess.Popen(
                        cmd,
                        start_new_session=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    )
                st.success("Background scraper launched! This page will auto-refresh to show progress.")
                import time; time.sleep(3)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to start scraper: {str(e)}")

    st.divider()

    # ── Scrape History (always visible) ──
    st.markdown("### Scrape History")
    scrape_log = get_scrape_log_summary()
    if not scrape_log.empty:
        success_log = scrape_log[scrape_log["status"] == "success"]
        failed_log = scrape_log[scrape_log["status"] == "failed"]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Successful Scrapes", len(success_log))
        with col2:
            st.metric("Failed Scrapes", len(failed_log))
        with col3:
            st.metric("Total Rows Scraped", format_units(success_log["total_rows"].sum()) if not success_log.empty else "0")

        # Category-level breakdown
        if not success_log.empty:
            with st.expander("Progress by Category"):
                cat_summary = success_log.groupby("category_code").agg(
                    states=("state", "nunique"),
                    years=("year", "nunique"),
                    rows=("total_rows", "sum"),
                ).reset_index()
                cat_summary.columns = ["Category", "States Done", "Years Done", "Total Rows"]
                cat_summary["Total Rows"] = cat_summary["Total Rows"].apply(format_units)
                st.dataframe(cat_summary, use_container_width=True, hide_index=True)

        with st.expander("Full Scrape Log"):
            st.dataframe(scrape_log, use_container_width=True, hide_index=True)
    else:
        st.info("No scrape history yet. Configure and start the scraper above.")

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

    st.divider()

    # ── Data Maintenance Tools ──
    st.markdown("### Data Maintenance")

    col_clean, col_agg = st.columns(2)

    with col_clean:
        st.markdown("**Clean Corrupt Scrape Data**")
        st.caption(
            "Removes state_monthly rows where OEM name is a serial number "
            "(1, 2, 3...) — caused by an earlier parser bug."
        )
        if st.button("Clean Corrupt Data", key="btn_cleanup"):
            with st.spinner("Cleaning corrupt rows..."):
                deleted = cleanup_corrupt_state_data()
                if deleted > 0:
                    st.success(f"Deleted {deleted:,} corrupt rows from state_monthly.")
                else:
                    st.info("No corrupt rows found — data is clean.")
                st.rerun()

    with col_agg:
        st.markdown("**Re-aggregate State → National**")
        st.caption(
            "Sums state-level volumes and upserts into national totals. "
            "Preserves Excel-sourced data; only updates scrape-sourced rows."
        )
        if st.button("Re-aggregate Now", key="btn_reaggregate"):
            with st.spinner("Aggregating state → national..."):
                agg_rows = aggregate_state_to_national()
                st.success(f"Aggregated {agg_rows:,} rows into national_monthly.")
                st.rerun()

# ── Tab 4: Local Setup Guide ──
with tab4:
    st.subheader("Running Locally")
    st.markdown("""
    The Vahan portal blocks cloud server IPs, so **all scraping must be done locally**.
    Follow these steps to run the dashboard and scrapers on your machine.
    """)

    st.markdown("### 1. Start the Dashboard")
    st.code("""cd "C:\\Users\\ritesh.vaidya\\OneDrive\\Documents\\Janchor\\Auto\\Tracking\\Janchor Tracking File\\Vahan\\App"
streamlit run app.py""", language="bash")
    st.caption("Opens at http://localhost:8501")

    st.divider()

    st.markdown("### 2. State-Level Scraper (HTTP)")
    st.markdown("""
    The **State Data Scraper** tab (above) launches the HTTP scraper from the UI itself.
    No extra setup needed — just configure categories/states/years and click **Start Scraping**.

    This scraper:
    - Uses HTTP requests (no Chrome/Selenium needed)
    - Runs in background — survives browser close
    - Populates `state_monthly` table (state-level OEM data)
    """)

    st.divider()

    st.markdown("### 3. Subsegment Scraper (Selenium)")
    st.markdown("""
    The **subsegment scraper** extracts national EV/CNG/Hybrid data using Selenium
    (requires Chrome installed). It handles the Vahan portal's checkbox filters
    that the HTTP scraper cannot.
    """)

    st.markdown("**Prerequisites:**")
    st.code("pip install selenium webdriver-manager", language="bash")

    st.markdown("**Run all subsegments for a fiscal year:**")
    st.code("""python scraper/run_subsegments.py --types EV_PV EV_2W EV_3W PV_CNG PV_HYBRID --fy 2025""", language="bash")
    st.caption("--fy 2025 = FY26 (Apr 2025 – Mar 2026). The parameter is the FY start year.")

    st.markdown("**Run a specific subsegment and month:**")
    st.code("python scraper/run_subsegments.py --types EV_2W --fy 2025 --month 3", language="bash")

    st.markdown("**With visible browser (for debugging):**")
    st.code("python scraper/run_subsegments.py --types EV_PV --fy 2025 --visible", language="bash")

    st.markdown("**Available subsegment types:**")
    sub_info = {
        "EV_PV": "Electric Passenger Vehicles (Tata Nexon EV, MG ZS EV, etc.)",
        "EV_2W": "Electric Two Wheelers (Ola, Ather, TVS iQube, etc.)",
        "EV_3W": "Electric Three Wheelers (e-rickshaws, etc.)",
        "PV_CNG": "CNG Passenger Vehicles (Maruti CNG, Hyundai CNG, etc.)",
        "PV_HYBRID": "Strong Hybrid PV (Toyota Hyryder, Maruti Grand Vitara, etc.)",
    }
    for code, desc in sub_info.items():
        st.markdown(f"- `{code}` — {desc}")

    st.divider()

    st.markdown("### 4. National OEM Scraper (HTTP)")
    st.markdown("""
    The national OEM scraper populates `national_oem_vehcat`, `national_oem_fuel`,
    and `national_oem_vehclass` tables. It uses the same HTTP scraper as the state
    scraper but with Y-axis set to Vehicle Category / Fuel / Maker.
    """)
    st.code("""python -c "
from scraper.vahan_http_scraper import VahanHttpScraper
scraper = VahanHttpScraper()
# Scrape national data for a specific year
scraper.scrape_national_oem(year=2026, modes=('category', 'fuel', 'maker'))
" """, language="bash")

    st.divider()

    st.markdown("### Quick Reference")
    ref_data = {
        "Task": [
            "Start dashboard",
            "Scrape state data",
            "Scrape EV/CNG/Hybrid subsegments",
            "Scrape all subsegments FY26",
            "Backfill FY20–FY25",
        ],
        "Command": [
            "streamlit run app.py",
            "Use State Data Scraper tab in UI",
            "python scraper/run_subsegments.py --types EV_2W --fy 2025",
            "python scraper/run_subsegments.py --types EV_PV EV_2W EV_3W PV_CNG PV_HYBRID --fy 2025",
            "Run above with --fy 2019 through --fy 2024",
        ],
        "Needs Chrome?": ["No", "No", "Yes", "Yes", "Yes"],
    }
    st.dataframe(pd.DataFrame(ref_data), use_container_width=True, hide_index=True)

