"""Selenium-based scraper for Vahan portal subsegment data.

This scraper handles queries that require checkbox filtering (left panel),
which the HTTP scraper cannot do. Used for:
  - EV_PV, EV_2W, EV_3W (fuel checkboxes + vehicle class checkboxes)
  - PV_CNG, PV_HYBRID (fuel checkboxes + vehicle class checkboxes)

Key discoveries from testing:
  - PrimeFaces dropdowns MUST be set via panel click (not JS), to fire AJAX
    and establish server-side state.
  - Checkbox filtering ONLY works with j_idt75 or j_idt84 (filter panel
    Refresh buttons). j_idt70 (main Refresh) ignores checkboxes entirely.
  - The table uses PrimeFaces DataTable with pagination (50 rows/page).
  - Month columns: idx 2=first month (JAN for CY, APR for FY).
"""
import time
import logging
import re
import html as _html
from datetime import datetime

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException,
        StaleElementReferenceException, ElementClickInterceptedException,
    )
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

from config.settings import VAHAN_URL, VAHAN_SCRAPE_CONFIGS
from config.oem_normalization import normalize_oem
from database.schema import get_connection

logger = logging.getLogger(__name__)

MONTH_LABELS = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
]


class VahanSeleniumScraper:
    """Selenium-based scraper for Vahan subsegment data requiring checkbox filters.

    Usage:
        scraper = VahanSeleniumScraper(headless=True)
        try:
            rows = scraper.scrape_subsegment("EV_PV", fy_start=2025)
            # rows stored in national_oem_subsegment table
        finally:
            scraper.close()
    """

    def __init__(self, headless=True):
        if not HAS_SELENIUM:
            raise ImportError(
                "Selenium is required. Install with: pip install selenium webdriver-manager"
            )
        self.driver = None
        self._init_driver(headless)
        self._page_loaded = False

    def _init_driver(self, headless):
        """Initialize Chrome WebDriver with appropriate options."""
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        )

        try:
            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
        except Exception:
            self.driver = webdriver.Chrome(options=options)

        self.wait = WebDriverWait(self.driver, 30)
        self.short_wait = WebDriverWait(self.driver, 10)

    def close(self):
        """Close the browser."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # -- Public API ----------------------------------------------------------

    def scrape_subsegment(self, subsegment_code, fy_start, month_num=None):
        """Scrape a subsegment for a financial year (Apr fy_start to Mar fy_start+1).

        Optimized: extracts ALL available months from each calendar year view
        in a single page load, instead of one page load per month.

        Args:
            subsegment_code: e.g. "EV_PV", "PV_CNG", "PV_HYBRID", "EV_2W", etc.
            fy_start: FY start year (e.g. 2025 for FY26 = Apr 2025 - Mar 2026)
            month_num: Optional specific month (1-12). If None, scrapes full year.

        Returns:
            Number of rows stored in DB.
        """
        config = VAHAN_SCRAPE_CONFIGS.get(subsegment_code)
        if not config:
            raise ValueError(
                f"No scrape config for \'{subsegment_code}\'. "
                f"Available: {list(VAHAN_SCRAPE_CONFIGS.keys())}"
            )

        if month_num:
            # Single month: just one CY scrape
            cy = fy_start if month_num >= 4 else fy_start + 1
            return self._scrape_cy_and_store(
                subsegment_code, config, cy, [month_num]
            )

        # Full FY: group months by calendar year for efficiency
        # FY26 (fy_start=2025): Apr-Dec 2025 + Jan-Mar 2026
        total_rows = 0

        # Calendar year 1: Apr-Dec of fy_start
        months_cy1 = list(range(4, 13))  # [4, 5, ..., 12]
        try:
            rows = self._scrape_cy_and_store(
                subsegment_code, config, fy_start, months_cy1
            )
            total_rows += rows
        except Exception as e:
            logger.error(f"  {subsegment_code} CY{fy_start}: FAILED - {e}")
            self._page_loaded = False

        # Calendar year 2: Jan-Mar of fy_start+1
        months_cy2 = list(range(1, 4))  # [1, 2, 3]
        try:
            rows = self._scrape_cy_and_store(
                subsegment_code, config, fy_start + 1, months_cy2
            )
            total_rows += rows
        except Exception as e:
            logger.error(f"  {subsegment_code} CY{fy_start + 1}: FAILED - {e}")
            self._page_loaded = False

        return total_rows

    def test_connection(self):
        """Test if the Vahan portal is accessible."""
        try:
            self.driver.get(VAHAN_URL)
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select"))
            )
            self._page_loaded = True
            return True, "Portal accessible"
        except Exception as e:
            return False, f"Portal not accessible: {e}"

    # -- Internal: page setup ------------------------------------------------

    def _scrape_cy_and_store(self, subsegment_code, config, year, months):
        """Scrape one calendar year view and extract multiple months at once.

        This is much faster than scraping each month individually because
        the table shows ALL months (JAN-DEC) in a single view.
        """
        logger.info(
            f"Scraping {subsegment_code} CY{year} "
            f"months={[MONTH_LABELS[m-1] for m in months]}"
        )

        # Load page and set up filters
        self._setup_filtered_view(config, year)

        # Extract data for each target month from the loaded table
        total_rows = 0
        for month in months:
            records = self._extract_month_data(month)
            if records:
                rows = self._store_records(subsegment_code, records, year, month)
                total_rows += rows
                logger.info(
                    f"  {subsegment_code} {year}-{month:02d}: {rows} OEMs stored"
                )
            else:
                logger.info(f"  {subsegment_code} {year}-{month:02d}: no data")

        return total_rows

    def _setup_filtered_view(self, config, year):
        """Set up the portal with dropdowns + checkboxes for a calendar year."""
        if not self._page_loaded:
            self._load_page()

        # Set dropdowns via PrimeFaces panel clicks (fires AJAX to server)
        self._pf_dropdown("yaxisVar", config.get("y_axis", "Maker"))
        self._pf_dropdown("xaxisVar", config.get("x_axis", "Month Wise"))
        self._pf_dropdown("selectedYearType", "Calendar Year")
        self._pf_dropdown("selectedYear", str(year))

        # Do an initial Refresh (j_idt70) to establish server state
        logger.info("Initial Refresh (j_idt70) to establish server state...")
        self.driver.execute_script("document.getElementById('j_idt70').click();")
        time.sleep(8)
        self._wait_for_unblock()

        # Open the left filter panel
        self._open_filter_panel()
        time.sleep(1)

        # Uncheck ALL checkboxes first (clean slate)
        self._uncheck_all("VhClass")
        self._uncheck_all("fuel")
        time.sleep(0.5)

        # Check the required checkboxes
        if "vehicle_class" in config:
            self._check_boxes("VhClass", config["vehicle_class"])
        if "fuel" in config:
            self._check_boxes("fuel", config["fuel"])
        time.sleep(0.5)

        # Click j_idt75 (filter panel Refresh) — this is the ONLY button
        # that processes checkbox filters!
        logger.info("Filter Refresh (j_idt75) with checkboxes...")
        self.driver.execute_script("document.getElementById('j_idt75').click();")
        time.sleep(8)
        self._wait_for_unblock()

        # Wait for table to load
        self._wait_for_table()

    def _load_page(self):
        """Load the Vahan portal page."""
        logger.info("Loading Vahan portal...")
        self.driver.get(VAHAN_URL)
        time.sleep(3)
        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "select[id*='yaxisVar']")
            )
        )
        self._page_loaded = True
        logger.info("Portal loaded")

    # -- Internal: PrimeFaces dropdown interaction ---------------------------

    def _pf_dropdown(self, widget_id, option_text, wait_secs=3):
        """Select a PrimeFaces dropdown value by clicking the panel overlay.

        This fires the AJAX behavior that updates server-side state — critical
        for proper filtering. Direct JS value setting does NOT work.
        """
        self._wait_for_unblock()

        # Open the dropdown panel
        self.driver.execute_script(f"""
            var trigger = document.querySelector('#{widget_id} .ui-selectonemenu-trigger');
            if (trigger) trigger.click();
        """)
        time.sleep(1)

        # Click the matching option
        result = self.driver.execute_script(f"""
            var panel = document.getElementById('{widget_id}_panel');
            if (!panel) return 'panel not found';
            var items = panel.querySelectorAll('li.ui-selectonemenu-item');
            for (var i = 0; i < items.length; i++) {{
                if (items[i].textContent.trim().indexOf('{option_text}') !== -1) {{
                    items[i].click();
                    return 'OK';
                }}
            }}
            return 'NOT FOUND';
        """)

        time.sleep(wait_secs)
        self._wait_for_unblock()

        val = self.driver.execute_script(
            f"return document.getElementById('{widget_id}_input').value;"
        )
        if result == 'OK':
            logger.info(f"Set {widget_id}={val}")
        else:
            logger.warning(f"Failed to set {widget_id}='{option_text}': {result}")

    # -- Internal: filter panel and checkboxes -------------------------------

    def _open_filter_panel(self):
        """Open the left filter panel (Layout west pane) if closed."""
        self.driver.execute_script(
            "var l=PF('widget_j_idt74'); if(l) l.toggle('west');"
        )
        time.sleep(2)
        logger.info("Opened filter panel")

    def _wait_for_unblock(self):
        """Wait for BlockUI overlay to disappear."""
        time.sleep(0.5)
        for _ in range(30):
            blocked = self.driver.execute_script("""
                var blockers = document.querySelectorAll('.ui-blockui');
                for (var i = 0; i < blockers.length; i++) {
                    if (blockers[i].offsetParent !== null
                        && blockers[i].style.display !== 'none') return true;
                }
                return false;
            """)
            if not blocked:
                break
            time.sleep(0.5)
        time.sleep(0.5)

    def _uncheck_all(self, panel_id):
        """Uncheck all checkboxes in a panel to start clean."""
        count = self.driver.execute_script(f"""
            var checked = document.querySelectorAll('#{panel_id} input:checked');
            var count = 0;
            checked.forEach(function(inp) {{
                var box = inp.parentElement.querySelector('.ui-chkbox-box')
                       || inp.parentElement.nextElementSibling;
                if (box) {{ box.click(); count++; }}
            }});
            return count;
        """)
        if count:
            logger.info(f"Unchecked {count} boxes in {panel_id}")
            time.sleep(0.5)

    def _check_boxes(self, panel_id, labels):
        """Check specific checkboxes in a panel by their label text.

        Uses JS click on the PrimeFaces checkbox div to handle any overlays.
        """
        all_labels = self.driver.execute_script(f"""
            var r = {{}};
            document.querySelectorAll('#{panel_id} label').forEach(function(lbl) {{
                var fid = lbl.getAttribute('for');
                if (fid) r[lbl.textContent.trim()] = fid;
            }});
            return r;
        """)

        checked = 0
        for lbl_text in labels:
            for_id = all_labels.get(lbl_text)
            if for_id:
                is_checked = self.driver.execute_script(
                    f"return document.getElementById('{for_id}').checked;"
                )
                if not is_checked:
                    self.driver.execute_script(f"""
                        var inp = document.getElementById('{for_id}');
                        var box = inp.parentElement.querySelector('.ui-chkbox-box')
                               || inp.parentElement.nextElementSibling;
                        if (box) box.click(); else inp.click();
                    """)
                    time.sleep(0.3)
                checked += 1
            else:
                logger.warning(
                    f"Checkbox label '{lbl_text}' not found in {panel_id}"
                )

        logger.info(f"Checked {checked}/{len(labels)} in {panel_id}: {labels}")

    # -- Internal: table loading and data extraction -------------------------

    def _wait_for_table(self):
        """Wait for the data table to load after clicking Refresh."""
        try:
            self._wait_for_unblock()
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#groupingTable tbody tr")
                )
            )
            time.sleep(2)
            logger.info("Data table loaded")
        except TimeoutException:
            logger.warning("Table load timeout - may have no data")

    def _extract_month_data(self, target_month):
        """Extract OEM volumes for a specific month from the loaded table.

        Table columns: S No | Maker | month1 | month2 | ... | TOTAL
        For Calendar Year: month1=JAN=col_idx 2, FEB=3, MAR=4, etc.

        Handles pagination: the groupingTable may show 50 rows per page.
        We iterate through all pages to capture all OEMs.
        """
        month_label = MONTH_LABELS[target_month - 1]

        # Find the column index for the target month
        col_idx = self._find_month_column(month_label)
        if col_idx is None:
            logger.warning(f"Month column '{month_label}' not found")
            return []

        logger.info(f"Extracting '{month_label}' from column index {col_idx}")

        all_records = []
        page_num = 1

        while True:
            # Extract current page data
            page_records = self._extract_page_data(col_idx)
            all_records.extend(page_records)
            logger.info(f"  Page {page_num}: {len(page_records)} records")

            # Check if there's a next page button
            has_next = self._goto_next_page()
            if not has_next:
                break
            page_num += 1

            # Safety: max 50 pages (EV_3W has 400+ small OEMs)
            if page_num > 50:
                logger.warning("Hit 50-page safety limit, stopping pagination")
                break

        logger.info(f"Extracted {len(all_records)} total records for {month_label} ({page_num} pages)")
        return all_records

    def _goto_next_page(self):
        """Click the next page button in the PrimeFaces DataTable paginator.

        Returns True if successfully navigated to next page, False if on last page.
        """
        try:
            # Check if paginator exists and has a next button that's not disabled
            result = self.driver.execute_script("""
                // PrimeFaces DataTable paginator: look for .ui-paginator-next
                var nextBtns = document.querySelectorAll(
                    '#groupingTable_paginator_bottom .ui-paginator-next, ' +
                    '.ui-paginator-next'
                );
                for (var i = 0; i < nextBtns.length; i++) {
                    var btn = nextBtns[i];
                    // Check if it's not disabled
                    if (!btn.classList.contains('ui-state-disabled')) {
                        btn.click();
                        return 'CLICKED';
                    }
                }
                // No paginator or last page
                return 'NO_NEXT';
            """)

            if result == 'CLICKED':
                time.sleep(3)  # Wait for table to reload
                self._wait_for_unblock()
                return True
            return False

        except Exception as e:
            logger.debug(f"Pagination check: {e}")
            return False

    def _find_month_column(self, month_label):
        """Find the column index for a given month label.

        The table has a multi-row header:
          Row 0: S No | Maker | Month Wise (colspan) | TOTAL
          Row 1:                JAN | FEB | MAR | ...
        In the <td> data rows: col 0=S No, col 1=Maker, col 2=JAN, etc.
        """
        headers = self.driver.execute_script("""
            var thead = document.querySelector('#groupingTable thead');
            if (!thead) return [];
            var rows = thead.querySelectorAll('tr');
            // Find the row with month labels (last row typically)
            for (var r = rows.length - 1; r >= 0; r--) {
                var cells = rows[r].querySelectorAll('th');
                for (var c = 0; c < cells.length; c++) {
                    if (cells[c].textContent.trim().toUpperCase() === 'JAN' ||
                        cells[c].textContent.trim().toUpperCase() === 'APR') {
                        // This is the month row — return all header texts
                        var result = [];
                        for (var i = 0; i < cells.length; i++) {
                            result.push(cells[i].textContent.trim().toUpperCase());
                        }
                        return result;
                    }
                }
            }
            return [];
        """)

        target = month_label.upper()
        for i, h in enumerate(headers):
            if h == target:
                # The month headers start at position 0 in their row,
                # but in <td> data rows, months start at column 2
                # (after S No and Maker). So add 2 to the position.
                return i + 2  # offset for S No + Maker columns

        logger.warning(f"Month headers found: {headers}, looking for '{target}'")
        return None

    def _extract_page_data(self, month_col_idx):
        """Extract OEM + volume data from the current table page."""
        records = []
        try:
            tbody = self.driver.find_element(
                By.CSS_SELECTOR, "#groupingTable tbody"
            )
            rows = tbody.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < month_col_idx + 1:
                    continue

                oem_raw = _html.unescape(cells[1].text.strip())
                if not oem_raw or oem_raw.upper() in ("TOTAL", "GRAND TOTAL", ""):
                    continue

                try:
                    vol_text = cells[month_col_idx].text.strip().replace(",", "")
                    volume = int(vol_text) if vol_text else 0
                except (ValueError, IndexError):
                    volume = 0

                if volume > 0:
                    records.append({"oem_raw": oem_raw, "volume": volume})

        except Exception as e:
            logger.error(f"Error extracting page data: {e}")

        return records

    # -- Internal: data storage ----------------------------------------------

    def _store_records(self, subsegment_code, records, year, month):
        """Store scraped records into national_oem_subsegment table.

        Multiple raw OEM names may normalize to the same name (e.g.,
        'MAHINDRA ELECTRIC AUTOMOBILE LTD' and 'MAHINDRA & MAHINDRA LIMITED'
        both → 'Mahindra'). We aggregate volumes by normalized name first
        to avoid the second insert overwriting the first.
        """
        # Aggregate by normalized name
        aggregated = {}
        for rec in records:
            oem = normalize_oem(rec["oem_raw"], subsegment_code)
            if oem is None:
                continue
            aggregated[oem] = aggregated.get(oem, 0) + rec["volume"]

        # Delete existing data for this slice, then insert fresh
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM national_oem_subsegment
            WHERE subsegment_code=? AND year=? AND month=?
        """, (subsegment_code, year, month))

        now = datetime.now().isoformat()
        for oem, volume in aggregated.items():
            cursor.execute("""
                INSERT INTO national_oem_subsegment
                    (oem_name, subsegment_code, year, month, volume, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (oem, subsegment_code, year, month, volume, now))

        conn.commit()
        conn.close()
        logger.info(
            f"Stored {len(aggregated)} OEMs ({sum(aggregated.values()):,.0f} total volume)"
        )
        return len(aggregated)
