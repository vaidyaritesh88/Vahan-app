"""Selenium-based scraper for Vahan portal state-level data."""
import time
import logging
from datetime import datetime

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False

from config.settings import VAHAN_URL, VAHAN_SCRAPE_CONFIGS
from config.oem_normalization import normalize_oem
from database.schema import get_connection

logger = logging.getLogger(__name__)

MONTH_MAP = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


class VahanScraper:
    """Scrapes the Vahan portal for state-level vehicle registration data."""

    def __init__(self, headless=True):
        if not HAS_SELENIUM:
            raise ImportError(
                "Selenium is required for scraping. Install with: pip install selenium webdriver-manager"
            )

        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
            self.driver.quit()

    def scrape_and_store(self, category_code, state_name, year):
        """Scrape one category for one state for one year, store results in DB."""
        config = VAHAN_SCRAPE_CONFIGS.get(category_code)
        if not config:
            raise ValueError(f"Unknown category: {category_code}")

        conn = get_connection()
        cursor = conn.cursor()

        # Log the scrape attempt
        cursor.execute(
            "INSERT INTO scrape_log (category_code, state, year, status, started_at) VALUES (?, ?, ?, 'running', ?)",
            (category_code, state_name, year, datetime.now().isoformat()),
        )
        log_id = cursor.lastrowid
        conn.commit()

        try:
            data = self._scrape_data(config, state_name, year)
            rows = self._store_data(conn, data, category_code, state_name, year)

            cursor.execute(
                "UPDATE scrape_log SET status='success', completed_at=?, rows_inserted=? WHERE id=?",
                (datetime.now().isoformat(), rows, log_id),
            )
            conn.commit()
            logger.info(f"Scraped {category_code}/{state_name}/{year}: {rows} rows")
            return rows

        except Exception as e:
            cursor.execute(
                "UPDATE scrape_log SET status='failed', completed_at=?, error_message=? WHERE id=?",
                (datetime.now().isoformat(), str(e), log_id),
            )
            conn.commit()
            logger.error(f"Scrape failed {category_code}/{state_name}/{year}: {e}")
            raise
        finally:
            conn.close()

    def _scrape_data(self, config, state_name, year):
        """Navigate to Vahan portal, set filters, and extract data."""
        self.driver.get(VAHAN_URL)
        time.sleep(5)  # Wait for PrimeFaces to initialize

        # Set state
        self._select_dropdown_by_visible_text("selectedState", state_name)
        time.sleep(3)

        # Set year
        self._select_dropdown_by_visible_text("selectedYear", str(year))
        time.sleep(2)

        # Set Y-axis to Maker
        self._select_dropdown_by_visible_text("yaxisVar", config.get("y_axis", "Maker"))
        time.sleep(1)

        # Set X-axis to Month Wise
        self._select_dropdown_by_visible_text("xaxisVar", config.get("x_axis", "Month Wise"))
        time.sleep(1)

        # Set vehicle class/category/fuel checkboxes
        if "vehicle_class" in config:
            self._select_checkboxes("vchType", config["vehicle_class"])
            time.sleep(2)
        if "vehicle_category" in config:
            self._select_checkboxes("vhCatgry", config["vehicle_category"])
            time.sleep(2)
        if "fuel" in config:
            self._select_checkboxes("fuel", config["fuel"])
            time.sleep(2)

        # Click refresh button
        self._click_refresh()
        time.sleep(8)  # Wait for data table to render

        # Extract data from the table
        return self._extract_table_data()

    def _select_dropdown_by_visible_text(self, partial_id, text):
        """Select a dropdown option by visible text."""
        try:
            select_el = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f"select[id*='{partial_id}']"))
            )
            Select(select_el).select_by_visible_text(text)
        except Exception:
            # Try finding by label text
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            for sel in selects:
                try:
                    sel_id = sel.get_attribute("id") or ""
                    if partial_id.lower() in sel_id.lower():
                        Select(sel).select_by_visible_text(text)
                        return
                except Exception:
                    continue
            logger.warning(f"Could not find dropdown: {partial_id}")

    def _select_checkboxes(self, panel_id, labels):
        """Select checkboxes in a multi-select panel."""
        try:
            panel = self.driver.find_element(By.CSS_SELECTOR, f"div[id*='{panel_id}']")
            checkboxes = panel.find_elements(By.CSS_SELECTOR, "div.ui-chkbox")
            for chk in checkboxes:
                label = chk.find_element(By.XPATH, "./following-sibling::label").text.strip()
                if label in labels:
                    chk_box = chk.find_element(By.CSS_SELECTOR, "div.ui-chkbox-box")
                    if "ui-state-active" not in chk_box.get_attribute("class"):
                        chk_box.click()
                        time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Could not select checkboxes for {panel_id}: {e}")

    def _click_refresh(self):
        """Click the refresh/submit button."""
        try:
            btn = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='refresh'], button[id*='submit']"))
            )
            btn.click()
        except Exception:
            # Try generic approach
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.ui-button")
            for b in buttons:
                txt = b.text.strip().lower()
                if "refresh" in txt or "submit" in txt or "go" in txt:
                    b.click()
                    return
            logger.warning("Could not find refresh button")

    def _extract_table_data(self):
        """Extract maker x month data from the PrimeFaces data table."""
        results = []
        try:
            table = self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".ui-datatable-tablewrapper table, table.ui-datatable"))
            )

            # Get headers (months)
            headers = []
            thead = table.find_element(By.TAG_NAME, "thead")
            header_cells = thead.find_elements(By.TAG_NAME, "th")
            for cell in header_cells:
                headers.append(cell.text.strip())

            # Get data rows
            tbody = table.find_element(By.TAG_NAME, "tbody")
            rows = tbody.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue

                oem_name = cells[0].text.strip()
                if not oem_name or oem_name.upper() in ("TOTAL", "GRAND TOTAL"):
                    continue

                for i, cell in enumerate(cells[1:], 1):
                    if i < len(headers):
                        try:
                            volume = int(cell.text.strip().replace(",", ""))
                            if volume > 0:
                                results.append({
                                    "oem_raw": oem_name,
                                    "month_label": headers[i],
                                    "volume": volume,
                                })
                        except (ValueError, IndexError):
                            continue

        except TimeoutException:
            logger.warning("Data table not found - page may not have loaded")
        except Exception as e:
            logger.error(f"Table extraction error: {e}")

        return results

    def _store_data(self, conn, data, category_code, state_name, year):
        """Store scraped data into state_monthly table."""
        cursor = conn.cursor()
        rows_inserted = 0

        for record in data:
            oem = normalize_oem(record["oem_raw"], category_code)
            if oem is None:
                continue

            # Parse month from label (e.g., "January", "Feb", "01", etc.)
            month_num = self._parse_month(record["month_label"])
            if month_num is None:
                continue

            cursor.execute("""
                INSERT INTO state_monthly (category_code, oem_name, state, year, month, volume)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(category_code, oem_name, state, year, month)
                DO UPDATE SET volume=excluded.volume, updated_at=CURRENT_TIMESTAMP
            """, (category_code, oem, state_name, year, month_num, record["volume"]))
            rows_inserted += 1

        conn.commit()
        return rows_inserted

    def _parse_month(self, label):
        """Parse a month label to a month number."""
        if not label:
            return None
        label = label.strip()

        # Try full month name
        for num, name in MONTH_MAP.items():
            if label.lower() == name.lower() or label.lower() == name[:3].lower():
                return num

        # Try numeric
        try:
            num = int(label)
            if 1 <= num <= 12:
                return num
        except ValueError:
            pass

        return None
