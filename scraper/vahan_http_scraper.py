"""HTTP-based scraper for Vahan portal — no Chrome/Selenium dependency.

Uses requests + PrimeFaces AJAX to fetch state-level OEM registration data.

NOTE: The Vahan portal (a Government of India website) often blocks cloud/
datacenter IPs. This scraper works best when run from a local machine or
residential IP. On Streamlit Cloud, the connection will likely be rejected.
"""
import re
import ssl
import time
import logging
import platform
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter

from config.settings import VAHAN_URL, VAHAN_SCRAPE_CONFIGS
from config.oem_normalization import normalize_oem
from database.schema import get_connection

logger = logging.getLogger(__name__)


class _TLSAdapter(HTTPAdapter):
    """HTTPS adapter that uses a permissive TLS context.

    Some government sites use older TLS configurations or ciphers that
    Python's default strict settings reject. This adapter relaxes those.
    """

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT:@SECLEVEL=1")
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

# Vahan portal state code mapping (our state name → portal code)
STATE_CODES = {
    "Andaman and Nicobar Islands": "AN",
    "Andhra Pradesh": "AP",
    "Arunachal Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BR",
    "Chandigarh": "CH",
    "Chhattisgarh": "CG",
    "Dadra and Nagar Haveli and Daman and Diu": "DD",
    "Delhi": "DL",
    "Goa": "GA",
    "Gujarat": "GJ",
    "Haryana": "HR",
    "Himachal Pradesh": "HP",
    "Jammu and Kashmir": "JK",
    "Jharkhand": "JH",
    "Karnataka": "KA",
    "Kerala": "KL",
    "Ladakh": "LA",
    "Lakshadweep": "LD",
    "Madhya Pradesh": "MP",
    "Maharashtra": "MH",
    "Manipur": "MN",
    "Meghalaya": "ML",
    "Mizoram": "MZ",
    "Nagaland": "NL",
    "Odisha": "OD",
    "Puducherry": "PY",
    "Punjab": "PB",
    "Rajasthan": "RJ",
    "Sikkim": "SK",
    "Tamil Nadu": "TN",
    "Telangana": "TS",
    "Tripura": "TR",
    "Uttar Pradesh": "UP",
    "Uttarakhand": "UK",
    "West Bengal": "WB",
}

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


class VahanHttpScraper:
    """Scrapes Vahan portal using direct HTTP requests (no browser needed).

    NOTE: The Vahan portal blocks cloud/datacenter IPs. This scraper is
    designed to run from a local machine. On Streamlit Cloud, the portal
    will reject the connection (ConnectionResetError).
    """

    MAX_RETRIES = 3

    def __init__(self, timeout=30, verify_ssl=True):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        })
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session.verify = verify_ssl
        self.viewstate = None
        self.form_url = VAHAN_URL
        self._page_loaded = False
        self._is_cloud = self._detect_cloud_env()

        # Mount TLS adapter for HTTPS to handle government site TLS quirks
        self.session.mount("https://", _TLSAdapter())

        # Dynamic IDs discovered from page
        self._state_id = None
        self._number_format_id = None

    @staticmethod
    def _detect_cloud_env():
        """Detect if we're running on Streamlit Cloud or similar."""
        # Streamlit Cloud runs on Linux with appuser
        import os
        home = os.environ.get("HOME", "")
        return (
            platform.system() == "Linux"
            and ("/home/appuser" in home or os.environ.get("STREAMLIT_RUNTIME", ""))
        )

    def _load_page(self):
        """Load the initial page to get ViewState and session cookie."""
        if self._page_loaded:
            return

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                r = self.session.get(self.form_url, timeout=self.timeout, verify=self.verify_ssl)
                r.raise_for_status()
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    wait = attempt * 2
                    logger.warning(f"Connection attempt {attempt} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
            except Exception:
                raise

        self.viewstate = self._extract_viewstate(r.text)
        if not self.viewstate:
            raise RuntimeError("Could not extract ViewState from Vahan portal")

        # Discover element IDs (they can change across deployments)
        self._discover_ids(r.text)
        self._page_loaded = True
        logger.info("Vahan portal page loaded, ViewState acquired")

    def _discover_ids(self, html):
        """Discover dynamic PrimeFaces element IDs from page HTML."""
        # State dropdown: the one with state codes (AN, AP, AR, etc.)
        selects = re.findall(r'<select[^>]*id="([^"]*)"[^>]*>(.*?)</select>', html, re.DOTALL)
        for sel_id, content in selects:
            if "'AP'" in content or "Andhra" in content:
                self._state_id = sel_id.replace("_input", "")
                break

        # Number format dropdown: the one with T, L, C, A
        for sel_id, content in selects:
            if "'A'" in content and "Actual" in content:
                self._number_format_id = sel_id.replace("_input", "")
                break

    def _extract_viewstate(self, text):
        """Extract javax.faces.ViewState from HTML or AJAX response."""
        # From HTML form
        m = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', text)
        if m:
            return m.group(1)
        # From AJAX partial response
        m = re.search(r'<update id="javax\.faces\.ViewState"><!\[CDATA\[([^\]]+)\]\]>', text)
        if m:
            return m.group(1)
        return None

    def _ajax_post(self, source, execute, render, extra_params=None):
        """Make a PrimeFaces AJAX POST request."""
        data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": source,
            "javax.faces.partial.execute": execute,
            "javax.faces.partial.render": render,
            "javax.faces.behavior.event": "change",
            "javax.faces.partial.event": "change",
            "masterLayout_formlogin": "masterLayout_formlogin",
            "javax.faces.ViewState": self.viewstate,
        }
        if extra_params:
            data.update(extra_params)

        r = self.session.post(self.form_url, data=data, timeout=self.timeout, headers={
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
        })
        r.raise_for_status()

        # Update ViewState from response
        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs

        return r.text

    def _button_post(self, button_id, extra_params=None):
        """Click a PrimeFaces command button via AJAX."""
        data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": button_id,
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": "@all",
            "masterLayout_formlogin": "masterLayout_formlogin",
            button_id: button_id,
            "javax.faces.ViewState": self.viewstate,
        }
        if extra_params:
            data.update(extra_params)

        r = self.session.post(self.form_url, data=data, timeout=self.timeout * 2, headers={
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
        })
        r.raise_for_status()

        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs

        return r.text

    def _set_filters(self, state_code, year, config):
        """Set all form filters via sequential AJAX calls."""
        # 1. Set number format to Actual
        if self._number_format_id:
            self._ajax_post(
                self._number_format_id,
                self._number_format_id,
                self._number_format_id,
                {f"{self._number_format_id}_input": "A"},
            )
            time.sleep(0.5)

        # 2. Select state — this triggers updates to RTO and other fields
        if self._state_id:
            resp = self._ajax_post(
                self._state_id,
                self._state_id,
                "selectedRto yaxisVar",
                {f"{self._state_id}_input": state_code},
            )
            time.sleep(1)

        # 3. Set Y-axis = Maker
        self._ajax_post(
            "yaxisVar", "yaxisVar", "yaxisVar",
            {"yaxisVar_input": config.get("y_axis", "Maker")},
        )
        time.sleep(0.5)

        # 4. Set X-axis = Month Wise
        self._ajax_post(
            "xaxisVar", "xaxisVar", "xaxisVar",
            {"xaxisVar_input": config.get("x_axis", "Month Wise")},
        )
        time.sleep(0.5)

        # 5. Set year type to Calendar Year
        self._ajax_post(
            "selectedYearType", "selectedYearType", "selectedYearType selectedYear",
            {"selectedYearType_input": "C"},
        )
        time.sleep(0.5)

        # 6. Set year
        self._ajax_post(
            "selectedYear", "selectedYear", "selectedYear",
            {"selectedYear_input": str(year)},
        )
        time.sleep(0.5)

        # 7. Set vehicle class/category/fuel checkbox filters
        self._set_checkbox_filters(config)
        time.sleep(0.5)

    def _set_checkbox_filters(self, config):
        """Set vehicle class, category, and fuel checkbox filters.

        PrimeFaces selectCheckboxMenu sends selected values in a specific format.
        We need to find the panel IDs and set the right checkbox values.
        """
        # Vehicle class filter
        if "vehicle_class" in config:
            self._toggle_checkboxes("selectedVhclType", config["vehicle_class"])

        # Vehicle category filter
        if "vehicle_category" in config:
            self._toggle_checkboxes("selectedVhclCatgry", config["vehicle_category"])

        # Fuel filter
        if "fuel" in config:
            self._toggle_checkboxes("selectedFuel", config["fuel"])

    def _toggle_checkboxes(self, panel_id, labels):
        """Toggle checkbox selections in a PrimeFaces selectCheckboxMenu."""
        # PrimeFaces selectCheckboxMenu submits selected items as panel_id=val1,val2,...
        # We need to find checkbox indices by their labels, then send toggling events

        # For each label, send a toggle AJAX call
        for i, label in enumerate(labels):
            try:
                data = {
                    "javax.faces.partial.ajax": "true",
                    "javax.faces.source": panel_id,
                    "javax.faces.partial.execute": panel_id,
                    "javax.faces.partial.render": panel_id,
                    "javax.faces.behavior.event": "toggleSelect",
                    "javax.faces.partial.event": "toggleSelect",
                    "masterLayout_formlogin": "masterLayout_formlogin",
                    panel_id: panel_id,
                    "javax.faces.ViewState": self.viewstate,
                }
                r = self.session.post(self.form_url, data=data, timeout=self.timeout, headers={
                    "Faces-Request": "partial/ajax",
                    "X-Requested-With": "XMLHttpRequest",
                })
                new_vs = self._extract_viewstate(r.text)
                if new_vs:
                    self.viewstate = new_vs
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"Checkbox toggle for {panel_id}/{label}: {e}")

    def _find_and_click_refresh(self, state_code, year, config):
        """Submit the form by finding and clicking the refresh button.

        As a fallback, we do a full form POST with all parameters.
        """
        # Build form data with all current selections
        form_data = {
            "masterLayout_formlogin": "masterLayout_formlogin",
            "javax.faces.ViewState": self.viewstate,
            "selectedRto_input": "-1",
            "yaxisVar_input": config.get("y_axis", "Maker"),
            "xaxisVar_input": config.get("x_axis", "Month Wise"),
            "selectedYearType_input": "C",
            "selectedYear_input": str(year),
        }

        if self._number_format_id:
            form_data[f"{self._number_format_id}_input"] = "A"
        if self._state_id:
            form_data[f"{self._state_id}_input"] = state_code

        # Try common refresh button IDs
        for btn_id in ["j_idt61", "j_idt59", "j_idt57", "j_idt63", "j_idt65"]:
            try:
                resp = self._button_post(btn_id, form_data)
                if self._has_data_table(resp):
                    return resp
            except Exception:
                continue

        # Fallback: try to find the button from page HTML
        # Reload page with current session to find button IDs
        r = self.session.get(self.form_url, timeout=self.timeout)
        buttons = re.findall(r'<button[^>]*id="([^"]*)"[^>]*type="submit"', r.text)
        for btn_id in buttons:
            if "idt" in btn_id.lower():
                try:
                    form_data["javax.faces.ViewState"] = self._extract_viewstate(r.text) or self.viewstate
                    resp = self._button_post(btn_id, form_data)
                    if self._has_data_table(resp):
                        return resp
                except Exception:
                    continue

        raise RuntimeError("Could not trigger data refresh — no valid button found")

    def _has_data_table(self, html):
        """Check if the response contains a data table with results."""
        return "<tbody" in html and ("<td" in html)

    def _detect_datatable_pagination(self, html):
        """Detect PrimeFaces DataTable ID and pagination info from response HTML.

        Returns (datatable_id, rows_per_page, total_rows) or (None, 0, 0) if
        no pagination is found.
        """
        # Look for DataTable widget ID in CDATA blocks or raw HTML
        search_text = html
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        if cdata_blocks:
            search_text = " ".join(cdata_blocks)

        # Find the datatable container div: <div id="XYZ" class="...ui-datatable...">
        dt_match = re.search(
            r'<div[^>]*id="([^"]*)"[^>]*class="[^"]*ui-datatable[^"]*"',
            search_text,
        )
        if not dt_match:
            return None, 0, 0

        dt_id = dt_match.group(1)

        # Look for paginator info.
        # PrimeFaces paginator renders something like:
        # <span class="ui-paginator-current">(1 of 10)</span>
        # or shows page links: <a class="ui-paginator-page ...">1</a> ... <a>10</a>
        # or has rows-per-page dropdown with total row count in the widget config.

        # Method 1: Look for "({current} of {total})" pattern in paginator
        page_match = re.search(
            r'ui-paginator-current[^>]*>\s*\(?\s*(\d+)\s+of\s+(\d+)\s*\)?',
            search_text,
        )
        if page_match:
            current_page = int(page_match.group(1))
            total_pages = int(page_match.group(2))
            # Try to find rows per page from the paginator dropdown or default
            rpp_match = re.search(
                r'<option[^>]*selected[^>]*>(\d+)</option>',
                search_text,
            )
            rows_per_page = int(rpp_match.group(1)) if rpp_match else 25
            total_rows = total_pages * rows_per_page
            logger.info(f"DataTable '{dt_id}': page {current_page}/{total_pages}, "
                        f"{rows_per_page} rows/page, ~{total_rows} total rows")
            return dt_id, rows_per_page, total_rows

        # Method 2: Count page link buttons to determine total pages
        page_links = re.findall(
            r'<(?:a|span)[^>]*class="[^"]*ui-paginator-page[^"]*"[^>]*>(\d+)',
            search_text,
        )
        if page_links:
            total_pages = max(int(p) for p in page_links)
            # Count rows in the current tbody to determine rows per page
            tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', search_text, re.DOTALL)
            if tbody_match:
                current_rows = len(re.findall(r'<tr[^>]*>', tbody_match.group(1)))
                rows_per_page = current_rows if current_rows > 0 else 25
            else:
                rows_per_page = 25
            total_rows = total_pages * rows_per_page
            logger.info(f"DataTable '{dt_id}': {total_pages} pages, "
                        f"{rows_per_page} rows/page, ~{total_rows} total rows")
            return dt_id, rows_per_page, total_rows

        # Method 3: Check if paginator div exists at all
        has_paginator = bool(re.search(
            r'ui-paginator', search_text,
        ))
        if has_paginator:
            # Paginator exists but we couldn't parse details — assume multi-page
            tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', search_text, re.DOTALL)
            if tbody_match:
                current_rows = len(re.findall(r'<tr[^>]*>', tbody_match.group(1)))
                rows_per_page = current_rows if current_rows > 0 else 25
            else:
                rows_per_page = 25
            # We don't know total — will paginate until empty
            logger.info(f"DataTable '{dt_id}': paginator detected, {rows_per_page} rows/page, total unknown")
            return dt_id, rows_per_page, -1  # -1 = unknown total

        return None, 0, 0

    def _fetch_datatable_page(self, dt_id, first, rows_per_page):
        """Fetch a specific page of a PrimeFaces DataTable via AJAX pagination.

        Args:
            dt_id: DataTable widget ID (e.g., 'groupingTable')
            first: 0-based row offset (0 for page 1, rows_per_page for page 2, etc.)
            rows_per_page: Number of rows per page

        Returns:
            Response HTML text.
        """
        data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": dt_id,
            "javax.faces.partial.execute": dt_id,
            "javax.faces.partial.render": dt_id,
            "javax.faces.behavior.event": "page",
            "javax.faces.partial.event": "page",
            f"{dt_id}_pagination": "true",
            f"{dt_id}_first": str(first),
            f"{dt_id}_rows": str(rows_per_page),
            "masterLayout_formlogin": "masterLayout_formlogin",
            "javax.faces.ViewState": self.viewstate,
        }

        r = self.session.post(self.form_url, data=data, timeout=self.timeout, headers={
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
        })
        r.raise_for_status()

        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs

        return r.text

    def _extract_table(self, html):
        """Parse the Maker x Month data table from AJAX response HTML.

        Returns list of dicts: [{oem_raw, month_label, volume}, ...]
        """
        results = []

        # Extract content from CDATA if it's an AJAX response
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        search_text = " ".join(cdata_blocks) if cdata_blocks else html

        # Find the data table
        table_match = re.search(
            r'<table[^>]*class="[^"]*ui-datatable[^"]*"[^>]*>(.*?)</table>',
            search_text, re.DOTALL
        )
        if not table_match:
            # Try finding any table with thead/tbody
            table_match = re.search(r'<table[^>]*>(.*?<thead.*?</tbody>.*?)</table>', search_text, re.DOTALL)

        if not table_match:
            logger.warning("No data table found in response")
            return results

        table_html = table_match.group(1)

        # Extract headers
        headers = []
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', table_html, re.DOTALL)
        if thead_match:
            headers = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', thead_match.group(1))
            headers = [h.strip() for h in headers if h.strip()]

        # Detect serial number column (S.No) — Vahan portal tables often
        # have a serial number as the first column before the Maker column.
        sno_offset = 0
        if headers:
            h0 = headers[0].lower().strip().replace('.', '').replace(' ', '')
            if h0 in ('sno', 'srno', 'slno', '#', 'serial', 'serialno', 'srn', 'no'):
                sno_offset = 1
                logger.debug(f"Detected serial number column '{headers[0]}', skipping it")

        # Extract data rows
        tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', table_html, re.DOTALL)
        if not tbody_match:
            return results

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_match.group(1), re.DOTALL)
        for row_html in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
            if not cells:
                continue

            # Determine OEM name cell index.
            # If S.No column detected via header, use offset.
            # Fallback: if cells[0] is purely numeric (serial number), skip it.
            oem_idx = sno_offset
            if oem_idx == 0:
                cell0_text = re.sub(r'<[^>]+>', '', cells[0]).strip()
                if cell0_text.isdigit() and len(cells) > 2:
                    oem_idx = 1

            if oem_idx >= len(cells):
                continue

            oem_raw = re.sub(r'<[^>]+>', '', cells[oem_idx]).strip()
            if not oem_raw or oem_raw.upper() in ("TOTAL", "GRAND TOTAL"):
                continue

            # Volume cells start after OEM name cell
            vol_start = oem_idx + 1
            # Month headers start at same offset as volume cells
            month_header_start = oem_idx + 1

            for i, cell in enumerate(cells[vol_start:]):
                header_idx = month_header_start + i
                if header_idx < len(headers):
                    vol_text = re.sub(r'<[^>]+>', '', cell).strip().replace(",", "")
                    try:
                        volume = int(float(vol_text))
                        if volume > 0:
                            results.append({
                                "oem_raw": oem_raw,
                                "month_label": headers[header_idx],
                                "volume": volume,
                            })
                    except (ValueError, IndexError):
                        continue

        return results

    def scrape_state_year(self, category_code, state_name, year):
        """Scrape one category for one state for one year.

        Handles paginated DataTable results by fetching all pages.

        Returns list of parsed records (not yet stored).
        """
        config = VAHAN_SCRAPE_CONFIGS.get(category_code)
        if not config:
            raise ValueError(f"Unknown category: {category_code}")

        state_code = STATE_CODES.get(state_name)
        if not state_code:
            raise ValueError(f"Unknown state: {state_name}. Available: {list(STATE_CODES.keys())}")

        # Ensure page is loaded
        self._load_page()

        # Reset session for each scrape (avoids stale state)
        self._page_loaded = False
        self._load_page()

        # Set all filters
        self._set_filters(state_code, year, config)

        # Click refresh and get first page of data
        response_html = self._find_and_click_refresh(state_code, year, config)

        # Parse the first page
        all_records = self._extract_table(response_html)

        # Check for pagination — the Vahan DataTable may have multiple pages
        dt_id, rows_per_page, total_rows = self._detect_datatable_pagination(response_html)

        if dt_id and rows_per_page > 0:
            # Fetch remaining pages
            max_pages = 50  # Safety limit
            page_num = 2
            first = rows_per_page  # Start of second page

            while page_num <= max_pages:
                # If we know the total, check if we've fetched enough
                if total_rows > 0 and first >= total_rows:
                    break

                try:
                    time.sleep(0.5)  # Brief delay between page requests
                    page_html = self._fetch_datatable_page(dt_id, first, rows_per_page)
                    page_records = self._extract_table(page_html)

                    if not page_records:
                        # No more data — we've reached the end
                        break

                    all_records.extend(page_records)
                    logger.info(f"Page {page_num}: {len(page_records)} records "
                                f"(total so far: {len(all_records)})")

                    first += rows_per_page
                    page_num += 1

                except Exception as e:
                    logger.warning(f"Pagination page {page_num} failed: {e}")
                    break

            logger.info(f"Fetched {page_num - 1} pages total for "
                        f"{category_code}/{state_name}/{year}")

        logger.info(f"Parsed {len(all_records)} records for {category_code}/{state_name}/{year}")
        return all_records

    def scrape_and_store(self, category_code, state_name, year):
        """Scrape and store data with logging. Returns number of rows upserted."""
        conn = get_connection()
        cursor = conn.cursor()

        # Log the attempt
        cursor.execute(
            "INSERT INTO scrape_log (category_code, state, year, status, started_at) "
            "VALUES (?, ?, ?, 'running', ?)",
            (category_code, state_name, year, datetime.now().isoformat()),
        )
        log_id = cursor.lastrowid
        conn.commit()

        try:
            records = self.scrape_state_year(category_code, state_name, year)
            rows = self._store_records(conn, records, category_code, state_name, year)

            cursor.execute(
                "UPDATE scrape_log SET status='success', completed_at=?, rows_inserted=? WHERE id=?",
                (datetime.now().isoformat(), rows, log_id),
            )
            conn.commit()
            return rows

        except Exception as e:
            cursor.execute(
                "UPDATE scrape_log SET status='failed', completed_at=?, error_message=? WHERE id=?",
                (datetime.now().isoformat(), str(e)[:500], log_id),
            )
            conn.commit()
            raise
        finally:
            conn.close()

    def _store_records(self, conn, records, category_code, state_name, year):
        """Store parsed records into state_monthly with upsert (no duplication)."""
        cursor = conn.cursor()
        rows = 0

        for rec in records:
            oem = normalize_oem(rec["oem_raw"], category_code)
            if oem is None:
                continue

            month_num = _parse_month(rec["month_label"])
            if month_num is None:
                continue

            cursor.execute("""
                INSERT INTO state_monthly (category_code, oem_name, state, year, month, volume)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(category_code, oem_name, state, year, month)
                DO UPDATE SET volume=excluded.volume, updated_at=CURRENT_TIMESTAMP
            """, (category_code, oem, state_name, year, month_num, rec["volume"]))
            rows += 1

        conn.commit()
        return rows

    def test_connection(self):
        """Test if Vahan portal is reachable. Returns (success, message).

        Automatically retries with SSL verification disabled if the first
        attempt fails due to an SSL error (common in corporate networks).
        Detects cloud environment and gives clear guidance.
        """
        try:
            self._page_loaded = False
            self._load_page()
            return True, "Connected to Vahan portal successfully."
        except requests.exceptions.SSLError as e:
            logger.warning(f"SSL error on first attempt: {e}")
            return self._retry_without_ssl(
                f"SSL certificate error (likely corporate proxy). "
                f"Detail: {str(e)[:150]}"
            )
        except requests.exceptions.ProxyError as e:
            return False, f"Proxy blocked the connection. Detail: {str(e)[:150]}"
        except requests.exceptions.ConnectionError as e:
            inner = str(e)
            # Check if SSL-related
            if "SSL" in inner or "CERTIFICATE" in inner.upper() or "ssl" in inner:
                logger.warning(f"SSL-related ConnectionError: {e}")
                return self._retry_without_ssl(
                    f"SSL/certificate error (likely corporate proxy). "
                    f"Detail: {inner[:150]}"
                )
            # Connection reset / refused — likely IP-based blocking
            if "reset" in inner.lower() or "refused" in inner.lower() or "aborted" in inner.lower():
                msg = (
                    "**Connection rejected by Vahan portal** (connection reset by server).\n\n"
                )
                if self._is_cloud:
                    msg += (
                        "This is expected on **Streamlit Cloud** — the Vahan portal "
                        "(a Government of India website) blocks requests from cloud/datacenter IPs.\n\n"
                        "**The scraper is designed to run from a local machine.** "
                        "To scrape data:\n"
                        "1. Run the app locally: `streamlit run app.py`\n"
                        "2. Use the scraper from your local machine\n"
                        "3. Push the updated database to deploy\n\n"
                        "All other features (dashboards, charts, AI Chat) work fine on Streamlit Cloud."
                    )
                else:
                    msg += (
                        "The Vahan portal may be temporarily down, or your network/firewall "
                        "is blocking the connection. Try again in a few minutes."
                    )
                return False, msg
            return False, f"Could not connect to Vahan portal. Detail: {inner[:200]}"
        except requests.exceptions.Timeout:
            return False, "Connection timed out. The Vahan portal may be slow or unreachable."
        except RuntimeError as e:
            return False, f"Portal responded but page parsing failed: {str(e)}"
        except Exception as e:
            return False, f"Connection failed: {type(e).__name__}: {str(e)[:200]}"

    def _retry_without_ssl(self, original_error_msg):
        """Retry connection with SSL verification disabled.

        Returns (success, message) tuple.
        """
        try:
            logger.info("Retrying connection with SSL verification disabled...")
            self.verify_ssl = False
            self.session.verify = False

            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            self._page_loaded = False
            self._load_page()
            return True, (
                "Connected to Vahan portal successfully (SSL verification disabled). "
                "Your network may use SSL inspection."
            )
        except Exception as retry_err:
            return False, (
                f"Initial error: {original_error_msg}\n\n"
                f"Retry without SSL also failed: {str(retry_err)[:150]}"
            )


def _parse_month(label):
    """Parse a month label (name or number) to month number."""
    if not label:
        return None
    label = label.strip().lower()

    # Try name lookup
    if label in MONTH_MAP:
        return MONTH_MAP[label]

    # Try numeric
    try:
        num = int(label)
        if 1 <= num <= 12:
            return num
    except ValueError:
        pass

    return None


def get_scrape_coverage():
    """Get a summary of what's been successfully scraped.

    Returns dict: {(category_code, state, year): latest_scrape_date}
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT category_code, state, year, MAX(completed_at) as last_scraped, SUM(rows_inserted) as total_rows
        FROM scrape_log
        WHERE status = 'success'
        GROUP BY category_code, state, year
    """)
    coverage = {}
    for row in cursor.fetchall():
        coverage[(row[0], row[1], row[2])] = {
            "last_scraped": row[3],
            "total_rows": row[4],
        }
    conn.close()
    return coverage


def get_pending_scrapes(categories, states, years):
    """Given desired categories/states/years, return only those not yet scraped.

    Returns list of (category_code, state_name, year) tuples that still need scraping.
    """
    coverage = get_scrape_coverage()
    pending = []
    for cat in categories:
        for state in states:
            for year in years:
                if (cat, state, year) not in coverage:
                    pending.append((cat, state, year))
    return pending
