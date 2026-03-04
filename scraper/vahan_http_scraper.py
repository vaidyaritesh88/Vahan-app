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

        # Form fields saved from the last refresh click, for use in pagination.
        # PrimeFaces AJAX serializes the entire form with every request —
        # pagination requests must include these or the server ignores them.
        self._last_form_params = {}

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
        """Extract javax.faces.ViewState from HTML or AJAX response.

        JSF ViewState can appear in multiple formats:
        1. HTML form: <input name="javax.faces.ViewState" value="...">
        2. AJAX update: <update id="j_id1:javax.faces.ViewState:0"><![CDATA[...]]>
           The update ID varies — it may include a naming container prefix
           (e.g., 'j_id1:') and a state index suffix (':0').
        """
        # From HTML form input (initial page load)
        m = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', text)
        if m:
            return m.group(1)
        # Also try value-before-name ordering
        m = re.search(r'value="([^"]+)"[^>]*name="javax\.faces\.ViewState"', text)
        if m:
            return m.group(1)
        # From AJAX partial response — flexible ID matching
        # Matches: "javax.faces.ViewState", "j_id1:javax.faces.ViewState:0", etc.
        m = re.search(
            r'<update\s+id="([^"]*javax\.faces\.ViewState[^"]*)"[^>]*>'
            r'<!\[CDATA\[([^\]]+)\]\]>',
            text,
        )
        if m:
            logger.debug(f"ViewState extracted from AJAX update id=\"{m.group(1)}\" "
                         f"(len={len(m.group(2))})")
            return m.group(2)
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
        old_vs_len = len(self.viewstate) if self.viewstate else 0
        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs
            logger.debug(f"_ajax_post({source}): ViewState updated "
                         f"({old_vs_len} → {len(new_vs)} chars)")
        else:
            logger.debug(f"_ajax_post({source}): no ViewState in response "
                         f"(keeping existing {old_vs_len} chars)")

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

        old_vs_len = len(self.viewstate) if self.viewstate else 0
        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs
            logger.info(f"_button_post({button_id}): ViewState updated "
                        f"({old_vs_len} → {len(new_vs)} chars)")
        else:
            logger.warning(f"_button_post({button_id}): NO ViewState in response! "
                           f"(keeping stale {old_vs_len} chars)")

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
        Also saves form field values for use in subsequent pagination requests.
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
                    self._save_form_params(form_data, resp)
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
                        self._save_form_params(form_data, resp)
                        return resp
                except Exception:
                    continue

        raise RuntimeError("Could not trigger data refresh — no valid button found")

    def _save_form_params(self, form_data, response_html):
        """Save form field values for reuse in pagination AJAX requests.

        PrimeFaces AJAX always serializes the entire form. Pagination requests
        that omit form fields (state, year, filters) will be ignored by the
        server — it returns only a ViewState update with no table data.

        We merge the explicitly-built form_data with any additional fields
        discovered in the response HTML (hidden inputs, scroll state, etc.).
        """
        # Start with our known form fields
        self._last_form_params = {
            k: v for k, v in form_data.items()
            if k != "javax.faces.ViewState"
        }

        # Merge in fields extracted from response HTML (hidden inputs,
        # select values, etc.). These may include additional fields like
        # groupingTable_scrollState that we didn't explicitly set.
        response_fields = self._extract_all_form_fields(response_html)
        for k, v in response_fields.items():
            if k not in self._last_form_params:
                self._last_form_params[k] = v

        logger.info(f"Saved {len(self._last_form_params)} form params for pagination")

    def _has_data_table(self, html):
        """Check if the response contains a data table with results."""
        return "<tbody" in html and ("<td" in html)

    def _extract_all_form_fields(self, html):
        """Extract all form field values from the response HTML.

        When the browser makes a PrimeFaces AJAX request, it serializes ALL
        fields in the enclosing <form> (hidden inputs, selects, text inputs)
        alongside the AJAX-specific parameters. Without these, the server
        does not know what filters are active and returns an empty update.

        Returns a dict of field names → values (excluding ViewState).
        """
        fields = {}

        # Work on CDATA content if it's an AJAX response, else raw HTML
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        search_text = " ".join(cdata_blocks) if cdata_blocks else html

        # Try to isolate the form content
        form_match = re.search(
            r'<form[^>]*id="masterLayout_formlogin"[^>]*>(.*?)</form>',
            search_text, re.DOTALL,
        )
        form_content = form_match.group(1) if form_match else search_text

        # 1. Hidden inputs: <input type="hidden" name="X" value="Y">
        #    Handles both attribute orderings
        for m in re.finditer(
            r'<input[^>]*type="hidden"[^>]*name="([^"]*)"[^>]*value="([^"]*)"',
            form_content,
        ):
            name, value = m.group(1), m.group(2)
            if "javax.faces.ViewState" not in name:
                fields[name] = value

        for m in re.finditer(
            r'<input[^>]*name="([^"]*)"[^>]*type="hidden"[^>]*value="([^"]*)"',
            form_content,
        ):
            name, value = m.group(1), m.group(2)
            if "javax.faces.ViewState" not in name:
                fields.setdefault(name, value)

        for m in re.finditer(
            r'<input[^>]*value="([^"]*)"[^>]*type="hidden"[^>]*name="([^"]*)"',
            form_content,
        ):
            value, name = m.group(1), m.group(2)
            if "javax.faces.ViewState" not in name:
                fields.setdefault(name, value)

        # 2. Select dropdowns: extract the selected option's value
        for m in re.finditer(
            r'<select[^>]*name="([^"]*)"[^>]*>(.*?)</select>',
            form_content, re.DOTALL,
        ):
            name = m.group(1)
            content = m.group(2)
            sel_opt = re.search(
                r'<option[^>]*selected[^>]*value="([^"]*)"', content
            )
            if sel_opt:
                fields[name] = sel_opt.group(1)
            else:
                # Try reversed attribute order
                sel_opt = re.search(
                    r'<option[^>]*value="([^"]*)"[^>]*selected', content
                )
                if sel_opt:
                    fields[name] = sel_opt.group(1)

        # 3. Text inputs
        for m in re.finditer(
            r'<input[^>]*type="text"[^>]*name="([^"]*)"[^>]*value="([^"]*)"',
            form_content,
        ):
            fields.setdefault(m.group(1), m.group(2))

        # 4. Checked checkboxes (PrimeFaces selectCheckboxMenu etc.)
        #    Only picks up checkboxes that have 'checked' in their HTML attributes.
        #    PrimeFaces often manages selection state via JS, so many won't be
        #    detected here — those are preserved in the ViewState instead.
        for m in re.finditer(
            r'<input[^>]*type="checkbox"[^>]*checked[^>]*/?\s*>',
            form_content, re.IGNORECASE,
        ):
            tag = m.group(0)
            nm = re.search(r'name="([^"]*)"', tag)
            vl = re.search(r'value="([^"]*)"', tag)
            if nm and vl:
                name, value = nm.group(1), vl.group(1)
                # Checkbox groups can have multiple values — keep first seen
                fields.setdefault(name, value)

        # 5. Checked radio buttons
        for m in re.finditer(
            r'<input[^>]*type="radio"[^>]*checked[^>]*/?\s*>',
            form_content, re.IGNORECASE,
        ):
            tag = m.group(0)
            nm = re.search(r'name="([^"]*)"', tag)
            vl = re.search(r'value="([^"]*)"', tag)
            if nm and vl:
                fields.setdefault(nm.group(1), vl.group(1))

        logger.info(f"Extracted {len(fields)} form fields for pagination: "
                    f"{list(fields.keys())[:15]}...")
        return fields

    def _extract_table_headers(self, html):
        """Extract column headers from a table in the HTML response.

        Used to save headers from the first page for reuse with pagination
        responses that may not include <thead>.
        """
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        search_text = " ".join(cdata_blocks) if cdata_blocks else html
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', search_text, re.DOTALL)
        if thead_match:
            headers = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', thead_match.group(1))
            return [h.strip() for h in headers if h.strip()]
        return []

    def _find_datatable_id(self, html):
        """Find the PrimeFaces DataTable widget ID from response HTML.

        Searches both raw HTML and CDATA blocks. Uses 8 strategies with
        increasing aggressiveness to find the DataTable ID.

        Returns the DataTable ID string, or None if not found.
        """
        search_text = html
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        if cdata_blocks:
            search_text = " ".join(cdata_blocks)

        # Log what we're searching through for debugging
        logger.info(f"DataTable ID search: HTML length={len(html)}, "
                    f"CDATA blocks={len(cdata_blocks)}, "
                    f"search_text length={len(search_text)}")

        # Strategy 1: <div id="X" class="...ui-datatable...">
        m = re.search(r'<div[^>]*id="([^"]*)"[^>]*class="[^"]*ui-datatable[^"]*"', search_text)
        if m:
            logger.info(f"Found DataTable ID (S1 div id+class): {m.group(1)}")
            return m.group(1)

        # Strategy 2: <div class="...ui-datatable..." id="X">  (reversed attr order)
        m = re.search(r'<div[^>]*class="[^"]*ui-datatable[^"]*"[^>]*id="([^"]*)"', search_text)
        if m:
            logger.info(f"Found DataTable ID (S2 div class+id): {m.group(1)}")
            return m.group(1)

        # Strategy 3: Look for <table> with ui-datatable class
        m = re.search(r'<table[^>]*id="([^"]*)"[^>]*class="[^"]*ui-datatable[^"]*"', search_text)
        if m:
            logger.info(f"Found DataTable ID (S3 table element): {m.group(1)}")
            return m.group(1)

        # Strategy 4: Look for any element with datatable-related id pattern
        m = re.search(r'id="([^"]*(?:groupingTable|dataTable|datatable)[^"]*)"', search_text, re.IGNORECASE)
        if m:
            logger.info(f"Found DataTable ID (S4 name pattern): {m.group(1)}")
            return m.group(1)

        # Strategy 5: Look for _paginator suffix which implies a DataTable
        m = re.search(r'id="([^"]*?)_paginator', search_text)
        if m:
            logger.info(f"Found DataTable ID (S5 paginator suffix): {m.group(1)}")
            return m.group(1)

        # Strategy 6: From AJAX <update id="X"> elements whose CDATA has <tbody>
        for umatch in re.finditer(r'<update\s+id="([^"]+)"[^>]*><!\[CDATA\[(.*?)\]\]>', html, re.DOTALL):
            uid = umatch.group(1)
            ucontent = umatch.group(2)
            if '<tbody' in ucontent and 'ViewState' not in uid and 'viewstate' not in uid.lower():
                logger.info(f"Found DataTable ID (S6 AJAX update with tbody): {uid}")
                return uid

        # Strategy 7: Any id attribute containing "table" (case insensitive)
        m = re.search(r'id="([^"]*[Tt]able[^"]*)"', search_text)
        if m and 'ViewState' not in m.group(1):
            logger.info(f"Found DataTable ID (S7 id containing 'table'): {m.group(1)}")
            return m.group(1)

        # Strategy 8: Find the id of the first <div> that is a parent of <tbody>
        m = re.search(r'<div[^>]*id="([^"]*)"[^>]*>[\s\S]{0,2000}<tbody', search_text)
        if m and 'ViewState' not in m.group(1):
            logger.info(f"Found DataTable ID (S8 parent div of tbody): {m.group(1)}")
            return m.group(1)

        # Log all IDs found for debugging
        all_ids = re.findall(r'id="([^"]{2,60})"', search_text)
        logger.warning(f"Could not find DataTable ID. All IDs found ({len(all_ids)}): "
                       f"{all_ids[:20]}")
        return None

    def _count_tbody_rows(self, html):
        """Count the number of <tr> rows in the first <tbody> found."""
        search_text = html
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        if cdata_blocks:
            search_text = " ".join(cdata_blocks)
        tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', search_text, re.DOTALL)
        if tbody_match:
            return len(re.findall(r'<tr[^>]*>', tbody_match.group(1)))
        return 0

    def _parse_datatable_config(self, html):
        """Extract PrimeFaces DataTable widget config from response HTML.

        Parses the PrimeFaces.cw("DataTable",...) JavaScript call to get:
        - dt_id: DataTable component ID (e.g., 'groupingTable')
        - rows_per_page: rows per page from paginator config
        - row_count: total row count
        - scrollable: whether it's a scrollable DataTable
        - live_scroll: whether lazy loading via scroll is enabled

        Returns dict with config keys, or None if not found.
        """
        # Find the full config string first
        start = html.find('PrimeFaces.cw("DataTable"')
        if start < 0:
            return None

        # Extract the full config block (up to closing parenthesis)
        depth = 0
        end = start
        for i in range(start, min(start + 2000, len(html))):
            if html[i] == '(':
                depth += 1
            elif html[i] == ')':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        config_str = html[start:end]
        logger.info(f"Raw DataTable config: {config_str[:300]}...")

        # Parse individual fields
        widget_var = re.search(r'PrimeFaces\.cw\("DataTable","([^"]+)"', config_str)
        dt_id = re.search(r'id:"([^"]+)"', config_str)
        rows = re.search(r'rows:(\d+)', config_str)
        row_count = re.search(r'rowCount:(\d+)', config_str)
        scrollable = re.search(r'scrollable:(true|false)', config_str)
        live_scroll = re.search(r'liveScroll:(true|false)', config_str)
        scroll_limit = re.search(r'scrollLimit:(\d+)', config_str)

        if not dt_id:
            return None

        config = {
            "widget_var": widget_var.group(1) if widget_var else "",
            "dt_id": dt_id.group(1),
            "rows_per_page": int(rows.group(1)) if rows else 25,
            "row_count": int(row_count.group(1)) if row_count else 0,
            "scrollable": scrollable.group(1) == "true" if scrollable else False,
            "live_scroll": live_scroll.group(1) == "true" if live_scroll else False,
            "scroll_limit": int(scroll_limit.group(1)) if scroll_limit else 0,
        }
        logger.info(f"DataTable config: {config}")
        return config

    def _fetch_datatable_page(self, dt_id, first, rows_per_page, mode="pagination"):
        """Fetch a specific page/scroll batch of a PrimeFaces DataTable.

        PrimeFaces DataTables have multiple navigation modes:
        - "pagination": Standard paginator without behavior event (most common).
            This is what PrimeFaces uses when there's NO server-side <p:ajax event="page">
            listener. The paginator sends a simple AJAX request with pagination params.
        - "live_scroll": Lazy-loading via scroll events (liveScroll:true).
            Uses _scrolling/_scrollOffset/_scrollRows params.
        - "page_event": Paginator WITH a server-side page behavior listener.
            Adds javax.faces.behavior.event=page (rarely needed).

        Args:
            dt_id: DataTable widget ID (e.g., 'groupingTable')
            first: 0-based row offset
            rows_per_page: Number of rows per page/scroll batch
            mode: "pagination" (default), "live_scroll", or "page_event"

        Returns:
            Response HTML text.
        """
        if mode == "live_scroll":
            # PrimeFaces scrollable DataTable with liveScroll:true
            data = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": dt_id,
                "javax.faces.partial.execute": dt_id,
                "javax.faces.partial.render": dt_id,
                f"{dt_id}_scrolling": "true",
                f"{dt_id}_skipChildren": "true",
                f"{dt_id}_scrollOffset": str(first),
                f"{dt_id}_scrollRows": str(rows_per_page),
                f"{dt_id}_encodeFeature": "true",
                "masterLayout_formlogin": "masterLayout_formlogin",
                "javax.faces.ViewState": self.viewstate,
            }
        else:
            # Standard pagination — NO behavior event
            # This matches PrimeFaces.ajax.Request.handle() which is called
            # when DataTable.paginate() has no server-side page listener.
            data = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": dt_id,
                "javax.faces.partial.execute": dt_id,
                "javax.faces.partial.render": dt_id,
                f"{dt_id}_pagination": "true",
                f"{dt_id}_first": str(first),
                f"{dt_id}_rows": str(rows_per_page),
                f"{dt_id}_skipChildren": "true",
                f"{dt_id}_encodeFeature": "true",
                "masterLayout_formlogin": "masterLayout_formlogin",
                "javax.faces.ViewState": self.viewstate,
            }
            # Only add behavior event for "page_event" mode (rarely needed)
            if mode == "page_event":
                data["javax.faces.behavior.event"] = "page"
                data["javax.faces.partial.event"] = "page"

        # ── Merge in saved form fields ──
        # PrimeFaces AJAX serializes the ENTIRE form with every request.
        # Without the filter fields (state, year, RTO, axes, etc.) the server
        # has no context and returns only a ViewState update.
        if self._last_form_params:
            for k, v in self._last_form_params.items():
                if k not in data:  # Don't override DataTable-specific params
                    data[k] = v

        logger.info(f"Pagination request: mode={mode}, dt_id={dt_id}, "
                    f"first={first}, rows={rows_per_page}, "
                    f"total_params={len(data)}, "
                    f"viewstate_len={len(self.viewstate) if self.viewstate else 0}")

        r = self.session.post(self.form_url, data=data, timeout=self.timeout, headers={
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
        })
        r.raise_for_status()

        # Check what the response contains
        has_tbody = "<tbody" in r.text and "<td" in r.text
        has_update = "<update" in r.text
        old_vs_len = len(self.viewstate) if self.viewstate else 0

        new_vs = self._extract_viewstate(r.text)
        if new_vs:
            self.viewstate = new_vs
            logger.info(f"Pagination response: {len(r.text)} chars, "
                        f"has_data={has_tbody}, has_update={has_update}, "
                        f"ViewState updated ({old_vs_len} → {len(new_vs)})")
        else:
            logger.warning(f"Pagination response: {len(r.text)} chars, "
                           f"has_data={has_tbody}, has_update={has_update}, "
                           f"NO ViewState in response!")

        return r.text

    def _fetch_all_datatable_rows(self, response_html, first_page_records):
        """Fetch all rows from a paginated/scrollable PrimeFaces DataTable.

        Detects whether the DataTable is scrollable or standard-paginated
        from the PrimeFaces widget config, then uses the appropriate AJAX
        mechanism to fetch all remaining rows.

        Saves debug HTML to debug/ folder for inspection.
        If pagination fails entirely, returns first_page_records unchanged.
        """
        import os

        # ── Save debug HTML for inspection ──
        try:
            debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, 'last_scrape_response.html')
            with open(debug_path, 'w', encoding='utf-8', errors='replace') as f:
                f.write(response_html)
            logger.info(f"Debug: saved response HTML ({len(response_html)} chars) "
                        f"to debug/last_scrape_response.html")
        except Exception as e:
            logger.debug(f"Could not save debug HTML: {e}")

        # ── Save headers from first page for reuse with pagination ──
        saved_headers = self._extract_table_headers(response_html)
        logger.info(f"First page headers ({len(saved_headers)}): {saved_headers[:8]}...")

        first_page_row_count = self._count_tbody_rows(response_html)
        first_page_oems = len(set(r["oem_raw"] for r in first_page_records)) if first_page_records else 0
        logger.info(f"First page: {first_page_row_count} table rows, "
                    f"{first_page_oems} unique OEMs, {len(first_page_records)} records")

        # If first page has very few rows, it's possibly not paginated
        if first_page_row_count <= 2:
            return first_page_records

        # ── Parse DataTable widget config ──
        dt_config = self._parse_datatable_config(response_html)
        dt_id = self._find_datatable_id(response_html)

        if dt_config:
            dt_id = dt_config["dt_id"]
            rows_per_page = dt_config["rows_per_page"]
            total_rows = dt_config["row_count"]
            scrollable = dt_config["scrollable"]
            live_scroll = dt_config.get("live_scroll", False)

            # Determine pagination mode:
            # - liveScroll:true → use scroll-load AJAX ("live_scroll")
            # - otherwise → standard pagination WITHOUT behavior event ("pagination")
            #   (scrollable:true with liveScroll:false just means fixed header UI)
            if live_scroll:
                fetch_mode = "live_scroll"
            else:
                fetch_mode = "pagination"

            logger.info(f"DataTable: id='{dt_id}', scrollable={scrollable}, "
                        f"liveScroll={live_scroll}, mode='{fetch_mode}', "
                        f"rows_per_page={rows_per_page}, total={total_rows}")
        else:
            rows_per_page = first_page_row_count
            total_rows = 0
            fetch_mode = "pagination"
            logger.info(f"No widget config found — using detected id='{dt_id}', "
                        f"mode='{fetch_mode}', rows_per_page={rows_per_page}")

        if not dt_id:
            logger.warning("No DataTable ID found — returning first page only")
            return first_page_records

        # ── Fetch remaining rows via scroll (for scrollable) or page ──
        all_records = list(first_page_records)
        seen_oems = set(r["oem_raw"] for r in all_records)

        # Calculate how many more batches we need
        if total_rows > 0:
            remaining = total_rows - first_page_row_count
            batches_needed = (remaining + rows_per_page - 1) // rows_per_page
        else:
            batches_needed = 49  # Safety limit

        logger.info(f"Fetching up to {batches_needed} more batches "
                    f"(mode='{fetch_mode}')")

        for batch in range(batches_needed):
            offset = first_page_row_count + (batch * rows_per_page)

            # If we know total, stop when we've reached it
            if total_rows > 0 and offset >= total_rows:
                logger.info(f"Reached total row count ({total_rows}) — stopping")
                break

            try:
                time.sleep(0.3)
                page_html = self._fetch_datatable_page(
                    dt_id, offset, rows_per_page, mode=fetch_mode
                )

                # Save first pagination response for debugging
                if batch == 0:
                    try:
                        pag_path = os.path.join(debug_dir, 'last_pagination_response.html')
                        with open(pag_path, 'w', encoding='utf-8', errors='replace') as f:
                            f.write(page_html)
                    except Exception:
                        pass

                page_records = self._extract_table(page_html, saved_headers)

                # If first batch returns no data, try fallback modes
                if not page_records and batch == 0:
                    if fetch_mode == "pagination":
                        logger.info("pagination mode returned empty — trying page_event mode")
                        page_html = self._fetch_datatable_page(
                            dt_id, offset, rows_per_page, mode="page_event"
                        )
                        page_records = self._extract_table(page_html, saved_headers)
                        if page_records:
                            fetch_mode = "page_event"
                            logger.info(f"page_event mode works! Switching to it.")
                    if not page_records and fetch_mode != "live_scroll":
                        logger.info("Trying live_scroll mode as last resort")
                        page_html = self._fetch_datatable_page(
                            dt_id, offset, rows_per_page, mode="live_scroll"
                        )
                        page_records = self._extract_table(page_html, saved_headers)
                        if page_records:
                            fetch_mode = "live_scroll"
                            logger.info(f"live_scroll mode works! Switching to it.")

                if not page_records:
                    logger.info(f"Batch {batch + 2}: empty — reached end of data")
                    break

                # Check for duplicate OEMs (server looping back)
                new_oems = set(r["oem_raw"] for r in page_records) - seen_oems
                if not new_oems and batch > 1:
                    logger.info(f"Batch {batch + 2}: all OEMs already seen — stopping")
                    break

                all_records.extend(page_records)
                seen_oems.update(r["oem_raw"] for r in page_records)
                logger.info(f"Batch {batch + 2} (offset={offset}): "
                            f"+{len(page_records)} records, +{len(new_oems)} new OEMs "
                            f"(total: {len(all_records)} records, {len(seen_oems)} OEMs)")

            except Exception as e:
                logger.warning(f"Batch {batch + 2} (offset={offset}) failed: {e}")
                break

        total_oems = len(seen_oems)
        if total_oems > first_page_oems:
            logger.info(f"Pagination SUCCESS: {total_oems} OEMs across "
                        f"{len(all_records)} records (vs {first_page_oems} on first page)")
        else:
            logger.warning(f"Pagination: no additional OEMs found. "
                           f"Check debug/last_pagination_response.html")

        return all_records

    def _extract_table(self, html, saved_headers=None):
        """Parse the Maker x Month data table from AJAX response HTML.

        Args:
            html: Response HTML (full page or AJAX partial response)
            saved_headers: Optional list of header strings to use if no <thead>
                           found (useful for pagination responses)

        Returns list of dicts: [{oem_raw, month_label, volume}, ...]
        """
        results = []

        # Extract content from CDATA if it's an AJAX response
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        search_text = " ".join(cdata_blocks) if cdata_blocks else html

        # Find the data table — try multiple patterns
        table_match = re.search(
            r'<table[^>]*class="[^"]*ui-datatable[^"]*"[^>]*>(.*?)</table>',
            search_text, re.DOTALL
        )
        if not table_match:
            # Try finding any table with thead/tbody
            table_match = re.search(r'<table[^>]*>(.*?<thead.*?</tbody>.*?)</table>', search_text, re.DOTALL)

        if not table_match:
            # Last resort: find any table that has a <tbody>
            table_match = re.search(r'<table[^>]*>(.*?</tbody>)', search_text, re.DOTALL)

        if not table_match:
            # Scroll/pagination responses may return rows without <table> wrapper
            # (e.g., just <tbody> or raw <tr> elements in CDATA)
            if '<tbody' in search_text or '<tr' in search_text:
                table_html = search_text
                logger.debug("No <table> found — using raw CDATA for row extraction")
            else:
                logger.warning("No data table found in response")
                return results
        else:
            table_html = table_match.group(1)

        # Extract headers
        headers = []
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', table_html, re.DOTALL)
        if thead_match:
            headers = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', thead_match.group(1))
            headers = [h.strip() for h in headers if h.strip()]

        # Fallback: use saved_headers from first page if no <thead> found
        if not headers and saved_headers:
            headers = saved_headers
            logger.debug(f"Using saved headers ({len(headers)} columns) for pagination page")

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
        first_page_records = self._extract_table(response_html)
        logger.info(f"First page: {len(first_page_records)} records for "
                    f"{category_code}/{state_name}/{year}")

        # Fetch all pages (handles pagination automatically)
        all_records = self._fetch_all_datatable_rows(response_html, first_page_records)

        logger.info(f"Total: {len(all_records)} records for {category_code}/{state_name}/{year}")
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
