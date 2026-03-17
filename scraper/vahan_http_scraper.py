"""HTTP-based scraper for Vahan portal — no Chrome/Selenium dependency.

Uses requests + PrimeFaces AJAX to fetch state-level OEM registration data.

NOTE: The Vahan portal (a Government of India website) often blocks cloud/
datacenter IPs. This scraper works best when run from a local machine or
residential IP. On Streamlit Cloud, the connection will likely be rejected.
"""
import html as _html
import re
import ssl
import time
import logging
import platform
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter

from config.settings import (
    VAHAN_URL, VAHAN_SCRAPE_CONFIGS,
    VHCLASS_TO_CATEGORY, FUEL_TO_SUBCATEGORY,
    VEHCAT_TO_CATEGORY, FUEL_TO_GROUP, VEHCLASS_TO_NATIONAL,
    FY_DROPDOWN_VALUES, MONTH_DROPDOWN_MAP,
)
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

        # Checkbox label→value maps discovered from page HTML.
        # Keys = PrimeFaces element names (VhClass, VhCatg, fuel).
        # Values = dict mapping label (e.g. "MOTOR CAR") to form value (e.g. "7").
        self._checkbox_options = {}  # {panel_name: {label: value, ...}}

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

        # Discover checkbox options for VhClass, VhCatg, fuel panels.
        # PrimeFaces SelectManyCheckbox renders as:
        #   <input id="VhClass:0" name="VhClass" type="checkbox" value="7" />
        #   <label for="VhClass:0">MOTOR CAR</label>
        for panel_name in ("VhClass", "VhCatg", "fuel"):
            pattern = (
                rf'<input\s+id="{panel_name}:(\d+)"\s+name="{panel_name}"'
                rf'\s+type="checkbox"\s+value="([^"]+)"\s*/?>'
                rf'.*?<label\s+for="{panel_name}:\1">([^<]+)</label>'
            )
            matches = re.findall(pattern, html)
            if matches:
                label_map = {_html.unescape(label.strip()): value for _, value, label in matches}
                self._checkbox_options[panel_name] = label_map
                logger.info(f"Discovered {len(label_map)} options for {panel_name}")
            else:
                logger.warning(f"No checkbox options found for {panel_name}")

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
        """Set all form filters via sequential AJAX calls.

        Args:
            state_code: State code (e.g. 'MH') or None for All States
            year: Year number (used for logging; actual value from config)
            config: Dict with keys: y_axis, x_axis, year_type ('C'/'F'),
                    year_value (e.g. '2024-2025' for FY)
        """
        # 1. Set number format to Actual
        if self._number_format_id:
            self._ajax_post(
                self._number_format_id,
                self._number_format_id,
                self._number_format_id,
                {f"{self._number_format_id}_input": "A"},
            )
            time.sleep(0.5)

        # 2. Select state (or All States if state_code is None/empty)
        if self._state_id:
            state_val = state_code if state_code else "-1"
            resp = self._ajax_post(
                self._state_id,
                self._state_id,
                "selectedRto yaxisVar",
                {f"{self._state_id}_input": state_val},
            )
            time.sleep(1)

        # 3. Set Y-axis
        self._ajax_post(
            "yaxisVar", "yaxisVar", "yaxisVar",
            {"yaxisVar_input": config.get("y_axis", "Maker")},
        )
        time.sleep(0.5)

        # 4. Set X-axis
        self._ajax_post(
            "xaxisVar", "xaxisVar", "xaxisVar",
            {"xaxisVar_input": config.get("x_axis", "Month Wise")},
        )
        time.sleep(0.5)

        # 5. Set year type: "C" = Calendar Year, "F" = Financial Year
        year_type = config.get("year_type", "C")
        self._ajax_post(
            "selectedYearType", "selectedYearType", "selectedYearType selectedYear",
            {"selectedYearType_input": year_type},
        )
        time.sleep(0.5)

        # 6. Set year (string value - could be "2026" or "2024-2025" for FY)
        year_val = config.get("year_value", str(year))
        self._ajax_post(
            "selectedYear", "selectedYear", "selectedYear",
            {"selectedYear_input": year_val},
        )
        time.sleep(0.5)

        # 7. Set checkbox filters (VhClass, fuel) if present in config.
        # The Vahan portal DOES respect checkbox filters when they are
        # included as repeated form params in the refresh POST.
        # Format: VhClass=7&VhClass=71 (requests handles list values).
        if 'vehicle_class' in config or 'fuel' in config:
            self._set_checkbox_filters(config)
            logger.info(f"Checkbox filters set: {list(self._checkbox_values.keys())}")

    def _set_checkbox_filters(self, config):
        """Resolve checkbox filter labels to form values.

        Maps config labels (e.g. "MOTOR CAR") to the form values the Vahan
        portal expects (e.g. "7") using the label→value maps discovered
        from the page HTML.

        Stores resolved values in self._checkbox_values for inclusion in
        the refresh button POST.  No AJAX calls needed — checkboxes are
        submitted as part of the form data, not toggled individually.

        Panel name mapping:
            config key "vehicle_class" → form element "VhClass"
            config key "fuel"          → form element "fuel"

        NOTE: "VhCatg" does NOT exist as a SelectManyCheckbox on the Vahan
        portal — only VhClass, fuel, and norms panels are available.  All
        category filtering must use vehicle_class (VhClass) values.
        """
        PANEL_MAP = {
            "vehicle_class": "VhClass",
            "fuel": "fuel",
        }

        self._checkbox_values = {}  # {panel_name: [value1, value2, ...]}

        for config_key, panel_name in PANEL_MAP.items():
            if config_key not in config:
                continue

            labels = config[config_key]
            label_map = self._checkbox_options.get(panel_name, {})
            resolved = []

            for label in labels:
                value = label_map.get(label)
                if value:
                    resolved.append(value)
                    logger.debug(f"Checkbox {panel_name}: '{label}' -> value={value}")
                else:
                    logger.warning(
                        f"Checkbox {panel_name}: label '{label}' not found in "
                        f"{len(label_map)} options. Available: "
                        f"{list(label_map.keys())[:10]}..."
                    )

            if resolved:
                self._checkbox_values[panel_name] = resolved
                logger.info(f"Checkbox filter {panel_name}: {len(resolved)} values selected "
                            f"({resolved})")
            else:
                logger.warning(f"No values resolved for {panel_name} — "
                               f"filter will NOT be applied!")

    def _find_and_click_refresh(self, state_code, year, config):
        """Submit the form by finding and clicking the refresh button.

        Includes checkbox filter values (VhClass, VhCatg, fuel) resolved by
        _set_checkbox_filters().  PrimeFaces SelectManyCheckbox submits
        multiple checked values as repeated form params, e.g.:
            VhClass=7&VhClass=71  (for "MOTOR CAR" + "MOTOR CAB")

        Also saves form field values for use in subsequent pagination requests.
        """
        # Build form data with all current selections
        form_data = {
            "masterLayout_formlogin": "masterLayout_formlogin",
            "javax.faces.ViewState": self.viewstate,
            "selectedRto_input": "-1",
            "yaxisVar_input": config.get("y_axis", "Maker"),
            "xaxisVar_input": config.get("x_axis", "Month Wise"),
            "selectedYearType_input": config.get("year_type", "C"),
            "selectedYear_input": config.get("year_value", str(year)),
        }

        if self._number_format_id:
            form_data[f"{self._number_format_id}_input"] = "A"
        if self._state_id:
            state_val = state_code if state_code else "-1"
            form_data[f"{self._state_id}_input"] = state_val

        # Include checkbox filter values as repeated form params.
        # requests.post(data={...}) handles list values correctly:
        #   {"VhClass": ["7", "71"]} -> VhClass=7&VhClass=71
        if hasattr(self, '_checkbox_values') and self._checkbox_values:
            for panel_name, values in self._checkbox_values.items():
                form_data[panel_name] = values
            logger.info(f"Checkbox filters in form: {self._checkbox_values}")

        # Try known refresh button IDs
        for btn_id in ["j_idt69", "j_idt76", "j_idt85", "j_idt84", "j_idt93", "j_idt103", "j_idt61", "j_idt59"]:
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

        Handles multi-row <thead> (Vahan portal uses 2 rows):
          Row 0: [S No, <Y-axis label>, <X-axis group label>, TOTAL]
          Row 1: [JAN, FEB, ..., DEC]
        Reconstructs as: row0[:2] + row1 + [row0[-1]]
        """
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', html, re.DOTALL)
        search_text = " ".join(cdata_blocks) if cdata_blocks else html
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', search_text, re.DOTALL)
        if not thead_match:
            return []

        header_rows = re.findall(r'<tr[^>]*>(.*?)</tr>', thead_match.group(1), re.DOTALL)
        if len(header_rows) >= 2:
            def _th(row_html):
                cells = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', row_html)
                return [c.strip() for c in cells if c.strip()]
            row0 = _th(header_rows[0])
            row1 = _th(header_rows[1])
            return row0[:2] + row1 + [row0[-1]] if len(row0) >= 3 else row0 + row1
        else:
            headers = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', thead_match.group(1))
            return [h.strip() for h in headers if h.strip()]

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

        # Extract headers — handle multi-row <thead>.
        # Vahan portal uses 2 header rows:
        #   Row 0: [S No, <Y-axis label>, <X-axis group label>, TOTAL]
        #   Row 1: [JAN, FEB, ..., DEC]  (individual period headers)
        # The X-axis group label (e.g. "Month Wise") has NO data column,
        # so we reconstruct: row0[:2] + row1 + [row0[-1]]
        headers = []
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', table_html, re.DOTALL)
        if thead_match:
            header_rows = re.findall(r'<tr[^>]*>(.*?)</tr>', thead_match.group(1), re.DOTALL)
            if len(header_rows) >= 2:
                # Multi-row header: reconstruct properly
                def _extract_th(row_html):
                    cells = re.findall(r'<(?:th|span)[^>]*>([^<]*)</(?:th|span)>', row_html)
                    return [c.strip() for c in cells if c.strip()]
                row0 = _extract_th(header_rows[0])
                row1 = _extract_th(header_rows[1])
                # row0[:2] = [S No, Y-axis label], row1 = months, row0[-1] = TOTAL
                headers = row0[:2] + row1 + [row0[-1]] if len(row0) >= 3 else row0 + row1
                logger.debug(f"Multi-row header reconstructed: {len(row0)} + {len(row1)} -> {len(headers)} headers")
            else:
                # Single-row header: use as-is
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

        # Extract data rows — try <tbody> first, then raw <tr> elements
        # (pagination responses return just <tr> rows without a <tbody> wrapper)
        tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', table_html, re.DOTALL)
        if tbody_match:
            row_source = tbody_match.group(1)
        else:
            # No <tbody> — use table_html directly (common for pagination pages)
            row_source = table_html
            logger.debug("No <tbody> found — extracting <tr> rows directly")

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', row_source, re.DOTALL)
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

            oem_raw = _html.unescape(re.sub(r'<[^>]+>', '', cells[oem_idx]).strip())
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


    # -- New Y-axis based scraping methods ------------------------------------
    # These replace the old category-specific checkbox approach.
    # Instead of filtering by VhClass checkboxes (which the server ignores),
    # we set Y-axis to "Vehicle Class" or "Fuel" and map the returned labels
    # to our category codes.

    def _scrape_raw(self, state_name, year, y_axis='Maker', x_axis='Month Wise'):
        """Core scrape: set filters, click refresh, extract all table data.

        Returns list of dicts: [{oem_raw, month_label, volume}, ...]
        where oem_raw is the first column label (depends on y_axis).
        """
        state_code = STATE_CODES.get(state_name)
        if not state_code:
            raise ValueError(f'Unknown state: {state_name}. '
                             f'Available: {list(STATE_CODES.keys())}')

        config = {'y_axis': y_axis, 'x_axis': x_axis}

        # Reset and reload page for fresh session state
        self._page_loaded = False
        self._load_page()

        # Set all dropdown filters (no checkbox filters needed)
        self._set_filters(state_code, year, config)

        # Click refresh and get first page of data
        response_html = self._find_and_click_refresh(state_code, year, config)

        # Parse first page
        first_page = self._extract_table(response_html)
        logger.info(f'_scrape_raw({y_axis}, {state_name}, {year}): '
                    f'first page {len(first_page)} records')

        # Fetch all pages (handles pagination)
        all_records = self._fetch_all_datatable_rows(response_html, first_page)
        logger.info(f'_scrape_raw({y_axis}, {state_name}, {year}): '
                    f'total {len(all_records)} records')
        return all_records

    def scrape_category_totals(self, state_name, year):
        """Scrape with Y-axis=Vehicle Class and map to category codes.

        Returns list of dicts: [{category_code, month_label, volume}, ...]
        Vehicle class labels are mapped using VHCLASS_TO_CATEGORY.
        Volumes for vehicle classes in the same category are summed.
        """
        from collections import defaultdict

        records = self._scrape_raw(state_name, year, y_axis='Vehicle Class')

        totals = defaultdict(lambda: defaultdict(int))
        unmapped = set()

        for rec in records:
            vclass = rec['oem_raw'].strip().upper()
            cat_code = VHCLASS_TO_CATEGORY.get(vclass)
            if cat_code:
                totals[cat_code][rec['month_label']] += rec['volume']
            else:
                unmapped.add(vclass)

        if unmapped:
            logger.info(f'Unmapped vehicle classes (ignored): {unmapped}')

        result = []
        for cat_code, months in totals.items():
            for month_label, volume in months.items():
                result.append({
                    'category_code': cat_code,
                    'month_label': month_label,
                    'volume': volume,
                })

        cats_found = set(r['category_code'] for r in result)
        logger.info(f'Category totals for {state_name}/{year}: '
                    f'{len(result)} records, categories: {cats_found}')
        return result

    def scrape_fuel_totals(self, state_name, year):
        """Scrape with Y-axis=Fuel and map to fuel subcategories.

        Returns list of dicts: [{subcategory, month_label, volume}, ...]
        Note: cross-category totals (EV = EV_PV + EV_2W + EV_3W).
        """
        from collections import defaultdict

        records = self._scrape_raw(state_name, year, y_axis='Fuel')

        totals = defaultdict(lambda: defaultdict(int))
        unmapped = set()

        for rec in records:
            fuel_type = rec['oem_raw'].strip().upper()
            subcat = FUEL_TO_SUBCATEGORY.get(fuel_type)
            if subcat:
                totals[subcat][rec['month_label']] += rec['volume']
            else:
                unmapped.add(fuel_type)

        if unmapped:
            logger.debug(f'Unmapped fuel types (ignored): {unmapped}')

        result = []
        for subcat, months in totals.items():
            for month_label, volume in months.items():
                result.append({
                    'subcategory': subcat,
                    'month_label': month_label,
                    'volume': volume,
                })

        subcats_found = set(r['subcategory'] for r in result)
        logger.info(f'Fuel totals for {state_name}/{year}: '
                    f'{len(result)} records, subcategories: {subcats_found}')
        return result

    def _store_category_totals(self, conn, records, state_name, year):
        """Store category total records into state_monthly.

        Uses oem_name='__TOTAL__' to distinguish from OEM-specific records.
        """
        cursor = conn.cursor()
        rows = 0
        for rec in records:
            month_num = _parse_month(rec['month_label'])
            if month_num is None:
                continue
            cursor.execute(
                "INSERT INTO state_monthly"
                "    (category_code, oem_name, state, year, month, volume)"
                " VALUES (?, '__TOTAL__', ?, ?, ?, ?)"
                " ON CONFLICT(category_code, oem_name, state, year, month)"
                " DO UPDATE SET volume=excluded.volume,"
                "              updated_at=CURRENT_TIMESTAMP",
                (rec['category_code'], state_name, year, month_num,
                 rec['volume']))
            rows += 1
        conn.commit()
        logger.info(f'Stored {rows} category total records for {state_name}/{year}')
        return rows

    def _store_fuel_totals(self, conn, records, state_name, year):
        """Store fuel subcategory totals into state_monthly.

        Uses category_code='__EV__'/'__CNG__'/'__HYBRID__' and
        oem_name='__TOTAL__'.
        """
        FUEL_CAT_MAP = {'EV': '__EV__', 'CNG': '__CNG__', 'HYBRID': '__HYBRID__'}
        cursor = conn.cursor()
        rows = 0
        for rec in records:
            month_num = _parse_month(rec['month_label'])
            if month_num is None:
                continue
            cat_code = FUEL_CAT_MAP.get(rec['subcategory'], rec['subcategory'])
            cursor.execute(
                "INSERT INTO state_monthly"
                "    (category_code, oem_name, state, year, month, volume)"
                " VALUES (?, '__TOTAL__', ?, ?, ?, ?)"
                " ON CONFLICT(category_code, oem_name, state, year, month)"
                " DO UPDATE SET volume=excluded.volume,"
                "              updated_at=CURRENT_TIMESTAMP",
                (cat_code, state_name, year, month_num, rec['volume']))
            rows += 1
        conn.commit()
        logger.info(f'Stored {rows} fuel total records for {state_name}/{year}')
        return rows

    def scrape_and_store_state(self, state_name, year,
                               modes=('category', 'fuel', 'maker')):
        """Scrape a state/year with multiple Y-axes and store all results.

        modes:
            'category' - Y-axis=Vehicle Class -> PV/2W/3W/CV/TRACTORS totals
            'fuel'     - Y-axis=Fuel -> EV/CNG/HYBRID totals
            'maker'    - Y-axis=Maker -> OEM totals (all categories combined)

        Returns total rows upserted.
        """
        conn = get_connection()
        cursor = conn.cursor()
        total_rows = 0

        mode_configs = [
            ('category', '__CAT_TOTALS__',
             lambda: self.scrape_category_totals(state_name, year),
             lambda recs: self._store_category_totals(conn, recs, state_name, year)),
            ('fuel', '__FUEL_TOTALS__',
             lambda: self.scrape_fuel_totals(state_name, year),
             lambda recs: self._store_fuel_totals(conn, recs, state_name, year)),
            ('maker', '__ALL__',
             lambda: self._scrape_raw(state_name, year, y_axis='Maker'),
             lambda recs: self._store_records(conn, recs, '__ALL__', state_name, year)),
        ]

        try:
            for mode_name, log_cat, scrape_fn, store_fn in mode_configs:
                if mode_name not in modes:
                    continue

                cursor.execute(
                    "INSERT INTO scrape_log "
                    "(category_code, state, year, status, started_at) "
                    "VALUES (?, ?, ?, 'running', ?)",
                    (log_cat, state_name, year, datetime.now().isoformat()))
                log_id = cursor.lastrowid
                conn.commit()

                try:
                    records = scrape_fn()
                    rows = store_fn(records)
                    total_rows += rows
                    cursor.execute(
                        "UPDATE scrape_log SET status='success', "
                        "completed_at=?, rows_inserted=? WHERE id=?",
                        (datetime.now().isoformat(), rows, log_id))
                    conn.commit()
                except Exception as e:
                    cursor.execute(
                        "UPDATE scrape_log SET status='failed', "
                        "completed_at=?, error_message=? WHERE id=?",
                        (datetime.now().isoformat(), str(e)[:500], log_id))
                    conn.commit()
                    raise

                time.sleep(2)

            return total_rows

        finally:
            conn.close()

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


    # -- National-level scraping methods ----------------------------------------
    # These scrape Maker x (VehCat/Fuel/VehClass) for All States (national data).

    def _switch_table_month(self, current_html, month_label):
        """Switch the in-table month dropdown to a specific month.

        The Vahan portal data table has year and month dropdowns above
        the table that filter the displayed data via AJAX. These dropdowns
        are inside the AJAX-loaded table area (CDATA blocks).

        Args:
            current_html: Current page HTML (to discover dropdown IDs)
            month_label: Month value to select (e.g. 'JAN', 'FEB', or 'All')

        Returns: Updated HTML after month switch
        """
        # Search both raw HTML and CDATA blocks for the month dropdown
        search_texts = [current_html]
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', current_html, re.DOTALL)
        if cdata_blocks:
            search_texts.append(" ".join(cdata_blocks))

        month_select_id = None
        for search in search_texts:
            selects = re.findall(r'<select[^>]*id="([^"]*)"[^>]*>(.*?)</select>',
                                  search, re.DOTALL)
            for sel_id, sel_content in selects:
                # Month dropdown has JAN/FEB/DEC or month names as options
                if ('JAN' in sel_content and 'FEB' in sel_content and 'DEC' in sel_content):
                    month_select_id = sel_id.replace("_input", "")
                    break
                # Also check for "All" option with month names
                if ('All' in sel_content and 'JAN' in sel_content):
                    month_select_id = sel_id.replace("_input", "")
                    break
            if month_select_id:
                break

        if not month_select_id:
            # Try known patterns for the month dropdown ID
            combined = " ".join(search_texts)
            for pattern in [r'id="([^"]*(?:Month|month|selectMonth)[^"]*_input)"']:
                m = re.search(pattern, combined)
                if m:
                    month_select_id = m.group(1).replace("_input", "")
                    logger.info(f"Found month dropdown via pattern: {month_select_id}")
                    break

        if not month_select_id:
            logger.warning("Could not find month dropdown; using unfiltered data")
            return current_html

        # Discover the correct render target from the onchange handler
        # PrimeFaces.ab({s:"groupingTable:selectMonth", u:"combTablePnl", ...})
        render_target = None
        combined = " ".join(search_texts)
        # Look for u:"<target>" in the onchange PrimeFaces.ab() config
        m = re.search(
            r'selectMonth[^>]*onchange="[^"]*u:&quot;([^&]+)&quot;',
            combined
        )
        if m:
            render_target = m.group(1)
            logger.info(f"Month dropdown render target from onchange: {render_target}")
        else:
            render_target = "combTablePnl"  # Known default from portal HTML
            logger.info(f"Using default render target: {render_target}")

        # Discover the correct option VALUE for this month label.
        # The dropdown options use YYYYMM format (e.g. "202504" for APR 2025),
        # not the label text ("APR"). We need to find the value matching our label.
        month_value = month_label  # fallback to label if value not found
        for search in search_texts:
            # Find the selectMonth <select> and its <option> elements
            sel_pattern = re.compile(
                r'<select[^>]*selectMonth[^>]*>(.*?)</select>', re.DOTALL)
            sel_match = sel_pattern.search(search)
            if sel_match:
                sel_html = sel_match.group(1)
                # Match options: <option value="202504">APR</option>
                opts = re.findall(
                    r'<option[^>]*value="([^"]*)"[^>]*>\s*' + re.escape(month_label) + r'\s*</option>',
                    sel_html, re.IGNORECASE)
                if opts:
                    month_value = opts[0]
                    logger.info(f"Month dropdown: label={month_label} -> value={month_value}")
                    break
                # Also try matching just the text content
                opts2 = re.findall(r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)</option>', sel_html)
                for val, text in opts2:
                    if text.strip().upper() == month_label.upper():
                        month_value = val
                        logger.info(f"Month dropdown: label={month_label} -> value={month_value}")
                        break
                if month_value != month_label:
                    break

        logger.info(f"Switching table month to {month_label} (value={month_value}, "
                    f"dropdown={month_select_id}, render={render_target})")

        # Build AJAX params matching the browser's PrimeFaces.ab() call
        # Include all saved form fields so the server knows current filter state
        ajax_data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": month_select_id,
            "javax.faces.partial.execute": month_select_id,
            "javax.faces.partial.render": render_target,
            "javax.faces.behavior.event": "change",
            "javax.faces.partial.event": "change",
            "masterLayout_formlogin": "masterLayout_formlogin",
            f"{month_select_id}_input": month_value,
            "javax.faces.ViewState": self.viewstate,
        }
        # Include saved form params (carries current filter state: year, state, axes)
        if self._last_form_params:
            for k, v in self._last_form_params.items():
                if k not in ajax_data and k != 'javax.faces.ViewState':
                    ajax_data[k] = v
        # Override the month value explicitly
        ajax_data[f"{month_select_id}_input"] = month_value

        resp = self.session.post(self.form_url, data=ajax_data, timeout=self.timeout, headers={
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
        })
        resp.raise_for_status()

        # Update ViewState
        new_vs = self._extract_viewstate(resp.text)
        if new_vs:
            self.viewstate = new_vs

        time.sleep(1.5)

        def _update_month_in_params(mlabel):
            """Update saved form params so subsequent pagination carries the month filter."""
            # Update the month value in _last_form_params (used by _fetch_datatable_page)
            self._last_form_params[f"{month_select_id}_input"] = month_value
            logger.info(f"Updated _last_form_params[{month_select_id}_input] = {month_value}")
            # Also re-extract form fields from the new response to pick up any changes
            new_fields = self._extract_all_form_fields(resp.text)
            if new_fields:
                for k, v in new_fields.items():
                    self._last_form_params[k] = v
                # Ensure month is set to what we requested (override any default)
                self._last_form_params[f"{month_select_id}_input"] = month_value

        if self._has_data_table(resp.text):
            logger.info(f"Month switch to {month_label}: got data table ({len(resp.text)} chars)")
            _update_month_in_params(month_label)
            return resp.text

        # Check CDATA blocks for table data
        import re as re2
        cdata_blocks = re2.findall(r'<!\[CDATA\[(.*?)\]\]>', resp.text, re2.DOTALL)
        for block in cdata_blocks:
            if '<tbody' in block and '<td' in block:
                logger.info(f"Month switch to {month_label}: found table in CDATA ({len(block)} chars)")
                _update_month_in_params(month_label)
                return resp.text

        logger.warning(f"Month switch to {month_label} returned no data table ({len(resp.text)} chars)")
        return current_html

    def _scrape_raw_national(self, year, y_axis='Maker', x_axis='Vehicle Category',
                              year_type='F', year_value=None, month_label=None,
                              checkbox_filters=None):
        """Scrape national data: All States, specified Y and X axes.

        The portal has an in-table month dropdown for VehCat/Fuel/VehClass views
        (visible after clicking Refresh). Use Calendar Year mode and switch the
        month dropdown to get per-month data.

        Args:
            year: Calendar or FY start year (used for logging)
            y_axis: Y-axis dropdown value (default 'Maker')
            x_axis: X-axis dropdown value ('Vehicle Category', 'Fuel',
                    'Vehicle Class', 'Month Wise')
            year_type: 'C' for Calendar Year, 'F' for Financial Year
            year_value: Exact dropdown value (e.g. '2024-2025' for FY)
            month_label: If set, switch in-table month dropdown to this value
                         (e.g. 'JAN', 'FEB'). None = use default ('All').
            checkbox_filters: Optional dict of checkbox filters to apply.
                         Keys: 'vehicle_class' (list), 'fuel' (list).
                         Values are label strings matching portal options.

        Returns list of dicts: [{oem_raw, month_label (column header), volume}, ...]
        """
        if year_value is None:
            year_value = str(year)

        config = {
            'y_axis': y_axis,
            'x_axis': x_axis,
            'year_type': year_type,
            'year_value': year_value,
        }
        if checkbox_filters:
            config.update(checkbox_filters)

        # Reset session
        self._page_loaded = False
        self._load_page()

        # Set filters with state_code=None for All States
        self._set_filters(None, year, config)

        # Click refresh
        response_html = self._find_and_click_refresh(None, year, config)

        # If specific month requested, switch the in-table month dropdown
        if month_label:
            response_html = self._switch_table_month(response_html, month_label)

        # Parse table (columns are veh categories / fuel types / months)
        first_page = self._extract_table(response_html)
        logger.info(f'_scrape_raw_national({x_axis}, {year_value}, month={month_label}): '
                    f'first page {len(first_page)} records')

        all_records = self._fetch_all_datatable_rows(response_html, first_page)
        logger.info(f'_scrape_raw_national: total {len(all_records)} records')
        return all_records

    def scrape_national_vehcat(self, fy_start_year, month_num=None):
        """Scrape Maker x Vehicle Category for a specific month or full year.

        Uses Calendar Year mode with the in-table month dropdown to get
        per-month OEM x VehCat data. If month_num is None, returns the
        full-year aggregate (dropdown set to 'All').

        Args:
            fy_start_year: FY start year (e.g. 2024 for FY25 = Apr 2024 - Mar 2025)
            month_num: Calendar month (1-12). None = full year aggregate.

        Returns: list of dicts [{oem_raw, month_label (veh cat code), volume}]
        """
        if month_num is None:
            # Annual aggregate using FY mode
            fy_value = FY_DROPDOWN_VALUES.get(
                fy_start_year, f"{fy_start_year}-{fy_start_year+1}")
            return self._scrape_raw_national(
                year=fy_start_year, y_axis='Maker', x_axis='Vehicle Category',
                year_type='F', year_value=fy_value,
            )
        # Monthly: use CY mode + in-table month dropdown
        cal_year = fy_start_year if month_num >= 4 else fy_start_year + 1
        month_label = MONTH_DROPDOWN_MAP[month_num]
        return self._scrape_raw_national(
            year=cal_year, y_axis='Maker', x_axis='Vehicle Category',
            year_type='C', year_value=str(cal_year), month_label=month_label,
        )

    def scrape_national_fuel(self, fy_start_year, month_num=None):
        """Scrape Maker x Fuel for a specific month or full year.

        Uses Calendar Year mode with the in-table month dropdown.

        Args:
            fy_start_year: FY start year
            month_num: Calendar month (1-12). None = full year aggregate.
        """
        if month_num is None:
            fy_value = FY_DROPDOWN_VALUES.get(
                fy_start_year, f"{fy_start_year}-{fy_start_year+1}")
            return self._scrape_raw_national(
                year=fy_start_year, y_axis='Maker', x_axis='Fuel',
                year_type='F', year_value=fy_value,
            )
        cal_year = fy_start_year if month_num >= 4 else fy_start_year + 1
        month_label = MONTH_DROPDOWN_MAP[month_num]
        return self._scrape_raw_national(
            year=cal_year, y_axis='Maker', x_axis='Fuel',
            year_type='C', year_value=str(cal_year), month_label=month_label,
        )

    def scrape_national_vehclass(self, fy_start_year, month_num=None):
        """Scrape Maker x Vehicle Class for a specific month or full year.

        Uses Calendar Year mode with the in-table month dropdown.

        Args:
            fy_start_year: FY start year
            month_num: Calendar month (1-12). None = full year aggregate.
        """
        if month_num is None:
            fy_value = FY_DROPDOWN_VALUES.get(
                fy_start_year, f"{fy_start_year}-{fy_start_year+1}")
            return self._scrape_raw_national(
                year=fy_start_year, y_axis='Maker', x_axis='Vehicle Class',
                year_type='F', year_value=fy_value,
            )
        cal_year = fy_start_year if month_num >= 4 else fy_start_year + 1
        month_label = MONTH_DROPDOWN_MAP[month_num]
        return self._scrape_raw_national(
            year=cal_year, y_axis='Maker', x_axis='Vehicle Class',
            year_type='C', year_value=str(cal_year), month_label=month_label,
        )



    def scrape_national_subsegment(self, fy_start_year, subsegment_code, month_num=None):
        """Scrape per-OEM monthly data for a subsegment using checkbox filters.

        Subsegments combine vehicle_class + fuel checkbox filters to get
        cross-tabulated data (e.g. EV within PV, CNG within PV).

        Uses VAHAN_SCRAPE_CONFIGS from config/settings.py which defines
        the checkbox filter labels for each subsegment code.

        Args:
            fy_start_year: FY start year (e.g. 2024 for FY25)
            subsegment_code: One of: EV_PV, EV_2W, EV_3W, PV_CNG, PV_HYBRID
            month_num: Calendar month (1-12). None = full year aggregate.

        Returns: list of dicts [{oem_raw, month_label, volume}]
        """
        from config.settings import VAHAN_SCRAPE_CONFIGS, FY_DROPDOWN_VALUES, MONTH_DROPDOWN_MAP

        if subsegment_code not in VAHAN_SCRAPE_CONFIGS:
            raise ValueError(f"Unknown subsegment: {subsegment_code}. "
                             f"Available: {list(VAHAN_SCRAPE_CONFIGS.keys())}")

        cfg = VAHAN_SCRAPE_CONFIGS[subsegment_code]
        checkbox_filters = {}
        if 'vehicle_class' in cfg:
            checkbox_filters['vehicle_class'] = cfg['vehicle_class']
        if 'fuel' in cfg:
            checkbox_filters['fuel'] = cfg['fuel']

        if not checkbox_filters:
            raise ValueError(f"Subsegment {subsegment_code} has no checkbox filters defined!")

        if month_num is None:
            # Annual aggregate using FY mode
            fy_value = FY_DROPDOWN_VALUES.get(
                fy_start_year, f"{fy_start_year}-{fy_start_year+1}")
            return self._scrape_raw_national(
                year=fy_start_year, y_axis='Maker', x_axis='Month Wise',
                year_type='F', year_value=fy_value,
                checkbox_filters=checkbox_filters,
            )

        # Monthly: use CY mode + in-table month dropdown
        cal_year = fy_start_year if month_num >= 4 else fy_start_year + 1
        month_label = MONTH_DROPDOWN_MAP[month_num]
        return self._scrape_raw_national(
            year=cal_year, y_axis='Maker', x_axis='Month Wise',
            year_type='C', year_value=str(cal_year), month_label=month_label,
            checkbox_filters=checkbox_filters,
        )

    def _store_national_subsegment(self, conn, records, fy_start_year,
                                    subsegment_code, month):
        """Store subsegment OEM data into national_oem_subsegment table.

        Args:
            conn: SQLite connection
            records: list of dicts from scrape_national_subsegment()
            fy_start_year: FY start year
            subsegment_code: e.g. 'EV_PV', 'PV_CNG'
            month: Calendar month (1-12) or 0 for annual

        Returns: number of rows upserted
        """
        from config.oem_normalization import normalize_oem
        from datetime import datetime

        cursor = conn.cursor()

        # Ensure table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS national_oem_subsegment (
                oem_name TEXT NOT NULL,
                subsegment_code TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                volume REAL DEFAULT 0,
                updated_at TEXT,
                PRIMARY KEY (oem_name, subsegment_code, year, month)
            )
        """)

        # Determine calendar year from FY + month
        if month and month >= 4:
            cal_year = fy_start_year
        elif month:
            cal_year = fy_start_year + 1
        else:
            cal_year = fy_start_year  # annual

        now = datetime.now().isoformat()
        rows = 0

        # Aggregate by OEM (month_label column is the month from X-axis)
        from collections import defaultdict
        oem_totals = defaultdict(float)
        for rec in records:
            oem_raw = rec.get('oem_raw', '')
            volume = rec.get('volume', 0)
            if not oem_raw or volume == 0:
                continue
            oem = normalize_oem(oem_raw, subsegment_code)
            if oem:
                oem_totals[oem] += volume

        for oem, volume in oem_totals.items():
            cursor.execute("""
                INSERT INTO national_oem_subsegment
                    (oem_name, subsegment_code, year, month, volume, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(oem_name, subsegment_code, year, month)
                DO UPDATE SET volume=excluded.volume, updated_at=excluded.updated_at
            """, (oem, subsegment_code, cal_year, month, volume, now))
            rows += 1

        conn.commit()
        return rows


    def scrape_national_monthly(self, fy_start_year):
        """Scrape Maker x Month Wise for national monthly OEM totals.

        IMPORTANT: Month Wise X-axis only works with Calendar Year mode,
        not Financial Year mode. An FY (Apr-Mar) spans two calendar years,
        so we scrape both CYs and combine:
          FY25 = CY2024 (months 4-12) + CY2025 (months 1-3)

        Args:
            fy_start_year: FY start year (e.g. 2024 for FY25)

        Returns: list of dicts [{oem_raw, month_label (e.g. 'JAN'), volume}]
                 with records from both calendar years
        """
        all_records = []

        # Part 1: CY = fy_start_year (months Apr-Dec)
        logger.info(f"Monthly scrape: CY{fy_start_year} for FY{str(fy_start_year+1)[-2:]} Apr-Dec")
        records_cy1 = self._scrape_raw_national(
            year=fy_start_year,
            y_axis='Maker',
            x_axis='Month Wise',
            year_type='C',
            year_value=str(fy_start_year),
        )
        # Filter to only FY months (Apr=4 through Dec=12)
        FY_MONTHS_CY1 = {'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'}
        for rec in records_cy1:
            if rec['month_label'].strip().upper() in FY_MONTHS_CY1:
                all_records.append(rec)
        logger.info(f"  CY{fy_start_year}: {len(records_cy1)} raw -> {len(all_records)} FY-filtered")

        # Part 2: CY = fy_start_year + 1 (months Jan-Mar)
        cy2 = fy_start_year + 1
        logger.info(f"Monthly scrape: CY{cy2} for FY{str(fy_start_year+1)[-2:]} Jan-Mar")
        records_cy2 = self._scrape_raw_national(
            year=cy2,
            y_axis='Maker',
            x_axis='Month Wise',
            year_type='C',
            year_value=str(cy2),
        )
        # Filter to only FY months (Jan-Mar)
        FY_MONTHS_CY2 = {'JAN', 'FEB', 'MAR'}
        count_before = len(all_records)
        for rec in records_cy2:
            if rec['month_label'].strip().upper() in FY_MONTHS_CY2:
                all_records.append(rec)
        logger.info(f"  CY{cy2}: {len(records_cy2)} raw -> {len(all_records) - count_before} FY-filtered")

        logger.info(f"Monthly scrape FY{str(fy_start_year+1)[-2:]}: total {len(all_records)} records")
        return all_records

    def _store_national_vehcat(self, conn, records, fy_start_year, month_num):
        """Store Maker x VehCat records into national_oem_vehcat."""
        cursor = conn.cursor()
        rows = 0
        # Determine calendar year from FY + month
        # month_num=0 means annual aggregate; months 4-12 are in fy_start_year, 1-3 in next year
        cal_year = fy_start_year if (month_num == 0 or month_num >= 4) else fy_start_year + 1

        for rec in records:
            oem_raw = rec['oem_raw']
            oem_name = normalize_oem(oem_raw) or oem_raw
            col_label = rec['month_label'].strip()  # Veh category code
            if col_label.upper() == 'TOTAL':
                continue  # Skip the row-total column
            cat_group = VEHCAT_TO_CATEGORY.get(col_label, 'OTHERS')
            volume = rec['volume']

            cursor.execute("""
                INSERT INTO national_oem_vehcat
                    (oem_name, oem_raw, veh_category, category_group,
                     year, month, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(oem_raw, veh_category, year, month)
                DO UPDATE SET oem_name=excluded.oem_name,
                              category_group=excluded.category_group,
                              volume=excluded.volume,
                              scraped_at=CURRENT_TIMESTAMP
            """, (oem_name, oem_raw, col_label, cat_group,
                  cal_year, month_num, volume))
            rows += 1
        conn.commit()
        logger.info(f"Stored {rows} national vehcat records for {month_num}/{cal_year}")
        return rows

    def _store_national_fuel(self, conn, records, fy_start_year, month_num):
        """Store Maker x Fuel records into national_oem_fuel."""
        cursor = conn.cursor()
        rows = 0
        # month_num=0 means annual aggregate; months 4-12 are in fy_start_year, 1-3 in next year
        cal_year = fy_start_year if (month_num == 0 or month_num >= 4) else fy_start_year + 1

        for rec in records:
            oem_raw = rec['oem_raw']
            oem_name = normalize_oem(oem_raw) or oem_raw
            fuel_type = rec['month_label'].strip()
            if fuel_type.upper() == 'TOTAL':
                continue  # Skip the row-total column
            fuel_group = FUEL_TO_GROUP.get(fuel_type, 'Others')
            volume = rec['volume']

            cursor.execute("""
                INSERT INTO national_oem_fuel
                    (oem_name, oem_raw, fuel_type, fuel_group,
                     year, month, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(oem_raw, fuel_type, year, month)
                DO UPDATE SET oem_name=excluded.oem_name,
                              fuel_group=excluded.fuel_group,
                              volume=excluded.volume,
                              scraped_at=CURRENT_TIMESTAMP
            """, (oem_name, oem_raw, fuel_type, fuel_group,
                  cal_year, month_num, volume))
            rows += 1
        conn.commit()
        logger.info(f"Stored {rows} national fuel records for {month_num}/{cal_year}")
        return rows

    def _store_national_vehclass(self, conn, records, fy_start_year, month_num):
        """Store Maker x VehClass records into national_oem_vehclass."""
        cursor = conn.cursor()
        rows = 0
        # month_num=0 means annual aggregate; months 4-12 are in fy_start_year, 1-3 in next year
        cal_year = fy_start_year if (month_num == 0 or month_num >= 4) else fy_start_year + 1

        for rec in records:
            oem_raw = rec['oem_raw']
            oem_name = normalize_oem(oem_raw) or oem_raw
            veh_class = rec['month_label'].strip()
            if veh_class.upper() == 'TOTAL':
                continue  # Skip the row-total column
            class_group = VEHCLASS_TO_NATIONAL.get(veh_class, 'OTHERS')
            volume = rec['volume']

            cursor.execute("""
                INSERT INTO national_oem_vehclass
                    (oem_name, oem_raw, veh_class, class_group,
                     year, month, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(oem_raw, veh_class, year, month)
                DO UPDATE SET oem_name=excluded.oem_name,
                              class_group=excluded.class_group,
                              volume=excluded.volume,
                              scraped_at=CURRENT_TIMESTAMP
            """, (oem_name, oem_raw, veh_class, class_group,
                  cal_year, month_num, volume))
            rows += 1
        conn.commit()
        logger.info(f"Stored {rows} national vehclass records for {month_num}/{cal_year}")
        return rows

    def _store_national_monthly(self, conn, records, fy_start_year):
        """Store OEM x Month national data into national_monthly table.

        Uses the existing national_monthly table (shared with Excel data).
        Scraped data uses category_code='__ALL__' (cross-category OEM totals)
        and source='scrape' to distinguish from Excel-sourced rows.

        Converts month labels (APR, MAY, ...) to calendar year/month,
        normalizes OEM names, and upserts into the database.

        Args:
            conn: SQLite connection
            records: List of {oem_raw, month_label, volume}
            fy_start_year: FY start year

        Returns: Number of rows upserted
        """
        from config.oem_normalization import normalize_oem

        MONTH_LABEL_TO_NUM = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        }

        # Aggregate by normalized OEM + month (multiple raw entities may map to same OEM)
        from collections import defaultdict
        agg = defaultdict(int)  # (oem_name, cal_year, month_num) -> volume

        for rec in records:
            month_label = rec['month_label'].strip().upper()
            if month_label == 'TOTAL':
                continue

            month_num = MONTH_LABEL_TO_NUM.get(month_label)
            if not month_num:
                logger.warning(f"Unknown month label: {month_label}")
                continue

            # Convert FY month to calendar year
            # Convert FY month to calendar year
            cal_year = fy_start_year if (month_num == 0 or month_num >= 4) else fy_start_year + 1

            oem_raw = rec['oem_raw']
            oem_name = normalize_oem(oem_raw)
            volume = rec['volume']

            if volume == 0:
                continue

            agg[(oem_name, cal_year, month_num)] += volume

        rows = 0
        for (oem_name, cal_year, month_num), volume in agg.items():
            conn.execute("""
                INSERT INTO national_monthly
                    (category_code, oem_name, year, month, volume, source, updated_at)
                VALUES ('__ALL__', ?, ?, ?, ?, 'scrape', datetime('now'))
                ON CONFLICT(category_code, oem_name, year, month)
                DO UPDATE SET volume=excluded.volume,
                              source=excluded.source,
                              updated_at=excluded.updated_at
            """, (oem_name, cal_year, month_num, volume))
            rows += 1

        conn.commit()
        logger.info(f"Stored {rows} national monthly rows for FY{str(fy_start_year+1)[-2:]}")
        return rows

    def scrape_and_store_national(self, fy_start_year, month_num=None,
                                   scrape_types=('vehcat', 'fuel', 'vehclass')):
        """Scrape and store national OEM data for a financial year.

        VehCat/Fuel/VehClass types support both annual aggregates (month_num=None)
        and per-month data (month_num=1-12) using the in-table month dropdown.
        The 'monthly' type (Y=Maker, X=Month Wise) always gives all 12 months.

        Args:
            fy_start_year: FY start year (e.g. 2024 for FY25)
            month_num: Calendar month (1-12) for per-month VehCat/Fuel/VehClass.
                       None or 0 = annual aggregate.
                       Ignored for 'monthly' type (months come from X-axis).
            scrape_types: Which scrapes to run. Options:
                - 'vehcat': Y=Maker x X=Vehicle Category
                - 'fuel': Y=Maker x X=Fuel
                - 'vehclass': Y=Maker x X=Vehicle Class
                - 'monthly': Y=Maker x X=Month Wise (cross-category monthly)

        Returns: total rows upserted
        """
        conn = get_connection()
        cursor = conn.cursor()
        total_rows = 0

        # For annual types, store with month=0 as sentinel
        # For monthly type, months come from the column headers
        meta_month = month_num if month_num else 0

        # month_num: 1-12 for per-month data, None/0 for annual aggregate
        m = month_num if month_num else None

        type_configs = {
            'vehcat': (
                'national_vehcat',
                lambda: self.scrape_national_vehcat(fy_start_year, m),
                lambda recs: self._store_national_vehcat(conn, recs, fy_start_year,
                                                          month_num if month_num else 0),
            ),
            'fuel': (
                'national_fuel',
                lambda: self.scrape_national_fuel(fy_start_year, m),
                lambda recs: self._store_national_fuel(conn, recs, fy_start_year,
                                                        month_num if month_num else 0),
            ),
            'vehclass': (
                'national_vehclass',
                lambda: self.scrape_national_vehclass(fy_start_year, m),
                lambda recs: self._store_national_vehclass(conn, recs, fy_start_year,
                                                            month_num if month_num else 0),
            ),
            'monthly': (
                'national_monthly',
                lambda: self.scrape_national_monthly(fy_start_year),
                lambda recs: self._store_national_monthly(conn, recs, fy_start_year),
            ),
        }

        # Add subsegment types dynamically from VAHAN_SCRAPE_CONFIGS
        from config.settings import VAHAN_SCRAPE_CONFIGS
        SUBSEGMENT_CODES = ['EV_PV', 'EV_2W', 'EV_3W', 'PV_CNG', 'PV_HYBRID',
                            'PV', '2W', '3W', 'TRACTORS']
        for sub_code in SUBSEGMENT_CODES:
            if sub_code in VAHAN_SCRAPE_CONFIGS:
                sc = sub_code  # capture for lambda
                type_configs[f'sub_{sub_code.lower()}'] = (
                    f'national_sub_{sub_code.lower()}',
                    lambda _sc=sc: self.scrape_national_subsegment(
                        fy_start_year, _sc, month_num if month_num else None),
                    lambda recs, _sc=sc: self._store_national_subsegment(
                        conn, recs, fy_start_year, _sc,
                        month_num if month_num else 0),
                )

        for stype in scrape_types:
            if stype not in type_configs:
                logger.warning(f"Unknown scrape type: {stype}")
                continue
            meta_type, scrape_fn, store_fn = type_configs[stype]

            # Log start in scrape_metadata
            cursor.execute("""
                INSERT INTO scrape_metadata
                    (scrape_type, year, month, started_at, status)
                VALUES (?, ?, ?, ?, 'running')
                ON CONFLICT(scrape_type, year, month, state)
                DO UPDATE SET started_at=excluded.started_at, status='running'
            """, (meta_type, fy_start_year, meta_month, datetime.now().isoformat()))
            conn.commit()

            try:
                records = scrape_fn()
                rows = store_fn(records)
                total_rows += rows

                cursor.execute("""
                    UPDATE scrape_metadata
                    SET completed_at=?, rows_upserted=?, status='completed'
                    WHERE scrape_type=? AND year=? AND month=?
                """, (datetime.now().isoformat(), rows,
                      meta_type, fy_start_year, meta_month))
                conn.commit()
                logger.info(f"National {stype} FY{str(fy_start_year+1)[-2:]}: {rows} rows stored")

            except Exception as e:
                cursor.execute("""
                    UPDATE scrape_metadata
                    SET completed_at=?, status='failed'
                    WHERE scrape_type=? AND year=? AND month=?
                """, (datetime.now().isoformat(),
                      meta_type, fy_start_year, meta_month))
                conn.commit()
                logger.error(f"National {stype} FY{str(fy_start_year+1)[-2:]}: {e}")
                raise

        conn.close()
        return total_rows


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


def get_pending_state_scrapes(states, years, modes=('category', 'fuel', 'maker')):
    """Return (state, year) pairs not yet successfully scraped.

    Checks scrape_log for '__CAT_TOTALS__', '__FUEL_TOTALS__', and '__ALL__' entries.
    """
    coverage = get_scrape_coverage()
    pending = []
    for state in states:
        for year in years:
            needs_scrape = False
            if 'category' in modes:
                if ('__CAT_TOTALS__', state, year) not in coverage:
                    needs_scrape = True
            if 'fuel' in modes:
                if ('__FUEL_TOTALS__', state, year) not in coverage:
                    needs_scrape = True
            if 'maker' in modes:
                if ('__ALL__', state, year) not in coverage:
                    needs_scrape = True
            if needs_scrape:
                pending.append((state, year))
    return pending