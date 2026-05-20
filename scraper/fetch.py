#!/usr/bin/env python3
"""
Fulton County GA – Motivated Seller Lead Scraper
=================================================
Playwright scrapes the Superior Court Clerk portal for LP, NOFC, TAXDEED,
judgments, liens, probate, NOC, and RELLP filings from the last LOOKBACK_DAYS.
Enriches records with property + mailing address from the County Appraiser
bulk parcel DBF.  Outputs records.json (dashboard + data) and a GHL-ready CSV.

Author: Propstor LLC / Atlas Agent
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sys
import tempfile
import time
import traceback
import unicodedata
import zipfile
from datetime import datetime, timedelta
from functools import wraps
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup, Tag
from dbfread import DBF
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

LOOKBACK_DAYS: int = int(os.getenv("LOOKBACK_DAYS", "7"))
HEADLESS: bool = os.getenv("HEADLESS", "true").lower() != "false"
MAX_RETRIES: int = 3
RETRY_DELAY: float = 4.0
PAGE_TIMEOUT: int = 45_000          # ms
NAV_TIMEOUT: int = 60_000           # ms
MAX_PAGES_PER_DOCTYPE: int = 50     # safety cap

# Clerk portal – Georgia Superior Court Clerks' Cooperative Authority (GSCCCA)
# Covers ALL 159 Georgia counties via COUNTIES env var.
CLERK_BASE_URL   = "https://search.gsccca.org"
CLERK_SEARCH_URL = "https://search.gsccca.org/RealEstateIndex.aspx"

# ── COUNTY SELECTION ──────────────────────────────────────────────────────────
# Set COUNTIES env var to comma-separated names or "ALL" for all 159 counties.
# Default: George's 6 target counties.
# Examples:  COUNTIES=ALL
#            COUNTIES=FULTON,COBB,GWINNETT,DEKALB
_COUNTIES_ENV = os.getenv("COUNTIES", "FULTON,CLAYTON,HOUSTON,COBB,GWINNETT,DOUGLAS")

# Full GSCCCA county name to numeric ID mapping (all 159 GA counties)
ALL_GA_COUNTIES: Dict[str, str] = {
    "APPLING":"1","ATKINSON":"2","BACON":"3","BAKER":"4","BALDWIN":"5",
    "BANKS":"6","BARROW":"7","BARTOW":"8","BEN HILL":"9","BERRIEN":"10",
    "BIBB":"11","BLECKLEY":"12","BRANTLEY":"13","BROOKS":"14","BRYAN":"15",
    "BULLOCH":"16","BURKE":"17","BUTTS":"18","CALHOUN":"19","CAMDEN":"20",
    "CANDLER":"21","CARROLL":"22","CATOOSA":"23","CHARLTON":"24","CHATHAM":"25",
    "CHATTAHOOCHEE":"26","CHATTOOGA":"27","CHEROKEE":"28","CLARKE":"29",
    "CLAY":"30","CLAYTON":"31","CLINCH":"32","COBB":"33","COFFEE":"34",
    "COLQUITT":"35","COLUMBIA":"36","COOK":"37","COWETA":"38","CRAWFORD":"39",
    "CRISP":"40","DADE":"41","DAWSON":"42","DECATUR":"43","DEKALB":"44",
    "DODGE":"45","DOOLY":"46","DOUGHERTY":"47","DOUGLAS":"48","EARLY":"49",
    "ECHOLS":"50","EFFINGHAM":"51","ELBERT":"52","EMANUEL":"53","EVANS":"54",
    "FANNIN":"55","FAYETTE":"56","FLOYD":"57","FORSYTH":"58","FRANKLIN":"59",
    "FULTON":"60","GILMER":"61","GLASCOCK":"62","GLYNN":"63","GORDON":"64",
    "GRADY":"65","GREENE":"66","GWINNETT":"67","HABERSHAM":"68","HALL":"69",
    "HANCOCK":"70","HARALSON":"71","HARRIS":"72","HART":"73","HEARD":"74",
    "HENRY":"75","HOUSTON":"76","IRWIN":"77","JACKSON":"78","JASPER":"79",
    "JEFF DAVIS":"80","JEFFERSON":"81","JENKINS":"82","JOHNSON":"83",
    "JONES":"84","LAMAR":"85","LANIER":"86","LAURENS":"87","LEE":"88",
    "LIBERTY":"89","LINCOLN":"90","LONG":"91","LOWNDES":"92","LUMPKIN":"93",
    "MACON":"94","MADISON":"95","MARION":"96","MCDUFFIE":"97","MCINTOSH":"98",
    "MERIWETHER":"99","MILLER":"100","MITCHELL":"101","MONROE":"102",
    "MONTGOMERY":"103","MORGAN":"104","MURRAY":"105","MUSCOGEE":"106",
    "NEWTON":"107","OCONEE":"108","OGLETHORPE":"109","PAULDING":"110",
    "PEACH":"111","PICKENS":"112","PIERCE":"113","PIKE":"114","POLK":"115",
    "PULASKI":"116","PUTNAM":"117","QUITMAN":"118","RABUN":"119",
    "RANDOLPH":"120","RICHMOND":"121","ROCKDALE":"122","SCHLEY":"123",
    "SCREVEN":"124","SEMINOLE":"125","SPALDING":"126","STEPHENS":"127",
    "STEWART":"128","SUMTER":"129","TALBOT":"130","TALIAFERRO":"131",
    "TATTNALL":"132","TAYLOR":"133","TELFAIR":"134","TERRELL":"135",
    "THOMAS":"136","TIFT":"137","TOOMBS":"138","TOWNS":"139","TREUTLEN":"140",
    "TROUP":"141","TURNER":"142","TWIGGS":"143","UNION":"144","UPSON":"145",
    "WALKER":"146","WALTON":"147","WARE":"148","WARREN":"149",
    "WASHINGTON":"150","WAYNE":"151","WEBSTER":"152","WHEELER":"153",
    "WHITE":"154","WHITFIELD":"155","WILCOX":"156","WILKES":"157",
    "WILKINSON":"158","WORTH":"159",
}


def _resolve_counties() -> "List[Tuple[str, str]]":
    raw = _COUNTIES_ENV.strip().upper()
    if raw in ("ALL", "*", ""):
        return sorted(ALL_GA_COUNTIES.items())
    names = [n.strip() for n in raw.split(",") if n.strip()]
    result = []
    for name in names:
        cid = ALL_GA_COUNTIES.get(name)
        if cid:
            result.append((name, cid))
        else:
            print(f"WARNING: Unknown county '{name}' – skipping")
    return result if result else [("FULTON", "60")]


ACTIVE_COUNTIES: "List[Tuple[str, str]]" = _resolve_counties()

# Fulton County Property Appraiser bulk data
PARCEL_BASE_URL  = "https://fultoncountypropertyappraiser.org"
PARCEL_SEARCH_URL = "https://fultoncountypropertyappraiser.org/property-search/"

# ── GSCCCA AUTH ───────────────────────────────────────────────────────────────
# Option A (recommended): paste the GSCCCA_COOKIES JSON from get_gsccca_cookie.py
# Option B (fallback):    set GSCCCA_USERNAME + GSCCCA_PASSWORD
# Store all of these as GitHub Secrets.
GSCCCA_COOKIES:  str = os.getenv("GSCCCA_COOKIES",  "")   # JSON cookie bundle
GSCCCA_USERNAME: str = os.getenv("GSCCCA_USERNAME", "")
GSCCCA_PASSWORD: str = os.getenv("GSCCCA_PASSWORD", "")
GSCCCA_LOGIN_URL: str = "https://search.gsccca.org/Login.aspx"

# Where to write outputs
OUTPUT_PATHS: List[str] = ["dashboard/records.json", "data/records.json"]
GHL_CSV_PATH: str = "data/ghl_export.csv"
PARCEL_CACHE_PATH: str = "data/parcel_cache.json"

# ─────────────────────────────────────────────────────────────────────────────
# DOCUMENT TYPE CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────

# (code -> (human label, category_key))
DOC_TYPES: Dict[str, Tuple[str, str]] = {
    "LP":       ("Lis Pendens",             "LP"),
    "NOFC":     ("Notice of Foreclosure",   "NOFC"),
    "TAXDEED":  ("Tax Deed",                "TAXDEED"),
    "JUD":      ("Judgment",                "JUD"),
    "CCJ":      ("Certified Judgment",      "JUD"),
    "DRJUD":    ("Domestic Judgment",       "JUD"),
    "LNCORPTX": ("Corp Tax Lien",           "LIEN"),
    "LNIRS":    ("IRS Lien",                "LIEN"),
    "LNFED":    ("Federal Lien",            "LIEN"),
    "LN":       ("Lien",                    "LIEN"),
    "LNMECH":   ("Mechanic Lien",           "LIEN"),
    "LNHOA":    ("HOA Lien",                "LIEN"),
    "MEDLN":    ("Medicaid Lien",           "LIEN"),
    "PRO":      ("Probate",                 "PRO"),
    "NOC":      ("Notice of Commencement",  "NOC"),
    "RELLP":    ("Release Lis Pendens",     "RELLP"),
}

CATEGORY_LABELS: Dict[str, str] = {
    "LP":      "Lis Pendens",
    "NOFC":    "Pre-foreclosure",
    "TAXDEED": "Tax Deed / Tax Sale",
    "JUD":     "Judgment",
    "LIEN":    "Lien",
    "PRO":     "Probate / Estate",
    "NOC":     "Notice of Commencement",
    "RELLP":   "Release – Lis Pendens",
}

# Seller-Score flag labels
FLAG_LP         = "Lis pendens"
FLAG_PREFC      = "Pre-foreclosure"
FLAG_JUD        = "Judgment lien"
FLAG_TAXLIEN    = "Tax lien"
FLAG_MECH       = "Mechanic lien"
FLAG_PROBATE    = "Probate / estate"
FLAG_LLC        = "LLC / corp owner"
FLAG_NEW        = "New this week"

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT, stream=sys.stdout)
log = logging.getLogger("fulton-scraper")

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def retry(max_tries: int = MAX_RETRIES, delay: float = RETRY_DELAY, exc=(Exception,)):
    """Decorator: retry up to max_tries times on the given exceptions."""
    def deco(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_tries + 1):
                try:
                    return await fn(*args, **kwargs)
                except exc as e:
                    last_exc = e
                    log.warning(
                        "Attempt %d/%d failed for %s: %s",
                        attempt, max_tries, fn.__name__, e,
                    )
                    if attempt < max_tries:
                        await asyncio.sleep(delay * attempt)
            log.error("All %d attempts failed for %s", max_tries, fn.__name__)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return deco


def sync_retry(max_tries: int = MAX_RETRIES, delay: float = RETRY_DELAY, exc=(Exception,)):
    """Same as retry but for synchronous functions."""
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_tries + 1):
                try:
                    return fn(*args, **kwargs)
                except exc as e:
                    last_exc = e
                    log.warning(
                        "Attempt %d/%d failed for %s: %s",
                        attempt, max_tries, fn.__name__, e,
                    )
                    if attempt < max_tries:
                        time.sleep(delay * attempt)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return deco


def clean_str(s: Any) -> str:
    """Strip and normalise a string, return '' for None/non-string."""
    if s is None:
        return ""
    txt = unicodedata.normalize("NFKC", str(s)).strip()
    # collapse internal whitespace
    return re.sub(r"\s{2,}", " ", txt)


def parse_amount(raw: str) -> Optional[float]:
    """Parse dollar string → float, or None if not parseable."""
    if not raw:
        return None
    digits = re.sub(r"[^\d.]", "", raw)
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


def date_range_strings(lookback: int = LOOKBACK_DAYS) -> Tuple[str, str]:
    """Return (start_date_str, end_date_str) formatted MM/DD/YYYY."""
    today = datetime.today()
    start = today - timedelta(days=lookback)
    return start.strftime("%m/%d/%Y"), today.strftime("%m/%d/%Y")


def name_variants(full_name: str) -> List[str]:
    """
    Generate lookup variants for an owner name:
      - Original
      - LAST FIRST  (if comma-separated: "Doe, John")
      - FIRST LAST  (swap)
      - Normalised upper
    """
    if not full_name:
        return []
    name = clean_str(full_name).upper()
    variants = {name}
    # Handle "LAST, FIRST" format
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        # LAST, FIRST -> FIRST LAST
        variants.add(f"{parts[1]} {parts[0]}")
        variants.add(f"{parts[0]} {parts[1]}")
    else:
        words = name.split()
        if len(words) >= 2:
            # FIRST LAST -> LAST FIRST
            variants.add(f"{words[-1]} {' '.join(words[:-1])}")
            # LAST, FIRST
            variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
    return list(variants)


# ─────────────────────────────────────────────────────────────────────────────
# PROPERTY APPRAISER – BULK PARCEL DOWNLOAD
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    """Downloads the Fulton County bulk parcel DBF and builds an owner-name index."""

    # Known download endpoints for Fulton County parcel data (try in order)
    PARCEL_DOWNLOAD_CANDIDATES = [
        # Direct county GIS open data portal
        "https://opendata.fultoncountyga.gov/datasets/fulton-county-parcels/explore",
        # Property appraiser bulk download (ASP.NET WebForms __doPostBack)
        "https://fultoncountypropertyappraiser.org/downloads/",
        # ArcGIS FeatureServer export (GeoJSON / CSV fallback)
        (
            "https://services.arcgis.com/gXbFIzHRtHGMRJgj/arcgis/rest/services/"
            "Fulton_Parcels/FeatureServer/0/query?"
            "where=1%3D1&outFields=*&f=json&resultRecordCount=2000&resultOffset=0"
        ),
    ]

    def __init__(self) -> None:
        # owner_upper_key -> {prop_address, prop_city, prop_state, prop_zip,
        #                      mail_address, mail_city, mail_state, mail_zip}
        self._index: Dict[str, Dict[str, str]] = {}
        self._loaded = False

    # ------------------------------------------------------------------
    @sync_retry(max_tries=MAX_RETRIES, exc=(Exception,))
    def _fetch_property_appraiser_page(self) -> Optional[bytes]:
        """
        Attempt to download the bulk parcel ZIP/DBF from the property appraiser.
        Uses requests + BeautifulSoup to parse the download page and trigger
        any __doPostBack form submissions.
        """
        session = requests.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; PropstorBot/1.0; +https://propstor.com)"
        )

        # Step 1 – load the downloads page
        download_page_urls = [
            "https://fultoncountypropertyappraiser.org/downloads/",
            "https://fultoncountypropertyappraiser.org/data/",
            PARCEL_SEARCH_URL,
        ]
        page_html: Optional[str] = None
        page_url: Optional[str] = None
        for url in download_page_urls:
            try:
                r = session.get(url, timeout=30)
                if r.ok:
                    page_html = r.text
                    page_url = url
                    break
            except Exception as exc:
                log.debug("Download page %s failed: %s", url, exc)

        if not page_html:
            log.warning("Could not load property appraiser download page")
            return None

        soup = BeautifulSoup(page_html, "lxml")

        # Step 2 – look for direct download links (.zip, .dbf, .csv)
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if any(href.lower().endswith(ext) for ext in (".zip", ".dbf", ".csv")):
                full_url = urljoin(page_url, href)
                log.info("Found direct parcel download: %s", full_url)
                r = session.get(full_url, timeout=120, stream=True)
                if r.ok:
                    return r.content

        # Step 3 – try __doPostBack triggers for ASP.NET WebForms
        form = soup.find("form")
        if form:
            viewstate_input = soup.find("input", {"name": "__VIEWSTATE"})
            eventval_input = soup.find("input", {"name": "__EVENTVALIDATION"})
            viewstate = viewstate_input["value"] if viewstate_input else ""
            eventval = eventval_input["value"] if eventval_input else ""
            action = form.get("action", page_url)
            if not action.startswith("http"):
                action = urljoin(page_url, action)

            for btn in soup.find_all(
                ["input", "button", "a"],
                string=re.compile(r"(download|parcel|bulk|data|export)", re.I),
            ):
                event_target = btn.get("name", btn.get("id", ""))
                if not event_target:
                    continue
                payload = {
                    "__EVENTTARGET": event_target,
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": viewstate,
                    "__EVENTVALIDATION": eventval,
                }
                try:
                    r = session.post(action, data=payload, timeout=120, stream=True)
                    ct = r.headers.get("Content-Type", "")
                    if r.ok and ("zip" in ct or "octet" in ct or "dbf" in ct):
                        log.info("DoPostBack download succeeded for target %s", event_target)
                        return r.content
                except Exception as exc:
                    log.debug("DoPostBack %s failed: %s", event_target, exc)

        log.warning("No downloadable parcel file found via property appraiser page")
        return None

    # ------------------------------------------------------------------
    def _fetch_arcgis_paginated(self) -> List[Dict]:
        """
        Fallback: Pull parcel records from ArcGIS FeatureServer in pages of 2000.
        Returns list of attribute dicts.
        """
        base = (
            "https://services.arcgis.com/gXbFIzHRtHGMRJgj/arcgis/rest/services/"
            "Fulton_Parcels/FeatureServer/0/query"
        )
        records: List[Dict] = []
        offset = 0
        page_size = 2000
        session = requests.Session()

        while True:
            params = {
                "where": "1=1",
                "outFields": (
                    "OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITE_ZIP,"
                    "ADDR_1,MAILADR1,CITY,MAILCITY,STATE,ZIP,MAILZIP,PARID"
                ),
                "f": "json",
                "resultRecordCount": page_size,
                "resultOffset": offset,
                "returnGeometry": "false",
            }
            try:
                r = session.get(base, params=params, timeout=60)
                data = r.json()
                features = data.get("features", [])
                if not features:
                    break
                records.extend(f["attributes"] for f in features)
                log.info("ArcGIS parcel page offset=%d → %d records", offset, len(features))
                if len(features) < page_size:
                    break
                offset += page_size
            except Exception as exc:
                log.warning("ArcGIS parcel fetch failed at offset %d: %s", offset, exc)
                break

        return records

    # ------------------------------------------------------------------
    def _fetch_open_data_csv(self) -> Optional[List[Dict]]:
        """
        Try Fulton County open data portal for parcel CSV export.
        """
        candidates = [
            # Fulton County GIS Open Data – different export endpoints
            "https://opendata.fultoncountyga.gov/api/download/v1/items/fulton-county-parcels/csv",
            "https://opendata.fultoncountyga.gov/datasets/fulton-county-parcels_0.csv",
            "https://opendata.fultoncountyga.gov/datasets/Fulton_Parcels.csv",
        ]
        session = requests.Session()
        for url in candidates:
            try:
                r = session.get(url, timeout=120, stream=True)
                if r.ok and "text/csv" in r.headers.get("Content-Type", ""):
                    lines = r.text.splitlines()
                    reader = csv.DictReader(lines)
                    rows = list(reader)
                    if rows:
                        log.info("Open-data CSV returned %d parcel rows", len(rows))
                        return rows
            except Exception as exc:
                log.debug("Open-data CSV %s: %s", url, exc)
        return None

    # ------------------------------------------------------------------
    def _build_index_from_rows(self, rows: List[Dict]) -> None:
        """
        Populate self._index from a list of dicts (DBF records, ArcGIS attrs, CSV rows).
        Keys tried: OWNER, OWN1, OWNERNAME – site addr: SITE_ADDR, SITEADDR –
        mail addr: ADDR_1, MAILADR1.
        """
        added = 0
        for row in rows:
            try:
                # ---- owner name ----
                owner = clean_str(
                    row.get("OWNER")
                    or row.get("OWN1")
                    or row.get("OWNERNAME")
                    or row.get("OWNER_NAME")
                    or ""
                )
                if not owner:
                    continue

                # ---- site / property address ----
                prop_addr = clean_str(
                    row.get("SITE_ADDR") or row.get("SITEADDR") or row.get("SITE_ADDRESS") or ""
                )
                prop_city = clean_str(row.get("SITE_CITY") or row.get("SITECITY") or "")
                prop_state = clean_str(row.get("SITE_STATE") or row.get("STATE") or "GA")
                prop_zip = clean_str(
                    row.get("SITE_ZIP") or row.get("SITEZIP") or row.get("ZIPCODE") or ""
                )

                # ---- mailing address ----
                mail_addr = clean_str(
                    row.get("ADDR_1") or row.get("MAILADR1") or row.get("MAIL_ADDR") or ""
                )
                mail_city = clean_str(
                    row.get("CITY") or row.get("MAILCITY") or row.get("MAIL_CITY") or ""
                )
                mail_state = clean_str(
                    row.get("STATE") or row.get("MAILSTATE") or "GA"
                )
                mail_zip = clean_str(
                    row.get("ZIP") or row.get("MAILZIP") or row.get("MAIL_ZIP") or ""
                )

                entry = {
                    "prop_address": prop_addr,
                    "prop_city": prop_city,
                    "prop_state": prop_state if prop_state else "GA",
                    "prop_zip": prop_zip,
                    "mail_address": mail_addr,
                    "mail_city": mail_city,
                    "mail_state": mail_state if mail_state else "GA",
                    "mail_zip": mail_zip,
                }

                for variant in name_variants(owner):
                    key = variant.upper().strip()
                    if key and key not in self._index:
                        self._index[key] = entry
                added += 1
            except Exception:
                pass  # never crash on bad parcel record

        log.info("Parcel index built: %d owner entries, %d name keys", added, len(self._index))

    # ------------------------------------------------------------------
    def load(self) -> None:
        """Try all parcel data sources in priority order."""
        if self._loaded:
            return

        # Check cached parcel data (avoid re-downloading on re-runs within same day)
        cache_path = Path(PARCEL_CACHE_PATH)
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < 24:
                log.info("Using cached parcel index (%0.1f hours old)", age_hours)
                try:
                    with open(cache_path) as f:
                        self._index = json.load(f)
                    self._loaded = True
                    return
                except Exception:
                    pass

        log.info("Loading Fulton County parcel data …")

        # ── Source 1: Property appraiser bulk download (DBF or ZIP) ──
        raw_bytes = None
        try:
            raw_bytes = self._fetch_property_appraiser_page()
        except Exception as exc:
            log.warning("Property appraiser bulk download failed: %s", exc)

        if raw_bytes:
            rows = self._parse_bulk_bytes(raw_bytes)
            if rows:
                self._build_index_from_rows(rows)
                self._loaded = True
                self._save_cache()
                return

        # ── Source 2: Open-data CSV ──
        try:
            csv_rows = self._fetch_open_data_csv()
            if csv_rows:
                self._build_index_from_rows(csv_rows)
                self._loaded = True
                self._save_cache()
                return
        except Exception as exc:
            log.warning("Open-data CSV failed: %s", exc)

        # ── Source 3: ArcGIS FeatureServer ──
        try:
            arcgis_rows = self._fetch_arcgis_paginated()
            if arcgis_rows:
                self._build_index_from_rows(arcgis_rows)
                self._loaded = True
                self._save_cache()
                return
        except Exception as exc:
            log.warning("ArcGIS parcel fallback failed: %s", exc)

        log.error("All parcel sources failed – address enrichment will be skipped")
        self._loaded = True  # mark loaded even if empty so we don't retry

    # ------------------------------------------------------------------
    def _parse_bulk_bytes(self, data: bytes) -> List[Dict]:
        """Parse ZIP→DBF or raw DBF bytes into a list of dicts."""
        rows: List[Dict] = []
        try:
            if data[:2] == b"PK":  # ZIP magic bytes
                with zipfile.ZipFile(BytesIO(data)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            log.info("Parsing DBF inside ZIP: %s", name)
                            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                                tmp.write(zf.read(name))
                                tmp_path = tmp.name
                            try:
                                table = DBF(tmp_path, encoding="latin-1", ignore_missing_memofile=True)
                                rows = [dict(rec) for rec in table]
                                log.info("DBF rows loaded: %d", len(rows))
                            finally:
                                os.unlink(tmp_path)
                            break
                        elif name.lower().endswith(".csv"):
                            with zf.open(name) as f:
                                reader = csv.DictReader(
                                    line.decode("latin-1") for line in f
                                )
                                rows = list(reader)
                            log.info("CSV-in-ZIP rows loaded: %d", len(rows))
                            break
            elif data[:3] in (b"\x03", b"\x83", b"\x8b"):  # DBF magic
                with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name
                try:
                    table = DBF(tmp_path, encoding="latin-1", ignore_missing_memofile=True)
                    rows = [dict(rec) for rec in table]
                    log.info("Raw DBF rows loaded: %d", len(rows))
                finally:
                    os.unlink(tmp_path)
        except Exception as exc:
            log.warning("Bulk bytes parse error: %s", exc)
        return rows

    # ------------------------------------------------------------------
    def _save_cache(self) -> None:
        try:
            Path(PARCEL_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
            with open(PARCEL_CACHE_PATH, "w") as f:
                json.dump(self._index, f)
            log.info("Parcel cache saved to %s", PARCEL_CACHE_PATH)
        except Exception as exc:
            log.warning("Parcel cache save failed: %s", exc)

    # ------------------------------------------------------------------
    def lookup(self, owner_name: str) -> Dict[str, str]:
        """Return address dict for owner_name, or empty dict if not found."""
        for variant in name_variants(owner_name):
            key = variant.upper().strip()
            if key in self._index:
                return self._index[key]
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# GSCCCA SCRAPER  (requests-based — no Playwright needed for searching)
#
# Confirmed URLs (from live browser session):
#   Main site:   https://www.gsccca.org
#   RE Search:   https://search.gsccca.org/RealEstate/InstrumentTypeSearch.aspx
#   Lien Search: https://search.gsccca.org/Lien/namesearch.asp
#   RE Detail:   https://search.gsccca.org/RealEstate/deedinfo.aspx
#
# Auth: GSCCCA_COOKIES secret (JSON from get_gsccca_cookie.py)
# ─────────────────────────────────────────────────────────────────────────────

GSCCCA_RE_SEARCH   = "https://search.gsccca.org/RealEstate/namesearch.asp"
GSCCCA_LIEN_SEARCH = "https://search.gsccca.org/Lien/namesearch.asp"

# Exact instrument dropdown values from the GSCCCA Real Estate Index
RE_INSTRUMENTS: Dict[str, str] = {
    "NOFC":   "DEED - FORECLOSURE",
    "TAXDEED":"TAX SALE DEED",
    "JUD":    "COURT ORDER",
    "LIEN":   "LIEN",
    "LNMECH": "MATERIALMANS LIEN",
    "PRO":    "ESTATE DOCUMENTATION",
    "PRO2":   "DEED - FROM ESTATE",
    "NOC":    "NOTICE",
    "LP":     "NOTICE",
    "RELLP":  "RELEASE",
    "SHRF":   "SHERIFF'S DEED",
}

# Lien Index instrument types
LIEN_INSTRUMENTS: Dict[str, str] = {
    "LNIRS":    "FEDERAL TAX LIEN",
    "LNFED":    "FEDERAL LIEN",
    "LNCORPTX": "STATE TAX LIEN",
    "LNHOA":    "CLAIM OF LIEN",
    "MEDLN":    "MEDICAID LIEN",
}


class ClerkScraper:
    """
    Scrapes GSCCCA using direct HTTP requests with injected session cookies.
    Real Estate uses the Premium Instrument Type Search.
    Liens use the Lien Index Name Search with % wildcard.
    No Playwright required for searching — only for the one-time cookie capture.
    """

    # ──────────────────────────────────────────────────────────────────────
    def _build_session(self) -> requests.Session:
        """Build an authenticated requests session from GSCCCA_COOKIES."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection":      "keep-alive",
        })

        if GSCCCA_COOKIES.strip():
            try:
                raw = json.loads(GSCCCA_COOKIES)
                if isinstance(raw, dict):
                    raw = [raw]
                for c in raw:
                    session.cookies.set(
                        c.get("name", ""), c.get("value", ""),
                        domain=c.get("domain", "search.gsccca.org"),
                        path=c.get("path", "/"),
                    )
                log.info("Session: injected %d cookie(s)", len(raw))
            except Exception as exc:
                log.error("Cookie injection error: %s", exc)
        elif GSCCCA_USERNAME and GSCCCA_PASSWORD:
            self._http_login(session)
        else:
            log.warning(
                "No GSCCCA auth. Run get_gsccca_cookie.py and set GSCCCA_COOKIES secret."
            )
        return session

    # ──────────────────────────────────────────────────────────────────────
    def _http_login(self, session: requests.Session) -> bool:
        """Fallback: attempt HTTP POST login to www.gsccca.org."""
        try:
            import cloudscraper
            s = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
        except ImportError:
            s = session

        try:
            r = s.get(GSCCCA_LOGIN_URL, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            payload = {}
            for inp in soup.find_all("input"):
                n = inp.get("name") or ""
                t = (inp.get("type") or "").lower()
                if n.startswith("__"):
                    payload[n] = inp.get("value", "")
                elif t in ("text", "email") and n:
                    payload[n] = GSCCCA_USERNAME
                elif t == "password" and n:
                    payload[n] = GSCCCA_PASSWORD
                elif t == "submit" and n:
                    payload[n] = inp.get("value", "Login")

            r2 = s.post(GSCCCA_LOGIN_URL, data=payload, timeout=30)
            if "login" not in r2.url.lower():
                session.cookies.update(s.cookies)
                log.info("HTTP login succeeded")
                return True
            log.warning("HTTP login: still on login page")
            return False
        except Exception as exc:
            log.error("HTTP login error: %s", exc)
            return False

    # ──────────────────────────────────────────────────────────────────────
    def _get_aspnet_fields(
        self, session: requests.Session, url: str
    ) -> Dict[str, str]:
        """GET a page and return its ASP.NET hidden form fields."""
        try:
            r = session.get(url, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            fields = {}
            for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR",
                         "__EVENTVALIDATION", "__EVENTTARGET", "__EVENTARGUMENT"]:
                el = soup.find("input", {"name": name})
                if el:
                    fields[name] = el.get("value", "")
            return fields
        except Exception as exc:
            log.warning("Could not fetch ASP.NET fields from %s: %s", url, exc)
            return {}

    # ──────────────────────────────────────────────────────────────────────
    @staticmethod
    def _normalise_date(raw: str) -> str:
        if not raw:
            return ""
        raw = clean_str(raw)
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y",
                    "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(raw[:20], fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", raw)
        if m:
            mo, dy, yr = m.group(1), m.group(2), m.group(3)
            if len(yr) == 2:
                yr = "20" + yr
            try:
                return datetime(int(yr), int(mo), int(dy)).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw

    # ──────────────────────────────────────────────────────────────────────
    def _search_re(
        self,
        session: requests.Session,
        instrument: str,
        county: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Search Real Estate Index via namesearch.asp (same endpoint as Lien).
        Uses % wildcard with instrument type selected — works without Premium
        and without IP restrictions.
        Form rule: short names (<=2 chars) require an instrument type selected.
        """
        all_records: List[Dict[str, Any]] = []
        fields = self._get_aspnet_fields(session, GSCCCA_RE_SEARCH)

        payload = {
            **fields,
            "PartyType":        "ALL",
            "InstrumentType":   instrument,
            "County":           county,
            "SearchName":       "%",
            "DateFrom":         start_date,
            "DateThru":         end_date,
            "Display":          "100",
            "TableDisplayType": "Regular",
            "SearchButton":     "Search",
        }

        page_num = 0
        current_url = GSCCCA_RE_SEARCH

        while page_num < MAX_PAGES_PER_DOCTYPE:
            page_num += 1
            try:
                if page_num == 1:
                    r = session.post(
                        GSCCCA_RE_SEARCH, data=payload, timeout=60,
                        headers={"Referer": GSCCCA_RE_SEARCH},
                    )
                else:
                    r = session.get(current_url, timeout=30)
                r.raise_for_status()
            except Exception as exc:
                log.error("RE search error page %d: %s", page_num, exc)
                break

            if "login" in r.url.lower():
                log.error("RE search: redirected to login – cookies expired")
                break

            soup = BeautifulSoup(r.text, "lxml")

            # Map instrument → our doc code
            doc_code = next(
                (k for k, v in RE_INSTRUMENTS.items() if v == instrument), "RE"
            )
            page_recs = self._parse_re_table(soup, doc_code, instrument, county)
            all_records.extend(page_recs)
            log.info("    Page %d → %d records", page_num, len(page_recs))

            if not page_recs:
                break

            # Pagination
            next_a = soup.find("a", string=re.compile(r"Next|>", re.I))
            if not next_a or not next_a.get("href"):
                break
            href = next_a["href"]
            current_url = href if href.startswith("http") else                           "https://search.gsccca.org" + href

        return all_records

    def _parse_re_table(
        self,
        soup: BeautifulSoup,
        doc_code: str,
        instrument: str,
        county: str,
    ) -> List[Dict[str, Any]]:
        """Parse Real Estate namesearch.asp Regular table results."""
        records: List[Dict[str, Any]] = []
        label, cat = DOC_TYPES.get(doc_code, (instrument, "RE"))

        # Find results table
        table = None
        for t in soup.find_all("table"):
            ths = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if len(ths) >= 3 and any(
                k in " ".join(ths) for k in ["book", "grantor", "date", "filed", "name"]
            ):
                table = t
                break

        if not table:
            # Check for "no records found" message
            body = soup.get_text()
            if re.search(r"no record|no result|0 record", body, re.I):
                log.debug("RE %s/%s: no records found", county, instrument)
            return []

        headers = [clean_str(th.get_text()) for th in table.find_all("th")]
        col_map: Dict[str, int] = {}
        for i, h in enumerate(headers):
            hl = h.lower()
            if "book" in hl:               col_map["book"]    = i
            elif "page" in hl:             col_map["page"]    = i
            elif "date" in hl or "filed" in hl: col_map["filed"] = i
            elif "grantor" in hl or "party" in hl or "name" in hl:
                col_map["grantor"] = i
            elif "grantee" in hl:          col_map["grantee"] = i
            elif "type" in hl:             col_map["type"]    = i
            elif "amount" in hl or "$" in hl: col_map["amount"] = i

        def cv(cells, f):
            idx = col_map.get(f)
            return clean_str(cells[idx].get_text()) if idx is not None and idx < len(cells) else ""

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                book = cv(cells, "book")
                pg   = cv(cells, "page")
                if not book:
                    continue
                doc_num = f"{book}/{pg}" if pg else book

                # Get view link
                clerk_url = ""
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        h = a["href"]
                        clerk_url = h if h.startswith("http") else                                     f"https://search.gsccca.org{h}"
                        break

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  cv(cells, "type") or label,
                    "doc_code":  doc_code,
                    "filed":     self._normalise_date(cv(cells, "filed")),
                    "grantor":   cv(cells, "grantor"),
                    "grantee":   cv(cells, "grantee"),
                    "legal":     "",
                    "amount":    cv(cells, "amount"),
                    "clerk_url": clerk_url,
                    "county":    county.title(),
                    "cat":       cat,
                    "cat_label": CATEGORY_LABELS.get(cat, cat),
                })
            except Exception:
                pass
        return records

    # ──────────────────────────────────────────────────────────────────────
    def _search_lien(
        self,
        session: requests.Session,
        doc_code: str,
        instrument: str,
        county: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """Search Lien Index by instrument type + county + date range."""
        fields = self._get_aspnet_fields(session, GSCCCA_LIEN_SEARCH)

        payload = {
            **fields,
            "PartyType":        "ALL",
            "InstrumentType":   instrument,
            "County":           county,
            "SearchName":       "%",
            "DateFrom":         start_date,
            "DateThru":         end_date,
            "Display":          "100",
            "TableDisplayType": "Regular",
            "SearchButton":     "Search",
        }

        try:
            r = session.post(
                GSCCCA_LIEN_SEARCH, data=payload, timeout=60,
                headers={"Referer": GSCCCA_LIEN_SEARCH},
            )
            r.raise_for_status()
        except Exception as exc:
            log.error("Lien search error: %s", exc)
            return []

        if "login" in r.url.lower():
            log.error("Lien search: redirected to login – cookies expired")
            return []

        soup = BeautifulSoup(r.text, "lxml")
        return self._parse_lien_table(soup, doc_code, instrument, county)

    # ──────────────────────────────────────────────────────────────────────
    def _parse_lien_table(
        self,
        soup: BeautifulSoup,
        doc_code: str,
        instrument: str,
        county: str,
    ) -> List[Dict[str, Any]]:
        """Parse lien index regular-style results table."""
        records: List[Dict[str, Any]] = []
        label, cat = DOC_TYPES.get(doc_code, (instrument, "LIEN"))

        # Find the results table
        table = None
        for t in soup.find_all("table"):
            ths = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if len(ths) >= 3 and any(
                k in " ".join(ths) for k in ["book", "grantor", "date", "filed"]
            ):
                table = t
                break

        if not table:
            return []

        headers = [clean_str(th.get_text()) for th in table.find_all("th")]
        col_map: Dict[str, int] = {}
        for i, h in enumerate(headers):
            hl = h.lower()
            if "book" in hl:    col_map["book"]  = i
            elif "page" in hl:  col_map["page"]  = i
            elif "date" in hl or "filed" in hl: col_map["filed"] = i
            elif "grantor" in hl: col_map["grantor"] = i
            elif "grantee" in hl: col_map["grantee"] = i
            elif "type" in hl:  col_map["type"]   = i
            elif "amount" in hl or "$" in hl: col_map["amount"] = i

        def cv(cells, f):
            idx = col_map.get(f)
            return clean_str(cells[idx].get_text()) if idx is not None and idx < len(cells) else ""

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                book = cv(cells, "book")
                pg   = cv(cells, "page")
                if not book:
                    continue
                doc_num = f"{book}/{pg}" if pg else book

                # Link
                clerk_url = ""
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        h = a["href"]
                        clerk_url = h if h.startswith("http") else \
                                    f"https://search.gsccca.org{h}"
                        break

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  cv(cells, "type") or label,
                    "doc_code":  doc_code,
                    "filed":     self._normalise_date(cv(cells, "filed")),
                    "grantor":   cv(cells, "grantor"),
                    "grantee":   cv(cells, "grantee"),
                    "legal":     "",
                    "amount":    cv(cells, "amount"),
                    "clerk_url": clerk_url,
                    "county":    county.title(),
                    "cat":       cat,
                    "cat_label": CATEGORY_LABELS.get(cat, cat),
                })
            except Exception:
                pass

        return records

    # ──────────────────────────────────────────────────────────────────────
    def scrape_all(
        self, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Main entry point. HTTP-based, no browser.
        Loops ACTIVE_COUNTIES x (RE_INSTRUMENTS + LIEN_INSTRUMENTS).
        """
        all_records: List[Dict[str, Any]] = []
        seen: set = set()
        session = self._build_session()

        log.info(
            "Scraping %d counties | %d RE + %d lien instrument types",
            len(ACTIVE_COUNTIES), len(RE_INSTRUMENTS), len(LIEN_INSTRUMENTS),
        )

        for c_idx, (county_name, county_id) in enumerate(ACTIVE_COUNTIES, 1):
            log.info("▶ County %d/%d: %s", c_idx, len(ACTIVE_COUNTIES), county_name)
            county_new = 0

            for doc_code, instrument in RE_INSTRUMENTS.items():
                log.info("  ┣━ RE %s – %s", doc_code, instrument)
                try:
                    recs = self._search_re(
                        session, instrument, county_name, start_date, end_date
                    )
                    for rec in recs:
                        key = f"{county_name}|{rec['doc_num']}"
                        if key not in seen and rec.get("doc_num"):
                            seen.add(key)
                            all_records.append(rec)
                            county_new += 1
                except Exception as exc:
                    log.error("RE %s/%s: %s", county_name, instrument, exc)
                time.sleep(0.5)

            for doc_code, instrument in LIEN_INSTRUMENTS.items():
                log.info("  ┣━ Lien %s – %s", doc_code, instrument)
                try:
                    recs = self._search_lien(
                        session, doc_code, instrument,
                        county_name, start_date, end_date
                    )
                    for rec in recs:
                        key = f"{county_name}|{rec['doc_num']}"
                        if key not in seen and rec.get("doc_num"):
                            seen.add(key)
                            all_records.append(rec)
                            county_new += 1
                except Exception as exc:
                    log.error("Lien %s/%s: %s", county_name, instrument, exc)
                time.sleep(0.5)

            log.info("  ✓ %s – %d new records", county_name, county_new)
            if c_idx < len(ACTIVE_COUNTIES):
                time.sleep(1.0)

        log.info(
            "Total: %d records across %d counties",
            len(all_records), len(ACTIVE_COUNTIES)
        )
        return all_records




# ─────────────────────────────────────────────────────────────────────────────
# LEAD SCORING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class LeadScorer:
    """
    Seller Score (0–100):
      Base 30
      +10 per motivating flag
      +20 if LP + NOFC both present for same owner
      +15 if amount > $100 k
      +10 if amount > $50 k
      +5  if filed within this week (LOOKBACK_DAYS)
      +5  if property address resolved
    Max cap: 100
    """

    CUTOFF_DAYS = LOOKBACK_DAYS

    # doc_code -> list of flag labels it triggers
    FLAG_MAP: Dict[str, List[str]] = {
        "LP":       [FLAG_LP],
        "NOFC":     [FLAG_PREFC],
        "TAXDEED":  [FLAG_TAXLIEN, "Tax deed / tax sale"],
        "JUD":      [FLAG_JUD],
        "CCJ":      [FLAG_JUD],
        "DRJUD":    [FLAG_JUD],
        "LNCORPTX": [FLAG_TAXLIEN],
        "LNIRS":    [FLAG_TAXLIEN],
        "LNFED":    [FLAG_TAXLIEN],
        "LN":       [FLAG_MECH],
        "LNMECH":   [FLAG_MECH],
        "LNHOA":    ["HOA lien"],
        "MEDLN":    ["Medicaid lien"],
        "PRO":      [FLAG_PROBATE],
        "NOC":      [],
        "RELLP":    [],
    }

    @staticmethod
    def _is_new_this_week(filed_str: str) -> bool:
        if not filed_str:
            return False
        try:
            filed_dt = datetime.strptime(filed_str, "%Y-%m-%d")
            return (datetime.today() - filed_dt).days <= LeadScorer.CUTOFF_DAYS
        except ValueError:
            return False

    @classmethod
    def score(
        cls,
        record: Dict[str, Any],
        owner_doc_codes: Optional[List[str]] = None,
    ) -> Tuple[int, List[str]]:
        """
        Compute (score, flags) for a single record.
        owner_doc_codes: all doc codes filed under the same owner (for combo bonus).
        """
        flags: List[str] = []
        score = 30  # base

        doc_code = record.get("doc_code", "")
        owner_upper = (record.get("grantor") or "").upper()

        # ── Flags from doc type ──
        for flag in cls.FLAG_MAP.get(doc_code, []):
            if flag not in flags:
                flags.append(flag)

        # ── LLC / corp detection ──
        corp_keywords = [" LLC", " INC", " CORP", " LTD", " L.L.C", " CO.", " LP ", " L.P."]
        if any(kw in owner_upper for kw in corp_keywords):
            if FLAG_LLC not in flags:
                flags.append(FLAG_LLC)

        # ── New this week ──
        if cls._is_new_this_week(record.get("filed", "")):
            if FLAG_NEW not in flags:
                flags.append(FLAG_NEW)

        # ── Amount bonuses ──
        amount = parse_amount(record.get("amount", ""))

        # ── Score calculation ──
        score += len(flags) * 10

        # LP + FC combo bonus (only if owner_doc_codes provided)
        if owner_doc_codes:
            has_lp = "LP" in owner_doc_codes
            has_fc = "NOFC" in owner_doc_codes or "TAXDEED" in owner_doc_codes
            if has_lp and has_fc:
                score += 20

        if amount:
            if amount > 100_000:
                score += 15
            elif amount > 50_000:
                score += 10

        if FLAG_NEW in flags:
            score += 5

        if record.get("prop_address"):
            score += 5

        return min(score, 100), flags


# ─────────────────────────────────────────────────────────────────────────────
# RECORD ENRICHMENT & ASSEMBLY
# ─────────────────────────────────────────────────────────────────────────────

def enrich_records(
    raw_records: List[Dict[str, Any]],
    parcel_lookup: ParcelLookup,
) -> List[Dict[str, Any]]:
    """
    Merge raw clerk records with parcel data.
    Computes seller score and flags.
    Returns list of fully enriched record dicts.
    """
    # Group doc codes by owner for combo bonuses
    owner_codes: Dict[str, List[str]] = {}
    for rec in raw_records:
        owner = clean_str(rec.get("grantor", "")).upper()
        if owner:
            owner_codes.setdefault(owner, []).append(rec.get("doc_code", ""))

    enriched: List[Dict[str, Any]] = []
    for rec in raw_records:
        try:
            owner = clean_str(rec.get("grantor", ""))
            owner_up = owner.upper()

            # Parcel lookup
            parcel = parcel_lookup.lookup(owner)

            # Try to extract address from legal description if no parcel match
            prop_addr = parcel.get("prop_address", "")
            if not prop_addr:
                legal = clean_str(rec.get("legal", ""))
                addr_match = re.search(
                    r"\d{1,5}\s+[A-Z][A-Za-z\s]{2,40}(?:ST|AVE|RD|DR|LN|BLVD|CT|WAY|PL|CIR)\b",
                    legal, re.I
                )
                if addr_match:
                    prop_addr = addr_match.group(0)

            amount_raw = clean_str(rec.get("amount", ""))
            amount_val = parse_amount(amount_raw)

            codes_for_owner = owner_codes.get(owner_up, [])
            score, flags = LeadScorer.score(
                {**rec, "prop_address": prop_addr},
                owner_doc_codes=codes_for_owner,
            )

            e = {
                "doc_num":      clean_str(rec.get("doc_num", "")),
                "doc_type":     clean_str(rec.get("doc_type", "")),
                "filed":        clean_str(rec.get("filed", "")),
                "cat":          clean_str(rec.get("cat", "")),
                "cat_label":    clean_str(rec.get("cat_label", "")),
                "owner":        owner,
                "grantee":      clean_str(rec.get("grantee", "")),
                "amount":       f"${amount_val:,.2f}" if amount_val else amount_raw,
                "legal":        clean_str(rec.get("legal", "")),
                # Property address (from parcel or legal description)
                "prop_address": prop_addr,
                "prop_city":    parcel.get("prop_city", ""),
                "prop_state":   parcel.get("prop_state", "GA"),
                "prop_zip":     parcel.get("prop_zip", ""),
                # Mailing address
                "mail_address": parcel.get("mail_address", ""),
                "mail_city":    parcel.get("mail_city", ""),
                "mail_state":   parcel.get("mail_state", "GA"),
                "mail_zip":     parcel.get("mail_zip", ""),
                # Links & metadata
                "clerk_url":    clean_str(rec.get("clerk_url", "")),
                "county":       clean_str(rec.get("county", "")),
                "flags":        flags,
                "score":        score,
            }
            enriched.append(e)
        except Exception as exc:
            log.warning("Enrichment failed for record %s: %s", rec.get("doc_num"), exc)

    # Sort by score descending, then filed date descending
    enriched.sort(key=lambda r: (-r["score"], r.get("filed", "") or ""), reverse=False)
    enriched.sort(key=lambda r: r["score"], reverse=True)
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT WRITERS
# ─────────────────────────────────────────────────────────────────────────────

def write_json_outputs(
    records: List[Dict[str, Any]],
    start_date: str,
    end_date: str,
) -> None:
    """Write records.json to all configured output paths."""
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Georgia GSCCCA (%d counties)" % len(ACTIVE_COUNTIES),
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }
    for path_str in OUTPUT_PATHS:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            log.info("Wrote %d records to %s", len(records), path)
        except Exception as exc:
            log.error("Failed to write %s: %s", path, exc)


def write_ghl_csv(records: List[Dict[str, Any]]) -> None:
    """Export GHL-ready CSV for CRM import."""
    path = Path(GHL_CSV_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    COLUMNS = [
        "First Name",
        "Last Name",
        "County",
        "Mailing Address",
        "Mailing City",
        "Mailing State",
        "Mailing Zip",
        "Property Address",
        "Property City",
        "Property State",
        "Property Zip",
        "Lead Type",
        "Document Type",
        "Date Filed",
        "Document Number",
        "Amount/Debt Owed",
        "Seller Score",
        "Motivated Seller Flags",
        "Source",
        "Public Records URL",
    ]

    def split_name(full: str) -> Tuple[str, str]:
        """Best-effort first/last split from full name string."""
        full = clean_str(full)
        if not full:
            return "", ""
        # "LAST, FIRST" → FIRST, LAST
        if "," in full:
            parts = [p.strip() for p in full.split(",", 1)]
            return parts[1], parts[0]
        words = full.split()
        if len(words) == 1:
            return "", words[0]
        return " ".join(words[:-1]), words[-1]

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            for rec in records:
                first, last = split_name(rec.get("owner", ""))
                writer.writerow({
                    "First Name":            first,
                    "Last Name":             last,
                    "County":                rec.get("county", ""),
                    "Mailing Address":       rec.get("mail_address", ""),
                    "Mailing City":          rec.get("mail_city", ""),
                    "Mailing State":         rec.get("mail_state", "GA"),
                    "Mailing Zip":           rec.get("mail_zip", ""),
                    "Property Address":      rec.get("prop_address", ""),
                    "Property City":         rec.get("prop_city", ""),
                    "Property State":        rec.get("prop_state", "GA"),
                    "Property Zip":          rec.get("prop_zip", ""),
                    "Lead Type":             rec.get("cat_label", ""),
                    "Document Type":         rec.get("doc_type", ""),
                    "Date Filed":            rec.get("filed", ""),
                    "Document Number":       rec.get("doc_num", ""),
                    "Amount/Debt Owed":      rec.get("amount", ""),
                    "Seller Score":          rec.get("score", 0),
                    "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                    "Source": (
                        "Fulton County Superior Court Clerk (search.gsccca.org)"
                    ),
                    "Public Records URL":    rec.get("clerk_url", ""),
                })
        log.info("GHL CSV written: %s (%d rows)", path, len(records))
    except Exception as exc:
        log.error("GHL CSV write failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    start_date, end_date = date_range_strings(LOOKBACK_DAYS)
    log.info(
        "Starting Georgia Motivated Seller Scraper | "
        "counties: %s | date range: %s → %s | lookback_days: %d",
        ", ".join(n for n,_ in ACTIVE_COUNTIES), start_date, end_date, LOOKBACK_DAYS,
    )

    # ── Step 1: Load parcel data ──
    parcel = ParcelLookup()
    try:
        parcel.load()
    except Exception as exc:
        log.error("Parcel load failed (continuing without address enrichment): %s", exc)

    # ── Step 2: Scrape GSCCCA via HTTP requests ──
    raw_records: List[Dict[str, Any]] = []
    try:
        scraper = ClerkScraper()
        raw_records = await asyncio.get_event_loop().run_in_executor(
            None, lambda: scraper.scrape_all(start_date, end_date)
        )
    except Exception as exc:
        log.error("Clerk scraper failed: %s\n%s", exc, traceback.format_exc())

    log.info("Raw records from clerk portal: %d", len(raw_records))

    # ── Step 3: Enrich with parcel data + scoring ──
    enriched = enrich_records(raw_records, parcel)
    log.info(
        "Enriched records: %d total | %d with property address",
        len(enriched),
        sum(1 for r in enriched if r.get("prop_address")),
    )

    # ── Step 4: Persist outputs ──
    write_json_outputs(enriched, start_date, end_date)
    write_ghl_csv(enriched)

    # ── Summary ──
    log.info("=" * 60)
    log.info("SCRAPE COMPLETE")
    log.info("  Total records:    %d", len(enriched))
    log.info("  With address:     %d", sum(1 for r in enriched if r.get("prop_address")))
    log.info(
        "  High-score (≥70): %d",
        sum(1 for r in enriched if r.get("score", 0) >= 70),
    )
    log.info("  GHL CSV:          %s", GHL_CSV_PATH)
    log.info("  JSON outputs:     %s", ", ".join(OUTPUT_PATHS))
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
