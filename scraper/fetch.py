#!/usr/bin/env python3
"""
Fulton County GA – Motivated Seller Lead Scraper
=================================================
Playwright scrapes the Superior Court Clerk portal for LP, NOFC, TAXDEED,
judgments, liens, probate, NOC, and RELLP filings from the last LOOKBACK_DAYS.
Enriches records with property + mailing address from the County Appraiser
bulk parcel DBF. Outputs records.json (dashboard + data) and a GHL-ready CSV.

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
PAGE_TIMEOUT: int = 45_000  # ms
NAV_TIMEOUT: int = 60_000  # ms
MAX_PAGES_PER_DOCTYPE: int = 50  # safety cap

CLERK_BASE_URL = "https://search.gsccca.org"
CLERK_SEARCH_URL = "https://search.gsccca.org/RealEstateIndex.aspx"

_COUNTIES_ENV = os.getenv("COUNTIES", "FULTON,CLAYTON,HOUSTON,COBB,GWINNETT,DOUGLAS")

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

PARCEL_BASE_URL = "https://fultoncountypropertyappraiser.org"
PARCEL_SEARCH_URL = "https://fultoncountypropertyappraiser.org/property-search/"

GSCCCA_USERNAME: str = os.getenv("GSCCCA_USERNAME", "")
GSCCCA_PASSWORD: str = os.getenv("GSCCCA_PASSWORD", "")
GSCCCA_LOGIN_URL: str = "https://search.gsccca.org/Login.aspx"

OUTPUT_PATHS: List[str] = ["dashboard/records.json", "data/records.json"]
GHL_CSV_PATH: str = "data/ghl_export.csv"
PARCEL_CACHE_PATH: str = "data/parcel_cache.json"

DOC_TYPES: Dict[str, Tuple[str, str]] = {
    "LP": ("Lis Pendens", "LP"),
    "NOFC": ("Notice of Foreclosure", "NOFC"),
    "TAXDEED": ("Tax Deed", "TAXDEED"),
    "JUD": ("Judgment", "JUD"),
    "CCJ": ("Certified Judgment", "JUD"),
    "DRJUD": ("Domestic Judgment", "JUD"),
    "LNCORPTX": ("Corp Tax Lien", "LIEN"),
    "LNIRS": ("IRS Lien", "LIEN"),
    "LNFED": ("Federal Lien", "LIEN"),
    "LN": ("Lien", "LIEN"),
    "LNMECH": ("Mechanic Lien", "LIEN"),
    "LNHOA": ("HOA Lien", "LIEN"),
    "MEDLN": ("Medicaid Lien", "LIEN"),
    "PRO": ("Probate", "PRO"),
    "NOC": ("Notice of Commencement", "NOC"),
    "RELLP": ("Release Lis Pendens", "RELLP"),
}

CATEGORY_LABELS: Dict[str, str] = {
    "LP": "Lis Pendens",
    "NOFC": "Pre-foreclosure",
    "TAXDEED": "Tax Deed / Tax Sale",
    "JUD": "Judgment",
    "LIEN": "Lien",
    "PRO": "Probate / Estate",
    "NOC": "Notice of Commencement",
    "RELLP": "Release – Lis Pendens",
}

FLAG_LP = "Lis pendens"
FLAG_PREFC = "Pre-foreclosure"
FLAG_JUD = "Judgment lien"
FLAG_TAXLIEN = "Tax lien"
FLAG_MECH = "Mechanic lien"
FLAG_PROBATE = "Probate / estate"
FLAG_LLC = "LLC / corp owner"
FLAG_NEW = "New this week"

LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT, stream=sys.stdout)
log = logging.getLogger("fulton-scraper")


def retry(max_tries: int = MAX_RETRIES, delay: float = RETRY_DELAY, exc=(Exception,)):
    def deco(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_tries + 1):
                try:
                    return await fn(*args, **kwargs)
                except exc as e:
                    last_exc = e
                    log.warning("Attempt %d/%d failed for %s: %s", attempt, max_tries, fn.__name__, e)
                    if attempt < max_tries:
                        await asyncio.sleep(delay * attempt)
            log.error("All %d attempts failed for %s", max_tries, fn.__name__)
            raise last_exc
        return wrapper
    return deco


def sync_retry(max_tries: int = MAX_RETRIES, delay: float = RETRY_DELAY, exc=(Exception,)):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_tries + 1):
                try:
                    return fn(*args, **kwargs)
                except exc as e:
                    last_exc = e
                    log.warning("Attempt %d/%d failed for %s: %s", attempt, max_tries, fn.__name__, e)
                    if attempt < max_tries:
                        time.sleep(delay * attempt)
            raise last_exc
        return wrapper
    return deco


def clean_str(s: Any) -> str:
    if s is None:
        return ""
    txt = unicodedata.normalize("NFKC", str(s)).strip()
    return re.sub(r"\s{2,}", " ", txt)


def parse_amount(raw: str) -> Optional[float]:
    if not raw:
        return None
    digits = re.sub(r"[^\d.]", "", raw)
    try:
        return float(digits) if digits else None
    except ValueError:
        return None


def date_range_strings(lookback: int = LOOKBACK_DAYS) -> Tuple[str, str]:
    today = datetime.today()
    start = today - timedelta(days=lookback)
    return start.strftime("%m/%d/%Y"), today.strftime("%m/%d/%Y")


def name_variants(full_name: str) -> List[str]:
    if not full_name:
        return []
    name = clean_str(full_name).upper()
    variants = {name}
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        variants.add(f"{parts[1]} {parts[0]}")
        variants.add(f"{parts[0]} {parts[1]}")
    else:
        words = name.split()
        if len(words) >= 2:
            variants.add(f"{words[-1]} {' '.join(words[:-1])}")
            variants.add(f"{words[-1]}, {' '.join(words[:-1])}")
    return list(variants)


class ParcelLookup:
    def __init__(self) -> None:
        self._index: Dict[str, Dict[str, str]] = {}
        self._loaded = False

    @sync_retry(max_tries=MAX_RETRIES, exc=(Exception,))
    def _fetch_property_appraiser_page(self) -> Optional[bytes]:
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (compatible; PropstorBot/1.0; +https://propstor.com)"
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
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if any(href.lower().endswith(ext) for ext in (".zip", ".dbf", ".csv")):
                full_url = urljoin(page_url, href)
                r = session.get(full_url, timeout=120, stream=True)
                if r.ok:
                    return r.content
        form = soup.find("form")
        if form:
            viewstate_input = soup.find("input", {"name": "__VIEWSTATE"})
            eventval_input = soup.find("input", {"name": "__EVENTVALIDATION"})
            viewstate = viewstate_input["value"] if viewstate_input else ""
            eventval = eventval_input["value"] if eventval_input else ""
            action = form.get("action", page_url)
            if not action.startswith("http"):
                action = urljoin(page_url, action)
            for btn in soup.find_all(["input", "button", "a"], string=re.compile(r"(download|parcel|bulk|data|export)", re.I)):
                event_target = btn.get("name", btn.get("id", ""))
                if not event_target:
                    continue
                payload = {"__EVENTTARGET": event_target, "__EVENTARGUMENT": "", "__VIEWSTATE": viewstate, "__EVENTVALIDATION": eventval}
                try:
                    r = session.post(action, data=payload, timeout=120, stream=True)
                    ct = r.headers.get("Content-Type", "")
                    if r.ok and ("zip" in ct or "octet" in ct or "dbf" in ct):
                        return r.content
                except Exception as exc:
                    log.debug("DoPostBack %s failed: %s", event_target, exc)
        return None

    def _fetch_arcgis_paginated(self) -> List[Dict]:
        base = "https://services.arcgis.com/gXbFIzHRtHGMRJgj/arcgis/rest/services/Fulton_Parcels/FeatureServer/0/query"
        records: List[Dict] = []
        offset = 0
        page_size = 2000
        session = requests.Session()
        while True:
            params = {"where": "1=1", "outFields": "OWNER,OWN1,SITE_ADDR,SITEADDR,SITE_CITY,SITE_ZIP,ADDR_1,MAILADR1,CITY,MAILCITY,STATE,ZIP,MAILZIP,PARID", "f": "json", "resultRecordCount": page_size, "resultOffset": offset, "returnGeometry": "false"}
            try:
                r = session.get(base, params=params, timeout=60)
                data = r.json()
                features = data.get("features", [])
                if not features:
                    break
                records.extend(f["attributes"] for f in features)
                if len(features) < page_size:
                    break
                offset += page_size
            except Exception as exc:
                log.warning("ArcGIS parcel fetch failed at offset %d: %s", offset, exc)
                break
        return records

    def _fetch_open_data_csv(self) -> Optional[List[Dict]]:
        candidates = ["https://opendata.fultoncountyga.gov/api/download/v1/items/fulton-county-parcels/csv", "https://opendata.fultoncountyga.gov/datasets/fulton-county-parcels_0.csv"]
        session = requests.Session()
        for url in candidates:
            try:
                r = session.get(url, timeout=120, stream=True)
                if r.ok and "text/csv" in r.headers.get("Content-Type", ""):
                    lines_data = r.text.splitlines()
                    reader = csv.DictReader(lines_data)
                    rows = list(reader)
                    if rows:
                        return rows
            except Exception as exc:
                log.debug("Open-data CSV %s: %s", url, exc)
        return None

    def _build_index_from_rows(self, rows: List[Dict]) -> None:
        added = 0
        for row in rows:
            try:
                owner = clean_str(row.get("OWNER") or row.get("OWN1") or row.get("OWNERNAME") or row.get("OWNER_NAME") or "")
                if not owner:
                    continue
                prop_addr = clean_str(row.get("SITE_ADDR") or row.get("SITEADDR") or row.get("SITE_ADDRESS") or "")
                prop_city = clean_str(row.get("SITE_CITY") or row.get("SITECITY") or "")
                prop_state = clean_str(row.get("SITE_STATE") or row.get("STATE") or "GA")
                prop_zip = clean_str(row.get("SITE_ZIP") or row.get("SITEZIP") or row.get("ZIPCODE") or "")
                mail_addr = clean_str(row.get("ADDR_1") or row.get("MAILADR1") or row.get("MAIL_ADDR") or "")
                mail_city = clean_str(row.get("CITY") or row.get("MAILCITY") or row.get("MAIL_CITY") or "")
                mail_state = clean_str(row.get("STATE") or row.get("MAILSTATE") or "GA")
                mail_zip = clean_str(row.get("ZIP") or row.get("MAILZIP") or row.get("MAIL_ZIP") or "")
                entry = {"prop_address": prop_addr, "prop_city": prop_city, "prop_state": prop_state if prop_state else "GA", "prop_zip": prop_zip, "mail_address": mail_addr, "mail_city": mail_city, "mail_state": mail_state if mail_state else "GA", "mail_zip": mail_zip}
                for variant in name_variants(owner):
                    key = variant.upper().strip()
                    if key and key not in self._index:
                        self._index[key] = entry
                        added += 1
            except Exception:
                pass
        log.info("Parcel index built: %d entries", added)

    def load(self) -> None:
        if self._loaded:
            return
        cache_path = Path(PARCEL_CACHE_PATH)
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < 24:
                try:
                    with open(cache_path) as f:
                        self._index = json.load(f)
                    self._loaded = True
                    return
                except Exception:
                    pass
        log.info("Loading Fulton County parcel data …")
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
        try:
            csv_rows = self._fetch_open_data_csv()
            if csv_rows:
                self._build_index_from_rows(csv_rows)
                self._loaded = True
                self._save_cache()
                return
        except Exception as exc:
            log.warning("Open-data CSV failed: %s", exc)
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
        self._loaded = True

    def _parse_bulk_bytes(self, data: bytes) -> List[Dict]:
        rows: List[Dict] = []
        try:
            if data[:2] == b"PK":
                with zipfile.ZipFile(BytesIO(data)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                                tmp.write(zf.read(name))
                                tmp_path = tmp.name
                            try:
                                table = DBF(tmp_path, encoding="latin-1", ignore_missing_memofile=True)
                                rows = [dict(rec) for rec in table]
                            finally:
                                os.unlink(tmp_path)
                            break
                        elif name.lower().endswith(".csv"):
                            with zf.open(name) as f:
                                reader = csv.DictReader(line.decode("latin-1") for line in f)
                                rows = list(reader)
                            break
            elif data[:3] in (b"\x03", b"\x83", b"\x8b"):
                with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name
                try:
                    table = DBF(tmp_path, encoding="latin-1", ignore_missing_memofile=True)
                    rows = [dict(rec) for rec in table]
                finally:
                    os.unlink(tmp_path)
        except Exception as exc:
            log.warning("Bulk bytes parse error: %s", exc)
        return rows

    def _save_cache(self) -> None:
        try:
            Path(PARCEL_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
            with open(PARCEL_CACHE_PATH, "w") as f:
                json.dump(self._index, f)
        except Exception as exc:
            log.warning("Parcel cache save failed: %s", exc)

    def lookup(self, owner_name: str) -> Dict[str, str]:
        for variant in name_variants(owner_name):
            key = variant.upper().strip()
            if key in self._index:
                return self._index[key]
        return {}


class ClerkScraper:
    SEARCH_URL = "https://search.gsccca.org/RealEstateIndex.aspx"
    GSCCCA_INSTRUMENT_MAP: Dict[str, List[str]] = {
        "LP": ["LP", "LIS PENDENS", "Lis Pendens"],
        "NOFC": ["NOFC","NOTICE OF FORECLOSURE","Notice of Foreclosure","NF","NOTICEOFFORECLOS"],
        "TAXDEED": ["TAXD","TAX DEED","Tax Deed","TD","TAXDEED"],
        "JUD": ["JUD","JUDGMENT","Judgment","JUDG","J"],
        "CCJ": ["CCJ","CERTIFIED COPY JUDGMENT","Certified Copy Judgment","CJ"],
        "DRJUD": ["DRJUD","DOMESTIC RELATIONS JUDGMENT","Domestic Relations Judgment","DR"],
        "LNCORPTX": ["LNCORPTX","CORP TAX LIEN","Corporate Tax Lien","FTL","STATE TAX LIEN","LNST"],
        "LNIRS": ["LNIRS","IRS LIEN","Federal Tax Lien","LNFED","FLN","FEDERAL TAX LIEN"],
        "LNFED": ["LNFED","FEDERAL LIEN","FEDERAL TAX LIEN","FTL"],
        "LN": ["LN","LIEN"],
        "LNMECH": ["LNMECH","MATERIALMAN'S LIEN","MECHANIC'S LIEN","ML","MATERIALMAN","Materialman"],
        "LNHOA": ["LNHOA","HOA LIEN","HOMEOWNERS ASSOC LIEN"],
        "MEDLN": ["MEDLN","MEDICAID LIEN","MED LIEN"],
        "PRO": ["PRO","PROBATE","Letters Testamentary","Letters of Administration","LT","LA"],
        "NOC": ["NOC","NOTICE OF COMMENCEMENT","Notice of Commencement"],
        "RELLP": ["RELLP","RELEASE LIS PENDENS","Release Lis Pendens","RLP"],
    }

    def __init__(self, browser: Browser) -> None:
        self.browser = browser

    async def _new_page(self) -> Tuple["BrowserContext", Page]:
        ctx = await self.browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        )
        ctx.set_default_timeout(PAGE_TIMEOUT)
        ctx.set_default_navigation_timeout(NAV_TIMEOUT)
        page = await ctx.new_page()
        return ctx, page

    @staticmethod
    def _normalise_date(raw: str) -> str:
        if not raw:
            return ""
        raw = clean_str(raw)
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw[:20], fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", raw)
        if m:
            mo, dy, yr = m.group(1), m.group(2), m.group(3)
            if len(yr) == 2:
                yr = "20" + yr if int(yr) < 50 else "19" + yr
            try:
                return datetime(int(yr), int(mo), int(dy)).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return raw

    async def _login(self, page: Page) -> bool:
        """
        Authenticate with GSCCCA. Exhaustive selector list + logs ALL inputs on the page.
        NEVER raises – returns True on success, False on any failure.
        Saves debug screenshot to /tmp/gsccca_login_debug.png on failure.
        """
        if not GSCCCA_USERNAME or not GSCCCA_PASSWORD:
            log.warning("GSCCCA_USERNAME / GSCCCA_PASSWORD not set. Scraper will run without auth.")
            return False

        log.info("Logging in to GSCCCA as %s …", GSCCCA_USERNAME)
        try:
            # Navigate to login page
            await page.goto(GSCCCA_LOGIN_URL, wait_until="domcontentloaded")

            # Wait for at least one <input> to appear
            try:
                await page.wait_for_selector("input", timeout=15000)
            except Exception:
                log.warning("No <input> elements appeared within 15s on login page")

            await page.wait_for_load_state("networkidle", timeout=15000)

            page_url = page.url
            page_title = await page.title()
            log.info("Login page URL after load: %s | title: %s", page_url, page_title)

            # Log ALL inputs on the page for debugging
            all_inputs = await page.locator("input").all()
            log.info("Found %d input element(s) on login page:", len(all_inputs))
            for inp in all_inputs:
                try:
                    inp_id   = await inp.get_attribute("id")   or ""
                    inp_name = await inp.get_attribute("name") or ""
                    inp_type = await inp.get_attribute("type") or ""
                    inp_ph   = await inp.get_attribute("placeholder") or ""
                    log.info("  INPUT id=%r name=%r type=%r placeholder=%r", inp_id, inp_name, inp_type, inp_ph)
                except Exception:
                    pass

            # Log iframes
            frames = page.frames
            log.info("Page has %d frame(s) (including main):", len(frames))
            for fi, frame in enumerate(frames):
                log.info("  Frame %d: url=%s", fi, frame.url)

            # Check if already logged in
            if "Login.aspx" not in page.url and "login.aspx" not in page.url:
                log.info("Already authenticated (%s)", page.url)
                return True

            # ── Exhaustive username selectors ──
            user_selectors = [
                # Standard GSCCCA field names
                "input#txtUserName",    "input[name='txtUserName']",
                "input#UserName",       "input[name='UserName']",
                "input#username",       "input[name='username']",
                "input#Username",       "input[name='username']",
                # ASP.NET ContentPlaceholder patterns
                "input[id$='txtUserName']",  "input[id$='UserName']",
                "input[id$='Username']",     "input[id$='UserName']",
                # By type/placeholder
                "input[type='email']",
                "input[placeholder*='user' i]",
                "input[placeholder*='email' i]",
                "input[placeholder*='login' i]",
                "input[autocomplete='username']",
                "input[autocomplete='email']",
                # Generic fallback – first visible text input
                "input[type='text']:visible",
                "input:not([type='hidden']):not([type='password']):not([type='checkbox']):not([type='submit']):not([type='button']):visible",
            ]

            user_el = None
            for sel in user_selectors:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible(timeout=1500):
                        user_el = el
                        log.info("Username field found via: %s", sel)
                        break
                except Exception:
                    pass

            if user_el is None:
                log.error("Could not find username field after trying %d selectors", len(user_selectors))
                await page.screenshot(path="/tmp/gsccca_login_debug.png", full_page=True)
                return False  # NEVER raise

            await user_el.triple_click()
            await user_el.fill(GSCCCA_USERNAME)
            log.info("Username filled: %s", GSCCCA_USERNAME)

            # ── Password selectors ──
            pass_selectors = [
                "input#txtPassword",         "input[name='txtPassword']",
                "input#Password",            "input[name='Password']",
                "input#password",            "input[name='password']",
                "input[id$='txtPassword']",  "input[id$='Password']",
                "input[type='password']",
                "input[autocomplete='current-password']",
                "input[placeholder*='pass' i]",
            ]

            pass_el = None
            for sel in pass_selectors:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible(timeout=1500):
                        pass_el = el
                        log.info("Password field found via: %s", sel)
                        break
                except Exception:
                    pass

            if pass_el is None:
                log.error("Could not find password field")
                await page.screenshot(path="/tmp/gsccca_login_debug.png", full_page=True)
                return False  # NEVER raise

            await pass_el.triple_click()
            await pass_el.fill(GSCCCA_PASSWORD)
            log.info("Password filled")

            # ── Submit selectors ──
            submit_selectors = [
                "input#btnLogin",            "input[name='btnLogin']",
                "button#btnLogin",
                "input#btnSubmit",           "input[name='btnSubmit']",
                "input[id$='btnLogin']",     "input[id$='btnSubmit']",
                "input[value='Login']",      "input[value='Log In']",
                "input[value='Submit']",     "input[value='Sign In']",
                "button:text('Login')",      "button:text('Log In')",
                "button:text('Sign In')",    "button:text('Submit')",
                "input[type='submit']",      "button[type='submit']",
            ]

            submitted = False
            for sel in submit_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=1500):
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=25000)
                        submitted = True
                        log.info("Login submitted via: %s", sel)
                        break
                except Exception:
                    pass

            if not submitted:
                log.info("No submit button found – pressing Enter")
                await page.keyboard.press("Enter")
                await page.wait_for_load_state("networkidle", timeout=15000)

            post_url = page.url
            page_text = (await page.inner_text("body")).lower()
            log.info("Post-login URL: %s", post_url)

            for err in ["invalid username", "invalid password", "incorrect", "login failed",
                        "authentication failed", "access denied", "please try again",
                        "unable to log", "account not found"]:
                if err in page_text:
                    log.error("GSCCCA login rejected: '%s'. Check GSCCCA_USERNAME/GSCCCA_PASSWORD secrets.", err)
                    await page.screenshot(path="/tmp/gsccca_login_debug.png", full_page=True)
                    return False  # NEVER raise

            if "Login.aspx" in post_url or "login.aspx" in post_url:
                log.error("Still on login page after submit. URL: %s", post_url)
                await page.screenshot(path="/tmp/gsccca_login_debug.png", full_page=True)
                return False  # NEVER raise

            log.info("✅ GSCCCA login successful → %s", post_url)
            return True

        except Exception as exc:
            log.error("GSCCCA _login() uncaught exception: %s", exc)
            try:
                await page.screenshot(path="/tmp/gsccca_login_debug.png", full_page=True)
            except Exception:
                pass
            return False  # NEVER raise

    async def _dismiss_modals(self, page: Page) -> None:
        for sel in ["input[value='I Agree']","button:text('I Agree')","button:text('Accept')","a:text('I Agree')","#btnAgree","[id*='Agree']","[id*='agree']","button:text('OK')","button:text('Continue')"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=1500):
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=6000)
                    log.info("Dismissed GSCCCA modal via: %s", sel)
                    break
            except Exception:
                pass

    async def _load_search_page(self, page: Page) -> bool:
        logged_in = await self._login(page)
        try:
            await page.goto(self.SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as exc:
            log.error("Failed to load GSCCCA search page post-login: %s", exc)
            return False
        if "Login.aspx" in page.url or "login.aspx" in page.url:
            log.warning("GSCCCA showing login page – credentials may be wrong. Attempting to scrape anyway.")
        await self._dismiss_modals(page)
        status = "authenticated" if logged_in else "UNAUTHENTICATED"
        log.info("GSCCCA search page ready [%s]", status)
        return True

    async def _set_county(self, page: Page, county_name: str = "FULTON", county_id: str = "60") -> bool:
        county_selectors = ["select#cboCounty","select[name='cboCounty']","select[id*='County' i]","select[name*='County' i]"]
        for sel in county_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    for val in [county_id, county_name, county_name.title(), county_name.capitalize()]:
                        try:
                            await el.select_option(value=val)
                            return True
                        except Exception:
                            pass
                    try:
                        await el.select_option(label=county_name)
                        return True
                    except Exception:
                        pass
                    options = await el.locator("option").all()
                    for opt in options:
                        text = (await opt.inner_text()).upper()
                        val2 = await opt.get_attribute("value") or ""
                        if county_name.upper() in text:
                            await el.select_option(value=val2)
                            log.info("County %s set via scan (value=%s)", county_name, val2)
                            return True
            except Exception:
                pass
        log.warning("Could not set county %s – skipping", county_name)
        return False

    async def _set_instrument_type(self, page: Page, doc_code: str) -> bool:
        candidates = self.GSCCCA_INSTRUMENT_MAP.get(doc_code, [doc_code])
        inst_selectors = ["select#cboInstrumentType","select[name='cboInstrumentType']","select[id*='Instrument' i]","select[name*='Instrument' i]","select[id*='Type' i]"]
        for sel in inst_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() == 0:
                    continue
                for cand in candidates:
                    for method in ("value", "label"):
                        try:
                            if method == "value":
                                await el.select_option(value=cand)
                            else:
                                await el.select_option(label=cand)
                            return True
                        except Exception:
                            pass
                options = await el.locator("option").all()
                for opt in options:
                    text = (await opt.inner_text()).upper()
                    val = await opt.get_attribute("value") or ""
                    for cand in candidates:
                        if cand.upper() in text or (val and cand.upper() in val.upper()):
                            await el.select_option(value=val)
                            return True
            except Exception as exc:
                log.debug("Instrument selector %s error: %s", sel, exc)
        return False

    async def _set_date_range(self, page: Page, start_date: str, end_date: str) -> None:
        date_pairs = [("input#txtDateFrom","input#txtDateTo"),("input[name='txtDateFrom']","input[name='txtDateTo']"),("input[id*='DateFrom' i]","input[id*='DateTo' i]"),("input[id*='FromDate' i]","input[id*='ToDate' i]"),("input[id*='StartDate' i]","input[id*='EndDate' i]")]
        for from_sel, to_sel in date_pairs:
            try:
                frm = page.locator(from_sel).first
                too = page.locator(to_sel).first
                if await frm.count() > 0 and await too.count() > 0:
                    await frm.triple_click()
                    await frm.fill(start_date)
                    await too.triple_click()
                    await too.fill(end_date)
                    return
            except Exception:
                pass

    async def _submit_search(self, page: Page) -> None:
        for sel in ["input#btnSearch","input[name='btnSearch']","button#btnSearch","input[value='Search']","input[value='Submit']","button:text('Search')","input[type='submit']","button[type='submit']"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=3000):
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    return
            except Exception:
                pass
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle", timeout=15000)
    def _parse_results_page(self, html: str, doc_code: str, filter_by_type: bool = False) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")
        table = None
        for tbl_id in ["GridView1","gvResults","GridViewResults","dgResults","ctl00_ContentPlaceHolder1_GridView1"]:
            t = soup.find("table", {"id": tbl_id})
            if t:
                table = t
                break
        if not table:
            for t in soup.find_all("table"):
                if len(t.find_all("th")) >= 4:
                    table = t
                    break
        if not table:
            return []
        header_row = table.find("tr")
        if not header_row:
            return []
        headers = [clean_str(th.get_text()) for th in header_row.find_all(["th","td"])]
        col_map = self._map_gsccca_columns(headers)
        label, cat = DOC_TYPES.get(doc_code, (doc_code, doc_code))
        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td","th"])
            if len(cells) < 3:
                continue
            try:
                def cv(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is None or idx >= len(cells):
                        return ""
                    return clean_str(cells[idx].get_text())
                def cl(field: str) -> str:
                    idx = col_map.get(field)
                    if idx is None or idx >= len(cells):
                        return ""
                    a = cells[idx].find("a", href=True)
                    if not a:
                        for cell in cells:
                            a = cell.find("a", href=True)
                            if a:
                                break
                    if not a:
                        return ""
                    href = a["href"]
                    return href if href.startswith("http") else urljoin(CLERK_BASE_URL, href)
                book = cv("book")
                page_num_str = cv("page_num")
                doc_num = f"{book}/{page_num_str}" if book and page_num_str else ""
                file_num = cv("file_num") or cv("doc_num")
                if not doc_num and file_num:
                    doc_num = file_num
                clerk_url = cl("book") or cl("doc_num") or cl("grantor") or ""
                if not doc_num:
                    for cell in cells:
                        a = cell.find("a", href=True)
                        if a:
                            text = clean_str(a.get_text())
                            if text:
                                doc_num = text
                                href = a["href"]
                                clerk_url = href if href.startswith("http") else urljoin(CLERK_BASE_URL, href)
                                break
                if not doc_num:
                    continue
                filed_raw = cv("filed") or cv("date")
                inst_type = cv("inst_type") or label
                if filter_by_type:
                    candidates = self.GSCCCA_INSTRUMENT_MAP.get(doc_code, [])
                    if not any(c.upper() in inst_type.upper() for c in candidates):
                        continue
                if not clerk_url and book and page_num_str:
                    clerk_url = f"https://search.gsccca.org/RealEstateIndex.aspx?county=60&book={book}&page={page_num_str}&instrumenttype={doc_code}"
                records.append({"doc_num": doc_num, "doc_type": inst_type if inst_type else label, "doc_code": doc_code, "filed": self._normalise_date(filed_raw), "grantor": cv("grantor"), "grantee": cv("grantee"), "legal": cv("legal"), "amount": cv("amount"), "clerk_url": clerk_url, "cat": cat, "cat_label": CATEGORY_LABELS.get(cat, cat)})
            except Exception as exc:
                log.debug("Row parse error: %s", exc)
        return records

    @staticmethod
    def _map_gsccca_columns(headers: List[str]) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        patterns = {"book": r"book", "page_num": r"\bpage\b", "file_num": r"file\s*(no|num|number)|doc\s*(no|num)", "filed": r"date|filed|record", "grantor": r"grantor|seller|owner|from|debtor", "grantee": r"grantee|buyer|lender|to\b|creditor", "inst_type": r"instrument|type|doc.?type", "legal": r"legal|desc|property|parcel", "amount": r"amount|consider|value|\$", "doc_num": r"doc.?(num|no\b|number)|instrument.?no"}
        for idx, header in enumerate(headers):
            h = header.lower()
            for field, pattern in patterns.items():
                if field not in mapping and re.search(pattern, h):
                    mapping[field] = idx
        return mapping
    async def _paginate(self, page: Page, doc_code: str, filter_by_type: bool) -> List[Dict[str, Any]]:
        all_records: List[Dict[str, Any]] = []
        current_page = 1
        while current_page <= MAX_PAGES_PER_DOCTYPE:
            await page.wait_for_load_state("domcontentloaded")
            html = await page.content()
            recs = self._parse_results_page(html, doc_code, filter_by_type)
            all_records.extend(recs)
            log.info(" Page %d → %d records (total: %d)", current_page, len(recs), len(all_records))
            if current_page == 1 and not recs:
                break
            soup = BeautifulSoup(html, "lxml")
            moved = await self._go_next_page(page, soup, current_page)
            if not moved:
                break
            current_page += 1
            await asyncio.sleep(1.0)
        return all_records

    async def _go_next_page(self, page: Page, soup: BeautifulSoup, current_page: int) -> bool:
        pager_cells = soup.find_all("td", {"colspan": True})
        for cell in pager_cells:
            for a in cell.find_all("a"):
                text = clean_str(a.get_text())
                href = a.get("href", "")
                if text in (">", "Next", "»", "next"):
                    if "doPostBack" in href or "javascript" in href.lower():
                        try:
                            await page.locator(f"a:text('{text}')").first.click()
                            await page.wait_for_load_state("networkidle", timeout=15000)
                            return True
                        except Exception:
                            pass
                    elif href and href not in ("#", ""):
                        full = href if href.startswith("http") else urljoin(CLERK_BASE_URL, href)
                        try:
                            await page.goto(full, wait_until="networkidle")
                            return True
                        except Exception:
                            pass
        next_num = str(current_page + 1)
        try:
            next_link = page.locator(f"a:text-is('{next_num}')").first
            if await next_link.is_visible(timeout=1500):
                await next_link.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                return True
        except Exception:
            pass
        for sel in ["a:text('>')", "a:text('»')", "a:text('Next')", "[title='Next Page']", "[aria-label='Next']"]:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=1000):
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    return True
            except Exception:
                pass
        try:
            await page.evaluate(f"__doPostBack('GridView1','Page${current_page + 1}')")
            await page.wait_for_load_state("networkidle", timeout=15000)
            return True
        except Exception:
            pass
        return False

    async def _scrape_one_type(self, page: Page, doc_code: str, start_date: str, end_date: str, county_name: str = "FULTON", county_id: str = "60") -> List[Dict[str, Any]]:
        try:
            await page.goto(self.SEARCH_URL, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=12000)
        except Exception as exc:
            log.warning("Navigation reset failed for %s/%s: %s", county_name, doc_code, exc)
        await self._set_county(page, county_name, county_id)
        matched = await self._set_instrument_type(page, doc_code)
        await self._set_date_range(page, start_date, end_date)
        await self._submit_search(page)
        records = await self._paginate(page, doc_code, filter_by_type=not matched)
        for rec in records:
            rec.setdefault("county", county_name.title())
        return records

    async def scrape_all(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        all_records: List[Dict[str, Any]] = []
        seen: set = set()
        total_counties = len(ACTIVE_COUNTIES)
        log.info("Scraping %d counties: %s", total_counties, ", ".join(n for n, _ in ACTIVE_COUNTIES))
        ctx, page = await self._new_page()
        try:
            loaded = await self._load_search_page(page)
            if not loaded:
                log.warning("GSCCCA search page did not load cleanly – continuing anyway")
            for c_idx, (county_name, county_id) in enumerate(ACTIVE_COUNTIES, 1):
                log.info("▶ County %d/%d: %s (id=%s)", c_idx, total_counties, county_name, county_id)
                county_new = 0
                for doc_code in DOC_TYPES:
                    log.info(" ┣━━ %s – %s", doc_code, DOC_TYPES[doc_code][0])
                    try:
                        records = await self._scrape_one_type(page, doc_code, start_date, end_date, county_name=county_name, county_id=county_id)
                        for rec in records:
                            key = f"{county_name}|{rec.get('doc_code','')}|{rec.get('doc_num','')}"
                            if key not in seen and rec.get("doc_num"):
                                seen.add(key)
                                all_records.append(rec)
                                county_new += 1
                        log.info(" → %d new records", len(records))
                    except Exception as exc:
                        log.error("Error %s/%s: %s\n%s", county_name, doc_code, exc, traceback.format_exc())
                    await asyncio.sleep(1.5)
                log.info(" ✓ %s done – %d records", county_name, county_new)
                if c_idx < total_counties:
                    await asyncio.sleep(3.0)
            await page.close()
        finally:
            await ctx.close()
        log.info("Total GSCCCA records: %d across %d counties", len(all_records), total_counties)
        return all_records
class LeadScorer:
    CUTOFF_DAYS = LOOKBACK_DAYS
    FLAG_MAP: Dict[str, List[str]] = {
        "LP": [FLAG_LP], "NOFC": [FLAG_PREFC], "TAXDEED": [FLAG_TAXLIEN, "Tax deed / tax sale"],
        "JUD": [FLAG_JUD], "CCJ": [FLAG_JUD], "DRJUD": [FLAG_JUD],
        "LNCORPTX": [FLAG_TAXLIEN], "LNIRS": [FLAG_TAXLIEN], "LNFED": [FLAG_TAXLIEN],
        "LN": [FLAG_MECH], "LNMECH": [FLAG_MECH], "LNHOA": ["HOA lien"],
        "MEDLN": ["Medicaid lien"], "PRO": [FLAG_PROBATE], "NOC": [], "RELLP": [],
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
    def score(cls, record: Dict[str, Any], owner_doc_codes: Optional[List[str]] = None) -> Tuple[int, List[str]]:
        flags: List[str] = []
        score = 30
        doc_code = record.get("doc_code", "")
        owner_upper = (record.get("grantor") or "").upper()
        for flag in cls.FLAG_MAP.get(doc_code, []):
            if flag not in flags:
                flags.append(flag)
        corp_keywords = [" LLC", " INC", " CORP", " LTD", " L.L.C", " CO.", " LP ", " L.P."]
        if any(kw in owner_upper for kw in corp_keywords):
            if FLAG_LLC not in flags:
                flags.append(FLAG_LLC)
        if cls._is_new_this_week(record.get("filed", "")):
            if FLAG_NEW not in flags:
                flags.append(FLAG_NEW)
        amount = parse_amount(record.get("amount", ""))
        score += len(flags) * 10
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


def enrich_records(raw_records: List[Dict[str, Any]], parcel_lookup: ParcelLookup) -> List[Dict[str, Any]]:
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
            parcel = parcel_lookup.lookup(owner)
            prop_addr = parcel.get("prop_address", "")
            if not prop_addr:
                legal = clean_str(rec.get("legal", ""))
                addr_match = re.search(r"\d{1,5}\s+[A-Z][A-Za-z\s]{2,40}(?:ST|AVE|RD|DR|LN|BLVD|CT|WAY|PL|CIR)\b", legal, re.I)
                if addr_match:
                    prop_addr = addr_match.group(0)
            amount_raw = clean_str(rec.get("amount", ""))
            amount_val = parse_amount(amount_raw)
            codes_for_owner = owner_codes.get(owner_up, [])
            score, flags = LeadScorer.score({**rec, "prop_address": prop_addr}, owner_doc_codes=codes_for_owner)
            e = {
                "doc_num": clean_str(rec.get("doc_num", "")), "doc_type": clean_str(rec.get("doc_type", "")),
                "filed": clean_str(rec.get("filed", "")), "cat": clean_str(rec.get("cat", "")),
                "cat_label": clean_str(rec.get("cat_label", "")), "owner": owner,
                "grantee": clean_str(rec.get("grantee", "")),
                "amount": f"${amount_val:,.2f}" if amount_val else amount_raw,
                "legal": clean_str(rec.get("legal", "")),
                "prop_address": prop_addr, "prop_city": parcel.get("prop_city", ""),
                "prop_state": parcel.get("prop_state", "GA"), "prop_zip": parcel.get("prop_zip", ""),
                "mail_address": parcel.get("mail_address", ""), "mail_city": parcel.get("mail_city", ""),
                "mail_state": parcel.get("mail_state", "GA"), "mail_zip": parcel.get("mail_zip", ""),
                "clerk_url": clean_str(rec.get("clerk_url", "")),
                "county": clean_str(rec.get("county", "")),
                "flags": flags, "score": score,
            }
            enriched.append(e)
        except Exception as exc:
            log.warning("Enrichment failed for record %s: %s", rec.get("doc_num"), exc)
    enriched.sort(key=lambda r: r["score"], reverse=True)
    return enriched
def write_json_outputs(records: List[Dict[str, Any]], start_date: str, end_date: str) -> None:
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Georgia GSCCCA (%d counties)" % len(ACTIVE_COUNTIES),
        "date_range": {"start": start_date, "end": end_date},
        "total": len(records), "with_address": with_address, "records": records,
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
    path = Path(GHL_CSV_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    COLUMNS = ["First Name","Last Name","County","Mailing Address","Mailing City","Mailing State","Mailing Zip","Property Address","Property City","Property State","Property Zip","Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    def split_name(full: str) -> Tuple[str, str]:
        full = clean_str(full)
        if not full:
            return "", ""
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
                    "First Name": first, "Last Name": last, "County": rec.get("county", ""),
                    "Mailing Address": rec.get("mail_address", ""), "Mailing City": rec.get("mail_city", ""),
                    "Mailing State": rec.get("mail_state", "GA"), "Mailing Zip": rec.get("mail_zip", ""),
                    "Property Address": rec.get("prop_address", ""), "Property City": rec.get("prop_city", ""),
                    "Property State": rec.get("prop_state", "GA"), "Property Zip": rec.get("prop_zip", ""),
                    "Lead Type": rec.get("cat_label", ""), "Document Type": rec.get("doc_type", ""),
                    "Date Filed": rec.get("filed", ""), "Document Number": rec.get("doc_num", ""),
                    "Amount/Debt Owed": rec.get("amount", ""), "Seller Score": rec.get("score", 0),
                    "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                    "Source": "Georgia GSCCCA (search.gsccca.org)",
                    "Public Records URL": rec.get("clerk_url", ""),
                })
        log.info("GHL CSV written: %s (%d rows)", path, len(records))
    except Exception as exc:
        log.error("GHL CSV write failed: %s", exc)


async def main() -> None:
    start_date, end_date = date_range_strings(LOOKBACK_DAYS)
    log.info("Starting Georgia Motivated Seller Scraper | counties: %s | date range: %s → %s | lookback_days: %d",
             ", ".join(n for n,_ in ACTIVE_COUNTIES), start_date, end_date, LOOKBACK_DAYS)
    parcel = ParcelLookup()
    try:
        parcel.load()
    except Exception as exc:
        log.error("Parcel load failed (continuing without address enrichment): %s", exc)
    raw_records: List[Dict[str, Any]] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage","--disable-gpu"],
        )
        try:
            scraper = ClerkScraper(browser)
            raw_records = await scraper.scrape_all(start_date, end_date)
        except Exception as exc:
            log.error("Clerk scraper failed: %s\n%s", exc, traceback.format_exc())
        finally:
            await browser.close()
    log.info("Raw records from clerk portal: %d", len(raw_records))
    enriched = enrich_records(raw_records, parcel)
    log.info("Enriched records: %d total | %d with property address", len(enriched), sum(1 for r in enriched if r.get("prop_address")))
    write_json_outputs(enriched, start_date, end_date)
    write_ghl_csv(enriched)
    log.info("=" * 60)
    log.info("SCRAPE COMPLETE")
    log.info(" Total records: %d", len(enriched))
    log.info(" With address: %d", sum(1 for r in enriched if r.get("prop_address")))
    log.info(" High-score (≥70): %d", sum(1 for r in enriched if r.get("score", 0) >= 70))
    log.info(" GHL CSV: %s", GHL_CSV_PATH)
    log.info(" JSON outputs: %s", ", ".join(OUTPUT_PATHS))
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())