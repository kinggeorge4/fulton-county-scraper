#!/usr/bin/env python3
"""
Fulton County Motivated Seller Lead Scraper
Collects public records from the Clerk of Superior Court portal
and enriches with parcel data from the Property Appraiser.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

try:
    from playwright.async_api import async_playwright, Page, Browser
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

CLERK_URL = "https://www.fultoncountyga.gov/inside-fulton-county/fulton-county-departments/clerk-of-superior-court"
PARCEL_SOURCES = [
    "https://fultoncountypropertyappraiser.org/property-search/",
    "https://opendata.fultoncountyga.gov/api/geospatial/data.csv",
    "https://services1.arcgis.com/GswF3ULKX5WMgeTx/arcgis/rest/services/Parcels/FeatureServer/0/query",
]

DOC_TYPES = {
    "LP":       ("LP",     "Lis Pendens",          "LP"),
    "NOFC":     ("FC",     "Notice of Foreclosure","NOFC"),
    "TAXDEED":  ("TAX",    "Tax Deed",             "TAXDEED"),
    "JUD":      ("JUD",    "Judgment",             "JUD"),
    "CCJ":      ("JUD",    "Certified Judgment",   "CCJ"),
    "DRJUD":    ("JUD",    "Domestic Judgment",    "DRJUD"),
    "LNCORPTX": ("LIEN",   "Corp Tax Lien",        "LNCORPTX"),
    "LNIRS":    ("LIEN",   "IRS Lien",             "LNIRS"),
    "LNFED":    ("LIEN",   "Federal Lien",         "LNFED"),
    "LN":       ("LIEN",   "Lien",                 "LN"),
    "LNMECH":   ("LIEN",   "Mechanic Lien",        "LNMECH"),
    "LNHOA":    ("LIEN",   "HOA Lien",             "LNHOA"),
    "MEDLN":    ("LIEN",   "Medicaid Lien",        "MEDLN"),
    "PRO":      ("PRO",    "Probate",              "PRO"),
    "NOC":      ("NOC",    "Notice of Commencement","NOC"),
    "RELLP":    ("RELLP",  "Release Lis Pendens",  "RELLP"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def retry(fn, attempts=3, delay=2):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if i < attempts - 1:
                log.warning(f"Retry {i+1}/{attempts}: {e}")
                time.sleep(delay * (i + 1))
            else:
                raise

def safe_get(url: str, session: requests.Session = None, **kwargs) -> Optional[requests.Response]:
    s = session or requests.Session()
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (compatible; FultonScraper/1.0)")
    try:
        r = retry(lambda: s.get(url, headers=headers, timeout=30, **kwargs))
        r.raise_for_status()
        return r
    except Exception as e:
        log.error(f"GET {url} failed: {e}")
        return None

def parse_amount(text: str) -> float:
    if not text:
        return 0.0
    cleaned = re.sub(r"[^0-9.]", "", str(text))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def normalize_name(name: str) -> List[str]:
    """Generate name variants for matching."""
    name = name.strip().upper()
    variants = [name]
    parts = re.split(r"[,\s]+", name)
    parts = [p for p in parts if p]
    if len(parts) >= 2:
        variants.append(parts[0] + " " + " ".join(parts[1:]))
        variants.append(" ".join(parts[1:]) + " " + parts[0])
        variants.append(parts[0] + ", " + " ".join(parts[1:]))
    return list(set(variants))

# ---------------------------------------------------------------------------
# Parcel Lookup
# ---------------------------------------------------------------------------

class ParcelLookup:
    CACHE_FILE = Path("parcel_cache/parcels.json")
    CACHE_TTL = 86400  # 24 hours

    def __init__(self):
        self.index: Dict[str, Dict] = {}
        self._loaded = False

    def _is_cache_fresh(self) -> bool:
        if not self.CACHE_FILE.exists():
            return False
        age = time.time() - self.CACHE_FILE.stat().st_mtime
        return age < self.CACHE_TTL

    def _load_cache(self):
        try:
            with self.CACHE_FILE.open() as f:
                data = json.load(f)
            self.index = data
            log.info(f"Loaded {len(self.index)} parcels from cache")
            self._loaded = True
        except Exception as e:
            log.warning(f"Cache load failed: {e}")

    def _save_cache(self):
        self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with self.CACHE_FILE.open("w") as f:
            json.dump(self.index, f)
        log.info(f"Saved {len(self.index)} parcels to cache")

    def load(self):
        if self._loaded:
            return
        if self._is_cache_fresh():
            self._load_cache()
            if self._loaded:
                return
        log.info("Building parcel index...")
        for source_fn in [self._load_from_appraiser, self._load_from_opendata, self._load_from_arcgis]:
            try:
                if source_fn():
                    self._save_cache()
                    self._loaded = True
                    return
            except Exception as e:
                log.warning(f"Parcel source {source_fn.__name__} failed: {e}")
        log.warning("All parcel sources failed - no parcel enrichment available")
        self._loaded = True

    def _index_record(self, rec: Dict):
        for variant in normalize_name(rec.get("owner", "")):
            if variant:
                self.index[variant] = rec

    def _load_from_appraiser(self) -> bool:
        """Try to download the bulk DBF from the property appraiser."""
        session = requests.Session()
        r = safe_get(PARCEL_SOURCES[0], session)
        if not r:
            return False
        soup = BeautifulSoup(r.text, "lxml")
        # Look for a bulk download link (.dbf or .zip)
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".dbf", "bulk", "download", "parcel"]):
                full_url = urljoin(PARCEL_SOURCES[0], href)
                log.info(f"Trying parcel download: {full_url}")
                dr = safe_get(full_url, session)
                if not dr:
                    continue
                ct = dr.headers.get("Content-Type", "")
                if "zip" in ct or href.lower().endswith(".zip"):
                    return self._parse_zip_dbf(dr.content)
                elif "dbf" in ct or href.lower().endswith(".dbf"):
                    return self._parse_dbf_bytes(dr.content)
        return False

    def _parse_zip_dbf(self, content: bytes) -> bool:
        if not HAS_DBF:
            return False
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    return False
                with zf.open(dbf_names[0]) as f:
                    return self._parse_dbf_bytes(f.read())
        except Exception as e:
            log.error(f"ZIP parse error: {e}")
            return False

    def _parse_dbf_bytes(self, content: bytes) -> bool:
        if not HAS_DBF:
            return False
        try:
            tmp = Path("/tmp/parcels.dbf")
            tmp.write_bytes(content)
            table = DBF(str(tmp), lowernames=True, ignore_missing_memofile=True)
            count = 0
            for row in table:
                rec = self._normalize_dbf_row(dict(row))
                if rec.get("owner"):
                    self._index_record(rec)
                    count += 1
            log.info(f"Indexed {count} parcel records from DBF")
            return count > 0
        except Exception as e:
            log.error(f"DBF parse error: {e}")
            return False

    def _normalize_dbf_row(self, row: Dict) -> Dict:
        owner = (row.get("owner") or row.get("own1") or row.get("ownername") or "").strip().upper()
        site_addr = (row.get("site_addr") or row.get("siteaddr") or row.get("address") or "").strip()
        site_city = (row.get("site_city") or row.get("sitecity") or row.get("city") or "").strip()
        site_zip  = (row.get("site_zip")  or row.get("sitezip")  or row.get("zip")  or "").strip()
        mail_addr = (row.get("addr_1") or row.get("mailadr1") or row.get("mail_addr") or "").strip()
        mail_city = (row.get("city")   or row.get("mailcity") or "").strip()
        mail_state= (row.get("state")  or row.get("mailstate") or "GA").strip()
        mail_zip  = (row.get("zip")    or row.get("mailzip")   or "").strip()
        return {
            "owner":       owner,
            "prop_address": site_addr,
            "prop_city":   site_city,
            "prop_state":  "GA",
            "prop_zip":    site_zip,
            "mail_address": mail_addr,
            "mail_city":   mail_city,
            "mail_state":  mail_state,
            "mail_zip":    mail_zip,
        }

    def _load_from_opendata(self) -> bool:
        """Try Fulton County open data CSV."""
        r = safe_get(PARCEL_SOURCES[1])
        if not r:
            return False
        try:
            reader = csv.DictReader(io.StringIO(r.text))
            count = 0
            for row in reader:
                rec = self._normalize_csv_row(row)
                if rec.get("owner"):
                    self._index_record(rec)
                    count += 1
            log.info(f"Indexed {count} parcel records from CSV")
            return count > 0
        except Exception as e:
            log.error(f"CSV parse error: {e}")
            return False

    def _normalize_csv_row(self, row: Dict) -> Dict:
        keys = {k.lower().replace(" ", "_"): v for k, v in row.items()}
        owner = (keys.get("owner_name") or keys.get("owner") or "").strip().upper()
        return {
            "owner":       owner,
            "prop_address": keys.get("site_address") or keys.get("property_address") or "",
            "prop_city":   keys.get("site_city") or "Atlanta",
            "prop_state":  "GA",
            "prop_zip":    keys.get("site_zip") or keys.get("zip_code") or "",
            "mail_address": keys.get("mail_address") or keys.get("mailing_address") or "",
            "mail_city":   keys.get("mail_city") or "",
            "mail_state":  keys.get("mail_state") or "GA",
            "mail_zip":    keys.get("mail_zip") or "",
        }

    def _load_from_arcgis(self) -> bool:
        """Try ArcGIS FeatureServer."""
        params = {
            "where": "1=1",
            "outFields": "OWNER,SITEADDR,SITECITY,SITEZIP,MAILADR1,MAILCITY,STATE,MAILZIP",
            "resultRecordCount": 2000,
            "f": "json",
        }
        r = safe_get(PARCEL_SOURCES[2], params=params)
        if not r:
            return False
        try:
            data = r.json()
            features = data.get("features", [])
            count = 0
            for feat in features:
                attrs = feat.get("attributes", {})
                rec = {
                    "owner":       (attrs.get("OWNER") or "").strip().upper(),
                    "prop_address": attrs.get("SITEADDR") or "",
                    "prop_city":   attrs.get("SITECITY") or "Atlanta",
                    "prop_state":  "GA",
                    "prop_zip":    attrs.get("SITEZIP") or "",
                    "mail_address": attrs.get("MAILADR1") or "",
                    "mail_city":   attrs.get("MAILCITY") or "",
                    "mail_state":  attrs.get("STATE") or "GA",
                    "mail_zip":    attrs.get("MAILZIP") or "",
                }
                if rec["owner"]:
                    self._index_record(rec)
                    count += 1
            log.info(f"Indexed {count} parcel records from ArcGIS")
            return count > 0
        except Exception as e:
            log.error(f"ArcGIS parse error: {e}")
            return False

    def lookup(self, owner: str) -> Dict:
        if not owner:
            return {}
        for variant in normalize_name(owner):
            if variant in self.index:
                return self.index[variant]
        return {}


# ---------------------------------------------------------------------------
# Lead Scorer
# ---------------------------------------------------------------------------

class LeadScorer:
    def __init__(self):
        self.week_ago = datetime.utcnow() - timedelta(days=7)

    def score(self, record: Dict) -> Tuple[int, List[str]]:
        flags = []
        pts = 30  # base

        cat = record.get("cat", "")
        doc_type = record.get("doc_type", "")
        amount = float(record.get("amount") or 0)
        filed_str = record.get("filed") or ""
        owner = record.get("owner") or ""
        prop_addr = record.get("prop_address") or ""

        # Categorical flags
        if cat == "LP":
            flags.append("Lis pendens")
            pts += 10
        if cat == "FC":
            flags.append("Pre-foreclosure")
            pts += 10
        if cat in ("JUD",):
            flags.append("Judgment lien")
            pts += 10
        if cat == "TAX" or "TAX" in doc_type.upper():
            flags.append("Tax lien")
            pts += 10
        if doc_type in ("LNMECH",):
            flags.append("Mechanic lien")
            pts += 10
        if cat == "PRO":
            flags.append("Probate / estate")
            pts += 10
        if re.search(r"\bLLC\b|\bCORP\b|\bINC\b|\bL\.P\.|\bL\.L\.C", owner, re.I):
            flags.append("LLC / corp owner")
            pts += 10

        # LP + Foreclosure combo bonus
        if cat == "LP" and any("foreclosure" in f.lower() or "pre-" in f.lower() for f in flags):
            pts += 20
        # Check if same owner has FC in records (simplified - just check doc type)
        if cat == "LP" and doc_type.upper() in ("NOFC", "LP"):
            pts += 5

        # Amount tiers
        if amount > 100_000:
            pts += 15
        elif amount > 50_000:
            pts += 10

        # Filed this week
        try:
            filed_dt = datetime.strptime(filed_str[:10], "%Y-%m-%d")
            if filed_dt >= self.week_ago:
                flags.append("New this week")
                pts += 5
        except Exception:
            pass

        # Has address
        if prop_addr:
            pts += 5

        return min(pts, 100), flags

# ---------------------------------------------------------------------------
# Clerk Scraper (Playwright)
# ---------------------------------------------------------------------------

class ClerkScraper:
    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date
        self.records: List[Dict] = []

    async def run(self) -> List[Dict]:
        if not HAS_PLAYWRIGHT:
            log.warning("Playwright not available - using fallback HTTP scraper")
            return self._http_fallback()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            page = await context.new_page()
            page.set_default_timeout(60000)

            try:
                portal_type = await self._detect_portal(page)
                log.info(f"Portal type: {portal_type}")

                for doc_code, (cat, cat_label, _) in DOC_TYPES.items():
                    try:
                        recs = await self._search_doc_type(page, doc_code, cat, cat_label, portal_type)
                        self.records.extend(recs)
                        log.info(f"  {doc_code}: {len(recs)} records")
                    except Exception as e:
                        log.error(f"  {doc_code} search failed: {e}")
                        continue

            finally:
                await browser.close()

        return self.records

    async def _detect_portal(self, page: Page) -> str:
        await page.goto(CLERK_URL, wait_until="networkidle", timeout=60000)
        content = await page.content()
        title = await page.title()

        if "civica" in content.lower() or "civica" in title.lower():
            return "civica"
        if "odyssey" in content.lower() or "tylertech" in title.lower():
            return "odyssey"
        if "infotrack" in content.lower():
            return "infotrack"

        # Look for any search form link
        for link in await page.query_selector_all("a[href]"):
            href = await link.get_attribute("href") or ""
            if any(kw in href.lower() for kw in ["search", "recording", "records", "document"]):
                return "generic_link:" + href
        return "generic"

    async def _search_doc_type(self, page: Page, doc_code: str, cat: str, cat_label: str, portal_type: str) -> List[Dict]:
        """Dispatch to the appropriate portal search strategy."""
        if portal_type.startswith("generic_link:"):
            search_url = urljoin(CLERK_URL, portal_type.split(":", 1)[1])
            return await self._generic_search(page, search_url, doc_code, cat, cat_label)
        elif portal_type == "civica":
            return await self._civica_search(page, doc_code, cat, cat_label)
        elif portal_type == "odyssey":
            return await self._odyssey_search(page, doc_code, cat, cat_label)
        else:
            return await self._generic_search(page, CLERK_URL, doc_code, cat, cat_label)

    async def _civica_search(self, page: Page, doc_code: str, cat: str, cat_label: str) -> List[Dict]:
        records = []
        try:
            await page.goto(CLERK_URL, wait_until="networkidle")
            # Fill date range and doc type
            date_fmt = "%m/%d/%Y"
            start_str = self.start_date.strftime(date_fmt)
            end_str = self.end_date.strftime(date_fmt)

            for sel in ["#beginDate", "#startDate", 'input[name*="begin"]', 'input[placeholder*="From"]']:
                el = await page.query_selector(sel)
                if el:
                    await el.fill(start_str)
                    break

            for sel in ["#endDate", "#stopDate", 'input[name*="end"]', 'input[placeholder*="To"]']:
                el = await page.query_selector(sel)
                if el:
                    await el.fill(end_str)
                    break

            for sel in ["#docType", 'select[name*="type"]', 'input[name*="type"]']:
                el = await page.query_selector(sel)
                if el:
                    tag = await el.evaluate("el => el.tagName")
                    if tag.upper() == "SELECT":
                        await el.select_option(value=doc_code)
                    else:
                        await el.fill(doc_code)
                    break

            await page.press("body", "Enter")
            await page.wait_for_load_state("networkidle", timeout=30000)
            records = await self._extract_table_records(page, doc_code, cat, cat_label)
        except Exception as e:
            log.debug(f"Civica search error: {e}")
        return records

    async def _odyssey_search(self, page: Page, doc_code: str, cat: str, cat_label: str) -> List[Dict]:
        return await self._generic_search(page, CLERK_URL, doc_code, cat, cat_label)

    async def _generic_search(self, page: Page, url: str, doc_code: str, cat: str, cat_label: str) -> List[Dict]:
        records = []
        try:
            await page.goto(url, wait_until="networkidle", timeout=45000)
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            records = self._parse_html_records(soup, doc_code, cat, cat_label, url)
        except Exception as e:
            log.debug(f"Generic search error for {doc_code}: {e}")
        return records

    async def _extract_table_records(self, page: Page, doc_code: str, cat: str, cat_label: str) -> List[Dict]:
        records = []
        page_num = 0
        while True:
            page_num += 1
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            new_recs = self._parse_html_records(soup, doc_code, cat, cat_label, page.url)
            records.extend(new_recs)
            # Pagination
            next_btn = await page.query_selector('a:has-text("Next"), button:has-text("Next"), [aria-label="Next"]')
            if not next_btn or page_num > 20:
                break
            try:
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                break
        return records

    def _parse_html_records(self, soup: BeautifulSoup, doc_code: str, cat: str, cat_label: str, base_url: str) -> List[Dict]:
        records = []
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers:
                continue
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                try:
                    rec = self._cells_to_record(cells, headers, doc_code, cat, cat_label, base_url)
                    if rec:
                        records.append(rec)
                except Exception:
                    continue
        return records

    def _cells_to_record(self, cells, headers, doc_code, cat, cat_label, base_url) -> Optional[Dict]:
        def cell_text(idx): return cells[idx].get_text(strip=True) if idx < len(cells) else ""
        def find_col(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h: return i
            return -1

        doc_num_idx = find_col("book", "doc", "instrument", "record")
        date_idx    = find_col("date", "filed", "recorded")
        grantor_idx = find_col("grantor", "owner", "party")
        grantee_idx = find_col("grantee", "to", "party2")
        amount_idx  = find_col("amount", "consideration", "value")
        legal_idx   = find_col("legal", "description")

        doc_num = cell_text(doc_num_idx) if doc_num_idx >= 0 else ""
        filed   = cell_text(date_idx)    if date_idx >= 0    else ""
        owner   = cell_text(grantor_idx) if grantor_idx >= 0 else ""
        grantee = cell_text(grantee_idx) if grantee_idx >= 0 else ""
        amount_raw = cell_text(amount_idx) if amount_idx >= 0 else ""
        legal   = cell_text(legal_idx)   if legal_idx >= 0   else cell_text(0)

        # Normalize date
        for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"]:
            try:
                filed = datetime.strptime(filed[:10], fmt[:len(filed)]).strftime("%Y-%m-%d")
                break
            except Exception:
                pass

        # Find doc link
        clerk_url = ""
        for cell in cells:
            a_tag = cell.find("a", href=True)
            if a_tag:
                clerk_url = urljoin(base_url, a_tag["href"])
                break

        if not doc_num and not owner:
            return None

        return {
            "doc_num":   doc_num,
            "doc_type":  doc_code,
            "filed":     filed,
            "cat":       cat,
            "cat_label": cat_label,
            "owner":     owner.upper(),
            "grantee":   grantee,
            "amount":    parse_amount(amount_raw),
            "legal":     legal,
            "prop_address": "",
            "prop_city":    "",
            "prop_state":   "GA",
            "prop_zip":     "",
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "GA",
            "mail_zip":     "",
            "clerk_url":    clerk_url,
            "flags":        [],
            "score":        30,
        }

    def _http_fallback(self) -> List[Dict]:
        """Fallback: plain HTTP requests to discover records."""
        records = []
        log.info("HTTP fallback: attempting to scrape without Playwright")
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FultonScraper/1.0)"})
        r = safe_get(CLERK_URL, session)
        if not r:
            return records
        soup = BeautifulSoup(r.text, "lxml")
        # Look for any record links or tables
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(kw in href.lower() for kw in ["record", "instrument", "doc", "book"]):
                full_url = urljoin(CLERK_URL, href)
                sub_r = safe_get(full_url, session)
                if sub_r:
                    sub_soup = BeautifulSoup(sub_r.text, "lxml")
                    for doc_code, (cat, cat_label, _) in DOC_TYPES.items():
                        recs = self._parse_html_records(sub_soup, doc_code, cat, cat_label, full_url)
                        records.extend(recs)
        return records


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def filter_by_date(records: List[Dict], start: datetime, end: datetime) -> List[Dict]:
    """Keep records within the date window."""
    out = []
    for r in records:
        try:
            filed_dt = datetime.strptime(r.get("filed", "")[:10], "%Y-%m-%d")
            if start <= filed_dt <= end:
                out.append(r)
        except Exception:
            out.append(r)  # keep if date unparseable
    return out


def deduplicate(records: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for r in records:
        key = (r.get("doc_num", ""), r.get("doc_type", ""), r.get("owner", ""))
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def enrich_with_parcels(records: List[Dict], parcel: ParcelLookup) -> List[Dict]:
    enriched = 0
    for r in records:
        p = parcel.lookup(r.get("owner", ""))
        if p:
            for field in ["prop_address", "prop_city", "prop_state", "prop_zip",
                          "mail_address", "mail_city", "mail_state", "mail_zip"]:
                if not r.get(field) and p.get(field):
                    r[field] = p[field]
            enriched += 1
    log.info(f"Enriched {enriched}/{len(records)} records with parcel data")
    return records


def score_records(records: List[Dict]) -> List[Dict]:
    scorer = LeadScorer()
    for r in records:
        score, flags = scorer.score(r)
        r["score"] = score
        r["flags"] = flags
    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    return records


def save_outputs(records: List[Dict], start: datetime, end: datetime):
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Fulton County Clerk of Superior Court",
        "date_range": {
            "start": start.strftime("%Y-%m-%d"),
            "end":   end.strftime("%Y-%m-%d"),
        },
        "total": len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records": records,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved {len(records)} records -> {path}")


def export_ghl_csv(records: List[Dict], out_path: Path = Path("ghl_export.csv")):
    columns = [
        "First Name", "Last Name", "Mailing Address", "Mailing City",
        "Mailing State", "Mailing Zip", "Property Address", "Property City",
        "Property State", "Property Zip", "Lead Type", "Document Type",
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score",
        "Motivated Seller Flags", "Source", "Public Records URL",
    ]
    rows = []
    for r in records:
        owner = (r.get("owner") or "").upper()
        parts = re.split(r"[,\s]+", owner)
        parts = [p for p in parts if p]
        last  = parts[0] if parts else ""
        first = " ".join(parts[1:]) if len(parts) > 1 else ""
        rows.append({
            "First Name": first,
            "Last Name": last,
            "Mailing Address": r.get("mail_address") or "",
            "Mailing City": r.get("mail_city") or "",
            "Mailing State": r.get("mail_state") or "GA",
            "Mailing Zip": r.get("mail_zip") or "",
            "Property Address": r.get("prop_address") or "",
            "Property City": r.get("prop_city") or "",
            "Property State": r.get("prop_state") or "GA",
            "Property Zip": r.get("prop_zip") or "",
            "Lead Type": r.get("cat_label") or "",
            "Document Type": r.get("doc_type") or "",
            "Date Filed": r.get("filed") or "",
            "Document Number": r.get("doc_num") or "",
            "Amount/Debt Owed": r.get("amount") or "",
            "Seller Score": r.get("score") or 0,
            "Motivated Seller Flags": "; ".join(r.get("flags") or []),
            "Source": "Fulton County Clerk of Superior Court",
            "Public Records URL": r.get("clerk_url") or "",
        })
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"GHL CSV exported: {out_path} ({len(rows)} rows)")


async def main():
    export_ghl = "--export-ghl" in sys.argv

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    log.info(f"Scraping Fulton County records: {start_date.date()} to {end_date.date()}")

    # Step 1: Scrape clerk portal
    scraper = ClerkScraper(start_date, end_date)
    records = await scraper.run()
    log.info(f"Raw records fetched: {len(records)}")

    # Step 2: Filter by date
    records = filter_by_date(records, start_date, end_date)
    log.info(f"After date filter: {len(records)}")

    # Step 3: Deduplicate
    records = deduplicate(records)
    log.info(f"After deduplication: {len(records)}")

    # Step 4: Parcel enrichment
    parcel = ParcelLookup()
    parcel.load()
    records = enrich_with_parcels(records, parcel)

    # Step 5: Score and rank
    records = score_records(records)

    # Step 6: Save outputs
    save_outputs(records, start_date, end_date)

    # Step 7: GHL export (optional)
    if export_ghl:
        export_ghl_csv(records)

    log.info(f"Done. {len(records)} leads saved. Top score: {records[0]['score'] if records else 0}")


if __name__ == "__main__":
    asyncio.run(main())
