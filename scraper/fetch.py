#!/usr/bin/env python3
"""
Georgia Multi-County Motivated Seller Lead Scraper
Targets the GSCCCA (Georgia Superior Court Clerks' Cooperative Authority)
at search.gsccca.org — the real statewide public records portal.
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
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"
OUTPUT_PATHS = [
    Path("dashboard/records.json"),
    Path("data/records.json"),
]

# GSCCCA - the real statewide GA public records portal
GSCCCA_SEARCH_URL = "https://search.gsccca.org/RealEstateIndex.aspx"
GSCCCA_BASE_URL   = "https://search.gsccca.org"

# All 159 GA counties with their GSCCCA numeric IDs
ALL_GA_COUNTIES = {
    "APPLING": 1, "ATKINSON": 2, "BACON": 3, "BAKER": 4, "BALDWIN": 5,
    "BANKS": 6, "BARROW": 7, "BARTOW": 8, "BEN HILL": 9, "BERRIEN": 10,
    "BIBB": 11, "BLECKLEY": 12, "BRANTLEY": 13, "BROOKS": 14, "BRYAN": 15,
    "BULLOCH": 16, "BURKE": 17, "BUTTS": 18, "CALHOUN": 19, "CAMDEN": 20,
    "CANDLER": 21, "CARROLL": 22, "CATOOSA": 23, "CHARLTON": 24, "CHATHAM": 25,
    "CHATTAHOOCHEE": 26, "CHATTOOGA": 27, "CHEROKEE": 28, "CLARKE": 29, "CLAY": 30,
    "CLAYTON": 31, "CLINCH": 32, "COBB": 33, "COFFEE": 34, "COLQUITT": 35,
    "COLUMBIA": 36, "COOK": 37, "COWETA": 38, "CRAWFORD": 39, "CRISP": 40,
    "DADE": 41, "DAWSON": 42, "DECATUR": 43, "DEKALB": 44, "DODGE": 45,
    "DOOLY": 46, "DOUGHERTY": 47, "DOUGLAS": 48, "EARLY": 49, "ECHOLS": 50,
    "EFFINGHAM": 51, "ELBERT": 52, "EMANUEL": 53, "EVANS": 54, "FANNIN": 55,
    "FAYETTE": 56, "FLOYD": 57, "FORSYTH": 58, "FRANKLIN": 59, "FULTON": 60,
    "GILMER": 61, "GLASCOCK": 62, "GLYNN": 63, "GORDON": 64, "GRADY": 65,
    "GREENE": 66, "GWINNETT": 67, "HABERSHAM": 68, "HALL": 69, "HANCOCK": 70,
    "HARALSON": 71, "HARRIS": 72, "HART": 73, "HEARD": 74, "HENRY": 75,
    "HOUSTON": 76, "IRWIN": 77, "JACKSON": 78, "JASPER": 79, "JEFF DAVIS": 80,
    "JEFFERSON": 81, "JENKINS": 82, "JOHNSON": 83, "JONES": 84, "LAMAR": 85,
    "LANIER": 86, "LAURENS": 87, "LEE": 88, "LIBERTY": 89, "LINCOLN": 90,
    "LONG": 91, "LOWNDES": 92, "LUMPKIN": 93, "MACON": 94, "MADISON": 95,
    "MARION": 96, "MCDUFFIE": 97, "MCINTOSH": 98, "MERIWETHER": 99, "MILLER": 100,
    "MITCHELL": 101, "MONROE": 102, "MONTGOMERY": 103, "MORGAN": 104, "MURRAY": 105,
    "MUSCOGEE": 106, "NEWTON": 107, "OCONEE": 108, "OGLETHORPE": 109, "PAULDING": 110,
    "PEACH": 111, "PICKENS": 112, "PIERCE": 113, "PIKE": 114, "POLK": 115,
    "PULASKI": 116, "PUTNAM": 117, "QUITMAN": 118, "RABUN": 119, "RANDOLPH": 120,
    "RICHMOND": 121, "ROCKDALE": 122, "SCHLEY": 123, "SCREVEN": 124, "SEMINOLE": 125,
    "SPALDING": 126, "STEPHENS": 127, "STEWART": 128, "SUMTER": 129, "TALBOT": 130,
    "TALIAFERRO": 131, "TATTNALL": 132, "TAYLOR": 133, "TELFAIR": 134, "TERRELL": 135,
    "THOMAS": 136, "TIFT": 137, "TOOMBS": 138, "TOWNS": 139, "TREUTLEN": 140,
    "TROUP": 141, "TURNER": 142, "TWIGGS": 143, "UNION": 144, "UPSON": 145,
    "WALKER": 146, "WALTON": 147, "WARE": 148, "WARREN": 149, "WASHINGTON": 150,
    "WAYNE": 151, "WEBSTER": 152, "WHEELER": 153, "WHITE": 154, "WHITFIELD": 155,
    "WILCOX": 156, "WILKES": 157, "WILKINSON": 158, "WORTH": 159,
}

DEFAULT_COUNTIES = "FULTON,CLAYTON,HOUSTON,COBB,GWINNETT,DOUGLAS"

# Instrument type labels on GSCCCA (map our codes -> GSCCCA dropdown value)
GSCCCA_INSTRUMENT_MAP = {
    "LP":       "LIS PENDENS",
    "NOFC":     "NOTICE OF FORECLOSURE",
    "TAXDEED":  "TAX DEED",
    "JUD":      "JUDGMENT",
    "CCJ":      "JUDGMENT",
    "DRJUD":    "JUDGMENT",
    "LNCORPTX": "LIEN",
    "LNIRS":    "LIEN",
    "LNFED":    "LIEN",
    "LN":       "LIEN",
    "LNMECH":   "LIEN",
    "LNHOA":    "LIEN",
    "MEDLN":    "LIEN",
    "PRO":      "PROBATE",
    "NOC":      "NOTICE OF COMMENCEMENT",
    "RELLP":    "RELEASE LIS PENDENS",
}

DOC_TYPES = {
    "LP":       ("LP",     "Lis Pendens"),
    "NOFC":     ("FC",     "Notice of Foreclosure"),
    "TAXDEED":  ("TAX",    "Tax Deed"),
    "JUD":      ("JUD",    "Judgment"),
    "CCJ":      ("JUD",    "Certified Judgment"),
    "DRJUD":    ("JUD",    "Domestic Judgment"),
    "LNCORPTX": ("LIEN",   "Corp Tax Lien"),
    "LNIRS":    ("LIEN",   "IRS Lien"),
    "LNFED":    ("LIEN",   "Federal Lien"),
    "LN":       ("LIEN",   "Lien"),
    "LNMECH":   ("LIEN",   "Mechanic Lien"),
    "LNHOA":    ("LIEN",   "HOA Lien"),
    "MEDLN":    ("LIEN",   "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# County resolution
# ---------------------------------------------------------------------------

def _resolve_counties() -> List[Tuple[str, int]]:
    raw = os.environ.get("COUNTIES", DEFAULT_COUNTIES).strip().upper()
    if raw == "ALL":
        return list(ALL_GA_COUNTIES.items())
    names = [n.strip() for n in raw.split(",") if n.strip()]
    resolved = []
    for name in names:
        cid = ALL_GA_COUNTIES.get(name)
        if cid:
            resolved.append((name, cid))
        else:
            log.warning(f"Unknown county '{name}' — skipping")
    if not resolved:
        log.warning("No valid counties resolved — falling back to FULTON")
        resolved = [("FULTON", 60)]
    return resolved

ACTIVE_COUNTIES = _resolve_counties()
log.info(f"Active counties ({len(ACTIVE_COUNTIES)}): {[c[0] for c in ACTIVE_COUNTIES]}")

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

def safe_get(url: str, session=None, **kwargs):
    s = session or requests.Session()
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (compatible; GAScraper/2.0)")
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
# Parcel Lookup (unchanged from v1)
# ---------------------------------------------------------------------------

class ParcelLookup:
    CACHE_FILE = Path("parcel_cache/parcels.json")
    CACHE_TTL = 86400

    def __init__(self):
        self.index: Dict[str, Dict] = {}
        self._loaded = False

    def _is_cache_fresh(self):
        if not self.CACHE_FILE.exists():
            return False
        return (time.time() - self.CACHE_FILE.stat().st_mtime) < self.CACHE_TTL

    def _load_cache(self):
        try:
            with self.CACHE_FILE.open() as f:
                self.index = json.load(f)
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
        log.info("Building parcel index (no valid cache)...")
        self._loaded = True

    def _index_record(self, rec: Dict):
        for variant in normalize_name(rec.get("owner", "")):
            if variant:
                self.index[variant] = rec

    def lookup(self, owner: str) -> Dict:
        if not owner:
            return {}
        for variant in normalize_name(owner):
            if variant in self.index:
                return self.index[variant]
        return {}

# ---------------------------------------------------------------------------
# GSCCCA Scraper
# ---------------------------------------------------------------------------

class GSCCCAScraper:
    """Scrapes the Georgia Superior Court Clerks' Cooperative Authority portal."""

    def __init__(self, start_date: datetime, end_date: datetime, counties: List[Tuple[str, int]]):
        self.start_date = start_date
        self.end_date   = end_date
        self.counties   = counties
        self.records: List[Dict] = []

    async def run(self) -> List[Dict]:
        if not HAS_PLAYWRIGHT:
            log.warning("Playwright not available — using HTTP fallback")
            return self._http_fallback()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
                viewport={"width": 1280, "height": 900},
            )
            page = await context.new_page()
            page.set_default_timeout(60_000)

            try:
                await self._accept_disclaimer(page)

                for county_name, county_id in self.counties:
                    log.info(f"Scraping {county_name} county (id={county_id})...")
                    for doc_code, (cat, cat_label) in DOC_TYPES.items():
                        try:
                            recs = await self._scrape_county_doctype(
                                page, county_name, county_id, doc_code, cat, cat_label
                            )
                            self.records.extend(recs)
                            if recs:
                                log.info(f"  {county_name}/{doc_code}: {len(recs)} records")
                        except Exception as e:
                            log.error(f"  {county_name}/{doc_code} failed: {e}")
                            continue

            finally:
                await browser.close()

        log.info(f"Total raw records: {len(self.records)}")
        return self.records

    async def _accept_disclaimer(self, page: Page):
        """Load GSCCCA and dismiss the disclaimer modal if present."""
        await page.goto(GSCCCA_SEARCH_URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(1500)

        # Common disclaimer selectors
        for sel in [
            'input[value="I Agree"]',
            'button:has-text("I Agree")',
            'a:has-text("I Agree")',
            '#btnAgree',
            '.disclaimer-agree',
        ]:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    await el.click()
                    await page.wait_for_load_state("networkidle", timeout=15_000)
                    log.info("Dismissed GSCCCA disclaimer")
                    return
            except Exception:
                continue

    async def _scrape_county_doctype(
        self,
        page: Page,
        county_name: str,
        county_id: int,
        doc_code: str,
        cat: str,
        cat_label: str,
    ) -> List[Dict]:
        records = []
        date_fmt = "%m/%d/%Y"
        start_str = self.start_date.strftime(date_fmt)
        end_str   = self.end_date.strftime(date_fmt)

        # Navigate to search page
        await page.goto(GSCCCA_SEARCH_URL, wait_until="domcontentloaded", timeout=45_000)
        await page.wait_for_timeout(800)

        # Select county
        county_sel = await page.query_selector(
            '#cphMain_cboCounty, select[name*="County"], select[id*="County"]'
        )
        if county_sel:
            await county_sel.select_option(value=str(county_id))
            await page.wait_for_timeout(400)

        # Select instrument type
        instrument_label = GSCCCA_INSTRUMENT_MAP.get(doc_code, "")
        if instrument_label:
            instr_sel = await page.query_selector(
                '#cphMain_cboInstrumentType, select[name*="Instrument"], select[id*="Instrument"]'
            )
            if instr_sel:
                try:
                    await instr_sel.select_option(label=instrument_label)
                except Exception:
                    # Try partial match
                    opts = await instr_sel.query_selector_all("option")
                    for opt in opts:
                        text = (await opt.text_content() or "").upper()
                        if instrument_label[:6] in text:
                            val = await opt.get_attribute("value")
                            await instr_sel.select_option(value=val)
                            break
                await page.wait_for_timeout(300)

        # Fill date range
        for sel in ['#cphMain_txtFromDate', 'input[name*="From"]', 'input[id*="From"]', 'input[placeholder*="From"]']:
            el = await page.query_selector(sel)
            if el:
                await el.triple_click()
                await el.fill(start_str)
                break

        for sel in ['#cphMain_txtToDate', 'input[name*="To"]', 'input[id*="To"]', 'input[placeholder*="To"]']:
            el = await page.query_selector(sel)
            if el:
                await el.triple_click()
                await el.fill(end_str)
                break

        # Submit search
        for sel in ['#cphMain_btnSearch', 'input[value="Search"]', 'button:has-text("Search")', 'input[type="submit"]']:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                break

        await page.wait_for_load_state("networkidle", timeout=30_000)
        await page.wait_for_timeout(1000)

        # Paginate and extract
        page_num = 0
        while True:
            page_num += 1
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            new_recs = self._parse_gsccca_results(soup, county_name, doc_code, cat, cat_label)
            records.extend(new_recs)

            # Check for next page
            next_link = None
            for candidate in soup.find_all("a"):
                txt = candidate.get_text(strip=True)
                if txt in (">", "Next", ">>") or (txt.isdigit() and int(txt) == page_num + 1):
                    next_link = candidate
                    break

            if not next_link or page_num >= 30:
                break

            # Use __doPostBack for GridView pagination
            onclick = next_link.get("href", "")
            if "__doPostBack" in onclick:
                # Extract event target
                m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", onclick)
                if m:
                    try:
                        await page.evaluate(
                            f"__doPostBack('{m.group(1)}','{m.group(2)}')"
                        )
                        await page.wait_for_load_state("networkidle", timeout=20_000)
                        await page.wait_for_timeout(800)
                        continue
                    except Exception:
                        pass

            # Standard click
            try:
                link_el = await page.query_selector(f'a:has-text("{next_link.get_text(strip=True)}")')
                if link_el:
                    await link_el.click()
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    await page.wait_for_timeout(800)
                else:
                    break
            except Exception:
                break

        return records

    def _parse_gsccca_results(
        self, soup: BeautifulSoup, county_name: str, doc_code: str, cat: str, cat_label: str
    ) -> List[Dict]:
        records = []

        # GSCCCA renders results in a GridView table
        result_table = None
        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(kw in " ".join(headers) for kw in ["grantor", "grantee", "book", "instrument", "date"]):
                result_table = tbl
                break

        if not result_table:
            return records

        headers = [th.get_text(strip=True).lower() for th in result_table.find_all("th")]

        def find_col(*names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return i
            return -1

        book_idx    = find_col("book", "deed book")
        page_idx    = find_col("page", "deed page")
        date_idx    = find_col("date", "file date", "recorded")
        grantor_idx = find_col("grantor", "party 1", "party1")
        grantee_idx = find_col("grantee", "party 2", "party2")
        amount_idx  = find_col("amount", "consideration")
        desc_idx    = find_col("description", "legal", "property")

        for row in result_table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if not cells or len(cells) < 2:
                continue
            try:
                def cell(idx):
                    return cells[idx].get_text(strip=True) if 0 <= idx < len(cells) else ""

                book = cell(book_idx)
                pg   = cell(page_idx)
                doc_num = f"{book}/{pg}" if book and pg else book or pg or ""

                filed_raw = cell(date_idx)
                filed = ""
                for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]:
                    try:
                        filed = datetime.strptime(filed_raw[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass
                if not filed:
                    filed = filed_raw

                owner   = cell(grantor_idx).upper()
                grantee = cell(grantee_idx)
                amount  = parse_amount(cell(amount_idx))
                legal   = cell(desc_idx)

                # Build clerk URL
                clerk_url = ""
                for c in cells:
                    a = c.find("a", href=True)
                    if a:
                        clerk_url = urljoin(GSCCCA_BASE_URL, a["href"])
                        break

                if not doc_num and not owner:
                    continue

                records.append({
                    "doc_num":      doc_num,
                    "doc_type":     doc_code,
                    "filed":        filed,
                    "cat":          cat,
                    "cat_label":    cat_label,
                    "county":       county_name,
                    "owner":        owner,
                    "grantee":      grantee,
                    "amount":       amount,
                    "legal":        legal,
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
                })
            except Exception:
                continue

        return records

    def _http_fallback(self) -> List[Dict]:
        """HTTP-only fallback when Playwright is unavailable."""
        records = []
        log.info("HTTP fallback: scraping GSCCCA without browser")
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; GAScraper/2.0)"})
        r = safe_get(GSCCCA_SEARCH_URL, session)
        if not r:
            return records
        soup = BeautifulSoup(r.text, "lxml")
        # Parse whatever is visible on the landing page
        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            for county_name, _ in self.counties:
                recs = self._parse_gsccca_results(soup, county_name, doc_code, cat, cat_label)
                records.extend(recs)
        return records

# ---------------------------------------------------------------------------
# Lead Scorer
# ---------------------------------------------------------------------------

class LeadScorer:
    def __init__(self):
        self.week_ago = datetime.utcnow() - timedelta(days=7)

    def score(self, record: Dict) -> Tuple[int, List[str]]:
        flags = []
        pts = 30

        cat      = record.get("cat", "")
        doc_type = record.get("doc_type", "")
        amount   = float(record.get("amount") or 0)
        filed_str= record.get("filed") or ""
        owner    = record.get("owner") or ""
        prop_addr= record.get("prop_address") or ""

        if cat == "LP":
            flags.append("Lis pendens"); pts += 10
        if cat == "FC":
            flags.append("Pre-foreclosure"); pts += 10
        if cat == "JUD":
            flags.append("Judgment lien"); pts += 10
        if cat == "TAX" or "TAX" in doc_type.upper():
            flags.append("Tax lien"); pts += 10
        if doc_type == "LNMECH":
            flags.append("Mechanic lien"); pts += 10
        if cat == "PRO":
            flags.append("Probate / estate"); pts += 10
        if re.search(r"\bLLC\b|\bCORP\b|\bINC\b|\bL\.P\.|\bL\.L\.C", owner, re.I):
            flags.append("LLC / corp owner"); pts += 10

        if cat == "LP" and any("foreclosure" in f.lower() for f in flags):
            pts += 20

        if amount > 100_000: pts += 15
        elif amount > 50_000: pts += 10

        try:
            if datetime.strptime(filed_str[:10], "%Y-%m-%d") >= self.week_ago:
                flags.append("New this week"); pts += 5
        except Exception:
            pass

        if prop_addr:
            pts += 5

        return min(pts, 100), flags

# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def filter_by_date(records, start, end):
    out = []
    for r in records:
        try:
            filed_dt = datetime.strptime(r.get("filed", "")[:10], "%Y-%m-%d")
            if start <= filed_dt <= end:
                out.append(r)
        except Exception:
            out.append(r)
    return out

def deduplicate(records):
    seen = set()
    out = []
    for r in records:
        key = (r.get("doc_num",""), r.get("doc_type",""), r.get("owner",""), r.get("county",""))
        if key not in seen:
            seen.add(key); out.append(r)
    return out

def enrich_with_parcels(records, parcel):
    enriched = 0
    for r in records:
        p = parcel.lookup(r.get("owner",""))
        if p:
            for field in ["prop_address","prop_city","prop_state","prop_zip",
                          "mail_address","mail_city","mail_state","mail_zip"]:
                if not r.get(field) and p.get(field):
                    r[field] = p[field]
            enriched += 1
    log.info(f"Enriched {enriched}/{len(records)} records with parcel data")
    return records

def score_records(records):
    scorer = LeadScorer()
    for r in records:
        score, flags = scorer.score(r)
        r["score"] = score; r["flags"] = flags
    records.sort(key=lambda r: r.get("score",0), reverse=True)
    return records

def save_outputs(records, start, end):
    payload = {
        "fetched_at":  datetime.utcnow().isoformat() + "Z",
        "source":      "GSCCCA - Georgia Superior Court Clerks Cooperative Authority",
        "date_range":  {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "counties":    [c[0] for c in ACTIVE_COUNTIES],
        "total":       len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":     records,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved {len(records)} records -> {path}")

def export_ghl_csv(records, out_path=Path("data/ghl_export.csv")):
    columns = [
        "First Name","Last Name","County","Mailing Address","Mailing City",
        "Mailing State","Mailing Zip","Property Address","Property City",
        "Property State","Property Zip","Lead Type","Document Type",
        "Date Filed","Document Number","Amount/Debt Owed","Seller Score",
        "Motivated Seller Flags","Source","Public Records URL",
    ]
    rows = []
    for r in records:
        owner = (r.get("owner") or "").upper()
        parts = re.split(r"[,\s]+", owner)
        parts = [p for p in parts if p]
        last  = parts[0] if parts else ""
        first = " ".join(parts[1:]) if len(parts) > 1 else ""
        rows.append({
            "First Name": first, "Last Name": last,
            "County": r.get("county") or "",
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
            "Source": "GSCCCA",
            "Public Records URL": r.get("clerk_url") or "",
        })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader(); writer.writerows(rows)
    log.info(f"GHL CSV exported: {out_path} ({len(rows)} rows)")

async def main():
    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    log.info(f"Date range: {start_date.date()} to {end_date.date()}")
    log.info(f"Counties:   {[c[0] for c in ACTIVE_COUNTIES]}")

    scraper = GSCCCAScraper(start_date, end_date, ACTIVE_COUNTIES)
    records = await scraper.run()
    log.info(f"Raw records: {len(records)}")

    records = filter_by_date(records, start_date, end_date)
    records = deduplicate(records)
    log.info(f"After dedup: {len(records)}")

    parcel = ParcelLookup()
    parcel.load()
    records = enrich_with_parcels(records, parcel)
    records = score_records(records)

    save_outputs(records, start_date, end_date)
    export_ghl_csv(records)

    log.info(f"Done. {len(records)} leads. Top score: {records[0]['score'] if records else 0}")

if __name__ == "__main__":
    asyncio.run(main())
