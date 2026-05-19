# Fulton County Motivated Seller Lead Scraper

Automated scraper that collects motivated seller leads from Fulton County, Georgia public records. Runs daily at 7 AM UTC via GitHub Actions and deploys a live dashboard to GitHub Pages.

## Lead Types Collected

- **(LP)** Lis Pendens
- **(NOFC)** Notice of Foreclosure
- **(TAXDEED)** Tax Deed
- **(JUD/CCJ/DRJUD)** Judgment / Certified Judgment / Domestic Judgment
- **(LNCORPTX/LNIRS/LNFED)** Corp Tax Lien / IRS Lien / Federal Lien
- **(LN/LNMECH/LNHOA)** Lien / Mechanic Lien / HOA Lien
- **(MEDLN)** Medicaid Lien
- **(PRO)** Probate Documents
- **(NOC)** Notice of Commencement
- **(RELLP)** Release Lis Pendens

## File Structure

```
scraper/
  fetch.py          # Main scraper (Playwright + BeautifulSoup)
  requirements.txt  # Python dependencies
dashboard/
  index.html        # Live lead dashboard
  records.json      # Latest scraped records
data/
  records.json      # Backup copy of records
.github/
  workflows/
    scrape.yml      # Daily automation + GitHub Pages deploy
```

## Setup

### 1. Fork / Clone this repo

### 2. Enable GitHub Actions
Go to **Settings → Actions → General** and set Workflow permissions to **Read and write**.

### 3. Enable GitHub Pages
Go to **Settings → Pages** and set Source to **GitHub Actions**.

### 4. Run manually first
Go to **Actions → Scrape Fulton County Records → Run workflow** to test.

### 5. Dashboard URL
Your live dashboard will be at: `https://[your-username].github.io/fulton-county-scraper/`

## Seller Score (0–100)

| Criteria | Points |
|----------|--------|
| Base score | 30 |
| Per distress flag | +10 |
| LP + Foreclosure combo | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed this week | +5 |
| Has property address | +5 |

## GHL Export

Run `python scraper/fetch.py --export-ghl` to generate a `ghl_export.csv` file ready to import into GoHighLevel.

## Data Sources

- **Clerk of Superior Court**: [Fulton County Clerk Portal](https://www.fultoncountyga.gov/inside-fulton-county/fulton-county-departments/clerk-of-superior-court)
- **Property Appraiser**: [Fulton County Property Appraiser](https://fultoncountypropertyappraiser.org/property-search/)
