#!/usr/bin/env python3
"""
get_gsccca_cookie.py
────────────────────
Run this ONCE on your local Mac to capture your GSCCCA session cookie.
It opens a real Chrome window so you can log in normally — no bot detection.

Usage:
    pip install playwright
    python -m playwright install chromium
    python get_gsccca_cookie.py

After logging in, press ENTER in this terminal.
The script prints the cookie value — copy it into GitHub Secrets.
"""

import asyncio
import json
from playwright.async_api import async_playwright


GSCCCA_URL = "https://www.gsccca.org"  # Main site – login from here


async def main():
    print("=" * 60)
    print("  GSCCCA Cookie Capture Tool — Propstor LLC")
    print("=" * 60)
    print()
    print("Opening Chrome browser...")
    print("→ Log in to GSCCCA as normal")
    print("→ Once you see the search page, come back here and press ENTER")
    print()

    async with async_playwright() as pw:
        # Launch VISIBLE (non-headless) browser — real Chrome, no bot detection
        browser = await pw.chromium.launch(
            headless=False,
            args=["--start-maximized"],
        )
        ctx  = await browser.new_context(viewport=None)
        page = await ctx.new_page()

        await page.goto(GSCCCA_URL)

        print("Waiting for you to log in...")
        print("(Press ENTER in this terminal once you're logged in)")
        input()

        # Grab all cookies from the browser context
        cookies = await ctx.cookies()

        await browser.close()

    if not cookies:
        print("❌ No cookies found. Did you log in?")
        return

    print()
    print("Cookies captured:")
    for c in cookies:
        print(f"  {c['name']} = {c['value'][:40]}{'...' if len(c['value']) > 40 else ''}")

    # Find the most likely session cookies
    session_cookies = {
        c["name"]: c["value"]
        for c in cookies
        if any(k in c["name"].lower() for k in [
            "session", "aspnet", ".aspxauth", "auth",
            "gsccca", "token", "userid", "user"
        ])
    }

    # Also include ALL cookies as a JSON blob (most reliable)
    all_cookie_json = json.dumps([
        {"name": c["name"], "value": c["value"],
         "domain": c["domain"], "path": c["path"]}
        for c in cookies
    ])

    print()
    print("=" * 60)
    print("  ADD THIS TO GITHUB SECRETS")
    print("=" * 60)
    print()
    print("Go to your repo → Settings → Secrets and variables → Actions")
    print("→ New repository secret")
    print()

    if session_cookies:
        for name, value in session_cookies.items():
            print(f"  Secret name:  {name.upper().replace('.','_').replace('-','_')}")
            print(f"  Secret value: {value}")
            print()

    print("  ── OR store the full cookie bundle (recommended) ──")
    print()
    print("  Secret name:  GSCCCA_COOKIES")
    print(f"  Secret value: {all_cookie_json}")
    print()
    print("=" * 60)
    print()
    print("After adding the secret, re-run the GitHub Actions workflow.")
    print("The scraper will inject these cookies automatically — no login needed.")


if __name__ == "__main__":
    asyncio.run(main())
