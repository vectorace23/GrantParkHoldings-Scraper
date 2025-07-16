#!/usr/bin/env python3
"""
email_scraper.py
================
Scrape publicly listed e-mail addresses from a list of firm websites.

Usage
-----
$ python email_scraper.py  \
    --input  "Trust & Estate - Scraping, Long Island, Queens - Sheet1.csv" \
    --website-column website \
    --output "trust_estate_emails.csv"

Dependencies
------------
pip install pandas requests beautifulsoup4 tqdm
"""

import argparse
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
COMMON_SUBPAGES = [
    "",              # homepage
    "contact",
    "about",
    "team",
    "support",
    "contact-us",
    "about-us",
    "our-team",
    "staff",
]
EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EmailScraperBot/1.0)"}
REQUEST_TIMEOUT = 10    # seconds
POLITENESS_DELAY = 0.5  # seconds between requests


# --------------------------------------------------------------------------- #
# Helper functions                                                            #
# --------------------------------------------------------------------------- #
def get_emails_from_url(url: str) -> set[str]:
    """Return a set of e-mail addresses found at *url* (or an empty set)."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return set()
        text = BeautifulSoup(resp.text, "html.parser").get_text()
        return set(re.findall(EMAIL_REGEX, text))
    except requests.RequestException:
        return set()


def scrape_emails(website: str) -> set[str]:
    """Scrape *website* and common subpages for any e-mail addresses."""
    if not isinstance(website, str) or not website.strip():
        return set()

    # Prepend http:// if no scheme is present
    if not website.startswith(("http://", "https://")):
        website = "http://" + website.strip()

    parsed = urlparse(website)
    base = f"{parsed.scheme}://{parsed.netloc}"
    found: set[str] = set()

    for sub in COMMON_SUBPAGES:
        target = urljoin(base + "/", sub) if sub else base
        found.update(get_emails_from_url(target))
        time.sleep(POLITENESS_DELAY)

    return found


# --------------------------------------------------------------------------- #
# Main routine                                                                #
# --------------------------------------------------------------------------- #
def run(input_csv: Path, website_col: str, output_csv: Path) -> None:
    """Load *input_csv*, scrape e-mails, and write results to *output_csv*."""
    df = pd.read_csv(input_csv)
    if website_col not in df.columns:
        raise ValueError(f"Column '{website_col}' not found in {input_csv}")

    scraped = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Scraping"):
        emails = scrape_emails(row.get(website_col, ""))
        scraped.append(", ".join(sorted(emails)) if emails else "")

    df["scraped_emails"] = scraped
    df.to_csv(output_csv, index=False)
    print(f"✓ Done. Results saved to {output_csv.resolve()}")


# --------------------------------------------------------------------------- #
# CLI interface                                                               #
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape publicly listed e-mail addresses from websites in a CSV."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the input CSV file containing website URLs.",
    )
    parser.add_argument(
        "--website-column",
        default="website",
        help="Name of the column in the CSV that holds the website URL.",
    )
    parser.add_argument(
        "--output",
        default="scraped_emails.csv",
        type=Path,
        help="Filename for the output CSV (default: scraped_emails.csv).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.website_column, args.output)
