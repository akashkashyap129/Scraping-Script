"""
main.py

Upgraded batch Bing + Google Maps + Email extractor for MSMEs.
- BATCH processing (resumable)
- Bing scraping (phone, website, aggregator links)
- Google Maps scraping (phone, website, rating, category)
- Website email extraction (regex + mailto)
- Multi-query retries and fallback queries
- Parallel processing with safety: MAX_WORKERS and MAPS_CONCURRENCY
- Saves each batch to an Excel file and a combined CSV log
"""

import os
import re
import time
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Dict, List
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Playwright for Google Maps
from playwright.sync_api import sync_playwright
from urllib.parse import quote_plus, urlparse

# -------------------------
# CONFIG
# -------------------------
INPUT_FILE = "MSME_cleaned.xlsx"
OUTPUT_FOLDER = "bing_batches"
BATCH_SIZE = 100

MAX_WORKERS = 8                   # concurrent worker threads (for requests + site visits)
MAPS_CONCURRENCY = 2              # limit number of concurrent Playwright maps scrapers
DELAY_RANGE = (0.6, 2.2)          # random sleep per company (seconds)
RETRIES_PER_QUERY = 3
REQUEST_TIMEOUT = 15              # seconds

USER_AGENTS = [
    # a small rotation list; add more if you want
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
]

AGGREGATOR_DOMAINS = ["indiamart.com", "justdial.com", "tradeindia.com", "exportersindia.com"]

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scrape_run.log"),
        logging.StreamHandler()
    ]
)

# -------------------------
# Utilities
# -------------------------
def random_delay():
    time.sleep(random.uniform(*DELAY_RANGE))

def safe_get(url: str, headers=None, timeout=REQUEST_TIMEOUT):
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        return r
    except Exception as e:
        logging.debug(f"safe_get error for {url}: {e}")
        return None

def extract_emails_from_text(text: str) -> List[str]:
    # Simple email regex
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    # unique and cleaned
    return sorted(set(emails))

def normalize_url(u: str) -> str:
    if not u:
        return ""
    try:
        p = urlparse(u)
        if p.scheme:
            return u
        else:
            return "http://" + u
    except:
        return u

# -------------------------
# Bing Scraper (requests + BeautifulSoup)
# -------------------------
def scrape_bing_for_company(query: str, user_agent: str) -> Dict:
    """
    Return: { 'bing_phone': ..., 'bing_website': ..., 'aggregator_links': [..] }
    """
    url = "https://www.bing.com/search?q=" + quote_plus(query)
    headers = {"User-Agent": user_agent}
    r = safe_get(url, headers=headers)
    if not r:
        return {"bing_phone": "", "bing_website": "", "aggregator_links": []}

    soup = BeautifulSoup(r.text, "html.parser")

    # Try to find a phone in snippet-like spans
    bing_phone = ""
    for span in soup.find_all("p"):
        txt = span.get_text(strip=True)
        if any(word in txt.lower() for word in ["phone", "tel", "contact"]):
            # naive extraction of phone-looking substrings
            phones = re.findall(r"(\+?\d[\d -]{6,}\d)", txt)
            if phones:
                bing_phone = phones[0]
                break

    # website: first non-bing external link
    bing_website = ""
    for a in soup.select("li.b_algo h2 a"):
        href = a.get("href", "")
        if href and "bing.com" not in href:
            bing_website = href
            break

    # aggregator links
    aggregator_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for dom in AGGREGATOR_DOMAINS:
            if dom in href and href not in aggregator_links:
                aggregator_links.append(href)

    return {
        "bing_phone": bing_phone,
        "bing_website": bing_website,
        "aggregator_links": aggregator_links
    }

# -------------------------
# Google Maps Scraper using Playwright
# -------------------------
maps_semaphore = threading.Semaphore(MAPS_CONCURRENCY)

def scrape_google_maps(query: str, user_agent: str) -> Dict:
    """
    Query Google Maps, return phone, website, rating, category.
    This uses Playwright and runs in its own small browser instance.
    """
    result = {"maps_phone": "", "maps_website": "", "maps_rating": "", "maps_category": ""}

    # Acquire semaphore to limit concurrent Playwright instances
    acquired = maps_semaphore.acquire(timeout=60)
    if not acquired:
        logging.warning("Timeout acquiring maps semaphore; skipping maps for query: " + query)
        return result

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # change to False if you want to debug visually
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()

            search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"
            logging.debug("Maps URL: " + search_url)
            page.goto(search_url, timeout=30000)
            # Wait a little for page to render
            time.sleep(random.uniform(2.0, 4.0))

            # Try to capture the panel details. Selectors can change; attempt multiple fallbacks.
            def safe_text(selector):
                try:
                    el = page.query_selector(selector)
                    return el.inner_text().strip() if el else ""
                except:
                    return ""

            # Phone: look for button with phone or span containing phone-like
            phone_text = ""
            # common maps selector: button[data-tooltip] or div[aria-label] or span
            phone_text = safe_text("button[aria-label*='Call']") or safe_text("button[aria-label*='Call'] span")
            if not phone_text:
                # fallback: search for text with +91 or digits
                all_text = page.inner_text("body")
                phones = re.findall(r"(\+?\d[\d\s\-]{6,}\d)", all_text)
                phone_text = phones[0] if phones else ""

            website = ""
            # maps often has a website link in a button with data-item-id attr
            website = safe_text("a[data-item-id='authority']") or safe_text("a[aria-label*='Website']")
            if not website:
                # Find link-like anchors
                anchors = page.query_selector_all("a")
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    if href.startswith("http") and "google" not in href:
                        website = href
                        break

            rating = safe_text("span[aria-hidden='true']")  # hacky fallback
            category = safe_text(".section-result-details-container .section-result-location-type") or ""

            result.update({
                "maps_phone": phone_text,
                "maps_website": website,
                "maps_rating": rating,
                "maps_category": category
            })

            context.close()
            browser.close()
    except Exception as e:
        logging.debug(f"Google Maps scrape error for {query}: {e}")
    finally:
        maps_semaphore.release()

    return result

# -------------------------
# Website Email Extractor
# -------------------------
def extract_emails_from_website(url: str, user_agent: str) -> List[str]:
    if not url:
        return []
    headers = {"User-Agent": user_agent}
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if not r or r.status_code >= 400:
            return []
        text = r.text
        emails = extract_emails_from_text(text)

        # Also look for mailto links
        soup = BeautifulSoup(text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href.split("mailto:")[1].split("?")[0]
                if email:
                    emails.append(email)
        return sorted(set(emails))
    except Exception as e:
        logging.debug(f"Email extraction error for {url}: {e}")
        return []

# -------------------------
# Multi-query generator
# -------------------------
FALLBACK_QUERIES = [
    "{name} {address} contact",
    "{name} {address} phone",
    "{name} {city} contact"  # city extraction naive: last chunk of address
]

def fallback_queries(name: str, address: str) -> List[str]:
    city = ""
    if "," in address:
        city = address.split(",")[-1].strip()
    else:
        parts = address.split()
        city = parts[-1] if parts else ""
    qs = [q.format(name=name, address=address, city=city) for q in FALLBACK_QUERIES]
    # primary query first
    return [f"{name} {address}"] + qs

# -------------------------
# Company Processor (single company)
# -------------------------
def process_company(row: Dict, worker_id: int) -> Dict:
    """
    row: dict with keys name, address
    returns result dict
    """
    name = row.get("name", "").strip()
    address = row.get("address", "").strip()
    result = {
        "name": name,
        "address": address,
        "bing_phone": "",
        "bing_website": "",
        "aggregator_links": [],
        "maps_phone": "",
        "maps_website": "",
        "maps_rating": "",
        "maps_category": "",
        "emails": [],
        "queries_tried": []
    }

    user_agent = random.choice(USER_AGENTS)

    queries = fallback_queries(name, address)
    for q in queries[:RETRIES_PER_QUERY]:
        try:
            logging.info(f"[W{worker_id}] Querying Bing for: {q}")
            result["queries_tried"].append(q)
            bing = scrape_bing_for_company(q, user_agent)
            # prefer first successful website/phone
            if bing.get("bing_phone") and not result["bing_phone"]:
                result["bing_phone"] = bing["bing_phone"]
            if bing.get("bing_website") and not result["bing_website"]:
                result["bing_website"] = bing["bing_website"]
            result["aggregator_links"].extend(bing.get("aggregator_links", []))

            # If we have website, attempt email extraction
            if result["bing_website"]:
                result["bing_website"] = normalize_url(result["bing_website"])
                emails = extract_emails_from_website(result["bing_website"], user_agent)
                if emails:
                    result["emails"].extend(emails)

            # If minimal results, continue to fallback queries
            if result["bing_phone"] or result["bing_website"]:
                break
            random_delay()
        except Exception as e:
            logging.debug(f"[W{worker_id}] Error during bing attempts for {name}: {e}")

    # Google Maps (always try at least once, but limited concurrency)
    try:
        logging.info(f"[W{worker_id}] Querying Google Maps for: {name} | {address}")
        gm = scrape_google_maps(f"{name} {address}", user_agent)
        # If maps provided phone/website, prefer maps (but keep bing too)
        if gm.get("maps_phone"):
            result["maps_phone"] = gm["maps_phone"]
        if gm.get("maps_website"):
            result["maps_website"] = gm["maps_website"]
        if gm.get("maps_rating"):
            result["maps_rating"] = gm["maps_rating"]
        if gm.get("maps_category"):
            result["maps_category"] = gm["maps_category"]

        # If maps website exists and we didn't get emails earlier, try extract
        site_to_check = result["maps_website"] or result["bing_website"]
        if site_to_check and not result["emails"]:
            site_to_check = normalize_url(site_to_check)
            emails = extract_emails_from_website(site_to_check, user_agent)
            if emails:
                result["emails"].extend(emails)
    except Exception as e:
        logging.debug(f"[W{worker_id}] Google Maps error for {name}: {e}")

    # normalize aggregator links list (unique)
    result["aggregator_links"] = sorted(list(set(result["aggregator_links"])))

    # normalize emails unique
    result["emails"] = sorted(list(set(result["emails"])))

    # small random delay before finishing
    random_delay()
    return result

# -------------------------
# Batch Processor
# -------------------------
def process_batch(batch_df: pd.DataFrame, batch_num: int):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    output_path = os.path.join(OUTPUT_FOLDER, f"batch_{batch_num+1}.xlsx")
    if os.path.exists(output_path):
        logging.info(f"Batch {batch_num+1} already exists at {output_path}, skipping.")
        return

    results = []
    total = len(batch_df)
    logging.info(f"Processing batch {batch_num+1} with {total} companies (workers={MAX_WORKERS})")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for idx, row in batch_df.iterrows():
            worker_id = (idx % MAX_WORKERS) + 1
            futures[executor.submit(process_company, row, worker_id)] = idx

        for fut in as_completed(futures):
            try:
                res = fut.result()
                results.append(res)
            except Exception as e:
                logging.error("Worker failed with exception: " + str(e))

    # Save results to Excel
    df_out = pd.DataFrame(results)
    # For nicer Excel columns flatten lists to comma-separated strings
    df_out["aggregator_links"] = df_out["aggregator_links"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    df_out["emails"] = df_out["emails"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    df_out.to_excel(output_path, index=False)
    logging.info(f"Saved batch {batch_num+1} results to {output_path}")

# -------------------------
# Main runner
# -------------------------
def run_all():
    if not os.path.exists(INPUT_FILE):
        logging.error(f"Input file not found: {INPUT_FILE}")
        return

    df = pd.read_excel(INPUT_FILE)
    if "name" not in df.columns or "address" not in df.columns:
        logging.error("INPUT_FILE must contain 'name' and 'address' columns")
        return

    total_companies = len(df)
    total_batches = (total_companies + BATCH_SIZE - 1) // BATCH_SIZE
    logging.info(f"Total companies: {total_companies} → {total_batches} batches (size {BATCH_SIZE})")

    for batch_num in range(total_batches):
        start = batch_num * BATCH_SIZE
        end = min(start + BATCH_SIZE, total_companies)
        batch_df = df.iloc[start:end].reset_index(drop=True)
        process_batch(batch_df, batch_num)

    logging.info("All batches processed.")

if __name__ == "__main__":
    run_all()
