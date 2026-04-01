"""
carIQ Price Intelligence Scraper v3
=====================================
Architecture:
  Cars24 / Spinny  →  httpx direct API calls (React SPAs, data via API)
  OLX / CarDekho   →  httpx fetch + BeautifulSoup HTML parsing (SSR pages)
"""

import asyncio
import re
import random
import logging
import argparse
import statistics
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from collections import defaultdict
import os
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CITIES = ["mumbai", "delhi", "bangalore", "pune", "hyderabad", "chennai"]

MODELS_TO_TRACK = [
    {"brand": "maruti", "model": "swift"},
    {"brand": "maruti", "model": "baleno"},
    {"brand": "maruti", "model": "wagon-r"},
    {"brand": "maruti", "model": "dzire"},
    {"brand": "hyundai", "model": "creta"},
    {"brand": "hyundai", "model": "i20"},
    {"brand": "hyundai", "model": "venue"},
    {"brand": "tata", "model": "nexon"},
    {"brand": "honda", "model": "city"},
    {"brand": "kia", "model": "seltos"},
]

# OLX city name mapping
OLX_CITY_NAMES = {
    "mumbai": "mumbai",
    "delhi": "delhi",
    "bangalore": "bangalore",
    "pune": "pune",
    "hyderabad": "hyderabad",
    "chennai": "chennai",
}

OLX_TRANSACTION_DISCOUNT = 0.85

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

API_HEADERS = {**HEADERS, "Accept": "application/json, text/plain, */*"}


@dataclass
class CertifiedListing:
    source: str
    brand: str
    model: str
    variant: str
    year: int
    fuel: str
    transmission: str
    km_driven: int
    city: str
    asking_price: int
    certification: str
    warranty_months: int
    listing_url: str
    scraped_at: str


@dataclass
class PrivateListing:
    source: str
    brand: str
    model: str
    variant: str
    year: int
    fuel: str
    transmission: str
    km_driven: int
    city: str
    asking_price: int
    seller_type: str
    days_listed: int
    listing_url: str
    scraped_at: str


# ─────────────────────────────────────────────
# PRICE PARSING
# ─────────────────────────────────────────────

def parse_price(text: str) -> Optional[int]:
    """Parse price strings like '₹6.85 Lakh', '₹ 4,60,000', '685000'"""
    if not text:
        return None
    text = str(text).replace(",", "").strip()
    # Lakh format: "6.85 Lakh" or "6.85 L"
    m = re.search(r"(\d+\.?\d*)\s*[Ll]akh", text, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d+\.?\d*)\s*[Ll]\b", text)
    if m:
        return int(float(m.group(1)) * 100000)
    # Plain number
    m = re.search(r"(\d{4,})", text.replace(" ", ""))
    if m:
        val = int(m.group(1))
        return val if val >= 50000 else None
    return None


def parse_km(text: str) -> int:
    """Parse km strings: '73,000 km', '42960 kms'"""
    if not text:
        return 0
    m = re.search(r"(\d[\d,]*)", text.replace(",", ""))
    return int(m.group(1)) if m else 0


# ─────────────────────────────────────────────
# SCRAPER 1: CARS24 BUY SECTION (httpx API)
# ─────────────────────────────────────────────

async def scrape_cars24(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  Cars24: {brand} {model} in {city}")

    endpoints = [
        f"https://www.cars24.com/api/v4/listings?city={city}&make={brand}&model={model}&page=1&size=24",
        f"https://www.cars24.com/api/v3/used-car/search?city={city}&make={brand}&model={model}&page=1&size=24",
        f"https://api.cars24.com/buyer-service/v1/car-listing?city={city}&make={brand}&model={model}&page=0&size=24",
    ]

    for url in endpoints:
        try:
            resp = await client.get(url, headers=API_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = _extract_items(data)
            if not items:
                continue

            now = datetime.now(timezone.utc).isoformat()
            for item in items[:24]:
                price_raw = (item.get("price") or item.get("resalePrice")
                             or item.get("sp") or item.get("askingPrice"))
                if not price_raw:
                    continue
                price = (int(price_raw) if isinstance(price_raw, (int, float))
                         else parse_price(str(price_raw)))
                if not price:
                    continue
                listings.append(CertifiedListing(
                    source="cars24", brand=brand, model=model,
                    variant=str(item.get("variant", item.get("variantName", ""))),
                    year=int(item.get("year", item.get("modelYear", 0)) or 0),
                    fuel=str(item.get("fuelType", item.get("fuel", ""))).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("kmDriven", item.get("odometerReading", 0)) or 0),
                    city=city, asking_price=price,
                    certification="200-point checked", warranty_months=12,
                    listing_url=f"https://www.cars24.com/car-details/{item.get('id', '')}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception as e:
            log.warning(f"    Cars24 endpoint failed ({url}): {e}")

    log.info(f"    Got {len(listings)} Cars24 listings")
    return listings


# ─────────────────────────────────────────────
# SCRAPER 2: SPINNY BUY SECTION (httpx API)
# ─────────────────────────────────────────────

async def scrape_spinny(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  Spinny: {brand} {model} in {city}")

    endpoints = [
        f"https://www.spinny.com/api/v2/listing/?make={brand.title()}&model={model.title()}&city={city}&page=1&page_size=24",
        f"https://www.spinny.com/api/v3/cars/?make={brand}&model={model}&city={city}&page=1&size=24",
    ]

    for url in endpoints:
        try:
            resp = await client.get(url, headers=API_HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = _extract_items(data)
            if not items:
                continue

            now = datetime.now(timezone.utc).isoformat()
            for item in items[:24]:
                price_raw = (item.get("sp") or item.get("price")
                             or item.get("selling_price"))
                if not price_raw:
                    continue
                price = (int(price_raw) if isinstance(price_raw, (int, float))
                         else parse_price(str(price_raw)))
                if not price:
                    continue
                listings.append(CertifiedListing(
                    source="spinny", brand=brand, model=model,
                    variant=str(item.get("variant", item.get("sub_model", ""))),
                    year=int(item.get("year", item.get("registration_year", 0)) or 0),
                    fuel=str(item.get("fuel_type", item.get("fuel", ""))).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("km_driven", item.get("kms_driven", 0)) or 0),
                    city=city, asking_price=price,
                    certification="Spinny Assured", warranty_months=12,
                    listing_url=f"https://www.spinny.com/used-cars/{item.get('slug', item.get('id', ''))}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception as e:
            log.warning(f"    Spinny endpoint failed ({url}): {e}")

    log.info(f"    Got {len(listings)} Spinny listings")
    return listings


def _extract_items(data) -> list:
    """Recursively find a list of car items in an API response"""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["content", "results", "cars", "carList", "data", "listings"]:
            val = data.get(key)
            if isinstance(val, list) and val:
                return val
            if isinstance(val, dict):
                inner = _extract_items(val)
                if inner:
                    return inner
    return []


# ─────────────────────────────────────────────
# SCRAPER 3: OLX (httpx + BeautifulSoup)
# Confirmed selectors from live inspection:
#   li[data-aut-id]           → each listing card
#   [data-aut-id="itemPrice"] → "₹ 4,60,000"
#   [data-aut-id="itemTitle"] → "Maruti Suzuki Swift"
#   [data-aut-id="itemSubTitle"] → "2021 - 73,000 km"
#   a[href]                   → listing URL
# ─────────────────────────────────────────────

async def scrape_olx(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  OLX: {brand} {model} in {city}")

    city_name = OLX_CITY_NAMES.get(city, city)
    query = f"{brand}-{model}"
    url = (
        f"https://www.olx.in/cars_c84/q-{query}"
        f"?search%5Blocations%5D%5B0%5D%5Bname%5D={city_name}"
    )

    try:
        resp = await client.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            log.warning(f"    OLX returned {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select("li[data-aut-id]")
        now = datetime.now(timezone.utc).isoformat()

        for card in cards:
            try:
                price_el = card.select_one('[data-aut-id="itemPrice"]')
                title_el = card.select_one('[data-aut-id="itemTitle"]')
                subtitle_el = card.select_one('[data-aut-id="itemSubTitle"]')
                link_el = card.select_one("a[href]")

                if not price_el:
                    continue

                price = parse_price(price_el.get_text())
                if not price:
                    continue

                # subtitle format: "2021 - 73,000 km"
                subtitle = subtitle_el.get_text() if subtitle_el else ""
                year_m = re.search(r"(20\d{2})", subtitle)
                km_m = re.search(r"([\d,]+)\s*km", subtitle, re.IGNORECASE)
                year = int(year_m.group(1)) if year_m else 0
                km = parse_km(km_m.group(1)) if km_m else 0

                title = title_el.get_text().strip() if title_el else f"{brand} {model}"
                href = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.olx.in" + href

                listings.append(PrivateListing(
                    source="olx", brand=brand, model=model,
                    variant=title, year=year,
                    fuel="petrol", transmission="manual",
                    km_driven=km, city=city,
                    asking_price=price, seller_type="individual",
                    days_listed=0, listing_url=href,
                    scraped_at=now,
                ))
            except Exception as e:
                log.warning(f"    OLX card parse error: {e}")

    except Exception as e:
        log.error(f"    OLX scrape failed for {brand} {model} {city}: {e}")

    log.info(f"    Got {len(listings)} OLX listings")
    return listings


# ─────────────────────────────────────────────
# SCRAPER 4: CARDEKHO (httpx + BeautifulSoup)
# Confirmed selectors from live inspection:
#   .cardColumn                → each listing card
#   .Price                     → "₹6.85 Lakh"
#   title text pattern         → "2021 Maruti Swift ZXI\n70,000 kms • Petrol • Manual"
#   a[href]                    → listing URL
# ─────────────────────────────────────────────

async def scrape_cardekho(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  CarDekho: {brand} {model} in {city}")

    url = f"https://www.cardekho.com/used-cars+in+{city}/{brand}+{model}"

    try:
        resp = await client.get(url, headers=HEADERS, timeout=20)
        if resp.status_code not in (200, 301, 302):
            log.warning(f"    CarDekho returned {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "lxml")
        cards = soup.select(".cardColumn")

        if not cards:
            # Fallback selector
            cards = soup.select(".NewUcExCard")

        now = datetime.now(timezone.utc).isoformat()

        for card in cards:
            try:
                price_el = card.select_one(".Price")
                link_el = card.select_one("a[href]")

                if not price_el:
                    continue

                price = parse_price(price_el.get_text())
                if not price:
                    continue

                # Full card text: "2021 Maruti Suzuki Swift ZXI\n70,000 kms • Petrol • Manual"
                card_text = card.get_text(" ", strip=True)

                year_m = re.search(r"\b(20\d{2})\b", card_text)
                km_m = re.search(r"([\d,]+)\s*kms?", card_text, re.IGNORECASE)
                fuel_m = re.search(r"\b(Petrol|Diesel|CNG|Electric|LPG)\b", card_text, re.IGNORECASE)
                trans_m = re.search(r"\b(Manual|Automatic|AMT|CVT|DCT)\b", card_text, re.IGNORECASE)

                # Title from h3 or first heading
                title_el = card.select_one("h3, h2, [class*='title'], [class*='Title']")
                title = title_el.get_text().strip() if title_el else card_text[:60]

                href = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.cardekho.com" + href

                listings.append(PrivateListing(
                    source="cardekho", brand=brand, model=model,
                    variant=title,
                    year=int(year_m.group(1)) if year_m else 0,
                    fuel=fuel_m.group(1).lower() if fuel_m else "petrol",
                    transmission=trans_m.group(1).lower() if trans_m else "manual",
                    km_driven=parse_km(km_m.group(1)) if km_m else 0,
                    city=city, asking_price=price,
                    seller_type="individual", days_listed=0,
                    listing_url=href, scraped_at=now,
                ))
            except Exception as e:
                log.warning(f"    CarDekho card parse error: {e}")

    except Exception as e:
        log.error(f"    CarDekho scrape failed for {brand} {model} {city}: {e}")

    log.info(f"    Got {len(listings)} CarDekho listings")
    return listings


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

class Database:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def save_certified(self, listings: list):
        if not listings:
            return
        try:
            self.client.table("certified_listings").upsert(
                [asdict(l) for l in listings], on_conflict="source,listing_url"
            ).execute()
            log.info(f"  Saved {len(listings)} certified listings")
        except Exception as e:
            log.error(f"  DB save error: {e}")

    def save_private(self, listings: list):
        if not listings:
            return
        try:
            self.client.table("private_listings").upsert(
                [asdict(l) for l in listings], on_conflict="source,listing_url"
            ).execute()
            log.info(f"  Saved {len(listings)} private listings")
        except Exception as e:
            log.error(f"  DB save error: {e}")

    def compute_estimates(self):
        log.info("Computing price estimates...")
        try:
            certified = self.client.table("certified_listings").select("*").execute().data or []
            private = self.client.table("private_listings").select("*").execute().data or []
        except Exception as e:
            log.error(f"  DB fetch error: {e}")
            return

        cert_groups = defaultdict(list)
        for l in certified:
            cert_groups[(l["brand"], l["model"], l["year"], l["city"])].append(l["asking_price"])

        priv_groups = defaultdict(list)
        for l in private:
            if l.get("seller_type") != "dealer":
                priv_groups[(l["brand"], l["model"], l["year"], l["city"])].append(l["asking_price"])

        all_keys = set(cert_groups) | set(priv_groups)
        now = datetime.now(timezone.utc).isoformat()
        estimates = []

        for key in all_keys:
            brand, model, year, city = key
            cp = sorted(cert_groups.get(key, []))
            pp = sorted(priv_groups.get(key, []))
            if not cp and not pp:
                continue
            pm = int(statistics.median(pp)) if pp else None
            pp25 = pp[len(pp) // 4] if len(pp) >= 4 else (pp[0] if pp else None)
            pp75 = pp[3 * len(pp) // 4] if len(pp) >= 4 else (pp[-1] if pp else None)
            cm = int(statistics.median(cp)) if cp else None
            sample = len(cp) + len(pp)
            estimates.append({
                "brand": brand, "model": model, "year": year, "city": city,
                "certified_low": min(cp) if cp else None,
                "certified_high": max(cp) if cp else None,
                "certified_median": cm,
                "certified_sample_size": len(cp),
                "private_asking_low": pp25,
                "private_asking_high": pp75,
                "private_asking_median": pm,
                "private_sample_size": len(pp),
                "realistic_low": int(pp25 * OLX_TRANSACTION_DISCOUNT) if pp25 else None,
                "realistic_high": int(pm * OLX_TRANSACTION_DISCOUNT) if pm else None,
                "discount_applied": OLX_TRANSACTION_DISCOUNT,
                "dealer_premium": (cm - pm) if cm and pm else None,
                "confidence_score": min(95, 50 + sample * 3),
                "demand_signal": "high" if sample > 20 else "medium" if sample > 10 else "low",
                "updated_at": now,
            })

        if estimates:
            try:
                self.client.table("price_estimates").upsert(
                    estimates, on_conflict="brand,model,year,city"
                ).execute()
                log.info(f"  Computed {len(estimates)} price estimates")
            except Exception as e:
                log.error(f"  DB estimates error: {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

async def run():
    db = Database()
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
        for car in MODELS_TO_TRACK:
            brand, model = car["brand"], car["model"]
            for city in CITIES:
                log.info(f"\n{'='*50}")
                log.info(f"Scraping: {brand} {model} | {city}")
                log.info(f"{'='*50}")

                await asyncio.sleep(random.uniform(1.0, 2.0))
                c24 = await scrape_cars24(client, brand, model, city)
                db.save_certified(c24)

                await asyncio.sleep(random.uniform(0.5, 1.5))
                sp = await scrape_spinny(client, brand, model, city)
                db.save_certified(sp)

                await asyncio.sleep(random.uniform(0.5, 1.5))
                olx = await scrape_olx(client, brand, model, city)
                db.save_private(olx)

                await asyncio.sleep(random.uniform(0.5, 1.5))
                cd = await scrape_cardekho(client, brand, model, city)
                db.save_private(cd)

    log.info("\nAll scraping done. Computing estimates...")
    db.compute_estimates()
    log.info("Complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="carIQ Price Scraper v3")
    parser.add_argument("--source", help="cars24|spinny|olx|cardekho")
    parser.add_argument("--compute-only", action="store_true")
    args = parser.parse_args()
    if args.compute_only:
        Database().compute_estimates()
    else:
        asyncio.run(run())
