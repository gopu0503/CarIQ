"""
carIQ Price Intelligence Scraper v4
=====================================
Data sources:
  CarDekho  →  JSON-LD ItemList in raw HTML (confirmed SSR, works without cookies)
  OLX       →  data-aut-id selectors in raw HTML (confirmed SSR, works without cookies)  
  Cars24    →  httpx API attempts (blocked from datacenter IPs, graceful fallback)
  Spinny    →  httpx API attempts (blocked from datacenter IPs, graceful fallback)

CarDekho URL pattern: /used-{brand}-{model}+cars+in+{city}
OLX URL pattern: /cars_c84/q-{brand}-{model}?search[locations][0][name]={city}
"""

import asyncio
import json
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

# CarDekho slug mapping — confirmed URL patterns
CARDEKHO_SLUGS = {
    ("maruti", "swift"):   "maruti-swift",
    ("maruti", "baleno"):  "maruti-baleno",
    ("maruti", "wagon-r"): "maruti-wagon-r",
    ("maruti", "dzire"):   "maruti-swift-dzire",
    ("hyundai", "creta"):  "hyundai-creta",
    ("hyundai", "i20"):    "hyundai-i20-elite",
    ("hyundai", "venue"):  "hyundai-venue",
    ("tata", "nexon"):     "tata-nexon",
    ("honda", "city"):     "honda-city",
    ("kia", "seltos"):     "kia-seltos",
}

MODELS_TO_TRACK = list(CARDEKHO_SLUGS.keys())

OLX_TRANSACTION_DISCOUNT = 0.85

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

API_HEADERS = {**BROWSER_HEADERS, "Accept": "application/json, text/plain, */*"}


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


def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    text = str(text).replace(",", "").strip()
    m = re.search(r"(\d+\.?\d*)\s*[Ll]akh", text, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d+\.?\d*)\s*[Ll]\b", text)
    if m:
        return int(float(m.group(1)) * 100000)
    m = re.search(r"(\d{4,})", text.replace(" ", ""))
    if m:
        val = int(m.group(1))
        return val if val >= 50000 else None
    return None


# ─────────────────────────────────────────────
# CARDEKHO — JSON-LD ItemList extraction
# Confirmed: raw HTML contains full ItemList schema with all 20 cars
# URL: /used-{slug}+cars+in+{city}
# ─────────────────────────────────────────────

async def scrape_cardekho(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  CarDekho: {brand} {model} in {city}")

    slug = CARDEKHO_SLUGS.get((brand, model), f"{brand}-{model}")
    url = f"https://www.cardekho.com/used-{slug}+cars+in+{city}"

    try:
        resp = await client.get(url, headers=BROWSER_HEADERS, timeout=25)
        if resp.status_code not in (200, 301, 302):
            log.warning(f"    CarDekho {resp.status_code} for {url}")
            return listings

        soup = BeautifulSoup(resp.text, "lxml")
        now = datetime.now(timezone.utc).isoformat()

        # Extract JSON-LD ItemList — confirmed present in raw HTML
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                # Handle array of schemas
                items_list = data if isinstance(data, list) else [data]
                for item in items_list:
                    if item.get("@type") == "ItemList" and item.get("itemListElement"):
                        for car in item["itemListElement"]:
                            if car.get("@type") != "Car":
                                continue
                            try:
                                price = car.get("offers", {}).get("price")
                                if not price:
                                    continue
                                price_int = int(price)
                                if price_int < 50000:
                                    continue

                                km_val = car.get("mileageFromOdometer", {}).get("value", 0)
                                fuel = car.get("fuelType", "")
                                transmission = car.get("vehicleTransmission", "")
                                year = int(car.get("vehicleModelDate", 0) or 0)
                                name = car.get("name", f"{brand} {model}")
                                car_url = car.get("url", url)

                                listings.append(PrivateListing(
                                    source="cardekho",
                                    brand=brand, model=model,
                                    variant=name,
                                    year=year,
                                    fuel=str(fuel).lower(),
                                    transmission=str(transmission).lower(),
                                    km_driven=int(km_val or 0),
                                    city=city,
                                    asking_price=price_int,
                                    seller_type="individual",
                                    days_listed=0,
                                    listing_url=car_url,
                                    scraped_at=now,
                                ))
                            except Exception as e:
                                log.warning(f"    CarDekho car parse error: {e}")
                        break  # Found ItemList, stop searching
            except json.JSONDecodeError:
                continue

    except Exception as e:
        log.error(f"    CarDekho failed for {brand} {model} {city}: {e}")

    log.info(f"    Got {len(listings)} CarDekho listings")
    return listings


# ─────────────────────────────────────────────
# OLX — data-aut-id HTML parsing
# Confirmed: data-aut-id="itemPrice" IS in raw HTML without cookies
# OLX may timeout from datacenter IPs — handled gracefully
# ─────────────────────────────────────────────

async def scrape_olx(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  OLX: {brand} {model} in {city}")

    query = f"{brand}-{model}"
    url = (
        f"https://www.olx.in/cars_c84/q-{query}"
        f"?search%5Blocations%5D%5B0%5D%5Bname%5D={city}"
    )

    try:
        resp = await client.get(url, headers=BROWSER_HEADERS, timeout=30)
        if resp.status_code != 200:
            log.warning(f"    OLX {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "lxml")
        now = datetime.now(timezone.utc).isoformat()

        # Confirmed selector: data-aut-id="itemPrice" spans with price text
        price_els = soup.select('[data-aut-id="itemPrice"]')

        for price_el in price_els:
            try:
                price = parse_price(price_el.get_text())
                if not price:
                    continue

                # Walk up to find the listing container
                container = price_el.parent
                for _ in range(5):
                    if container is None:
                        break
                    if container.find('[data-aut-id="itemSubTitle"]') or container.select_one('[data-aut-id="itemSubTitle"]'):
                        break
                    container = container.parent

                subtitle_el = container.select_one('[data-aut-id="itemSubTitle"]') if container else None
                title_el = container.select_one('[data-aut-id="itemTitle"]') if container else None
                link_el = container.find("a", href=True) if container else None

                subtitle = subtitle_el.get_text().strip() if subtitle_el else ""
                # Subtitle format: "2021 - 73,000 km"
                year_m = re.search(r"(20\d{2})", subtitle)
                km_m = re.search(r"([\d,]+)\s*km", subtitle, re.IGNORECASE)

                title = title_el.get_text().strip() if title_el else f"{brand} {model}"
                href = link_el.get("href", "") if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.olx.in" + href

                listings.append(PrivateListing(
                    source="olx",
                    brand=brand, model=model,
                    variant=title,
                    year=int(year_m.group(1)) if year_m else 0,
                    fuel="petrol",
                    transmission="manual",
                    km_driven=int(km_m.group(1).replace(",", "")) if km_m else 0,
                    city=city,
                    asking_price=price,
                    seller_type="individual",
                    days_listed=0,
                    listing_url=href,
                    scraped_at=now,
                ))
            except Exception as e:
                log.warning(f"    OLX item parse error: {e}")

    except Exception as e:
        log.error(f"    OLX failed for {brand} {model} {city}: {e}")

    log.info(f"    Got {len(listings)} OLX listings")
    return listings


# ─────────────────────────────────────────────
# CARS24 — API attempts (blocked from datacenter IPs)
# ─────────────────────────────────────────────

async def scrape_cars24(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  Cars24: {brand} {model} in {city}")
    endpoints = [
        f"https://api.cars24.com/buyer-service/v2/car?city={city}&make={brand}&model={model}&page=0&size=20",
        f"https://www.cars24.com/api/car-search?city={city}&make={brand}&model={model}&page=1&size=20",
    ]
    for url in endpoints:
        try:
            resp = await client.get(url, headers=API_HEADERS, timeout=12)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = _extract_items(data)
            if not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:20]:
                price = item.get("price") or item.get("resalePrice") or item.get("sp")
                if not price:
                    continue
                price_int = int(price) if isinstance(price, (int, float)) else parse_price(str(price))
                if not price_int:
                    continue
                listings.append(CertifiedListing(
                    source="cars24", brand=brand, model=model,
                    variant=str(item.get("variant", "")),
                    year=int(item.get("year", item.get("modelYear", 0)) or 0),
                    fuel=str(item.get("fuelType", "")).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("kmDriven", item.get("odometerReading", 0)) or 0),
                    city=city, asking_price=price_int,
                    certification="200-point checked", warranty_months=12,
                    listing_url=f"https://www.cars24.com/car-details/{item.get('id', '')}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception:
            pass
    log.info(f"    Got {len(listings)} Cars24 listings")
    return listings


async def scrape_spinny(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  Spinny: {brand} {model} in {city}")
    endpoints = [
        f"https://www.spinny.com/api/v2/listing/?make={brand.title()}&model={model.title()}&city={city}&page=1&page_size=20",
    ]
    for url in endpoints:
        try:
            resp = await client.get(url, headers=API_HEADERS, timeout=12)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = _extract_items(data)
            if not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:20]:
                price = item.get("sp") or item.get("price") or item.get("selling_price")
                if not price:
                    continue
                price_int = int(price) if isinstance(price, (int, float)) else parse_price(str(price))
                if not price_int:
                    continue
                listings.append(CertifiedListing(
                    source="spinny", brand=brand, model=model,
                    variant=str(item.get("variant", "")),
                    year=int(item.get("year", item.get("registration_year", 0)) or 0),
                    fuel=str(item.get("fuel_type", "")).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("km_driven", 0) or 0),
                    city=city, asking_price=price_int,
                    certification="Spinny Assured", warranty_months=12,
                    listing_url=f"https://www.spinny.com/used-cars/{item.get('slug', '')}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception:
            pass
    log.info(f"    Got {len(listings)} Spinny listings")
    return listings


def _extract_items(data) -> list:
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
                "certified_median": cm, "certified_sample_size": len(cp),
                "private_asking_low": pp25, "private_asking_high": pp75,
                "private_asking_median": pm, "private_sample_size": len(pp),
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
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for (brand, model) in MODELS_TO_TRACK:
            for city in CITIES:
                log.info(f"\n{'='*50}")
                log.info(f"Scraping: {brand} {model} | {city}")
                log.info(f"{'='*50}")

                await asyncio.sleep(random.uniform(1.5, 3.0))

                cd = await scrape_cardekho(client, brand, model, city)
                db.save_private(cd)

                await asyncio.sleep(random.uniform(1.0, 2.0))

                olx = await scrape_olx(client, brand, model, city)
                db.save_private(olx)

                await asyncio.sleep(random.uniform(0.5, 1.5))

                c24 = await scrape_cars24(client, brand, model, city)
                db.save_certified(c24)

                await asyncio.sleep(random.uniform(0.5, 1.5))

                sp = await scrape_spinny(client, brand, model, city)
                db.save_certified(sp)

    log.info("\nAll done. Computing estimates...")
    db.compute_estimates()
    log.info("Complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="carIQ Price Scraper v4")
    parser.add_argument("--compute-only", action="store_true")
    args = parser.parse_args()
    if args.compute_only:
        Database().compute_estimates()
    else:
        asyncio.run(run())
