"""
carIQ Price Intelligence Scraper v2
=====================================
Uses direct HTTP API calls instead of Playwright browser automation.
Playwright gets blocked by bot detection on GitHub Actions IPs.
httpx API calls are faster, lighter, and bypass bot detection.
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
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CITIES = ["mumbai", "delhi", "bangalore", "pune", "hyderabad", "chennai"]

CITY_IDS = {
    "mumbai": "1000070",
    "delhi": "1000030",
    "bangalore": "1000010",
    "pune": "1000099",
    "hyderabad": "1000050",
    "chennai": "1000020",
}

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

OLX_TRANSACTION_DISCOUNT = 0.85

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Connection": "keep-alive",
}


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


def parse_price(price_str: str):
    if not price_str:
        return None
    price_str = str(price_str).replace(",", "").replace("Rs", "").strip()
    lakh_match = re.search(r"(\d+\.?\d*)\s*[Ll]", price_str)
    if lakh_match:
        return int(float(lakh_match.group(1)) * 100000)
    num_match = re.search(r"(\d+)", price_str)
    if num_match:
        val = int(num_match.group(1))
        if val > 10000:
            return val
    return None


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
            resp = await client.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for key in ["content", "cars", "results", "carList", "data"]:
                    val = data.get(key)
                    if isinstance(val, list) and val:
                        items = val
                        break
                    if isinstance(val, dict):
                        inner = val.get("content") or val.get("cars") or val.get("results")
                        if isinstance(inner, list) and inner:
                            items = inner
                            break
            if not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:24]:
                price = item.get("price") or item.get("resalePrice") or item.get("sp") or item.get("askingPrice")
                if not price:
                    continue
                price_int = int(price) if isinstance(price, (int, float)) else parse_price(str(price))
                if not price_int or price_int < 50000:
                    continue
                listings.append(CertifiedListing(
                    source="cars24", brand=brand, model=model,
                    variant=str(item.get("variant", item.get("variantName", ""))),
                    year=int(item.get("year", item.get("modelYear", 0)) or 0),
                    fuel=str(item.get("fuelType", item.get("fuel", ""))).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("kmDriven", item.get("odometerReading", 0)) or 0),
                    city=city, asking_price=price_int,
                    certification="200-point checked", warranty_months=12,
                    listing_url=f"https://www.cars24.com/car-details/{item.get('id', item.get('carId', ''))}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception as e:
            log.warning(f"    Cars24 endpoint failed: {e}")
    log.info(f"    Got {len(listings)} Cars24 listings")
    return listings


async def scrape_spinny(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  Spinny: {brand} {model} in {city}")
    endpoints = [
        f"https://www.spinny.com/api/v2/listing/?make={brand.title()}&model={model.title()}&city={city}&page=1&page_size=24",
        f"https://www.spinny.com/api/v3/cars/?make={brand}&model={model}&city={city}&page=1&size=24",
    ]
    for url in endpoints:
        try:
            resp = await client.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for key in ["results", "cars", "data", "carList"]:
                    val = data.get(key)
                    if isinstance(val, list) and val:
                        items = val
                        break
            if not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:24]:
                price = item.get("sp") or item.get("price") or item.get("selling_price")
                if not price:
                    continue
                price_int = int(price) if isinstance(price, (int, float)) else parse_price(str(price))
                if not price_int or price_int < 50000:
                    continue
                listings.append(CertifiedListing(
                    source="spinny", brand=brand, model=model,
                    variant=str(item.get("variant", item.get("sub_model", ""))),
                    year=int(item.get("year", item.get("registration_year", 0)) or 0),
                    fuel=str(item.get("fuel_type", item.get("fuel", ""))).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("km_driven", item.get("kms_driven", 0)) or 0),
                    city=city, asking_price=price_int,
                    certification="Spinny Assured", warranty_months=12,
                    listing_url=f"https://www.spinny.com/used-cars/{item.get('slug', item.get('id', ''))}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception as e:
            log.warning(f"    Spinny endpoint failed: {e}")
    log.info(f"    Got {len(listings)} Spinny listings")
    return listings


async def scrape_olx(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  OLX: {brand} {model} in {city}")
    city_id = CITY_IDS.get(city, "")
    endpoints = [
        f"https://www.olx.in/api/relevance/v4/search?category=1320&location={city_id}&query={brand}+{model}&page=1&size=25",
        f"https://www.olx.in/api/relevance/v3/search?category_id=84&location={city_id}&query={brand}+{model}&page=1",
    ]
    for url in endpoints:
        try:
            resp = await client.get(url, timeout=20)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data.get("data") if isinstance(data, dict) else data
            if not isinstance(items, list) or not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:25]:
                try:
                    price_data = item.get("price", {})
                    if isinstance(price_data, dict):
                        price_val = price_data.get("value", {})
                        price_raw = price_val.get("raw") if isinstance(price_val, dict) else price_val
                    else:
                        price_raw = price_data
                    if not price_raw:
                        price_raw = item.get("priceValue") or item.get("price_value")
                    price_int = parse_price(str(price_raw)) if price_raw else None
                    if not price_int or price_int < 50000:
                        continue
                    title = item.get("title", f"{brand} {model}")
                    year_match = re.search(r"(20\d{2})", title)
                    listings.append(PrivateListing(
                        source="olx", brand=brand, model=model, variant=title,
                        year=int(year_match.group(1)) if year_match else 0,
                        fuel="petrol", transmission="manual", km_driven=0,
                        city=city, asking_price=price_int,
                        seller_type="individual", days_listed=0,
                        listing_url=f"https://www.olx.in/item/{item.get('id', '')}",
                        scraped_at=now,
                    ))
                except Exception:
                    pass
            if listings:
                break
        except Exception as e:
            log.warning(f"    OLX endpoint failed: {e}")
    log.info(f"    Got {len(listings)} OLX listings")
    return listings


async def scrape_cardekho(client: httpx.AsyncClient, brand: str, model: str, city: str) -> list:
    listings = []
    log.info(f"  CarDekho: {brand} {model} in {city}")
    endpoints = [
        f"https://www.cardekho.com/api/v3/used-car/listing?make={brand.title()}&model={model.title()}&city={city}&page=1&count=24",
        f"https://api.cardekho.com/v1/usedcar/search?make={brand}&model={model}&city={city}&page=1&size=24",
    ]
    for url in endpoints:
        try:
            resp = await client.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for key in ["carList", "cars", "results", "data"]:
                    val = data.get(key)
                    if isinstance(val, list) and val:
                        items = val
                        break
            if not items:
                continue
            now = datetime.now(timezone.utc).isoformat()
            for item in items[:24]:
                price = item.get("price") or item.get("askingPrice") or item.get("sp")
                if not price:
                    continue
                price_int = int(price) if isinstance(price, (int, float)) else parse_price(str(price))
                if not price_int or price_int < 50000:
                    continue
                listings.append(PrivateListing(
                    source="cardekho", brand=brand, model=model,
                    variant=str(item.get("variant", "")),
                    year=int(item.get("year", item.get("modelYear", 0)) or 0),
                    fuel=str(item.get("fuelType", "")).lower(),
                    transmission=str(item.get("transmission", "")).lower(),
                    km_driven=int(item.get("km", item.get("kmDriven", 0)) or 0),
                    city=city, asking_price=price_int,
                    seller_type="individual", days_listed=0,
                    listing_url=f"https://www.cardekho.com/used-car-details/{item.get('id', '')}",
                    scraped_at=now,
                ))
            if listings:
                break
        except Exception as e:
            log.warning(f"    CarDekho endpoint failed: {e}")
    log.info(f"    Got {len(listings)} CarDekho listings")
    return listings


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
            log.error(f"  DB error: {e}")

    def save_private(self, listings: list):
        if not listings:
            return
        try:
            self.client.table("private_listings").upsert(
                [asdict(l) for l in listings], on_conflict="source,listing_url"
            ).execute()
            log.info(f"  Saved {len(listings)} private listings")
        except Exception as e:
            log.error(f"  DB error: {e}")

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
            pp25 = pp[len(pp) // 4] if pp else None
            pp75 = pp[3 * len(pp) // 4] if pp else None
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
                self.client.table("price_estimates").upsert(estimates, on_conflict="brand,model,year,city").execute()
                log.info(f"  Computed {len(estimates)} estimates")
            except Exception as e:
                log.error(f"  DB estimate error: {e}")


async def run():
    db = Database()
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        for car in MODELS_TO_TRACK:
            brand, model = car["brand"], car["model"]
            for city in CITIES:
                log.info(f"\n{'='*50}\nScraping: {brand} {model} | {city}\n{'='*50}")
                await asyncio.sleep(random.uniform(0.8, 2.0))
                c24 = await scrape_cars24(client, brand, model, city)
                db.save_certified(c24)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                sp = await scrape_spinny(client, brand, model, city)
                db.save_certified(sp)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                olx = await scrape_olx(client, brand, model, city)
                db.save_private(olx)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                cd = await scrape_cardekho(client, brand, model, city)
                db.save_private(cd)
    log.info("\nAll done. Computing estimates...")
    db.compute_estimates()
    log.info("Complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source")
    parser.add_argument("--compute-only", action="store_true")
    args = parser.parse_args()
    if args.compute_only:
        Database().compute_estimates()
    else:
        asyncio.run(run())
