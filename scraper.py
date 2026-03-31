"""
carIQ Price Intelligence Scraper
=================================
Scrapes Cars24 (buy), Spinny (buy), OLX, and CarDekho listings nightly.
Stores raw listings in Supabase and computes price estimates.

Setup:
  pip install playwright supabase python-dotenv
  playwright install chromium

Run:
  python scraper.py                    # scrape all sources
  python scraper.py --source cars24    # single source
  python scraper.py --compute-only     # just recompute estimates

Schedule (GitHub Actions or cron):
  0 2 * * *  python scraper.py        # runs at 2am every night
"""

import asyncio
import json
import re
import time
import random
import logging
import argparse
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, Browser
from supabase import create_client, Client

load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Cities to scrape — start with these, expand later
CITIES = ["mumbai", "delhi", "bangalore", "pune", "hyderabad", "chennai"]

# Car models to track — top 20 by volume in India
MODELS_TO_TRACK = [
    {"brand": "maruti", "model": "swift"},
    {"brand": "maruti", "model": "baleno"},
    {"brand": "maruti", "model": "wagon-r"},
    {"brand": "maruti", "model": "dzire"},
    {"brand": "maruti", "model": "brezza"},
    {"brand": "hyundai", "model": "creta"},
    {"brand": "hyundai", "model": "i20"},
    {"brand": "hyundai", "model": "venue"},
    {"brand": "tata", "model": "nexon"},
    {"brand": "tata", "model": "punch"},
    {"brand": "honda", "model": "city"},
    {"brand": "kia", "model": "seltos"},
    {"brand": "mahindra", "model": "xuv300"},
    {"brand": "toyota", "model": "innova"},
    {"brand": "toyota", "model": "fortuner"},
]

# Years to track
YEARS = list(range(2018, 2025))

# Delay between requests — be polite, avoid blocks
MIN_DELAY = 2.0
MAX_DELAY = 5.0

# Realistic transaction discount applied to OLX asking prices
OLX_TRANSACTION_DISCOUNT = 0.85  # 15% below asking

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────

@dataclass
class CertifiedListing:
    """Cars24 / Spinny buy-side listing (certified dealer)"""
    source: str          # 'cars24' or 'spinny'
    brand: str
    model: str
    variant: str
    year: int
    fuel: str
    transmission: str
    km_driven: int
    city: str
    asking_price: int    # what the dealer charges the buyer
    certification: str   # e.g. "200-point checked"
    warranty_months: int
    listing_url: str
    scraped_at: str


@dataclass
class PrivateListing:
    """OLX / CarDekho private seller listing"""
    source: str          # 'olx' or 'cardekho'
    brand: str
    model: str
    variant: str
    year: int
    fuel: str
    transmission: str
    km_driven: int
    city: str
    asking_price: int    # what the private seller is asking
    seller_type: str     # 'individual' or 'dealer'
    days_listed: int
    listing_url: str
    scraped_at: str


# ─────────────────────────────────────────────
# BROWSER UTILITIES
# ─────────────────────────────────────────────

async def get_browser(playwright) -> Browser:
    """Launch browser with anti-detection settings"""
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ]
    )
    return browser


async def get_page(browser: Browser) -> Page:
    """Create a new page with realistic browser fingerprint"""
    context = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        extra_http_headers={
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    page = await context.new_page()

    # Mask webdriver property — key anti-detection step
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-IN', 'en']});
    """)
    return page


async def random_delay():
    """Wait a random amount to appear human"""
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)


def parse_price(price_str: str) -> Optional[int]:
    """
    Parse Indian price formats to integer rupees.
    '₹6.5 L' → 650000
    '₹12,50,000' → 1250000
    '6.5 Lakh' → 650000
    """
    if not price_str:
        return None

    price_str = price_str.replace(",", "").replace("₹", "").replace("\u20b9", "").strip()

    # Handle Lakh format: "6.5 L" or "6.5 Lakh"
    lakh_match = re.search(r"(\d+\.?\d*)\s*[Ll]", price_str)
    if lakh_match:
        return int(float(lakh_match.group(1)) * 100000)

    # Handle Crore format: "1.2 Cr"
    crore_match = re.search(r"(\d+\.?\d*)\s*[Cc]r", price_str)
    if crore_match:
        return int(float(crore_match.group(1)) * 10000000)

    # Handle plain number
    num_match = re.search(r"(\d+)", price_str)
    if num_match:
        val = int(num_match.group(1))
        # If < 1000 it's probably in thousands — skip
        return val if val > 10000 else None

    return None


def parse_km(km_str: str) -> Optional[int]:
    """
    Parse km strings to integer.
    '45,000 km' → 45000
    '1.2 L km' → 120000
    """
    if not km_str:
        return None
    km_str = km_str.replace(",", "").lower()
    lakh_match = re.search(r"(\d+\.?\d*)\s*l", km_str)
    if lakh_match:
        return int(float(lakh_match.group(1)) * 100000)
    num_match = re.search(r"(\d+)", km_str)
    if num_match:
        return int(num_match.group(1))
    return None


# ─────────────────────────────────────────────
# SCRAPER 1: CARS24 BUY SECTION
# ─────────────────────────────────────────────

class Cars24Scraper:
    """
    Scrapes Cars24 buy listings.
    URL pattern: cars24.com/buy-used-{brand}-{model}-cars-{city}/
    """

    BASE_URL = "https://www.cars24.com"

    async def scrape_model_city(
        self, page: Page, brand: str, model: str, city: str
    ) -> list[CertifiedListing]:
        listings = []

        # Cars24 URL format
        url = f"{self.BASE_URL}/buy-used-{brand}-{model}-cars-{city}/"
        log.info(f"  Cars24: {brand} {model} in {city}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await random_delay()

            # Wait for listings to appear
            await page.wait_for_selector(
                "[data-testid='car-card'], .car-card, article",
                timeout=10000
            )

            # Scroll to load more listings
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await asyncio.sleep(1.5)

            # Extract listing cards
            cards = await page.query_selector_all(
                "[data-testid='car-card'], .car-card, article[class*='card']"
            )

            for card in cards[:30]:  # cap at 30 per model/city
                try:
                    listing = await self._parse_card(card, city, page.url)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.warning(f"    Failed to parse card: {e}")

        except Exception as e:
            log.error(f"    Cars24 scrape failed for {brand} {model} {city}: {e}")

            # Fallback: try their internal API
            listings = await self._try_api_fallback(page, brand, model, city)

        log.info(f"    Got {len(listings)} listings")
        return listings

    async def _parse_card(
        self, card, city: str, page_url: str
    ) -> Optional[CertifiedListing]:
        """Parse a single Cars24 listing card"""
        now = datetime.now(timezone.utc).isoformat()

        # Cars24 card structure (inspect their DOM to verify these selectors)
        title_el = await card.query_selector("h3, h2, [class*='title'], [class*='name']")
        price_el = await card.query_selector("[class*='price'], [data-testid*='price']")
        km_el = await card.query_selector("[class*='km'], [class*='kilometer']")
        fuel_el = await card.query_selector("[class*='fuel']")
        link_el = await card.query_selector("a")

        if not title_el or not price_el:
            return None

        title = await title_el.inner_text()
        price_text = await price_el.inner_text()
        km_text = await km_el.inner_text() if km_el else ""
        fuel_text = await fuel_el.inner_text() if fuel_el else ""
        href = await link_el.get_attribute("href") if link_el else ""

        price = parse_price(price_text)
        km = parse_km(km_text)

        if not price:
            return None

        # Parse title: "2021 Maruti Swift VXI"
        year_match = re.search(r"(20\d{2})", title)
        year = int(year_match.group(1)) if year_match else 0

        # Extract brand/model/variant from title
        parts = title.strip().split()
        brand = parts[1].lower() if len(parts) > 1 else ""
        model = parts[2].lower() if len(parts) > 2 else ""
        variant = " ".join(parts[3:]) if len(parts) > 3 else ""

        transmission = "automatic" if any(
            x in title.lower() for x in ["at", "automatic", "amt", "cvt", "dct"]
        ) else "manual"

        return CertifiedListing(
            source="cars24",
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            fuel=fuel_text.lower().strip(),
            transmission=transmission,
            km_driven=km or 0,
            city=city,
            asking_price=price,
            certification="200-point checked",
            warranty_months=12,
            listing_url=f"{self.BASE_URL}{href}" if href.startswith("/") else href,
            scraped_at=now
        )

    async def _try_api_fallback(
        self, page: Page, brand: str, model: str, city: str
    ) -> list[CertifiedListing]:
        """
        Cars24 internal API fallback.
        Intercept their XHR calls to get clean JSON.
        This is often more reliable than DOM scraping.
        """
        listings = []
        responses = []

        # Listen for API responses
        async def handle_response(response):
            if "api" in response.url and "listing" in response.url:
                try:
                    data = await response.json()
                    responses.append(data)
                except Exception:
                    pass

        page.on("response", handle_response)

        # Trigger the page load which will fire the API calls
        url = f"{self.BASE_URL}/buy-used-{brand}-{model}-cars-{city}/"
        await page.goto(url, timeout=30000)
        await asyncio.sleep(4)  # wait for API calls to complete

        page.remove_listener("response", handle_response)

        # Parse the intercepted API responses
        now = datetime.now(timezone.utc).isoformat()
        for response_data in responses:
            items = (
                response_data.get("data", {}).get("content", [])
                or response_data.get("results", [])
                or response_data.get("cars", [])
            )
            for item in items[:30]:
                try:
                    price = item.get("price") or item.get("resalePrice")
                    if not price:
                        continue

                    listings.append(CertifiedListing(
                        source="cars24",
                        brand=item.get("make", brand).lower(),
                        model=item.get("model", model).lower(),
                        variant=item.get("variant", ""),
                        year=item.get("year", item.get("modelYear", 0)),
                        fuel=item.get("fuelType", "").lower(),
                        transmission=item.get("transmission", "").lower(),
                        km_driven=item.get("kmDriven", item.get("odometerReading", 0)),
                        city=city,
                        asking_price=int(price),
                        certification="200-point checked",
                        warranty_months=item.get("warrantyMonths", 12),
                        listing_url=f"{self.BASE_URL}/car-details/{item.get('id', '')}",
                        scraped_at=now
                    ))
                except Exception as e:
                    log.warning(f"    API parse error: {e}")

        return listings


# ─────────────────────────────────────────────
# SCRAPER 2: SPINNY BUY SECTION
# ─────────────────────────────────────────────

class SpinnyScraper:
    """
    Scrapes Spinny buy listings.
    URL pattern: spinny.com/used-cars-in-{city}/{brand}/{model}/
    """

    BASE_URL = "https://www.spinny.com"

    async def scrape_model_city(
        self, page: Page, brand: str, model: str, city: str
    ) -> list[CertifiedListing]:
        listings = []

        url = f"{self.BASE_URL}/used-cars-in-{city}/{brand}/{model}/"
        log.info(f"  Spinny: {brand} {model} in {city}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await random_delay()

            # Spinny is React-rendered — wait for the listing grid
            await page.wait_for_selector(
                ".car-card, [class*='CarCard'], [class*='listing-card']",
                timeout=10000
            )

            # Scroll to trigger lazy loading
            for _ in range(4):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await asyncio.sleep(1.0)

            cards = await page.query_selector_all(
                ".car-card, [class*='CarCard'], [class*='listing-card']"
            )

            for card in cards[:30]:
                try:
                    listing = await self._parse_card(card, city)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.warning(f"    Spinny card parse error: {e}")

        except Exception as e:
            log.error(f"    Spinny scrape failed for {brand} {model} {city}: {e}")

        log.info(f"    Got {len(listings)} listings")
        return listings

    async def _parse_card(self, card, city: str) -> Optional[CertifiedListing]:
        now = datetime.now(timezone.utc).isoformat()

        title_el = await card.query_selector("h2, h3, [class*='name'], [class*='title']")
        price_el = await card.query_selector("[class*='price'], [class*='Price']")
        km_el = await card.query_selector("[class*='km'], [class*='odometer']")
        fuel_el = await card.query_selector("[class*='fuel'], [class*='Fuel']")
        link_el = await card.query_selector("a")

        if not title_el or not price_el:
            return None

        title = await title_el.inner_text()
        price_text = await price_el.inner_text()
        km_text = await km_el.inner_text() if km_el else ""
        fuel_text = await fuel_el.inner_text() if fuel_el else ""
        href = await link_el.get_attribute("href") if link_el else ""

        price = parse_price(price_text)
        km = parse_km(km_text)

        if not price:
            return None

        year_match = re.search(r"(20\d{2})", title)
        year = int(year_match.group(1)) if year_match else 0
        parts = title.strip().split()
        brand = parts[1].lower() if len(parts) > 1 else ""
        model = parts[2].lower() if len(parts) > 2 else ""
        variant = " ".join(parts[3:]) if len(parts) > 3 else ""

        transmission = "automatic" if any(
            x in title.lower() for x in ["at", "automatic", "amt", "cvt"]
        ) else "manual"

        return CertifiedListing(
            source="spinny",
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            fuel=fuel_text.lower().strip(),
            transmission=transmission,
            km_driven=km or 0,
            city=city,
            asking_price=price,
            certification="Spinny Assured",
            warranty_months=12,
            listing_url=f"{self.BASE_URL}{href}" if href.startswith("/") else href,
            scraped_at=now
        )


# ─────────────────────────────────────────────
# SCRAPER 3: OLX LISTINGS
# ─────────────────────────────────────────────

class OLXScraper:
    """
    Scrapes OLX used car listings (private sellers).
    URL: olx.in/cars_c84?q={brand}+{model}&search[city_id]={city_id}
    """

    BASE_URL = "https://www.olx.in"

    # OLX city IDs — inspect their API calls to find these
    CITY_IDS = {
        "mumbai": "1000",
        "delhi": "2",
        "bangalore": "3",
        "pune": "4",
        "hyderabad": "5",
        "chennai": "6",
    }

    async def scrape_model_city(
        self, page: Page, brand: str, model: str, city: str
    ) -> list[PrivateListing]:
        listings = []

        query = f"{brand} {model}"
        city_id = self.CITY_IDS.get(city, "")
        url = f"{self.BASE_URL}/cars_c84?q={query.replace(' ', '+')}"
        if city_id:
            url += f"&search[city_id]={city_id}"

        log.info(f"  OLX: {brand} {model} in {city}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await random_delay()

            # Handle potential cookie consent
            try:
                consent_btn = await page.query_selector(
                    "button[id*='accept'], button[class*='accept']"
                )
                if consent_btn:
                    await consent_btn.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            await page.wait_for_selector(
                "[data-aut-id='itemBox'], .EIR5N, li[class*='listing']",
                timeout=10000
            )

            # Scroll and paginate
            for page_num in range(3):  # scrape 3 pages
                if page_num > 0:
                    next_btn = await page.query_selector(
                        "[data-aut-id='pagination-next'], a[aria-label='Next page']"
                    )
                    if not next_btn:
                        break
                    await next_btn.click()
                    await asyncio.sleep(3)

                cards = await page.query_selector_all(
                    "[data-aut-id='itemBox'], .EIR5N, li[class*='listing']"
                )

                for card in cards:
                    try:
                        listing = await self._parse_card(card, brand, model, city)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        log.warning(f"    OLX card parse error: {e}")

                await random_delay()

        except Exception as e:
            log.error(f"    OLX scrape failed for {brand} {model} {city}: {e}")

        log.info(f"    Got {len(listings)} listings")
        return listings

    async def _parse_card(
        self, card, brand: str, model: str, city: str
    ) -> Optional[PrivateListing]:
        now = datetime.now(timezone.utc).isoformat()

        title_el = await card.query_selector(
            "[data-aut-id='itemTitle'], .JFjVA, h2, h3"
        )
        price_el = await card.query_selector(
            "[data-aut-id='itemPrice'], .rui-3iCOT, [class*='price']"
        )
        subtitle_el = await card.query_selector(
            "[data-aut-id='item-detail-DETAILS'], .zLvFQ, [class*='subtitle']"
        )
        date_el = await card.query_selector(
            "[data-aut-id='itemDate'], ._2DGqt, [class*='date']"
        )
        link_el = await card.query_selector("a")

        if not title_el or not price_el:
            return None

        title = await title_el.inner_text()
        price_text = await price_el.inner_text()
        subtitle_text = await subtitle_el.inner_text() if subtitle_el else ""
        date_text = await date_el.inner_text() if date_el else ""
        href = await link_el.get_attribute("href") if link_el else ""

        price = parse_price(price_text)
        if not price:
            return None

        # Parse year from title
        year_match = re.search(r"(20\d{2})", title + subtitle_text)
        year = int(year_match.group(1)) if year_match else 0

        # Parse km from subtitle: "2021 · 45,000 km · Petrol"
        km_match = re.search(r"([\d,]+)\s*km", subtitle_text, re.IGNORECASE)
        km = int(km_match.group(1).replace(",", "")) if km_match else 0

        # Parse fuel
        fuel = "petrol"
        for f in ["petrol", "diesel", "cng", "electric", "hybrid"]:
            if f in subtitle_text.lower() or f in title.lower():
                fuel = f
                break

        # Parse transmission
        transmission = "automatic"
        if any(x in subtitle_text.lower() + title.lower()
               for x in ["manual", "mt"]):
            transmission = "manual"

        # Estimate days listed from date string
        days_listed = self._parse_days_listed(date_text)

        # OLX has both individual and dealer sellers
        seller_type = "dealer" if any(
            x in title.lower() for x in ["dealer", "motors", "auto", "cars"]
        ) else "individual"

        # Extract variant from title
        variant = title.replace(str(year), "").replace(
            brand.title(), ""
        ).replace(model.title(), "").strip()

        return PrivateListing(
            source="olx",
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            fuel=fuel,
            transmission=transmission,
            km_driven=km,
            city=city,
            asking_price=price,
            seller_type=seller_type,
            days_listed=days_listed,
            listing_url=f"{self.BASE_URL}{href}" if href.startswith("/") else href,
            scraped_at=now
        )

    def _parse_days_listed(self, date_str: str) -> int:
        """Convert 'Today', 'Yesterday', '5 days ago' → integer days"""
        if not date_str:
            return 0
        date_str = date_str.lower().strip()
        if "today" in date_str:
            return 0
        if "yesterday" in date_str:
            return 1
        days_match = re.search(r"(\d+)\s*day", date_str)
        if days_match:
            return int(days_match.group(1))
        weeks_match = re.search(r"(\d+)\s*week", date_str)
        if weeks_match:
            return int(weeks_match.group(1)) * 7
        return 30  # default if can't parse


# ─────────────────────────────────────────────
# SCRAPER 4: CARDEKHO LISTINGS
# ─────────────────────────────────────────────

class CarDekhaScraper:
    """
    Scrapes CarDekho used car listings.
    URL: cardekho.com/used-cars+in+{city}/{brand}+{model}
    """

    BASE_URL = "https://www.cardekho.com"

    async def scrape_model_city(
        self, page: Page, brand: str, model: str, city: str
    ) -> list[PrivateListing]:
        listings = []

        url = f"{self.BASE_URL}/used-cars+in+{city}/{brand}+{model}"
        log.info(f"  CarDekho: {brand} {model} in {city}")

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await random_delay()

            await page.wait_for_selector(
                ".gsc_col-sm-12, [class*='listingCard'], [class*='used-car-card']",
                timeout=10000
            )

            cards = await page.query_selector_all(
                ".gsc_col-sm-12, [class*='listingCard']"
            )

            for card in cards[:25]:
                try:
                    listing = await self._parse_card(card, brand, model, city)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    log.warning(f"    CarDekho parse error: {e}")

        except Exception as e:
            log.error(f"    CarDekho scrape failed: {e}")

        log.info(f"    Got {len(listings)} listings")
        return listings

    async def _parse_card(
        self, card, brand: str, model: str, city: str
    ) -> Optional[PrivateListing]:
        now = datetime.now(timezone.utc).isoformat()

        title_el = await card.query_selector("h3, h2, [class*='title']")
        price_el = await card.query_selector("[class*='price'], [class*='Price']")
        details_el = await card.query_selector("[class*='detail'], [class*='spec']")
        link_el = await card.query_selector("a")

        if not title_el or not price_el:
            return None

        title = await title_el.inner_text()
        price_text = await price_el.inner_text()
        details_text = await details_el.inner_text() if details_el else ""
        href = await link_el.get_attribute("href") if link_el else ""

        price = parse_price(price_text)
        if not price:
            return None

        year_match = re.search(r"(20\d{2})", title + details_text)
        year = int(year_match.group(1)) if year_match else 0

        km_match = re.search(r"([\d,]+)\s*km", details_text, re.IGNORECASE)
        km = int(km_match.group(1).replace(",", "")) if km_match else 0

        fuel = "petrol"
        for f in ["petrol", "diesel", "cng", "electric"]:
            if f in details_text.lower():
                fuel = f
                break

        transmission = "manual"
        if any(x in details_text.lower() for x in ["automatic", "amt", "cvt", "dct"]):
            transmission = "automatic"

        variant = title.replace(str(year), "").strip()

        return PrivateListing(
            source="cardekho",
            brand=brand,
            model=model,
            variant=variant,
            year=year,
            fuel=fuel,
            transmission=transmission,
            km_driven=km,
            city=city,
            asking_price=price,
            seller_type="individual",
            days_listed=0,
            listing_url=f"{self.BASE_URL}{href}" if href.startswith("/") else href,
            scraped_at=now
        )


# ─────────────────────────────────────────────
# DATABASE LAYER
# ─────────────────────────────────────────────

class Database:
    """Supabase database operations"""

    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def save_certified_listings(self, listings: list[CertifiedListing]):
        """Save Cars24/Spinny listings to DB"""
        if not listings:
            return
        data = [asdict(l) for l in listings]
        self.client.table("certified_listings").upsert(
            data, on_conflict="source,listing_url"
        ).execute()
        log.info(f"  Saved {len(data)} certified listings")

    def save_private_listings(self, listings: list[PrivateListing]):
        """Save OLX/CarDekho listings to DB"""
        if not listings:
            return
        data = [asdict(l) for l in listings]
        self.client.table("private_listings").upsert(
            data, on_conflict="source,listing_url"
        ).execute()
        log.info(f"  Saved {len(data)} private listings")

    def compute_price_estimates(self):
        """
        Nightly computation: aggregate raw listings into price estimates.
        Called after all scraping is complete.
        Computes median, P25, P75, and realistic transaction price.
        """
        log.info("Computing price estimates...")

        # Get all certified listings from last 30 days
        certified = self.client.table("certified_listings").select("*").gte(
            "scraped_at",
            (datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0
            ) - __import__("datetime").timedelta(days=30)).isoformat()
        ).execute().data

        # Get all private listings from last 30 days
        private = self.client.table("private_listings").select("*").gte(
            "scraped_at",
            (datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0
            ) - __import__("datetime").timedelta(days=30)).isoformat()
        ).execute().data

        # Group by brand/model/year/city and compute aggregates
        estimates = self._aggregate(certified, private)

        # Save computed estimates
        if estimates:
            self.client.table("price_estimates").upsert(
                estimates,
                on_conflict="brand,model,year,city"
            ).execute()
            log.info(f"  Computed {len(estimates)} price estimates")

    def _aggregate(
        self,
        certified: list[dict],
        private: list[dict]
    ) -> list[dict]:
        """Group listings and compute price stats per brand/model/year/city"""
        from collections import defaultdict
        import statistics

        # Group certified listings
        cert_groups = defaultdict(list)
        for l in certified:
            key = (l["brand"], l["model"], l["year"], l["city"])
            cert_groups[key].append(l["asking_price"])

        # Group private listings — filter out dealers
        priv_groups = defaultdict(list)
        priv_days = defaultdict(list)
        for l in private:
            if l.get("seller_type") == "dealer":
                continue  # only private sellers
            key = (l["brand"], l["model"], l["year"], l["city"])
            priv_groups[key].append(l["asking_price"])
            if l.get("days_listed"):
                priv_days[key].append(l["days_listed"])

        # All unique keys
        all_keys = set(cert_groups.keys()) | set(priv_groups.keys())
        now = datetime.now(timezone.utc).isoformat()
        estimates = []

        for key in all_keys:
            brand, model, year, city = key
            cert_prices = cert_groups.get(key, [])
            priv_prices = priv_groups.get(key, [])
            days = priv_days.get(key, [])

            if not cert_prices and not priv_prices:
                continue

            # Compute certified dealer stats
            cert_low = min(cert_prices) if cert_prices else None
            cert_high = max(cert_prices) if cert_prices else None
            cert_median = int(statistics.median(cert_prices)) if cert_prices else None

            # Compute private asking stats
            priv_median = int(statistics.median(priv_prices)) if priv_prices else None
            priv_p25 = int(sorted(priv_prices)[len(priv_prices) // 4]) if priv_prices else None
            priv_p75 = int(sorted(priv_prices)[3 * len(priv_prices) // 4]) if priv_prices else None

            # Compute realistic transaction price (our estimate)
            realistic_low = int(priv_p25 * OLX_TRANSACTION_DISCOUNT) if priv_p25 else None
            realistic_high = int(priv_median * OLX_TRANSACTION_DISCOUNT) if priv_median else None

            # Confidence score based on sample size
            sample_size = len(cert_prices) + len(priv_prices)
            confidence = min(95, 50 + (sample_size * 3))

            # Average days listed = demand signal
            avg_days = int(statistics.mean(days)) if days else None
            demand = (
                "high" if avg_days and avg_days < 20
                else "medium" if avg_days and avg_days < 40
                else "low"
            )

            # Dealer premium
            dealer_premium = None
            if cert_median and priv_median:
                dealer_premium = cert_median - priv_median

            estimates.append({
                "brand": brand,
                "model": model,
                "year": year,
                "city": city,
                "certified_low": cert_low,
                "certified_high": cert_high,
                "certified_median": cert_median,
                "certified_sample_size": len(cert_prices),
                "private_asking_low": priv_p25,
                "private_asking_high": priv_p75,
                "private_asking_median": priv_median,
                "private_sample_size": len(priv_prices),
                "realistic_low": realistic_low,
                "realistic_high": realistic_high,
                "discount_applied": OLX_TRANSACTION_DISCOUNT,
                "dealer_premium": dealer_premium,
                "confidence_score": confidence,
                "avg_days_listed": avg_days,
                "demand_signal": demand,
                "updated_at": now
            })

        return estimates


# ─────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────

async def run_scraper(source_filter: Optional[str] = None):
    """Main scraping loop — runs all scrapers for all models × cities"""
    db = Database()

    cars24 = Cars24Scraper()
    spinny = SpinnyScraper()
    olx = OLXScraper()
    cardekho = CarDekhaScraper()

    async with async_playwright() as pw:
        browser = await get_browser(pw)

        for car in MODELS_TO_TRACK:
            brand = car["brand"]
            model = car["model"]

            for city in CITIES:
                log.info(f"\n{'='*50}")
                log.info(f"Scraping: {brand} {model} | {city}")
                log.info(f"{'='*50}")

                # CERTIFIED SCRAPERS — Cars24 buy & Spinny buy
                if not source_filter or source_filter == "cars24":
                    page = await get_page(browser)
                    try:
                        listings = await cars24.scrape_model_city(
                            page, brand, model, city
                        )
                        db.save_certified_listings(listings)
                    finally:
                        await page.close()
                    await random_delay()

                if not source_filter or source_filter == "spinny":
                    page = await get_page(browser)
                    try:
                        listings = await spinny.scrape_model_city(
                            page, brand, model, city
                        )
                        db.save_certified_listings(listings)
                    finally:
                        await page.close()
                    await random_delay()

                # PRIVATE SCRAPERS — OLX & CarDekho
                if not source_filter or source_filter == "olx":
                    page = await get_page(browser)
                    try:
                        listings = await olx.scrape_model_city(
                            page, brand, model, city
                        )
                        db.save_private_listings(listings)
                    finally:
                        await page.close()
                    await random_delay()

                if not source_filter or source_filter == "cardekho":
                    page = await get_page(browser)
                    try:
                        listings = await cardekho.scrape_model_city(
                            page, brand, model, city
                        )
                        db.save_private_listings(listings)
                    finally:
                        await page.close()
                    await random_delay()

        await browser.close()

    # After all scraping, compute price estimates
    log.info("\nAll scraping complete. Computing price estimates...")
    db.compute_price_estimates()
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser(description="carIQ Price Scraper")
    parser.add_argument("--source", help="Scrape only one source: cars24|spinny|olx|cardekho")
    parser.add_argument("--compute-only", action="store_true", help="Skip scraping, just recompute estimates")
    args = parser.parse_args()

    if args.compute_only:
        db = Database()
        db.compute_price_estimates()
    else:
        asyncio.run(run_scraper(source_filter=args.source))


if __name__ == "__main__":
    main()
