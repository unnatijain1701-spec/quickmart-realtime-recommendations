"""
config.py - the single place for every setting, threshold and assumption.
Change numbers here, never inside the other scripts.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# ---------------- connections (from .env) ----------------
MONGO_URI = os.environ.get("MONGO_URI", "")
DB_NAME = os.environ.get("DB_NAME", "qc_streaming")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

# ---------------- Kafka topics ----------------
TOPIC_EVENTS = "customer_events"          # keyed by user_id
TOPIC_RECS = "recommendations"
TOPIC_ALERTS = "alerts"
TOPIC_PARTITIONS = 3

# ---------------- streaming windows ----------------
TRENDING_WINDOW_SEC = 300       # sliding window (use 90 during the live demo)
TRENDING_SLIDE_SEC = 10         # how often the trending ranking is refreshed
TUMBLING_SEC = 60               # dashboard metric buckets

# ---------------- late events (simulator) ----------------
LATE_FRACTION = 0.07            # 5 to 10 percent of events arrive late
LATE_DELAY_MIN_SEC = 30
LATE_DELAY_MAX_SEC = 300

# ---------------- recommendation rules ----------------
TOP_N_RECS = 3
TREND_WEIGHT_CLICK = 1
TREND_WEIGHT_ADD = 3
FREE_DELIVERY_THRESHOLD = 299   # Rs. With the realistic prices a typical order is about Rs 280, so this is a real target.
FREE_DELIVERY_MAX_GAP = 120     # suggest only when the cart is 1 to this many Rs short
RATING_MIN_COUNT = 15           # a product's average counts only with this many ratings
RATING_LOW_CUTOFF = 3.0         # candidates averaging below this get replaced

# ---------------- pipeline alerts ----------------
ALERT_MAX_DELAY_SEC = 2
ALERT_MAX_LATE_SHARE = 0.15

# ---------------- simulator and experiment assumptions ----------------
RANDOM_SEED = 42
BASE_CTR = 0.10                 # anchored on the campaign file (about 10 percent click rate)
BASE_CLICK_TO_CART = 0.30       # assumption
BASE_CART_TO_ORDER = 0.40       # assumption

# ---------------- affinity table (ASSUMED, stated in the presentation) ----------------
# Product-level pairs are used first, category-level pairs as a fallback.
AFFINITY_PRODUCT_PAIRS = [
    ("Bread", "Butter"), ("Milk", "Biscuits"), ("Eggs", "Bread"), ("Milk", "Cereal"),
    ("Chips", "Cola"), ("Ice Cream", "Cookies"), ("Instant Noodles", "Cola"),
    ("Rice", "Pulses"), ("Tomatoes", "Onions"), ("Potatoes", "Onions"),
    ("Shampoo", "Soap"), ("Toothpaste", "Soap"), ("Diapers", "Baby Wipes"),
    ("Dog Food", "Pet Treats"), ("Detergent", "Dish Soap"), ("Cheese", "Bread"),
]
AFFINITY_CATEGORY_PAIRS = [
    ("Dairy & Breakfast", "Snacks & Munchies"),
    ("Snacks & Munchies", "Cold Drinks & Juices"),
    ("Instant & Frozen Food", "Cold Drinks & Juices"),
    ("Fruits & Vegetables", "Grocery & Staples"),
    ("Household Care", "Personal Care"),
    ("Baby Care", "Pharmacy"),
    ("Pet Care", "Household Care"),
]

# ---------------- simulated offers (named after the campaign file) ----------------
OFFERS = [
    {"name": "Flash Sale", "category": "Snacks & Munchies", "discount_pct": 15},
    {"name": "Weekend Special", "category": "Cold Drinks & Juices", "discount_pct": 10},
    {"name": "Category Promotion", "category": "Dairy & Breakfast", "discount_pct": 12},
]

# ---------------- simulator behaviour (ASSUMED, M2) ----------------
SESSION_VIEWS_RANGE = (3, 10)       # product impressions per session
THINK_TIME_RANGE_SEC = (1.5, 6.0)   # pause between impressions
PREFERRED_CATEGORY_P = 0.6          # chance a viewed product comes from a category the user bought before
ORGANIC_CLICK_P = 0.35              # impression -> click (no recommendation involved)
ORGANIC_CART_P = 0.50               # click -> add to cart
AFFINITY_FOLLOW_P = 0.60            # after an add, chance the customer also looks at an affinity partner
REMOVE_FROM_CART_P = 0.15           # chance of removing one item before checkout
CART_TO_ORDER_P = BASE_CART_TO_ORDER
ORDER_RATING_P = 0.30               # share of orders that get rated
RATING_DELAY_RANGE_SEC = (60, 240)  # rating arrives this long after the order

# ---------------- recommendation engine (M3) ----------------
STALE_REFRESH_SEC = 3600            # control group sees data as of the last refresh (hourly)
PAIR_COUNT_SESSIONS = 3000          # simulated sessions used to initialise "frequently bought together"
FREE_DELIVERY_MAX_OVERSHOOT = 80    # prefer an add-on that crosses the threshold by at most this many Rs
TREND_TOP_K = 10                    # trending candidates kept per refresh
METRICS_FLUSH_SEC = 5               # how often the current minute bucket is written to MongoDB

# ---------------- customer screen (M4) ----------------
WEB_HOST = "127.0.0.1"
WEB_PORT = 8000


# ---------------- dashboard (M5) ----------------
DASH_HOST = "127.0.0.1"
DASH_PORT = 8050
DASH_REFRESH_MS = 3000              # dashboard refresh interval
DASH_DEFAULT_WINDOW_MIN = 15        # default time window shown


def offer_for(category):
    """The store-wide category promotion for a category (applied at checkout), or None."""
    for o in OFFERS:
        if o["category"] == category:
            return o
    return None


# ---------------- realistic prices (rupees per typical pack; ILLUSTRATIVE bands, edit freely) ----------------
# The supplied dataset has synthetic prices (onions at Rs 948). make_realistic_prices.py draws one price per product
# inside the band for its NAME (the same product id always gets the same price) and writes data/products_priced.csv.
# These bands are approximate, from general knowledge of Indian quick-commerce shelf prices. They are not Blinkit's prices.
PRICE_BANDS = {
    "Baby Food": (150, 400), "Baby Wipes": (90, 350), "Diapers": (300, 1200),
    "Cola": (20, 100), "Iced Tea": (20, 90), "Lemonade": (20, 80), "Mango Drink": (20, 95), "Orange Juice": (60, 140),
    "Bread": (25, 70), "Butter": (55, 300), "Cereal": (60, 500), "Cheese": (70, 250), "Curd": (30, 90), "Eggs": (50, 130), "Milk": (28, 75),
    "Bananas": (40, 80), "Carrots": (25, 60), "Mangoes": (70, 220), "Onions": (25, 60), "Potatoes": (25, 55), "Spinach": (15, 35), "Tomatoes": (25, 70),
    "Pulses": (80, 220), "Rice": (60, 650), "Salt": (20, 35), "Sugar": (42, 70), "Wheat Flour": (50, 320),
    "Detergent": (60, 450), "Dish Soap": (35, 250), "Toilet Cleaner": (70, 200),
    "Frozen Biryani": (130, 350), "Frozen Pizza": (100, 400), "Frozen Vegetables": (70, 200), "Ice Cream": (20, 450), "Instant Noodles": (12, 200),
    "Lotion": (90, 450), "Shampoo": (60, 450), "Soap": (25, 220), "Toothpaste": (50, 220),
    "Cat Food": (100, 800), "Dog Food": (150, 1200), "Pet Treats": (80, 500),
    "Cough Syrup": (60, 220), "Pain Reliever": (20, 150), "Vitamins": (120, 700),
    "Biscuits": (10, 120), "Chips": (10, 120), "Chocolates": (10, 300), "Cookies": (30, 250), "Nuts": (100, 800), "Popcorn": (20, 150),
}
PRODUCTS_CSV = DATA_DIR / "products_priced.csv"
if not PRODUCTS_CSV.exists():                    # fall back to the supplied (synthetic) prices until the file is made
    PRODUCTS_CSV = DATA_DIR / "blinkit_products.csv"


def lan_ip():
    """This computer's address on the local network (what another laptop types to reach it)."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))          # no data is sent; it only picks the network card in use
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()
