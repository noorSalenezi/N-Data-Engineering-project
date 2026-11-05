"""Generate the Qafila dataset used in every lab.

    uv run python prepare_data.py          # writes labs/data/raw/...
    uv run python prepare_data.py --force  # regenerate even if it exists

Qafila is a fictional quick-commerce grocery platform in Riyadh, Jeddah and Dammam.
Everything here is synthetic and generated deterministically (stdlib `random` with a
fixed seed), so every participant gets byte-identical data and the labs run offline.
No real person, ID number, phone number or IBAN is used: national IDs and IBANs have
valid check digits but are random.

The data deliberately contains the problems real sources have (see DATA_ISSUES below):
labs discover them on Day 1 and fix them on Day 2.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

LABS = Path(__file__).resolve().parent
RAW = LABS / "data" / "raw"
SEED = 2026
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 10, 1, tzinfo=timezone.utc)          # exclusive: data covers Aug + Sep 2026
CDC_START = datetime(2026, 9, 1, tzinfo=timezone.utc)      # snapshot date for the CDC feed
STREAM_START = datetime(2026, 9, 24, tzinfo=timezone.utc)  # delivery events: last 7 days
RIYADH = timezone(timedelta(hours=3))
SCHEMA_CHANGE = datetime(2026, 9, 15, tzinfo=timezone.utc)  # payments API adds a field

DATA_ISSUES = """
customers   duplicate accounts (same national_id, different email casing); mixed mobile formats;
            ~5% missing email; Arabic names with tatweel/diacritics; soft deletes (is_deleted)
orders      orders of customers erased under PDPL (customer row hard-deleted -> orphan FK)
order_items a handful of negative quantities
products    two SKUs with trailing spaces
payments    failed attempts before a capture; a few amounts that do not match the order;
            'wallet_provider' field appears from 2026-09-15 (schema evolution); API rate limits
cdc         redelivered (duplicate) events; events arrive out of order (sort by lsn)
deliveries  duplicate events; late events (courier offline); three timestamp formats
suppliers   W38 uses decimal commas and Arabic-Indic digits; W39 renames unit_cost -> cost_sar,
            adds currency, and has zero/negative costs and unknown SKUs
"""

# ---------------------------------------------------------------------------
# Reference lists
# ---------------------------------------------------------------------------

CITIES = {
    "Riyadh": {"ar": "الرياض", "weight": 0.50,
               "districts": ["Olaya", "Al Malqa", "Al Yasmin", "Al Nakheel", "Al Sulimaniyah", "Hittin", "Al Rawdah", "Al Naseem"]},
    "Jeddah": {"ar": "جدة", "weight": 0.32,
               "districts": ["Al Rawdah", "Al Hamra", "Al Salamah", "Obhur", "Al Zahra", "Al Safa"]},
    "Dammam": {"ar": "الدمام", "weight": 0.18,
               "districts": ["Al Faisaliyah", "Al Shati", "Al Mazruiyah", "Al Nuzha"]},
}

# store_id, city, name_en, name_ar, lat, lon, share of the city's orders (RUH-01 is a hot spot)
STORES = [
    ("RUH-01", "Riyadh", "Olaya", "العليا", 24.6957, 46.6827, 0.42),
    ("RUH-02", "Riyadh", "Al Malqa", "الملقا", 24.8114, 46.6133, 0.14),
    ("RUH-03", "Riyadh", "Al Yasmin", "الياسمين", 24.8240, 46.6420, 0.12),
    ("RUH-04", "Riyadh", "Al Sulimaniyah", "السليمانية", 24.7055, 46.7010, 0.12),
    ("RUH-05", "Riyadh", "Al Naseem", "النسيم", 24.7330, 46.8270, 0.10),
    ("RUH-06", "Riyadh", "Hittin", "حطين", 24.7640, 46.6000, 0.10),
    ("JED-01", "Jeddah", "Al Rawdah", "الروضة", 21.5640, 39.1530, 0.32),
    ("JED-02", "Jeddah", "Al Hamra", "الحمراء", 21.5170, 39.1680, 0.26),
    ("JED-03", "Jeddah", "Obhur", "أبحر", 21.7140, 39.1080, 0.20),
    ("JED-04", "Jeddah", "Al Safa", "الصفا", 21.5800, 39.2100, 0.22),
    ("DMM-01", "Dammam", "Al Faisaliyah", "الفيصلية", 26.4120, 50.0450, 0.55),
    ("DMM-02", "Dammam", "Al Shati", "الشاطئ", 26.4600, 50.1100, 0.45),
]

# item_en, item_ar, category, base price (SAR), tags used in descriptions
ITEMS = [
    ("Fresh Milk", "حليب طازج", "Dairy", 6.5, "breakfast;keep refrigerated;full fat"),
    ("Laban", "لبن", "Dairy", 5.0, "traditional;keep refrigerated;drink with kabsa"),
    ("Greek Yogurt", "زبادي يوناني", "Dairy", 7.0, "high protein;keep refrigerated;breakfast"),
    ("Labneh", "لبنة", "Dairy", 9.0, "breakfast;spread;keep refrigerated"),
    ("Cheddar Cheese", "جبنة شيدر", "Dairy", 14.0, "sandwiches;sliced;keep refrigerated"),
    ("Butter", "زبدة", "Dairy", 12.0, "baking;unsalted;keep refrigerated"),
    ("Eggs 30 Pack", "بيض ٣٠ حبة", "Dairy", 22.0, "breakfast;free range;protein"),
    ("Arabic Bread", "خبز عربي", "Bakery", 3.0, "fresh daily;flatbread;sandwiches"),
    ("Samoli Bread", "خبز صامولي", "Bakery", 4.0, "fresh daily;sandwiches;soft"),
    ("Croissant", "كرواسون", "Bakery", 6.0, "breakfast;butter;pastry"),
    ("Toast Bread", "خبز توست", "Bakery", 7.0, "breakfast;sliced;sandwiches"),
    ("Bananas", "موز", "Fruits & Vegetables", 7.0, "fresh;imported;snack"),
    ("Red Apples", "تفاح أحمر", "Fruits & Vegetables", 9.0, "fresh;crunchy;snack"),
    ("Tomatoes", "طماطم", "Fruits & Vegetables", 5.0, "fresh;local farms;salad"),
    ("Cucumbers", "خيار", "Fruits & Vegetables", 5.0, "fresh;local farms;salad"),
    ("Onions", "بصل", "Fruits & Vegetables", 4.0, "cooking;kabsa;staple"),
    ("Potatoes", "بطاطس", "Fruits & Vegetables", 6.0, "cooking;fries;staple"),
    ("Lemons", "ليمون", "Fruits & Vegetables", 6.0, "fresh;juice;salad"),
    ("Mangoes", "مانجو", "Fruits & Vegetables", 15.0, "fresh;sweet;seasonal"),
    ("Watermelon", "بطيخ", "Fruits & Vegetables", 18.0, "fresh;summer;sweet"),
    ("Fresh Whole Chicken", "دجاج طازج كامل", "Meat & Poultry", 19.0, "fresh;local;grill"),
    ("Chicken Breast", "صدور دجاج", "Meat & Poultry", 29.0, "fresh;protein;grill"),
    ("Minced Beef", "لحم بقري مفروم", "Meat & Poultry", 38.0, "fresh;burgers;cooking"),
    ("Lamb Chops", "ريش غنم", "Meat & Poultry", 65.0, "fresh;local naimi;grill"),
    ("Mineral Water 12x330ml", "مياه معدنية ١٢ عبوة", "Beverages", 12.0, "drinks;family pack;hydration"),
    ("Orange Juice", "عصير برتقال", "Beverages", 9.0, "drinks;no added sugar;breakfast"),
    ("Cola 6 Pack", "مشروب غازي ٦ عبوات", "Beverages", 14.0, "drinks;cold;party"),
    ("Black Tea", "شاي أسود", "Beverages", 14.0, "drinks;hot;100 bags"),
    ("Arabic Coffee", "قهوة عربية", "Dates & Coffee", 35.0, "saudi coffee;cardamom;guests"),
    ("Sukkari Dates", "تمر سكري", "Dates & Coffee", 45.0, "dates;qassim;sweet"),
    ("Ajwa Dates", "تمر عجوة", "Dates & Coffee", 70.0, "dates;madinah;premium"),
    ("Khalas Dates", "تمر خلاص", "Dates & Coffee", 38.0, "dates;al ahsa;with coffee"),
    ("Cardamom", "هيل", "Dates & Coffee", 28.0, "spice;coffee;aromatic"),
    ("Saffron", "زعفران", "Dates & Coffee", 40.0, "spice;premium;rice"),
    ("Potato Chips", "شيبس بطاطس", "Snacks", 3.0, "snack;salty;kids"),
    ("Chocolate Bar", "لوح شوكولاتة", "Snacks", 4.0, "snack;sweet;kids"),
    ("Mixed Nuts", "مكسرات مشكلة", "Snacks", 25.0, "snack;roasted;guests"),
    ("Date Biscuits", "معمول بالتمر", "Snacks", 12.0, "snack;sweet;with coffee"),
    ("Dish Soap", "سائل غسيل الصحون", "Household", 11.0, "cleaning;kitchen;lemon scent"),
    ("Laundry Detergent", "منظف غسيل", "Household", 32.0, "cleaning;laundry;large pack"),
    ("Tissue Paper", "مناديل", "Household", 15.0, "paper;soft;family pack"),
    ("Garbage Bags", "أكياس نفايات", "Household", 12.0, "cleaning;kitchen;strong"),
    ("Shampoo", "شامبو", "Personal Care", 18.0, "hair care;family;gentle"),
    ("Toothpaste", "معجون أسنان", "Personal Care", 9.0, "oral care;fluoride;mint"),
    ("Hand Soap", "صابون يدين", "Personal Care", 8.0, "hygiene;gentle;family"),
    ("Frozen Vegetables", "خضار مجمدة", "Frozen", 10.0, "frozen;cooking;quick meal"),
    ("Ice Cream", "آيس كريم", "Frozen", 14.0, "frozen;dessert;kids"),
    ("Frozen Samosa", "سمبوسة مجمدة", "Frozen", 16.0, "frozen;ramadan;snack"),
    ("Basmati Rice 5kg", "أرز بسمتي ٥ كجم", "Pantry", 42.0, "staple;kabsa;long grain"),
    ("Sugar", "سكر", "Pantry", 9.0, "staple;baking;white"),
    ("Flour", "دقيق", "Pantry", 8.0, "staple;baking;all purpose"),
    ("Olive Oil", "زيت زيتون", "Pantry", 35.0, "cooking;extra virgin;salad"),
    ("Red Lentils", "عدس أحمر", "Pantry", 7.0, "staple;soup;protein"),
    ("Pasta", "معكرونة", "Pantry", 4.0, "staple;quick meal;kids"),
    ("Tahini", "طحينة", "Pantry", 14.0, "spread;hummus;sesame"),
    ("Chickpeas", "حمص حب", "Pantry", 6.0, "staple;hummus;protein"),
]
BRANDS = [("Al Waha", "الواحة"), ("Nakheel", "النخيل"), ("Barakah", "بركة"), ("Sahra", "صحراء"), ("Qafila Basics", "قافلة بيسك")]
SIZES = [("", "", 1.0), ("Family Size", "حجم عائلي", 1.8), ("Small", "صغير", 0.6)]

AR_MALE = ["محمد", "عبدالله", "فهد", "سعود", "خالد", "عبدالعزيز", "سلطان", "تركي", "ناصر", "فيصل", "عمر", "يوسف", "إبراهيم", "بندر", "ماجد", "راكان"]
AR_FEMALE = ["نورة", "سارة", "ريم", "هيفاء", "لمى", "منيرة", "العنود", "جواهر", "مها", "أمل", "هند", "دانة", "لطيفة", "شهد", "رهف", "غادة"]
AR_FAMILY = ["القحطاني", "العتيبي", "الشهري", "الغامدي", "الدوسري", "الحربي", "الزهراني", "المطيري", "السبيعي", "العنزي", "الشمري", "الأحمدي", "الخالدي", "التميمي", "البقمي", "الجهني"]
LATIN_FIRST = ["Mohammed", "Abdullah", "Fahad", "Sara", "Noura", "Ahmed", "Omar", "Priya", "Rahul", "Maria", "Jose", "Aisha", "Fatima", "Ali", "Hassan", "Anil", "Grace", "Joseph", "Mona", "Khaled"]
LATIN_LAST = ["Al-Qahtani", "Al-Otaibi", "Al-Harbi", "Khan", "Sharma", "Santos", "Reyes", "Hassan", "Mahmoud", "Ibrahim", "Nair", "Fernandes", "Al-Zahrani", "Saleh", "Mostafa", "Pillai"]
TRANSLIT = {"محمد": "mohammed", "عبدالله": "abdullah", "فهد": "fahad", "سعود": "saud", "خالد": "khalid", "عبدالعزيز": "abdulaziz",
            "سلطان": "sultan", "تركي": "turki", "ناصر": "nasser", "فيصل": "faisal", "عمر": "omar", "يوسف": "yousef", "إبراهيم": "ibrahim",
            "بندر": "bandar", "ماجد": "majed", "راكان": "rakan", "نورة": "noura", "سارة": "sara", "ريم": "reem", "هيفاء": "haifa", "لمى": "lama",
            "منيرة": "munira", "العنود": "alanoud", "جواهر": "jawaher", "مها": "maha", "أمل": "amal", "هند": "hind", "دانة": "dana",
            "لطيفة": "latifa", "شهد": "shahad", "رهف": "rahaf", "غادة": "ghada"}
EMAIL_DOMAINS = ["gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "icloud.com"]
PAYMENT_METHODS = [("card", 0.55), ("apple_pay", 0.25), ("cash", 0.12), ("wallet", 0.08)]
WALLETS = ["stc pay", "urpay", "mada pay"]
REVIEW_EN = {
    5: ["Excellent quality, will order again", "Fresh and delivered fast", "Exactly as described, great value", "Best {item} I have bought online"],
    4: ["Good quality, packaging could be better", "Nice {item}, delivery was a bit late", "Good value for money"],
    3: ["Average, nothing special", "OK but a little expensive", "Quality varies from order to order"],
    2: ["Not fresh this time", "Arrived damaged, partly usable", "Smaller than expected"],
    1: ["Expired product, very disappointed", "Wrong item delivered", "Leaking package, had to throw it away"],
}
REVIEW_AR = {
    5: ["جودة ممتازة وسأطلب مرة أخرى", "طازج ووصل بسرعة", "مطابق للوصف وسعره ممتاز"],
    4: ["جيد لكن التغليف يحتاج تحسين", "منتج جميل والتوصيل تأخر قليلا", "يستاهل سعره"],
    3: ["عادي لا شيء مميز", "مقبول لكن السعر مرتفع شوي", "الجودة تختلف من طلب لطلب"],
    2: ["المنتج غير طازج هذه المرة", "وصل متضرر", "الحجم أصغر من المتوقع"],
    1: ["المنتج منتهي الصلاحية", "وصلني منتج خطأ", "العبوة تسرب واضطررت لرميها"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def luhn_digit(body: str) -> str:
    """Check digit so that body + digit passes the Luhn test (as Saudi national IDs do)."""
    total = 0
    for i, ch in enumerate(reversed(body)):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return str((10 - total % 10) % 10)


def national_id(rng: random.Random, citizen: bool) -> str:
    body = ("1" if citizen else "2") + "".join(str(rng.randint(0, 9)) for _ in range(8))
    return body + luhn_digit(body)


def saudi_iban(rng: random.Random) -> str:
    """SA + 2 check digits + 2-digit bank code + 18 digits, valid mod-97 (ISO 13616)."""
    bban = f"{rng.choice([10, 15, 20, 30, 45, 55, 60, 65, 80])}" + "".join(str(rng.randint(0, 9)) for _ in range(18))
    numeric = int(bban + "2810" + "00")  # S=28, A=10
    check = 98 - numeric % 97
    return f"SA{check:02d}{bban}"


def mobile(rng: random.Random) -> str:
    digits = "5" + str(rng.choice([0, 3, 4, 5, 6, 9])) + "".join(str(rng.randint(0, 9)) for _ in range(7))
    style = rng.random()
    if style < 0.45:
        return "+966" + digits
    if style < 0.75:
        return "0" + digits
    if style < 0.90:
        return f"+966 {digits[:2]} {digits[2:5]} {digits[5:]}"
    return "966" + digits


def weighted(rng: random.Random, pairs):
    r, acc = rng.random(), 0.0
    for value, w in pairs:
        acc += w
        if r <= acc:
            return value
    return pairs[-1][0]


def ts(dt: datetime) -> str:
    """OLTP convention: naive UTC 'YYYY-MM-DD HH:MM:SS'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def to_arabic_digits(s: str) -> str:
    return s.translate(str.maketrans("0123456789.", "٠١٢٣٤٥٦٧٨٩٫"))


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def make_products(rng: random.Random) -> list[dict]:
    products, pid = [], 1000
    for item_en, item_ar, cat, price, tags in ITEMS:
        brands = rng.sample(BRANDS, k=rng.choice([2, 3, 3, 4]))
        for b_en, b_ar in brands:
            for s_en, s_ar, mult in SIZES[: rng.choice([1, 2, 3])]:
                pid += 1
                name_en = " ".join(x for x in [b_en, item_en, s_en] if x)
                name_ar = " ".join(x for x in [item_ar, b_ar, s_ar] if x)
                t = tags.split(";")
                desc_en = f"{item_en} by {b_en}. {t[0].capitalize()}, {t[1]}; great for {t[2]}." + (f" {s_en} pack." if s_en else "")
                desc_ar = f"{item_ar} من {b_ar}. منتج مناسب ضمن فئة {cat_ar(cat)}." + (f" {s_ar}." if s_ar else "")
                products.append({
                    "product_id": pid,
                    "sku": f"QF-{cat[:3].upper()}-{pid}",
                    "name_en": name_en,
                    "name_ar": name_ar,
                    "category": cat,
                    "brand": b_en,
                    "unit_price": round(price * mult * rng.uniform(0.9, 1.15), 2),
                    "description_en": desc_en,
                    "description_ar": desc_ar,
                    "is_active": 1 if rng.random() > 0.04 else 0,
                    "updated_at": ts(START - timedelta(days=rng.randint(1, 200))),
                })
    # data issues: two SKUs with trailing spaces; Arabic tatweel / diacritics in a few names
    for p in rng.sample(products, 2):
        p["sku"] = p["sku"] + "  "
    for p in rng.sample(products, 6):
        p["name_ar"] = p["name_ar"].replace("ا", "ـا", 1)
    for p in rng.sample(products, 4):
        p["name_ar"] = p["name_ar"].replace("ل", "لَ", 1)
    return products


def cat_ar(cat: str) -> str:
    return {"Dairy": "الألبان", "Bakery": "المخبوزات", "Fruits & Vegetables": "الخضار والفواكه", "Meat & Poultry": "اللحوم والدواجن",
            "Beverages": "المشروبات", "Dates & Coffee": "التمور والقهوة", "Snacks": "الوجبات الخفيفة", "Household": "المنظفات",
            "Personal Care": "العناية الشخصية", "Frozen": "المجمدات", "Pantry": "المواد الأساسية"}[cat]


def make_customer(rng: random.Random, cid: int, created: datetime) -> dict:
    city = weighted(rng, [(c, v["weight"]) for c, v in CITIES.items()])
    citizen = rng.random() < 0.62
    if citizen or rng.random() < 0.3:
        female = rng.random() < 0.45
        first = rng.choice(AR_FEMALE if female else AR_MALE)
        family = rng.choice(AR_FAMILY)
        name = f"{first} {family}"
        local = TRANSLIT.get(first, "user") + "." + str(rng.randint(10, 999))
    else:
        first, last = rng.choice(LATIN_FIRST), rng.choice(LATIN_LAST)
        name = f"{first} {last}"
        local = f"{first}.{last}".lower().replace("-", "") + str(rng.randint(1, 99))
    email = None if rng.random() < 0.05 else f"{local}@{rng.choice(EMAIL_DOMAINS)}"
    return {
        "customer_id": cid,
        "full_name": name,
        "email": email,
        "mobile": mobile(rng),
        "national_id": national_id(rng, citizen),
        "iban": saudi_iban(rng) if rng.random() < 0.35 else None,  # stored for refunds
        "city": city,
        "district": rng.choice(CITIES[city]["districts"]),
        "created_at": ts(created),
        "updated_at": ts(created),
        "is_deleted": 0,
    }


def make_customers(rng: random.Random, n: int) -> list[dict]:
    span = (CDC_START - datetime(2025, 1, 1, tzinfo=timezone.utc)).total_seconds()
    custs = [make_customer(rng, 100000 + i, datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=rng.random() * span))
             for i in range(n)]
    # duplicate accounts: same person registers again with different email casing / mobile format
    for src in rng.sample(custs, n // 100):
        dup = dict(src)
        dup["customer_id"] = 100000 + len(custs)
        dup["email"] = src["email"].upper() if src["email"] else None
        dup["mobile"] = mobile(rng) if rng.random() < 0.3 else src["mobile"].replace("+966", "0")
        created = datetime.strptime(src["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) + timedelta(days=rng.randint(10, 200))
        created = min(created, CDC_START - timedelta(hours=1))
        dup["created_at"] = dup["updated_at"] = ts(created)
        custs.append(dup)
    return custs


def make_cdc(rng: random.Random, customers: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """September change feed for customers. Returns (snapshot rows, events in arrival order, final state)."""
    snapshot = [dict(c) for c in customers]
    state = {c["customer_id"]: dict(c) for c in customers}
    events, lsn = [], 5_000_000
    next_id = 100000 + len(customers)
    t = CDC_START
    while t < END:
        t += timedelta(seconds=rng.expovariate(1 / 420))  # ~one change every 7 minutes
        if t >= END:
            break
        lsn += rng.randint(1, 40)
        r = rng.random()
        live = [cid for cid, c in state.items()] if r >= 0.25 else None
        if r < 0.25:  # new customer
            c = make_customer(rng, next_id, t)
            next_id += 1
            state[c["customer_id"]] = c
            events.append({"op": "c", "before": None, "after": dict(c)})
        elif r < 0.97:  # update
            cid = rng.choice(live)
            before = dict(state[cid])
            after = dict(before)
            field = rng.random()
            if field < 0.35:
                after["mobile"] = mobile(rng)
            elif field < 0.60:
                after["district"] = rng.choice(CITIES[after["city"]]["districts"])
            elif field < 0.75:
                city = rng.choice([c for c in CITIES if c != after["city"]])
                after["city"], after["district"] = city, rng.choice(CITIES[city]["districts"])
            elif field < 0.90 and after["email"]:
                after["email"] = after["email"].split("@")[0] + "@" + rng.choice(EMAIL_DOMAINS)
            else:
                after["is_deleted"] = 1  # soft delete (account closed)
            after["updated_at"] = ts(t)
            state[cid] = after
            events.append({"op": "u", "before": before, "after": dict(after)})
        else:  # hard delete: PDPL erasure request
            cid = rng.choice(live)
            before = state.pop(cid)
            events.append({"op": "d", "before": dict(before), "after": None})
        events[-1].update({"source": {"db": "qafila", "table": "customers", "lsn": lsn}, "ts_ms": int(t.timestamp() * 1000)})
    # arrival order: mostly by lsn, with local reordering and ~3% redelivered duplicates
    arrivals = []
    for i, e in enumerate(events):
        arrivals.append((i + rng.uniform(-4, 4), e))
        if rng.random() < 0.03:
            arrivals.append((i + rng.uniform(1, 30), e))
    arrivals.sort(key=lambda x: x[0])
    return snapshot, [e for _, e in arrivals], state


def make_orders(rng: random.Random, customers: dict[int, dict], products: list[dict]) -> tuple[list, list]:
    stores_by_city = {}
    for s in STORES:
        stores_by_city.setdefault(s[1], []).append(s)
    active = [p for p in products if p["is_active"]]
    popular = sorted(active, key=lambda p: rng.random())
    weights = [1 / (i + 5) for i in range(len(popular))]  # long-tail product popularity
    total_w = sum(weights)
    cum, acc = [], 0.0
    for w in weights:
        acc += w / total_w
        cum.append(acc)

    def pick_product():
        r = rng.random()
        lo, hi = 0, len(cum) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cum[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return popular[lo]

    cust_list = list(customers.values())
    orders, items, oid = [], [], 5_000_000
    day = START
    hour_w = [0.2, 0.1, 0.05, 0.03, 0.03, 0.05, 0.2, 0.5, 0.8, 0.9, 1.0, 1.2, 1.5, 1.4, 1.1, 1.0, 1.0, 1.1, 1.4, 1.8, 2.0, 1.9, 1.5, 0.8]
    while day < END:
        d = (day - START).days
        n = int(620 * (1 + 0.004 * d))                              # growth
        local_dow = (day + timedelta(hours=3)).weekday()             # Mon=0 ... Fri=4, Sat=5
        if local_dow in (3, 4):
            n = int(n * 1.25)                                         # Thu/Fri peak
        if day.month == 9 and day.day == 23:
            n = int(n * 1.9)                                          # Saudi National Day promo
        for _ in range(n):
            c = rng.choice(cust_list)
            # Riyadh local hour -> UTC
            hour = weighted(rng, [(h, w / sum(hour_w)) for h, w in enumerate(hour_w)])
            local = datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59), tzinfo=RIYADH)
            created = local.astimezone(timezone.utc)
            if not (START <= created < END):
                continue
            cs = stores_by_city[c["city"]]
            store = weighted(rng, [(s, s[6]) for s in cs])
            method = weighted(rng, PAYMENT_METHODS)
            r = rng.random()
            status = "delivered" if r < 0.885 else "cancelled" if r < 0.955 else "refunded" if r < 0.975 else "failed_delivery"
            if created > END - timedelta(hours=1) and rng.random() < 0.5:
                status = "placed"
            promo = None
            if day.month == 9 and day.day == 23 and rng.random() < 0.6:
                promo = "WATANI26"
            elif rng.random() < 0.12:
                promo = rng.choice(["WELCOME10", "FREEDEL", "WEEKEND15"])
            oid += 1
            updated = created + timedelta(minutes=rng.randint(20, 70))
            if status == "refunded":
                updated = created + timedelta(days=rng.randint(1, 6), minutes=rng.randint(0, 600))
            orders.append({
                "order_id": oid, "customer_id": c["customer_id"], "store_id": store[0], "status": status,
                "payment_method": method, "promo_code": promo, "delivery_fee": 0.0 if promo == "FREEDEL" else 9.0,
                "created_at": ts(created), "updated_at": ts(min(updated, END - timedelta(seconds=1))),
            })
            for line in range(1, rng.choice([1, 2, 2, 3, 3, 4, 5, 6, 8]) + 1):
                p = pick_product()
                qty = rng.choice([1, 1, 1, 2, 2, 3, 4])
                if rng.random() < 0.0008:
                    qty = -qty  # data issue
                price = round(p["unit_price"] * rng.uniform(0.97, 1.03), 2)
                disc = 0.0
                if promo in ("WATANI26", "WEEKEND15"):
                    disc = round(price * qty * 0.15, 2)
                elif promo == "WELCOME10":
                    disc = round(price * qty * 0.10, 2)
                items.append({"order_id": oid, "line_no": line, "product_id": p["product_id"], "quantity": qty,
                              "unit_price": price, "discount": disc})
        day += timedelta(days=1)
    return orders, items


def make_payments(rng: random.Random, orders: list[dict], items: list[dict]) -> list[dict]:
    totals = {}
    for it in items:
        totals[it["order_id"]] = totals.get(it["order_id"], 0.0) + it["unit_price"] * it["quantity"] - it["discount"]
    pays, pid = [], 9_000_000
    for o in orders:
        if o["payment_method"] == "cash" or o["status"] == "placed":
            continue
        created = datetime.strptime(o["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        amount = round(totals.get(o["order_id"], 0.0) + o["delivery_fee"], 2)
        if rng.random() < 0.004:
            amount = round(amount + rng.choice([-1, 1]) * rng.uniform(0.01, 5), 2)  # data issue: mismatch
        attempts = 2 if rng.random() < 0.04 else 1
        for a in range(attempts):
            pid += 1
            t = created + timedelta(seconds=20 + 40 * a)
            status = "failed" if a < attempts - 1 else ("refunded" if o["status"] in ("refunded", "cancelled") else "captured")
            upd = t if status != "refunded" else datetime.strptime(o["updated_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            p = {"payment_id": f"pay_{pid}", "order_id": o["order_id"], "amount": amount, "currency": "SAR",
                 "method": o["payment_method"], "status": status,
                 "card_last4": f"{rng.randint(0, 9999):04d}" if o["payment_method"] in ("card", "apple_pay") else None,
                 "created_at": iso_z(t), "updated_at": iso_z(upd)}
            if o["payment_method"] == "wallet" and upd >= SCHEMA_CHANGE:
                p["wallet_provider"] = rng.choice(WALLETS)
            pays.append(p)
    pays.sort(key=lambda p: (p["updated_at"], p["payment_id"]))
    return pays


def make_delivery_events(rng: random.Random, orders: list[dict]) -> list[dict]:
    store_city = {s[0]: s[1] for s in STORES}
    store_pos = {s[0]: (s[4], s[5]) for s in STORES}
    couriers = {city: [f"CR-{city[:3].upper()}-{i:03d}" for i in range(1, {"Riyadh": 90, "Jeddah": 60, "Dammam": 30}[city] + 1)] for city in CITIES}
    events, eid = [], 0
    for o in orders:
        created = datetime.strptime(o["created_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        if created < STREAM_START or o["status"] == "placed":
            continue
        city = store_city[o["store_id"]]
        courier = rng.choice(couriers[city])
        lat0, lon0 = store_pos[o["store_id"]]
        busy = 1.6 if o["store_id"] == "RUH-01" else 1.0
        t = created + timedelta(minutes=rng.uniform(0.5, 3) * busy)
        steps = [("assigned", t)]
        if o["status"] == "cancelled" and rng.random() < 0.7:
            steps.append(("cancelled", t + timedelta(minutes=rng.uniform(1, 6))))
        else:
            t = t + timedelta(minutes=rng.uniform(4, 11) * busy)
            steps.append(("picked_up", t))
            t = t + timedelta(minutes=rng.uniform(0.5, 2))
            steps.append(("out_for_delivery", t))
            t = t + timedelta(minutes=max(4.0, rng.gauss(16, 6)))
            steps.append(("failed" if o["status"] == "failed_delivery" else "delivered", t))
        offline = rng.random() < 0.03
        for kind, et in steps:
            eid += 1
            delay = timedelta(seconds=rng.uniform(0.3, 8))
            if offline and kind in ("out_for_delivery", "delivered", "failed"):
                delay = timedelta(minutes=rng.uniform(5, 50))  # courier phone offline: late arrival
            style = rng.random()
            if style < 0.8:
                event_time = et.astimezone(RIYADH).isoformat(timespec="milliseconds")       # 2026-09-24T21:05:11.123+03:00
            elif style < 0.95:
                event_time = iso_z(et)                                                         # 2026-09-24T18:05:11.123Z
            else:
                event_time = str(int(et.timestamp() * 1000))                                   # epoch millis (old app version)
            e = {"event_id": f"ev-{eid:07d}", "order_id": o["order_id"], "store_id": o["store_id"], "city": city,
                 "courier_id": courier, "event_type": kind, "event_time": event_time,
                 "lat": round(lat0 + rng.uniform(-0.04, 0.04), 5), "lon": round(lon0 + rng.uniform(-0.04, 0.04), 5),
                 "_ingest_ts": et + delay}
            events.append(e)
            if rng.random() < 0.015:
                d = dict(e)
                d["_ingest_ts"] = e["_ingest_ts"] + timedelta(seconds=rng.uniform(1, 90))  # redelivery
                events.append(d)
    events.sort(key=lambda e: e["_ingest_ts"])
    for e in events:
        e["ingest_time"] = iso_z(e.pop("_ingest_ts"))
    return events


def make_supplier_files(rng: random.Random, products: list[dict], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    suppliers = ["SUP-NAJD-DAIRY", "SUP-DATES-QSM", "SUP-FRESHFARMS", "SUP-HOMECARE", "SUP-PANTRY-CO"]
    by_cat = {"Dairy": 0, "Bakery": 2, "Fruits & Vegetables": 2, "Meat & Poultry": 2, "Beverages": 0, "Dates & Coffee": 1,
              "Snacks": 4, "Household": 3, "Personal Care": 3, "Frozen": 4, "Pantry": 4}
    for week, monday in [(36, "2026-08-31"), (37, "2026-09-07"), (38, "2026-09-14"), (39, "2026-09-21")]:
        rows = []
        for p in rng.sample(products, 160):
            cost = round(p["unit_price"] * rng.uniform(0.55, 0.8), 2)
            rows.append({"supplier_id": suppliers[by_cat[p["category"]]], "sku": p["sku"].strip(), "product_name_ar": p["name_ar"],
                         "unit_cost": cost, "valid_from": monday, "min_order_qty": rng.choice([6, 12, 24, 48])})
        path = out / f"prices_2026-W{week}.csv"
        if week == 38:  # decimal commas and Arabic-Indic digits from a supplier's Excel export
            for r in rows:
                v = f"{r['unit_cost']:.2f}".replace(".", ",")
                r["unit_cost"] = to_arabic_digits(f"{r['unit_cost']:.2f}") if rng.random() < 0.2 else v
        if week == 39:  # contract change: renamed column, new column, bad values, unknown SKUs
            for r in rows:
                r["cost_sar"] = r.pop("unit_cost")
                r["currency"] = "SAR"
            for r in rng.sample(rows, 4):
                r["cost_sar"] = rng.choice([0, -5.5, -1])
            for r in rng.sample(rows, 3):
                r["sku"] = f"QF-NEW-{rng.randint(9000, 9999)}"
        with path.open("w", newline="", encoding="utf-8-sig" if week == 38 else "utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)


def make_reviews(rng: random.Random, orders: list[dict], items: list[dict], products: list[dict]) -> list[dict]:
    pmap = {p["product_id"]: p for p in products}
    delivered = {o["order_id"]: o for o in orders if o["status"] == "delivered"}
    pool = [it for it in items if it["order_id"] in delivered]
    reviews = []
    for i, it in enumerate(rng.sample(pool, 3000)):
        p, o = pmap[it["product_id"]], delivered[it["order_id"]]
        rating = weighted(rng, [(5, 0.45), (4, 0.25), (3, 0.12), (2, 0.08), (1, 0.10)])
        arabic = rng.random() < 0.55
        text = rng.choice((REVIEW_AR if arabic else REVIEW_EN)[rating]).replace("{item}", p["name_en"].split(" ", 1)[-1].lower())
        created = datetime.strptime(o["updated_at"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=rng.randint(1, 72))
        reviews.append({"review_id": 700000 + i, "product_id": p["product_id"], "customer_id": o["customer_id"], "rating": rating,
                        "lang": "ar" if arabic else "en", "review_text": text, "created_at": created.strftime("%Y-%m-%d %H:%M:%S")})
    return reviews


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_sqlite(path: Path, tables: dict[str, list[dict]], ddl: dict[str, str]) -> None:
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    for name, rows in tables.items():
        con.execute(ddl[name])
        cols = list(rows[0].keys()) if rows else []
        con.executemany(f"INSERT INTO {name} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                        [tuple(r[c] for c in cols) for r in rows])
    con.commit()
    con.close()


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


DDL = {
    "customers": """CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, email TEXT, mobile TEXT,
        national_id TEXT, iban TEXT, city TEXT, district TEXT, created_at TEXT, updated_at TEXT, is_deleted INTEGER DEFAULT 0)""",
    "stores": """CREATE TABLE stores (store_id TEXT PRIMARY KEY, city TEXT, name_en TEXT, name_ar TEXT, lat REAL, lon REAL, opened_at TEXT)""",
    "products": """CREATE TABLE products (product_id INTEGER PRIMARY KEY, sku TEXT, name_en TEXT, name_ar TEXT, category TEXT, brand TEXT,
        unit_price REAL, description_en TEXT, description_ar TEXT, is_active INTEGER, updated_at TEXT)""",
    "orders": """CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, store_id TEXT, status TEXT, payment_method TEXT,
        promo_code TEXT, delivery_fee REAL, created_at TEXT, updated_at TEXT)""",
    "order_items": """CREATE TABLE order_items (order_id INTEGER, line_no INTEGER, product_id INTEGER, quantity INTEGER,
        unit_price REAL, discount REAL, PRIMARY KEY (order_id, line_no))""",
}


def main(force: bool = False) -> None:
    marker = RAW / "_SUCCESS"
    if marker.exists() and not force:
        print(f"data already prepared in {RAW} (use --force to regenerate)")
        return
    if RAW.exists():
        shutil.rmtree(RAW)
    RAW.mkdir(parents=True)
    rng = random.Random(SEED)

    products = make_products(rng)
    customers0 = make_customers(rng, 6000)
    snapshot, cdc_events, final_state = make_cdc(rng, customers0)
    # Orders use every customer that ever existed (erased customers leave orphan orders behind)
    everyone = {c["customer_id"]: c for c in snapshot}
    for e in cdc_events:
        if e["op"] == "c":
            everyone.setdefault(e["after"]["customer_id"], e["after"])
    orders, items = make_orders(rng, everyone, products)
    # customers created in September cannot order before they exist: shift those orders' customer
    created = {cid: c["created_at"] for cid, c in everyone.items()}
    old_ids = [c["customer_id"] for c in snapshot]
    for o in orders:
        while created[o["customer_id"]] > o["created_at"]:
            o["customer_id"] = rng.choice(old_ids)
    payments = make_payments(rng, orders, items)
    events = make_delivery_events(rng, orders)
    reviews = make_reviews(rng, orders, items, products)

    stores = [{"store_id": s[0], "city": s[1], "name_en": f"Qafila {s[2]}", "name_ar": f"قافلة {s[3]}", "lat": s[4], "lon": s[5],
               "opened_at": f"2024-{(i % 12) + 1:02d}-01"} for i, s in enumerate(STORES)]
    final_customers = sorted(final_state.values(), key=lambda c: c["customer_id"])

    write_sqlite(RAW / "qafila_oltp.sqlite",
                 {"customers": final_customers, "stores": stores, "products": products, "orders": orders, "order_items": items}, DDL)
    write_jsonl(RAW / "api" / "payments.jsonl", payments)
    write_csv(RAW / "cdc" / "customers_snapshot_2026-09-01.csv", snapshot)
    write_jsonl(RAW / "cdc" / "customers_changes.jsonl", cdc_events)
    write_jsonl(RAW / "events" / "delivery_events.jsonl", events)
    make_supplier_files(rng, products, RAW / "suppliers")
    write_csv(RAW / "reviews.csv", reviews)
    (RAW / "DATA_ISSUES.txt").write_text(DATA_ISSUES.strip() + "\n", encoding="utf-8")
    marker.write_text("ok\n")

    print(f"wrote {RAW}")
    print(f"  customers {len(final_customers):,} (snapshot {len(snapshot):,}, {len(cdc_events):,} CDC events)")
    print(f"  products {len(products):,} | stores {len(stores)} | orders {len(orders):,} | order_items {len(items):,}")
    print(f"  payments {len(payments):,} | delivery events {len(events):,} | reviews {len(reviews):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="regenerate even if data exists")
    main(ap.parse_args().force)
