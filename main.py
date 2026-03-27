
import asyncio
import html
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import CommandStart, Text
from aiogram.utils.exceptions import MessageNotModified
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

load_dotenv(Path(__file__).with_name(".env"))

# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions").strip()
DB_PATH = os.getenv("DB_PATH", "finance_bot.db").strip()
TIMEZONE_NAME = os.getenv("BOT_TIMEZONE", "Asia/Tashkent").strip()
DEFAULT_USD_RATE = Decimal(os.getenv("DEFAULT_USD_RATE", "12750") or "12750")
ADMIN_IDS = set()
for part in os.getenv("ADMIN_IDS", "").split(","):
    part = part.strip()
    if part.isdigit():
        ADMIN_IDS.add(int(part))

try:
    TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TZ = timezone(timedelta(hours=5))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("finance-bot-py39")

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)

# =========================================================
# CONSTANTS / UI
# =========================================================
BUTTON_TODAY = "📊 Bugun"
BUTTON_MONTH = "🗓 Oy"
BUTTON_BALANCE = "💰 Balans"
BUTTON_EXPORT = "📤 Excel"
BUTTON_RECORDS = "🧾 So‘nggi 10"
BUTTON_UNDO = "↩️ Undo"
BUTTON_RATE = "⚙️ Kurs"
BUTTON_HELP = "ℹ️ Yordam"

MAIN_KEYBOARD = types.ReplyKeyboardMarkup(resize_keyboard=True)
MAIN_KEYBOARD.row(BUTTON_TODAY, BUTTON_MONTH)
MAIN_KEYBOARD.row(BUTTON_BALANCE, BUTTON_EXPORT)
MAIN_KEYBOARD.row(BUTTON_RECORDS, BUTTON_UNDO)
MAIN_KEYBOARD.row(BUTTON_RATE, BUTTON_HELP)

HELP_TEXT = (
    "<b>Qanday ishlaydi</b>\n"
    "• <code>+...</code> bilan boshlangan summa = <b>kirim</b>\n"
    "• <code>-...</code> yoki belgisiz summa = <b>chiqim</b>\n"
    "• <code>mln</code> = 1 000 000\n"
    "• <code>ming</code> yoki <code>k</code> = 1 000\n"
    "• <code>$</code> yoki <code>usd</code> bo‘lsa, kurs bo‘yicha <b>UZS</b> ga aylantiriladi\n\n"
    "<b>Misollar</b>\n"
    "<code>+250$ klient to‘lovi</code>\n"
    "<code>100 ming dostavka</code>\n"
    "<code>+517ming azam aka labo</code>\n"
    "<code>3,9 mln metan resor</code>\n"
    "<code>[27.03.2026 16:45] Алишеров Орифжон: 500$ +350 ming -100 ming 120 ming dokument</code>\n\n"
    "<b>Buyruqlar</b>\n"
    "/start — menyu\n"
    "/help — yordam\n"
    "/stats [today|week|month|YYYY-MM|YYYY-MM-DD] — statistika\n"
    "/balance [period] — balans\n"
    "/records [son] — oxirgi yozuvlar\n"
    "/categories [period] — kategoriya kesimi\n"
    "/export [period|all] — Excel yuklab olish\n"
    "/rate [qiymat] — USD kursini ko‘rish yoki o‘zgartirish\n"
    "/undo — oxirgi saqlangan amalni bekor qilish\n"
    "/delete &lt;id&gt; — bitta yozuvni o‘chirish"
)

AMOUNT_RE = re.compile(
    r"(?<![\w/])"
    r"(?P<sign>[+-]?)\s*"
    r"(?P<number>\d+(?:[.,]\d{1,3})*(?:[.,]\d+)?)"
    r"\s*(?P<mult>mln\.?|million|mlyon|млн\.?|ming|mingta|thousand|тыс\.?|k)?"
    r"\s*(?P<currency>\$|usd|dollar|dollars|доллар|uzs|sum|сум|so['ʻ’]?m|som)?",
    re.IGNORECASE,
)

TELEGRAM_LINE_RE = re.compile(
    r"^\[(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\]\s*(?P<author>[^:]+):\s*(?P<body>.+)$"
)

INCOME = "income"
EXPENSE = "expense"

CATEGORY_KEYWORDS = {
    "dostavka": ["dostavka", "delivery", "yetkaz", "temir"],
    "dokument": ["dokument", "document", "hujjat"],
    "avans": ["avans", "advance"],
    "predoplata": ["predoplata", "prepayment", "oldindan"],
    "xizmat": ["xizmat", "service", "resor", "metan", "remont", "nikel"],
    "transport": ["labo", "mashina", "avto", "benzin", "yoqilg", "metan"],
    "qarz": ["qarz", "berdim", "oldim", "olindi"],
    "maosh": ["maosh", "oylik", "salary"],
    "sotuv": ["to'lov", "tolov", "oplata", "sale", "sotuv", "kirim"],
}

PENDING_BY_TOKEN: Dict[str, "PendingBatch"] = {}
LAST_PENDING_TOKEN_BY_USER: Dict[int, str] = {}

# =========================================================
# DATA MODELS
# =========================================================
@dataclass
class TxDraft:
    tx_at: datetime
    author: str
    direction: str
    original_amount: Decimal
    original_currency: str
    usd_rate: Decimal
    amount_uzs: int
    description: str
    source_line: str
    category: str
    counterparty: str

@dataclass
class PendingBatch:
    token: str
    user_id: int
    chat_id: int
    source_text: str
    created_at: datetime
    items: List[TxDraft]


# =========================================================
# HELPERS
# =========================================================
def now_local() -> datetime:
    return datetime.now(TZ)

def user_full_name(user: types.User) -> str:
    if user.full_name:
        return user.full_name
    pieces = [user.first_name or "", user.last_name or ""]
    return " ".join([p for p in pieces if p]).strip() or str(user.id)

def fmt_int(amount: int) -> str:
    return "{:,}".format(int(amount)).replace(",", " ")

def fmt_decimal(amount: Decimal) -> str:
    normalized = amount.quantize(Decimal("0.01")).normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text

def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")

def collapse_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS

def decimal_from_number(number_text: str, multiplier: Optional[str]) -> Decimal:
    text = (number_text or "").strip().replace(" ", "")
    mult_present = bool(multiplier)

    if mult_present:
        if text.count(",") + text.count(".") == 1:
            text = text.replace(",", ".")
        else:
            last_dot = text.rfind(".")
            last_comma = text.rfind(",")
            sep_pos = max(last_dot, last_comma)
            if sep_pos != -1:
                int_part = re.sub(r"[.,]", "", text[:sep_pos])
                frac_part = re.sub(r"[.,]", "", text[sep_pos + 1 :])
                text = int_part + "." + frac_part
            else:
                text = re.sub(r"[.,]", "", text)
    else:
        if text.count(",") + text.count(".") == 1:
            if "," in text:
                left, right = text.split(",", 1)
                if len(right) == 3 and len(left) >= 1:
                    text = left + right
                else:
                    text = left + "." + right
            else:
                left, right = text.split(".", 1)
                if len(right) == 3 and len(left) >= 1:
                    text = left + right
                else:
                    text = left + "." + right
        else:
            text = re.sub(r"[.,]", "", text)

    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")

def multiplier_value(multiplier: Optional[str]) -> Decimal:
    if not multiplier:
        return Decimal("1")
    m = multiplier.lower()
    if m.startswith("mln") or "million" in m or "mlyon" in m or "млн" in m:
        return Decimal("1000000")
    if m.startswith("ming") or "thousand" in m or "тыс" in m or m == "k":
        return Decimal("1000")
    return Decimal("1")

def normalize_currency(curr: Optional[str]) -> str:
    if not curr:
        return "UZS"
    c = curr.lower()
    if c in ["$", "usd", "dollar", "dollars", "доллар"]:
        return "USD"
    return "UZS"

def guess_counterparty(description: str) -> str:
    text = collapse_spaces(description)
    if not text:
        return ""
    tokens = text.split()
    if len(tokens) >= 2:
        joined = " ".join(tokens[:2])
        if re.search(r"[A-Za-zА-Яа-яЁё]", joined):
            return joined[:80]
    if tokens and re.search(r"[A-Za-zА-Яа-яЁё]", tokens[0]):
        return tokens[0][:80]
    return ""

def guess_category(description: str, direction: str) -> str:
    lowered = (description or "").lower()
    for category, words in CATEGORY_KEYWORDS.items():
        for word in words:
            if word in lowered:
                return category
    return "kirim" if direction == INCOME else "chiqim"

async def groq_enrich(description: str) -> Dict[str, str]:
    if not GROQ_API_KEY or not description:
        return {}
    payload = {
        "model": GROQ_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Siz finance parser helpersiz. "
                    "Faqat JSON qaytaring: "
                    '{"category":"...", "counterparty":"...", "cleaned_description":"..."} . '
                    "Kategoriya juda qisqa bo'lsin. "
                    "Agar counterparty aniq bo'lmasa bo'sh qoldiring."
                ),
            },
            {"role": "user", "content": description},
        ],
    }
    headers = {
        "Authorization": "Bearer " + GROQ_API_KEY,
        "Content-Type": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=15)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(GROQ_BASE_URL, headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    logger.warning("Groq returned status %s", resp.status)
                    return {}
                data = await resp.json()
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        result = {}
        for key in ("category", "counterparty", "cleaned_description"):
            value = collapse_spaces(str(parsed.get(key, "")))
            result[key] = value[:1200]
        return result
    except Exception:
        logger.exception("Groq enrich failed")
        return {}

def strip_amount_fragments(line: str) -> str:
    cleaned = AMOUNT_RE.sub(" ", line)
    cleaned = re.sub(r"\s*[\+\-]\s*", " ", cleaned)
    return collapse_spaces(cleaned)

def probably_false_match(number_text: str, sign: str, mult: Optional[str], curr: Optional[str]) -> bool:
    raw = re.sub(r"[^\d]", "", number_text or "")
    if not raw:
        return True
    if sign == "" and not mult and not curr and len(raw) <= 2:
        return True
    if sign == "" and not mult and not curr and re.fullmatch(r"0\d{1,3}", raw):
        return True
    return False

async def parse_line_to_drafts(
    line: str,
    default_dt: datetime,
    default_author: str,
    usd_rate: Decimal,
) -> List[TxDraft]:
    line = line.strip()
    if not line:
        return []

    tx_at = default_dt
    author = default_author
    body = line

    match = TELEGRAM_LINE_RE.match(line)
    if match:
        try:
            tx_at = datetime.strptime(match.group("dt"), "%d.%m.%Y %H:%M").replace(tzinfo=TZ)
        except ValueError:
            tx_at = default_dt
        author = collapse_spaces(match.group("author")) or default_author
        body = collapse_spaces(match.group("body"))

    if not body:
        return []

    description = strip_amount_fragments(body)
    drafts = []

    for amount_match in AMOUNT_RE.finditer(body):
        sign = amount_match.group("sign") or ""
        number_text = amount_match.group("number") or ""
        mult = amount_match.group("mult")
        curr = amount_match.group("currency")

        if probably_false_match(number_text, sign, mult, curr):
            continue

        base_value = decimal_from_number(number_text, mult)
        if base_value <= 0:
            continue

        original_amount = base_value * multiplier_value(mult)
        direction = INCOME if sign == "+" else EXPENSE
        original_currency = normalize_currency(curr)

        if original_currency == "USD":
            amount_uzs_decimal = (original_amount * usd_rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        else:
            amount_uzs_decimal = original_amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

        cleaned_desc = description or body
        enriched = await groq_enrich(cleaned_desc)
        category = enriched.get("category") or guess_category(cleaned_desc, direction)
        counterparty = enriched.get("counterparty") or guess_counterparty(cleaned_desc)
        final_desc = enriched.get("cleaned_description") or cleaned_desc

        drafts.append(
            TxDraft(
                tx_at=tx_at,
                author=author,
                direction=direction,
                original_amount=original_amount,
                original_currency=original_currency,
                usd_rate=usd_rate,
                amount_uzs=int(amount_uzs_decimal),
                description=final_desc[:1000],
                source_line=body[:1000],
                category=category[:100],
                counterparty=counterparty[:120],
            )
        )

    return drafts

async def parse_message_to_pending(message: types.Message, usd_rate: Decimal) -> Optional[PendingBatch]:
    text = (message.text or "").strip()
    if not text:
        return None
    author = user_full_name(message.from_user)
    default_dt = message.date.astimezone(TZ) if message.date else now_local()
    items: List[TxDraft] = []
    for raw_line in text.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        line_items = await parse_line_to_drafts(raw_line, default_dt, author, usd_rate)
        items.extend(line_items)
    if not items:
        return None

    token = uuid4().hex[:12]
    pending = PendingBatch(
        token=token,
        user_id=message.from_user.id,
        chat_id=message.chat.id,
        source_text=text[:6000],
        created_at=now_local(),
        items=items,
    )
    PENDING_BY_TOKEN[token] = pending
    LAST_PENDING_TOKEN_BY_USER[message.from_user.id] = token
    return pending

def build_pending_preview(pending: PendingBatch) -> str:
    income_total = sum(item.amount_uzs for item in pending.items if item.direction == INCOME)
    expense_total = sum(item.amount_uzs for item in pending.items if item.direction == EXPENSE)
    net = income_total - expense_total

    lines = [
        "<b>Tekshiruv oynasi</b>",
        "Topilgan amallar: <b>{}</b>".format(len(pending.items)),
        "",
    ]
    for idx, item in enumerate(pending.items[:12], start=1):
        icon = "🟢" if item.direction == INCOME else "🔴"
        original = "{} {}".format(fmt_decimal(item.original_amount), item.original_currency)
        lines.append(
            "{idx}. {icon} <b>{direction}</b> — <b>{uzs} UZS</b> "
            "(asl: {original})\n"
            "   <i>{when}</i>\n"
            "   Izoh: {desc}\n"
            "   Kategoriya: {cat}".format(
                idx=idx,
                icon=icon,
                direction="Kirim" if item.direction == INCOME else "Chiqim",
                uzs=fmt_int(item.amount_uzs),
                original=html.escape(original),
                when=fmt_dt(item.tx_at),
                desc=html.escape(item.description or "-"),
                cat=html.escape(item.category or "-"),
            )
        )
    if len(pending.items) > 12:
        lines.append("… yana <b>{}</b> ta amal bor".format(len(pending.items) - 12))
    lines.extend(
        [
            "",
            "Jami kirim: <b>{} UZS</b>".format(fmt_int(income_total)),
            "Jami chiqim: <b>{} UZS</b>".format(fmt_int(expense_total)),
            "Qoldiq: <b>{} UZS</b>".format(fmt_int(net)),
            "",
            "Saqlansinmi?",
        ]
    )
    return "\n".join(lines)

def pending_keyboard(token: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Saqlash", callback_data="save:" + token),
        types.InlineKeyboardButton("❌ Bekor qilish", callback_data="cancel:" + token),
    )
    return kb

def saved_keyboard(batch_id: int) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("↩️ Oxirgi batchni bekor qilish", callback_data="undo_batch:" + str(batch_id)))
    return kb

def parse_period_arg(arg: str) -> Tuple[Optional[datetime], Optional[datetime], str]:
    now = now_local()
    arg = (arg or "month").strip().lower()
    if arg in ("today", "bugun"):
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return start, end, "bugun"
    if arg in ("week", "hafta"):
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
        return start, end, "shu hafta"
    if arg in ("month", "oy"):
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, "shu oy"
    if arg == "all":
        return None, None, "barchasi"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", arg):
        start = datetime.strptime(arg, "%Y-%m-%d").replace(tzinfo=TZ)
        end = start + timedelta(days=1)
        return start, end, arg
    if re.fullmatch(r"\d{4}-\d{2}", arg):
        start = datetime.strptime(arg, "%Y-%m").replace(day=1, tzinfo=TZ)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, arg
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end, "shu oy"

def command_arg(text: str) -> str:
    if not text:
        return ""
    parts = text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""

# =========================================================
# DATABASE
# =========================================================
async def ensure_column(db: aiosqlite.Connection, table: str, column: str, decl: str) -> None:
    cursor = await db.execute("PRAGMA table_info({})".format(table))
    rows = await cursor.fetchall()
    existing = set(row[1] for row in rows)
    if column not in existing:
        await db.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, column, decl))

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                source_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                saved_at TEXT NOT NULL,
                summary_text TEXT NOT NULL DEFAULT '',
                item_count INTEGER NOT NULL DEFAULT 0,
                income_total_uzs INTEGER NOT NULL DEFAULT 0,
                expense_total_uzs INTEGER NOT NULL DEFAULT 0,
                net_total_uzs INTEGER NOT NULL DEFAULT 0,
                undone_at TEXT
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                author TEXT NOT NULL DEFAULT '',
                source_line TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                counterparty TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL,
                original_amount REAL NOT NULL DEFAULT 0,
                original_currency TEXT NOT NULL DEFAULT 'UZS',
                usd_rate REAL NOT NULL DEFAULT 0,
                amount_uzs INTEGER NOT NULL DEFAULT 0,
                tx_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                is_deleted INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(batch_id) REFERENCES batches(id)
            );

            CREATE INDEX IF NOT EXISTS idx_transactions_tx_at ON transactions(tx_at);
            CREATE INDEX IF NOT EXISTS idx_transactions_direction ON transactions(direction);
            CREATE INDEX IF NOT EXISTS idx_transactions_deleted ON transactions(is_deleted);
            CREATE INDEX IF NOT EXISTS idx_transactions_batch ON transactions(batch_id);
            """
        )

        # Migrations for old databases
        for column, decl in [
            ("summary_text", "TEXT NOT NULL DEFAULT ''"),
            ("item_count", "INTEGER NOT NULL DEFAULT 0"),
            ("income_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("expense_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("net_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("undone_at", "TEXT"),
        ]:
            await ensure_column(db, "batches", column, decl)

        for column, decl in [
            ("user_id", "INTEGER NOT NULL DEFAULT 0"),
            ("chat_id", "INTEGER NOT NULL DEFAULT 0"),
            ("author", "TEXT NOT NULL DEFAULT ''"),
            ("source_line", "TEXT NOT NULL DEFAULT ''"),
            ("description", "TEXT NOT NULL DEFAULT ''"),
            ("counterparty", "TEXT NOT NULL DEFAULT ''"),
            ("category", "TEXT NOT NULL DEFAULT ''"),
            ("direction", "TEXT NOT NULL DEFAULT 'expense'"),
            ("original_amount", "REAL NOT NULL DEFAULT 0"),
            ("original_currency", "TEXT NOT NULL DEFAULT 'UZS'"),
            ("usd_rate", "REAL NOT NULL DEFAULT 0"),
            ("amount_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("tx_at", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            await ensure_column(db, "transactions", column, decl)

        cur = await db.execute("SELECT value FROM settings WHERE key = 'usd_rate'")
        row = await cur.fetchone()
        if not row:
            await db.execute(
                "INSERT OR REPLACE INTO settings(key, value) VALUES('usd_rate', ?)",
                (str(DEFAULT_USD_RATE),),
            )
        await db.commit()

async def get_usd_rate() -> Decimal:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = 'usd_rate'")
        row = await cur.fetchone()
        if row and row[0]:
            try:
                return Decimal(str(row[0]))
            except InvalidOperation:
                return DEFAULT_USD_RATE
    return DEFAULT_USD_RATE

async def set_usd_rate(value: Decimal) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES('usd_rate', ?)",
            (str(value),),
        )
        await db.commit()

async def save_pending_batch(pending: PendingBatch) -> int:
    created_at_text = now_local().isoformat()
    income_total = sum(item.amount_uzs for item in pending.items if item.direction == INCOME)
    expense_total = sum(item.amount_uzs for item in pending.items if item.direction == EXPENSE)
    net_total = income_total - expense_total
    summary_text = build_pending_preview(pending)

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO batches (
                user_id, chat_id, source_text, created_at, saved_at, summary_text,
                item_count, income_total_uzs, expense_total_uzs, net_total_uzs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pending.user_id,
                pending.chat_id,
                pending.source_text,
                pending.created_at.isoformat(),
                created_at_text,
                summary_text,
                len(pending.items),
                income_total,
                expense_total,
                net_total,
            ),
        )
        batch_id = cur.lastrowid

        for item in pending.items:
            await db.execute(
                """
                INSERT INTO transactions (
                    batch_id, user_id, chat_id, author, source_line, description, counterparty,
                    category, direction, original_amount, original_currency, usd_rate, amount_uzs,
                    tx_at, created_at, is_deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    batch_id,
                    pending.user_id,
                    pending.chat_id,
                    item.author,
                    item.source_line,
                    item.description,
                    item.counterparty,
                    item.category,
                    item.direction,
                    float(item.original_amount),
                    item.original_currency,
                    float(item.usd_rate),
                    item.amount_uzs,
                    item.tx_at.isoformat(),
                    created_at_text,
                ),
            )
        await db.commit()
        return int(batch_id)

async def undo_batch(batch_id: int, requester_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM batches WHERE id = ?", (batch_id,))
        row = await cur.fetchone()
        if not row:
            return False
        if row["undone_at"]:
            return False
        if (row["user_id"] != requester_id) and (not is_admin(requester_id)):
            return False
        undone_at = now_local().isoformat()
        await db.execute("UPDATE batches SET undone_at = ? WHERE id = ?", (undone_at, batch_id))
        await db.execute("UPDATE transactions SET is_deleted = 1 WHERE batch_id = ?", (batch_id,))
        await db.commit()
        return True

async def undo_last_batch_for_user(user_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id FROM batches
            WHERE user_id = ? AND undone_at IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        batch_id = int(row["id"])
    success = await undo_batch(batch_id, user_id)
    return batch_id if success else None

def base_where_and_params(start: Optional[datetime], end: Optional[datetime]) -> Tuple[str, List[Any]]:
    where = ["is_deleted = 0"]
    params: List[Any] = []
    if start is not None:
        where.append("tx_at >= ?")
        params.append(start.isoformat())
    if end is not None:
        where.append("tx_at < ?")
        params.append(end.isoformat())
    return " WHERE " + " AND ".join(where), params

async def stats_for_period(start: Optional[datetime], end: Optional[datetime]) -> Dict[str, int]:
    where_sql, params = base_where_and_params(start, end)
    query = (
        "SELECT "
        "COALESCE(SUM(CASE WHEN direction = 'income' THEN amount_uzs ELSE 0 END), 0) AS income_total, "
        "COALESCE(SUM(CASE WHEN direction = 'expense' THEN amount_uzs ELSE 0 END), 0) AS expense_total, "
        "COUNT(*) AS tx_count "
        "FROM transactions" + where_sql
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        income = int(row["income_total"] or 0)
        expense = int(row["expense_total"] or 0)
        return {
            "income": income,
            "expense": expense,
            "net": income - expense,
            "count": int(row["tx_count"] or 0),
        }

async def records_latest(limit: int) -> List[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, tx_at, direction, amount_uzs, original_amount, original_currency,
                   category, counterparty, description, author
            FROM transactions
            WHERE is_deleted = 0
            ORDER BY tx_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return await cur.fetchall()

async def category_breakdown(start: Optional[datetime], end: Optional[datetime]) -> List[aiosqlite.Row]:
    where_sql, params = base_where_and_params(start, end)
    query = (
        "SELECT category, "
        "COALESCE(SUM(CASE WHEN direction = 'income' THEN amount_uzs ELSE 0 END), 0) AS income_total, "
        "COALESCE(SUM(CASE WHEN direction = 'expense' THEN amount_uzs ELSE 0 END), 0) AS expense_total, "
        "COUNT(*) AS tx_count "
        "FROM transactions" + where_sql + " GROUP BY category ORDER BY expense_total DESC, income_total DESC"
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        return await cur.fetchall()

async def delete_transaction(tx_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("UPDATE transactions SET is_deleted = 1 WHERE id = ? AND is_deleted = 0", (tx_id,))
        await db.commit()
        return cur.rowcount > 0

async def fetch_transactions_for_export(start: Optional[datetime], end: Optional[datetime]) -> List[aiosqlite.Row]:
    where_sql, params = base_where_and_params(start, end)
    query = (
        "SELECT id, batch_id, tx_at, direction, original_amount, original_currency, usd_rate, amount_uzs, "
        "author, category, counterparty, description, source_line, created_at "
        "FROM transactions" + where_sql + " ORDER BY tx_at ASC, id ASC"
    )
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        return await cur.fetchall()

# =========================================================
# EXCEL
# =========================================================
def style_header(ws, row_idx: int = 1) -> None:
    fill = PatternFill("solid", fgColor="1F4E78")
    font = Font(color="FFFFFF", bold=True)
    border = Border(
        left=Side(style="thin", color="D9E1F2"),
        right=Side(style="thin", color="D9E1F2"),
        top=Side(style="thin", color="D9E1F2"),
        bottom=Side(style="thin", color="D9E1F2"),
    )
    for cell in ws[row_idx]:
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border

def auto_width(ws) -> None:
    widths: Dict[int, int] = {}
    for row in ws.iter_rows():
        for cell in row:
            value = "" if cell.value is None else str(cell.value)
            widths[cell.column] = max(widths.get(cell.column, 0), min(len(value) + 2, 50))
    for col_idx, width in widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = width

def tx_row_values(row: aiosqlite.Row) -> List[Any]:
    tx_at = datetime.fromisoformat(row["tx_at"]).astimezone(TZ) if row["tx_at"] else now_local()
    return [
        row["id"],
        tx_at.strftime("%d.%m.%Y %H:%M"),
        tx_at.strftime("%Y"),
        tx_at.strftime("%m"),
        tx_at.strftime("%d"),
        tx_at.strftime("%H:%M"),
        "Kirim" if row["direction"] == INCOME else "Chiqim",
        row["original_amount"],
        row["original_currency"],
        row["usd_rate"],
        row["amount_uzs"],
        row["author"],
        row["category"],
        row["counterparty"],
        row["description"],
        row["source_line"],
        row["batch_id"],
        row["created_at"],
    ]

def build_excel_file(rows: List[aiosqlite.Row], label: str) -> str:
    wb = Workbook()
    ws_dash = wb.active
    ws_dash.title = "Dashboard"
    ws_all = wb.create_sheet("Barcha")
    ws_income = wb.create_sheet("Kirim")
    ws_expense = wb.create_sheet("Chiqim")

    income_total = sum(int(r["amount_uzs"]) for r in rows if r["direction"] == INCOME)
    expense_total = sum(int(r["amount_uzs"]) for r in rows if r["direction"] == EXPENSE)
    net_total = income_total - expense_total

    ws_dash["A1"] = "Hisobot"
    ws_dash["B1"] = label
    ws_dash["A2"] = "Yaratilgan vaqt"
    ws_dash["B2"] = fmt_dt(now_local())
    ws_dash["A4"] = "Jami kirim (UZS)"
    ws_dash["B4"] = income_total
    ws_dash["A5"] = "Jami chiqim (UZS)"
    ws_dash["B5"] = expense_total
    ws_dash["A6"] = "Qoldiq (UZS)"
    ws_dash["B6"] = net_total
    ws_dash["A7"] = "Amallar soni"
    ws_dash["B7"] = len(rows)
    ws_dash["A1"].font = Font(bold=True, size=14)
    for cell in ("A4", "A5", "A6", "A7"):
        ws_dash[cell].font = Font(bold=True)
    auto_width(ws_dash)

    headers = [
        "ID",
        "Sana-vaqt",
        "Yil",
        "Oy",
        "Kun",
        "Soat",
        "Yo‘nalish",
        "Asl summa",
        "Valyuta",
        "USD kursi",
        "UZS summa",
        "Muallif",
        "Kategoriya",
        "Kontragent",
        "Izoh",
        "Asl satr",
        "Batch ID",
        "Saqlangan vaqt",
    ]
    for ws in (ws_all, ws_income, ws_expense):
        ws.append(headers)
        style_header(ws)

    for row in rows:
        values = tx_row_values(row)
        ws_all.append(values)
        if row["direction"] == INCOME:
            ws_income.append(values)
        else:
            ws_expense.append(values)

    for ws in (ws_all, ws_income, ws_expense):
        ws.freeze_panes = "A2"
        auto_width(ws)

    path = os.path.join(tempfile.gettempdir(), "finance_export_{}.xlsx".format(uuid4().hex[:8]))
    wb.save(path)
    return path

# =========================================================
# RESPONSE BUILDERS
# =========================================================
def build_stats_text(label: str, stats: Dict[str, int]) -> str:
    return (
        "<b>Statistika — {label}</b>\n"
        "Amallar soni: <b>{count}</b>\n"
        "Jami kirim: <b>{income} UZS</b>\n"
        "Jami chiqim: <b>{expense} UZS</b>\n"
        "Qoldiq: <b>{net} UZS</b>"
    ).format(
        label=html.escape(label),
        count=stats["count"],
        income=fmt_int(stats["income"]),
        expense=fmt_int(stats["expense"]),
        net=fmt_int(stats["net"]),
    )

def build_records_text(rows: List[aiosqlite.Row]) -> str:
    if not rows:
        return "Hozircha yozuv topilmadi."
    parts = ["<b>So‘nggi yozuvlar</b>"]
    for row in rows:
        dt = datetime.fromisoformat(row["tx_at"]).astimezone(TZ) if row["tx_at"] else now_local()
        icon = "🟢" if row["direction"] == INCOME else "🔴"
        parts.append(
            "{icon} <b>#{id}</b> — <b>{amount} UZS</b> ({kind})\n"
            "<i>{when}</i>\n"
            "Izoh: {desc}\n"
            "Kategoriya: {cat}\n"
            "Muallif: {author}".format(
                icon=icon,
                id=row["id"],
                amount=fmt_int(int(row["amount_uzs"] or 0)),
                kind="kirim" if row["direction"] == INCOME else "chiqim",
                when=dt.strftime("%d.%m.%Y %H:%M"),
                desc=html.escape(row["description"] or "-"),
                cat=html.escape(row["category"] or "-"),
                author=html.escape(row["author"] or "-"),
            )
        )
    return "\n\n".join(parts)

def build_categories_text(rows: List[aiosqlite.Row], label: str) -> str:
    if not rows:
        return "Bu davr bo‘yicha kategoriya ma’lumoti topilmadi."
    parts = ["<b>Kategoriyalar — {}</b>".format(html.escape(label))]
    for row in rows:
        income = fmt_int(int(row["income_total"] or 0))
        expense = fmt_int(int(row["expense_total"] or 0))
        parts.append(
            "• <b>{cat}</b> — kirim: <b>{income}</b>, chiqim: <b>{expense}</b>, amal: <b>{count}</b>".format(
                cat=html.escape(row["category"] or "-"),
                income=income,
                expense=expense,
                count=int(row["tx_count"] or 0),
            )
        )
    return "\n".join(parts)

# =========================================================
# HANDLERS
# =========================================================
@dp.message_handler(CommandStart())
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        "Salom. Menga summali matn yoki Telegram eksport logini yubor.\n"
        "Men avval preview chiqaraman, keyin saqlashni so‘rayman.",
        reply_markup=MAIN_KEYBOARD,
    )
    await message.answer(HELP_TEXT, reply_markup=MAIN_KEYBOARD)

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message) -> None:
    await message.answer(HELP_TEXT, reply_markup=MAIN_KEYBOARD)

@dp.message_handler(commands=["rate"])
async def cmd_rate(message: types.Message) -> None:
    arg = command_arg(message.text or "")
    if not arg:
        rate = await get_usd_rate()
        await message.answer("Joriy USD kursi: <b>{}</b>".format(fmt_decimal(rate)))
        return
    if not is_admin(message.from_user.id):
        await message.answer("Kursni faqat admin o‘zgartira oladi.")
        return
    try:
        value = Decimal(arg.replace(",", "."))
    except InvalidOperation:
        await message.answer("Noto‘g‘ri kurs. Masalan: <code>/rate 12750</code>")
        return
    if value <= 0:
        await message.answer("Kurs 0 dan katta bo‘lishi kerak.")
        return
    await set_usd_rate(value)
    await message.answer("Yangi USD kursi saqlandi: <b>{}</b>".format(fmt_decimal(value)))

@dp.message_handler(commands=["stats", "balance"])
async def cmd_stats(message: types.Message) -> None:
    arg = command_arg(message.text or "")
    start, end, label = parse_period_arg(arg or "month")
    stats = await stats_for_period(start, end)
    await message.answer(build_stats_text(label, stats))

@dp.message_handler(commands=["records"])
async def cmd_records(message: types.Message) -> None:
    arg = command_arg(message.text or "")
    try:
        limit = max(1, min(100, int(arg or "10")))
    except ValueError:
        limit = 10
    rows = await records_latest(limit)
    await message.answer(build_records_text(rows))

@dp.message_handler(commands=["categories"])
async def cmd_categories(message: types.Message) -> None:
    arg = command_arg(message.text or "")
    start, end, label = parse_period_arg(arg or "month")
    rows = await category_breakdown(start, end)
    await message.answer(build_categories_text(rows, label))

@dp.message_handler(commands=["delete"])
async def cmd_delete(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("O‘chirish faqat admin uchun.")
        return
    arg = command_arg(message.text or "")
    if not arg.isdigit():
        await message.answer("Masalan: <code>/delete 15</code>")
        return
    ok = await delete_transaction(int(arg))
    await message.answer("Yozuv o‘chirildi." if ok else "Topilmadi yoki oldin o‘chirilgan.")

@dp.message_handler(commands=["undo"])
async def cmd_undo(message: types.Message) -> None:
    batch_id = await undo_last_batch_for_user(message.from_user.id)
    if not batch_id:
        await message.answer("Bekor qilinadigan oxirgi batch topilmadi.")
        return
    await message.answer("Oxirgi saqlangan batch bekor qilindi: <b>#{}</b>".format(batch_id))

@dp.message_handler(commands=["export"])
async def cmd_export(message: types.Message) -> None:
    arg = command_arg(message.text or "")
    start, end, label = parse_period_arg(arg or "month")
    rows = await fetch_transactions_for_export(start, end)
    if not rows:
        await message.answer("Eksport uchun yozuv topilmadi.")
        return
    path = build_excel_file(rows, label)
    await message.answer_document(
        types.InputFile(path, filename="hisobot_{}.xlsx".format(label.replace(" ", "_"))),
        caption="Excel hisobot tayyor: <b>{}</b>".format(html.escape(label)),
    )

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("save:"))
async def callback_save(callback: types.CallbackQuery) -> None:
    token = callback.data.split(":", 1)[1]
    pending = PENDING_BY_TOKEN.get(token)
    if not pending:
        await callback.answer("Bu preview muddati tugagan.", show_alert=True)
        return
    if callback.from_user.id != pending.user_id and not is_admin(callback.from_user.id):
        await callback.answer("Bu preview sizga tegishli emas.", show_alert=True)
        return

    batch_id = await save_pending_batch(pending)
    PENDING_BY_TOKEN.pop(token, None)
    LAST_PENDING_TOKEN_BY_USER.pop(pending.user_id, None)

    text = (
        "✅ Saqlandi.\n"
        "Batch ID: <b>#{batch_id}</b>\n"
        "Amallar soni: <b>{count}</b>".format(batch_id=batch_id, count=len(pending.items))
    )
    try:
        await callback.message.edit_text(text, reply_markup=saved_keyboard(batch_id))
    except MessageNotModified:
        pass
    except Exception:
        await callback.message.answer(text, reply_markup=saved_keyboard(batch_id))
    await callback.answer("Saqlandi")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cancel:"))
async def callback_cancel(callback: types.CallbackQuery) -> None:
    token = callback.data.split(":", 1)[1]
    pending = PENDING_BY_TOKEN.get(token)
    if pending and (callback.from_user.id == pending.user_id or is_admin(callback.from_user.id)):
        PENDING_BY_TOKEN.pop(token, None)
        LAST_PENDING_TOKEN_BY_USER.pop(pending.user_id, None)
    try:
        await callback.message.edit_text("❌ Saqlash bekor qilindi.")
    except MessageNotModified:
        pass
    except Exception:
        await callback.message.answer("❌ Saqlash bekor qilindi.")
    await callback.answer("Bekor qilindi")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("undo_batch:"))
async def callback_undo_batch(callback: types.CallbackQuery) -> None:
    batch_id_text = callback.data.split(":", 1)[1]
    if not batch_id_text.isdigit():
        await callback.answer("Noto‘g‘ri batch.", show_alert=True)
        return
    batch_id = int(batch_id_text)
    ok = await undo_batch(batch_id, callback.from_user.id)
    if not ok:
        await callback.answer("Bekor qilib bo‘lmadi.", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass
    await callback.message.answer("↩️ Batch bekor qilindi: <b>#{}</b>".format(batch_id))
    await callback.answer("Bekor qilindi")

@dp.message_handler(Text(equals=BUTTON_HELP))
async def button_help(message: types.Message) -> None:
    await cmd_help(message)

@dp.message_handler(Text(equals=BUTTON_RATE))
async def button_rate(message: types.Message) -> None:
    rate = await get_usd_rate()
    await message.answer("Joriy USD kursi: <b>{}</b>".format(fmt_decimal(rate)))

@dp.message_handler(Text(equals=BUTTON_TODAY))
async def button_today(message: types.Message) -> None:
    start, end, label = parse_period_arg("today")
    stats = await stats_for_period(start, end)
    await message.answer(build_stats_text(label, stats))

@dp.message_handler(Text(equals=BUTTON_MONTH))
async def button_month(message: types.Message) -> None:
    start, end, label = parse_period_arg("month")
    stats = await stats_for_period(start, end)
    await message.answer(build_stats_text(label, stats))

@dp.message_handler(Text(equals=BUTTON_BALANCE))
async def button_balance(message: types.Message) -> None:
    start, end, label = parse_period_arg("month")
    stats = await stats_for_period(start, end)
    await message.answer(build_stats_text(label, stats))

@dp.message_handler(Text(equals=BUTTON_RECORDS))
async def button_records(message: types.Message) -> None:
    rows = await records_latest(10)
    await message.answer(build_records_text(rows))

@dp.message_handler(Text(equals=BUTTON_EXPORT))
async def button_export(message: types.Message) -> None:
    await message.answer("Oy bo‘yicha Excel uchun: <code>/export month</code>\nHammasi uchun: <code>/export all</code>")

@dp.message_handler(Text(equals=BUTTON_UNDO))
async def button_undo(message: types.Message) -> None:
    await cmd_undo(message)

@dp.message_handler(content_types=types.ContentType.TEXT)
async def handle_text(message: types.Message) -> None:
    text = (message.text or "").strip()
    if not text:
        return

    if text.startswith("/"):
        return

    usd_rate = await get_usd_rate()
    pending = await parse_message_to_pending(message, usd_rate)
    if not pending:
        await message.answer(
            "Men bu matndan ishonchli summa topolmadim.\n"
            "Masalan: <code>+250$ klient</code> yoki <code>100 ming dostavka</code>"
        )
        return

    preview = build_pending_preview(pending)
    await message.answer(preview, reply_markup=pending_keyboard(pending.token))

# =========================================================
# STARTUP
# =========================================================
async def on_startup(_: Dispatcher) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN topilmadi")
    await init_db()
    commands = [
        types.BotCommand("start", "Botni ishga tushirish"),
        types.BotCommand("help", "Yordam"),
        types.BotCommand("stats", "Statistika"),
        types.BotCommand("balance", "Balans"),
        types.BotCommand("records", "So‘nggi yozuvlar"),
        types.BotCommand("categories", "Kategoriya kesimi"),
        types.BotCommand("export", "Excel eksport"),
        types.BotCommand("rate", "USD kursi"),
        types.BotCommand("undo", "Oxirgi batchni bekor qilish"),
        types.BotCommand("delete", "Yozuvni o‘chirish"),
    ]
    await bot.set_my_commands(commands)
    logger.info("Bot ishga tushdi")

def main() -> None:
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

if __name__ == "__main__":
    main()
