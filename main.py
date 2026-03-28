import asyncio
import csv
import io
import json
import logging
import os
import re
import secrets
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

load_dotenv()

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
DB_PATH = os.getenv("DB_PATH", "finance_bot.db").strip()
TIMEZONE_NAME = os.getenv("BOT_TIMEZONE", os.getenv("TIMEZONE", "Asia/Tashkent")).strip()
DEFAULT_USD_RATE = Decimal(os.getenv("DEFAULT_USD_RATE", "12750").strip() or "12750")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
CBU_USD_API = os.getenv("CBU_USD_API", "https://cbu.uz/uz/arkhiv-kursov-valyut/json/USD/").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()

try:
    TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TZ = timezone(timedelta(hours=5))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("finance_bot")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())

PENDING_BATCHES: Dict[str, Dict[str, Any]] = {}

# ---------------------------------------------------------
# State
# ---------------------------------------------------------
class RateStates(StatesGroup):
    waiting_manual_rate = State()


# ---------------------------------------------------------
# Data structures
# ---------------------------------------------------------
@dataclass
class ParsedItem:
    tx_type: str               # income | expense
    amount_original: str       # original numeric string
    currency: str              # UZS | USD
    amount_uzs: int            # normalized to UZS
    description: str
    category: str
    counterparty: str
    author: str
    tx_at: str                 # ISO string
    source_line: str
    raw_text: str
    sign: str


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def parse_iso_to_local_text(value: Optional[str]) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return fmt_dt(dt)
    except Exception:
        return value


def money_fmt(value: int | float | Decimal) -> str:
    try:
        n = int(round(float(value)))
    except Exception:
        n = 0
    s = f"{n:,}".replace(",", " ")
    return f"{s} so'm"


def compact_money(value: int | float | Decimal) -> str:
    try:
        n = int(round(float(value)))
    except Exception:
        n = 0
    negative = n < 0
    n = abs(n)
    if n >= 1_000_000_000:
        out = f"{n / 1_000_000_000:.2f} mlrd"
    elif n >= 1_000_000:
        out = f"{n / 1_000_000:.2f} mln"
    elif n >= 1_000:
        out = f"{n / 1_000:.2f} ming"
    else:
        out = str(n)
    return f"- {out}" if negative else out


def sanitize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def strip_command_prefix(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("/"):
        return ""
    return text


def start_of_day(dt: Optional[datetime] = None) -> datetime:
    dt = dt or now_local()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def start_of_week(dt: Optional[datetime] = None) -> datetime:
    dt = dt or now_local()
    base = start_of_day(dt)
    return base - timedelta(days=base.weekday())


def start_of_month(dt: Optional[datetime] = None) -> datetime:
    dt = dt or now_local()
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def parse_dt_guess(text: Optional[str]) -> datetime:
    if not text:
        return now_local()
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        with suppress(Exception):
            return datetime.strptime(text, fmt).replace(tzinfo=TZ)
    return now_local()


def ensure_note_quality(note: str, source_text: str) -> str:
    note = sanitize_text(note)
    if note:
        return note
    source_text = sanitize_text(source_text)
    if source_text:
        return source_text[:180]
    return "Izoh kiritilmagan"


def deduce_category(text: str) -> str:
    t = sanitize_text(text).lower()
    rules = [
        ("dokument", "Hujjat"),
        ("dostav", "Dostavka"),
        ("temir", "Temir / material"),
        ("metan", "Gaz / metan"),
        ("resor", "Xizmat / ta'mirlash"),
        ("xizmat", "Xizmat / ta'mirlash"),
        ("avans", "Avans"),
        ("predopl", "Oldindan to'lov"),
        ("opl", "To'lov / olib kelish"),
        ("labo", "Avto / labo"),
        ("nikel", "Qo'shimcha ish"),
        ("ber", "To'lov / berildi"),
        ("olindi", "Tushum"),
    ]
    for key, cat in rules:
        if key in t:
            return cat
    return "Boshqa"


def deduce_counterparty(text: str) -> str:
    raw = sanitize_text(text)
    low = raw.lower()
    patterns = [
        r"([A-Za-zА-Яа-яЁёʻ’'`\-]+\s+(?:aka|oka|ustoz|dost|do'st))",
        r"([A-Za-zА-Яа-яЁёʻ’'`\-]+)\s+(?:aka|oka)",
        r"\b(azam|orif|xusniddin|serik|saidmannapovich)\b",
    ]
    for p in patterns:
        m = re.search(p, raw, re.IGNORECASE)
        if m:
            return sanitize_text(m.group(1))[:80]
    # First 1-2 words before a keyword
    m = re.search(r"^([\wʻ’'`-]+(?:\s+[\wʻ’'`-]+){0,2})\s+(?:labo|xizmat|dostavka|avans|ber|olindi)", raw, re.IGNORECASE)
    if m:
        return sanitize_text(m.group(1))[:80]
    return ""


async def maybe_ai_enrich(note: str) -> Tuple[str, str, str]:
    """Return improved_note, category, counterparty. Fallback to heuristics."""
    base_note = ensure_note_quality(note, note)
    base_category = deduce_category(base_note)
    base_counterparty = deduce_counterparty(base_note)

    if not GROQ_API_KEY:
        return base_note, base_category, base_counterparty

    prompt = (
        "Sen moliyaviy yozuvlardan izoh, kategoriya va kontragentni ajratuvchi yordamchisan. "
        "Faqat JSON qaytar. JSON format: "
        '{"note":"...", "category":"...", "counterparty":"..."}. '
        "Izoh juda qisqa emas, aniq va ishbilarmon bo'lsin. Kategoriya 2-4 so'zdan oshmasin. "
        "Matn: " + base_note
    )

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": "Doim faqat yaroqli JSON qaytar."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        note = ensure_note_quality(obj.get("note") or base_note, base_note)
        category = sanitize_text(obj.get("category") or base_category)[:80] or base_category
        counterparty = sanitize_text(obj.get("counterparty") or base_counterparty)[:80]
        return note, category, counterparty
    except Exception as e:
        log.warning("AI enrich fallback: %s", e)
        return base_note, base_category, base_counterparty


# ---------------------------------------------------------
# Parsing
# ---------------------------------------------------------
EXPORT_LINE_RE = re.compile(
    r"^\[(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\]\s*(?P<author>[^:]+):\s*(?P<text>.+)$"
)

AMOUNT_RE = re.compile(
    r"(?P<sign>[+-]?)\s*"
    r"(?P<number>(?:\d{1,3}(?:[\s,]\d{3})+|\d+)(?:[.,]\d+)?)\s*"
    r"(?P<unit>mln|million|ming|k|usd|\$|sum|som|so'm|s[oʻ'’]m)?",
    re.IGNORECASE,
)


def parse_input_lines(raw_text: str) -> List[Dict[str, str | None]]:
    rows: List[Dict[str, str | None]] = []
    raw_text = (raw_text or "").strip()
    if not raw_text:
        return rows

    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = EXPORT_LINE_RE.match(line)
        if m:
            rows.append(
                {
                    "author": sanitize_text(m.group("author")),
                    "text": sanitize_text(m.group("text")),
                    "dt": sanitize_text(m.group("dt")),
                    "source_type": "telegram_export",
                    "source_line": line,
                }
            )
        else:
            rows.append(
                {
                    "author": "",
                    "text": sanitize_text(line),
                    "dt": None,
                    "source_type": "plain_text",
                    "source_line": line,
                }
            )
    return rows


def normalize_amount(number_str: str, unit: str | None, usd_rate: Decimal) -> Tuple[str, int, str]:
    raw = sanitize_text(number_str).replace(" ", "")
    # 12,750 -> 12750 ; 12.5 -> 12.5 ; 12,5 -> 12.5
    if "," in raw and "." not in raw:
        parts = raw.split(",")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]) and len(parts[0]) <= 3:
            raw = "".join(parts)
        else:
            raw = raw.replace(",", ".")
    raw = raw.replace(",", "")
    value = Decimal(raw)
    unit_norm = (unit or "").lower().strip()

    if unit_norm in {"mln", "million"}:
        uzs = int((value * Decimal("1000000")).quantize(Decimal("1")))
        return str(value), uzs, "UZS"
    if unit_norm in {"ming", "k"}:
        uzs = int((value * Decimal("1000")).quantize(Decimal("1")))
        return str(value), uzs, "UZS"
    if unit_norm in {"usd", "$"}:
        uzs = int((value * usd_rate).quantize(Decimal("1")))
        return str(value), uzs, "USD"
    # plain amount -> UZS
    uzs = int(value.quantize(Decimal("1")))
    return str(value), uzs, "UZS"


async def parse_transactions_from_text(
    text: str,
    usd_rate: Decimal,
    author: str = "",
    source_dt: Optional[str] = None,
    raw_line: str = "",
) -> List[ParsedItem]:
    text = sanitize_text(text)
    if not text:
        return []

    matches = [m for m in AMOUNT_RE.finditer(text) if m.group("number")]
    if not matches:
        return []

    tx_at = parse_dt_guess(source_dt).isoformat()
    tail_after_last = text[matches[-1].end():].strip(" -+.,;:")

    items: List[ParsedItem] = []
    for idx, m in enumerate(matches):
        sign = (m.group("sign") or "").strip()
        number = m.group("number")
        unit = m.group("unit")
        try:
            original, amount_uzs, currency = normalize_amount(number, unit, usd_rate)
        except Exception:
            continue

        tx_type = "income" if sign == "+" else "expense"
        segment_start = m.end()
        segment_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        note_segment = text[segment_start:segment_end].strip(" -+.,;:")
        if not note_segment:
            note_segment = tail_after_last
        note = ensure_note_quality(note_segment, text)
        note, category, counterparty = await maybe_ai_enrich(note)

        items.append(
            ParsedItem(
                tx_type=tx_type,
                amount_original=original,
                currency=currency,
                amount_uzs=amount_uzs,
                description=note,
                category=category,
                counterparty=counterparty,
                author=author,
                tx_at=tx_at,
                source_line=raw_line or text,
                raw_text=text,
                sign=sign or "-",
            )
        )

    # If all notes identical and there is a richer full text, keep it but not noisy.
    return items


# ---------------------------------------------------------
# Database
# ---------------------------------------------------------
CREATE_TRANSACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER,
    tx_type TEXT NOT NULL,
    amount_original TEXT NOT NULL DEFAULT '',
    currency TEXT NOT NULL DEFAULT 'UZS',
    amount_uzs INTEGER NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    counterparty TEXT NOT NULL DEFAULT '',
    author TEXT NOT NULL DEFAULT '',
    tx_at TEXT NOT NULL,
    source_line TEXT NOT NULL DEFAULT '',
    raw_text TEXT NOT NULL DEFAULT '',
    sign TEXT NOT NULL DEFAULT '-',
    created_at TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    deleted_at TEXT,
    deleted_reason TEXT NOT NULL DEFAULT ''
);
"""

CREATE_BATCHES_SQL = """
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    saved_at TEXT NOT NULL,
    summary_text TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    income_total_uzs INTEGER NOT NULL DEFAULT 0,
    expense_total_uzs INTEGER NOT NULL DEFAULT 0,
    net_total_uzs INTEGER NOT NULL DEFAULT 0,
    undone_at TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    deleted_reason TEXT NOT NULL DEFAULT ''
);
"""

CREATE_SETTINGS_SQL = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
"""


async def ensure_column(db: aiosqlite.Connection, table: str, column: str, decl: str) -> None:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = {r[1] for r in rows}
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TRANSACTIONS_SQL)
        await db.executescript(CREATE_BATCHES_SQL)
        await db.executescript(CREATE_SETTINGS_SQL)

        tx_columns = [
            ("batch_id", "INTEGER"),
            ("tx_type", "TEXT NOT NULL DEFAULT 'expense'"),
            ("amount_original", "TEXT NOT NULL DEFAULT ''"),
            ("currency", "TEXT NOT NULL DEFAULT 'UZS'"),
            ("amount_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("description", "TEXT NOT NULL DEFAULT ''"),
            ("category", "TEXT NOT NULL DEFAULT ''"),
            ("counterparty", "TEXT NOT NULL DEFAULT ''"),
            ("author", "TEXT NOT NULL DEFAULT ''"),
            ("tx_at", "TEXT NOT NULL DEFAULT ''"),
            ("source_line", "TEXT NOT NULL DEFAULT ''"),
            ("raw_text", "TEXT NOT NULL DEFAULT ''"),
            ("sign", "TEXT NOT NULL DEFAULT '-'"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
            ("deleted_at", "TEXT"),
            ("deleted_reason", "TEXT NOT NULL DEFAULT ''"),
        ]
        batch_columns = [
            ("source_text", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("saved_at", "TEXT NOT NULL DEFAULT ''"),
            ("summary_text", "TEXT NOT NULL DEFAULT ''"),
            ("item_count", "INTEGER NOT NULL DEFAULT 0"),
            ("income_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("expense_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("net_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("undone_at", "TEXT"),
            ("is_deleted", "INTEGER NOT NULL DEFAULT 0"),
            ("deleted_reason", "TEXT NOT NULL DEFAULT ''"),
        ]
        for col, decl in tx_columns:
            await ensure_column(db, "transactions", col, decl)
        for col, decl in batch_columns:
            await ensure_column(db, "batches", col, decl)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_transactions_tx_at ON transactions(tx_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_transactions_batch_id ON transactions(batch_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_transactions_deleted ON transactions(is_deleted)")

        await set_setting_db(db, "usd_rate", str(DEFAULT_USD_RATE), commit=False, only_if_missing=True)
        await set_setting_db(db, "live_reset_at", start_of_day().isoformat(), commit=False, only_if_missing=True)
        await db.commit()


async def set_setting_db(
    db: aiosqlite.Connection,
    key: str,
    value: str,
    *,
    commit: bool = False,
    only_if_missing: bool = False,
) -> None:
    if only_if_missing:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        if row is not None:
            return
    await db.execute(
        "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
        (key, value),
    )
    if commit:
        await db.commit()


async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row[0] if row else default


async def get_usd_rate() -> Decimal:
    raw = await get_setting("usd_rate", str(DEFAULT_USD_RATE))
    try:
        return Decimal(raw)
    except Exception:
        return DEFAULT_USD_RATE


async def set_usd_rate(value: Decimal, source: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await set_setting_db(db, "usd_rate", str(value), commit=False)
        await set_setting_db(db, "usd_rate_source", source, commit=False)
        await set_setting_db(db, "usd_rate_updated_at", now_local().isoformat(), commit=False)
        await db.commit()


async def get_live_reset_at() -> datetime:
    raw = await get_setting("live_reset_at", start_of_day().isoformat())
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ)
        return dt.astimezone(TZ)
    except Exception:
        return start_of_day()


async def reset_live_period() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await set_setting_db(db, "live_reset_at", now_local().isoformat(), commit=True)


async def save_pending_batch(pending: Dict[str, Any]) -> int:
    created_at = now_local().isoformat()
    items: List[ParsedItem] = pending["items"]
    income_total = sum(x.amount_uzs for x in items if x.tx_type == "income")
    expense_total = sum(x.amount_uzs for x in items if x.tx_type == "expense")
    net_total = income_total - expense_total
    summary_text = pending.get("summary_text") or ""
    source_text = pending.get("source_text") or ""

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO batches(
                source_text, created_at, saved_at, summary_text,
                item_count, income_total_uzs, expense_total_uzs, net_total_uzs,
                undone_at, is_deleted, deleted_reason
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, '')
            """,
            (
                source_text,
                created_at,
                created_at,
                summary_text,
                len(items),
                income_total,
                expense_total,
                net_total,
            ),
        )
        batch_id = cur.lastrowid

        for item in items:
            await db.execute(
                """
                INSERT INTO transactions(
                    batch_id, tx_type, amount_original, currency, amount_uzs,
                    description, category, counterparty, author, tx_at,
                    source_line, raw_text, sign, created_at, is_deleted,
                    deleted_at, deleted_reason
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, '')
                """,
                (
                    batch_id,
                    item.tx_type,
                    item.amount_original,
                    item.currency,
                    item.amount_uzs,
                    item.description,
                    item.category,
                    item.counterparty,
                    item.author,
                    item.tx_at,
                    item.source_line,
                    item.raw_text,
                    item.sign,
                    created_at,
                ),
            )
        await db.commit()
    return int(batch_id)


async def undo_last_batch() -> Optional[Tuple[int, int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM batches WHERE undone_at IS NULL AND is_deleted = 0 ORDER BY id DESC LIMIT 1"
        )
        row = await cur.fetchone()
        if not row:
            return None
        batch_id = int(row[0])
        undone_at = now_local().isoformat()
        await db.execute(
            "UPDATE batches SET undone_at = ?, is_deleted = 1, deleted_reason = 'undo' WHERE id = ?",
            (undone_at, batch_id),
        )
        await db.execute(
            "UPDATE transactions SET is_deleted = 1, deleted_at = ?, deleted_reason = 'undo' WHERE batch_id = ?",
            (undone_at, batch_id),
        )
        cur = await db.execute("SELECT COUNT(*) FROM transactions WHERE batch_id = ?", (batch_id,))
        cnt = int((await cur.fetchone())[0])
        await db.commit()
        return batch_id, cnt


async def get_recent_transactions(limit: int = 10) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT * FROM transactions
            WHERE is_deleted = 0
            ORDER BY datetime(tx_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def fetch_period_summary(start_dt: datetime, end_dt: Optional[datetime] = None) -> Dict[str, Any]:
    start_iso = start_dt.isoformat()
    end_iso = (end_dt or now_local()).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                COUNT(*) AS count_all,
                COALESCE(SUM(CASE WHEN tx_type='income' THEN amount_uzs ELSE 0 END), 0) AS income_total,
                COALESCE(SUM(CASE WHEN tx_type='expense' THEN amount_uzs ELSE 0 END), 0) AS expense_total
            FROM transactions
            WHERE is_deleted = 0 AND datetime(tx_at) >= datetime(?) AND datetime(tx_at) <= datetime(?)
            """,
            (start_iso, end_iso),
        )
        base = dict(await cur.fetchone())

        cur = await db.execute(
            """
            SELECT category, COUNT(*) AS cnt, COALESCE(SUM(amount_uzs), 0) AS total
            FROM transactions
            WHERE is_deleted = 0 AND tx_type='expense'
              AND datetime(tx_at) >= datetime(?) AND datetime(tx_at) <= datetime(?)
            GROUP BY category
            ORDER BY total DESC, cnt DESC
            LIMIT 5
            """,
            (start_iso, end_iso),
        )
        expense_categories = [dict(r) for r in await cur.fetchall()]

        cur = await db.execute(
            """
            SELECT category, COUNT(*) AS cnt, COALESCE(SUM(amount_uzs), 0) AS total
            FROM transactions
            WHERE is_deleted = 0 AND tx_type='income'
              AND datetime(tx_at) >= datetime(?) AND datetime(tx_at) <= datetime(?)
            GROUP BY category
            ORDER BY total DESC, cnt DESC
            LIMIT 5
            """,
            (start_iso, end_iso),
        )
        income_categories = [dict(r) for r in await cur.fetchall()]

        cur = await db.execute(
            """
            SELECT * FROM transactions
            WHERE is_deleted = 0 AND datetime(tx_at) >= datetime(?) AND datetime(tx_at) <= datetime(?)
            ORDER BY datetime(tx_at) DESC, id DESC
            LIMIT 10
            """,
            (start_iso, end_iso),
        )
        recent = [dict(r) for r in await cur.fetchall()]

    income_total = int(base.get("income_total") or 0)
    expense_total = int(base.get("expense_total") or 0)
    return {
        "count": int(base.get("count_all") or 0),
        "income_total": income_total,
        "expense_total": expense_total,
        "net_total": income_total - expense_total,
        "expense_categories": expense_categories,
        "income_categories": income_categories,
        "recent": recent,
        "start_dt": start_dt,
        "end_dt": end_dt or now_local(),
    }


async def export_rows(start_dt: Optional[datetime] = None, end_dt: Optional[datetime] = None) -> List[Dict[str, Any]]:
    where = ["is_deleted = 0"]
    params: List[Any] = []
    if start_dt:
        where.append("datetime(tx_at) >= datetime(?)")
        params.append(start_dt.isoformat())
    if end_dt:
        where.append("datetime(tx_at) <= datetime(?)")
        params.append(end_dt.isoformat())

    sql = f"SELECT * FROM transactions WHERE {' AND '.join(where)} ORDER BY datetime(tx_at) ASC, id ASC"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, tuple(params))
        return [dict(r) for r in await cur.fetchall()]


# ---------------------------------------------------------
# Keyboards
# ---------------------------------------------------------
def main_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(KeyboardButton("📊 Bugun"), KeyboardButton("📅 Haftalik"), KeyboardButton("🗓 Oylik"))
    kb.row(KeyboardButton("💱 Kursni belgilash"), KeyboardButton("📤 Export"))
    kb.row(KeyboardButton("📝 Text hisobot"), KeyboardButton("↩️ Oxirgi amalni bekor qilish"))
    kb.row(KeyboardButton("🔄 Yangilash"), KeyboardButton("📚 Arxiv"))
    return kb


def pending_keyboard(token: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Saqlash", callback_data=f"save:{token}"),
        InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel:{token}"),
    )
    return kb


def confirm_reset_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Ha, yangilash", callback_data="reset:confirm"),
        InlineKeyboardButton("❌ Yo'q", callback_data="reset:cancel"),
    )
    return kb


def rate_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🌐 API orqali", callback_data="rate:api_fetch"),
        InlineKeyboardButton("⌨️ Qo'lda kiritish", callback_data="rate:manual_start"),
    )
    kb.add(InlineKeyboardButton("❌ Bekor", callback_data="rate:cancel"))
    return kb


def rate_confirm_keyboard(value: str, source: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Saqlash", callback_data=f"rate:save:{value}:{source}"),
        InlineKeyboardButton("❌ Bekor", callback_data="rate:cancel"),
    )
    return kb


# ---------------------------------------------------------
# Auth / utility
# ---------------------------------------------------------
async def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


def build_preview_text(items: List[ParsedItem], usd_rate: Decimal) -> str:
    lines = [
        "<b>Tekshiruv preview</b>",
        f"USD kursi: <b>{money_fmt(usd_rate)}</b>",
        "",
    ]
    income = 0
    expense = 0
    for i, item in enumerate(items, 1):
        emoji = "🟢" if item.tx_type == "income" else "🔴"
        income += item.amount_uzs if item.tx_type == "income" else 0
        expense += item.amount_uzs if item.tx_type == "expense" else 0
        lines.extend([
            f"{emoji} <b>{i}. {'Kirim' if item.tx_type == 'income' else 'Chiqim'}</b>",
            f"   Summa: <b>{money_fmt(item.amount_uzs)}</b> ({item.amount_original} {item.currency})",
            f"   Izoh: {item.description}",
            f"   Kategoriya: {item.category}",
            f"   Kontragent: {item.counterparty or '-'}",
            f"   Sana: {parse_iso_to_local_text(item.tx_at)}",
            "",
        ])
    lines.append(f"🟢 Jami kirim: <b>{money_fmt(income)}</b>")
    lines.append(f"🔴 Jami chiqim: <b>{money_fmt(expense)}</b>")
    lines.append(f"⚖️ Qoldiq: <b>{money_fmt(income - expense)}</b>")
    lines.append("")
    lines.append("Saqlansinmi?")
    return "\n".join(lines)


def build_report_text(title: str, data: Dict[str, Any]) -> str:
    lines = [
        f"<b>{title}</b>",
        f"Davr: {fmt_dt(data['start_dt'])} — {fmt_dt(data['end_dt'])}",
        f"Tranzaksiya soni: <b>{data['count']}</b>",
        f"🟢 Kirim: <b>{money_fmt(data['income_total'])}</b>",
        f"🔴 Chiqim: <b>{money_fmt(data['expense_total'])}</b>",
        f"⚖️ Qoldiq: <b>{money_fmt(data['net_total'])}</b>",
        "",
    ]

    if data["income_categories"]:
        lines.append("<b>Top kirim kategoriyalar:</b>")
        for row in data["income_categories"]:
            lines.append(f"• {row['category'] or 'Boshqa'} — {money_fmt(row['total'])} ({row['cnt']} ta)")
        lines.append("")

    if data["expense_categories"]:
        lines.append("<b>Top chiqim kategoriyalar:</b>")
        for row in data["expense_categories"]:
            lines.append(f"• {row['category'] or 'Boshqa'} — {money_fmt(row['total'])} ({row['cnt']} ta)")
        lines.append("")

    if data["recent"]:
        lines.append("<b>Oxirgi yozuvlar:</b>")
        for row in data["recent"][:8]:
            emo = "🟢" if row["tx_type"] == "income" else "🔴"
            lines.append(
                f"{emo} {parse_iso_to_local_text(row['tx_at'])} | {money_fmt(row['amount_uzs'])} | {row['description']}"
            )
    return "\n".join(lines)


async def create_export_files(rows: List[Dict[str, Any]]) -> Tuple[io.BytesIO, io.BytesIO, io.BytesIO]:
    # XLSX
    wb = Workbook()
    ws = wb.active
    ws.title = "Barcha"
    headers = [
        "ID", "Batch ID", "Sana", "Turi", "Original summa", "Valyuta", "UZS summa",
        "Izoh", "Kategoriya", "Kontragent", "Muallif", "Belgi", "Asl satr"
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")
    income_total = 0
    expense_total = 0
    for row in rows:
        if row["tx_type"] == "income":
            income_total += int(row["amount_uzs"])
        else:
            expense_total += int(row["amount_uzs"])
        ws.append([
            row["id"], row["batch_id"], parse_iso_to_local_text(row["tx_at"]),
            "Kirim" if row["tx_type"] == "income" else "Chiqim",
            row["amount_original"], row["currency"], row["amount_uzs"],
            row["description"], row["category"], row["counterparty"], row["author"],
            row["sign"], row["source_line"],
        ])
    for col in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max(max_len + 2, 12), 42)

    dash = wb.create_sheet("Dashboard", 0)
    dash["A1"] = "Ko'rsatkich"
    dash["B1"] = "Qiymat"
    dash["A1"].font = dash["B1"].font = Font(bold=True)
    dash.append(["Jami yozuv", len(rows)])
    dash.append(["Jami kirim", income_total])
    dash.append(["Jami chiqim", expense_total])
    dash.append(["Qoldiq", income_total - expense_total])
    dash.column_dimensions["A"].width = 24
    dash.column_dimensions["B"].width = 20

    xlsx_buf = io.BytesIO()
    wb.save(xlsx_buf)
    xlsx_buf.seek(0)

    # CSV utf-8-sig for Excel
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([
            row["id"], row["batch_id"], parse_iso_to_local_text(row["tx_at"]),
            "Kirim" if row["tx_type"] == "income" else "Chiqim",
            row["amount_original"], row["currency"], row["amount_uzs"],
            row["description"], row["category"], row["counterparty"], row["author"],
            row["sign"], row["source_line"],
        ])
    csv_bytes = io.BytesIO(csv_buf.getvalue().encode("utf-8-sig"))
    csv_bytes.seek(0)

    # TXT
    txt_lines = ["Moliya hisobot export", "=" * 40, ""]
    for row in rows:
        txt_lines.extend([
            f"ID: {row['id']}",
            f"Sana: {parse_iso_to_local_text(row['tx_at'])}",
            f"Turi: {'Kirim' if row['tx_type'] == 'income' else 'Chiqim'}",
            f"Summa: {money_fmt(row['amount_uzs'])} ({row['amount_original']} {row['currency']})",
            f"Izoh: {row['description']}",
            f"Kategoriya: {row['category']}",
            f"Kontragent: {row['counterparty'] or '-'}",
            f"Muallif: {row['author'] or '-'}",
            f"Asl satr: {row['source_line']}",
            "-" * 40,
        ])
    txt_bytes = io.BytesIO("\n".join(txt_lines).encode("utf-8"))
    txt_bytes.seek(0)
    return xlsx_buf, csv_bytes, txt_bytes


async def fetch_cbu_usd_rate() -> Dict[str, str]:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(CBU_USD_API) as resp:
            resp.raise_for_status()
            data = await resp.json()
    if not isinstance(data, list) or not data:
        raise ValueError("API bo'sh javob qaytardi")
    row = data[0]
    rate = str(row.get("Rate", "")).replace(",", ".").strip()
    if not rate:
        raise ValueError("Rate topilmadi")
    return {
        "rate": rate,
        "date": str(row.get("Date", "")).strip(),
        "ccy": str(row.get("Ccy", "USD")).strip(),
        "name": str(row.get("CcyNm_UZ", "AQSH dollari")).strip(),
    }


# ---------------------------------------------------------
# Handlers
# ---------------------------------------------------------
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message) -> None:
    text = (
        "<b>Moliya bot tayyor.</b>\n\n"
        "Matn yozing yoki Telegram export satrini yuboring.\n"
        "Misollar:\n"
        "• <code>+250$+517 ming azam aka labo berari olindi</code>\n"
        "• <code>100 ming temir dostavkaga berdim</code>\n"
        "• <code>[27.03.2026 12:01] Ali: +250$ + 300 ming olindi</code>\n\n"
        "Qoidalar:\n"
        "• <b>+</b> = kirim\n"
        "• <b>-</b> yoki belgisiz = chiqim\n"
        "• <b>mln</b> = million\n"
        "• <b>ming</b> / <b>k</b> = ming\n"
        "• <b>$</b> / <b>usd</b> = dollar kurs bo'yicha so'mga o'tadi"
    )
    await message.answer(text, reply_markup=main_keyboard())


@dp.message_handler(commands=["rate"])
async def cmd_rate(message: types.Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    args = (message.get_args() or "").strip()
    if not args:
        rate = await get_usd_rate()
        source = await get_setting("usd_rate_source", "default")
        updated_at = await get_setting("usd_rate_updated_at", "-")
        await message.answer(
            f"💱 Joriy USD kursi: <b>{money_fmt(rate)}</b>\nManba: {source}\nYangilangan: {parse_iso_to_local_text(updated_at)}",
            reply_markup=main_keyboard(),
        )
        return
    try:
        cleaned = args.replace(" ", "")
        if "," in cleaned and "." not in cleaned:
            cleaned = cleaned.replace(",", "")
        value = Decimal(cleaned)
        if value <= 0:
            raise ValueError
    except Exception:
        await message.answer("❌ Noto'g'ri format. Misol: <code>/rate 12750</code>")
        return
    await set_usd_rate(value, "manual_command")
    await message.answer(f"✅ USD kurs saqlandi: <b>{money_fmt(value)}</b>", reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "💱 Kursni belgilash")
async def rate_menu_handler(message: types.Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    rate = await get_usd_rate()
    source = await get_setting("usd_rate_source", "default")
    updated = await get_setting("usd_rate_updated_at", "-")
    text = (
        f"<b>USD kursini belgilash</b>\n"
        f"Joriy kurs: <b>{money_fmt(rate)}</b>\n"
        f"Manba: {source}\n"
        f"Yangilangan: {parse_iso_to_local_text(updated)}\n\n"
        f"Tanlang:\n"
        f"• API orqali avtomatik olish\n"
        f"• Qo'lda kiritish\n\n"
        f"Qo'lda kiritish formatlari:\n"
        f"<code>12750</code> yoki <code>12,750</code>"
    )
    await message.answer(text, reply_markup=rate_menu_keyboard())


@dp.callback_query_handler(lambda c: c.data == "rate:api_fetch")
async def cb_rate_api_fetch(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer("Ruxsat yo'q", show_alert=True)
        return
    try:
        info = await fetch_cbu_usd_rate()
        await call.message.edit_text(
            f"🌐 API kursi topildi\n"
            f"Valyuta: {info['name']} ({info['ccy']})\n"
            f"Kurs: <b>{info['rate']}</b>\n"
            f"Sana: {info['date']}\n\n"
            f"Shu kursni saqlaysizmi?",
            reply_markup=rate_confirm_keyboard(info["rate"], "api"),
        )
        await call.answer()
    except Exception as e:
        await call.answer("API xato", show_alert=True)
        await call.message.answer(f"❌ API orqali kurs olinmadi: {e}")


@dp.callback_query_handler(lambda c: c.data == "rate:manual_start")
async def cb_rate_manual_start(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer("Ruxsat yo'q", show_alert=True)
        return
    await RateStates.waiting_manual_rate.set()
    await call.answer()
    await call.message.answer(
        "⌨️ Yangi USD kursini yuboring.\n\n"
        "To'g'ri formatlar:\n"
        "• <code>12750</code>\n"
        "• <code>12,750</code>\n"
        "• <code>12750.50</code>"
    )


@dp.message_handler(state=RateStates.waiting_manual_rate, content_types=types.ContentType.TEXT)
async def state_rate_manual(message: types.Message, state: FSMContext) -> None:
    if not await is_admin(message.from_user.id):
        await state.finish()
        return
    raw = (message.text or "").replace(" ", "").strip()
    try:
        if "," in raw and "." not in raw:
            raw = raw.replace(",", "")
        value = Decimal(raw)
        if value <= 0:
            raise InvalidOperation
    except Exception:
        await message.answer(
            "❌ Noto'g'ri format.\nMisollar:\n<code>12750</code>\n<code>12,750</code>\n<code>12750.50</code>"
        )
        return
    await state.finish()
    await message.answer(
        f"💱 Kiritilgan kurs: <b>{money_fmt(value)}</b>\n\nSaqlaysizmi?",
        reply_markup=rate_confirm_keyboard(str(value), "manual"),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("rate:save:"))
async def cb_rate_save(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer("Ruxsat yo'q", show_alert=True)
        return
    try:
        _, _, value, source = call.data.split(":", 3)
        amount = Decimal(value)
        await set_usd_rate(amount, source)
        await call.message.edit_text(f"✅ USD kurs saqlandi: <b>{money_fmt(amount)}</b>\nManba: {source}")
        await call.answer("Saqlandi")
    except Exception as e:
        await call.answer("Saqlashda xato", show_alert=True)
        await call.message.answer(f"❌ Xato: {e}")


@dp.callback_query_handler(lambda c: c.data == "rate:cancel", state="*")
async def cb_rate_cancel(call: CallbackQuery, state: FSMContext) -> None:
    await state.finish()
    with suppress(Exception):
        await call.message.edit_reply_markup(reply_markup=None)
    await call.answer("Bekor qilindi")


@dp.message_handler(lambda m: m.text == "🔄 Yangilash")
async def ask_reset_live(message: types.Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    await message.answer(
        "Bugungi live hisob 0 dan boshlansinmi?\nEski arxiv o'chmaydi va bazada saqlanadi.",
        reply_markup=confirm_reset_keyboard(),
    )


@dp.callback_query_handler(lambda c: c.data == "reset:confirm")
async def cb_reset_confirm(call: CallbackQuery) -> None:
    if not await is_admin(call.from_user.id):
        await call.answer("Ruxsat yo'q", show_alert=True)
        return
    await reset_live_period()
    await call.answer("Yangilandi")
    await call.message.edit_text(
        f"✅ Live hisob yangilandi.\nYangi start: <b>{fmt_dt(now_local())}</b>\nEski yozuvlar arxivda saqlanadi."
    )


@dp.callback_query_handler(lambda c: c.data == "reset:cancel")
async def cb_reset_cancel(call: CallbackQuery) -> None:
    await call.answer("Bekor qilindi")
    with suppress(Exception):
        await call.message.edit_text("❌ Yangilash bekor qilindi.")


@dp.message_handler(lambda m: m.text == "↩️ Oxirgi amalni bekor qilish")
async def btn_undo(message: types.Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    res = await undo_last_batch()
    if not res:
        await message.answer("Oxirgi amal topilmadi.")
        return
    batch_id, count = res
    await message.answer(f"✅ Bekor qilindi. Batch ID: <b>{batch_id}</b>, yozuvlar: <b>{count}</b>")


@dp.message_handler(commands=["undo"])
async def cmd_undo(message: types.Message) -> None:
    await btn_undo(message)


@dp.message_handler(lambda m: m.text == "📊 Bugun")
async def btn_today(message: types.Message) -> None:
    reset_at = await get_live_reset_at()
    start_dt = max(start_of_day(), reset_at)
    data = await fetch_period_summary(start_dt)
    title = "📊 Bugungi live hisobot"
    await message.answer(build_report_text(title, data), reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "📅 Haftalik")
async def btn_week(message: types.Message) -> None:
    data = await fetch_period_summary(start_of_week())
    await message.answer(build_report_text("📅 Haftalik hisobot", data), reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "🗓 Oylik")
async def btn_month(message: types.Message) -> None:
    data = await fetch_period_summary(start_of_month())
    await message.answer(build_report_text("🗓 Oylik hisobot", data), reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "📝 Text hisobot")
async def btn_text_report(message: types.Message) -> None:
    today_live = await fetch_period_summary(max(start_of_day(), await get_live_reset_at()))
    week = await fetch_period_summary(start_of_week())
    month = await fetch_period_summary(start_of_month())
    text = "\n\n".join([
        build_report_text("📊 Bugun", today_live),
        build_report_text("📅 Haftalik", week),
        build_report_text("🗓 Oylik", month),
    ])
    for chunk in [text[i:i+3500] for i in range(0, len(text), 3500)]:
        await message.answer(chunk, reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "📚 Arxiv")
async def btn_archive(message: types.Message) -> None:
    rows = await get_recent_transactions(15)
    if not rows:
        await message.answer("Arxiv bo'sh.")
        return
    lines = ["<b>Oxirgi arxiv yozuvlari</b>"]
    for row in rows:
        emo = "🟢" if row["tx_type"] == "income" else "🔴"
        lines.append(
            f"{emo} ID {row['id']} | {parse_iso_to_local_text(row['tx_at'])} | {money_fmt(row['amount_uzs'])} | {row['description']}"
        )
    await message.answer("\n".join(lines), reply_markup=main_keyboard())


@dp.message_handler(lambda m: m.text == "📤 Export")
async def btn_export(message: types.Message) -> None:
    rows = await export_rows()
    if not rows:
        await message.answer("Export uchun yozuvlar yo'q.")
        return
    xlsx_buf, csv_buf, txt_buf = await create_export_files(rows)
    stamp = now_local().strftime("%Y%m%d_%H%M")
    await message.answer("Fayllar tayyor. Excel ochilmasa CSV yoki TXT dan foydalaning.")
    await bot.send_document(message.chat.id, InputFile(xlsx_buf, filename=f"hisobot_{stamp}.xlsx"))
    await bot.send_document(message.chat.id, InputFile(csv_buf, filename=f"hisobot_{stamp}.csv"))
    await bot.send_document(message.chat.id, InputFile(txt_buf, filename=f"hisobot_{stamp}.txt"))


@dp.message_handler(commands=["records"])
async def cmd_records(message: types.Message) -> None:
    args = (message.get_args() or "10").strip()
    try:
        limit = max(1, min(50, int(args)))
    except Exception:
        limit = 10
    rows = await get_recent_transactions(limit)
    if not rows:
        await message.answer("Yozuvlar topilmadi.")
        return
    lines = [f"<b>Oxirgi {len(rows)} yozuv</b>"]
    for row in rows:
        emo = "🟢" if row["tx_type"] == "income" else "🔴"
        lines.append(
            f"{emo} ID {row['id']} | {parse_iso_to_local_text(row['tx_at'])} | {money_fmt(row['amount_uzs'])} | {row['description']}"
        )
    await message.answer("\n".join(lines))


@dp.message_handler(commands=["delete"])
async def cmd_delete(message: types.Message) -> None:
    if not await is_admin(message.from_user.id):
        return
    arg = (message.get_args() or "").strip()
    if not arg.isdigit():
        await message.answer("Misol: <code>/delete 15</code>")
        return
    tx_id = int(arg)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM transactions WHERE id = ? AND is_deleted = 0", (tx_id,))
        row = await cur.fetchone()
        if not row:
            await message.answer("Topilmadi yoki allaqachon o'chirilgan.")
            return
        ts = now_local().isoformat()
        await db.execute(
            "UPDATE transactions SET is_deleted = 1, deleted_at = ?, deleted_reason = 'manual_delete' WHERE id = ?",
            (ts, tx_id),
        )
        await db.commit()
    await message.answer(f"✅ O'chirildi: ID <b>{tx_id}</b>")


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message) -> None:
    args = (message.get_args() or "today").strip().lower()
    if args in {"today", "bugun"}:
        await btn_today(message)
    elif args in {"week", "hafta", "weekly"}:
        await btn_week(message)
    elif args in {"month", "oy", "monthly"}:
        await btn_month(message)
    else:
        await message.answer("Variantlar: <code>/stats today</code>, <code>/stats week</code>, <code>/stats month</code>")


@dp.message_handler(content_types=types.ContentType.TEXT)
async def parse_any_text(message: types.Message) -> None:
    text = strip_command_prefix(message.text or "")
    if not text:
        return

    ignored_buttons = {
        "📊 Bugun", "📅 Haftalik", "🗓 Oylik", "💱 Kursni belgilash",
        "📤 Export", "📝 Text hisobot", "↩️ Oxirgi amalni bekor qilish",
        "🔄 Yangilash", "📚 Arxiv"
    }
    if text in ignored_buttons:
        return

    usd_rate = await get_usd_rate()
    rows = parse_input_lines(text)
    items: List[ParsedItem] = []
    for row in rows:
        items.extend(
            await parse_transactions_from_text(
                row["text"] or "",
                usd_rate=usd_rate,
                author=row["author"] or sanitize_text(message.from_user.full_name or ""),
                source_dt=row["dt"],
                raw_line=row["source_line"] or row["text"] or "",
            )
        )

    if not items:
        await message.answer(
            "Summalar topilmadi.\n\n"
            "Misollar:\n"
            "<code>+250$+517 ming azam aka labo berari olindi</code>\n"
            "<code>100 ming temir dostavkaga berdim</code>\n"
            "<code>[27.03.2026 12:01] Ali: +250$ + 300 ming olindi</code>",
            reply_markup=main_keyboard(),
        )
        return

    token = secrets.token_hex(8)
    preview = build_preview_text(items, usd_rate)
    PENDING_BATCHES[token] = {
        "items": items,
        "source_text": text,
        "summary_text": preview,
        "user_id": message.from_user.id,
        "created_at": now_local().isoformat(),
    }
    await message.answer(preview, reply_markup=pending_keyboard(token))


@dp.callback_query_handler(lambda c: c.data.startswith("save:"))
async def callback_save(call: CallbackQuery) -> None:
    token = call.data.split(":", 1)[1]
    pending = PENDING_BATCHES.get(token)
    if not pending:
        await call.answer("Session topilmadi yoki eskirdi", show_alert=True)
        return
    if pending["user_id"] != call.from_user.id and not await is_admin(call.from_user.id):
        await call.answer("Bu preview sizga tegishli emas", show_alert=True)
        return
    batch_id = await save_pending_batch(pending)
    items: List[ParsedItem] = pending["items"]
    income_total = sum(x.amount_uzs for x in items if x.tx_type == "income")
    expense_total = sum(x.amount_uzs for x in items if x.tx_type == "expense")
    text = (
        f"✅ Saqlandi. Batch ID: <b>{batch_id}</b>\n"
        f"🟢 Kirim: <b>{money_fmt(income_total)}</b>\n"
        f"🔴 Chiqim: <b>{money_fmt(expense_total)}</b>\n"
        f"⚖️ Qoldiq: <b>{money_fmt(income_total - expense_total)}</b>"
    )
    PENDING_BATCHES.pop(token, None)
    with suppress(Exception):
        await call.message.edit_text(text)
    await call.answer("Saqlandi")


@dp.callback_query_handler(lambda c: c.data.startswith("cancel:"))
async def callback_cancel(call: CallbackQuery) -> None:
    token = call.data.split(":", 1)[1]
    PENDING_BATCHES.pop(token, None)
    with suppress(Exception):
        await call.message.edit_text("❌ Saqlash bekor qilindi.")
    await call.answer("Bekor qilindi")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
async def on_startup(_: Dispatcher) -> None:
    await init_db()
    log.info("Bot ishga tushdi")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN topilmadi")
    await on_startup(dp)
    await dp.start_polling()


if __name__ == "__main__":
    asyncio.run(main())
