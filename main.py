import asyncio
import csv
import io
import json
import logging
import os
import re
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl
import hashlib
import hmac

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "finance_bot.db").strip() or "finance_bot.db"
WEB_APP_URL = os.getenv("WEB_APP_URL", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"
DEFAULT_USD_RATE = Decimal(os.getenv("DEFAULT_USD_RATE", "12750").strip() or "12750")
TIMEZONE_NAME = os.getenv("BOT_TIMEZONE", "Asia/Tashkent").strip() or "Asia/Tashkent"
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN topilmadi")

if ZoneInfo is not None:
    try:
        TZ = ZoneInfo(TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        TZ = timezone(timedelta(hours=5))
else:
    TZ = timezone(timedelta(hours=5))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("finance-bot")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

CBU_USD_URL = "https://cbu.uz/uz/arkhiv-kursov-valyut/json/USD/"
EXPORT_LINE_RE = re.compile(
    r"^\[(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\]\s*(?P<author>[^:]+):\s*(?P<text>.+)$"
)

AMOUNT_RE = re.compile(
    r"(?P<sign>[+-]?)\s*(?P<number>\d+(?:[\s,]\d{3})*(?:[.,]\d+)?|\d+(?:[.,]\d+)?)\s*(?P<unit>mln|million|ming|k|usd|\$|sum|som|so['’`]?m)?",
    re.IGNORECASE,
)

STOPWORDS = {
    "aka", "opa", "uchun", "ga", "dan", "va", "bilan", "berdim", "oldim", "olindi",
    "predoplata", "dokumentiga", "labo", "labosiga", "xizmat", "sum", "so'm", "som", "usd"
}


class RateStates(StatesGroup):
    waiting_manual_rate = State()


class DeleteStates(StatesGroup):
    waiting_record_id = State()


@dataclass
class ParsedRecord:
    tx_type: str
    amount_uzs: int
    currency: str
    amount_original: str
    usd_rate_used: str
    description: str
    author_name: str
    tx_at: str
    source_text: str
    source_kind: str
    meta_json: str = "{}"
    category: str = "boshqa"
    counterparty: str = ""


@dataclass
class PendingBatch:
    owner_id: int
    source_text: str
    created_at: str
    summary_text: str
    items: List[ParsedRecord] = field(default_factory=list)


def now_tz() -> datetime:
    return datetime.now(TZ)


def now_iso() -> str:
    return now_tz().strftime("%Y-%m-%d %H:%M:%S")


def display_dt(value: str) -> str:
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return value


def money_fmt_uzs(value: Any) -> str:
    try:
        dec = Decimal(str(value))
    except Exception:
        dec = Decimal("0")
    q = dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"{int(q):,}".replace(",", " ") + " so‘m"


def money_fmt_decimal(value: Any) -> str:
    try:
        dec = Decimal(str(value))
    except Exception:
        dec = Decimal("0")
    s = format(dec.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def parse_export_or_plain(raw_text: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for line in (raw_text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        m = EXPORT_LINE_RE.match(line)
        if m:
            rows.append({
                "author": m.group("author").strip(),
                "text": m.group("text").strip(),
                "dt": m.group("dt").strip(),
                "kind": "telegram_export",
                "raw_line": line,
            })
        else:
            rows.append({
                "author": "",
                "text": line,
                "dt": "",
                "kind": "plain_text",
                "raw_line": line,
            })
    return rows


def clean_number(num: str) -> Decimal:
    s = num.strip().replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            s = "".join(parts)
        else:
            s = s.replace(",", ".")
    return Decimal(s)


def detect_category_and_counterparty(description: str) -> Tuple[str, str]:
    text = description.lower()
    category = "boshqa"
    if any(k in text for k in ["dostav", "yetkaz", "transport"]):
        category = "dostavka"
    elif any(k in text for k in ["dokument", "hujjat"]):
        category = "dokument"
    elif any(k in text for k in ["avans", "predoplata", "oldindan"]):
        category = "avans"
    elif any(k in text for k in ["metan", "resor", "nikel", "temir", "labo", "xizmat"]):
        category = "xizmat"
    elif any(k in text for k in ["olindi", "kelib tushdi", "keldi"]):
        category = "kirim"
    elif any(k in text for k in ["berdim", "to'ladim", "to‘ladim", "chiqdi"]):
        category = "chiqim"

    tokens = [t for t in re.findall(r"[\w‘’'-]+", description, flags=re.UNICODE) if t]
    counterparty = ""
    candidates = []
    for i, tok in enumerate(tokens):
        low = tok.lower()
        if low in STOPWORDS or low.isdigit() or len(low) < 3:
            continue
        candidates.append(tok)
        if len(candidates) >= 2:
            break
    if candidates:
        counterparty = " ".join(candidates)
    return category, counterparty


async def maybe_ai_enrich(description: str) -> Tuple[str, str, str]:
    if not GROQ_API_KEY or not description.strip():
        category, cp = detect_category_and_counterparty(description)
        return description.strip(), category, cp

    payload = {
        "model": GROQ_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Sen moliyaviy yozuvlarni tozalovchi yordamchisan. "
                    "Qisqa, aniq JSON qaytar: {\"description\": str, \"category\": str, \"counterparty\": str}."
                ),
            },
            {
                "role": "user",
                "content": description[:1200],
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=18)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
            ) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"Groq {resp.status}")
                data = await resp.json()
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        description2 = str(parsed.get("description") or description).strip()
        category = str(parsed.get("category") or "boshqa").strip().lower()
        counterparty = str(parsed.get("counterparty") or "").strip()
        return description2, category or "boshqa", counterparty
    except Exception:
        category, cp = detect_category_and_counterparty(description)
        return description.strip(), category, cp


async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS if ADMIN_IDS else True


async def ensure_column(db: aiosqlite.Connection, table: str, column: str, decl: str) -> None:
    cur = await db.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in await cur.fetchall()}
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                source_text TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                saved_at TEXT NOT NULL DEFAULT '',
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
                owner_id INTEGER NOT NULL,
                tx_type TEXT NOT NULL,
                amount_uzs INTEGER NOT NULL,
                currency TEXT NOT NULL,
                amount_original TEXT NOT NULL,
                usd_rate_used TEXT NOT NULL,
                description TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'boshqa',
                counterparty TEXT NOT NULL DEFAULT '',
                author_name TEXT NOT NULL DEFAULT '',
                tx_at TEXT NOT NULL,
                source_text TEXT NOT NULL DEFAULT '',
                source_kind TEXT NOT NULL DEFAULT 'plain_text',
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT '',
                deleted_at TEXT,
                FOREIGN KEY(batch_id) REFERENCES batches(id)
            );
            """
        )
        for column, decl in [
            ("source_text", "TEXT NOT NULL DEFAULT ''"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("saved_at", "TEXT NOT NULL DEFAULT ''"),
            ("summary_text", "TEXT NOT NULL DEFAULT ''"),
            ("item_count", "INTEGER NOT NULL DEFAULT 0"),
            ("income_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("expense_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("net_total_uzs", "INTEGER NOT NULL DEFAULT 0"),
            ("undone_at", "TEXT"),
        ]:
            await ensure_column(db, "batches", column, decl)

        for column, decl in [
            ("category", "TEXT NOT NULL DEFAULT 'boshqa'"),
            ("counterparty", "TEXT NOT NULL DEFAULT ''"),
            ("author_name", "TEXT NOT NULL DEFAULT ''"),
            ("source_text", "TEXT NOT NULL DEFAULT ''"),
            ("source_kind", "TEXT NOT NULL DEFAULT 'plain_text'"),
            ("meta_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("created_at", "TEXT NOT NULL DEFAULT ''"),
            ("deleted_at", "TEXT"),
        ]:
            await ensure_column(db, "transactions", column, decl)

        await db.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES('usd_rate', ?)",
            (str(DEFAULT_USD_RATE),),
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES('current_period_start', ?)",
            (now_iso(),),
        )
        await db.commit()


async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
    return row[0] if row else default


async def set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            (key, value),
        )
        await db.commit()


async def get_usd_rate() -> Decimal:
    raw = await get_setting("usd_rate", str(DEFAULT_USD_RATE))
    try:
        return Decimal(raw)
    except Exception:
        return DEFAULT_USD_RATE


async def fetch_cbu_usd_rate() -> Tuple[Decimal, str]:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(CBU_USD_URL) as resp:
            resp.raise_for_status()
            data = await resp.json()
    if not isinstance(data, list) or not data:
        raise RuntimeError("CBU API bo‘sh javob qaytardi")
    item = data[0]
    rate = Decimal(str(item.get("Rate", "0")).replace(",", "."))
    dt = str(item.get("Date", "")).strip()
    return rate, dt


async def get_current_period_start() -> str:
    return await get_setting("current_period_start", now_iso())


async def reset_current_period() -> str:
    ts = now_iso()
    await set_setting("current_period_start", ts)
    return ts


async def count_active_transactions() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM transactions WHERE deleted_at IS NULL")
        row = await cur.fetchone()
        return int(row[0] or 0)


async def parse_text_to_records(raw_text: str, default_author: str) -> List[ParsedRecord]:
    rows = parse_export_or_plain(raw_text)
    if not rows:
        return []
    usd_rate = await get_usd_rate()
    records: List[ParsedRecord] = []
    for row in rows:
        text = row["text"]
        matches = list(AMOUNT_RE.finditer(text))
        matches = [m for m in matches if m.group("number")]
        if not matches:
            continue
        for idx, m in enumerate(matches):
            sign = (m.group("sign") or "").strip()
            try:
                number = clean_number(m.group("number"))
            except Exception:
                continue
            unit = (m.group("unit") or "").lower().strip()
            currency = "UZS"
            amount_uzs = Decimal("0")
            amount_original = number
            if unit in {"mln", "million"}:
                amount_uzs = number * Decimal("1000000")
            elif unit in {"ming", "k"}:
                amount_uzs = number * Decimal("1000")
            elif unit in {"usd", "$"}:
                currency = "USD"
                amount_uzs = number * usd_rate
            else:
                amount_uzs = number

            tx_type = "income" if sign == "+" else "expense"
            seg_start = m.end()
            seg_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            desc = text[seg_start:seg_end].strip(" -+.,;:") or text.strip()
            desc2, category, cp = await maybe_ai_enrich(desc)

            tx_at = row["dt"]
            if tx_at:
                try:
                    tx_at2 = datetime.strptime(tx_at, "%d.%m.%Y %H:%M")
                    tx_at_str = tx_at2.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    tx_at_str = now_iso()
            else:
                tx_at_str = now_iso()

            records.append(
                ParsedRecord(
                    tx_type=tx_type,
                    amount_uzs=int(amount_uzs.quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
                    currency=currency,
                    amount_original=money_fmt_decimal(amount_original),
                    usd_rate_used=money_fmt_decimal(usd_rate),
                    description=desc2 or desc,
                    author_name=row["author"] or default_author,
                    tx_at=tx_at_str,
                    source_text=row["raw_line"],
                    source_kind=row["kind"],
                    category=category,
                    counterparty=cp,
                    meta_json=json.dumps({"unit": unit, "sign": sign}, ensure_ascii=False),
                )
            )
    return records


def summarize_records(items: List[ParsedRecord]) -> Tuple[str, int, int, int]:
    income = sum(x.amount_uzs for x in items if x.tx_type == "income")
    expense = sum(x.amount_uzs for x in items if x.tx_type == "expense")
    net = income - expense
    parts = [
        f"Yozuvlar: {len(items)} ta",
        f"Kirim: {money_fmt_uzs(income)}",
        f"Chiqim: {money_fmt_uzs(expense)}",
        f"Qoldiq: {money_fmt_uzs(net)}",
    ]
    return "\n".join(parts), income, expense, net


async def build_preview_text(items: List[ParsedRecord], source_text: str) -> str:
    summary, income, expense, net = summarize_records(items)
    lines = ["<b>Preview</b>", summary, "", "<b>Yozuvlar:</b>"]
    for idx, x in enumerate(items[:20], 1):
        sign = "+" if x.tx_type == "income" else "-"
        orig = f"{sign}{x.amount_original} {x.currency}"
        lines.append(
            f"{idx}. {orig} → {money_fmt_uzs(x.amount_uzs)}\n"
            f"   Izoh: {x.description or '-'}\n"
            f"   Kategoriya: {x.category}\n"
            f"   Kontragent: {x.counterparty or '-'}\n"
            f"   Sana: {display_dt(x.tx_at)}"
        )
    if len(items) > 20:
        lines.append(f"... va yana {len(items) - 20} ta yozuv")
    lines.append("")
    lines.append("<b>Asl matn:</b>")
    lines.append(source_text[:3000])
    return "\n".join(lines)


async def save_pending_batch(pending: PendingBatch) -> int:
    summary_text, income, expense, net = summarize_records(pending.items)
    saved_at = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO batches(
                owner_id, source_text, created_at, saved_at, summary_text,
                item_count, income_total_uzs, expense_total_uzs, net_total_uzs, undone_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                pending.owner_id,
                pending.source_text,
                pending.created_at,
                saved_at,
                summary_text,
                len(pending.items),
                income,
                expense,
                net,
            ),
        )
        batch_id = int(cur.lastrowid)
        for x in pending.items:
            await db.execute(
                """
                INSERT INTO transactions(
                    batch_id, owner_id, tx_type, amount_uzs, currency, amount_original,
                    usd_rate_used, description, category, counterparty, author_name,
                    tx_at, source_text, source_kind, meta_json, created_at, deleted_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    batch_id,
                    pending.owner_id,
                    x.tx_type,
                    x.amount_uzs,
                    x.currency,
                    x.amount_original,
                    x.usd_rate_used,
                    x.description,
                    x.category,
                    x.counterparty,
                    x.author_name,
                    x.tx_at,
                    x.source_text,
                    x.source_kind,
                    x.meta_json,
                    saved_at,
                ),
            )
        await db.commit()
    return batch_id


async def undo_last_batch(owner_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id FROM batches
            WHERE owner_id=? AND undone_at IS NULL
            ORDER BY id DESC LIMIT 1
            """,
            (owner_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        batch_id = int(row[0])
        ts = now_iso()
        await db.execute("UPDATE batches SET undone_at=? WHERE id=?", (ts, batch_id))
        await db.execute("UPDATE transactions SET deleted_at=? WHERE batch_id=? AND deleted_at IS NULL", (ts, batch_id))
        await db.commit()
        return batch_id


async def fetch_totals(period: str) -> Dict[str, Any]:
    now = now_tz()
    if period == "today":
        start = datetime(now.year, now.month, now.day, tzinfo=TZ)
    elif period == "week":
        start = datetime(now.year, now.month, now.day, tzinfo=TZ) - timedelta(days=now.weekday())
    elif period == "month":
        start = datetime(now.year, now.month, 1, tzinfo=TZ)
    elif period == "live":
        raw = await get_current_period_start()
        try:
            start = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
        except Exception:
            start = now
    else:
        start = datetime(1970, 1, 1, tzinfo=TZ)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
              COALESCE(SUM(CASE WHEN tx_type='income' THEN amount_uzs ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN tx_type='expense' THEN amount_uzs ELSE 0 END),0),
              COUNT(*)
            FROM transactions
            WHERE deleted_at IS NULL AND tx_at >= ?
            """,
            (start_s,),
        )
        income, expense, count = await cur.fetchone()
    income = int(income or 0)
    expense = int(expense or 0)
    return {
        "start": start_s,
        "income": income,
        "expense": expense,
        "net": income - expense,
        "count": int(count or 0),
    }


async def recent_records(limit: int = 10, period_start: Optional[str] = None) -> List[Dict[str, Any]]:
    query = (
        "SELECT id, tx_at, tx_type, amount_uzs, currency, amount_original, description, category, counterparty "
        "FROM transactions WHERE deleted_at IS NULL"
    )
    params: List[Any] = []
    if period_start:
        query += " AND tx_at >= ?"
        params.append(period_start)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
    result = []
    for r in rows:
        result.append(
            {
                "id": r[0],
                "tx_at": r[1],
                "tx_type": r[2],
                "amount_uzs": r[3],
                "currency": r[4],
                "amount_original": r[5],
                "description": r[6],
                "category": r[7],
                "counterparty": r[8],
            }
        )
    return result


async def delete_record(record_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM transactions WHERE id=? AND deleted_at IS NULL", (record_id,))
        row = await cur.fetchone()
        if not row:
            return False
        await db.execute("UPDATE transactions SET deleted_at=? WHERE id=?", (now_iso(), record_id))
        await db.commit()
        return True


async def get_category_summary(period: str) -> List[Tuple[str, int]]:
    start = (await fetch_totals(period))["start"]
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT category, COALESCE(SUM(CASE WHEN tx_type='expense' THEN amount_uzs ELSE 0 END),0) AS total
            FROM transactions
            WHERE deleted_at IS NULL AND tx_at >= ?
            GROUP BY category
            ORDER BY total DESC
            """,
            (start,),
        )
        return [(r[0] or "boshqa", int(r[1] or 0)) for r in await cur.fetchall()]


async def export_files() -> Tuple[str, str, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT id, tx_at, tx_type, amount_uzs, currency, amount_original, usd_rate_used,
                   description, category, counterparty, author_name, source_kind, source_text, created_at
            FROM transactions
            WHERE deleted_at IS NULL
            ORDER BY tx_at ASC, id ASC
            """
        )
        rows = await cur.fetchall()

    ts = now_tz().strftime("%Y%m%d_%H%M%S")
    base = Path(tempfile.gettempdir()) / f"finance_export_{ts}"
    xlsx_path = str(base.with_suffix(".xlsx"))
    csv_path = str(base.with_suffix(".csv"))
    txt_path = str(base.with_suffix(".txt"))

    wb = Workbook()
    ws = wb.active
    ws.title = "Barcha"
    headers = [
        "ID", "Sana", "Turi", "UZS", "Valyuta", "Original", "USD kurs", "Izoh",
        "Kategoriya", "Kontragent", "Muallif", "Manba turi", "Asl satr", "Yaratilgan"
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")
    for row in rows:
        ws.append(list(row))
    for col in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)

    dash = wb.create_sheet("Dashboard", 0)
    today = await fetch_totals("today")
    week = await fetch_totals("week")
    month = await fetch_totals("month")
    live = await fetch_totals("live")
    dash_rows = [
        ["Ko‘rsatkich", "Qiymat"],
        ["Live qoldiq", money_fmt_uzs(live["net"])],
        ["Bugun qoldiq", money_fmt_uzs(today["net"])],
        ["Haftalik qoldiq", money_fmt_uzs(week["net"])],
        ["Oylik qoldiq", money_fmt_uzs(month["net"])],
        ["Jami yozuvlar", await count_active_transactions()],
    ]
    for r in dash_rows:
        dash.append(r)
    dash["A1"].font = dash["B1"].font = Font(bold=True)
    dash.column_dimensions["A"].width = 24
    dash.column_dimensions["B"].width = 24
    wb.save(xlsx_path)

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("FINANCE HISOBOT\n\n")
        for row in rows:
            f.write(
                f"#{row[0]} | {display_dt(row[1])} | {row[2]} | {money_fmt_uzs(row[3])} | {row[7]} | {row[8]}\n"
            )
    return xlsx_path, csv_path, txt_path


def main_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    if WEB_APP_URL:
        kb.add(KeyboardButton("🧾 Web App", web_app=WebAppInfo(url=WEB_APP_URL)))
    kb.row("📊 Bugun", "📅 Haftalik", "🗓 Oylik")
    kb.row("📈 Live", "🔄 Yangilash", "↩️ Undo")
    kb.row("💱 Kursni belgilash", "📄 Records", "📤 Export")
    kb.row("📝 Text hisobot", "📚 Kategoriyalar")
    return kb


def save_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Saqlash", callback_data="pending:save"),
        InlineKeyboardButton("❌ Bekor qilish", callback_data="pending:cancel"),
    )
    return kb


def refresh_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Ha, nolga tushir", callback_data="refresh:confirm"),
        InlineKeyboardButton("❌ Yo‘q", callback_data="refresh:cancel"),
    )
    return kb


def rate_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🌐 API orqali olish", callback_data="rate:api"),
        InlineKeyboardButton("⌨️ Qo‘lda kiritish", callback_data="rate:manual"),
    )
    kb.add(InlineKeyboardButton("❌ Bekor", callback_data="rate:cancel"))
    return kb


def rate_confirm_kb(rate: str, source: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Saqlash", callback_data=f"rate:save:{rate}:{source}"),
        InlineKeyboardButton("❌ Bekor", callback_data="rate:cancel"),
    )
    return kb


async def send_period_report(message: types.Message, period: str, title: str) -> None:
    totals = await fetch_totals(period)
    lines = [
        f"<b>{title}</b>",
        f"Boshlanish: {display_dt(totals['start'])}",
        f"Yozuvlar: {totals['count']} ta",
        f"Kirim: {money_fmt_uzs(totals['income'])}",
        f"Chiqim: {money_fmt_uzs(totals['expense'])}",
        f"Qoldiq: {money_fmt_uzs(totals['net'])}",
    ]
    recs = await recent_records(8, period_start=totals["start"])
    if recs:
        lines.append("")
        lines.append("<b>So‘nggi yozuvlar:</b>")
        for x in recs:
            sign = "+" if x["tx_type"] == "income" else "-"
            lines.append(
                f"#{x['id']} {display_dt(x['tx_at'])} | {sign}{money_fmt_uzs(x['amount_uzs'])} | {x['description']}"
            )
    await message.answer("\n".join(lines), reply_markup=main_kb())


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer(
        "Assalomu alaykum. Bot tayyor. Oddiy matn, Telegram export yoki Web App orqali yozuv yuborishingiz mumkin.\n\n"
        "Misol: <code>+250$+517 ming azam aka labo berari olindi</code>",
        reply_markup=main_kb(),
    )


@dp.message_handler(commands=["rate"])
async def cmd_rate(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    args = (message.get_args() or "").strip()
    if not args:
        rate = await get_usd_rate()
        await message.answer(
            f"Joriy USD kursi: <b>{money_fmt_decimal(rate)}</b>\n"
            f"Qo‘lda o‘rnatish: <code>/rate 12750</code>",
            reply_markup=main_kb(),
        )
        return
    try:
        val = clean_number(args)
        if val <= 0:
            raise ValueError
    except Exception:
        await message.answer("Format xato. Misol: <code>/rate 12750</code>")
        return
    await set_setting("usd_rate", money_fmt_decimal(val))
    await set_setting("usd_rate_source", "manual")
    await set_setting("usd_rate_updated_at", now_iso())
    await message.answer(f"✅ Yangi USD kurs saqlandi: <b>{money_fmt_decimal(val)}</b>")


@dp.message_handler(commands=["undo"])
async def cmd_undo(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    batch_id = await undo_last_batch(message.from_user.id)
    if batch_id is None:
        await message.answer("Bekor qilinadigan oxirgi amal topilmadi.")
        return
    await message.answer(f"↩️ Batch bekor qilindi: #{batch_id}", reply_markup=main_kb())


@dp.message_handler(commands=["records"])
async def cmd_records(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    args = (message.get_args() or "").strip()
    limit = 10
    if args.isdigit():
        limit = max(1, min(50, int(args)))
    rows = await recent_records(limit)
    if not rows:
        await message.answer("Hozircha yozuv yo‘q.")
        return
    text = ["<b>So‘nggi yozuvlar</b>"]
    for x in rows:
        sign = "+" if x["tx_type"] == "income" else "-"
        text.append(
            f"#{x['id']} | {display_dt(x['tx_at'])}\n"
            f"{sign}{money_fmt_uzs(x['amount_uzs'])} | {x['description']}\n"
            f"Kategoriya: {x['category']} | Kontragent: {x['counterparty'] or '-'}"
        )
    await message.answer("\n\n".join(text), reply_markup=main_kb())


@dp.message_handler(commands=["delete"])
async def cmd_delete(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    args = (message.get_args() or "").strip()
    if args.isdigit():
        ok = await delete_record(int(args))
        await message.answer("✅ O‘chirildi" if ok else "Topilmadi", reply_markup=main_kb())
        return
    await DeleteStates.waiting_record_id.set()
    await message.answer("O‘chiriladigan yozuv ID sini yuboring. Masalan: <code>123</code>")


@dp.message_handler(state=DeleteStates.waiting_record_id, content_types=types.ContentType.TEXT)
async def state_delete_record(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Faqat ID yuboring. Masalan: <code>123</code>")
        return
    ok = await delete_record(int(text))
    await state.finish()
    await message.answer("✅ O‘chirildi" if ok else "Topilmadi", reply_markup=main_kb())


@dp.message_handler(lambda m: m.text == "📊 Bugun")
async def btn_today(message: types.Message):
    await send_period_report(message, "today", "Bugungi hisobot")


@dp.message_handler(lambda m: m.text == "📅 Haftalik")
async def btn_week(message: types.Message):
    await send_period_report(message, "week", "Haftalik hisobot")


@dp.message_handler(lambda m: m.text == "🗓 Oylik")
async def btn_month(message: types.Message):
    await send_period_report(message, "month", "Oylik hisobot")


@dp.message_handler(lambda m: m.text == "📈 Live")
async def btn_live(message: types.Message):
    await send_period_report(message, "live", "Live hisobot")


@dp.message_handler(lambda m: m.text == "↩️ Undo")
async def btn_undo(message: types.Message):
    await cmd_undo(message)


@dp.message_handler(lambda m: m.text == "🔄 Yangilash")
async def btn_refresh(message: types.Message):
    await message.answer(
        "Joriy live hisob 0 dan boshlansinmi? Eski arxiv saqlanib qoladi.",
        reply_markup=refresh_confirm_kb(),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("refresh:"))
async def callback_refresh(call: CallbackQuery):
    if call.data == "refresh:cancel":
        await call.answer("Bekor qilindi")
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    ts = await reset_current_period()
    await call.answer("Yangilandi")
    await call.message.edit_text(
        f"✅ Live hisob yangilandi. Yangi boshlanish: <b>{display_dt(ts)}</b>",
        reply_markup=None,
    )


@dp.message_handler(lambda m: m.text == "💱 Kursni belgilash")
async def btn_rate(message: types.Message):
    rate = await get_usd_rate()
    src = await get_setting("usd_rate_source", "default")
    updated = await get_setting("usd_rate_updated_at", "-")
    await message.answer(
        f"<b>USD kurs sozlamasi</b>\n"
        f"Joriy kurs: {money_fmt_decimal(rate)}\n"
        f"Manba: {src}\n"
        f"Yangilangan: {display_dt(updated) if updated != '-' else '-'}\n\n"
        f"Tanlang:",
        reply_markup=rate_menu_kb(),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("rate:"), state="*")
async def callback_rate(call: CallbackQuery, state: FSMContext):
    data = call.data
    if data == "rate:cancel":
        await state.finish()
        await call.answer("Bekor qilindi")
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    if data == "rate:manual":
        await state.finish()
        await RateStates.waiting_manual_rate.set()
        await call.answer()
        await call.message.answer(
            "Yangi kursni yuboring.\nMisollar:\n<code>12750</code>\n<code>12,750</code>\n<code>12750.5</code>"
        )
        return
    if data == "rate:api":
        try:
            rate, dt = await fetch_cbu_usd_rate()
            await call.answer()
            await call.message.edit_text(
                f"CBU API orqali topildi:\nKurs: <b>{money_fmt_decimal(rate)}</b>\nSana: {dt}\n\nShu kursni saqlaysizmi?",
                reply_markup=rate_confirm_kb(money_fmt_decimal(rate), "api"),
            )
        except Exception as e:
            await call.answer("API xato", show_alert=True)
            await call.message.answer(f"API dan olishda xato: {e}")
        return
    if data.startswith("rate:save:"):
        _, _, rate_value, source = data.split(":", 3)
        await set_setting("usd_rate", rate_value)
        await set_setting("usd_rate_source", source)
        await set_setting("usd_rate_updated_at", now_iso())
        await call.answer("Saqlandi")
        await call.message.edit_text(f"✅ USD kurs saqlandi: <b>{rate_value}</b> | Manba: {source}")


@dp.message_handler(state=RateStates.waiting_manual_rate, content_types=types.ContentType.TEXT)
async def state_rate_manual(message: types.Message, state: FSMContext):
    raw = (message.text or "").strip()
    try:
        val = clean_number(raw)
        if val <= 0:
            raise ValueError
    except Exception:
        await message.answer("Format xato. Misol: <code>12750</code>")
        return
    await state.finish()
    await message.answer(
        f"Kiritilgan kurs: <b>{money_fmt_decimal(val)}</b>\nSaqlaymizmi?",
        reply_markup=rate_confirm_kb(money_fmt_decimal(val), "manual"),
    )


@dp.message_handler(lambda m: m.text == "📄 Records")
async def btn_records(message: types.Message):
    await cmd_records(message)


@dp.message_handler(lambda m: m.text == "📚 Kategoriyalar")
async def btn_categories(message: types.Message):
    cats = await get_category_summary("month")
    if not cats:
        await message.answer("Kategoriya bo‘yicha ma’lumot yo‘q.")
        return
    lines = ["<b>Oylik kategoriyalar</b>"]
    for name, total in cats[:20]:
        lines.append(f"• {name}: {money_fmt_uzs(total)}")
    await message.answer("\n".join(lines), reply_markup=main_kb())


@dp.message_handler(lambda m: m.text == "📝 Text hisobot")
async def btn_text_report(message: types.Message):
    today = await fetch_totals("today")
    week = await fetch_totals("week")
    month = await fetch_totals("month")
    live = await fetch_totals("live")
    text = (
        f"<b>Text hisobot</b>\n\n"
        f"Live: {money_fmt_uzs(live['net'])}\n"
        f"Bugun: {money_fmt_uzs(today['net'])}\n"
        f"Haftalik: {money_fmt_uzs(week['net'])}\n"
        f"Oylik: {money_fmt_uzs(month['net'])}"
    )
    await message.answer(text, reply_markup=main_kb())


@dp.message_handler(lambda m: m.text == "📤 Export")
async def btn_export(message: types.Message):
    xlsx_path, csv_path, txt_path = await export_files()
    await message.answer("Export tayyor. Fayllar yuborilmoqda...")
    await bot.send_document(message.chat.id, types.InputFile(xlsx_path), caption="Excel export")
    await bot.send_document(message.chat.id, types.InputFile(csv_path), caption="CSV export")
    await bot.send_document(message.chat.id, types.InputFile(txt_path), caption="Text export")


@dp.message_handler(content_types=types.ContentType.WEB_APP_DATA)
async def handle_web_app_data(message: types.Message, state: FSMContext):
    try:
        payload = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("Web App dan noto‘g‘ri JSON keldi.")
        return
    raw_text = (payload.get("text") or "").strip()
    note = (payload.get("note") or "").strip()
    source = (payload.get("source") or "mini_app").strip()
    if note:
        raw_text = f"{raw_text} | {note}"
    if not raw_text:
        await message.answer("Web App matni bo‘sh.")
        return
    items = await parse_text_to_records(raw_text, message.from_user.full_name)
    if not items:
        await message.answer("Summalar topilmadi. Misol: <code>+250$+517 ming azam aka labo berari olindi</code>")
        return
    for x in items:
        x.source_kind = source or "mini_app"
    preview = await build_preview_text(items, raw_text)
    pending = PendingBatch(
        owner_id=message.from_user.id,
        source_text=raw_text,
        created_at=now_iso(),
        summary_text="",
        items=items,
    )
    await state.update_data(pending=batch_to_dict(pending))
    await message.answer(preview, reply_markup=save_confirm_kb())


@dp.callback_query_handler(lambda c: c.data.startswith("pending:"), state="*")
async def callback_pending(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pending_raw = data.get("pending")
    if not pending_raw:
        await call.answer("Pending ma’lumot topilmadi", show_alert=True)
        return
    pending = dict_to_batch(pending_raw)
    if call.data == "pending:cancel":
        await state.update_data(pending=None)
        await call.answer("Bekor qilindi")
        await call.message.edit_reply_markup(reply_markup=None)
        return
    batch_id = await save_pending_batch(pending)
    await state.update_data(pending=None)
    await call.answer("Saqlandi")
    await call.message.edit_reply_markup(reply_markup=None)
    summary, income, expense, net = summarize_records(pending.items)
    await call.message.answer(
        f"✅ Saqlandi. Batch #{batch_id}\n{summary}",
        reply_markup=main_kb(),
    )


@dp.message_handler(content_types=types.ContentType.TEXT, state="*")
async def handle_text(message: types.Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text:
        return
    handled = {
        "📊 Bugun", "📅 Haftalik", "🗓 Oylik", "📈 Live", "🔄 Yangilash", "↩️ Undo",
        "💱 Kursni belgilash", "📄 Records", "📤 Export", "📝 Text hisobot", "📚 Kategoriyalar",
    }
    if text in handled:
        return
    items = await parse_text_to_records(text, message.from_user.full_name)
    if not items:
        await message.answer(
            "Summalar topilmadi.\nMisol: <code>+250$+517 ming azam aka labo berari olindi</code>",
            reply_markup=main_kb(),
        )
        return
    preview = await build_preview_text(items, text)
    pending = PendingBatch(
        owner_id=message.from_user.id,
        source_text=text,
        created_at=now_iso(),
        summary_text="",
        items=items,
    )
    await state.update_data(pending=batch_to_dict(pending))
    await message.answer(preview, reply_markup=save_confirm_kb())


def batch_to_dict(batch: PendingBatch) -> Dict[str, Any]:
    return {
        "owner_id": batch.owner_id,
        "source_text": batch.source_text,
        "created_at": batch.created_at,
        "summary_text": batch.summary_text,
        "items": [x.__dict__ for x in batch.items],
    }


def dict_to_batch(data: Dict[str, Any]) -> PendingBatch:
    return PendingBatch(
        owner_id=int(data["owner_id"]),
        source_text=str(data["source_text"]),
        created_at=str(data["created_at"]),
        summary_text=str(data.get("summary_text") or ""),
        items=[ParsedRecord(**x) for x in data.get("items", [])],
    )


async def on_startup(_: Dispatcher) -> None:
    await init_db()
    logger.info("Bot ishga tushdi")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
