import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    FSInputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv
from groq import AsyncGroq
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
DB_PATH = os.getenv("DB_PATH", "finance_bot.db").strip()
TIMEZONE_NAME = os.getenv("BOT_TIMEZONE", "Asia/Tashkent").strip()
DEFAULT_USD_RATE = float(os.getenv("DEFAULT_USD_RATE", "12750") or 12750)
ADMIN_IDS = {
    int(part.strip())
    for part in os.getenv("ADMIN_IDS", "").split(",")
    if part.strip().isdigit()
}

try:
    TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TZ = timezone(timedelta(hours=5))

router = Router()
logger = logging.getLogger("finance-bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

BUTTON_TODAY = "📊 Bugun"
BUTTON_MONTH = "🗓 Oy"
BUTTON_BALANCE = "💰 Balans"
BUTTON_EXPORT = "📤 Excel"
BUTTON_RECORDS = "🧾 So‘nggi 10"
BUTTON_RATE = "⚙️ Kurs"
BUTTON_HELP = "ℹ️ Yordam"

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BUTTON_TODAY), KeyboardButton(text=BUTTON_MONTH)],
        [KeyboardButton(text=BUTTON_BALANCE), KeyboardButton(text=BUTTON_EXPORT)],
        [KeyboardButton(text=BUTTON_RECORDS), KeyboardButton(text=BUTTON_RATE)],
        [KeyboardButton(text=BUTTON_HELP)],
    ],
    resize_keyboard=True,
    input_field_placeholder="Log yoki summali matn yuboring…",
)

HELP_TEXT = (
    "Bu bot summalarni juda sodda qoida bilan hisoblaydi:\n"
    "• '+' bilan boshlangan summa = kirim\n"
    "• qolgan summa = chiqim\n"
    "• 'mln' = 1 000 000\n"
    "• 'ming' = 1 000\n"
    "• '$' yoki 'usd' bo‘lsa, joriy kurs bo‘yicha UZS ga aylantiriladi\n\n"
    "Misollar:\n"
    "+250$ klient to‘lovi\n"
    "100 ming dostavka\n"
    "+517ming azam aka\n"
    "3,9 mln resor xizmat\n"
    "500$ +350 ming -100 ming 120 ming dokument\n\n"
    "Buyruqlar:\n"
    "/start — menyu\n"
    "/help — yordam\n"
    "/stats [today|week|month|YYYY-MM|YYYY-MM-DD] — statistika\n"
    "/balance [today|week|month|YYYY-MM|YYYY-MM-DD] — balans\n"
    "/records [son] — oxirgi yozuvlar\n"
    "/export [today|week|month|YYYY-MM|YYYY-MM-DD|all] — Excel\n"
    "/rate [qiymat] — USD kursi\n"
    "/delete <id> — yozuvni o‘chirish\n"
    "/categories [period] — kategoriya kesimi"
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

AI_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "category": {"type": "string"},
        "counterparty": {"type": "string"},
        "clean_note": {"type": "string"},
    },
    "required": ["category", "counterparty", "clean_note"],
    "additionalProperties": False,
}

GROQ_CLIENT: Optional[AsyncGroq] = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# =========================================================
# DATA CLASSES
# =========================================================
@dataclass
class ParsedLine:
    raw_line: str
    body: str
    author: str
    tx_dt: datetime
    line_index: int


@dataclass
class MoneyHit:
    raw_token: str
    sign: str
    direction: str
    currency: str
    amount_original: float
    amount_uzs: int
    amount_text: str
    start: int
    end: int


# =========================================================
# GENERAL HELPERS
# =========================================================
def now_local() -> datetime:
    return datetime.now(TZ)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat()


def from_iso(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(TZ)


def start_of_day(dt: datetime) -> datetime:
    return dt.astimezone(TZ).replace(hour=0, minute=0, second=0, microsecond=0)


def safe_author(message: Message) -> str:
    if not message.from_user:
        return "Noma'lum"
    full_name = " ".join(
        part for part in [message.from_user.first_name, message.from_user.last_name] if part
    ).strip()
    return full_name or message.from_user.username or str(message.from_user.id)


def money_fmt_uzs(value: float | int) -> str:
    return f"{int(round(value)):,}".replace(",", " ") + " so‘m"


def money_fmt_usd(value: float) -> str:
    if float(value).is_integer():
        return f"${int(value):,}".replace(",", " ")
    return f"${value:,.2f}".replace(",", " ")


def balance_emoji(amount: int) -> str:
    return "🟢" if amount >= 0 else "🔴"


def parse_telegram_dt(value: str) -> datetime:
    naive = datetime.strptime(value, "%d.%m.%Y %H:%M")
    return naive.replace(tzinfo=TZ)


def parse_period(arg: Optional[str]) -> tuple[Optional[datetime], Optional[datetime], str]:
    arg = (arg or "month").strip().lower()
    base = now_local()

    if arg == "all":
        return None, None, "all"

    if arg == "today":
        start = start_of_day(base)
        end = start + timedelta(days=1)
        return start, end, "today"

    if arg == "week":
        start = start_of_day(base - timedelta(days=base.weekday()))
        end = start + timedelta(days=7)
        return start, end, "week"

    if arg == "month":
        start = base.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, start.strftime("%Y-%m")

    if re.fullmatch(r"\d{4}-\d{2}", arg):
        start = datetime.strptime(arg + "-01", "%Y-%m-%d").replace(tzinfo=TZ)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, arg

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", arg):
        start = datetime.strptime(arg, "%Y-%m-%d").replace(tzinfo=TZ)
        return start, start + timedelta(days=1), arg

    return parse_period("month")


def is_allowed(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


async def guard(message: Message) -> bool:
    user_id = message.from_user.id if message.from_user else 0
    if not is_allowed(user_id):
        await message.answer("Sizga bu botdan foydalanish uchun ruxsat berilmagan.")
        return False
    return True


# =========================================================
# PARSING
# =========================================================
def normalize_body(body: str) -> str:
    text = body.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_lines(text: str, fallback_author: str) -> list[ParsedLine]:
    results: list[ParsedLine] = []
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]

    for idx, raw_line in enumerate(raw_lines, start=1):
        match = TELEGRAM_LINE_RE.match(raw_line)
        if match:
            tx_dt = parse_telegram_dt(match.group("dt"))
            author = match.group("author").strip()
            body = normalize_body(match.group("body"))
        else:
            tx_dt = now_local()
            author = fallback_author
            body = normalize_body(raw_line)
        results.append(
            ParsedLine(
                raw_line=raw_line,
                body=body,
                author=author,
                tx_dt=tx_dt,
                line_index=idx,
            )
        )
    return results


def _to_number(token: str, multiplier: str | None) -> float:
    cleaned = token.replace(" ", "")
    mult = (multiplier or "").lower()

    if mult in {"mln", "mln.", "million", "mlyon", "млн", "млн."}:
        cleaned = cleaned.replace(",", ".")
        value = float(cleaned)
        return value * 1_000_000

    if mult in {"ming", "mingta", "thousand", "тыс", "тыс.", "k"}:
        cleaned = cleaned.replace(",", ".")
        value = float(cleaned)
        return value * 1_000

    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(",", "")
    elif cleaned.count(",") == 1 and "." not in cleaned:
        left, right = cleaned.split(",")
        if len(right) == 3:
            cleaned = left + right
        else:
            cleaned = left + "." + right
    elif cleaned.count(".") == 1:
        left, right = cleaned.split(".")
        if len(right) == 3:
            cleaned = left + right

    value = float(cleaned)
    return value


def _currency_kind(currency: str | None) -> str:
    value = (currency or "").lower().strip()
    if value in {"$", "usd", "dollar", "dollars", "доллар"}:
        return "USD"
    return "UZS"


def _should_accept_hit(sign: str, number: str, mult: str | None, currency: str | None) -> bool:
    if sign in {"+", "-"}:
        return True
    if mult:
        return True
    if currency:
        return True
    digits_only = re.sub(r"\D", "", number)
    return len(digits_only) >= 4


def parse_money_hits(body: str, usd_rate: float) -> list[MoneyHit]:
    hits: list[MoneyHit] = []

    for match in AMOUNT_RE.finditer(body):
        sign = (match.group("sign") or "").strip()
        number = (match.group("number") or "").strip()
        multiplier = (match.group("mult") or "").strip() or None
        currency_token = (match.group("currency") or "").strip() or None
        raw_token = match.group(0).strip()

        if not number:
            continue
        if not _should_accept_hit(sign, number, multiplier, currency_token):
            continue

        try:
            value = _to_number(number, multiplier)
        except ValueError:
            continue

        currency = _currency_kind(currency_token)
        direction = INCOME if sign == "+" else EXPENSE
        amount_original = abs(value)
        if currency == "USD":
            amount_uzs = int(round(amount_original * usd_rate))
            amount_text = money_fmt_usd(amount_original)
        else:
            amount_uzs = int(round(amount_original))
            amount_text = money_fmt_uzs(amount_original)

        hits.append(
            MoneyHit(
                raw_token=raw_token,
                sign=sign,
                direction=direction,
                currency=currency,
                amount_original=amount_original,
                amount_uzs=amount_uzs,
                amount_text=amount_text,
                start=match.start(),
                end=match.end(),
            )
        )

    return hits


def strip_amount_tokens(body: str, hits: list[MoneyHit]) -> str:
    text = body
    for hit in sorted(hits, key=lambda item: item.start, reverse=True):
        text = text[: hit.start] + " " + text[hit.end :]
    text = re.sub(r"\s+", " ", text).strip(" ,.;:-")
    return text


def fallback_counterparty(text: str) -> str:
    patterns = [
        r"\b([A-ZА-ЯЁЎҚҒҲ][a-zа-яёўқғҳ]+\s+(?:aka|aka\.?|akaга|аka|akaга))\b",
        r"\b([A-ZА-ЯЁЎҚҒҲ][a-zа-яёўқғҳ]+\s+[A-ZА-ЯЁЎҚҒҲ][a-zа-яёўқғҳ]+)\b",
        r"\b([A-ZА-ЯЁЎҚҒҲ][a-zа-яёўқғҳ]+\s+aka)\b",
    ]
    for pattern in patterns:
        found = re.search(pattern, text, re.IGNORECASE)
        if found:
            return found.group(1).strip()
    return ""


def fallback_category(text: str) -> str:
    low = text.lower()
    mapping = {
        "dostav": "Dostavka",
        "deliver": "Dostavka",
        "temir": "Material",
        "nikel": "Material",
        "metan": "Yoqilg‘i",
        "gaz": "Yoqilg‘i",
        "benzin": "Yoqilg‘i",
        "resor": "Xizmat",
        "xizmat": "Xizmat",
        "predopl": "Avans",
        "avans": "Avans",
        "dokument": "Dokument",
        "dok": "Dokument",
        "labo": "Avto",
        "mashina": "Avto",
        "chek": "Hisobot",
        "to‘lov": "Savdo",
        "tolov": "Savdo",
        "olindi": "Savdo",
        "berdim": "Xarajat",
    }
    for key, value in mapping.items():
        if key in low:
            return value
    return "Boshqa"


async def enrich_metadata(body: str, hits: list[MoneyHit]) -> dict[str, str]:
    clean_note = strip_amount_tokens(body, hits) or body
    fallback = {
        "category": fallback_category(clean_note),
        "counterparty": fallback_counterparty(clean_note),
        "clean_note": clean_note,
    }

    if not GROQ_CLIENT:
        return fallback

    try:
        compact_hits = [
            {
                "direction": hit.direction,
                "currency": hit.currency,
                "amount_text": hit.amount_text,
                "raw_token": hit.raw_token,
            }
            for hit in hits
        ]
        prompt = (
            "Quyidagi moliyaviy yozuv uchun faqat metadata chiqaring. "
            "Summalarni va yo‘nalishni hisoblamang, ular allaqachon aniqlangan. "
            "Kategoriya qisqa bo‘lsin. Counterparty topilmasa bo‘sh string qaytaring.\n\n"
            f"Matn: {body}\n"
            f"Aniqlangan summalar: {json.dumps(compact_hits, ensure_ascii=False)}"
        )
        response = await GROQ_CLIENT.chat.completions.create(
            model=GROQ_MODEL,
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": "You extract metadata for finance records in Uzbek/Russian. Return only valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "finance_metadata",
                    "strict": True,
                    "schema": AI_SCHEMA,
                },
            },
        )
        content = response.choices[0].message.content or "{}"
        data = json.loads(content)
        return {
            "category": (data.get("category") or fallback["category"]).strip()[:60],
            "counterparty": (data.get("counterparty") or fallback["counterparty"]).strip()[:80],
            "clean_note": (data.get("clean_note") or fallback["clean_note"]).strip()[:300],
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Groq metadata fallback ishladi: %s", exc)
        return fallback


# =========================================================
# DATABASE
# =========================================================
async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA foreign_keys=ON;")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_hash TEXT UNIQUE NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER,
                user_id INTEGER,
                note_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                author TEXT NOT NULL,
                original_text TEXT NOT NULL,
                clean_note TEXT NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('income', 'expense')),
                category TEXT NOT NULL,
                counterparty TEXT NOT NULL,
                currency TEXT NOT NULL CHECK(currency IN ('UZS', 'USD')),
                amount_original REAL NOT NULL,
                amount_uzs INTEGER NOT NULL,
                usd_rate REAL NOT NULL,
                parser TEXT NOT NULL
            )
            """
        )
        await db.commit()


async def set_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, value, to_iso(now_local())),
        )
        await db.commit()


async def get_setting(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_usd_rate() -> float:
    stored = await get_setting("usd_rate")
    if stored is None:
        return DEFAULT_USD_RATE
    try:
        return float(stored)
    except ValueError:
        return DEFAULT_USD_RATE


async def save_transactions(
    chat_id: int,
    message_id: int | None,
    user_id: int | None,
    parsed_line: ParsedLine,
    hits: list[MoneyHit],
    meta: dict[str, str],
    usd_rate: float,
) -> int:
    inserted = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for hit in hits:
            raw_hash = (
                f"{chat_id}|{message_id}|{parsed_line.line_index}|{parsed_line.raw_line}|"
                f"{hit.start}|{hit.direction}|{hit.currency}|{hit.amount_original}"
            )
            entry_hash = hashlib.sha1(raw_hash.encode("utf-8")).hexdigest()
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO transactions (
                    entry_hash, chat_id, message_id, user_id, note_date, created_at, author,
                    original_text, clean_note, direction, category, counterparty,
                    currency, amount_original, amount_uzs, usd_rate, parser
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry_hash,
                    chat_id,
                    message_id,
                    user_id,
                    to_iso(parsed_line.tx_dt),
                    to_iso(now_local()),
                    parsed_line.author,
                    parsed_line.raw_line,
                    meta.get("clean_note", parsed_line.body),
                    hit.direction,
                    meta.get("category", "Boshqa"),
                    meta.get("counterparty", ""),
                    hit.currency,
                    hit.amount_original,
                    hit.amount_uzs,
                    usd_rate,
                    "deterministic+groq" if GROQ_CLIENT else "deterministic",
                ),
            )
            inserted += cursor.rowcount
        await db.commit()
    return inserted


async def fetch_rows(
    chat_id: int,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    clauses = ["chat_id = ?"]
    params: list[Any] = [chat_id]

    if start is not None and end is not None:
        clauses.append("note_date >= ?")
        clauses.append("note_date < ?")
        params.extend([to_iso(start), to_iso(end)])

    sql = (
        "SELECT id, note_date, author, clean_note, original_text, direction, category, counterparty, "
        "currency, amount_original, amount_uzs, usd_rate, parser "
        "FROM transactions WHERE "
        + " AND ".join(clauses)
        + " ORDER BY note_date DESC, id DESC"
    )
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def delete_row(chat_id: int, row_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM transactions WHERE chat_id = ? AND id = ?",
            (chat_id, row_id),
        )
        await db.commit()
        return cursor.rowcount > 0


# =========================================================
# REPORTS
# =========================================================
def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        INCOME: {"UZS": 0.0, "USD": 0.0, "UZS_EQ": 0},
        EXPENSE: {"UZS": 0.0, "USD": 0.0, "UZS_EQ": 0},
        "count": len(rows),
    }

    for row in rows:
        direction = row["direction"]
        currency = row["currency"]
        summary[direction][currency] += float(row["amount_original"])
        summary[direction]["UZS_EQ"] += int(row["amount_uzs"])

    income_eq = summary[INCOME]["UZS_EQ"]
    expense_eq = summary[EXPENSE]["UZS_EQ"]
    summary["balance_uzs_eq"] = income_eq - expense_eq
    return summary


def build_dashboard_text(label: str, rows: list[dict[str, Any]], usd_rate: float) -> str:
    summary = summarize_rows(rows)
    income = summary[INCOME]
    expense = summary[EXPENSE]
    balance = summary["balance_uzs_eq"]

    lines = [
        f"📌 {label} bo‘yicha holat",
        f"Yozuvlar soni: {summary['count']}",
        "",
        "📥 Kirim:",
        f"• UZS: {money_fmt_uzs(income['UZS'])}",
        f"• USD: {money_fmt_usd(income['USD'])}",
        f"• UZS ekv.: {money_fmt_uzs(income['UZS_EQ'])}",
        "",
        "📤 Chiqim:",
        f"• UZS: {money_fmt_uzs(expense['UZS'])}",
        f"• USD: {money_fmt_usd(expense['USD'])}",
        f"• UZS ekv.: {money_fmt_uzs(expense['UZS_EQ'])}",
        "",
        f"{balance_emoji(balance)} Sof balans: {money_fmt_uzs(balance)}",
        f"💱 USD kursi: {money_fmt_uzs(usd_rate)}",
    ]
    return "\n".join(lines)


def build_records_text(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Hozircha yozuv yo‘q."

    lines = ["🧾 Oxirgi yozuvlar:"]
    for row in rows:
        dt = from_iso(row["note_date"]).strftime("%d.%m.%Y %H:%M")
        sign = "+" if row["direction"] == INCOME else "-"
        amount = (
            money_fmt_usd(float(row["amount_original"]))
            if row["currency"] == "USD"
            else money_fmt_uzs(float(row["amount_original"]))
        )
        tail = []
        if row["category"]:
            tail.append(row["category"])
        if row["counterparty"]:
            tail.append(row["counterparty"])
        extra = f" [{', '.join(tail)}]" if tail else ""
        lines.append(f"#{row['id']} | {dt} | {sign}{amount} | {row['clean_note']}{extra}")
    return "\n".join(lines)


def build_category_text(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "Bu davrda kategoriya bo‘yicha yozuv yo‘q."

    stats: dict[str, dict[str, float]] = {}
    for row in rows:
        category = row["category"] or "Boshqa"
        bucket = stats.setdefault(category, {"income": 0.0, "expense": 0.0})
        bucket[row["direction"]] += float(row["amount_uzs"])

    sorted_items = sorted(
        stats.items(), key=lambda item: (item[1]["income"] - item[1]["expense"]), reverse=True
    )
    lines = ["📚 Kategoriya kesimi (UZS ekvivalent):"]
    for name, values in sorted_items:
        balance = int(values["income"] - values["expense"])
        lines.append(
            f"• {name}: +{money_fmt_uzs(values['income'])} / -{money_fmt_uzs(values['expense'])} / {money_fmt_uzs(balance)}"
        )
    return "\n".join(lines)


def autosize_sheet(ws) -> None:
    for col_idx, column_cells in enumerate(ws.columns, start=1):
        max_length = 0
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_length + 2, 38)


def add_table_sheet(wb: Workbook, title: str, rows: list[dict[str, Any]], direction: Optional[str]) -> None:
    ws = wb.create_sheet(title)
    headers = [
        "ID",
        "Sana",
        "Muallif",
        "Yo‘nalish",
        "Kategoriya",
        "Kontragent",
        "Valyuta",
        "Original summa",
        "UZS ekv.",
        "Izoh",
        "Asl matn",
    ]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="D9D9D9")

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)

    filtered = rows if direction is None else [row for row in rows if row["direction"] == direction]
    for row in filtered:
        dt = from_iso(row["note_date"]).strftime("%d.%m.%Y %H:%M")
        original_amount = (
            money_fmt_usd(float(row["amount_original"]))
            if row["currency"] == "USD"
            else money_fmt_uzs(float(row["amount_original"]))
        )
        ws.append(
            [
                row["id"],
                dt,
                row["author"],
                "Kirim" if row["direction"] == INCOME else "Chiqim",
                row["category"],
                row["counterparty"],
                row["currency"],
                original_amount,
                int(row["amount_uzs"]),
                row["clean_note"],
                row["original_text"],
            ]
        )

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    autosize_sheet(ws)


def build_excel(rows: list[dict[str, Any]], period_label: str, usd_rate: float) -> str:
    wb = Workbook()
    dashboard = wb.active
    dashboard.title = "Dashboard"

    title_fill = PatternFill("solid", fgColor="0F766E")
    section_fill = PatternFill("solid", fgColor="E2F0D9")
    white_font = Font(color="FFFFFF", bold=True, size=12)
    bold_font = Font(bold=True)

    dashboard.merge_cells("A1:D1")
    dashboard["A1"] = f"Finance Dashboard — {period_label}"
    dashboard["A1"].fill = title_fill
    dashboard["A1"].font = white_font
    dashboard["A1"].alignment = Alignment(horizontal="center")

    summary = summarize_rows(rows)
    dashboard["A3"] = "Ko‘rsatkich"
    dashboard["B3"] = "Qiymat"
    dashboard["A3"].fill = section_fill
    dashboard["B3"].fill = section_fill
    dashboard["A3"].font = bold_font
    dashboard["B3"].font = bold_font

    metrics = [
        ("Yozuvlar soni", summary["count"]),
        ("Kirim UZS", money_fmt_uzs(summary[INCOME]["UZS"])),
        ("Kirim USD", money_fmt_usd(summary[INCOME]["USD"])),
        ("Kirim UZS ekv.", money_fmt_uzs(summary[INCOME]["UZS_EQ"])),
        ("Chiqim UZS", money_fmt_uzs(summary[EXPENSE]["UZS"])),
        ("Chiqim USD", money_fmt_usd(summary[EXPENSE]["USD"])),
        ("Chiqim UZS ekv.", money_fmt_uzs(summary[EXPENSE]["UZS_EQ"])),
        ("Sof balans", money_fmt_uzs(summary["balance_uzs_eq"])),
        ("USD kursi", money_fmt_uzs(usd_rate)),
    ]
    for idx, pair in enumerate(metrics, start=4):
        dashboard[f"A{idx}"] = pair[0]
        dashboard[f"B{idx}"] = pair[1]

    autosize_sheet(dashboard)
    add_table_sheet(wb, "Barcha", rows, None)
    add_table_sheet(wb, "Kirim", rows, INCOME)
    add_table_sheet(wb, "Chiqim", rows, EXPENSE)

    fd, path = tempfile.mkstemp(prefix="finance_report_", suffix=".xlsx")
    os.close(fd)
    wb.save(path)
    return path


# =========================================================
# BOT COMMANDS
# =========================================================
async def set_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Menyu"),
            BotCommand(command="help", description="Yordam"),
            BotCommand(command="stats", description="Statistika"),
            BotCommand(command="balance", description="Balans"),
            BotCommand(command="records", description="Oxirgi yozuvlar"),
            BotCommand(command="export", description="Excel eksport"),
            BotCommand(command="rate", description="USD kursi"),
            BotCommand(command="delete", description="Yozuvni o‘chirish"),
            BotCommand(command="categories", description="Kategoriya kesimi"),
        ]
    )


async def send_dashboard(message: Message, period: str = "month") -> None:
    usd_rate = await get_usd_rate()
    start, end, label = parse_period(period)
    rows = await fetch_rows(message.chat.id, start, end)
    await message.answer(build_dashboard_text(label, rows, usd_rate), reply_markup=MAIN_KEYBOARD)


@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    if not await guard(message):
        return
    await message.answer(
        "Assalomu alaykum. Matn yuboring — bot summalarni ajratadi, bazaga yozadi va hisobot tayyorlaydi.",
        reply_markup=MAIN_KEYBOARD,
    )
    await send_dashboard(message, "month")


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    if not await guard(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KEYBOARD)


@router.message(Command("stats"))
async def stats_handler(message: Message) -> None:
    if not await guard(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    await send_dashboard(message, arg)


@router.message(Command("balance"))
async def balance_handler(message: Message) -> None:
    if not await guard(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    await send_dashboard(message, arg)


@router.message(Command("records"))
async def records_handler(message: Message) -> None:
    if not await guard(message):
        return
    limit = 10
    if message.text and " " in message.text:
        raw = message.text.split(maxsplit=1)[1].strip()
        if raw.isdigit():
            limit = min(max(int(raw), 1), 50)
    rows = await fetch_rows(message.chat.id, None, None, limit=limit)
    await message.answer(build_records_text(rows), reply_markup=MAIN_KEYBOARD)


@router.message(Command("categories"))
async def categories_handler(message: Message) -> None:
    if not await guard(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    start, end, _label = parse_period(arg)
    rows = await fetch_rows(message.chat.id, start, end)
    await message.answer(build_category_text(rows), reply_markup=MAIN_KEYBOARD)


@router.message(Command("rate"))
async def rate_handler(message: Message) -> None:
    if not await guard(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        rate = await get_usd_rate()
        await message.answer(
            f"Joriy USD kursi: {money_fmt_uzs(rate)}\nYangi kurs: /rate 12800",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    raw_value = parts[1].replace(" ", "").replace(",", ".")
    try:
        value = float(raw_value)
        if value <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Kurs noto‘g‘ri. Misol: /rate 12750", reply_markup=MAIN_KEYBOARD)
        return

    await set_setting("usd_rate", str(value))
    await message.answer(f"✅ USD kursi saqlandi: {money_fmt_uzs(value)}", reply_markup=MAIN_KEYBOARD)


@router.message(Command("delete"))
async def delete_handler(message: Message) -> None:
    if not await guard(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1 or not parts[1].strip().isdigit():
        await message.answer("Misol: /delete 15", reply_markup=MAIN_KEYBOARD)
        return
    row_id = int(parts[1].strip())
    deleted = await delete_row(message.chat.id, row_id)
    if deleted:
        await message.answer(f"🗑 #{row_id} o‘chirildi.", reply_markup=MAIN_KEYBOARD)
    else:
        await message.answer(f"#{row_id} topilmadi.", reply_markup=MAIN_KEYBOARD)


@router.message(Command("export"))
async def export_handler(message: Message) -> None:
    if not await guard(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    start, end, label = parse_period(arg)
    usd_rate = await get_usd_rate()
    rows = await fetch_rows(message.chat.id, start, end)
    if not rows:
        await message.answer("Eksport uchun yozuv topilmadi.", reply_markup=MAIN_KEYBOARD)
        return

    path = build_excel(rows, label, usd_rate)
    try:
        await message.answer_document(
            FSInputFile(path, filename=f"finance_{label}.xlsx"),
            caption=build_dashboard_text(label, rows, usd_rate),
            reply_markup=MAIN_KEYBOARD,
        )
    finally:
        Path(path).unlink(missing_ok=True)


# =========================================================
# BUTTONS
# =========================================================
@router.message(F.text == BUTTON_TODAY)
async def button_today(message: Message) -> None:
    if not await guard(message):
        return
    await send_dashboard(message, "today")


@router.message(F.text == BUTTON_MONTH)
async def button_month(message: Message) -> None:
    if not await guard(message):
        return
    await send_dashboard(message, "month")


@router.message(F.text == BUTTON_BALANCE)
async def button_balance(message: Message) -> None:
    if not await guard(message):
        return
    await send_dashboard(message, "month")


@router.message(F.text == BUTTON_EXPORT)
async def button_export(message: Message) -> None:
    if not await guard(message):
        return
    message.text = "/export month"
    await export_handler(message)


@router.message(F.text == BUTTON_RECORDS)
async def button_records(message: Message) -> None:
    if not await guard(message):
        return
    message.text = "/records 10"
    await records_handler(message)


@router.message(F.text == BUTTON_RATE)
async def button_rate(message: Message) -> None:
    if not await guard(message):
        return
    rate = await get_usd_rate()
    await message.answer(
        f"Joriy USD kursi: {money_fmt_uzs(rate)}\nYangi kurs berish uchun: /rate 12800",
        reply_markup=MAIN_KEYBOARD,
    )


@router.message(F.text == BUTTON_HELP)
async def button_help(message: Message) -> None:
    if not await guard(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KEYBOARD)


# =========================================================
# TEXT INGESTION
# =========================================================
@router.message(F.text)
async def ingest_text_handler(message: Message) -> None:
    if not await guard(message):
        return
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    usd_rate = await get_usd_rate()
    lines = extract_lines(text, safe_author(message))

    inserted_total = 0
    parsed_count = 0
    skipped_lines: list[str] = []
    income_hits = 0
    expense_hits = 0
    income_uzs = 0
    expense_uzs = 0

    for parsed_line in lines:
        hits = parse_money_hits(parsed_line.body, usd_rate)
        if not hits:
            skipped_lines.append(parsed_line.raw_line)
            continue

        meta = await enrich_metadata(parsed_line.body, hits)
        inserted = await save_transactions(
            chat_id=message.chat.id,
            message_id=message.message_id,
            user_id=message.from_user.id if message.from_user else None,
            parsed_line=parsed_line,
            hits=hits,
            meta=meta,
            usd_rate=usd_rate,
        )
        inserted_total += inserted
        parsed_count += 1
        for hit in hits:
            if hit.direction == INCOME:
                income_hits += 1
                income_uzs += hit.amount_uzs
            else:
                expense_hits += 1
                expense_uzs += hit.amount_uzs

    month_start, month_end, label = parse_period("month")
    month_rows = await fetch_rows(message.chat.id, month_start, month_end)
    month_text = build_dashboard_text(label, month_rows, usd_rate)

    reply_lines = [
        "✅ Matn qayta ishlindi.",
        f"Qatorlar: {len(lines)} | Summali qatorlar: {parsed_count}",
        f"Yangi yozuvlar: {inserted_total}",
        f"Kirim soni: {income_hits} | UZS ekv.: {money_fmt_uzs(income_uzs)}",
        f"Chiqim soni: {expense_hits} | UZS ekv.: {money_fmt_uzs(expense_uzs)}",
    ]
    if skipped_lines:
        reply_lines.append(f"Summa topilmagan qatorlar: {len(skipped_lines)}")
    reply_lines.extend(["", month_text])
    await message.answer("\n".join(reply_lines), reply_markup=MAIN_KEYBOARD)


# =========================================================
# MAIN
# =========================================================
async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN topilmadi")

    await init_db()
    bot = Bot(BOT_TOKEN)
    await set_bot_commands(bot)

    dp = Dispatcher()
    dp.include_router(router)

    logger.info("Bot ishga tushmoqda…")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot to‘xtatildi")
