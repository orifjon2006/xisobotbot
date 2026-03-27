import asyncio
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
TIMEZONE_NAME = os.getenv("BOT_TIMEZONE", os.getenv("TIMEZONE", "Asia/Tashkent")).strip()
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

GROQ_CLIENT: Optional[AsyncGroq] = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

logger = logging.getLogger("finance_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
router = Router()

INCOME = "income"
EXPENSE = "expense"
PENDING = "pending"
SAVED = "saved"
CANCELED = "canceled"
UNDONE = "undone"

# =========================================================
# UI
# =========================================================
BTN_TODAY = "📊 Bugun"
BTN_MONTH = "🗓 Oy"
BTN_BALANCE = "💰 Balans"
BTN_RECORDS = "🧾 So‘nggi"
BTN_EXPORT = "📤 Excel"
BTN_UNDO = "↩️ Oxirgi amalni bekor qilish"
BTN_HELP = "ℹ️ Yordam"

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_MONTH)],
        [KeyboardButton(text=BTN_BALANCE), KeyboardButton(text=BTN_RECORDS)],
        [KeyboardButton(text=BTN_EXPORT), KeyboardButton(text=BTN_UNDO)],
        [KeyboardButton(text=BTN_HELP)],
    ],
    resize_keyboard=True,
    input_field_placeholder="Masalan: +250$ klient to‘lovi yoki Telegram log matni",
)

HELP_TEXT = (
    "Assalomu alaykum. Bu bot summalarni aniq qoidalar bilan hisoblaydi.\n\n"
    "Qoidalar:\n"
    "• '+' bilan boshlangan summa = kirim\n"
    "• '+' bo‘lmasa summa = chiqim\n"
    "• 'mln' = 1 000 000\n"
    "• 'ming' yoki 'k' = 1 000\n"
    "• '$' yoki 'usd' bo‘lsa UZS ga kurs bo‘yicha aylantiriladi\n"
    "• Avval preview chiqadi, keyin 'Saqlash' ni bossangiz bazaga yoziladi\n"
    "• /undo oxirgi saqlangan batchni bekor qiladi\n\n"
    "Misollar:\n"
    "+250$ azam aka to‘lovi\n"
    "100 ming dostavka\n"
    "+517 ming labo berari\n"
    "3,9 mln xizmat predoplata\n"
    "+500$ +350 ming 100 ming dokument\n\n"
    "Buyruqlar:\n"
    "/start — menyu\n"
    "/help — yordam\n"
    "/stats [today|week|month|all|YYYY-MM|YYYY-MM-DD] — statistika\n"
    "/balance [period] — balans\n"
    "/records [son] — oxirgi yozuvlar\n"
    "/export [period] — Excel hisobot\n"
    "/rate [qiymat] — USD kursi\n"
    "/undo — oxirgi amalni bekor qilish\n"
    "/delete <id> — bitta yozuvni o‘chirish\n"
    "/categories [period] — kategoriya kesimi"
)


# =========================================================
# REGEX / PARSING
# =========================================================
TELEGRAM_LINE_RE = re.compile(
    r"^\[(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\]\s*(?P<author>[^:]+):\s*(?P<body>.+)$"
)

AMOUNT_RE = re.compile(
    r"(?<![\w/])"
    r"(?P<sign>[+-]?)\s*"
    r"(?P<number>(?:\d{1,3}(?:[\s.,]\d{3})+|\d+)(?:[.,]\d+)?)"
    r"\s*(?P<mult>mln\.?|million|mlyon|млн\.?|ming|k|thousand|тыс\.?)?"
    r"\s*(?P<currency>\$|usd|dollar|dollars|доллар|uzs|sum|som|сум|so['ʻ’]?m|so‘m)?",
    re.IGNORECASE,
)

NAME_HINT_RE = re.compile(r"\b([A-ZА-ЯЁ][a-zа-яё]+(?:\s+[A-ZА-ЯЁ][a-zа-яё]+){0,2})\b")


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
class ParsedTransaction:
    direction: str
    currency: str
    amount_original: float
    amount_uzs: int
    amount_text: str
    note: str
    category: str
    counterparty: str
    raw_line: str
    line_index: int
    author: str
    tx_at: str
    usd_rate: float


# =========================================================
# TIME / FORMAT HELPERS
# =========================================================
def now_local() -> datetime:
    return datetime.now(TZ)


def parse_telegram_dt(value: str) -> datetime:
    return datetime.strptime(value, "%d.%m.%Y %H:%M").replace(tzinfo=TZ)


def start_of_day(dt: datetime) -> datetime:
    return dt.astimezone(TZ).replace(hour=0, minute=0, second=0, microsecond=0)


def parse_period(arg: Optional[str]) -> tuple[Optional[datetime], Optional[datetime], str]:
    raw = (arg or "month").strip().lower()
    base = now_local()

    if raw == "all":
        return None, None, "all"
    if raw == "today":
        start = start_of_day(base)
        return start, start + timedelta(days=1), "today"
    if raw == "week":
        start = start_of_day(base - timedelta(days=base.weekday()))
        return start, start + timedelta(days=7), "week"
    if raw == "month":
        start = start_of_day(base.replace(day=1))
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, "month"

    if re.fullmatch(r"\d{4}-\d{2}", raw):
        year, month = map(int, raw.split("-"))
        start = datetime(year, month, 1, tzinfo=TZ)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=TZ)
        else:
            end = datetime(year, month + 1, 1, tzinfo=TZ)
        return start, end, raw

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        year, month, day = map(int, raw.split("-"))
        start = datetime(year, month, day, tzinfo=TZ)
        return start, start + timedelta(days=1), raw

    return parse_period("month")


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
    if abs(value - int(value)) < 1e-9:
        return "$" + f"{int(value):,}".replace(",", " ")
    return "$" + f"{value:,.2f}".replace(",", " ")


def dt_fmt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def iso_now() -> str:
    return now_local().isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(TZ)


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip(" ,;-—")


# =========================================================
# PARSER CORE
# =========================================================
def parse_decimalish(raw: str, multiplier_hint: str) -> float:
    text = raw.replace("\u00A0", " ").replace(" ", "")
    sep_count = text.count(",") + text.count(".")
    multiplier_hint = multiplier_hint.lower()

    if sep_count == 0:
        return float(text)

    if sep_count == 1:
        sep = "," if "," in text else "."
        left, right = text.split(sep, 1)
        if len(right) in (1, 2):
            return float(left + "." + right)
        if len(right) == 3 and multiplier_hint.startswith(("mln", "million", "mlyon", "млн")):
            return float(left + "." + right)
        return float(left + right)

    last_comma = text.rfind(",")
    last_dot = text.rfind(".")
    last_idx = max(last_comma, last_dot)
    right = text[last_idx + 1 :]
    if len(right) in (1, 2):
        integer_part = re.sub(r"[,.]", "", text[:last_idx])
        return float(integer_part + "." + right)
    return float(re.sub(r"[,.]", "", text))


def normalize_currency(currency_raw: str) -> str:
    value = (currency_raw or "").strip().lower()
    if value in {"$", "usd", "dollar", "dollars", "доллар"}:
        return "USD"
    return "UZS"


def multiplier_value(mult_raw: str) -> int:
    value = (mult_raw or "").strip().lower().replace(".", "")
    if value in {"mln", "million", "mlyon", "млн"}:
        return 1_000_000
    if value in {"ming", "k", "thousand", "тыс"}:
        return 1_000
    return 1


def detect_direction(sign: str) -> str:
    return INCOME if sign == "+" else EXPENSE


def extract_lines(text: str, fallback_author: str, fallback_dt: datetime) -> list[ParsedLine]:
    lines: list[ParsedLine] = []
    for idx, raw_line in enumerate([part.strip() for part in text.splitlines() if part.strip()], start=1):
        m = TELEGRAM_LINE_RE.match(raw_line)
        if m:
            lines.append(
                ParsedLine(
                    raw_line=raw_line,
                    body=m.group("body").strip(),
                    author=m.group("author").strip(),
                    tx_dt=parse_telegram_dt(m.group("dt")),
                    line_index=idx,
                )
            )
        else:
            lines.append(
                ParsedLine(
                    raw_line=raw_line,
                    body=raw_line,
                    author=fallback_author,
                    tx_dt=fallback_dt,
                    line_index=idx,
                )
            )
    return lines


def heuristic_category(note: str) -> str:
    text = note.lower()
    pairs = [
        (["dostav", "delivery", "доставка"], "Dostavka"),
        (["avans", "predoplata", "oldindan"], "Avans"),
        (["dokument", "hujjat"], "Dokument"),
        (["metan", "gaz", "benzin", "yoqilg"], "Yoqilg‘i"),
        (["labo", "tracker", "nikel", "resor"], "Avto xizmat"),
        (["xizmat", "service", "ustanovka", "ремонт"], "Xizmat"),
        (["oylik", "maosh", "zarplata"], "Ish haqi"),
        (["ijara", "arenda"], "Ijara"),
        (["temir", "material", "zapchast"], "Material"),
        (["qarz", "debt"], "Qarz"),
    ]
    for keys, label in pairs:
        if any(key in text for key in keys):
            return label
    return "Boshqa"


def heuristic_counterparty(note: str) -> str:
    match = NAME_HINT_RE.search(note)
    if match:
        return match.group(1)
    low = note.lower()
    for token in ["aka", "dost", "mijoz", "klient", "ustaga"]:
        pos = low.find(token)
        if pos > 0:
            left = note[: pos + len(token)]
            words = left.split()
            return " ".join(words[-3:])
    return ""


async def enrich_note_with_ai(note: str) -> tuple[str, str, str]:
    clean_note = normalize_spaces(note)
    category = heuristic_category(clean_note)
    counterparty = heuristic_counterparty(clean_note)

    if not GROQ_CLIENT or not clean_note:
        return clean_note, category, counterparty

    try:
        completion = await asyncio.wait_for(
            GROQ_CLIENT.chat.completions.create(
                model=GROQ_MODEL,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You extract accounting metadata. Return compact JSON with keys: "
                            "clean_note, category, counterparty. Keep Uzbek-friendly labels."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "text": clean_note,
                                "rules": {
                                    "category": "Short label like Avans, Xizmat, Dostavka, Yoqilg‘i, Dokument, Qarz, Material, Boshqa",
                                    "counterparty": "Person or company if present, otherwise empty string",
                                },
                            },
                            ensure_ascii=False,
                        ),
                    },
                ],
            ),
            timeout=20,
        )
        raw = completion.choices[0].message.content or "{}"
        data = json.loads(raw)
        clean_note = normalize_spaces(data.get("clean_note") or clean_note)
        category = normalize_spaces(data.get("category") or category)[:60] or category
        counterparty = normalize_spaces(data.get("counterparty") or counterparty)[:80]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Groq enrichment fallback ishladi: %s", exc)

    return clean_note, category, counterparty


def remove_amount_tokens(body: str) -> str:
    without_tokens = AMOUNT_RE.sub(" ", body)
    without_stray = re.sub(r"\s*[+\-]\s*", " ", without_tokens)
    return normalize_spaces(without_stray)


def parse_line_hits(body: str, usd_rate: float) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for match in AMOUNT_RE.finditer(body):
        number_raw = match.group("number") or ""
        currency_raw = match.group("currency") or ""
        mult_raw = match.group("mult") or ""
        sign = (match.group("sign") or "").strip()
        full_text = normalize_spaces(match.group(0) or "")

        # reject weak matches like bare 018 or 2026 without currency/multiplier/sign and with small/noisy token
        if not (currency_raw or mult_raw or sign or re.search(r"\s", full_text)):
            if number_raw.startswith("0") and len(re.sub(r"\D", "", number_raw)) > 1:
                continue
            if len(re.sub(r"\D", "", number_raw)) <= 2:
                continue

        amount = parse_decimalish(number_raw, mult_raw)
        amount *= multiplier_value(mult_raw)
        currency = normalize_currency(currency_raw)
        direction = detect_direction(sign)
        amount_uzs = int(round(amount * usd_rate)) if currency == "USD" else int(round(amount))

        hits.append(
            {
                "direction": direction,
                "currency": currency,
                "amount_original": float(amount),
                "amount_uzs": amount_uzs,
                "amount_text": full_text,
            }
        )
    return hits


async def build_transactions_from_text(text: str, author: str, base_dt: datetime, usd_rate: float) -> list[ParsedTransaction]:
    lines = extract_lines(text, author, base_dt)
    transactions: list[ParsedTransaction] = []

    for line in lines:
        hits = parse_line_hits(line.body, usd_rate)
        if not hits:
            continue
        stripped_note = remove_amount_tokens(line.body)
        clean_note, category, counterparty = await enrich_note_with_ai(stripped_note)
        for hit in hits:
            transactions.append(
                ParsedTransaction(
                    direction=hit["direction"],
                    currency=hit["currency"],
                    amount_original=hit["amount_original"],
                    amount_uzs=hit["amount_uzs"],
                    amount_text=hit["amount_text"],
                    note=clean_note or stripped_note or line.body,
                    category=category,
                    counterparty=counterparty,
                    raw_line=line.raw_line,
                    line_index=line.line_index,
                    author=line.author,
                    tx_at=line.tx_dt.isoformat(),
                    usd_rate=usd_rate,
                )
            )
    return transactions


# =========================================================
# DB
# =========================================================
async def db_connect() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    return conn


async def init_db() -> None:
    conn = await db_connect()
    try:
        await conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                source_message_id INTEGER,
                user_id INTEGER,
                author_name TEXT,
                status TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                confirmed_at TEXT,
                canceled_at TEXT,
                undone_at TEXT,
                preview_text TEXT
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                batch_id INTEGER NOT NULL,
                source_message_id INTEGER,
                user_id INTEGER,
                author_name TEXT,
                direction TEXT NOT NULL,
                currency TEXT NOT NULL,
                amount_original REAL NOT NULL,
                usd_rate REAL NOT NULL,
                amount_uzs INTEGER NOT NULL,
                amount_text TEXT NOT NULL,
                category TEXT,
                counterparty TEXT,
                note TEXT,
                raw_line TEXT,
                line_index INTEGER,
                tx_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                deleted_at TEXT,
                FOREIGN KEY(batch_id) REFERENCES batches(id)
            );

            CREATE INDEX IF NOT EXISTS idx_transactions_chat_txat
                ON transactions(chat_id, tx_at);
            CREATE INDEX IF NOT EXISTS idx_transactions_batch
                ON transactions(batch_id);
            CREATE INDEX IF NOT EXISTS idx_batches_chat_status
                ON batches(chat_id, status, created_at);
            """
        )
        await conn.execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES('usd_rate', ?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (str(DEFAULT_USD_RATE), iso_now()),
        )
        await conn.commit()
    finally:
        await conn.close()


async def get_setting(key: str, default: str = "") -> str:
    conn = await db_connect()
    try:
        async with conn.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row["value"] if row else default
    finally:
        await conn.close()


async def set_setting(key: str, value: str) -> None:
    conn = await db_connect()
    try:
        await conn.execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, iso_now()),
        )
        await conn.commit()
    finally:
        await conn.close()


async def get_usd_rate() -> float:
    raw = await get_setting("usd_rate", str(DEFAULT_USD_RATE))
    try:
        value = float(raw)
        if value > 0:
            return value
    except ValueError:
        pass
    return DEFAULT_USD_RATE


async def create_pending_batch(
    chat_id: int,
    source_message_id: int,
    user_id: Optional[int],
    author_name: str,
    raw_text: str,
    transactions: list[ParsedTransaction],
    preview_text: str,
) -> int:
    payload = json.dumps([asdict(item) for item in transactions], ensure_ascii=False)
    conn = await db_connect()
    try:
        cur = await conn.execute(
            """
            INSERT INTO batches(
                chat_id, source_message_id, user_id, author_name, status,
                raw_text, payload_json, created_at, preview_text
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                source_message_id,
                user_id,
                author_name,
                PENDING,
                raw_text,
                payload,
                iso_now(),
                preview_text,
            ),
        )
        await conn.commit()
        return int(cur.lastrowid)
    finally:
        await conn.close()


async def get_batch(batch_id: int) -> Optional[aiosqlite.Row]:
    conn = await db_connect()
    try:
        async with conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)) as cur:
            return await cur.fetchone()
    finally:
        await conn.close()


async def save_batch(batch_id: int) -> tuple[bool, str]:
    conn = await db_connect()
    try:
        await conn.execute("BEGIN")
        async with conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)) as cur:
            batch = await cur.fetchone()
        if not batch:
            await conn.rollback()
            return False, "Batch topilmadi."
        if batch["status"] != PENDING:
            await conn.rollback()
            return False, "Bu batch allaqachon qayta ishlangan."

        payload = json.loads(batch["payload_json"])
        created_at = iso_now()
        for item in payload:
            await conn.execute(
                """
                INSERT INTO transactions(
                    chat_id, batch_id, source_message_id, user_id, author_name,
                    direction, currency, amount_original, usd_rate, amount_uzs,
                    amount_text, category, counterparty, note, raw_line,
                    line_index, tx_at, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch["chat_id"],
                    batch_id,
                    batch["source_message_id"],
                    batch["user_id"],
                    item["author"],
                    item["direction"],
                    item["currency"],
                    item["amount_original"],
                    item["usd_rate"],
                    item["amount_uzs"],
                    item["amount_text"],
                    item.get("category") or "Boshqa",
                    item.get("counterparty") or "",
                    item.get("note") or "",
                    item.get("raw_line") or "",
                    item.get("line_index") or 0,
                    item["tx_at"],
                    created_at,
                ),
            )
        await conn.execute(
            "UPDATE batches SET status = ?, confirmed_at = ? WHERE id = ?",
            (SAVED, created_at, batch_id),
        )
        await conn.commit()
        return True, "Saqlandi"
    except Exception as exc:  # noqa: BLE001
        await conn.rollback()
        logger.exception("save_batch xato")
        return False, f"Saqlashda xato: {exc}"
    finally:
        await conn.close()


async def cancel_batch(batch_id: int) -> tuple[bool, str]:
    conn = await db_connect()
    try:
        cur = await conn.execute(
            "UPDATE batches SET status = ?, canceled_at = ? WHERE id = ? AND status = ?",
            (CANCELED, iso_now(), batch_id, PENDING),
        )
        await conn.commit()
        if cur.rowcount:
            return True, "Bekor qilindi"
        return False, "Batch topilmadi yoki allaqachon saqlangan."
    finally:
        await conn.close()


async def undo_last_batch(chat_id: int) -> tuple[bool, str, Optional[int]]:
    conn = await db_connect()
    try:
        await conn.execute("BEGIN")
        async with conn.execute(
            """
            SELECT id FROM batches
            WHERE chat_id = ? AND status = ?
            ORDER BY confirmed_at DESC, id DESC
            LIMIT 1
            """,
            (chat_id, SAVED),
        ) as cur:
            batch = await cur.fetchone()
        if not batch:
            await conn.rollback()
            return False, "Bekor qilish uchun oxirgi saqlangan amal topilmadi.", None

        batch_id = int(batch["id"])
        deleted_at = iso_now()
        await conn.execute(
            "UPDATE transactions SET is_deleted = 1, deleted_at = ? WHERE batch_id = ? AND is_deleted = 0",
            (deleted_at, batch_id),
        )
        await conn.execute(
            "UPDATE batches SET status = ?, undone_at = ? WHERE id = ?",
            (UNDONE, deleted_at, batch_id),
        )
        await conn.commit()
        return True, "Oxirgi amal bekor qilindi.", batch_id
    except Exception as exc:  # noqa: BLE001
        await conn.rollback()
        return False, f"Bekor qilishda xato: {exc}", None
    finally:
        await conn.close()


async def fetch_rows(
    chat_id: int,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: Optional[int] = None,
) -> list[aiosqlite.Row]:
    conn = await db_connect()
    try:
        sql = "SELECT * FROM transactions WHERE chat_id = ? AND is_deleted = 0"
        params: list[Any] = [chat_id]
        if start is not None:
            sql += " AND tx_at >= ?"
            params.append(start.isoformat())
        if end is not None:
            sql += " AND tx_at < ?"
            params.append(end.isoformat())
        sql += " ORDER BY tx_at DESC, id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        async with conn.execute(sql, params) as cur:
            return await cur.fetchall()
    finally:
        await conn.close()


async def delete_row(chat_id: int, row_id: int) -> bool:
    conn = await db_connect()
    try:
        cur = await conn.execute(
            """
            UPDATE transactions
            SET is_deleted = 1, deleted_at = ?
            WHERE chat_id = ? AND id = ? AND is_deleted = 0
            """,
            (iso_now(), chat_id, row_id),
        )
        await conn.commit()
        return bool(cur.rowcount)
    finally:
        await conn.close()


# =========================================================
# REPORTING
# =========================================================
def split_totals(rows: list[aiosqlite.Row | dict[str, Any]]) -> tuple[int, int, int]:
    income = 0
    expense = 0
    for row in rows:
        direction = row["direction"]
        amount = int(row["amount_uzs"])
        if direction == INCOME:
            income += amount
        else:
            expense += amount
    return income, expense, income - expense


def period_title(label: str) -> str:
    mapping = {
        "today": "Bugungi hisobot",
        "week": "Haftalik hisobot",
        "month": "Oylik hisobot",
        "all": "Umumiy hisobot",
    }
    return mapping.get(label, f"Hisobot: {label}")


def build_dashboard_text(label: str, rows: list[aiosqlite.Row], usd_rate: float) -> str:
    income, expense, balance = split_totals(rows)
    lines = [
        f"{period_title(label)}",
        f"\nKurs: 1 USD = {money_fmt_uzs(usd_rate)}",
        f"Kirim: {money_fmt_uzs(income)}",
        f"Chiqim: {money_fmt_uzs(expense)}",
        f"Balans: {money_fmt_uzs(balance)}",
        f"Operatsiyalar: {len(rows)} ta",
    ]
    if rows:
        lines.append("\nSo‘nggi yozuvlar:")
        for row in rows[:5]:
            dt = dt_fmt(parse_iso(row["tx_at"]))
            sign = "+" if row["direction"] == INCOME else "-"
            lines.append(
                f"{sign} #{row['id']} | {dt} | {money_fmt_uzs(row['amount_uzs'])} | {row['note'] or row['category'] or 'Izohsiz'}"
            )
    return "\n".join(lines)


def build_records_text(rows: list[aiosqlite.Row]) -> str:
    if not rows:
        return "Yozuv topilmadi."
    lines = ["So‘nggi yozuvlar:"]
    for row in rows:
        dt = dt_fmt(parse_iso(row["tx_at"]))
        if row["currency"] == "USD":
            original = f"{money_fmt_usd(float(row['amount_original']))} → {money_fmt_uzs(row['amount_uzs'])}"
        else:
            original = money_fmt_uzs(row["amount_uzs"])
        sign = "+" if row["direction"] == INCOME else "-"
        extra = f" | {row['counterparty']}" if row["counterparty"] else ""
        lines.append(
            f"{sign} #{row['id']} | {dt} | {original} | {row['note'] or 'Izohsiz'}{extra}"
        )
    return "\n".join(lines)


def build_category_text(rows: list[aiosqlite.Row]) -> str:
    if not rows:
        return "Kategoriya bo‘yicha yozuv topilmadi."
    summary: dict[str, dict[str, int]] = {}
    for row in rows:
        cat = row["category"] or "Boshqa"
        bucket = summary.setdefault(cat, {"income": 0, "expense": 0})
        bucket[row["direction"]] += int(row["amount_uzs"])
    lines = ["Kategoriya kesimi:"]
    for cat, stats in sorted(summary.items(), key=lambda x: x[1]["income"] + x[1]["expense"], reverse=True):
        lines.append(
            f"• {cat}: kirim {money_fmt_uzs(stats['income'])}, chiqim {money_fmt_uzs(stats['expense'])}, balans {money_fmt_uzs(stats['income'] - stats['expense'])}"
        )
    return "\n".join(lines)


def build_preview_text(transactions: list[ParsedTransaction], usd_rate: float) -> str:
    income, expense, balance = split_totals([asdict(item) for item in transactions])
    lines = [
        "Tekshiruv preview:",
        f"\nKurs: 1 USD = {money_fmt_uzs(usd_rate)}",
        f"Kirim: {money_fmt_uzs(income)}",
        f"Chiqim: {money_fmt_uzs(expense)}",
        f"Balans: {money_fmt_uzs(balance)}",
        f"Yozuvlar: {len(transactions)} ta",
        "\nQuyidagilar saqlanadi:",
    ]
    for idx, tx in enumerate(transactions[:20], start=1):
        dt = dt_fmt(parse_iso(tx.tx_at))
        sign = "+" if tx.direction == INCOME else "-"
        if tx.currency == "USD":
            amount = f"{money_fmt_usd(tx.amount_original)} → {money_fmt_uzs(tx.amount_uzs)}"
        else:
            amount = money_fmt_uzs(tx.amount_uzs)
        tail = f" | {tx.note}" if tx.note else ""
        cat = f" | {tx.category}" if tx.category else ""
        lines.append(f"{idx}) {sign} {dt} | {amount}{cat}{tail}")
    if len(transactions) > 20:
        lines.append(f"… yana {len(transactions) - 20} ta yozuv bor")
    lines.append("\nSaqlansinmi?")
    return "\n".join(lines)


def pending_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Saqlash", callback_data=f"batch:save:{batch_id}"),
                InlineKeyboardButton(text="❌ Bekor qilish", callback_data=f"batch:cancel:{batch_id}"),
            ]
        ]
    )


def undo_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Oxirgi amalni bekor qilish", callback_data=f"batch:undo:{batch_id}")]
        ]
    )


def autosize_sheet(ws) -> None:  # noqa: ANN001
    for column_cells in ws.columns:
        length = 0
        col_letter = get_column_letter(column_cells[0].column)
        for cell in column_cells:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            length = max(length, len(str(cell.value or "")))
        ws.column_dimensions[col_letter].width = min(max(length + 2, 12), 40)


def write_table_sheet(ws, title: str, rows: list[aiosqlite.Row]) -> None:  # noqa: ANN001
    headers = [
        "ID",
        "Sana",
        "Yil",
        "Oy",
        "Kun",
        "Soat",
        "Yo‘nalish",
        "Valyuta",
        "Asl summa",
        "USD kurs",
        "UZS summa",
        "Kategoriya",
        "Kontragent",
        "Izoh",
        "Muallif",
        "Batch ID",
        "Xom satr",
    ]
    ws.title = title
    ws.append(headers)
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.border = border
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row in rows:
        dt = parse_iso(row["tx_at"])
        original = money_fmt_usd(float(row["amount_original"])) if row["currency"] == "USD" else money_fmt_uzs(row["amount_original"])
        ws.append(
            [
                row["id"],
                dt_fmt(dt),
                dt.year,
                dt.month,
                dt.day,
                dt.strftime("%H:%M"),
                "Kirim" if row["direction"] == INCOME else "Chiqim",
                row["currency"],
                original,
                row["usd_rate"],
                row["amount_uzs"],
                row["category"],
                row["counterparty"],
                row["note"],
                row["author_name"],
                row["batch_id"],
                row["raw_line"],
            ]
        )
    ws.freeze_panes = "A2"
    autosize_sheet(ws)


def build_excel(rows: list[aiosqlite.Row], label: str, usd_rate: float) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = "Dashboard"
    income, expense, balance = split_totals(rows)
    dashboard_rows = [
        ["Hisobot", period_title(label)],
        ["Kurs", f"1 USD = {money_fmt_uzs(usd_rate)}"],
        ["Kirim", income],
        ["Chiqim", expense],
        ["Balans", balance],
        ["Operatsiyalar soni", len(rows)],
        ["Yaratilgan vaqt", dt_fmt(now_local())],
    ]
    for row in dashboard_rows:
        ws.append(row)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    autosize_sheet(ws)

    all_rows = rows
    income_rows = [row for row in rows if row["direction"] == INCOME]
    expense_rows = [row for row in rows if row["direction"] == EXPENSE]

    write_table_sheet(wb.create_sheet(), "Barcha", all_rows)
    write_table_sheet(wb.create_sheet(), "Kirim", income_rows)
    write_table_sheet(wb.create_sheet(), "Chiqim", expense_rows)

    fd, path = tempfile.mkstemp(prefix=f"finance_{label}_", suffix=".xlsx")
    os.close(fd)
    wb.save(path)
    return path


# =========================================================
# ACCESS / COMMON ACTIONS
# =========================================================
async def guard_message(message: Message) -> bool:
    if not ADMIN_IDS:
        return True
    user_id = message.from_user.id if message.from_user else None
    if user_id in ADMIN_IDS:
        return True
    await message.answer("Siz bu botdan foydalanish uchun ruxsatli emassiz.")
    return False


async def guard_callback(callback: CallbackQuery) -> bool:
    if not ADMIN_IDS:
        return True
    if callback.from_user.id in ADMIN_IDS:
        return True
    await callback.answer("Ruxsat yo‘q", show_alert=True)
    return False


async def send_dashboard(message: Message, period: str) -> None:
    start, end, label = parse_period(period)
    rows = await fetch_rows(message.chat.id, start, end)
    usd_rate = await get_usd_rate()
    await message.answer(build_dashboard_text(label, rows, usd_rate), reply_markup=MAIN_KB)


async def send_records(message: Message, limit: int = 10) -> None:
    rows = await fetch_rows(message.chat.id, None, None, limit=limit)
    await message.answer(build_records_text(rows), reply_markup=MAIN_KB)


# =========================================================
# COMMANDS
# =========================================================
@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    await message.answer(
        "Assalomu alaykum. Matn yuboring, men avval preview ko‘rsataman, keyin tasdiqlasangiz bazaga yozaman.",
        reply_markup=MAIN_KB,
    )
    await send_dashboard(message, "month")


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KB)


@router.message(Command("stats"))
@router.message(Command("balance"))
async def stats_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    await send_dashboard(message, arg)


@router.message(Command("records"))
async def records_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    limit = 10
    if message.text and " " in message.text:
        raw = message.text.split(maxsplit=1)[1].strip()
        if raw.isdigit():
            limit = min(max(int(raw), 1), 100)
    await send_records(message, limit)


@router.message(Command("categories"))
async def categories_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    start, end, _ = parse_period(arg)
    rows = await fetch_rows(message.chat.id, start, end)
    await message.answer(build_category_text(rows), reply_markup=MAIN_KB)


@router.message(Command("rate"))
async def rate_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 1:
        rate = await get_usd_rate()
        await message.answer(
            f"Joriy USD kursi: {money_fmt_uzs(rate)}\nYangilash uchun: /rate 12800",
            reply_markup=MAIN_KB,
        )
        return
    raw_value = parts[1].replace(" ", "").replace(",", ".")
    try:
        value = float(raw_value)
        if value <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Kurs noto‘g‘ri. Misol: /rate 12750", reply_markup=MAIN_KB)
        return
    await set_setting("usd_rate", str(value))
    await message.answer(f"✅ USD kursi saqlandi: {money_fmt_uzs(value)}", reply_markup=MAIN_KB)


@router.message(Command("export"))
async def export_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    arg = message.text.split(maxsplit=1)[1] if message.text and " " in message.text else "month"
    start, end, label = parse_period(arg)
    rows = await fetch_rows(message.chat.id, start, end)
    if not rows:
        await message.answer("Eksport uchun yozuv topilmadi.", reply_markup=MAIN_KB)
        return
    usd_rate = await get_usd_rate()
    path = build_excel(rows, label, usd_rate)
    try:
        await message.answer_document(
            FSInputFile(path, filename=f"finance_{label}.xlsx"),
            caption=build_dashboard_text(label, rows, usd_rate),
            reply_markup=MAIN_KB,
        )
    finally:
        Path(path).unlink(missing_ok=True)


@router.message(Command("undo"))
async def undo_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    ok, text, _batch_id = await undo_last_batch(message.chat.id)
    await message.answer(("✅ " if ok else "⚠️ ") + text, reply_markup=MAIN_KB)
    if ok:
        await send_dashboard(message, "month")


@router.message(Command("delete"))
async def delete_handler(message: Message) -> None:
    if not await guard_message(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer("Misol: /delete 15", reply_markup=MAIN_KB)
        return
    row_id = int(parts[1].strip())
    deleted = await delete_row(message.chat.id, row_id)
    if deleted:
        await message.answer(f"🗑 #{row_id} o‘chirildi.", reply_markup=MAIN_KB)
    else:
        await message.answer(f"#{row_id} topilmadi.", reply_markup=MAIN_KB)


# =========================================================
# BUTTONS
# =========================================================
@router.message(F.text == BTN_TODAY)
async def btn_today(message: Message) -> None:
    if not await guard_message(message):
        return
    await send_dashboard(message, "today")


@router.message(F.text == BTN_MONTH)
async def btn_month(message: Message) -> None:
    if not await guard_message(message):
        return
    await send_dashboard(message, "month")


@router.message(F.text == BTN_BALANCE)
async def btn_balance(message: Message) -> None:
    if not await guard_message(message):
        return
    await send_dashboard(message, "all")


@router.message(F.text == BTN_RECORDS)
async def btn_records(message: Message) -> None:
    if not await guard_message(message):
        return
    await send_records(message, 10)


@router.message(F.text == BTN_EXPORT)
async def btn_export(message: Message) -> None:
    if not await guard_message(message):
        return
    rows = await fetch_rows(message.chat.id, *parse_period("month")[:2])
    if not rows:
        await message.answer("Eksport uchun yozuv topilmadi.", reply_markup=MAIN_KB)
        return
    usd_rate = await get_usd_rate()
    path = build_excel(rows, "month", usd_rate)
    try:
        await message.answer_document(
            FSInputFile(path, filename="finance_month.xlsx"),
            caption=build_dashboard_text("month", rows, usd_rate),
            reply_markup=MAIN_KB,
        )
    finally:
        Path(path).unlink(missing_ok=True)


@router.message(F.text == BTN_UNDO)
async def btn_undo(message: Message) -> None:
    if not await guard_message(message):
        return
    ok, text, _ = await undo_last_batch(message.chat.id)
    await message.answer(("✅ " if ok else "⚠️ ") + text, reply_markup=MAIN_KB)
    if ok:
        await send_dashboard(message, "month")


@router.message(F.text == BTN_HELP)
async def btn_help(message: Message) -> None:
    if not await guard_message(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KB)


# =========================================================
# CALLBACKS
# =========================================================
@router.callback_query(F.data.startswith("batch:save:"))
async def callback_save_batch(callback: CallbackQuery) -> None:
    if not await guard_callback(callback):
        return
    batch_id = int(callback.data.split(":")[-1])
    if not callback.message:
        await callback.answer("Xabar topilmadi", show_alert=True)
        return
    batch = await get_batch(batch_id)
    if not batch or batch["chat_id"] != callback.message.chat.id:
        await callback.answer("Batch topilmadi", show_alert=True)
        return
    ok, text = await save_batch(batch_id)
    await callback.answer("Saqlandi" if ok else "Xato", show_alert=not ok)
    if callback.message:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(
            ("✅ " if ok else "⚠️ ") + text,
            reply_markup=undo_keyboard(batch_id) if ok else MAIN_KB,
        )
        if ok:
            await send_dashboard(callback.message, "month")


@router.callback_query(F.data.startswith("batch:cancel:"))
async def callback_cancel_batch(callback: CallbackQuery) -> None:
    if not await guard_callback(callback):
        return
    batch_id = int(callback.data.split(":")[-1])
    ok, text = await cancel_batch(batch_id)
    await callback.answer("Bekor qilindi" if ok else "Xato", show_alert=not ok)
    if callback.message:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(("❌ " if ok else "⚠️ ") + text, reply_markup=MAIN_KB)


@router.callback_query(F.data.startswith("batch:undo:"))
async def callback_undo_batch(callback: CallbackQuery) -> None:
    if not await guard_callback(callback):
        return
    batch_id = int(callback.data.split(":")[-1])
    if not callback.message:
        await callback.answer("Xabar topilmadi", show_alert=True)
        return
    batch = await get_batch(batch_id)
    if not batch or batch["chat_id"] != callback.message.chat.id:
        await callback.answer("Batch topilmadi", show_alert=True)
        return
    ok, text, undone_id = await undo_last_batch(callback.message.chat.id)
    if ok and undone_id != batch_id:
        text = f"Oxirgi saqlangan batch #{undone_id} bekor qilindi. So‘ralgan batch esa eng oxirgisi emas edi."
    await callback.answer("Bekor qilindi" if ok else "Xato", show_alert=not ok)
    if callback.message:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except TelegramBadRequest:
            pass
        await callback.message.answer(("↩️ " if ok else "⚠️ ") + text, reply_markup=MAIN_KB)
        if ok:
            await send_dashboard(callback.message, "month")


# =========================================================
# TEXT INGESTION
# =========================================================
@router.message(F.text)
async def ingest_text(message: Message) -> None:
    if not await guard_message(message):
        return
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    usd_rate = await get_usd_rate()
    transactions = await build_transactions_from_text(
        text=text,
        author=safe_author(message),
        base_dt=message.date.astimezone(TZ),
        usd_rate=usd_rate,
    )
    if not transactions:
        await message.answer(
            "Summalar topilmadi. Misol yuboring: +250$ klient yoki 100 ming dostavka",
            reply_markup=MAIN_KB,
        )
        return

    preview = build_preview_text(transactions, usd_rate)
    batch_id = await create_pending_batch(
        chat_id=message.chat.id,
        source_message_id=message.message_id,
        user_id=message.from_user.id if message.from_user else None,
        author_name=safe_author(message),
        raw_text=text,
        transactions=transactions,
        preview_text=preview,
    )
    await message.answer(preview, reply_markup=pending_keyboard(batch_id))


# =========================================================
# BOOTSTRAP
# =========================================================
async def set_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Botni ishga tushirish"),
            BotCommand(command="help", description="Yordam"),
            BotCommand(command="stats", description="Statistika"),
            BotCommand(command="records", description="So‘nggi yozuvlar"),
            BotCommand(command="export", description="Excel hisobot"),
            BotCommand(command="rate", description="USD kursi"),
            BotCommand(command="undo", description="Oxirgi amalni bekor qilish"),
        ]
    )


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN topilmadi")

    await init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await set_commands(bot)
    logger.info("Bot ishga tushdi")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot to‘xtatildi")
