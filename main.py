import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from aiogram.utils import executor
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

load_dotenv(Path(__file__).with_name('.env'))

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
GROQ_API_KEY = os.getenv('GROQ_API_KEY', '').strip()
GROQ_MODEL = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile').strip()
GROQ_URL = os.getenv('GROQ_BASE_URL', 'https://api.groq.com/openai/v1/chat/completions').strip()
DB_PATH = os.getenv('DB_PATH', 'finance_bot.db').strip()
TIMEZONE_NAME = os.getenv('BOT_TIMEZONE', 'Asia/Tashkent').strip()
DEFAULT_USD_RATE = Decimal(os.getenv('DEFAULT_USD_RATE', '12750') or '12750')
EXPORT_SEND_XLSX = os.getenv('EXPORT_SEND_XLSX', '1').strip() not in {'0', 'false', 'False'}
EXPORT_SEND_CSV = os.getenv('EXPORT_SEND_CSV', '1').strip() not in {'0', 'false', 'False'}

ADMIN_IDS = set()
for chunk in os.getenv('ADMIN_IDS', '').split(','):
    chunk = chunk.strip()
    if chunk.isdigit():
        ADMIN_IDS.add(int(chunk))

try:
    TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    TZ = timezone(timedelta(hours=5))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger('finance-bot')

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)

# ============================================================
# UI
# ============================================================
BTN_TODAY = '📊 Bugun'
BTN_MONTH = '🗓 Oy'
BTN_BALANCE = '💰 Balans'
BTN_EXPORT = '📤 Export'
BTN_RECORDS = '🧾 So‘nggi 10'
BTN_UNDO = '↩️ Undo'
BTN_RATE = '⚙️ Kurs'
BTN_RESET_TODAY = '🔄 Yangilash'
BTN_HELP = 'ℹ️ Yordam'
BTN_TEXT_REPORT = '📄 Text hisobot'

MAIN_KB = ReplyKeyboardMarkup(resize_keyboard=True)
MAIN_KB.row(KeyboardButton(BTN_TODAY), KeyboardButton(BTN_MONTH))
MAIN_KB.row(KeyboardButton(BTN_BALANCE), KeyboardButton(BTN_EXPORT))
MAIN_KB.row(KeyboardButton(BTN_RECORDS), KeyboardButton(BTN_TEXT_REPORT))
MAIN_KB.row(KeyboardButton(BTN_UNDO), KeyboardButton(BTN_RATE), KeyboardButton(BTN_RESET_TODAY))
MAIN_KB.row(KeyboardButton(BTN_HELP))

HELP_TEXT = (
    '<b>Bot imkoniyatlari</b>\n'
    '• <code>+250$</code> → kirim\n'
    '• <code>100 ming</code> → chiqim\n'
    '• <code>+350 ming</code> → kirim\n'
    '• <code>3,9 mln</code> → 3 900 000\n'
    '• <code>$</code> yoki <code>usd</code> → USD, kurs bo‘yicha UZS ga o‘tkaziladi\n\n'
    '<b>Muhim qoida</b>\n'
    '• Summaning boshida <code>+</code> bo‘lsa kirim\n'
    '• <code>-</code> bo‘lsa chiqim\n'
    '• Belgi bo‘lmasa ham chiqim deb olinadi\n\n'
    '<b>Tugmalar</b>\n'
    '• <b>Bugun</b> — bugungi resetdan keyingi bugungi amallar\n'
    '• <b>Yangilash</b> — bugungi hisobni 0 dan qayta boshlaydi\n'
    '• <b>Export</b> — XLSX + CSV + text summary\n'
    '• <b>Undo</b> — oxirgi saqlangan batchni bekor qiladi\n\n'
    '<b>Buyruqlar</b>\n'
    '/start\n'
    '/help\n'
    '/stats [today|month|all|YYYY-MM|YYYY-MM-DD]\n'
    '/records [10]\n'
    '/categories [today|month|all]\n'
    '/rate [12750]\n'
    '/export [today|month|all]\n'
    '/undo\n'
    '/delete ID\n'
    '/reset_today'
)

# ============================================================
# REGEX / MODELS
# ============================================================
TELEGRAM_LINE_RE = re.compile(
    r'^\[(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\]\s*(?P<author>[^:]+):\s*(?P<body>.+)$'
)

AMOUNT_RE = re.compile(
    r'(?<![\w/])'
    r'(?P<sign>[+-]?)\s*'
    r'(?P<number>\d+(?:[.,]\d{1,3})*(?:[.,]\d+)?)'
    r'\s*(?P<mult>mln\.?|million|mlyon|млн\.?|ming|mingta|thousand|тыс\.?|k)?'
    r'\s*(?P<currency>\$|usd|dollar|dollars|доллар|uzs|sum|сум|so[\'\u02bb\u2019]?m|som)?',
    re.IGNORECASE,
)

INCOME = 'income'
EXPENSE = 'expense'

CATEGORY_KEYWORDS = {
    'Dostavka': ['dostavka', 'delivery', 'yetkaz', 'temir'],
    'Dokument': ['dokument', 'document', 'hujjat'],
    'Avans': ['avans', 'advance'],
    'Predoplata': ['predoplata', 'prepayment', 'oldindan'],
    'Xizmat': ['xizmat', 'service', 'resor', 'metan', 'remont', 'nikel'],
    'Transport': ['labo', 'mashina', 'avto', 'benzin', 'yoqilg', 'metan'],
    'Qarz': ['qarz', 'berdim', 'oldim', 'olindi'],
    'Maosh': ['maosh', 'oylik', 'salary'],
    'Sotuv': ['to\'lov', 'tolov', 'oplata', 'sale', 'sotuv', 'kirim'],
}

@dataclass
class ParsedLine:
    raw_line: str
    body: str
    author: str
    tx_dt: datetime
    line_index: int

@dataclass
class MoneyHit:
    raw_text: str
    sign: str
    direction: str
    currency: str
    amount_original: Decimal
    amount_uzs: int
    usd_rate: Decimal
    start: int
    end: int

@dataclass
class TxDraft:
    note_date: datetime
    author: str
    direction: str
    currency: str
    amount_original: Decimal
    amount_uzs: int
    usd_rate: Decimal
    clean_note: str
    category: str
    counterparty: str
    original_text: str

@dataclass
class PendingBatch:
    token: str
    chat_id: int
    user_id: int
    source_text: str
    created_at: datetime
    txs: List[TxDraft]

PENDING: Dict[str, PendingBatch] = {}

# ============================================================
# GENERIC HELPERS
# ============================================================
def now_local() -> datetime:
    return datetime.now(TZ)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat()


def from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime('%d.%m.%Y %H:%M')


def collapse_spaces(text: str) -> str:
    return re.sub(r'\s+', ' ', (text or '').strip())


def is_admin(user_id: int) -> bool:
    return not ADMIN_IDS or user_id in ADMIN_IDS


def money_fmt_uzs(value: Any) -> str:
    return f"{int(Decimal(str(value)).quantize(Decimal('1'), rounding=ROUND_HALF_UP)):,}".replace(',', ' ') + ' so\'m'


def money_fmt_usd(value: Any) -> str:
    q = Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    txt = format(q, 'f').rstrip('0').rstrip('.')
    return txt + ' $'


def balance_emoji(v: int) -> str:
    if v > 0:
        return '🟢'
    if v < 0:
        return '🔴'
    return '⚪'


def user_display_name(user: Optional[types.User]) -> str:
    if user is None:
        return 'Unknown'
    full = ' '.join(part for part in [user.first_name, user.last_name] if part)
    return full.strip() or str(user.id)


def parse_decimal(number_text: str, multiplier: Optional[str]) -> Decimal:
    text = (number_text or '').strip().replace(' ', '')
    has_mult = bool(multiplier)
    if has_mult:
        if text.count(',') + text.count('.') == 1:
            text = text.replace(',', '.')
        else:
            last_dot = text.rfind('.')
            last_comma = text.rfind(',')
            sep_pos = max(last_dot, last_comma)
            if sep_pos != -1:
                int_part = re.sub(r'[.,]', '', text[:sep_pos])
                frac_part = re.sub(r'[.,]', '', text[sep_pos + 1 :])
                text = int_part + '.' + frac_part
            else:
                text = re.sub(r'[.,]', '', text)
    else:
        if text.count(',') + text.count('.') == 1:
            sep = ',' if ',' in text else '.'
            left, right = text.split(sep, 1)
            if len(right) == 3 and len(left) >= 1:
                text = left + right
            else:
                text = left + '.' + right
        else:
            text = re.sub(r'[.,]', '', text)

    val = Decimal(text)
    mult = (multiplier or '').lower().strip('. ')
    if mult in {'mln', 'million', 'mlyon', 'млн'}:
        val *= Decimal('1000000')
    elif mult in {'ming', 'mingta', 'thousand', 'тыс', 'k'}:
        val *= Decimal('1000')
    return val


def is_usd(currency_raw: Optional[str]) -> bool:
    c = (currency_raw or '').lower()
    return c in {'$', 'usd', 'dollar', 'dollars', 'доллар'}


def normalize_currency(currency_raw: Optional[str]) -> str:
    return 'USD' if is_usd(currency_raw) else 'UZS'


def infer_category(text: str) -> str:
    lower = text.lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(k in lower for k in keywords):
            return cat
    return 'Boshqa'


def infer_counterparty(text: str) -> str:
    words = re.findall(r"[A-Za-zА-Яа-яЁёʻ’'\-]{3,}", text)
    banned = {
        'sum', 'usd', 'dollar', 'dollars', 'ming', 'mln', 'labo', 'dokument', 'xizmat', 'predoplata',
        'avans', 'resor', 'metan', 'delivery', 'temir', 'oplata', 'tolov', 'to\'lov', 'nikel'
    }
    for i in range(len(words) - 1):
        pair = f'{words[i]} {words[i+1]}'
        if all(w.lower() not in banned for w in pair.split()):
            return pair
    for w in words:
        if w.lower() not in banned:
            return w
    return ''


def strip_amount_tokens(text: str) -> str:
    return collapse_spaces(AMOUNT_RE.sub(' ', text))


def parse_telegram_or_plain_lines(text: str, fallback_author: str) -> List[ParsedLine]:
    lines: List[ParsedLine] = []
    base = now_local()
    for idx, raw in enumerate((text or '').splitlines(), start=1):
        raw = raw.strip()
        if not raw:
            continue
        m = TELEGRAM_LINE_RE.match(raw)
        if m:
            dt = datetime.strptime(m.group('dt'), '%d.%m.%Y %H:%M').replace(tzinfo=TZ)
            lines.append(
                ParsedLine(
                    raw_line=raw,
                    body=collapse_spaces(m.group('body')),
                    author=collapse_spaces(m.group('author')),
                    tx_dt=dt,
                    line_index=idx,
                )
            )
        else:
            lines.append(
                ParsedLine(
                    raw_line=raw,
                    body=collapse_spaces(raw),
                    author=fallback_author,
                    tx_dt=base,
                    line_index=idx,
                )
            )
    return lines


def parse_money_hits(text: str, usd_rate: Decimal) -> List[MoneyHit]:
    hits: List[MoneyHit] = []
    for m in AMOUNT_RE.finditer(text or ''):
        raw_text = m.group(0).strip()
        sign = (m.group('sign') or '').strip()
        number = m.group('number')
        mult = m.group('mult')
        curr = m.group('currency')

        # Avoid false positives like bare 018 from car plate fragments
        if not curr and not mult and len(re.sub(r'\D', '', number)) <= 2:
            continue
        if not curr and not mult and sign == '' and len(number) <= 3:
            continue

        try:
            amount_original = parse_decimal(number, mult)
        except (InvalidOperation, ValueError):
            continue

        currency = normalize_currency(curr)
        direction = INCOME if sign == '+' else EXPENSE
        if currency == 'USD':
            amount_uzs_dec = (amount_original * usd_rate).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
        else:
            amount_uzs_dec = amount_original.quantize(Decimal('1'), rounding=ROUND_HALF_UP)
        amount_uzs = int(amount_uzs_dec)
        if amount_uzs <= 0:
            continue

        hits.append(
            MoneyHit(
                raw_text=raw_text,
                sign=sign,
                direction=direction,
                currency=currency,
                amount_original=amount_original,
                amount_uzs=amount_uzs,
                usd_rate=usd_rate,
                start=m.start(),
                end=m.end(),
            )
        )
    return hits


async def enrich_note(text: str, hits: List[MoneyHit]) -> Dict[str, str]:
    clean_note = strip_amount_tokens(text) or collapse_spaces(text)
    category = infer_category(clean_note)
    counterparty = infer_counterparty(clean_note)

    if not GROQ_API_KEY:
        return {'clean_note': clean_note, 'category': category, 'counterparty': counterparty}

    schema_text = (
        'Return ONLY valid JSON with keys clean_note, category, counterparty. '
        'Do not include markdown. Keep clean_note concise. If uncertain, preserve existing meaning.'
    )
    prompt = {
        'text': text,
        'current': {'clean_note': clean_note, 'category': category, 'counterparty': counterparty},
        'rules': [
            'Language can be Uzbek/Russian/mixed.',
            'Do not change money values.',
            'Use short category labels.',
            'Counterparty should be a person/company name if present.',
        ],
    }
    payload = {
        'model': GROQ_MODEL,
        'temperature': 0.1,
        'response_format': {'type': 'json_object'},
        'messages': [
            {'role': 'system', 'content': schema_text},
            {'role': 'user', 'content': json.dumps(prompt, ensure_ascii=False)},
        ],
    }
    headers = {
        'Authorization': f'Bearer {GROQ_API_KEY}',
        'Content-Type': 'application/json',
    }
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(GROQ_URL, headers=headers, json=payload) as resp:
                if resp.status >= 400:
                    logger.warning('Groq HTTP %s: %s', resp.status, await resp.text())
                    return {'clean_note': clean_note, 'category': category, 'counterparty': counterparty}
                data = await resp.json()
        content = data['choices'][0]['message']['content']
        parsed = json.loads(content)
        return {
            'clean_note': collapse_spaces(parsed.get('clean_note') or clean_note),
            'category': collapse_spaces(parsed.get('category') or category) or category,
            'counterparty': collapse_spaces(parsed.get('counterparty') or counterparty),
        }
    except Exception as e:
        logger.warning('Groq enrich failed: %s', e)
        return {'clean_note': clean_note, 'category': category, 'counterparty': counterparty}


def build_preview_text(pending: PendingBatch) -> str:
    income = sum(tx.amount_uzs for tx in pending.txs if tx.direction == INCOME)
    expense = sum(tx.amount_uzs for tx in pending.txs if tx.direction == EXPENSE)
    lines = [
        '<b>Preview</b>',
        f'Qatorlar: {len(pending.txs)}',
        f'📥 Kirim: {money_fmt_uzs(income)}',
        f'📤 Chiqim: {money_fmt_uzs(expense)}',
        f'{balance_emoji(income-expense)} Sof: {money_fmt_uzs(income-expense)}',
        '',
    ]
    for idx, tx in enumerate(pending.txs[:20], start=1):
        sign = '+' if tx.direction == INCOME else '-'
        amt = money_fmt_usd(tx.amount_original) if tx.currency == 'USD' else money_fmt_uzs(tx.amount_original)
        lines.append(
            f"{idx}. {fmt_dt(tx.note_date)} | {sign}{amt} | {tx.clean_note}"
        )
    if len(pending.txs) > 20:
        lines.append(f"... yana {len(pending.txs)-20} ta yozuv")
    lines.append('')
    lines.append('Saqlansinmi?')
    return '\n'.join(lines)


def save_cancel_kb(token: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('✅ Saqlash', callback_data=f'save:{token}'),
        InlineKeyboardButton('❌ Bekor qilish', callback_data=f'cancel:{token}'),
    )
    return kb


def reset_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton('✅ Ha, bugunni yangila', callback_data='reset_today:confirm'),
        InlineKeyboardButton('❌ Yo‘q', callback_data='reset_today:cancel'),
    )
    return kb

# ============================================================
# DB LAYER
# ============================================================
async def ensure_column(db: aiosqlite.Connection, table: str, column: str, decl: str) -> None:
    cur = await db.execute(f'PRAGMA table_info({table})')
    cols = [row[1] for row in await cur.fetchall()]
    if column not in cols:
        await db.execute(f'ALTER TABLE {table} ADD COLUMN {column} {decl}')


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            '''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_resets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                reset_date TEXT NOT NULL,
                reset_at TEXT NOT NULL,
                user_id INTEGER,
                note TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,
                source_text TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                saved_at TEXT NOT NULL DEFAULT '',
                summary_text TEXT NOT NULL DEFAULT '',
                item_count INTEGER NOT NULL DEFAULT 0,
                income_total_uzs INTEGER NOT NULL DEFAULT 0,
                expense_total_uzs INTEGER NOT NULL DEFAULT 0,
                net_total_uzs INTEGER NOT NULL DEFAULT 0,
                undone_at TEXT,
                reset_scope_date TEXT
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER,
                entry_hash TEXT UNIQUE NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,
                note_date TEXT NOT NULL,
                created_at TEXT NOT NULL,
                author TEXT NOT NULL,
                original_text TEXT NOT NULL,
                clean_note TEXT NOT NULL,
                direction TEXT NOT NULL CHECK(direction IN ('income','expense')),
                category TEXT NOT NULL,
                counterparty TEXT NOT NULL,
                currency TEXT NOT NULL CHECK(currency IN ('UZS','USD')),
                amount_original TEXT NOT NULL,
                amount_uzs INTEGER NOT NULL,
                usd_rate TEXT NOT NULL,
                parser TEXT NOT NULL,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                deleted_at TEXT,
                FOREIGN KEY(batch_id) REFERENCES batches(id)
            );
            '''
        )

        for column, decl in [
            ('source_text', "TEXT NOT NULL DEFAULT ''"),
            ('created_at', "TEXT NOT NULL DEFAULT ''"),
            ('saved_at', "TEXT NOT NULL DEFAULT ''"),
            ('summary_text', "TEXT NOT NULL DEFAULT ''"),
            ('item_count', 'INTEGER NOT NULL DEFAULT 0'),
            ('income_total_uzs', 'INTEGER NOT NULL DEFAULT 0'),
            ('expense_total_uzs', 'INTEGER NOT NULL DEFAULT 0'),
            ('net_total_uzs', 'INTEGER NOT NULL DEFAULT 0'),
            ('undone_at', 'TEXT'),
            ('reset_scope_date', 'TEXT'),
        ]:
            await ensure_column(db, 'batches', column, decl)

        for column, decl in [
            ('batch_id', 'INTEGER'),
            ('is_deleted', 'INTEGER NOT NULL DEFAULT 0'),
            ('deleted_at', 'TEXT'),
        ]:
            await ensure_column(db, 'transactions', column, decl)

        await db.commit()
        if await get_setting_db(db, 'usd_rate') is None:
            await set_setting_db(db, 'usd_rate', str(DEFAULT_USD_RATE))
        await db.commit()


async def get_setting_db(db: aiosqlite.Connection, key: str) -> Optional[str]:
    cur = await db.execute('SELECT value FROM settings WHERE key = ?', (key,))
    row = await cur.fetchone()
    return row[0] if row else None


async def set_setting_db(db: aiosqlite.Connection, key: str, value: str) -> None:
    await db.execute(
        '''
        INSERT INTO settings(key, value, updated_at)
        VALUES(?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        ''',
        (key, value, to_iso(now_local())),
    )


async def get_usd_rate() -> Decimal:
    async with aiosqlite.connect(DB_PATH) as db:
        stored = await get_setting_db(db, 'usd_rate')
    try:
        return Decimal(stored or str(DEFAULT_USD_RATE))
    except InvalidOperation:
        return DEFAULT_USD_RATE


async def get_today_reset_anchor(chat_id: int, today: datetime) -> datetime:
    reset_date = today.astimezone(TZ).strftime('%Y-%m-%d')
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            'SELECT reset_at FROM daily_resets WHERE chat_id = ? AND reset_date = ? ORDER BY id DESC LIMIT 1',
            (chat_id, reset_date),
        )
        row = await cur.fetchone()
    if row and row[0]:
        return from_iso(row[0])
    start = today.astimezone(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return start


async def create_today_reset(chat_id: int, user_id: int, note: str = 'manual reset') -> datetime:
    now = now_local()
    reset_date = now.strftime('%Y-%m-%d')
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''
            INSERT INTO daily_resets(chat_id, reset_date, reset_at, user_id, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (chat_id, reset_date, to_iso(now), user_id, note, to_iso(now)),
        )
        await db.commit()
    return now


async def save_batch(pending: PendingBatch) -> int:
    income = sum(tx.amount_uzs for tx in pending.txs if tx.direction == INCOME)
    expense = sum(tx.amount_uzs for tx in pending.txs if tx.direction == EXPENSE)
    net = income - expense
    reset_scope_date = pending.created_at.strftime('%Y-%m-%d')
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            '''
            INSERT INTO batches(
                chat_id, user_id, source_text, created_at, saved_at, summary_text,
                item_count, income_total_uzs, expense_total_uzs, net_total_uzs, reset_scope_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                pending.chat_id,
                pending.user_id,
                pending.source_text,
                to_iso(pending.created_at),
                to_iso(now_local()),
                build_preview_text(pending),
                len(pending.txs),
                income,
                expense,
                net,
                reset_scope_date,
            ),
        )
        batch_id = cur.lastrowid

        for tx in pending.txs:
            raw_hash = (
                f"{pending.chat_id}|{pending.user_id}|{fmt_dt(tx.note_date)}|{tx.author}|"
                f"{tx.direction}|{tx.currency}|{tx.amount_original}|{tx.clean_note}|{tx.original_text}"
            )
            entry_hash = hashlib.sha1(raw_hash.encode('utf-8')).hexdigest()
            await db.execute(
                '''
                INSERT OR IGNORE INTO transactions(
                    batch_id, entry_hash, chat_id, user_id, note_date, created_at, author,
                    original_text, clean_note, direction, category, counterparty,
                    currency, amount_original, amount_uzs, usd_rate, parser
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    batch_id,
                    entry_hash,
                    pending.chat_id,
                    pending.user_id,
                    to_iso(tx.note_date),
                    to_iso(now_local()),
                    tx.author,
                    tx.original_text,
                    tx.clean_note,
                    tx.direction,
                    tx.category,
                    tx.counterparty,
                    tx.currency,
                    str(tx.amount_original),
                    tx.amount_uzs,
                    str(tx.usd_rate),
                    'deterministic+groq' if GROQ_API_KEY else 'deterministic',
                ),
            )
        await db.commit()
    return int(batch_id)


async def undo_last_batch(chat_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            'SELECT id FROM batches WHERE chat_id = ? AND undone_at IS NULL ORDER BY id DESC LIMIT 1',
            (chat_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        batch_id = int(row[0])
        undone_at = to_iso(now_local())
        await db.execute('UPDATE batches SET undone_at = ? WHERE id = ?', (undone_at, batch_id))
        await db.execute('UPDATE transactions SET is_deleted = 1, deleted_at = ? WHERE batch_id = ?', (undone_at, batch_id))
        await db.commit()
        return batch_id


async def delete_row(chat_id: int, row_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            'UPDATE transactions SET is_deleted = 1, deleted_at = ? WHERE chat_id = ? AND id = ? AND is_deleted = 0',
            (to_iso(now_local()), chat_id, row_id),
        )
        await db.commit()
        return cur.rowcount > 0


def parse_period(arg: str) -> Tuple[Optional[datetime], Optional[datetime], str, bool]:
    arg = (arg or 'month').strip().lower()
    now = now_local()
    if arg in {'all', 'hammasi'}:
        return None, None, 'hammasi', False
    if arg in {'today', 'bugun'}:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return start, end, 'bugun', True
    if arg in {'month', 'oy'}:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, 'oy', False
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', arg):
        start = datetime.strptime(arg, '%Y-%m-%d').replace(tzinfo=TZ)
        end = start + timedelta(days=1)
        return start, end, arg, False
    if re.fullmatch(r'\d{4}-\d{2}', arg):
        start = datetime.strptime(arg, '%Y-%m').replace(tzinfo=TZ)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end, arg, False
    # default month
    return parse_period('month')


async def fetch_rows(chat_id: int, start: Optional[datetime], end: Optional[datetime], *,
                     today_reset_scope: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    clauses = ['chat_id = ?', 'is_deleted = 0']
    params: List[Any] = [chat_id]

    if start is not None and end is not None:
        clauses.append('note_date >= ?')
        clauses.append('note_date < ?')
        params.extend([to_iso(start), to_iso(end)])

    if today_reset_scope and start is not None:
        reset_anchor = await get_today_reset_anchor(chat_id, start)
        clauses.append('created_at >= ?')
        params.append(to_iso(reset_anchor))

    sql = (
        'SELECT id, batch_id, note_date, created_at, author, clean_note, original_text, direction, category, '
        'counterparty, currency, amount_original, amount_uzs, usd_rate, parser '
        'FROM transactions WHERE ' + ' AND '.join(clauses) + ' ORDER BY note_date DESC, id DESC'
    )
    if limit:
        sql += ' LIMIT ?'
        params.append(limit)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

# ============================================================
# REPORTS
# ============================================================
def summarize_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary = {
        INCOME: {'UZS': Decimal('0'), 'USD': Decimal('0'), 'UZS_EQ': 0},
        EXPENSE: {'UZS': Decimal('0'), 'USD': Decimal('0'), 'UZS_EQ': 0},
        'count': len(rows),
    }
    for row in rows:
        direction = row['direction']
        currency = row['currency']
        summary[direction][currency] += Decimal(str(row['amount_original']))
        summary[direction]['UZS_EQ'] += int(row['amount_uzs'])
    summary['balance_uzs_eq'] = summary[INCOME]['UZS_EQ'] - summary[EXPENSE]['UZS_EQ']
    return summary


def build_dashboard_text(label: str, rows: List[Dict[str, Any]], usd_rate: Decimal, *, note: str = '') -> str:
    s = summarize_rows(rows)
    income = s[INCOME]
    expense = s[EXPENSE]
    balance = s['balance_uzs_eq']
    lines = [
        f'<b>{label.title()} bo‘yicha holat</b>',
        f'Yozuvlar: {s["count"]}',
        '',
        '📥 <b>Kirim</b>',
        f'• UZS: {money_fmt_uzs(income["UZS"])}',
        f'• USD: {money_fmt_usd(income["USD"])}',
        f'• UZS ekv.: {money_fmt_uzs(income["UZS_EQ"])}',
        '',
        '📤 <b>Chiqim</b>',
        f'• UZS: {money_fmt_uzs(expense["UZS"])}',
        f'• USD: {money_fmt_usd(expense["USD"])}',
        f'• UZS ekv.: {money_fmt_uzs(expense["UZS_EQ"])}',
        '',
        f'{balance_emoji(balance)} <b>Sof balans</b>: {money_fmt_uzs(balance)}',
        f'💱 USD kursi: {money_fmt_uzs(usd_rate)}',
    ]
    if note:
        lines.extend(['', note])
    return '\n'.join(lines)


def build_records_text(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return 'Yozuv yo‘q.'
    lines = ['<b>Oxirgi yozuvlar</b>']
    for row in rows:
        dt = fmt_dt(from_iso(row['note_date']))
        sign = '+' if row['direction'] == INCOME else '-'
        amount = money_fmt_usd(row['amount_original']) if row['currency'] == 'USD' else money_fmt_uzs(row['amount_original'])
        tail = []
        if row.get('category'):
            tail.append(row['category'])
        if row.get('counterparty'):
            tail.append(row['counterparty'])
        suffix = f" [{' | '.join(tail)}]" if tail else ''
        lines.append(f"#{row['id']} | {dt} | {sign}{amount} | {row['clean_note']}{suffix}")
    return '\n'.join(lines)


def build_category_text(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return 'Kategoriya bo‘yicha yozuv yo‘q.'
    agg: Dict[str, Dict[str, int]] = {}
    for row in rows:
        cat = row['category'] or 'Boshqa'
        agg.setdefault(cat, {'income': 0, 'expense': 0})
        agg[cat][row['direction']] += int(row['amount_uzs'])
    items = sorted(agg.items(), key=lambda kv: kv[1]['income'] - kv[1]['expense'], reverse=True)
    lines = ['<b>Kategoriya kesimi</b>']
    for cat, vals in items:
        net = vals['income'] - vals['expense']
        lines.append(
            f"• {cat}: +{money_fmt_uzs(vals['income'])} / -{money_fmt_uzs(vals['expense'])} / {money_fmt_uzs(net)}"
        )
    return '\n'.join(lines)


def build_text_export(label: str, rows: List[Dict[str, Any]], usd_rate: Decimal) -> str:
    lines = [build_dashboard_text(label, rows, usd_rate), '', '—' * 36, '', build_records_text(rows)]
    return '\n'.join(lines)


def autosize(ws) -> None:
    for idx, column in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in column:
            val = '' if cell.value is None else str(cell.value)
            max_len = max(max_len, len(val))
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 40)


def add_table_sheet(wb: Workbook, title: str, rows: List[Dict[str, Any]], direction: Optional[str]) -> None:
    ws = wb.create_sheet(title)
    headers = ['ID', 'Batch', 'Sana', 'Muallif', 'Yo‘nalish', 'Kategoriya', 'Kontragent', 'Valyuta', 'Original summa', 'UZS ekv.', 'Izoh', 'Asl matn']
    ws.append(headers)
    fill = PatternFill('solid', fgColor='1F4E78')
    font = Font(color='FFFFFF', bold=True)
    thin = Side(style='thin', color='D9D9D9')
    for cell in ws[1]:
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
    filtered = rows if direction is None else [r for r in rows if r['direction'] == direction]
    for row in filtered:
        dt = fmt_dt(from_iso(row['note_date']))
        original = money_fmt_usd(row['amount_original']) if row['currency'] == 'USD' else money_fmt_uzs(row['amount_original'])
        ws.append([
            row['id'], row.get('batch_id') or '', dt, row['author'], row['direction'], row['category'], row['counterparty'],
            row['currency'], original, int(row['amount_uzs']), row['clean_note'], row['original_text'],
        ])
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical='top', wrap_text=True)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
    autosize(ws)
    ws.freeze_panes = 'A2'


def build_excel(rows: List[Dict[str, Any]], label: str, usd_rate: Decimal) -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = 'Dashboard'
    dashboard = build_dashboard_text(label, rows, usd_rate).replace('<b>', '').replace('</b>', '')
    for line in dashboard.splitlines():
        ws.append([line])
    autosize(ws)
    add_table_sheet(wb, 'Barcha', rows, None)
    add_table_sheet(wb, 'Kirim', rows, INCOME)
    add_table_sheet(wb, 'Chiqim', rows, EXPENSE)
    path = os.path.join(tempfile.gettempdir(), f'finance_{uuid4().hex[:8]}.xlsx')
    wb.save(path)
    return path


def build_csv(rows: List[Dict[str, Any]]) -> str:
    path = os.path.join(tempfile.gettempdir(), f'finance_{uuid4().hex[:8]}.csv')
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'Batch', 'Sana', 'Muallif', 'Yo‘nalish', 'Kategoriya', 'Kontragent', 'Valyuta', 'Original summa', 'UZS ekv.', 'Izoh', 'Asl matn'])
        for row in rows:
            dt = fmt_dt(from_iso(row['note_date']))
            original = money_fmt_usd(row['amount_original']) if row['currency'] == 'USD' else money_fmt_uzs(row['amount_original'])
            writer.writerow([
                row['id'], row.get('batch_id') or '', dt, row['author'], row['direction'], row['category'], row['counterparty'],
                row['currency'], original, int(row['amount_uzs']), row['clean_note'], row['original_text'],
            ])
    return path

# ============================================================
# BOT HELPERS
# ============================================================
async def guard(message: types.Message) -> bool:
    uid = message.from_user.id if message.from_user else 0
    if not is_admin(uid):
        await message.answer('Sizda ruxsat yo‘q.')
        return False
    return True


async def send_period_dashboard(message: types.Message, period_arg: str) -> None:
    usd_rate = await get_usd_rate()
    start, end, label, reset_scope = parse_period(period_arg)
    rows = await fetch_rows(message.chat.id, start, end, today_reset_scope=reset_scope)
    note = ''
    if reset_scope and start is not None:
        reset_anchor = await get_today_reset_anchor(message.chat.id, start)
        note = f'Bugungi hisob {fmt_dt(reset_anchor)} dan boshlangan.'
    await message.answer(build_dashboard_text(label, rows, usd_rate, note=note), reply_markup=MAIN_KB)

# ============================================================
# COMMANDS
# ============================================================
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message) -> None:
    if not await guard(message):
        return
    await message.answer('Assalomu alaykum. Matn yuboring — bot oldin preview ko‘rsatadi, keyin saqlaydi.', reply_markup=MAIN_KB)
    await send_period_dashboard(message, 'today')


@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message) -> None:
    if not await guard(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KB)


@dp.message_handler(commands=['stats', 'balance'])
async def cmd_stats(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = message.get_args() or 'month'
    await send_period_dashboard(message, arg)


@dp.message_handler(commands=['records'])
async def cmd_records(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = message.get_args().strip() if message.get_args() else '10'
    limit = 10
    if arg.isdigit():
        limit = max(1, min(50, int(arg)))
    rows = await fetch_rows(message.chat.id, None, None, limit=limit)
    await message.answer(build_records_text(rows), reply_markup=MAIN_KB)


@dp.message_handler(commands=['categories'])
async def cmd_categories(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = message.get_args() or 'month'
    start, end, _label, reset_scope = parse_period(arg)
    rows = await fetch_rows(message.chat.id, start, end, today_reset_scope=reset_scope)
    await message.answer(build_category_text(rows), reply_markup=MAIN_KB)


@dp.message_handler(commands=['rate'])
async def cmd_rate(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = (message.get_args() or '').strip()
    if not arg:
        rate = await get_usd_rate()
        await message.answer(f'Joriy USD kursi: {money_fmt_uzs(rate)}\nYangi kurs: /rate 12750', reply_markup=MAIN_KB)
        return
    raw = arg.replace(' ', '').replace(',', '.')
    try:
        value = Decimal(raw)
        if value <= 0:
            raise InvalidOperation
    except Exception:
        await message.answer('Kurs noto‘g‘ri. Misol: /rate 12750', reply_markup=MAIN_KB)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await set_setting_db(db, 'usd_rate', str(value))
        await db.commit()
    await message.answer(f'✅ USD kursi saqlandi: {money_fmt_uzs(value)}', reply_markup=MAIN_KB)


async def send_exports(message: types.Message, period_arg: str) -> None:
    start, end, label, reset_scope = parse_period(period_arg)
    usd_rate = await get_usd_rate()
    rows = await fetch_rows(message.chat.id, start, end, today_reset_scope=reset_scope)
    if not rows:
        await message.answer('Eksport uchun yozuv topilmadi.', reply_markup=MAIN_KB)
        return

    dashboard = build_dashboard_text(label, rows, usd_rate)
    sent_any = False
    xlsx_path = None
    csv_path = None
    try:
        if EXPORT_SEND_XLSX:
            try:
                xlsx_path = build_excel(rows, label, usd_rate)
                await message.answer_document(InputFile(xlsx_path, filename=f'finance_{label}.xlsx'), caption='XLSX eksport tayyor.')
                sent_any = True
            except Exception as e:
                logger.exception('XLSX export failed: %s', e)
                await message.answer('XLSX tayyorlashda muammo bo‘ldi. CSV va text yuboraman.')
        if EXPORT_SEND_CSV:
            csv_path = build_csv(rows)
            await message.answer_document(InputFile(csv_path, filename=f'finance_{label}.csv'), caption='CSV eksport tayyor.')
            sent_any = True
        text_path = os.path.join(tempfile.gettempdir(), f'finance_{uuid4().hex[:8]}.txt')
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(build_text_export(label, rows, usd_rate))
        await message.answer_document(InputFile(text_path, filename=f'finance_{label}.txt'), caption=dashboard, reply_markup=MAIN_KB)
        sent_any = True
        Path(text_path).unlink(missing_ok=True)
    finally:
        if xlsx_path:
            Path(xlsx_path).unlink(missing_ok=True)
        if csv_path:
            Path(csv_path).unlink(missing_ok=True)
    if not sent_any:
        await message.answer(dashboard, reply_markup=MAIN_KB)


@dp.message_handler(commands=['export'])
async def cmd_export(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = message.get_args() or 'month'
    await send_exports(message, arg)


@dp.message_handler(commands=['undo'])
async def cmd_undo(message: types.Message) -> None:
    if not await guard(message):
        return
    batch_id = await undo_last_batch(message.chat.id)
    if batch_id is None:
        await message.answer('Bekor qilinadigan saqlangan batch topilmadi.', reply_markup=MAIN_KB)
        return
    await message.answer(f'↩️ Oxirgi batch bekor qilindi: #{batch_id}', reply_markup=MAIN_KB)
    await send_period_dashboard(message, 'today')


@dp.message_handler(commands=['delete'])
async def cmd_delete(message: types.Message) -> None:
    if not await guard(message):
        return
    arg = (message.get_args() or '').strip()
    if not arg.isdigit():
        await message.answer('Misol: /delete 15', reply_markup=MAIN_KB)
        return
    ok = await delete_row(message.chat.id, int(arg))
    await message.answer('🗑 Yozuv o‘chirildi.' if ok else 'Yozuv topilmadi.', reply_markup=MAIN_KB)


@dp.message_handler(commands=['reset_today'])
async def cmd_reset_today(message: types.Message) -> None:
    if not await guard(message):
        return
    await message.answer('Bugungi hisobni 0 dan boshlashni tasdiqlaysizmi? Eski arxiv saqlanadi.', reply_markup=reset_confirm_kb())

# ============================================================
# BUTTONS
# ============================================================
@dp.message_handler(lambda m: (m.text or '').strip() == BTN_TODAY)
async def btn_today(message: types.Message) -> None:
    if not await guard(message):
        return
    await send_period_dashboard(message, 'today')


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_MONTH)
async def btn_month(message: types.Message) -> None:
    if not await guard(message):
        return
    await send_period_dashboard(message, 'month')


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_BALANCE)
async def btn_balance(message: types.Message) -> None:
    if not await guard(message):
        return
    await send_period_dashboard(message, 'month')


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_EXPORT)
async def btn_export(message: types.Message) -> None:
    if not await guard(message):
        return
    await send_exports(message, 'month')


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_RECORDS)
async def btn_records(message: types.Message) -> None:
    if not await guard(message):
        return
    rows = await fetch_rows(message.chat.id, None, None, limit=10)
    await message.answer(build_records_text(rows), reply_markup=MAIN_KB)


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_TEXT_REPORT)
async def btn_text_report(message: types.Message) -> None:
    if not await guard(message):
        return
    start, end, label, reset_scope = parse_period('today')
    usd_rate = await get_usd_rate()
    rows = await fetch_rows(message.chat.id, start, end, today_reset_scope=reset_scope)
    await message.answer(build_text_export(label, rows, usd_rate), reply_markup=MAIN_KB)


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_UNDO)
async def btn_undo(message: types.Message) -> None:
    if not await guard(message):
        return
    await cmd_undo(message)


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_RATE)
async def btn_rate(message: types.Message) -> None:
    if not await guard(message):
        return
    rate = await get_usd_rate()
    await message.answer(f'Joriy USD kursi: {money_fmt_uzs(rate)}\nYangi kurs: /rate 12750', reply_markup=MAIN_KB)


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_RESET_TODAY)
async def btn_reset(message: types.Message) -> None:
    if not await guard(message):
        return
    await message.answer('Bugungi hisobni 0 dan boshlashni tasdiqlaysizmi? Eski arxiv saqlanadi.', reply_markup=reset_confirm_kb())


@dp.message_handler(lambda m: (m.text or '').strip() == BTN_HELP)
async def btn_help(message: types.Message) -> None:
    if not await guard(message):
        return
    await message.answer(HELP_TEXT, reply_markup=MAIN_KB)

# ============================================================
# CALLBACKS
# ============================================================
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('save:'))
async def cb_save(callback: types.CallbackQuery) -> None:
    token = callback.data.split(':', 1)[1]
    pending = PENDING.pop(token, None)
    if pending is None:
        await callback.answer('Bu preview eskirgan.', show_alert=True)
        return
    batch_id = await save_batch(pending)
    await callback.answer('Saqlandi')
    if callback.message:
        await callback.message.edit_reply_markup()
        await callback.message.answer(f'✅ Saqlandi. Batch #{batch_id}', reply_markup=MAIN_KB)
        fake_message = callback.message
        await send_period_dashboard(fake_message, 'today')


@dp.callback_query_handler(lambda c: c.data and c.data.startswith('cancel:'))
async def cb_cancel(callback: types.CallbackQuery) -> None:
    token = callback.data.split(':', 1)[1]
    PENDING.pop(token, None)
    await callback.answer('Bekor qilindi')
    if callback.message:
        await callback.message.edit_reply_markup()
        await callback.message.answer('❌ Saqlash bekor qilindi.', reply_markup=MAIN_KB)


@dp.callback_query_handler(lambda c: c.data == 'reset_today:confirm')
async def cb_reset_confirm(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else 0
    if callback.message is None:
        await callback.answer()
        return
    reset_at = await create_today_reset(callback.message.chat.id, user_id)
    await callback.answer('Bugungi hisob yangilandi')
    await callback.message.edit_reply_markup()
    await callback.message.answer(
        f'🔄 Bugungi hisob {fmt_dt(reset_at)} dan 0 ga tushirildi. Eski arxiv saqlanib qoldi.',
        reply_markup=MAIN_KB,
    )
    await send_period_dashboard(callback.message, 'today')


@dp.callback_query_handler(lambda c: c.data == 'reset_today:cancel')
async def cb_reset_cancel(callback: types.CallbackQuery) -> None:
    await callback.answer('Bekor qilindi')
    if callback.message:
        await callback.message.edit_reply_markup()
        await callback.message.answer('Reset bekor qilindi.', reply_markup=MAIN_KB)

# ============================================================
# INGESTION
# ============================================================
@dp.message_handler(content_types=types.ContentType.TEXT)
async def ingest(message: types.Message) -> None:
    if not await guard(message):
        return
    text = (message.text or '').strip()
    if not text or text.startswith('/') or text in {
        BTN_TODAY, BTN_MONTH, BTN_BALANCE, BTN_EXPORT, BTN_RECORDS, BTN_UNDO, BTN_RATE, BTN_RESET_TODAY, BTN_HELP, BTN_TEXT_REPORT
    }:
        return

    usd_rate = await get_usd_rate()
    lines = parse_telegram_or_plain_lines(text, user_display_name(message.from_user))
    drafts: List[TxDraft] = []
    skipped = 0

    for line in lines:
        hits = parse_money_hits(line.body, usd_rate)
        if not hits:
            skipped += 1
            continue
        meta = await enrich_note(line.body, hits)
        for hit in hits:
            drafts.append(
                TxDraft(
                    note_date=line.tx_dt,
                    author=line.author,
                    direction=hit.direction,
                    currency=hit.currency,
                    amount_original=hit.amount_original,
                    amount_uzs=hit.amount_uzs,
                    usd_rate=hit.usd_rate,
                    clean_note=meta['clean_note'],
                    category=meta['category'],
                    counterparty=meta['counterparty'],
                    original_text=line.raw_line,
                )
            )

    if not drafts:
        await message.answer('Summa topilmadi. Matn formatini tekshir.', reply_markup=MAIN_KB)
        return

    token = uuid4().hex[:12]
    pending = PendingBatch(
        token=token,
        chat_id=message.chat.id,
        user_id=message.from_user.id if message.from_user else 0,
        source_text=text[:10000],
        created_at=now_local(),
        txs=drafts,
    )
    PENDING[token] = pending

    preview = build_preview_text(pending)
    if skipped:
        preview += f'\n\n⚠️ Summa topilmagan qatorlar: {skipped}'
    await message.answer(preview, reply_markup=save_cancel_kb(token))

# ============================================================
# STARTUP
# ============================================================
async def on_startup(_dp: Dispatcher) -> None:
    await init_db()
    await bot.set_my_commands([
        BotCommand('start', 'Menyu'),
        BotCommand('help', 'Yordam'),
        BotCommand('stats', 'Statistika'),
        BotCommand('records', 'So‘nggi yozuvlar'),
        BotCommand('categories', 'Kategoriya kesimi'),
        BotCommand('rate', 'USD kursi'),
        BotCommand('export', 'Export'),
        BotCommand('undo', 'Oxirgi batchni bekor qilish'),
        BotCommand('delete', 'Yozuvni o‘chirish'),
        BotCommand('reset_today', 'Bugungi hisobni 0 qilish'),
    ])
    logger.info('Bot ishga tushdi')


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError('TELEGRAM_BOT_TOKEN topilmadi')
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


if __name__ == '__main__':
    main()
