import os
import sys
import json
import base64
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)
logger = logging.getLogger("syucai")

KZT_TZ = timezone(timedelta(hours=5))  # Asia/Almaty ~ UTC+5 (без DST)


# ----------------------------
# Config
# ----------------------------
TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
SUBS_SHEET_NAME = os.getenv("SUBS_SHEET_NAME", "subscriptions").strip()

# Админы: "123,456"
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()]

# Render/Webhook base URL:
# 1) WEBHOOK_BASE_URL = https://<your-service>.onrender.com
# или
# 2) RENDER_EXTERNAL_HOSTNAME = <your-service>.onrender.com  (Render часто даёт)
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()

PORT = int(os.getenv("PORT", "10000"))

# Trial rules
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))
DAILY_PUSH_HOUR = int(os.getenv("DAILY_PUSH_HOUR", "9"))
DAILY_PUSH_MINUTE = int(os.getenv("DAILY_PUSH_MINUTE", "0"))

# ----------------------------
# Text dictionaries (замени на твои финальные тексты)
# ----------------------------
TEXT_OD: Dict[int, str] = {
    1: "День перезапуска и обнуления. Не спеши с новыми решениями, избегай крупных обязательств.",
    2: "День дипломатии и баланса. Хорош для переговоров, примирения и аккуратных договорённостей.",
    3: "День энергии и общения. Подходит для выступлений, знакомств и продвижения.",
    4: "День структуры и дисциплины. Закрывай хвосты, наведи порядок, действуй по плану.",
    5: "День перемен. Гибкость, движение, поездки, новые идеи — но без хаоса.",
    6: "День семьи и гармонии. Хорош для дома, заботы, отношений и красоты.",
    7: "День анализа и тишины. Фокус, обучение, внутренняя работа.",
    8: "День ресурсов и денег. Практичность, сделки, рост эффективности.",
    9: "День завершений. Закрывай циклы, подводи итоги, освобождай место новому.",
}

TEXT_LD: Dict[int, str] = {
    1: "ЛД=1 — старт, инициатива, самостоятельность. Действуй первым.",
    2: "ЛД=2 — мягкость, партнёрство, дипломатия. Делай вместе.",
    3: "ЛД=3 — креатив, общение, самовыражение. Покажи себя.",
    4: "ЛД=4 — порядок, дисциплина, фундамент. Делай шаг за шагом.",
    5: "ЛД=5 — перемены, движение, свобода. Пробуй новое.",
    6: "ЛД=6 — забота, отношения, дом. Восстанови гармонию.",
    7: "ЛД=7 — анализ, тишина, фокус, глубина. Не распыляйся.",
    8: "ЛД=8 — ресурсы и деньги. Думай прагматично.",
    9: "ЛД=9 — завершения, прощание со старым. Закрой задачи.",
}

# Краткие описания для ЛГ/ЛМ (как ты и просил: в обычные дни кратко)
TEXT_LG_SHORT: Dict[int, str] = {
    1: "ЛГ=1 — год стартов.",
    2: "ЛГ=2 — год партнёрства.",
    3: "ЛГ=3 — год роста и коммуникации.",
    4: "ЛГ=4 — год дисциплины и фундамента.",
    5: "ЛГ=5 — год перемен.",
    6: "ЛГ=6 — год семьи и гармонии.",
    7: "ЛГ=7 — год глубины и анализа.",
    8: "ЛГ=8 — год денег и результата.",
    9: "ЛГ=9 — год завершений.",
}

TEXT_LM_SHORT: Dict[int, str] = {
    1: "ЛМ=1 — месяц стартов.",
    2: "ЛМ=2 — месяц баланса и отношений.",
    3: "ЛМ=3 — месяц общения.",
    4: "ЛМ=4 — месяц порядка.",
    5: "ЛМ=5 — месяц перемен.",
    6: "ЛМ=6 — месяц семьи.",
    7: "ЛМ=7 — месяц глубины.",
    8: "ЛМ=8 — месяц денег.",
    9: "ЛМ=9 — месяц завершений.",
}


# ----------------------------
# Helpers: stable GOOGLE_SA_JSON parser
# ----------------------------
def _looks_like_base64(s: str) -> bool:
    if len(s) < 20:
        return False
    # часто base64 начинается на "ewog" (pretty JSON) или "eyJ" (compact JSON)
    if s.startswith(("ewog", "eyJ", "ewo", "e30", "e1")):
        return True
    # грубая эвристика: только base64-символы
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r")
    return all(ch in allowed for ch in s)


def load_google_sa_json() -> Dict[str, Any]:
    """
    Поддерживает:
    - GOOGLE_SA_JSON: plain JSON
    - GOOGLE_SA_JSON: base64(JSON)
    - GOOGLE_SA_JSON_B64: base64(JSON) (если хочешь хранить отдельно)
    Никогда не падает "тихо": логирует причину.
    """
    raw_b64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()
    raw = os.getenv("GOOGLE_SA_JSON", "").strip()

    if raw_b64:
        try:
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            logger.error("GOOGLE_SA_JSON_B64 decode failed: %s", e)
            raise

    if not raw:
        raise ValueError("GOOGLE_SA_JSON is empty")

    # 1) пробуем как JSON
    try:
        return json.loads(raw)
    except Exception:
        pass

    # 2) пробуем как base64(JSON)
    if _looks_like_base64(raw):
        try:
            decoded = base64.b64decode(raw).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            logger.error("GOOGLE_SA_JSON base64 decode failed: %s", e)
            raise

    # 3) последний шанс: иногда в ENV ломают переносы/экранирование
    # (например, вставили JSON с raw newline внутри строки)
    # Тут уже честно — не магия: отдадим понятную ошибку.
    raise ValueError("GOOGLE_SA_JSON is not valid JSON and not valid base64(JSON)")


def make_gspread_client() -> gspread.Client:
    sa = load_google_sa_json()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------
# Google Sheets storage (subscriptions)
# ----------------------------
EXPECTED_HEADERS = [
    "telegram_user_id",
    "status",
    "plan",
    "trial_expires",
    "birth_date",
    "created_at",
    "last_seen_at",
    "username",
    "first_name",
    "last_name",
    "registered_on",
    "last_full_ym",
]

@dataclass
class SubRow:
    telegram_user_id: int
    status: str
    plan: str
    trial_expires: str
    birth_date: str
    created_at: str
    last_seen_at: str
    username: str
    first_name: str
    last_name: str
    registered_on: str
    last_full_ym: str


class SheetStore:
    def __init__(self):
        self.client: Optional[gspread.Client] = None
        self.sheet = None
        self.ws = None
        self.headers: List[str] = []

    def ready(self) -> bool:
        return self.ws is not None

    def init(self) -> None:
        if not GSHEET_ID:
            raise ValueError("GSHEET_ID is empty")
        self.client = make_gspread_client()
        self.sheet = self.client.open_by_key(GSHEET_ID)
        self.ws = self.sheet.worksheet(SUBS_SHEET_NAME)

        self.headers = [h.strip() for h in self.ws.row_values(1)]
        missing = [h for h in EXPECTED_HEADERS if h not in self.headers]
        if missing:
            raise ValueError(f"subscriptions header missing columns: {missing}")

    def _row_to_dicts(self) -> List[Dict[str, str]]:
        values = self.ws.get_all_values()
        if not values or len(values) < 2:
            return []
        hdr = values[0]
        out = []
        for r in values[1:]:
            d = {hdr[i]: (r[i] if i < len(r) else "") for i in range(len(hdr))}
            out.append(d)
        return out

    def get_user(self, user_id: int) -> Optional[Dict[str, str]]:
        rows = self._row_to_dicts()
        for d in rows:
            if str(d.get("telegram_user_id", "")).strip() == str(user_id):
                return d
        return None

    def upsert_user(self, sr: SubRow) -> None:
        # find row index
        rows = self.ws.get_all_values()
        hdr = rows[0]
        target_idx = None
        for i, r in enumerate(rows[1:], start=2):
            if len(r) > 0 and str(r[hdr.index("telegram_user_id")]).strip() == str(sr.telegram_user_id):
                target_idx = i
                break

        data = {
            "telegram_user_id": str(sr.telegram_user_id),
            "status": sr.status,
            "plan": sr.plan,
            "trial_expires": sr.trial_expires,
            "birth_date": sr.birth_date,
            "created_at": sr.created_at,
            "last_seen_at": sr.last_seen_at,
            "username": sr.username,
            "first_name": sr.first_name,
            "last_name": sr.last_name,
            "registered_on": sr.registered_on,
            "last_full_ym": sr.last_full_ym,
        }

        row_values = [data.get(col, "") for col in hdr]

        if target_idx is None:
            self.ws.append_row(row_values, value_input_option="USER_ENTERED")
        else:
            # update entire row
            self.ws.update(f"A{target_idx}:{chr(64+len(hdr))}{target_idx}", [row_values])

    def set_plan(self, user_id: int, status: str, plan: str, trial_expires: str = "") -> None:
        d = self.get_user(user_id)
        if not d:
            return
        now = now_kzt().isoformat(sep=" ", timespec="seconds")
        sr = SubRow(
            telegram_user_id=user_id,
            status=status,
            plan=plan,
            trial_expires=trial_expires or d.get("trial_expires", ""),
            birth_date=d.get("birth_date", ""),
            created_at=d.get("created_at", now),
            last_seen_at=now,
            username=d.get("username", ""),
            first_name=d.get("first_name", ""),
            last_name=d.get("last_name", ""),
            registered_on=d.get("registered_on", ""),
            last_full_ym=d.get("last_full_ym", ""),
        )
        self.upsert_user(sr)

    def list_active_users_for_daily(self) -> List[int]:
        rows = self._row_to_dicts()
        ids: List[int] = []
        today = date.today()
        for d in rows:
            try:
                uid = int(d.get("telegram_user_id", "0"))
            except Exception:
                continue
            status = (d.get("status") or "").strip().lower()
            plan = (d.get("plan") or "").strip().lower()
            if status == "blocked":
                continue

            if plan == "premium":
                ids.append(uid)
                continue

            if plan == "trial":
                # только если ещё не истёк
                te = (d.get("trial_expires") or "").strip()
                if te:
                    try:
                        exp = datetime.fromisoformat(te).date()
                        if today <= exp:
                            ids.append(uid)
                    except Exception:
                        pass
        return ids


store = SheetStore()


# ----------------------------
# Numerology logic (простая и предсказуемая)
# ----------------------------
def digit_sum(n: int) -> int:
    s = 0
    for ch in str(abs(n)):
        s += ord(ch) - 48
    return s

def reduce_1_9(n: int) -> int:
    n = abs(n)
    while n > 9:
        n = digit_sum(n)
    return n if n != 0 else 9

def now_kzt() -> datetime:
    return datetime.now(tz=KZT_TZ)

def calc_general_day(d: date) -> int:
    # Общий день: сумма дня+месяца+года → редукция 1..9
    return reduce_1_9(d.day + d.month + d.year)

def calc_personal_year(bd: date, today: date) -> int:
    return reduce_1_9(bd.day + bd.month + today.year)

def calc_personal_month(py: int, today: date) -> int:
    return reduce_1_9(py + today.month)

def calc_personal_day(pm: int, today: date) -> int:
    return reduce_1_9(pm + today.day)

def ym_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


# ----------------------------
# Access logic
# ----------------------------
def parse_birth_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    # accepted: YYYY-MM-DD or DD.MM.YYYY
    try:
        if "-" in s:
            return datetime.fromisoformat(s).date()
    except Exception:
        pass
    try:
        if "." in s:
            dd, mm, yy = s.split(".")
            return date(int(yy), int(mm), int(dd))
    except Exception:
        return None
    return None

def trial_is_active(user_row: Dict[str, str], today: date) -> bool:
    if (user_row.get("plan") or "").strip().lower() != "trial":
        return False
    if (user_row.get("status") or "").strip().lower() == "blocked":
        return False
    te = (user_row.get("trial_expires") or "").strip()
    if not te:
        return False
    try:
        exp = datetime.fromisoformat(te).date()
        return today <= exp
    except Exception:
        return False

def premium_is_active(user_row: Dict[str, str]) -> bool:
    return (user_row.get("plan") or "").strip().lower() == "premium" and (user_row.get("status") or "").strip().lower() != "blocked"

def should_full_message(user_row: Dict[str, str], today: date) -> bool:
    """
    - Premium: всегда full
    - Trial: full только если trial активен:
        - В первый день после регистрации: full
        - дальше: short (как ты просил)
      Но: ты попросил "полный доступ 3 дня как премиум" — это про функции,
      а формат текста ты хотел: 1-й день полный, дальше короткий. Так и делаем.
    """
    if premium_is_active(user_row):
        return True
    if trial_is_active(user_row, today):
        reg = (user_row.get("registered_on") or "").strip()
        if reg:
            try:
                rdate = datetime.fromisoformat(reg).date()
                return (today == rdate)
            except Exception:
                pass
        # если нет registered_on — считаем первый день как "сегодня"
        return True
    return False

def ensure_trial_expired_autoblock(user_row: Dict[str, str], today: date) -> Tuple[bool, Optional[str]]:
    """
    Возвращает (blocked_now, reason)
    """
    plan = (user_row.get("plan") or "").strip().lower()
    status = (user_row.get("status") or "").strip().lower()
    if plan != "trial" or status == "blocked":
        return (False, None)
    te = (user_row.get("trial_expires") or "").strip()
    if not te:
        return (False, None)
    try:
        exp = datetime.fromisoformat(te).date()
        if today > exp:
            return (True, "trial expired")
    except Exception:
        return (False, None)
    return (False, None)


# ----------------------------
# Message format
# ----------------------------
def make_forecast_message(today: date, bd: date, full: bool) -> str:
    od = calc_general_day(today)
    py = calc_personal_year(bd, today)
    pm = calc_personal_month(py, today)
    pd = calc_personal_day(pm, today)

    od_text = TEXT_OD.get(od, f"ОД={od}")
    ld_text = TEXT_LD.get(pd, f"ЛД={pd}")
    lg_text = TEXT_LG_SHORT.get(py, f"ЛГ={py}")
    lm_text = TEXT_LM_SHORT.get(pm, f"ЛМ={pm}")

    if full:
        # Полное: ОД и ЛД расширенно, ЛГ и ЛМ кратко (как ты просил)
        return (
            f"📅 Дата: {today.strftime('%d.%m.%Y')}\n\n"
            f"🌐 Общий день: {od}\n{od_text}\n\n"
            f"🗓 Личный год: {py}\n{lg_text}\n"
            f"🗓 Личный месяц: {pm}\n{lm_text}\n\n"
            f"🔢 Личный день: {pd}\n{ld_text}\n"
        )

    # Короткая версия (после 1-го дня trial)
    return (
        f"📅 {today.strftime('%d.%m.%Y')}\n"
        f"🌐 ОД {od}: {od_text}\n"
        f"🗓 ЛГ {py}: {lg_text}\n"
        f"🗓 ЛМ {pm}: {lm_text}\n"
        f"🔢 ЛД {pd}: {ld_text}\n"
    )


# ----------------------------
# Admin notify
# ----------------------------
async def notify_admins(app: Application, text: str) -> None:
    if not ADMIN_IDS:
        return
    for aid in ADMIN_IDS:
        try:
            await app.bot.send_message(chat_id=aid, text=text)
        except Exception as e:
            logger.warning("Failed notify admin %s: %s", aid, e)


# ----------------------------
# Handlers
# ----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    uid = user.id
    now = now_kzt().isoformat(sep=" ", timespec="seconds")
    today = now_kzt().date()

    # ensure sheet row
    if store.ready():
        d = store.get_user(uid)
        if not d:
            # new user -> create trial 3 days full-access (functionally)
            trial_expires = (today + timedelta(days=TRIAL_DAYS - 1)).isoformat()
            sr = SubRow(
                telegram_user_id=uid,
                status="active",
                plan="trial",
                trial_expires=trial_expires,
                birth_date="",
                created_at=now,
                last_seen_at=now,
                username=user.username or "",
                first_name=user.first_name or "",
                last_name=user.last_name or "",
                registered_on=today.isoformat(),
                last_full_ym="",
            )
            store.upsert_user(sr)
            await notify_admins(
                context.application,
                f"🆕 Новый пользователь: {uid} @{user.username or '-'} {user.first_name or ''} {user.last_name or ''}\n"
                f"plan=trial until {trial_expires}",
            )
        else:
            # update last seen + profile fields
            sr = SubRow(
                telegram_user_id=uid,
                status=d.get("status", "active"),
                plan=d.get("plan", "trial"),
                trial_expires=d.get("trial_expires", ""),
                birth_date=d.get("birth_date", ""),
                created_at=d.get("created_at", now),
                last_seen_at=now,
                username=user.username or d.get("username", "") or "",
                first_name=user.first_name or d.get("first_name", "") or "",
                last_name=user.last_name or d.get("last_name", "") or "",
                registered_on=d.get("registered_on", "") or today.isoformat(),
                last_full_ym=d.get("last_full_ym", "") or "",
            )
            store.upsert_user(sr)

    await update.message.reply_text(
        "Привет! 👋\n\n"
        "Команды:\n"
        "/setbd DD.MM.YYYY — задать дату рождения\n"
        "/today — прогноз на сегодня\n"
        "/status — статус доступа\n"
    )

async def setbd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    uid = user.id

    if not store.ready():
        await update.message.reply_text("⚠️ Google Sheets пока недоступен. Попробуй позже.")
        return

    if not context.args:
        await update.message.reply_text("Формат: /setbd DD.MM.YYYY (например /setbd 15.03.1995)")
        return

    bd_raw = context.args[0].strip()
    bd = parse_birth_date(bd_raw)
    if not bd:
        await update.message.reply_text("Не понял дату. Формат: DD.MM.YYYY или YYYY-MM-DD")
        return

    d = store.get_user(uid)
    if not d:
        await update.message.reply_text("Сначала нажми /start")
        return

    now = now_kzt().isoformat(sep=" ", timespec="seconds")
    today = now_kzt().date()

    sr = SubRow(
        telegram_user_id=uid,
        status=d.get("status", "active"),
        plan=d.get("plan", "trial"),
        trial_expires=d.get("trial_expires", ""),
        birth_date=bd.isoformat(),
        created_at=d.get("created_at", now),
        last_seen_at=now,
        username=user.username or d.get("username", "") or "",
        first_name=user.first_name or d.get("first_name", "") or "",
        last_name=user.last_name or d.get("last_name", "") or "",
        registered_on=d.get("registered_on", "") or today.isoformat(),
        last_full_ym=d.get("last_full_ym", "") or "",
    )
    store.upsert_user(sr)
    await update.message.reply_text(f"✅ Дата рождения сохранена: {bd.strftime('%d.%m.%Y')}")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    uid = user.id

    if not store.ready():
        await update.message.reply_text("⚠️ Google Sheets пока недоступен.")
        return

    d = store.get_user(uid)
    if not d:
        await update.message.reply_text("Сначала нажми /start")
        return

    today = now_kzt().date()
    blocked_now, _ = ensure_trial_expired_autoblock(d, today)
    if blocked_now:
        store.set_plan(uid, status="blocked", plan=d.get("plan", "trial"), trial_expires=d.get("trial_expires", ""))
        await update.message.reply_text("⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.")
        return

    plan = (d.get("plan") or "").strip()
    status = (d.get("status") or "").strip()
    te = (d.get("trial_expires") or "").strip()
    msg = f"📌 Статус: {status}\n📦 План: {plan}"
    if plan.lower() == "trial" and te:
        msg += f"\n⏳ Trial до: {te}"
    await update.message.reply_text(msg)

async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    uid = user.id
    today = now_kzt().date()

    if not store.ready():
        await update.message.reply_text("⚠️ Google Sheets пока недоступен.")
        return

    d = store.get_user(uid)
    if not d:
        await update.message.reply_text("Сначала нажми /start")
        return

    # auto-block after trial
    blocked_now, _ = ensure_trial_expired_autoblock(d, today)
    if blocked_now:
        store.set_plan(uid, status="blocked", plan=d.get("plan", "trial"), trial_expires=d.get("trial_expires", ""))
        await update.message.reply_text("⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.")
        return

    bd = parse_birth_date(d.get("birth_date", ""))
    if not bd:
        await update.message.reply_text("Сначала задай дату рождения: /setbd DD.MM.YYYY")
        return

    full = should_full_message(d, today)
    msg = make_forecast_message(today, bd, full=full)

    # отметить, что в этом месяце уже был full (для логики можно расширять)
    now = now_kzt().isoformat(sep=" ", timespec="seconds")
    sr = SubRow(
        telegram_user_id=uid,
        status=d.get("status", "active"),
        plan=d.get("plan", "trial"),
        trial_expires=d.get("trial_expires", ""),
        birth_date=d.get("birth_date", ""),
        created_at=d.get("created_at", now),
        last_seen_at=now,
        username=user.username or d.get("username", "") or "",
        first_name=user.first_name or d.get("first_name", "") or "",
        last_name=user.last_name or d.get("last_name", "") or "",
        registered_on=d.get("registered_on", "") or today.isoformat(),
        last_full_ym=ym_key(today) if full else (d.get("last_full_ym", "") or ""),
    )
    store.upsert_user(sr)

    # отметки про доступ
    plan = (d.get("plan") or "").strip().lower()
    if plan == "premium":
        msg += "\n⭐️ Premium активен: полный прогноз доступен + ежедневка 09:00."
    elif plan == "trial":
        msg += f"\n🧪 Trial активен до {d.get('trial_expires','')}: доступ как Premium (формат текста: 1-й день полный, дальше коротко)."

    await update.message.reply_text(msg)

# ----------------------------
# Daily broadcast (09:00 KZT)
# ----------------------------
async def daily_broadcast(app: Application) -> None:
    if not store.ready():
        logger.warning("Daily broadcast skipped: Google Sheets not ready")
        return

    ids = store.list_active_users_for_daily()
    if not ids:
        return

    today = now_kzt().date()
    sent = 0
    for uid in ids:
        d = store.get_user(uid)
        if not d:
            continue

        blocked_now, _ = ensure_trial_expired_autoblock(d, today)
        if blocked_now:
            store.set_plan(uid, status="blocked", plan=d.get("plan", "trial"), trial_expires=d.get("trial_expires", ""))
            continue

        bd = parse_birth_date(d.get("birth_date", ""))
        if not bd:
            continue

        full = should_full_message(d, today)
        msg = "☀️ Ежедневка 09:00\n\n" + make_forecast_message(today, bd, full=full)

        try:
            await app.bot.send_message(chat_id=uid, text=msg)
            sent += 1
        except Exception as e:
            logger.warning("Daily send failed uid=%s: %s", uid, e)

    logger.info("Daily broadcast done. sent=%s", sent)


# ----------------------------
# Error handler
# ----------------------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error: %s", context.error)

# ----------------------------
# Scheduler init (must run inside event loop)
# ----------------------------
scheduler = AsyncIOScheduler(timezone=KZT_TZ)

async def post_init(app: Application) -> None:
    # init Google Sheets
    try:
        store.init()
        logger.info("Google Sheets ready: sheet=%s ws=%s", GSHEET_ID[:6] + "...", SUBS_SHEET_NAME)
    except Exception as e:
        logger.warning("Google Sheets not ready: %s", e)

    # schedule daily
    try:
        scheduler.remove_all_jobs()
        scheduler.add_job(
            lambda: app.create_task(daily_broadcast(app)),
            trigger=CronTrigger(hour=DAILY_PUSH_HOUR, minute=DAILY_PUSH_MINUTE),
            id="daily_broadcast",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Daily broadcast scheduled at %02d:%02d", DAILY_PUSH_HOUR, DAILY_PUSH_MINUTE)
    except Exception as e:
        logger.error("Scheduler failed: %s", e)

# ----------------------------
# Webhook bootstrap
# ----------------------------
def compute_webhook_url() -> str:
    base = WEBHOOK_BASE_URL
    if not base and RENDER_EXTERNAL_HOSTNAME:
        base = f"https://{RENDER_EXTERNAL_HOSTNAME}"
    base = (base or "").rstrip("/")
    if not base:
        raise ValueError("WEBHOOK_BASE_URL is empty and RENDER_EXTERNAL_HOSTNAME is empty. Need external base URL.")
    return base

def main() -> None:
    if not TOKEN:
        logger.error("TELEGRAM_TOKEN is empty")
        sys.exit(1)

    # webhook path: use token as secret path
    url_path = TOKEN

    logger.info(
        "BOOT ENV: TOKEN_set=%s GSHEET_ID_set=%s GOOGLE_SA_JSON_len=%s GOOGLE_SA_JSON_B64_len=%s",
        bool(TOKEN),
        bool(GSHEET_ID),
        len(os.getenv("GOOGLE_SA_JSON", "") or ""),
        len(os.getenv("GOOGLE_SA_JSON_B64", "") or ""),
    )

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setbd", setbd))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_error_handler(on_error)

    webhook_base = compute_webhook_url()
    webhook_url = f"{webhook_base}/{url_path}"

    logger.info("Starting webhook server on 0.0.0.0:%s path=/%s", PORT, url_path)
    logger.info("Webhook URL will be set to: %s", webhook_url)

    # run_webhook: no polling => no 409 conflicts
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=url_path,
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
