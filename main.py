import os
import re
import json
import base64
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# LOGGING
# =========================
LOG = logging.getLogger("syucai")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - syucai - %(levelname)s - %(message)s",
)

TZ = ZoneInfo("Asia/Almaty")

# =========================
# ENV / CONFIG
# =========================

def env_first(*names: str) -> str:
    """Return first non-empty env var among names."""
    for n in names:
        v = os.getenv(n, "").strip()
        if v:
            return v
    return ""

TELEGRAM_TOKEN = env_first("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TOKEN")
GSHEET_ID = env_first("GSHEET_ID", "GOOGLE_SHEET_ID")
SUBS_SHEET_NAME = env_first("SUBS_SHEET_NAME", "SHEET_NAME") or "subscriptions"

# Для webhook на Render:
# PUBLIC_URL = https://<твоя-ссылка>.onrender.com  (без / в конце)
PUBLIC_URL = env_first("PUBLIC_URL", "RENDER_EXTERNAL_URL", "SERVICE_URL")

# Путь для webhook — можно любой, но лучше рандомный
WEBHOOK_SECRET_PATH = env_first("WEBHOOK_SECRET_PATH") or "telegram/webhook/8f3b2c1a"
WEBHOOK_PATH = f"/{WEBHOOK_SECRET_PATH.lstrip('/')}"

PORT = int(env_first("PORT") or "10000")

# Google SA json: либо строкой JSON (в одну линию), либо base64
GOOGLE_SA_JSON = env_first("GOOGLE_SA_JSON")
GOOGLE_SA_JSON_B64 = env_first("GOOGLE_SA_JSON_B64")

# =========================
# UI (keyboard)
# =========================

KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📅 Сегодня"), KeyboardButton("🧾 Мой статус")],
        [KeyboardButton("🎂 Изменить ДР"), KeyboardButton("ℹ️ Помощь")],
    ],
    resize_keyboard=True,
)

# =========================
# TEXTS (логика из твоего описания)
# =========================

DATE_RE = re.compile(r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*$")

def reduce_1_9(n: int) -> int:
    """Digital root in 1..9."""
    n = abs(int(n))
    while n > 9:
        s = 0
        for ch in str(n):
            s += ord(ch) - 48
        n = s
    return 9 if n == 0 else n

def parse_ddmmyyyy(s: str) -> Optional[date]:
    m = DATE_RE.match(s or "")
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None

# ---- Общий день: считаем по сумме цифр всей даты.
# Плюс спец-правило: 10/20/30 любого месяца -> текст предупреждения для ОД=9.
OD_WARNING_TEXT = (
    "Сегодня Общий день: 9. Сегодня нежелательно начинать новые проекты, "
    "лучше завершать дела и закрывать хвосты. Перенеси на другой день крупные покупки, "
    "договоры, кредиты и т.д."
)

OD_TEXT_FULL = {
    1: "День инициативы и самостоятельности. Хорош для новых стартов, решений и лидерства. "
       "Действуй напрямую, без лишних согласований.",
    2: "День сотрудничества и дипломатии. Лучше договариваться, слушать, выстраивать контакт. "
       "Не дави — работай мягко.",
    3: "Благоприятный день через анализ, успех и мышление. Отлично подходит для серьезных решений, "
       "подписания договоров и совершения покупок.",
    4: "День порядка и дисциплины. Лучше закрывать хвосты, заниматься документами и рутиной. "
       "Планируй, структурируй, наводи порядок.",
    5: "День общения и движения. Хорош для встреч, переговоров, поездок, продаж и активностей. "
       "Главное — не распыляйся.",
    6: "Благоприятный день через любовь, успех и гармонию. Хорош для семьи, отношений, красоты, "
       "покупок и начала больших проектов. Подходит для подписания договоров.",
    7: "День анализа и уединения. Хорош для обучения, чтения, аналитики, внутренней настройки. "
       "Не перегружай себя шумом.",
    8: "День денег и результата. Хорош для работы, управления, финансовых решений, крупных задач. "
       "Действуй прагматично.",
    9: "День завершения. Закрывай дела, отдавай долги, подводи итоги, освобождай место под новое. "
       "Не стартуй лишнего."
}

OD_TEXT_SHORT = {
    k: v.split(".")[0] + "." for k, v in OD_TEXT_FULL.items()
}

# ---- Личный год/месяц/день: по твоей логике:
# ЛГ = ДР(день)+ДР(месяц)+текущий год
# ЛМ = ЛГ + текущий месяц
# ЛД = ЛМ + текущий день
def calc_personal_year(birth: date, today: date) -> int:
    return reduce_1_9(birth.day + birth.month + sum(int(c) for c in str(today.year)))

def calc_personal_month(py: int, today: date) -> int:
    return reduce_1_9(py + today.month)

def calc_personal_day(pm: int, today: date) -> int:
    return reduce_1_9(pm + today.day)

# Минимальные тексты (чтобы НЕ “пропали” и бот работал всегда).
# Если хочешь — потом заменим на твои “большие” описания 1..9 из твоего файла/методики.
LG_SHORT = {i: f"Личный год {i}." for i in range(1, 10)}
LM_SHORT = {i: f"Личный месяц {i}." for i in range(1, 10)}
LD_SHORT = {i: f"Личный день {i}." for i in range(1, 10)}

LG_FULL = {i: f"Ваш Личный год {i}. (полное описание)" for i in range(1, 10)}
LM_FULL = {i: f"Личный месяц {i}. (полное описание)" for i in range(1, 10)}
LD_FULL = {i: f"Личный день {i}. (полное описание)" for i in range(1, 10)}

# =========================
# GOOGLE SHEETS (старый формат колонок)
# =========================

COLUMNS = [
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
class UserRow:
    telegram_user_id: int
    status: str = "active"
    plan: str = "trial"
    trial_expires: str = ""         # YYYY-MM-DD
    birth_date: str = ""            # DD.MM.YYYY
    created_at: str = ""            # ISO
    last_seen_at: str = ""          # ISO
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    registered_on: str = ""         # YYYY-MM-DD
    last_full_ym: str = ""          # YYYY-MM

class SheetsStore:
    def __init__(self):
        self._ready = False
        self._ws = None

    def ready(self) -> bool:
        return self._ready and self._ws is not None

    def init(self) -> None:
        if not GSHEET_ID:
            LOG.warning("GSHEET_ID is empty; Google Sheets disabled.")
            return

        sa_json = ""
        if GOOGLE_SA_JSON.strip():
            sa_json = GOOGLE_SA_JSON.strip()
        elif GOOGLE_SA_JSON_B64.strip():
            try:
                sa_json = base64.b64decode(GOOGLE_SA_JSON_B64.strip()).decode("utf-8")
            except Exception as e:
                LOG.warning(f"Bad GOOGLE_SA_JSON_B64: {e}")
                return

        if not sa_json:
            LOG.warning("GOOGLE_SA_JSON / GOOGLE_SA_JSON_B64 is empty; Google Sheets disabled.")
            return

        try:
            sa_info = json.loads(sa_json)
        except Exception as e:
            LOG.warning(f"Google Sheets not ready: invalid JSON ({e})")
            return

        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(GSHEET_ID)
            ws = sh.worksheet(SUBS_SHEET_NAME)

            # ensure header
            header = ws.row_values(1)
            if [h.strip() for h in header] != COLUMNS:
                ws.update("A1", [COLUMNS])
                LOG.info("Header updated to old-format columns.")

            self._ws = ws
            self._ready = True
            LOG.info("Google Sheets ready.")
        except Exception as e:
            LOG.warning(f"Google Sheets init failed: {e}")

    def _find_row_index_by_user_id(self, user_id: int) -> Optional[int]:
        if not self.ready():
            return None
        try:
            col = self._ws.col_values(1)  # telegram_user_id
            # header at 1
            for i in range(2, len(col) + 1):
                if str(col[i - 1]).strip() == str(user_id):
                    return i
            return None
        except Exception as e:
            LOG.warning(f"_find_row_index_by_user_id failed: {e}")
            return None

    def get_user(self, user_id: int) -> Optional[UserRow]:
        if not self.ready():
            return None
        idx = self._find_row_index_by_user_id(user_id)
        if not idx:
            return None
        try:
            row = self._ws.row_values(idx)
            data = {COLUMNS[i]: (row[i] if i < len(row) else "") for i in range(len(COLUMNS))}
            return UserRow(
                telegram_user_id=int(data["telegram_user_id"] or user_id),
                status=data.get("status", "active") or "active",
                plan=data.get("plan", "trial") or "trial",
                trial_expires=data.get("trial_expires", "") or "",
                birth_date=data.get("birth_date", "") or "",
                created_at=data.get("created_at", "") or "",
                last_seen_at=data.get("last_seen_at", "") or "",
                username=data.get("username", "") or "",
                first_name=data.get("first_name", "") or "",
                last_name=data.get("last_name", "") or "",
                registered_on=data.get("registered_on", "") or "",
                last_full_ym=data.get("last_full_ym", "") or "",
            )
        except Exception as e:
            LOG.warning(f"get_user failed: {e}")
            return None

    def upsert_user(self, u: UserRow) -> None:
        if not self.ready():
            return
        idx = self._find_row_index_by_user_id(u.telegram_user_id)
        values = [
            str(u.telegram_user_id),
            u.status,
            u.plan,
            u.trial_expires,
            u.birth_date,
            u.created_at,
            u.last_seen_at,
            u.username,
            u.first_name,
            u.last_name,
            u.registered_on,
            u.last_full_ym,
        ]
        try:
            if idx:
                self._ws.update(f"A{idx}:L{idx}", [values])
            else:
                self._ws.append_row(values, value_input_option="USER_ENTERED")
        except Exception as e:
            LOG.warning(f"upsert_user failed: {e}")

STORE = SheetsStore()

# =========================
# BUSINESS RULES (показ полных/кратких)
# =========================

def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()

def today_date() -> date:
    return datetime.now(TZ).date()

def ym(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def should_send_full_for_lg_lm(user: UserRow, today: date) -> bool:
    """Полные ЛГ/ЛМ: в первый раз (когда last_full_ym пуст) и 1-го числа каждого месяца."""
    if today.day == 1:
        return True
    if not (user.last_full_ym or "").strip():
        return True
    # если сменился месяц относительно last_full_ym — тоже шлём полные (чтобы не потерять)
    return user.last_full_ym.strip() != ym(today)

def format_forecast(
    birth: date,
    today: date,
    full_lg_lm: bool,
) -> Tuple[str, int, int, int, int]:
    od = reduce_1_9(sum(int(c) for c in f"{today.day:02d}{today.month:02d}{today.year:04d}"))
    # спец правило 10/20/30
    is_10_20_30 = today.day in (10, 20, 30)

    py = calc_personal_year(birth, today)
    pm = calc_personal_month(py, today)
    pd = calc_personal_day(pm, today)

    # ОД всегда “полное”, но 10/20/30 — предупреждение
    if is_10_20_30:
        od_line = OD_WARNING_TEXT
        od_desc = ""
    else:
        od_line = f"🌐 Общий день (ОД): {od}"
        od_desc = OD_TEXT_FULL.get(od, f"Общий день {od}.")

    # ЛД всегда полное
    ld_full = LD_FULL.get(pd, f"Личный день {pd}.")
    # ЛМ/ЛГ — полные по правилу, иначе кратко
    if full_lg_lm:
        lg_text = LG_FULL.get(py, f"Ваш Личный год {py}.")
        lm_text = LM_FULL.get(pm, f"Личный месяц {pm}.")
    else:
        lg_text = LG_SHORT.get(py, f"Личный год {py}.")
        lm_text = LM_SHORT.get(pm, f"Личный месяц {pm}.")

    text_parts = []
    text_parts.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    text_parts.append(od_line)
    if od_desc:
        text_parts.append(od_desc)

    # блок ЛГ/ЛМ/ЛД
    text_parts.append("")
    text_parts.append(f"🧮 ЛГ / ЛМ / ЛД: {py} / {pm} / {pd}")
    text_parts.append(lg_text)
    text_parts.append(lm_text)
    text_parts.append(ld_full)

    return "\n".join(text_parts).strip(), od, py, pm, pd

# =========================
# TELEGRAM HANDLERS
# =========================

def ensure_env_or_die() -> None:
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN (или TELEGRAM_TOKEN/TOKEN) не задан в Render Env Vars.")
    if not PUBLIC_URL:
        raise ValueError("PUBLIC_URL (https://<service>.onrender.com) не задан в Render Env Vars для webhook.")

async def upsert_from_update(update: Update) -> UserRow:
    """Безопасно обновляем last_seen + профиль пользователя, создаём если нет."""
    user = update.effective_user
    if not user:
        # fallback, чтобы не падало
        uid = 0
        ur = STORE.get_user(uid) or UserRow(telegram_user_id=uid)
        return ur

    uid = user.id
    existing = STORE.get_user(uid) or UserRow(telegram_user_id=uid)

    # created_at / registered_on — фиксируем только при первом создании
    if not existing.created_at:
        existing.created_at = now_iso()
    if not existing.registered_on:
        existing.registered_on = today_date().isoformat()

    existing.last_seen_at = now_iso()
    existing.username = user.username or existing.username or ""
    existing.first_name = user.first_name or existing.first_name or ""
    existing.last_name = user.last_name or existing.last_name or ""

    STORE.upsert_user(existing)
    return existing

def trial_expire_default(created_iso: str) -> str:
    """По умолчанию trial 7 дней с created_at (если надо — меняй)."""
    try:
        d = datetime.fromisoformat(created_iso).date()
    except Exception:
        d = today_date()
    return (d + timedelta(days=7)).isoformat()

async def send_help(update: Update) -> None:
    txt = (
        "Команды:\n"
        "• 📅 Сегодня — прогноз на сегодня\n"
        "• 🎂 Изменить ДР — заново задать дату рождения\n"
        "• 🧾 Мой статус — план/триал\n\n"
        "Можно просто отправить дату рождения в формате ДД.ММ.ГГГГ — прогноз придёт сразу."
    )
    await update.effective_chat.send_message(txt, reply_markup=KB)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)

    # если в таблице нет trial_expires — ставим дефолт
    if u.plan == "trial" and not u.trial_expires:
        u.trial_expires = trial_expire_default(u.created_at)
        STORE.upsert_user(u)

    if u.birth_date and parse_ddmmyyyy(u.birth_date):
        await update.message.reply_text(
            "Ты уже зарегистрирован. Нажми «📅 Сегодня» или отправь новую дату рождения.",
            reply_markup=KB,
        )
        return

    await update.message.reply_text(
        "Привет! Отправь дату рождения в формате ДД.ММ.ГГГГ (например 16.09.1994) — и я сразу дам прогноз на сегодня.",
        reply_markup=KB,
    )

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    if not u.birth_date:
        await update.message.reply_text("Сначала отправь дату рождения ДД.ММ.ГГГГ.", reply_markup=KB)
        return

    b = parse_ddmmyyyy(u.birth_date)
    if not b:
        await update.message.reply_text("Дата рождения в таблице битая. Отправь заново ДД.ММ.ГГГГ.", reply_markup=KB)
        return

    t = today_date()
    full_lg_lm = should_send_full_for_lg_lm(u, t)
    msg, *_ = format_forecast(b, t, full_lg_lm)

    # если отправили полный ЛГ/ЛМ — отметим месяц
    if full_lg_lm:
        u.last_full_ym = ym(t)
        STORE.upsert_user(u)

    await update.message.reply_text(msg, reply_markup=KB)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    plan = u.plan or "trial"
    trial = u.trial_expires or "-"
    status = u.status or "active"
    b = u.birth_date or "-"
    txt = (
        f"🧾 Статус: {status}\n"
        f"💳 План: {plan}\n"
        f"🎁 Trial до: {trial}\n"
        f"🎂 ДР: {b}"
    )
    await update.message.reply_text(txt, reply_markup=KB)

async def cmd_setbirth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = await upsert_from_update(update)
    u.birth_date = ""
    # чтобы “первый раз” сработал снова — очищаем last_full_ym
    u.last_full_ym = ""
    STORE.upsert_user(u)
    await update.message.reply_text("Ок. Отправь дату рождения ДД.ММ.ГГГГ.", reply_markup=KB)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # защита от None
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    u = await upsert_from_update(update)

    # кнопки
    if text == "📅 Сегодня":
        await cmd_today(update, context)
        return
    if text == "🧾 Мой статус":
        await cmd_status(update, context)
        return
    if text == "🎂 Изменить ДР":
        await cmd_setbirth(update, context)
        return
    if text == "ℹ️ Помощь":
        await send_help(update)
        return

    # если прислали дату рождения — сразу сохраняем и сразу даём прогноз
    d = parse_ddmmyyyy(text)
    if d:
        # сохраняем
        u.birth_date = d.strftime("%d.%m.%Y")
        # если первый раз задают ДР — считаем это “первым разом” (полные ЛГ/ЛМ)
        u.last_full_ym = ""  # чтобы full_lg_lm=True сработал гарантированно
        # trial_expires если пустой
        if u.plan == "trial" and not u.trial_expires:
            u.trial_expires = trial_expire_default(u.created_at)

        STORE.upsert_user(u)

        # сразу прогноз
        t = today_date()
        full_lg_lm = True  # “в первый раз” — всё полное
        msg, *_ = format_forecast(d, t, full_lg_lm)

        u.last_full_ym = ym(t)
        STORE.upsert_user(u)

        await update.message.reply_text(msg, reply_markup=KB)
        return

    # неизвестный текст
    await update.message.reply_text(
        "Не понял. Отправь дату рождения ДД.ММ.ГГГГ или нажми «📅 Сегодня».",
        reply_markup=KB,
    )

# =========================
# MAIN (WEBHOOK)
# =========================

def main() -> None:
    ensure_env_or_die()

    # init sheets
    STORE.init()

    LOG.info(f"BOOT ENV: TOKEN_set={bool(TELEGRAM_TOKEN)} GSHEET_ID_set={bool(GSHEET_ID)} "
             f"PUBLIC_URL={PUBLIC_URL} PORT={PORT} WEBHOOK_PATH={WEBHOOK_PATH}")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("setbirth", cmd_setbirth))
    app.add_handler(CommandHandler("help", lambda u, c: send_help(u)))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    # ВАЖНО:
    # run_webhook сам поднимет HTTP сервер (Render увидит открытый порт)
    # и сам выставит webhook в Telegram на url=PUBLIC_URL+WEBHOOK_PATH
    webhook_url = f"{PUBLIC_URL.rstrip('/')}{WEBHOOK_PATH}"
    LOG.info(f"Webhook server listen=0.0.0.0:{PORT} path={WEBHOOK_PATH} => {webhook_url}")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH.lstrip("/"),
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
