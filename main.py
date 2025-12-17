import os
import json
import re
import logging
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("syucai")

# =========================
# CONFIG
# =========================
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Almaty"))

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set")

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON", "").strip()
SUBS_SHEET_NAME = os.getenv("SUBS_SHEET_NAME", "subscriptions").strip()

TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))

ADMIN_CHAT_IDS = []
_admin_raw = os.getenv("ADMIN_CHAT_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_CHAT_IDS.append(int(x))

STATUS_ACTIVE = "active"
STATUS_BLOCKED = "blocked"
PLAN_TRIAL = "trial"
PLAN_PREMIUM = "premium"
PLAN_BLOCKED = "blocked"

SUBS_COLUMNS = [
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

MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📅 Прогноз на сегодня")],
        [KeyboardButton("👤 Профиль")],
    ],
    resize_keyboard=True,
)

BIRTHDATE_STATE = 1

# =========================
# TEXTS (как в прошлом коде)
# =========================
TEXTS: Dict[str, Any] = {
    "general_day": {
        "3": "Благоприятный день через анализ, успех. Хороший день для принятия серьёзных решений, подписания договоров и совершения покупок.",
        "6": "Благоприятный день через любовь, успех. Хороший день для принятия решений, для подписания договоров. Делайте покупки, начинайте большие проекты.",
    },
    "unfavorable_day_text": (
        "Сегодня нежелательно начинать новые проекты и события. Есть высокая вероятность обнуления всех результатов ваших действий. "
        "Рекомендуется отложить на другой день крупные покупки, договоры, кредиты и т.д."
    ),
    "personal_year": {
        "1": {
            "title": "Личный год 1. Начало нового цикла.",
            "full": (
                "Это время выбора направления, в котором ты хочешь реализоваться в ближайшие 9 лет. Именно сейчас приходит самый мощный энергетический поток за весь цикл.\n\n"
                "Рекомендации:\n"
                "– Отличный период для открытия собственного дела или запуска нового проекта.\n"
                "– Определи для себя одно ключевое направление и сосредоточься на нем, не распыляясь.\n"
                "– Развивай лидерские качества и учись брать ответственность на себя.\n\n"
                "– Старайся сохранять внутренний позитивный настрой: тогда энергия будет работать на результат."
            ),
        },
        "2": {
            "title": "Личный год 2. Год дипломатии и отношений.",
            "full": (
                "Появляется больше реалистичности и стремления докопаться до сути. Активизируется энергия воспоминаний, усиливается чувственность. "
                "Во всём важно проявлять дипломатию. Серьёзные решения лучше отложить до следующего года. Полезно пить больше воды.\n\n"
                "Может проявляться медлительность, сомнения, усиление депрессивных состояний. Мысли и действия часто направлены на разрыв отношений. "
                "Усиливается желание манипулировать."
            ),
        },
        "3": {
            "title": "Личный год 3. Год анализа и успеха.",
            "full": (
                "В этот период пробуждается аналитическое мышление: человек начинает планировать, подводить итоги и более осознанно подходить к своим действиям. "
                "Это время планирования и ведения учета.\n\n"
                "Рекомендации:\n"
                "– Действуй через анализ и расчет.\n"
                "– В бизнесе и совместных делах выстраивай справедливое и прозрачное управление.\n"
                "– Планируй шаги на день, месяц и год вперед.\n"
                "– Подводи промежуточные итоги, корректируй и обновляй планы по мере необходимости.\n"
                "– Следи за своим временем: куда оно уходит и какие результаты приносит.\n\n"
                "Если энергия года не используется:\n"
                "Могут проявляться лень, азарт, корысть и стремление к быстрой выгоде. В итоге это приводит к разрушению планов и потере ресурсов."
            ),
        },
        "4": {"title": "Личный год 4. Год трансформации и перемен.", "full": "В этот период активно происходят перемены, трансформация личности, переоценка ценностей. Может уйти из жизни что-то дорогое или значимое, к чему вы были привязаны."},
        "5": {"title": "Личный год 5. Год общения и открытых возможностей.", "full": "Хороший период, чтобы заводить новые знакомства, общаться, заниматься бизнесом, делиться мыслями и выкладывать посты в социальных сетях. Активное общение приносит новые возможности, успех, карьерный рост и материальные блага.\n\nВсё тайное становится явным — могут открыться тайны и секреты.\nМожет проявляться беспечность.\n\nГлавное — не вступать в борьбу и сопротивление, а говорить «да» и соглашаться. Тогда энергия возможностей не будет блокироваться."},
        "6": {"title": "Личный год 6. Год любви и успеха.", "full": "Работает энергия любви и счастья. Проявляйте творчество и любовь, ищите креативный подход даже в простых делах. Дарите заботу, говорите близким тёплые слова, проявляйте тактильность и внимание. Старайтесь создавать комфорт для других.\n\nВ минусе может проявляться стремление к лени, мстительности и заботе только о собственном комфорте."},
        "7": {"title": "Личный год 7. Год глубины и обучения.", "full": "Год глубины, обучения и внутреннего роста. Хорошо идти в изучение, прокачивать навыки, усиливать дисциплину и фокус. Подходит для уединения, работы над собой, поиска смысла и нового уровня осознанности."},
        "8": {"title": "Личный год 8. Год ресурсов и денег.", "full": "Год ресурсов, денег и управления. Хорошо заниматься финансами, карьерой, масштабированием, усилением личной силы. Важно действовать системно и держать фокус на результат."},
        "9": {"title": "Личный год 9. Год завершений и очищения.", "full": "Год завершений и очищения. Подходит для закрытия хвостов, завершения проектов, отпускания лишнего. Важно подвести итоги и подготовить почву для нового цикла."},
    },
    "personal_month": {
        "1": {"title": "Личный месяц 1. Месяц стартов и инициатив.", "full": "Месяц стартов и инициатив. Хорошо начинать новые задачи, делать первые шаги, пробовать. Важно не распыляться и двигаться в выбранном направлении."},
        "2": {"title": "Личный месяц 2. Месяц дипломатии и выстраивания отношений.", "full": "Появляется больше реалистичности и стремления докопаться до сути. Активизируется энергия воспоминаний, усиливается чувственность. Во всём важно проявлять дипломатию. Серьёзные решения лучше отложить до следующего месяца. Полезно пить больше воды.\n\nМожет проявляться медлительность, сомнения, усиление депрессивных состояний. Мысли и действия часто направлены на разрыв отношений. Усиливается желание манипулировать."},
        "3": {"title": "Личный месяц 3. Месяц анализа и успеха.", "full": "Месяц анализа и успеха. Хорошо планировать, считать, улучшать процессы, наводить порядок, подводить итоги и корректировать курс."},
        "4": {"title": "Личный месяц 4. Месяц трансформации и перемен.", "full": "Месяц трансформации и перемен. Могут происходить резкие развороты, переоценка, завершения и обновления. Важна гибкость и готовность отпустить старое."},
        "5": {"title": "Личный месяц 5. Месяц общения и возможностей.", "full": "Месяц общения и возможностей. Больше контактов, движений, договорённостей и новых шансов. Хорошо проявляться публично и расширять круг общения."},
        "6": {"title": "Личный месяц 6. Месяц любви и успеха.", "full": "Месяц любви и успеха. Хорошо укреплять отношения, создавать красоту и комфорт, заниматься творчеством и важными решениями."},
        "7": {"title": "Личный месяц 7. Месяц глубины и роста.", "full": "Месяц глубины и роста. Хорошо учиться, углубляться, анализировать, выстраивать личные смыслы и укреплять дисциплину."},
        "8": {"title": "Личный месяц 8. Месяц ресурсов и денег.", "full": "Месяц ресурсов и денег. Хорошо управлять финансами, усиливать доход, строить систему, договариваться о выгодных условиях."},
        "9": {"title": "Личный месяц 9. Месяц завершений и очищения.", "full": "Месяц завершений и очищения. Подходит для закрытия проектов, расхламления, наведения порядка и подготовки к новому этапу."},
    },
    "personal_day": {
        "1": {"title": "Личный день 1. День новых начинаний.", "full": "День новых начинаний. Любое начинание сегодня будет благоприятным и получит поддержку энергии дня."},
        "2": {"title": "Личный день 2. День понимания и дипломатии.", "full": "Проявляйте терпение и понимание... Проживайте день через дипломатию и мягкость."},
        "3": {"title": "Личный день 3. День анализа и планирования.", "full": "Анализируйте каждое действие... Действуйте через холодный анализ — иначе возможны потери."},
        "4": {"title": "Личный день 4. День мистических событий.", "full": "День мистических событий — как положительных, так и отрицательных... Визуализируйте желаемое, позволяйте себе мечтать без ограничений — именно это сегодня даст мощный импульс."},
        "5": {"title": "Личный день 5. День общения и открытых возможностей.", "full": "Хороший день, чтобы заводить новые знакомства... Тогда энергия возможностей не будет блокироваться."},
        "6": {"title": "Личный день 6. День любви и успеха.", "full": "Работает энергия любви и счастья... В минусе может проявляться стремление к лени, мстительности и заботе только о собственном комфорте."},
        "7": {"title": "Личный день 7. День кризиса или осознанной трансформации.", "full": "День кризиса или осознанной трансформации. Хорошо уходить в тишину, анализировать и делать выводы. Важно не конфликтовать, а направлять энергию в осознанные изменения."},
        "8": {"title": "Личный день 8. День ресурсов и денег.", "full": "День ресурсов, денег и управления. Хорошо заниматься финансами, делами, дисциплиной и ответственными решениями."},
        "9": {"title": "Личный день 9. День завершений и очищения.", "full": "День завершений и очищения. Хорошо закрывать хвосты, завершать проекты, отпускать лишнее и подводить итоги."},
    },
}

# =========================
# NUMEROLOGY
# =========================
def _now() -> datetime:
    return datetime.now(TZ)

def _today() -> date:
    return _now().date()

def digits_sum_to_1_9(n: int) -> int:
    while n > 9:
        s = 0
        for ch in str(n):
            s += ord(ch) - 48
        n = s
    return 9 if n == 0 else n

def sum_digits_of_date(d: date) -> int:
    s = 0
    for ch in d.strftime("%d%m%Y"):
        s += ord(ch) - 48
    return digits_sum_to_1_9(s)

def personal_year(birth: date, today: date) -> int:
    s = 0
    for ch in birth.strftime("%d%m"):
        s += ord(ch) - 48
    for ch in today.strftime("%Y"):
        s += ord(ch) - 48
    return digits_sum_to_1_9(s)

def personal_month(py: int, today: date) -> int:
    m_red = digits_sum_to_1_9(sum(int(c) for c in str(today.month)))
    return digits_sum_to_1_9(py + m_red)

def personal_day(pm: int, today: date) -> int:
    dd_red = digits_sum_to_1_9(sum(int(c) for c in str(today.day)))
    return digits_sum_to_1_9(pm + dd_red)

# =========================
# GOOGLE SHEETS (safe init)
# =========================
@dataclass
class UserRec:
    telegram_user_id: int
    status: str
    plan: str
    trial_expires: Optional[date]
    birth_date: Optional[date]
    created_at: datetime
    last_seen_at: datetime
    username: str
    first_name: str
    last_name: str
    registered_on: date
    last_full_ym: str

_gs_ok = False
_gs_client: Optional[gspread.Client] = None
_subs_ws: Optional[gspread.Worksheet] = None

def _parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def gs_init_safe() -> bool:
    global _gs_ok, _gs_client, _subs_ws
    if _gs_ok:
        return True
    if not (GSHEET_ID and GOOGLE_SA_JSON):
        log.warning("Google Sheets disabled: GSHEET_ID/GOOGLE_SA_JSON not set")
        return False
    try:
        sa_info = json.loads(GOOGLE_SA_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        _gs_client = gspread.authorize(creds)
        sh = _gs_client.open_by_key(GSHEET_ID)
        _subs_ws = sh.worksheet(SUBS_SHEET_NAME)

        header = _subs_ws.row_values(1)
        if header != SUBS_COLUMNS:
            _subs_ws.resize(rows=max(_subs_ws.row_count, 2), cols=len(SUBS_COLUMNS))
            _subs_ws.update("A1", [SUBS_COLUMNS])

        _gs_ok = True
        log.info("Google Sheets connected OK")
        return True
    except Exception as e:
        log.warning("Google Sheets not ready: %s", e)
        return False

def gs_find_row(user_id: int) -> Optional[int]:
    assert _subs_ws is not None
    col = _subs_ws.col_values(1)
    for idx, v in enumerate(col[1:], start=2):
        if str(user_id) == str(v).strip():
            return idx
    return None

def gs_get_user(user_id: int) -> Optional[UserRec]:
    if not gs_init_safe():
        return None
    assert _subs_ws is not None
    row_idx = gs_find_row(user_id)
    if not row_idx:
        return None
    row = _subs_ws.row_values(row_idx)
    row += [""] * (len(SUBS_COLUMNS) - len(row))
    data = dict(zip(SUBS_COLUMNS, row))
    created_at = _parse_dt(data["created_at"]) or _now()
    last_seen_at = _parse_dt(data["last_seen_at"]) or _now()
    trial_expires = _parse_date(data["trial_expires"])
    birth_date = _parse_date(data["birth_date"])
    registered_on = _parse_date(data["registered_on"]) or created_at.date()
    return UserRec(
        telegram_user_id=int(data["telegram_user_id"]),
        status=(data["status"] or STATUS_ACTIVE).strip(),
        plan=(data["plan"] or PLAN_TRIAL).strip(),
        trial_expires=trial_expires,
        birth_date=birth_date,
        created_at=created_at,
        last_seen_at=last_seen_at,
        username=(data["username"] or "").strip(),
        first_name=(data["first_name"] or "").strip(),
        last_name=(data["last_name"] or "").strip(),
        registered_on=registered_on,
        last_full_ym=(data["last_full_ym"] or "").strip(),
    )

def gs_upsert_user(update: Update) -> Tuple[Optional[UserRec], bool]:
    if not gs_init_safe():
        return None, False

    assert update.effective_user is not None
    u = update.effective_user
    assert _subs_ws is not None

    row_idx = gs_find_row(u.id)
    now = _now()
    is_new = False

    if not row_idx:
        is_new = True
        registered_on = now.date()
        trial_expires = registered_on + timedelta(days=TRIAL_DAYS)
        values = {
            "telegram_user_id": str(u.id),
            "status": STATUS_ACTIVE,
            "plan": PLAN_TRIAL,
            "trial_expires": trial_expires.strftime("%Y-%m-%d"),
            "birth_date": "",
            "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "username": u.username or "",
            "first_name": u.first_name or "",
            "last_name": u.last_name or "",
            "registered_on": registered_on.strftime("%Y-%m-%d"),
            "last_full_ym": "",
        }
        _subs_ws.append_row([values[c] for c in SUBS_COLUMNS], value_input_option="USER_ENTERED")
        row_idx = gs_find_row(u.id)

    # update last_seen + identity
    if row_idx:
        row_vals = _subs_ws.row_values(row_idx)
        row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
        row_vals[SUBS_COLUMNS.index("last_seen_at")] = now.strftime("%Y-%m-%d %H:%M:%S")
        row_vals[SUBS_COLUMNS.index("username")] = u.username or ""
        row_vals[SUBS_COLUMNS.index("first_name")] = u.first_name or ""
        row_vals[SUBS_COLUMNS.index("last_name")] = u.last_name or ""
        _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")

    rec = gs_get_user(u.id)
    return rec, is_new

def gs_set_birth(user_id: int, bd: date) -> bool:
    if not gs_init_safe():
        return False
    assert _subs_ws is not None
    row_idx = gs_find_row(user_id)
    if not row_idx:
        return False
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    row_vals[SUBS_COLUMNS.index("birth_date")] = bd.strftime("%Y-%m-%d")
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")
    return True

def gs_set_last_full_ym(user_id: int, ym: str) -> None:
    if not gs_init_safe():
        return
    assert _subs_ws is not None
    row_idx = gs_find_row(user_id)
    if not row_idx:
        return
    row_vals = _subs_ws.row_values(row_idx)
    row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
    row_vals[SUBS_COLUMNS.index("last_full_ym")] = ym
    _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")

def gs_block_if_trial_expired(rec: UserRec, today: date) -> UserRec:
    if rec.status == STATUS_ACTIVE and rec.plan == PLAN_TRIAL and rec.trial_expires and today > rec.trial_expires:
        # write block
        if gs_init_safe():
            assert _subs_ws is not None
            row_idx = gs_find_row(rec.telegram_user_id)
            if row_idx:
                row_vals = _subs_ws.row_values(row_idx)
                row_vals += [""] * (len(SUBS_COLUMNS) - len(row_vals))
                row_vals[SUBS_COLUMNS.index("status")] = STATUS_BLOCKED
                row_vals[SUBS_COLUMNS.index("plan")] = PLAN_BLOCKED
                _subs_ws.update(f"A{row_idx}", [row_vals], value_input_option="USER_ENTERED")
        rec.status = STATUS_BLOCKED
        rec.plan = PLAN_BLOCKED
    return rec

def gs_all_users_safe() -> List[UserRec]:
    if not gs_init_safe():
        return []
    assert _subs_ws is not None
    rows = _subs_ws.get_all_values()
    if not rows or rows[0] != SUBS_COLUMNS:
        return []
    out: List[UserRec] = []
    for r in rows[1:]:
        r += [""] * (len(SUBS_COLUMNS) - len(r))
        d = dict(zip(SUBS_COLUMNS, r))
        try:
            uid = int(d["telegram_user_id"])
        except Exception:
            continue
        created_at = _parse_dt(d["created_at"]) or _now()
        last_seen_at = _parse_dt(d["last_seen_at"]) or _now()
        trial_expires = _parse_date(d["trial_expires"])
        birth_date = _parse_date(d["birth_date"])
        registered_on = _parse_date(d["registered_on"]) or created_at.date()
        out.append(
            UserRec(
                telegram_user_id=uid,
                status=(d["status"] or STATUS_ACTIVE).strip(),
                plan=(d["plan"] or PLAN_TRIAL).strip(),
                trial_expires=trial_expires,
                birth_date=birth_date,
                created_at=created_at,
                last_seen_at=last_seen_at,
                username=(d["username"] or "").strip(),
                first_name=(d["first_name"] or "").strip(),
                last_name=(d["last_name"] or "").strip(),
                registered_on=registered_on,
                last_full_ym=(d["last_full_ym"] or "").strip(),
            )
        )
    return out

# =========================
# ACCESS / RULES
# =========================
def has_full_access(rec: UserRec, today: date) -> bool:
    if rec.status != STATUS_ACTIVE:
        return False
    if rec.plan == PLAN_PREMIUM:
        return True
    if rec.plan == PLAN_TRIAL and rec.trial_expires and today <= rec.trial_expires:
        return True
    return False

def should_full_ym(rec: UserRec, today: date) -> bool:
    ym = today.strftime("%Y-%m")
    if rec.last_full_ym == ym:
        return False
    if today.day == 1:
        return True
    if rec.registered_on == today:
        return True
    return False

def parse_birth_date(text: str) -> Optional[date]:
    text = (text or "").strip()
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None

def render_forecast(rec: UserRec, today: date, full_year_month: bool) -> str:
    assert rec.birth_date is not None
    od = sum_digits_of_date(today)
    py = personal_year(rec.birth_date, today)
    pm = personal_month(py, today)
    pd = personal_day(pm, today)

    # OD desc: 10/20/30 -> unfavorable, else only for 3/6
    if today.day in (10, 20, 30):
        od_desc = TEXTS["unfavorable_day_text"]
    else:
        od_desc = TEXTS["general_day"].get(str(od), "")

    lines: List[str] = []
    lines.append(f"📅 Дата: {today.strftime('%d.%m.%Y')}")
    lines.append("")
    if od_desc:
        lines.append(f"🌐 Сегодня Общий день: {od} - {od_desc}")
    else:
        lines.append(f"🌐 Сегодня Общий день: {od}")
    lines.append("")

    y = TEXTS["personal_year"][str(py)]
    m = TEXTS["personal_month"][str(pm)]
    d = TEXTS["personal_day"][str(pd)]

    if full_year_month:
        lines.append(f"🗓 {y['title']}")
        lines.append(y["full"])
        lines.append("")
        lines.append(f"🗓 {m['title']}")
        lines.append(m["full"])
    else:
        lines.append(f"🗓 {y['title']}")
        lines.append(f"🗓 {m['title']}")

    lines.append("")
    lines.append(f"🔢 {d['title']}")
    lines.append(d["full"])

    if rec.plan == PLAN_PREMIUM and rec.status == STATUS_ACTIVE:
        lines.append("")
        lines.append("⭐️ Premium активен: полный прогноз доступен + ежедневка 09:00.")
    elif rec.plan == PLAN_TRIAL and rec.status == STATUS_ACTIVE and rec.trial_expires and today <= rec.trial_expires:
        lines.append("")
        lines.append(f"🎁 Trial активен до {rec.trial_expires.strftime('%d.%m.%Y')}: полный прогноз доступен + ежедневка 09:00.")

    return "\n".join(lines).strip()

def render_profile(rec: Optional[UserRec]) -> str:
    if rec is None:
        return "👤 Профиль\n\nGoogle Sheets недоступен. Проверь GOOGLE_SA_JSON / доступ к таблице."
    bd = rec.birth_date.strftime("%d.%m.%Y") if rec.birth_date else "не задана"
    te = rec.trial_expires.strftime("%d.%m.%Y") if rec.trial_expires else "-"
    return (
        "👤 Профиль\n\n"
        f"ID: {rec.telegram_user_id}\n"
        f"Статус: {rec.status}\n"
        f"План: {rec.plan}\n"
        f"Trial до: {te}\n"
        f"Дата рождения: {bd}\n"
    ).strip()

# =========================
# ADMIN NOTIFY
# =========================
async def notify_admins(app: Application, text: str) -> None:
    if not ADMIN_CHAT_IDS:
        return
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id=admin_id, text=text)
        except Exception:
            log.exception("Failed to notify admin %s", admin_id)

# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    today = _today()
    rec, is_new = gs_upsert_user(update)

    if is_new and rec is not None:
        u = update.effective_user
        await notify_admins(
            context.application,
            f"🆕 Новый пользователь: id={u.id}, username=@{u.username or '-'} {u.first_name or ''} {u.last_name or ''}\n"
            f"Trial до: {(today + timedelta(days=TRIAL_DAYS)).strftime('%d.%m.%Y')}",
        )

    if rec is None:
        await update.message.reply_text(
            "⚠️ База (Google Sheets) недоступна.\n"
            "Проверь GOOGLE_SA_JSON (должен быть валидным JSON одной строкой) и доступ service account к таблице.",
            reply_markup=MENU,
        )
        return ConversationHandler.END

    rec = gs_block_if_trial_expired(rec, today)
    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return ConversationHandler.END

    if rec.birth_date is None:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: 05.03.1994",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MENU,
        )
        return BIRTHDATE_STATE

    full_ym = has_full_access(rec, today) and should_full_ym(rec, today)
    if full_ym:
        gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))

    msg = render_forecast(rec, today, full_ym)
    await update.message.reply_text(msg, reply_markup=MENU)
    return ConversationHandler.END

async def set_birth_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    today = _today()
    rec, _ = gs_upsert_user(update)
    if rec is None:
        await update.message.reply_text("⚠️ Google Sheets недоступен. Попробуйте позже.", reply_markup=MENU)
        return ConversationHandler.END

    rec = gs_block_if_trial_expired(rec, today)
    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return ConversationHandler.END

    bd = parse_birth_date(update.message.text)
    if not bd:
        await update.message.reply_text("Неверный формат. Введите так: 05.03.1994")
        return BIRTHDATE_STATE

    if not gs_set_birth(rec.telegram_user_id, bd):
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return BIRTHDATE_STATE

    rec2 = gs_get_user(rec.telegram_user_id)
    if rec2 is None or rec2.birth_date is None:
        await update.message.reply_text("❌ Не смог сохранить дату рождения. Проверь доступ к Google Sheets.")
        return BIRTHDATE_STATE

    full_ym = has_full_access(rec2, today) and should_full_ym(rec2, today)
    if full_ym:
        gs_set_last_full_ym(rec2.telegram_user_id, today.strftime("%Y-%m"))

    msg = render_forecast(rec2, today, full_ym)
    await update.message.reply_text(msg, reply_markup=MENU)
    return ConversationHandler.END

async def on_menu_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    today = _today()
    rec, _ = gs_upsert_user(update)
    if rec is None:
        await update.message.reply_text("⚠️ Google Sheets недоступен. Попробуйте позже.", reply_markup=MENU)
        return

    rec = gs_block_if_trial_expired(rec, today)
    if rec.status != STATUS_ACTIVE:
        await update.message.reply_text(
            "⛔️ Доступ ограничен.\nTrial закончился или доступ отключён.\nОбратитесь к администратору.",
            reply_markup=MENU,
        )
        return

    if rec.birth_date is None:
        await update.message.reply_text(
            "Введите дату рождения в формате *ДД.ММ.ГГГГ*\nПример: 05.03.1994",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=MENU,
        )
        return

    full_ym = has_full_access(rec, today) and should_full_ym(rec, today)
    if full_ym:
        gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))

    msg = render_forecast(rec, today, full_ym)
    await update.message.reply_text(msg, reply_markup=MENU)

async def on_menu_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rec, _ = gs_upsert_user(update)
    await update.message.reply_text(render_profile(rec), reply_markup=MENU)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled error: %s", context.error)

# =========================
# DAILY BROADCAST via JobQueue
# =========================
async def daily_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    today = _today()

    users = gs_all_users_safe()
    if not users:
        return

    for rec in users:
        rec = gs_block_if_trial_expired(rec, today)
        if rec.status != STATUS_ACTIVE:
            continue
        if rec.birth_date is None:
            continue
        if not has_full_access(rec, today):
            continue

        full_ym = should_full_ym(rec, today)
        if full_ym:
            gs_set_last_full_ym(rec.telegram_user_id, today.strftime("%Y-%m"))

        msg = render_forecast(rec, today, full_ym)
        try:
            await app.bot.send_message(chat_id=rec.telegram_user_id, text=msg, reply_markup=MENU)
        except Exception:
            log.exception("daily_broadcast: failed to send to %s", rec.telegram_user_id)

# =========================
# MAIN
# =========================
def main() -> None:
    # Don't crash if Sheets is broken - log it
    gs_init_safe()

    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={BIRTHDATE_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_birth_date_handler)]},
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.Regex(r"^📅 Прогноз на сегодня$"), on_menu_today))
    app.add_handler(MessageHandler(filters.Regex(r"^👤 Профиль$"), on_menu_profile))
    app.add_error_handler(error_handler)

    # JobQueue daily 09:00
    # If PTB installed without job-queue extras -> job_queue can be None
    if app.job_queue is None:
        log.warning('JobQueue is not available. Install: pip install "python-telegram-bot[job-queue]"')
    else:
        app.job_queue.run_daily(daily_broadcast, time=dtime(9, 0, tzinfo=TZ), name="daily_broadcast")
        log.info("Daily broadcast scheduled at 09:00")

    log.info("Bot started")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
