import os, json, base64, logging, asyncio
from datetime import datetime, timedelta
import pytz
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
import gspread
from google.oauth2.service_account import Credentials

# Настройки логирования
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Константы
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip('/')
GSHEET_ID = os.getenv("GSHEET_ID")
GOOGLE_SA_JSON_B64 = os.getenv("GOOGLE_SA_JSON_B64")

# --- ТЕКСТОВЫЕ ДАННЫЕ (ИЗ ВАШИХ ФАЙЛОВ) ---
DESC_LG = {
    "1": "✨ Личный год 1: Начало нового цикла. Самый мощный энергетический поток. Рекомендация: Открывай свое дело, бери ответственность. В минусе: Депрессия.",
    "2": "✨ Личный год 2: Построение отношений. Рекомендация: Развивай дипломатию, мягко отпускай старое. В минусе: Болезненные разрывы.",
    "3": "✨ Личный год 3: Анализ и успех. Рекомендация: Действуй через расчет, планируй наперед. В минусе: Лень, азарт.",
    "4": "✨ Личный год 4: Мистика и постановка целей. Рекомендация: Ставь цели, будь креативным. В минусе: Неудовлетворенность.",
    "5": "✨ Личный год 5: Коммуникация и удача. Рекомендация: Расширяй связи, путешествуй. В минусе: Борьба за справедливость.",
    "6": "✨ Личный год 6: Удача и комфорт. Рекомендация: Дари любовь, инвестируй. В минусе: Лень, мстительность.",
    "7": "✨ Личный год 7: Трансформация. Рекомендация: Не начинай новое, занимайся духовным ростом. В минусе: Хаос.",
    "8": "✨ Личный год 8: Труд и обучение. Рекомендация: Покупай недвижимость, учись. В минусе: Усталость.",
    "9": "✨ Личный год 9: Завершение. Рекомендация: Прощай обиды, служи людям. В минусе: Эмоциональность."
}

DESC_LM = {
    "1": "🌙 Месяц 1: Стратегия и планирование. Будь лидером.",
    "2": "🌙 Месяц 2: Дипломатия. Пей воду, не принимай резких решений.",
    "3": "🌙 Месяц 3: Анализ. Структурируй планы, учись.",
    "4": "🌙 Месяц 4: Постановка целей. Избегай иллюзий.",
    "5": "🌙 Месяц 5: Масштабирование. Хорошо для бизнеса и поездок.",
    "6": "🌙 Месяц 6: Любовь и успех. Время для инвестиций и брака.",
    "7": "🌙 Месяц 7: Дисциплина. Либо взлет, либо падение.",
    "8": "🌙 Месяц 8: Труд. Контролируй финансы и здоровье.",
    "9": "🌙 Месяц 9: Благодарность. Подводи итоги, помогай другим."
}

DESC_LD = {
    "1": "📍 Личный день 1: Новые начинания. Будь смелым, реализуй план.",
    "2": "📍 Личный день 2: Дипломатия. Слушай других, налаживай связи.",
    "3": "📍 Личный день 3: Анализ. Избегай азарта, все просчитывай.",
    "4": "📍 Личный день 4: Креатив. Ставь честные цели.",
    "5": "📍 Личный день 5: Масштабирование. Лучший день для торговли.",
    "6": "📍 Личный день 6: Комфорт. Дари тепло близким, создавай уют.",
    "7": "📍 Личный день 7: Трансформация. Дисциплина тела (ходьба).",
    "8": "📍 Личный день 8: Труд. Получай навыки, не бери кредиты.",
    "9": "📍 Личный день 9: Служение. Баня, массаж, отдача долгов."
}

# --- ФУНКЦИИ ---
def reduce9(n):
    while n > 9: n = sum(map(int, str(n)))
    return n

def sync_user(update, birth=None, last_ym=None):
    try:
        user = update.effective_user
        uid = str(user.id)
        creds_info = json.loads(base64.b64decode(GOOGLE_SA_JSON_B64).decode("utf-8"))
        creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        ws = gspread.authorize(creds).open_by_key(GSHEET_ID).worksheet("subscriptions")
        
        rows = ws.get_all_values()
        idx = next((i for i, r in enumerate(rows) if r[0] == uid), -1)
        now_ts = datetime.now(pytz.timezone("Asia/Almaty")).strftime("%d.%m.%Y %H:%M")

        if idx == -1:
            # Создание строки (14 колонок)
            new_row = [uid, "active", "trial", (datetime.now()+timedelta(days=3)).strftime("%d.%m.%Y"), 
                       birth or "", now_ts, now_ts, user.username or "", user.first_name or "", 
                       user.last_name or "", datetime.now().strftime("%d.%m.%Y"), last_ym or "", "Asia/Almaty", ""]
            ws.append_row(new_row)
            return new_row
        else:
            idx += 1
            ws.update_cell(idx, 7, now_ts) # last_seen_at
            if birth: ws.update_cell(idx, 5, birth)
            if last_ym: ws.update_cell(idx, 12, last_ym)
            return ws.row_values(idx)
    except Exception as e:
        log.error(f"GS Error: {e}"); return None

# --- ОБРАБОТЧИКИ ---
async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    sync_user(u)
    await u.message.reply_text("✨ Введите дату рождения (ДД.ММ.ГГГГ):",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📅 Мой прогноз")]], resize_keyboard=True))

async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    text = u.message.text.strip()
    if len(text) == 10 and "." in text:
        sync_user(u, birth=text)
        await u.message.reply_text(f"✅ Дата {text} сохранена! Нажмите кнопку.")
        return

    if text == "📅 Мой прогноз":
        user = sync_user(u)
        if not user or not user[4]:
            await u.message.reply_text("Сначала введите дату рождения!"); return
        
        bd = datetime.strptime(user[4], "%d.%m.%Y")
        now = datetime.now(pytz.timezone("Asia/Almaty"))
        lg, lm, ld, od = reduce9(bd.day+bd.month+now.year), reduce9(reduce9(bd.day+bd.month+now.year)+now.month), reduce9(reduce9(reduce9(bd.day+bd.month+now.year)+now.month)+now.day), reduce9(now.day+now.month+now.year)
        ym_key = now.strftime("%m.%Y")
        
        msg = f"📅 *Прогноз на {now.strftime('%d.%m.%Y')}*\n\n"
        if now.day in [10, 20, 30]: msg += "⚠️ *Внимание!* Неблагоприятная дата (10, 20, 30). Возможен срыв планов.\n\n"
        elif od in [3, 6]: msg += f"🌟 *Общий день {od}:* Успех в делах и покупках!\n\n"
        else: msg += f"🌐 *Общий день:* {od}\n\n"

        if user[11] != ym_key:
            msg += f"{DESC_LG.get(str(lg), '')}\n\n{DESC_LM.get(str(lm), '')}\n\n"
            sync_user(u, last_ym=ym_key)
        
        msg += f"{DESC_LD.get(str(ld), '')}"
        await u.message.reply_text(msg, parse_mode="Markdown")

# --- ЗАПУСК ---
app = Flask(__name__)
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

@app.route("/webhook", methods=["POST"])
def webhook():
    # Используем основной цикл событий
    asyncio.run_coroutine_threadsafe(application.process_update(Update.de_json(request.get_json(force=True), application.bot)), loop)
    return "OK", 200

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # Настройка вебхука
    loop.run_until_complete(application.initialize())
    loop.run_until_complete(application.bot.set_webhook(f"{PUBLIC_URL}/webhook"))
    # Запуск Flask
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))