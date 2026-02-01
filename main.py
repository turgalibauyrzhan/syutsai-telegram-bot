from telegram.ext import Application, CommandHandler
import os

TOKEN = os.getenv("TELEGRAM_TOKEN")
PUBLIC_URL = os.getenv("PUBLIC_URL")
PORT = int(os.environ.get("PORT", 8080))

async def start(update, context):
    await update.message.reply_text("Я живой")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))

app.run_webhook(
    listen="0.0.0.0",
    port=PORT,
    webhook_url=f"{PUBLIC_URL}/webhook",
)
