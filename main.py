# --- WEBHOOK LOGIC ---
app = Flask(__name__)

# Инициализируем приложение бота глобально
application = Application.builder().token(TELEGRAM_TOKEN).build()
application.add_handler(CommandHandler("start", start))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        try:
            update_data = request.get_json(force=True)
            update = Update.de_json(update_data, application.bot)
            
            # Создаем новый цикл для каждого запроса — это решит проблему "засыпания"
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(application.process_update(update))
            finally:
                loop.close()
                
        except Exception as e:
            log.error(f"Ошибка при обработке апдейта: {e}")
            
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    return "Бот работает!", 200

if __name__ == "__main__":
    # Разовая инициализация бота перед запуском Flask
    setup_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(setup_loop)
    try:
        setup_loop.run_until_complete(application.initialize())
        setup_loop.run_until_complete(application.bot.set_webhook(f"{PUBLIC_URL}/webhook"))
        log.info(f"Вебхук успешно установлен на {PUBLIC_URL}/webhook")
    finally:
        setup_loop.close()

    # Запуск сервера
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)