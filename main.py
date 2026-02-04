async def handle_msg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    try:
        text = u.message.text.strip()
        uid = u.effective_user.id
        log.info(f"Получено сообщение от {uid}: {text}")

        # 1. Попытка синхронизации с Google (самое уязвимое место)
        try:
            user = sync_user(uid)
        except Exception as e:
            log.error(f"Ошибка Google Sheets: {e}")
            await u.message.reply_text("⚠️ Ошибка базы данных. Проверьте настройки таблицы.")
            return

        # 2. Логика ввода даты
        if len(text) == 10 and "." in text:
            sync_user(uid, {"birth": text})
            await u.message.reply_text(f"✅ Дата {text} сохранена! Нажмите 'Мой прогноз'.")
            return

        # 3. Логика прогноза
        if text == "📅 Мой прогноз":
            if not user or not user[4]:
                await u.message.reply_text("Сначала введите дату рождения в формате ДД.ММ.ГГГГ")
                return

            # Здесь расчеты Сюцай
            res = get_calc(user[4], user[12] or "Asia/Almaty")
            
            # ФОРМИРОВАНИЕ СООБЩЕНИЯ (тексты из ваших CSV)
            msg = f"📅 *Прогноз на {res['date']}*\n\n"
            msg += f"🌐 *Общий день:* {res['od']}\n"
            msg += f"📍 *Личный день:* {res['ld']}\n"
            
            await u.message.reply_text(msg, parse_mode="Markdown")

    except Exception as e:
        # Если код упадет, бот пришлет саму ошибку (удобно для отладки)
        log.error(f"Глобальная ошибка: {e}")
        await u.message.reply_text(f"❌ Произошла ошибка в коде: {str(e)}")

# ПРИНУДИТЕЛЬНЫЙ ПЕРЕЗАПУСК ВЕБХУКА
async def setup():
    try:
        await application.initialize()
        await application.start()
        # Сбрасываем и ставим заново, чтобы быть уверенным
        await application.bot.delete_webhook()
        await asyncio.sleep(1) 
        await application.bot.set_webhook(f"{PUBLIC_URL}/webhook")
        log.info("Webhook successfully reset and set!")
    except Exception as e:
        log.error(f"Setup fail: {e}")