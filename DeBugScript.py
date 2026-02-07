import re

file_path = 'moexmomentumbot_sectors.py'

new_method = '''    def send_telegram_message(self, message: str, silent: bool = False, force: bool = False) -> bool:
        """
        Отправка сообщения в Telegram с автоматической разбивкой длинных текстов
        """
        # Проверка лимита частоты отправки
        if force:
            logger.debug(f"📨 Принудительная отправка сообщения (force=True)")
        elif not force and not self.should_send_notification() and not silent:
            logger.debug(f"⏰ Пропускаем оповещение (прошло менее 24 часов)")
            return False
        
        if not self.telegram_token or not self.telegram_chat_id:
            if not silent:
                logger.warning("⚠️ Нет данных для Telegram")
            return False

        # === ЛОГИКА РАЗБИВКИ СООБЩЕНИЙ (Telegram limit ~4096 chars) ===
        messages_to_send = []
        max_len = 4000  # Берем с запасом
        
        if len(message) > max_len:
            logger.info(f"📨 Сообщение длинное ({len(message)} симв.), разбиваем на части...")
            temp_msg = message
            while temp_msg:
                if len(temp_msg) <= max_len:
                    messages_to_send.append(temp_msg)
                    break
                
                # Ищем перенос строки для красивой разбивки
                split_pos = temp_msg.rfind('\\n', 0, max_len)
                if split_pos == -1:
                    split_pos = max_len
                
                chunk = temp_msg[:split_pos]
                messages_to_send.append(chunk)
                temp_msg = temp_msg[split_pos:]
        else:
            messages_to_send = [message]

        # === ОТПРАВКА ЧАСТЕЙ ===
        all_success = True
        
        for i, msg_chunk in enumerate(messages_to_send):
            chunk_success = False
            
            # Если частей много, добавляем паузу между ними
            if i > 0:
                time.sleep(0.5)

            for attempt in range(self.max_telegram_retries):
                try:
                    url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                    data = {
                        "chat_id": self.telegram_chat_id,
                        "text": msg_chunk,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                        "disable_notification": silent
                    }
                    
                    response = requests.post(url, data=data, timeout=10)
                    
                    if response.status_code == 200:
                        if not silent:
                            self.last_notification_time = datetime.now()
                        chunk_success = True
                        break # Успех, выходим из цикла попыток
                        
                    elif response.status_code == 400 and data.get('parse_mode'):
                        # Если ошибка форматирования, пробуем без Markdown
                        logger.warning(f"⚠️ Ошибка Telegram 400 (Part {i+1}). Пробуем без Markdown.")
                        data.pop('parse_mode')
                        response = requests.post(url, data=data, timeout=10)
                        if response.status_code == 200:
                            chunk_success = True
                            break
                    else:
                        if not silent:
                            logger.warning(f"Ошибка Telegram (попытка {attempt+1}): {response.status_code}")
                        
                except Exception as e:
                    if not silent:
                        logger.warning(f"Ошибка отправки Telegram (попытка {attempt+1}): {e}")
                
                if attempt < self.max_telegram_retries - 1:
                    time.sleep(self.telegram_retry_delay)
            
            if not chunk_success:
                all_success = False
                logger.error(f"❌ Не удалось отправить часть сообщения #{i+1}")

        return all_success'''

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Ищем начало функции
    start_marker = '    def send_telegram_message(self, message: str, silent: bool = False, force: bool = False) -> bool:'
    start_idx = content.find(start_marker)
    
    if start_idx == -1:
        print("❌ Не удалось найти метод send_telegram_message.")
    else:
        # Ищем следующую функцию (load_state)
        next_func = '    def load_state(self):'
        end_idx = content.find(next_func, start_idx)
        
        if end_idx == -1:
             print("❌ Не удалось найти конец метода.")
        else:
            # Заменяем
            new_content = content[:start_idx] + new_method + "\n    \n" + content[end_idx:]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            print("✅ Метод отправки Telegram обновлен! Теперь он умеет разбивать длинные сообщения.")

except Exception as e:
    print(f"❌ Ошибка: {e}")
