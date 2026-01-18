import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BCS_Test_Bot')

# Загрузка переменных окружения
load_dotenv()

class BCSConnectionTester:
    """Минимальный бот для проверки подключения к BCS API"""
    
    def __init__(self):
        self.base_url = "https://trade-api.bcs.ru"
        self.api_key = os.getenv('BCS_API_KEY')
        self.api_secret = os.getenv('BCS_API_SECRET')
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Проверка наличия ключей
        self.check_env_variables()
        
        # Сессия для запросов
        self.session = requests.Session()
        self.access_token = None
    
    def check_env_variables(self):
        """Проверка наличия переменных окружения"""
        missing_vars = []
        
        if not self.api_key:
            missing_vars.append('BCS_API_KEY')
        if not self.api_secret:
            missing_vars.append('BCS_API_SECRET')
        if not self.telegram_token:
            missing_vars.append('TELEGRAM_TOKEN')
        if not self.telegram_chat_id:
            missing_vars.append('TELEGRAM_CHAT_ID')
        
        if missing_vars:
            logger.error(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
            logger.error("Добавьте их в файл .env:")
            logger.error("BCS_API_KEY=ваш_ключ")
            logger.error("BCS_API_SECRET=ваш_секрет")
            logger.error("TELEGRAM_TOKEN=токен_бота")
            logger.error("TELEGRAM_CHAT_ID=id_чата")
            raise ValueError(f"Отсутствуют переменные окружения: {missing_vars}")
        
        logger.info("✅ Все переменные окружения найдены")
    
    def authenticate(self):
        """Аутентификация в BCS API"""
        try:
            logger.info("🔑 Попытка аутентификации в BCS API...")
            
            auth_url = f"{self.base_url}/oauth/token"
            
            # Basic Auth для получения токена
            auth_string = f"{self.api_key}:{self.api_secret}"
            auth_encoded = requests.utils.quote(auth_string)
            
            headers = {
                'Authorization': f'Basic {auth_encoded}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            data = {
                'grant_type': 'client_credentials',
                'scope': 'read'
            }
            
            response = self.session.post(auth_url, headers=headers, data=data, timeout=10)
            
            if response.status_code == 200:
                auth_data = response.json()
                self.access_token = auth_data.get('access_token')
                
                if self.access_token:
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/json'
                    })
                    logger.info("✅ Аутентификация успешна")
                    return True
                else:
                    logger.error("❌ Токен не получен в ответе")
                    logger.error(f"Ответ: {auth_data}")
                    return False
            else:
                logger.error(f"❌ Ошибка аутентификации: {response.status_code}")
                logger.error(f"Ответ: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка при аутентификации: {e}")
            return False
    
    def test_connection(self):
        """Тестовый запрос к BCS API"""
        try:
            logger.info("📡 Тестовый запрос к BCS API...")
            
            # Пробуем несколько endpoints для проверки
            
            # 1. Проверка получения инструментов
            test_url = f"{self.base_url}/api/v1/instruments"
            params = {
                'symbol': 'SBER',
                'market': 'MOEX',
                'board': 'TQBR'
            }
            
            response = self.session.get(test_url, params=params, timeout=10)
            
            logger.info(f"📊 Endpoint: {test_url}")
            logger.info(f"📊 Код ответа: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Тестовый запрос успешен")
                logger.info(f"📋 Ответ: {json.dumps(data, indent=2)[:500]}...")
                return True
            else:
                logger.error(f"❌ Тестовый запрос неуспешен")
                logger.error(f"Ответ: {response.text[:500]}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка тестового запроса: {e}")
            return False
    
    def send_telegram_message(self, message):
        """Отправка сообщения в Telegram"""
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                "chat_id": self.telegram_chat_id,
                "text": message,
                "parse_mode": "Markdown"
            }
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Сообщение отправлено в Telegram")
                return True
            else:
                logger.error(f"❌ Ошибка отправки в Telegram: {response.status_code}")
                logger.error(f"Ответ: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка отправки Telegram: {e}")
            return False
    
    def run_test(self):
        """Запуск всех проверок"""
        logger.info("=" * 50)
        logger.info("🚀 ЗАПУСК ПРОВЕРКИ ПОДКЛЮЧЕНИЯ К BCS API")
        logger.info("=" * 50)
        
        # Сообщение в Telegram о начале проверки
        start_msg = f"*🚀 Начало проверки подключения BCS API*\nВремя: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}"
        self.send_telegram_message(start_msg)
        
        # Проверка 1: Аутентификация
        auth_result = self.authenticate()
        
        # Проверка 2: Тестовый запрос
        test_result = False
        if auth_result:
            test_result = self.test_connection()
        
        # Формирование итогового сообщения
        if auth_result and test_result:
            result_msg = (
                f"*✅ ПРОВЕРКА ПРОЙДЕНА УСПЕШНО*\n\n"
                f"• Аутентификация: ✅ Успешно\n"
                f"• Тестовый запрос: ✅ Успешно\n"
                f"• Время проверки: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}\n\n"
                f"BCS API доступен для работы."
            )
            logger.info("✅ Все проверки пройдены успешно")
        else:
            result_msg = (
                f"*❌ ПРОВЕРКА НЕ ПРОЙДЕНА*\n\n"
                f"• Аутентификация: {'✅ Успешно' if auth_result else '❌ Ошибка'}\n"
                f"• Тестовый запрос: {'✅ Успешно' if test_result else '❌ Ошибка'}\n"
                f"• Время проверки: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}\n\n"
                f"Проверьте настройки подключения к BCS API."
            )
            logger.error("❌ Проверка не пройдена")
        
        # Отправка итогового сообщения
        self.send_telegram_message(result_msg)
        
        logger.info("=" * 50)
        logger.info("🏁 ПРОВЕРКА ЗАВЕРШЕНА")
        logger.info("=" * 50)
        
        return auth_result and test_result


def main():
    """Основная функция"""
    try:
        tester = BCSConnectionTester()
        result = tester.run_test()
        
        if result:
            print("\n" + "="*50)
            print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО")
            print("BCS API готов к использованию!")
            print("="*50)
            return 0
        else:
            print("\n" + "="*50)
            print("❌ ПРОВЕРКА НЕ ПРОЙДЕНА")
            print("Проверьте логи выше для диагностики")
            print("="*50)
            return 1
            
    except Exception as e:
        logger.error(f"💀 Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)