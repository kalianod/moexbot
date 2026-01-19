import apimoex
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional
import time

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MOEXSectorAnalyzer:
    """Анализатор секторов акций MOEX с использованием apimoex"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MomentumBotMOEX/1.0',
            'Accept': 'application/json'
        })
        
        # Проверяем доступность apimoex
        try:
            import apimoex
            self.has_apimoex = True
            logger.info("✅ apimoex доступен")
        except ImportError:
            self.has_apimoex = False
            logger.error("❌ apimoex не установлен. Установите: pip install apimoex")
            raise
    
    def get_all_tqbr_stocks_with_apimoex(self) -> List[Dict]:
        """Получить все акции с площадки TQBR используя apimoex"""
        try:
            logger.info("Используем apimoex для получения списка акций...")
            
            # Получаем все ценные бумаги с основной площадки TQBR
            stocks_data = apimoex.get_board_securities(
                self.session,
                board='TQBR',
                columns=('SECID', 'SHORTNAME', 'ISSUECAPITALIZATION', 'LISTLEVEL')
            )
            
            if not stocks_data:
                logger.warning("apimoex вернул пустой список")
                return []
            
            # Преобразуем данные в нужный формат
            stocks = []
            for item in stocks_data:
                stocks.append({
                    'symbol': item.get('SECID', ''),
                    'name': item.get('SHORTNAME', ''),
                    'market_cap': item.get('ISSUECAPITALIZATION', 0),
                    'list_level': item.get('LISTLEVEL', 0)
                })
            
            logger.info(f"apimoex: получено {len(stocks)} акций с TQBR")
            return stocks
            
        except Exception as e:
            logger.error(f"Ошибка при использовании apimoex: {e}")
            return []
    
    def get_all_tqbr_stocks_fallback(self) -> List[Dict]:
        """Резервный метод получения акций через прямой API"""
        try:
            logger.info("Используем резервный метод (прямой API)...")
            
            url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
            params = {
                'iss.meta': 'off',
                'securities.columns': 'SECID,SHORTNAME,ISSUECAPITALIZATION,LISTLEVEL',
                'start': 0,
                'limit': 1000
            }
            
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                securities = data.get('securities', {}).get('data', [])
                
                stocks = []
                for item in securities:
                    if len(item) >= 4:
                        stocks.append({
                            'symbol': item[0],
                            'name': item[1],
                            'market_cap': item[2] if item[2] else 0,
                            'list_level': item[3] if len(item) > 3 else 0
                        })
                
                logger.info(f"Прямой API: получено {len(stocks)} акций с TQBR")
                return stocks
            else:
                logger.error(f"Ошибка получения акций: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка резервного метода: {e}")
            return []
    
    def get_all_tqbr_stocks(self) -> List[Dict]:
        """Получить все акции с площадки TQBR (основной метод через apimoex)"""
        stocks = []
        
        if self.has_apimoex:
            stocks = self.get_all_tqbr_stocks_with_apimoex()
        
        # Если apimoex не сработал, пробуем резервный метод
        if not stocks:
            stocks = self.get_all_tqbr_stocks_fallback()
        
        # Фильтруем акции (исключаем пустые символы и некорректные данные)
        filtered_stocks = []
        for stock in stocks:
            symbol = stock.get('symbol', '').strip()
            name = stock.get('name', '').strip()
            
            if symbol and name and len(symbol) <= 10:  # Проверяем что тикер не слишком длинный
                filtered_stocks.append(stock)
        
        logger.info(f"После фильтрации осталось {len(filtered_stocks)} акций")
        return filtered_stocks
    
    def get_index_members_apimoex(self, index_symbol: str = "IMOEX") -> List[str]:
        """Получить состав индекса используя apimoex"""
        try:
            logger.info(f"Получаем состав индекса {index_symbol} через apimoex...")
            
            # Получаем данные по индексу
            url = f"https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/{index_symbol}.json"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Ищем данные о бумагах в индексе
                members = []
                
                # Пробуем разные возможные пути к данным
                possible_paths = [
                    ['analytics', 'data'],
                    ['securities', 'data'],
                    ['data']
                ]
                
                for path in possible_paths:
                    current_data = data
                    for key in path:
                        if key in current_data:
                            current_data = current_data[key]
                        else:
                            current_data = None
                            break
                    
                    if current_data and isinstance(current_data, list):
                        for row in current_data:
                            if row and len(row) > 0:
                                symbol = str(row[0]).strip()
                                if symbol:
                                    members.append(symbol)
                        break
                
                if members:
                    logger.info(f"В индексе {index_symbol} найдено {len(members)} бумаг")
                else:
                    logger.warning(f"Не удалось найти бумаги в индексе {index_symbol}")
                
                return members
                
        except Exception as e:
            logger.error(f"Ошибка получения состава индекса {index_symbol}: {e}")
        
        return []
    
    def determine_sector_by_name(self, symbol: str, name: str) -> str:
        """Определить сектор по названию и символу"""
        symbol_lower = symbol.lower()
        name_lower = name.lower()
        
        # Финансы
        finance_keywords = ['сбер', 'втб', 'тиньк', 'tcs', 'моск.бир', 'банк', 'газпромбанк', 
                           'открытие', 'альфа', 'банк', 'финанс', 'кредит', 'страх', 'инвест',
                           'vtb', 'sber', 'tinkoff', 'rosbank', 'raiffeisen']
        if any(kw in symbol_lower or kw in name_lower for kw in finance_keywords):
            return 'Финансы'
        
        # Нефть и газ
        oil_keywords = ['газп', 'лукойл', 'роснефт', 'новат', 'сургут', 'татнефт', 'башнефт',
                        'транснефт', 'нефт', 'газ', 'энерг', 'нефтегаз', 'gazp', 'lukoil',
                        'rosneft', 'novatek', 'surgut', 'tatneft']
        if any(kw in symbol_lower or kw in name_lower for kw in oil_keywords):
            return 'Нефть и газ'
        
        # Металлургия
        metal_keywords = ['норни', 'полюс', 'распад', 'мечел', 'ммк', 'нлмк', 'северст', 'русал',
                          'алроса', 'металл', 'сталь', 'никел', 'медь', 'алюмин', 'золот', 'серебр',
                          'nornickel', 'polyus', 'rusal', 'alrosa', 'mmk', 'nlmk', 'severstal']
        if any(kw in symbol_lower or kw in name_lower for kw in metal_keywords):
            return 'Металлургия'
        
        # IT
        it_keywords = ['яндекс', 'vк', 'ozon', 'positive', 'ксел', 'qiwi', 'tinkoff', 
                       'лаборатория', 'softline', 'it', 'софт', 'информ', 'технол', 'цифр',
                       'yandex', 'vk', 'ozon', 'qiwi', 'mail.ru', 'vkontakte']
        if any(kw in symbol_lower or kw in name_lower for kw in it_keywords):
            return 'IT'
        
        # Телеком
        telecom_keywords = ['мтс', 'ростелеком', 'билайн', 'мегафон', 'tele2', 'эр-телеком',
                            'телеком', 'связь', 'коммуник', 'mts', 'rostelecom', 'beeline',
                            'megafon', 'tele2']
        if any(kw in symbol_lower or kw in name_lower for kw in telecom_keywords):
            return 'Телекоммуникации'
        
        # Ритейл
        retail_keywords = ['магнит', 'x5', 'лента', 'дикси', 'метр', 'окей', 'детск', 
                           'пятерочк', 'мелодия', 'ритейл', 'торгов', 'продаж', 'маркет',
                           'magnit', 'x5', 'lenta', 'dixy', 'metro', 'oke', 'detsky mir']
        if any(kw in symbol_lower or kw in name_lower for kw in retail_keywords):
            return 'Потребительские товары'
        
        # Химия
        chem_keywords = ['фосагро', 'акрон', 'куйбышев', 'нижнекамск', 'хим', 'азот', 'калий',
                         'удобрен', 'химия', 'полимер', 'пластмасс', 'phosagro', 'akron',
                         'kuibyshev', 'nkhz']
        if any(kw in symbol_lower or kw in name_lower for kw in chem_keywords):
            return 'Химия и нефтехимия'
        
        # Энергетика
        energy_keywords = ['русгидро', 'интер рао', 'фск', 'россет', 'эн+', 'энергия', 'тепло',
                           'моэск', 'мрск', 'гидро', 'электр', 'энергетик', 'rushydro',
                           'inter rao', 'fsk', 'rosseti', 'en+', 'mosenergo', 'tgk']
        if any(kw in symbol_lower or kw in name_lower for kw in energy_keywords):
            return 'Электроэнергетика'
        
        # Транспорт
        transport_keywords = ['аэрофлот', 'совкомфлот', 'новороссийск', 'транспорт', 'порт',
                              'аэропорт', 'логистик', 'перевоз', 'груз', 'судо', 'авто',
                              'aeroflot', 'sovcomflot', 'novorossiysk', 'port', 'logistics']
        if any(kw in symbol_lower or kw in name_lower for kw in transport_keywords):
            return 'Транспорт'
        
        # Строительство
        construction_keywords = ['пик', 'лср', 'строитель', 'девелопер', 'недвиж', 'газстрой',
                                 'мостотрест', 'строй', 'архитек', 'проект', 'pik', 'lsr',
                                 'developer', 'real estate']
        if any(kw in symbol_lower or kw in name_lower for kw in construction_keywords):
            return 'Строительство'
        
        # Фармацевтика
        pharma_keywords = ['биокад', 'протек', 'фарм', 'мед', 'вита', 'герофарм', 'лекарств',
                           'фармацевт', 'здоров', 'био', 'biocad', 'protek', 'pharm', 'pharma']
        if any(kw in symbol_lower or kw in name_lower for kw in pharma_keywords):
            return 'Фармацевтика'
        
        # Добыча
        mining_keywords = ['уголь', 'золот', 'алмаз', 'руд', 'шахт', 'прииск', 'mining',
                           'coal', 'gold', 'diamond', 'mine']
        if any(kw in symbol_lower or kw in name_lower for kw in mining_keywords):
            return 'Добыча'
        
        # Машиностроение
        engineering_keywords = ['камаз', 'автоваз', 'об', 'авиа', 'судо', 'маш', 'завод',
                                'тяжмаш', 'kamaz', 'avtovaz', 'omsk', 'oms']
        if any(kw in symbol_lower or kw in name_lower for kw in engineering_keywords):
            return 'Машиностроение'
        
        # Холдинги
        holding_keywords = ['система', 'агх', 'афк', 'холдинг', 'инвест', 'группа',
                            'sistema', 'afk', 'holding', 'group']
        if any(kw in symbol_lower or kw in name_lower for kw in holding_keywords):
            return 'Холдинги'
        
        # Агро
        agro_keywords = ['черкизово', 'разгуляй', 'агро', 'зерно', 'мясо', 'птиц', 'сахар',
                         'cherkizovo', 'razgulay', 'agro', 'grain', 'meat', 'sugar']
        if any(kw in symbol_lower or kw in name_lower for kw in agro_keywords):
            return 'Сельское хозяйство'
        
        # Если не нашли подходящий сектор, пробуем определить по символу
        if symbol_lower.startswith('ru'):
            return 'Облигации'
        elif 'p' in symbol_lower[-1:]:
            return 'Привилегированные акции'
        elif symbol_lower.endswith('d'):
            return 'Депозитарные расписки'
        
        return 'Другое'
    
    def analyze_sectors(self, max_stocks: int = 300) -> Dict[str, List[Dict]]:
        """Проанализировать все акции и сгруппировать по секторам"""
        logger.info("Начинаем анализ секторов...")
        
        # Получаем все акции
        stocks = self.get_all_tqbr_stocks()
        
        if not stocks:
            logger.error("Не удалось получить список акций")
            return {}
        
        logger.info(f"Всего получено {len(stocks)} акций")
        
        # Ограничиваем количество для анализа
        stocks_to_analyze = stocks[:max_stocks]
        
        sectors = {}
        
        logger.info(f"Анализируем {len(stocks_to_analyze)} акций...")
        
        # Получаем состав индекса Мосбиржи для маркировки
        imoex_members = self.get_index_members_apimoex("IMOEX")
        
        for i, stock in enumerate(stocks_to_analyze, 1):
            symbol = stock['symbol']
            name = stock['name']
            
            # Определяем сектор
            sector = self.determine_sector_by_name(symbol, name)
            
            # Добавляем в соответствующий сектор
            if sector not in sectors:
                sectors[sector] = []
            
            # Проверяем, входит ли акция в индекс Мосбиржи
            in_index = symbol in imoex_members
            
            sectors[sector].append({
                'symbol': symbol,
                'name': name,
                'market_cap': stock.get('market_cap', 0),
                'list_level': stock.get('list_level', 0),
                'in_index': in_index
            })
            
            # Логируем прогресс
            if i % 50 == 0:
                logger.info(f"Обработано {i}/{len(stocks_to_analyze)} акций...")
        
        # Сортируем акции внутри секторов по капитализации
        for sector in sectors:
            sectors[sector].sort(key=lambda x: x['market_cap'], reverse=True)
        
        return sectors
    
    def print_sector_analysis(self, sectors: Dict[str, List[Dict]]):
        """Вывести анализ секторов в лог"""
        logger.info("\n" + "="*80)
        logger.info("АНАЛИЗ СЕКТОРОВ МОСБИРЖИ (через apimoex)")
        logger.info("="*80)
        
        if not sectors:
            logger.error("Нет данных для анализа")
            return
        
        total_stocks = sum(len(stocks) for stocks in sectors.values())
        
        # Сортируем сектора по количеству акций
        sorted_sectors = sorted(sectors.items(), key=lambda x: len(x[1]), reverse=True)
        
        for sector, stocks in sorted_sectors:
            logger.info(f"\n🏢 {sector.upper()}: {len(stocks)} акций ({len(stocks)/total_stocks*100:.1f}%)")
            logger.info("-" * 60)
            
            # Считаем сколько акций в индексе Мосбиржи
            in_index_count = sum(1 for s in stocks if s.get('in_index', False))
            
            if in_index_count > 0:
                logger.info(f"  Из них в индексе Мосбиржи: {in_index_count}")
            
            # Показываем топ-10 акций в секторе
            for i, stock in enumerate(stocks[:10], 1):
                market_cap = stock['market_cap']
                cap_str = f"{market_cap:,.0f}" if market_cap > 0 else "н/д"
                
                # Отмечаем акции в индексе
                index_marker = "📈" if stock.get('in_index', False) else " "
                
                logger.info(f"  {index_marker} {i:2d}. {stock['symbol']:<6} - {stock['name'][:35]:<35} | Кап: {cap_str}")
            
            if len(stocks) > 10:
                logger.info(f"  ... и еще {len(stocks) - 10} акций")
        
        # Сводная статистика
        logger.info("\n" + "="*80)
        logger.info("СВОДНАЯ СТАТИСТИКА:")
        logger.info("-" * 80)
        
        logger.info(f"Всего акций проанализировано: {total_stocks}")
        logger.info(f"Всего секторов: {len(sectors)}")
        
        # Сектора с наибольшим количеством акций
        logger.info("\nТоп-5 секторов по количеству акций:")
        for i, (sector, stocks) in enumerate(sorted_sectors[:5], 1):
            percentage = len(stocks) / total_stocks * 100
            logger.info(f"  {i}. {sector:<25} - {len(stocks):>3} акций ({percentage:>5.1f}%)")
        
        # Сектора подходящие для стратегии (более 3 акций)
        good_sectors = [(sector, stocks) for sector, stocks in sectors.items() 
                       if len(stocks) >= 3]
        
        if good_sectors:
            logger.info(f"\nСектора с достаточным количеством акций (≥3) для стратегии: {len(good_sectors)}")
            for sector, stocks in sorted(good_sectors, key=lambda x: len(x[1]), reverse=True):
                logger.info(f"  • {sector}: {len(stocks)} акций")
        else:
            logger.warning("\nНет секторов с достаточным количеством акций для стратегии отбора топ-3")
        
        # Сектора с менее чем 3 акциями
        small_sectors = [(sector, stocks) for sector, stocks in sectors.items() 
                        if len(stocks) < 3]
        if small_sectors:
            logger.info(f"\nСектора с менее чем 3 акциями ({len(small_sectors)} секторов):")
            for sector, stocks in small_sectors:
                logger.info(f"  • {sector}: {len(stocks)} акций")
    
    def get_recommended_sectors(self, sectors: Dict[str, List[Dict]]) -> List[str]:
        """Получить список рекомендованных секторов для стратегии"""
        recommended_base = [
            'Финансы',
            'Нефть и газ', 
            'Металлургия',
            'Электроэнергетика',
            'Телекоммуникации',
            'Потребительские товары',
            'Химия и нефтехимия',
            'IT',
            'Транспорт'
        ]
        
        # Фильтруем только те сектора, которые есть в данных и где достаточно акций
        recommended = []
        for sector in recommended_base:
            if sector in sectors and len(sectors[sector]) >= 3:
                recommended.append(sector)
        
        return recommended
    
    def analyze_for_strategy(self, sectors: Dict[str, List[Dict]]):
        """Проанализировать сектора для стратегии отбора топ-3"""
        if not sectors:
            return
        
        logger.info("\n" + "="*80)
        logger.info("АНАЛИЗ ДЛЯ СТРАТЕГИИ ОТБОРА ТОП-3 ПО СЕКТОРАМ")
        logger.info("="*80)
        
        recommended = self.get_recommended_sectors(sectors)
        
        if not recommended:
            logger.warning("Нет подходящих секторов для стратегии")
            return
        
        logger.info(f"Найдено {len(recommended)} подходящих секторов:")
        
        for sector in recommended:
            stocks = sectors[sector]
            logger.info(f"\n📊 {sector.upper()}: {len(stocks)} акций")
            
            # Показываем топ-3 по капитализации
            top_3 = stocks[:3]
            for i, stock in enumerate(top_3, 1):
                market_cap = stock['market_cap']
                cap_str = f"{market_cap:,.0f}" if market_cap > 0 else "н/д"
                index_marker = "📈" if stock.get('in_index', False) else ""
                
                logger.info(f"  {i}. {index_marker} {stock['symbol']} - {stock['name'][:30]}: {cap_str}")
        
        # Общий план отбора
        logger.info("\n" + "="*80)
        logger.info("ПЛАН СТРАТЕГИИ:")
        logger.info("-" * 80)
        
        total_possible = sum(min(3, len(sectors[s])) for s in recommended)
        logger.info(f"• Всего можно отобрать до {total_possible} акций")
        logger.info(f"• Из {len(recommended)} секторов")
        logger.info("• По 3 акции из каждого сектора (или меньше, если в секторе меньше 3 акций)")
        
        logger.info("\nРекомендуемые сектора для включения в стратегию:")
        for sector in recommended:
            count = len(sectors[sector])
            logger.info(f"  • {sector}: {count} акций (отбираем {min(3, count)})")

def main():
    """Основная функция"""
    try:
        # Проверяем доступность apimoex
        import apimoex
    except ImportError:
        logger.error("❌ apimoex не установлен. Установите: pip install apimoex")
        return
    
    analyzer = MOEXSectorAnalyzer()
    
    # Анализируем сектора
    sectors = analyzer.analyze_sectors(max_stocks=300)
    
    if sectors:
        # Выводим анализ в лог
        analyzer.print_sector_analysis(sectors)
        
        # Анализ для стратегии
        analyzer.analyze_for_strategy(sectors)
        
    else:
        logger.error("Не удалось проанализировать сектора")

if __name__ == "__main__":
    main()