import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import logging.handlers
import json
import warnings
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from functools import lru_cache
from collections import defaultdict
import traceback
import csv

warnings.filterwarnings('ignore')

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ ==========
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger('MomentumBotC1')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler(
    f'logs/momentum_c1_hedge_{datetime.now().strftime("%Y%m")}.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# ============================================

# ИМПОРТ apimoex С ОБРАБОТКОЙ ОШИБОК
try:
    import apimoex
    HAS_APIMOEX = True
    logger.info("✅ apimoex успешно импортирован")
except ImportError:
    HAS_APIMOEX = False
    logger.error("❌ apimoex не установлен. Установите: pip install apimoex")
except Exception as e:
    HAS_APIMOEX = False
    logger.error(f"❌ Ошибка импорта apimoex: {e}")

load_dotenv()

# ========== КЛАСС ДЛЯ ВИРТУАЛЬНОЙ СДЕЛКИ ==========
@dataclass
class VirtualTrade:
    """Класс для хранения информации о виртуальной сделке"""
    symbol: str
    action: str  # 'BUY', 'SELL', 'HEDGE_OPEN', 'HEDGE_CLOSE'
    price: float
    quantity: int = 0
    entry_time: datetime = None
    exit_time: datetime = None
    entry_price: float = 0.0
    exit_price: float = 0.0
    profit_pct: float = 0.0
    reason: str = ''
    stop_loss: float = 0.0
    sector: str = ''
    is_hedge: bool = False
    
    def to_dict(self):
        """Конвертация в словарь для JSON"""
        return {
            'symbol': self.symbol,
            'action': self.action,
            'price': self.price,
            'quantity': self.quantity,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'profit_pct': self.profit_pct,
            'reason': self.reason,
            'stop_loss': self.stop_loss,
            'sector': self.sector,
            'is_hedge': self.is_hedge
        }
    
    @classmethod
    def from_dict(cls, data):
        """Создание из словаря"""
        trade = cls(
            symbol=data['symbol'],
            action=data['action'],
            price=data['price'],
            quantity=data['quantity'],
            entry_price=data['entry_price'],
            exit_price=data['exit_price'],
            profit_pct=data['profit_pct'],
            reason=data['reason'],
            stop_loss=data['stop_loss'],
            sector=data['sector'],
            is_hedge=data['is_hedge']
        )
        if data.get('entry_time'):
            trade.entry_time = datetime.fromisoformat(data['entry_time'])
        if data.get('exit_time'):
            trade.exit_time = datetime.fromisoformat(data['exit_time'])
        return trade

# ========== КЛАСС ДЛЯ ВИРТУАЛЬНОГО ПОРТФЕЛЯ ==========
class VirtualPortfolio:
    """Управление виртуальными позициями и историей сделок"""
    def __init__(self, initial_cash=1_000_000):
        self.cash = initial_cash
        self.positions: Dict[str, Dict] = {}  # symbol -> {'entry_price', 'entry_time', 'quantity', 'stop_loss', ...}
        self.hedge_position: Dict = {'active': False, 'entry_price': 0, 'entry_time': None, 'quantity': 0}
        self.trade_history: List[VirtualTrade] = []
        self.equity_curve = []
    
    def to_dict(self):
        """Конвертация портфеля в словарь для JSON"""
        return {
            'cash': self.cash,
            'positions': {
                k: {
                    **v,
                    'entry_time': v['entry_time'].isoformat() if v.get('entry_time') else None
                }
                for k, v in self.positions.items()
            },
            'hedge_position': {
                **self.hedge_position,
                'entry_time': self.hedge_position['entry_time'].isoformat() if self.hedge_position.get('entry_time') else None
            },
            'trade_history': [t.to_dict() for t in self.trade_history[-100:]]  # Последние 100 сделок
        }
    
    @classmethod
    def from_dict(cls, data):
        """Восстановление портфеля из словаря"""
        portfolio = cls(initial_cash=data.get('cash', 1_000_000))
        
        # Восстановление позиций
        for sym, pos in data.get('positions', {}).items():
            if pos.get('entry_time'):
                pos['entry_time'] = datetime.fromisoformat(pos['entry_time'])
            portfolio.positions[sym] = pos
        
        # Восстановление хедж-позиции
        hedge = data.get('hedge_position', {})
        if hedge.get('entry_time'):
            hedge['entry_time'] = datetime.fromisoformat(hedge['entry_time'])
        portfolio.hedge_position = hedge
        
        # Восстановление истории сделок
        for t_data in data.get('trade_history', []):
            portfolio.trade_history.append(VirtualTrade.from_dict(t_data))
        
        return portfolio
    
    def open_position(self, symbol: str, price: float, stop_loss: float, sector: str = '', reason: str = '') -> bool:
        """Открытие длинной позиции (равные веса)"""
        # Равные веса: делим cash на количество позиций
        active_positions = len(self.positions)
        
        # Пересчитываем вес для всех позиций
        new_weight = 1.0 / (active_positions + 1)
        
        # Корректируем существующие позиции (уменьшаем количество)
        for pos in self.positions.values():
            pos['quantity'] = int(self.cash * new_weight / pos['entry_price'])
        
        # Открываем новую позицию
        quantity = int(self.cash * new_weight / price)
        if quantity <= 0:
            logger.warning(f"⚠️ Недостаточно средств для открытия {symbol}")
            return False
        
        self.positions[symbol] = {
            'entry_price': price,
            'entry_time': datetime.now(),
            'quantity': quantity,
            'stop_loss': stop_loss,
            'sector': sector,
            'reason': reason,
            'name': symbol
        }
        
        # Уменьшаем cash на стоимость позиции
        self.cash -= quantity * price
        
        trade = VirtualTrade(
            symbol=symbol,
            action='BUY',
            price=price,
            quantity=quantity,
            entry_time=datetime.now(),
            entry_price=price,
            reason=reason,
            stop_loss=stop_loss,
            sector=sector,
            is_hedge=False
        )
        self.trade_history.append(trade)
        logger.info(f"📈 BUY {symbol}: {quantity} шт по {price:.2f}, SL {stop_loss:.2f}")
        return True
    
    def close_position(self, symbol: str, price: float, reason: str = '') -> bool:
        """Закрытие длинной позиции"""
        if symbol not in self.positions:
            return False
        
        pos = self.positions[symbol]
        quantity = pos['quantity']
        entry_price = pos['entry_price']
        profit = (price - entry_price) * quantity
        profit_pct = (price - entry_price) / entry_price * 100
        
        self.cash += price * quantity
        
        trade = VirtualTrade(
            symbol=symbol,
            action='SELL',
            price=price,
            quantity=quantity,
            entry_time=pos['entry_time'],
            exit_time=datetime.now(),
            entry_price=entry_price,
            exit_price=price,
            profit_pct=profit_pct,
            reason=reason,
            stop_loss=pos.get('stop_loss', 0),
            sector=pos.get('sector', ''),
            is_hedge=False
        )
        self.trade_history.append(trade)
        del self.positions[symbol]
        
        profit_emoji = "📈" if profit_pct > 0 else "📉"
        logger.info(f"📉 SELL {symbol}: {profit_pct:+.2f}% {profit_emoji}, {reason}")
        return True
    
    def open_hedge(self, price: float) -> bool:
        """Открытие хедж-позиции (шорт индекса)"""
        if self.hedge_position['active']:
            return False
        
        # Шорт на 100% портфеля
        total_value = self.get_total_value()
        quantity = int(total_value / price) if price > 0 else 0
        
        self.hedge_position = {
            'active': True,
            'entry_price': price,
            'entry_time': datetime.now(),
            'quantity': quantity
        }
        
        trade = VirtualTrade(
            symbol='MCFTR_HEDGE',
            action='HEDGE_OPEN',
            price=price,
            quantity=quantity,
            entry_time=datetime.now(),
            entry_price=price,
            reason='Hedge signal triggered',
            is_hedge=True
        )
        self.trade_history.append(trade)
        logger.info(f"🔒 HEDGE OPEN: MCFTR шорт {quantity} шт по {price:.2f}")
        return True
    
    def close_hedge(self, price: float) -> bool:
        """Закрытие хедж-позиции"""
        if not self.hedge_position['active']:
            return False
        
        entry_price = self.hedge_position['entry_price']
        quantity = self.hedge_position['quantity']
        
        # Прибыль от шорта = (вход - выход) * количество
        profit = (entry_price - price) * quantity
        profit_pct = (entry_price - price) / entry_price * 100 if entry_price > 0 else 0
        
        trade = VirtualTrade(
            symbol='MCFTR_HEDGE',
            action='HEDGE_CLOSE',
            price=price,
            quantity=quantity,
            entry_time=self.hedge_position['entry_time'],
            exit_time=datetime.now(),
            entry_price=entry_price,
            exit_price=price,
            profit_pct=profit_pct,
            reason='Hedge close signal',
            is_hedge=True
        )
        self.trade_history.append(trade)
        
        self.hedge_position = {'active': False, 'entry_price': 0, 'entry_time': None, 'quantity': 0}
        logger.info(f"🔓 HEDGE CLOSE: MCFTR, PnL: {profit_pct:+.2f}%")
        return True
    
    def get_total_value(self) -> float:
        """Общая стоимость портфеля (кэш + позиции)"""
        value = self.cash
        
        for pos in self.positions.values():
            value += pos['quantity'] * pos['entry_price']  # Используем цену входа для простоты
        
        return value
    
    def save_trades_to_csv(self, filename='logs/virtual_trades_c1.csv'):
        """Сохранение истории сделок в CSV"""
        if not self.trade_history:
            return
        
        df = pd.DataFrame([{
            'symbol': t.symbol,
            'action': t.action,
            'entry_time': t.entry_time.strftime('%Y-%m-%d %H:%M:%S') if t.entry_time else '',
            'exit_time': t.exit_time.strftime('%Y-%m-%d %H:%M:%S') if t.exit_time else '',
            'entry_price': t.entry_price,
            'exit_price': t.exit_price,
            'quantity': t.quantity,
            'profit_pct': t.profit_pct,
            'reason': t.reason,
            'stop_loss': t.stop_loss,
            'sector': t.sector,
            'is_hedge': t.is_hedge
        } for t in self.trade_history])
        
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"💾 История сделок сохранена в {filename} ({len(self.trade_history)} записей)")

# ========== ОСНОВНОЙ КЛАСС БОТА ==========
class MomentumBotC1:
    """Бот стратегии C1 с хеджем (ROC(252), топ-10, ребаланс 40 дней)"""
    
    def __init__(self):
        self.telegram_token = os.getenv('TELEGRAM_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Параметры стратегии C1
        self.rebalance_days = 40
        self.top_n = 10
        self.sma_fast = 10
        self.sma_slow = 50
        self.sma_entry = 100
        self.use_trend_filter = True
        self.use_entry_sma_filter = True
        
        # Параметры выхода (B5)
        self.use_sma_exit = True
        self.use_atr_trailing = True
        self.atr_multiplier = 4.0
        self.atr_period = 14
        
        # Параметры хеджа
        self.hedge_sma_period = 200
        self.hedge_threshold = 0.005  # 0.5%
        self.hedge_enabled = True  # Хедж активен
        
        self.benchmark_symbol = 'MCFTR'
        self.benchmark_name = 'Индекс Мосбиржи полной доходности'
        
        # Временные настройки
        self.check_times = ["14:10", "19:10"]
        self.report_time = "19:30"
        self.last_rebalance_date = None
        self.analysis_request_delay = 0.5
        
        # Кэш и данные
        self.data_fetcher = MOEXDataFetcherC1(self)
        self.virtual_portfolio = VirtualPortfolio()
        
        # Для отчетов
        self.asset_ranking: List[AssetDataC1] = []
        self.sector_performance = {}
        
        # Загрузка состояния
        self.load_state()
        
        logger.info("=" * 60)
        logger.info("🚀 MOMENTUM BOT C1 С ХЕДЖЕМ")
        logger.info(f"📈 Стратегия: C1 (ROC252, SMA{self.sma_fast}/{self.sma_slow}, SMA{self.sma_entry})")
        logger.info(f"🎯 Отбор: топ-{self.top_n} по ROC252")
        logger.info(f"🛡️ Выход: B5 (ATR x{self.atr_multiplier}, SMA exit)")
        logger.info(f"🔒 Хедж: SMA{self.hedge_sma_period}, порог {self.hedge_threshold*100}%")
        logger.info(f"📅 Ребаланс: каждые {self.rebalance_days} дней")
        logger.info("=" * 60)

    def load_state(self):
        """Загрузка состояния из JSON с обработкой ошибок"""
        try:
            state_file = 'logs/bot_state_c1.json'
            if not os.path.exists(state_file):
                logger.info("📁 Файл состояния не найден, начинаем с чистого портфеля")
                return
            
            with open(state_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    logger.warning("⚠️ Файл состояния пуст")
                    return
                state = json.loads(content)
            
            # Восстановление портфеля
            if 'portfolio' in state:
                self.virtual_portfolio = VirtualPortfolio.from_dict(state['portfolio'])
            
            # Восстановление даты ребаланса
            if state.get('last_rebalance_date'):
                try:
                    self.last_rebalance_date = datetime.fromisoformat(state['last_rebalance_date'])
                except (ValueError, TypeError):
                    logger.warning(f"⚠️ Неверный формат даты ребаланса: {state['last_rebalance_date']}")
                    self.last_rebalance_date = None
            
            logger.info(f"💾 Состояние загружено. Позиций: {len(self.virtual_portfolio.positions)}, "
                       f"хедж: {'активен' if self.virtual_portfolio.hedge_position['active'] else 'неактивен'}, "
                       f"сделок в истории: {len(self.virtual_portfolio.trade_history)}")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON: {e}")
            logger.info("🔄 Создаем новый файл состояния")
            self.save_state()  # Создаем новый корректный файл
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки состояния: {e}")
            logger.info("🔄 Продолжаем с чистым портфелем")

    def save_state(self):
        """Сохранение состояния"""
        try:
            state = {
                'portfolio': self.virtual_portfolio.to_dict(),
                'last_rebalance_date': self.last_rebalance_date.isoformat() if self.last_rebalance_date else None,
                'timestamp': datetime.now().isoformat(),
                'version': 'c1_hedge_v1.0'
            }
            
            with open('logs/bot_state_c1.json', 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Состояние сохранено. Позиций: {len(self.virtual_portfolio.positions)}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения состояния: {e}")

    def should_rebalance(self) -> bool:
        """Проверка необходимости ребаланса (раз в 40 дней)"""
        if self.last_rebalance_date is None:
            logger.info("🔄 Первый ребаланс")
            return True
        
        days_passed = (datetime.now() - self.last_rebalance_date).days
        if days_passed >= self.rebalance_days:
            logger.info(f"🔄 Ребаланс: прошло {days_passed} дней (порог {self.rebalance_days})")
            return True
        
        return False

    def analyze_assets(self) -> List['AssetDataC1']:
        """Анализ активов: ROC252 + фильтры C1, отбор топ-10"""
        top_assets = self.data_fetcher.get_top_assets()
        if not top_assets:
            logger.error("❌ Нет активов для анализа")
            return []
        
        logger.info(f"📊 Анализ {len(top_assets)} активов...")
        
        assets = []
        benchmark_data = self.data_fetcher.get_benchmark_data()
        
        for i, asset_info in enumerate(top_assets):
            if asset_info['symbol'] == self.benchmark_symbol:
                continue
            
            try:
                asset = self.data_fetcher.calculate_asset_data(asset_info)
                if asset is None:
                    continue
                
                # Фильтры C1
                if asset.roc252 <= 0:
                    logger.debug(f"❌ {asset.symbol}: ROC252 = {asset.roc252:.1f}% <= 0")
                    continue
                
                if self.use_trend_filter and not asset.sma_signal:
                    logger.debug(f"❌ {asset.symbol}: SMA({self.sma_fast}) > SMA({self.sma_slow}) = {asset.sma_signal}")
                    continue
                
                if self.use_entry_sma_filter and asset.current_price <= asset.sma_entry:
                    logger.debug(f"❌ {asset.symbol}: цена {asset.current_price:.2f} <= SMA{self.sma_entry} {asset.sma_entry:.2f}")
                    continue
                
                assets.append(asset)
                
                if i % 20 == 0:
                    logger.debug(f"✅ Прогресс: {i}/{len(top_assets)}, найдено {len(assets)}")
                
            except Exception as e:
                logger.error(f"Ошибка анализа {asset_info['symbol']}: {e}")
                continue
        
        # Сортируем по ROC252 и берем топ-10
        assets.sort(key=lambda x: x.roc252, reverse=True)
        selected = assets[:self.top_n]
        
        logger.info(f"✅ Отобрано {len(selected)} активов из {len(assets)} прошедших фильтры")
        
        if benchmark_data:
            logger.info(f"📈 Бенчмарк MCFTR ROC252: {benchmark_data['roc252']:+.1f}%")
        
        for i, asset in enumerate(selected, 1):
            logger.info(f"  {i}. {asset.symbol}: ROC252 = {asset.roc252:+.1f}%, цена: {asset.current_price:.2f}, сектор: {asset.sector}")
        
        return selected

    def check_hedge_conditions(self) -> Tuple[bool, bool]:
        """Проверка условий открытия/закрытия хеджа"""
        if not self.hedge_enabled:
            return False, False
        
        # Получаем данные индекса
        df = self.data_fetcher.get_cached_historical_data(self.benchmark_symbol, 400)
        if df is None or len(df) < 2:
            logger.debug("⚠️ Недостаточно данных для проверки хеджа")
            return False, False
        
        try:
            current_close = df['close'].iloc[-1]
            prev_high = df['high'].iloc[-2]
            prev_low = df['low'].iloc[-2]
            
            # SMA200
            sma200 = df['close'].rolling(window=self.hedge_sma_period).mean().iloc[-1]
            if pd.isna(sma200):
                logger.debug("⚠️ SMA200 не рассчитана")
                return False, False
            
            hedge_enabled = current_close > sma200
            
            # Пороги
            open_threshold = prev_low * (1 - self.hedge_threshold)
            close_threshold = prev_high * (1 + self.hedge_threshold)
            
            should_open = hedge_enabled and not self.virtual_portfolio.hedge_position['active'] and current_close < open_threshold
            should_close = self.virtual_portfolio.hedge_position['active'] and current_close > close_threshold
            
            if should_open:
                logger.info(f"🔔 Сигнал HEDGE_OPEN: цена {current_close:.2f} < {prev_low:.2f} - {self.hedge_threshold*100}% = {open_threshold:.2f}")
            if should_close:
                logger.info(f"🔔 Сигнал HEDGE_CLOSE: цена {current_close:.2f} > {prev_high:.2f} + {self.hedge_threshold*100}% = {close_threshold:.2f}")
            
            return should_open, should_close
            
        except Exception as e:
            logger.error(f"Ошибка проверки хеджа: {e}")
            return False, False

    def generate_signals(self, assets: List['AssetDataC1']) -> List[VirtualTrade]:
        """Генерация сигналов: BUY/SELL, ребаланс, стоп-лоссы, хедж"""
        signals = []
        
        # 1. Проверка хеджа
        hedge_open, hedge_close = self.check_hedge_conditions()
        if hedge_open:
            price, _, _ = self.data_fetcher.get_current_price(self.benchmark_symbol)
            if price and price > 0:
                if self.virtual_portfolio.open_hedge(price):
                    signals.append(self.virtual_portfolio.trade_history[-1])
        
        if hedge_close:
            price, _, _ = self.data_fetcher.get_current_price(self.benchmark_symbol)
            if price and price > 0:
                if self.virtual_portfolio.close_hedge(price):
                    signals.append(self.virtual_portfolio.trade_history[-1])
        
        # 2. Проверка стоп-лоссов для существующих позиций
        for symbol in list(self.virtual_portfolio.positions.keys()):
            # Получаем текущую цену
            price, _, _ = self.data_fetcher.get_current_price(symbol)
            if price is None or price <= 0:
                continue
            
            pos = self.virtual_portfolio.positions[symbol]
            stop_loss = pos.get('stop_loss', 0)
            
            if stop_loss > 0 and price <= stop_loss:
                if self.virtual_portfolio.close_position(symbol, price, reason=f"Стоп-лосс ({stop_loss:.2f})"):
                    signals.append(self.virtual_portfolio.trade_history[-1])
                continue
            
            # Получаем свежие данные актива
            asset_info = {
                'symbol': symbol, 
                'name': pos.get('name', symbol), 
                'sector': pos.get('sector', ''), 
                'source': 'moex'
            }
            asset = self.data_fetcher.calculate_asset_data(asset_info)
            if asset:
                # Выход по SMA exit (SMA50)
                if self.use_sma_exit and price < asset.sma_slow:
                    if self.virtual_portfolio.close_position(symbol, price, reason=f"SMA exit: {price:.2f} < SMA{self.sma_slow} {asset.sma_slow:.2f}"):
                        signals.append(self.virtual_portfolio.trade_history[-1])
                    continue
                
                # Выход по отрицательному ROC252
                if asset.roc252 <= 0:
                    if self.virtual_portfolio.close_position(symbol, price, reason=f"ROC252 отрицательный ({asset.roc252:+.1f}%)"):
                        signals.append(self.virtual_portfolio.trade_history[-1])
                    continue
                
                # Выход по отрицательному тренд-фильтру
                if self.use_trend_filter and not asset.sma_signal:
                    if self.virtual_portfolio.close_position(symbol, price, reason=f"SMA{self.sma_fast} > SMA{self.sma_slow} = False"):
                        signals.append(self.virtual_portfolio.trade_history[-1])
                    continue
                
                # Выход по SMA entry
                if self.use_entry_sma_filter and price <= asset.sma_entry:
                    if self.virtual_portfolio.close_position(symbol, price, reason=f"Цена <= SMA{self.sma_entry} {asset.sma_entry:.2f}"):
                        signals.append(self.virtual_portfolio.trade_history[-1])
                    continue
        
        # 3. Ребаланс (раз в 40 дней)
        if self.should_rebalance():
            logger.info(f"🔄 ЗАПУСК РЕБАЛАНСА")
            
            # Определяем, какие акции должны быть в портфеле
            selected_symbols = {asset.symbol for asset in assets}
            
            # Закрываем позиции, которые не прошли отбор
            for symbol in list(self.virtual_portfolio.positions.keys()):
                if symbol not in selected_symbols:
                    price, _, _ = self.data_fetcher.get_current_price(symbol)
                    if price and price > 0:
                        if self.virtual_portfolio.close_position(symbol, price, reason="Исключена при ребалансе"):
                            signals.append(self.virtual_portfolio.trade_history[-1])
            
            # Открываем новые позиции
            for asset in assets:
                if asset.symbol not in self.virtual_portfolio.positions:
                    price = asset.current_price
                    stop_loss = self.calculate_stop_loss(asset)
                    success = self.virtual_portfolio.open_position(
                        symbol=asset.symbol,
                        price=price,
                        stop_loss=stop_loss,
                        sector=asset.sector,
                        reason=f"Ребаланс, ROC252: {asset.roc252:+.1f}%"
                    )
                    if success:
                        signals.append(self.virtual_portfolio.trade_history[-1])
            
            self.last_rebalance_date = datetime.now()
            self.save_state()
        
        return signals

    def calculate_stop_loss(self, asset: 'AssetDataC1') -> float:
        """Расчет стоп-лосса по ATR или минимуму"""
        if self.use_atr_trailing and asset.atr > 0:
            stop = asset.current_price - self.atr_multiplier * asset.atr
            # Ограничиваем 5-20%
            pct = (asset.current_price - stop) / asset.current_price * 100
            if pct < 5:
                stop = asset.current_price * 0.95
            elif pct > 20:
                stop = asset.current_price * 0.80
            return max(stop, 0.01)
        return 0.0

    def send_signals_to_telegram(self, signals: List[VirtualTrade]):
        """Отправка сигналов в Telegram с красивым форматированием"""
        for signal in signals:
            if signal.is_hedge:
                if signal.action == 'HEDGE_OPEN':
                    msg = (
                        f"🔒 *ХЕДЖ ОТКРЫТ*\n"
                        f"═══════════════════════════\n"
                        f"💰 Цена: {signal.price:.2f} руб\n"
                        f"📈 SMA{self.hedge_sma_period}: включен\n"
                        f"⚡ Порог: {self.hedge_threshold*100}%\n"
                        f"🕐 {signal.entry_time.strftime('%H:%M:%S %d.%m.%Y')}\n"
                        f"═══════════════════════════\n"
                        f"{signal.reason}"
                    )
                else:
                    profit_emoji = "📈" if signal.profit_pct > 0 else "📉"
                    days = (signal.exit_time - signal.entry_time).days if signal.exit_time and signal.entry_time else 0
                    msg = (
                        f"🔓 *ХЕДЖ ЗАКРЫТ*\n"
                        f"═══════════════════════════\n"
                        f"💰 Вход: {signal.entry_price:.2f} руб\n"
                        f"💰 Выход: {signal.exit_price:.2f} руб\n"
                        f"📊 Прибыль: **{signal.profit_pct:+.2f}%** {profit_emoji}\n"
                        f"📅 Дней: {days}\n"
                        f"🕐 {signal.exit_time.strftime('%H:%M:%S %d.%m.%Y')}\n"
                        f"═══════════════════════════\n"
                        f"{signal.reason}"
                    )
            else:
                if signal.action == 'BUY':
                    asset = self._get_asset(signal.symbol)
                    roc252 = asset.roc252 if asset else 0
                    msg = (
                        f"🎯 *BUY: {signal.symbol}*\n"
                        f"═══════════════════════════\n"
                        f"🏢 {signal.sector}\n"
                        f"💰 Цена: {signal.price:.2f} руб\n"
                        f"⛔ Стоп-лосс: **{signal.stop_loss:.2f} руб**\n"
                        f"📊 ROC252: **{roc252:+.1f}%**\n"
                        f"🕐 {signal.entry_time.strftime('%H:%M:%S %d.%m.%Y')}\n"
                        f"═══════════════════════════\n"
                        f"{signal.reason}"
                    )
                else:
                    profit_emoji = "📈" if signal.profit_pct > 0 else "📉"
                    stop_hit = "⛔" if "стоп-лосс" in signal.reason.lower() else ""
                    days = (signal.exit_time - signal.entry_time).days if signal.exit_time and signal.entry_time else 0
                    msg = (
                        f"🎯 *SELL: {signal.symbol}* {stop_hit}\n"
                        f"═══════════════════════════\n"
                        f"💰 Вход: {signal.entry_price:.2f} руб\n"
                        f"💰 Выход: {signal.exit_price:.2f} руб\n"
                        f"📊 Прибыль: **{signal.profit_pct:+.2f}%** {profit_emoji}\n"
                        f"📅 Дней: {days}\n"
                        f"🕐 {signal.exit_time.strftime('%H:%M:%S %d.%m.%Y')}\n"
                        f"═══════════════════════════\n"
                        f"{signal.reason}"
                    )
            self.send_telegram_message(msg, force=True)
            time.sleep(0.5)  # Пауза между сообщениями
    
    def _get_asset(self, symbol: str) -> Optional['AssetDataC1']:
        """Поиск актива в текущем рейтинге"""
        for asset in self.asset_ranking:
            if asset.symbol == symbol:
                return asset
        return None

    def send_telegram_message(self, message: str, silent: bool = False, force: bool = True) -> bool:
        """Отправка сообщения в Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("⚠️ Telegram не настроен")
            return False
        
        max_len = 4000
        messages = []
        if len(message) > max_len:
            while message:
                if len(message) <= max_len:
                    messages.append(message)
                    break
                split_pos = message.rfind('\n', 0, max_len)
                if split_pos == -1:
                    split_pos = max_len
                messages.append(message[:split_pos])
                message = message[split_pos:]
        else:
            messages = [message]
        
        success = True
        for msg in messages:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                data = {
                    "chat_id": self.telegram_chat_id,
                    "text": msg,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                    "disable_notification": silent
                }
                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    logger.debug("✅ Сообщение отправлено в Telegram")
                elif response.status_code == 400:
                    # Ошибка форматирования Markdown, пробуем без него
                    data.pop('parse_mode', None)
                    response = requests.post(url, data=data, timeout=10)
                    if response.status_code == 200:
                        logger.debug("✅ Сообщение отправлено без Markdown")
                    else:
                        logger.warning(f"⚠️ Ошибка Telegram: {response.status_code}")
                        success = False
                else:
                    logger.warning(f"⚠️ Ошибка Telegram: {response.status_code}")
                    success = False
            except Exception as e:
                logger.error(f"❌ Ошибка отправки Telegram: {e}")
                success = False
        
        return success

    def format_combined_report(self, assets: List['AssetDataC1']) -> str:
        """Формирование дневного отчета"""
        if not assets:
            return "📊 *Нет данных для отчета*"
        
        current_date = datetime.now().strftime('%d.%m.%Y')
        benchmark = self.data_fetcher.get_benchmark_data()
        benchmark_roc = benchmark['roc252'] if benchmark else 0
        
        msg = f"🎯 *C1 MOMENTUM ОТЧЕТ*\n"
        msg += f"📅 {current_date} | 📈 MCFTR ROC252: {benchmark_roc:+.1f}%\n"
        msg += "═══════════════════════════\n\n"
        
        # Топ-10 по ROC252
        msg += "*🏆 ТОП-10 ПО ROC252:*\n"
        for i, asset in enumerate(assets[:10], 1):
            vs_bench = asset.roc252 - benchmark_roc if benchmark else 0
            status = "🟢 IN" if asset.symbol in self.virtual_portfolio.positions else "⚪ OUT"
            msg += f"{i}. {asset.symbol}: {asset.roc252:+.1f}% | vs бенч: {vs_bench:+.1f}% | {asset.current_price:.2f}₽ {status}\n"
        
        msg += "\n*📊 ТЕКУЩИЕ ПОЗИЦИИ:*\n"
        if self.virtual_portfolio.positions:
            for symbol, pos in self.virtual_portfolio.positions.items():
                entry = pos['entry_price']
                stop = pos.get('stop_loss', 0)
                current_price, _, _ = self.data_fetcher.get_current_price(symbol)
                current_price = current_price or entry
                profit = ((current_price - entry) / entry) * 100
                profit_emoji = "📈" if profit > 0 else "📉"
                msg += f"• {symbol}: вход {entry:.2f} → {current_price:.2f} {profit_emoji} {profit:+.1f}%, стоп {stop:.2f}, {pos.get('sector', '')}\n"
        else:
            msg += "Нет активных позиций\n"
        
        if self.virtual_portfolio.hedge_position['active']:
            msg += f"\n🔒 *ХЕДЖ АКТИВЕН*: вход {self.virtual_portfolio.hedge_position['entry_price']:.2f}\n"
        
        msg += "\n═══════════════════════════\n"
        msg += f"⚙️ ROC252 > 0, SMA{self.sma_fast}>{self.sma_slow}, цена > SMA{self.sma_entry}\n"
        msg += f"🛡️ ATR x{self.atr_multiplier}, SMA exit, хедж SMA{self.hedge_sma_period} {self.hedge_threshold*100}%\n"
        msg += f"📊 Виртуальный портфель: {self.virtual_portfolio.cash:.0f} RUB\n"
        
        return msg

    def run_strategy_cycle(self, send_report: bool = False):
        """Один цикл стратегии"""
        logger.info("🔄 Запуск цикла стратегии...")
        
        try:
            # Анализ активов
            assets = self.analyze_assets()
            if not assets:
                logger.warning("❌ Нет активов для анализа")
                return
            
            self.asset_ranking = assets
            
            # Проверка хеджа и генерация сигналов
            signals = self.generate_signals(assets)
            
            # Отправка сигналов в Telegram
            if signals:
                self.send_signals_to_telegram(signals)
                logger.info(f"📨 Отправлено {len(signals)} сигналов")
            
            # Отправка отчета по расписанию
            if send_report:
                report = self.format_combined_report(assets)
                self.send_telegram_message(report, force=True)
                logger.info("📊 Отчет отправлен")
            
            # Сохранение сделок в CSV
            self.virtual_portfolio.save_trades_to_csv()
            self.save_state()
            
            logger.info(f"✅ Цикл завершен. Сигналов: {len(signals)}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле стратегии: {e}")
            logger.error(traceback.format_exc())

    def should_run_check_now(self) -> bool:
        """Проверка расписания"""
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        
        for check_time in self.check_times:
            try:
                check_dt = datetime.strptime(check_time, "%H:%M")
                current_dt = datetime.strptime(current_time, "%H:%M")
                
                # Разница в минутах
                diff = abs((current_dt - check_dt).total_seconds() / 60)
                if diff <= 5:  # В пределах 5 минут
                    logger.info(f"⏰ Время проверки: {current_time} (запланировано {check_time})")
                    return True
            except Exception as e:
                logger.error(f"Ошибка парсинга времени: {e}")
        
        return False

    def should_send_report_now(self) -> bool:
        """Проверка времени отчета"""
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        
        try:
            report_dt = datetime.strptime(self.report_time, "%H:%M")
            current_dt = datetime.strptime(current_time, "%H:%M")
            diff = abs((current_dt - report_dt).total_seconds() / 60)
            return diff <= 5
        except Exception as e:
            logger.error(f"Ошибка парсинга времени отчета: {e}")
            return False

    def _get_next_check_time(self) -> datetime:
        """Ближайшее время проверки"""
        now = datetime.now()
        times = []
        
        for t in self.check_times + [self.report_time]:
            try:
                dt = datetime.strptime(t, "%H:%M")
                dt = dt.replace(year=now.year, month=now.month, day=now.day)
                if dt < now:
                    dt += timedelta(days=1)
                times.append(dt)
            except Exception:
                continue
        
        return min(times) if times else now + timedelta(minutes=5)

    def run(self):
        """Основной цикл"""
        logger.info("🚀 Запуск бота C1 с хеджем")
        
        # Проверка конфигурации
        if not os.path.exists('sectors_config.json'):
            logger.error("❌ Файл sectors_config.json не найден!")
            self.send_telegram_message("❌ *ОШИБКА*: Файл sectors_config.json не найден", force=True)
            return
        
        # Приветственное сообщение
        welcome = (
            f"🚀 *MOMENTUM C1 HEDGE BOT ЗАПУЩЕН*\n"
            f"📈 Стратегия: C1 (ROC252, топ-10)\n"
            f"🛡️ Выход: B5 (ATR x{self.atr_multiplier}, SMA exit)\n"
            f"🔒 Хедж: SMA{self.hedge_sma_period}, порог {self.hedge_threshold*100}%\n"
            f"📅 Ребаланс: каждые {self.rebalance_days} дней\n"
            f"🕐 Расписание: проверки {self.check_times[0]}, {self.check_times[1]}, отчет {self.report_time}\n"
            f"📊 Виртуальный портфель: {self.virtual_portfolio.cash:.0f} RUB\n"
            f"📁 Лог сделок: logs/virtual_trades_c1.csv"
        )
        self.send_telegram_message(welcome, force=True)
        
        try:
            while True:
                now = datetime.now()
                
                if self.should_run_check_now() or self.should_send_report_now():
                    send_report = self.should_send_report_now()
                    self.run_strategy_cycle(send_report=send_report)
                
                # Сон до следующей проверки (не более 5 минут)
                next_check = self._get_next_check_time()
                sleep_seconds = min((next_check - now).total_seconds(), 300)
                
                if sleep_seconds > 0:
                    logger.debug(f"💤 Сон {sleep_seconds:.0f} сек до {next_check.strftime('%H:%M')}")
                    time.sleep(sleep_seconds)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Остановка по Ctrl+C")
            self.virtual_portfolio.save_trades_to_csv()
            self.save_state()
            self.send_telegram_message("🛑 *БОТ ОСТАНОВЛЕН*", force=True)
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            logger.error(traceback.format_exc())
            self.send_telegram_message(f"❌ *КРИТИЧЕСКАЯ ОШИБКА*\n{str(e)[:200]}", force=True)

# ========== КЛАССЫ ДЛЯ ДАННЫХ И ЗАГРУЗКИ ==========
@dataclass
class AssetDataC1:
    symbol: str
    name: str
    current_price: float
    roc252: float
    sma_fast: float
    sma_slow: float
    sma_entry: float
    sma_signal: bool
    atr: float
    sector: str
    source: str
    timestamp: datetime = field(default_factory=datetime.now)

class MOEXDataFetcherC1:
    """Загрузка данных с MOEX"""
    def __init__(self, bot: MomentumBotC1):
        self.bot = bot
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MomentumBotC1/1.0'})
        self.request_delay = 0.5
        self.max_retries = 3
        self._cache = {
            'historical_data': {},
            'benchmark': {'data': None, 'timestamp': None, 'ttl': 24*3600}
        }
        self.sectors_config = self._load_sectors_config()
    
    def _load_sectors_config(self) -> Dict:
        """Загрузка конфигурации секторов (только для информации)"""
        try:
            if os.path.exists('sectors_config.json'):
                with open('sectors_config.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки sectors_config.json: {e}")
        return {'sectors': {}, 'default_sector': 'Другое'}
    
    def get_top_assets(self) -> List[Dict]:
        """Получение списка акций из конфига (без секторных квот)"""
        assets = []
        sectors = self.sectors_config.get('sectors', {})
        
        for sector_name, sector_data in sectors.items():
            for stock in sector_data.get('stocks', []):
                assets.append({
                    'symbol': stock['Ticker'].upper(),
                    'name': stock.get('Name', stock['Ticker']),
                    'sector': sector_name,
                    'source': 'config'
                })
        
        logger.info(f"📊 Загружено {len(assets)} акций из конфига")
        return assets
    
    def get_current_price(self, symbol: str) -> Tuple[Optional[float], Optional[float], str]:
        """Получение текущей цены"""
        for attempt in range(self.max_retries):
            try:
                # Пробуем TQBR (акции)
                for board in ['TQBR', 'SNDX']:
                    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}/securities/{symbol}.json"
                    resp = self.session.get(url, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        marketdata = data.get('marketdata', {}).get('data', [])
                        if marketdata:
                            row = marketdata[0]
                            cols = data.get('marketdata', {}).get('columns', [])
                            if 'LAST' in cols:
                                idx = cols.index('LAST')
                                price = row[idx]
                                if price is not None:
                                    try:
                                        price_float = float(price)
                                        if price_float > 0:
                                            return price_float, 0, f'moex_{board}'
                                    except (ValueError, TypeError):
                                        pass
                
                time.sleep(self.request_delay)
                
            except Exception as e:
                logger.debug(f"Ошибка получения цены {symbol}: {e}")
        
        return None, 0, ''
    
    def get_cached_historical_data(self, symbol: str, days: int = 400) -> Optional[pd.DataFrame]:
        """Кэшированные исторические данные"""
        cache_key = f"{symbol}_{days}"
        
        if cache_key in self._cache['historical_data']:
            cache = self._cache['historical_data'][cache_key]
            if (datetime.now() - cache['timestamp']).total_seconds() < cache['ttl']:
                return cache['data']
        
        df = self._fetch_historical_data(symbol, days)
        if df is not None:
            self._cache['historical_data'][cache_key] = {
                'data': df,
                'timestamp': datetime.now(),
                'ttl': 24*3600
            }
        return df
    
    def _fetch_historical_data(self, symbol: str, days: int) -> Optional[pd.DataFrame]:
        """Запрос исторических данных"""
        end = datetime.now()
        start = end - timedelta(days=days)
        
        # Пробуем apimoex
        if HAS_APIMOEX:
            try:
                data = apimoex.get_board_candles(
                    self.session,
                    security=symbol,
                    board='TQBR',
                    interval=24,
                    start=start.strftime('%Y-%m-%d'),
                    end=end.strftime('%Y-%m-%d')
                )
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    df = df.rename(columns={'end': 'timestamp'})
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    for col in ['open', 'close', 'high', 'low']:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    df = df.dropna(subset=['close'])
                    return df
            except Exception as e:
                logger.debug(f"apimoex error for {symbol}: {e}")
        
        # Резервный метод
        url = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}/candles.json"
        params = {
            'from': start.strftime('%Y-%m-%d'),
            'till': end.strftime('%Y-%m-%d'),
            'interval': 24,
            'candles.columns': 'open,close,high,low,value,volume,end'
        }
        
        try:
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                candles = data.get('candles', {}).get('data', [])
                if candles:
                    df = pd.DataFrame(candles, columns=['open', 'close', 'high', 'low', 'value', 'volume', 'timestamp'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    for col in ['open', 'close', 'high', 'low']:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    df = df.dropna(subset=['close'])
                    return df
        except Exception as e:
            logger.debug(f"Fallback error for {symbol}: {e}")
        
        return None
    
    def calculate_asset_data(self, asset_info: Dict) -> Optional[AssetDataC1]:
        """Расчет ROC252, SMA и ATR для одной акции"""
        symbol = asset_info['symbol']
        df = self.get_cached_historical_data(symbol, 400)
        
        if df is None or len(df) < 252:
            logger.debug(f"⚠️ {symbol}: недостаточно исторических данных ({len(df) if df is not None else 0} < 252)")
            return None
        
        # Текущая цена
        current_price = df['close'].iloc[-1]
        if current_price <= 0:
            return None
        
        # ROC252: (close - close_252) / close_252 * 100
        close_252 = df['close'].iloc[-252] if len(df) >= 252 else df['close'].iloc[0]
        roc252 = ((current_price - close_252) / close_252) * 100
        
        # SMA
        sma_fast = df['close'].rolling(window=self.bot.sma_fast).mean().iloc[-1]
        sma_slow = df['close'].rolling(window=self.bot.sma_slow).mean().iloc[-1]
        sma_entry = df['close'].rolling(window=self.bot.sma_entry).mean().iloc[-1]
        sma_signal = sma_fast > sma_slow
        
        # ATR
        high = df['high']
        low = df['low']
        close = df['close']
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(window=self.bot.atr_period).mean().iloc[-1]
        atr = atr if not pd.isna(atr) else 0.0
        
        return AssetDataC1(
            symbol=symbol,
            name=asset_info.get('name', symbol),
            current_price=current_price,
            roc252=roc252,
            sma_fast=sma_fast,
            sma_slow=sma_slow,
            sma_entry=sma_entry,
            sma_signal=sma_signal,
            atr=atr,
            sector=asset_info.get('sector', 'Другое'),
            source=asset_info.get('source', 'moex')
        )
    
    def get_benchmark_data(self) -> Optional[Dict]:
        """Данные бенчмарка MCFTR"""
        cache = self._cache['benchmark']
        if cache['data'] and (datetime.now() - cache['timestamp']).total_seconds() < cache['ttl']:
            return cache['data']
        
        df = self.get_cached_historical_data(self.bot.benchmark_symbol, 400)
        if df is None or len(df) < 252:
            logger.warning("⚠️ Недостаточно данных для бенчмарка MCFTR")
            return None
        
        current = df['close'].iloc[-1]
        close_252 = df['close'].iloc[-252] if len(df) >= 252 else df['close'].iloc[0]
        roc252 = ((current - close_252) / close_252) * 100
        
        data = {
            'symbol': self.bot.benchmark_symbol,
            'current_price': current,
            'roc252': roc252,
            'timestamp': datetime.now()
        }
        
        self._cache['benchmark'] = {
            'data': data, 
            'timestamp': datetime.now(), 
            'ttl': 24*3600
        }
        
        return data

# ========== ЗАПУСК ==========
def main():
    bot = MomentumBotC1()
    bot.run()

if __name__ == "__main__":
    main()