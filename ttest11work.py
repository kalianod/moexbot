import os
import time
from dotenv import load_dotenv
from t_tech.invest import SubscribeOrderBookRequest, OrderBookInstrument, MarketDataRequest, SubscriptionAction
from t_tech.invest.utils import quotation_to_decimal
from t_tech.invest.sandbox.client import SandboxClient
from decimal import Decimal, ROUND_HALF_UP

# Загружаем переменные из .env файла
load_dotenv()

# Берем токен из .env файла
TOKEN = os.getenv("TINKOFF_TOKEN") or os.getenv("INVEST_TOKEN")

def main():
    if not TOKEN:
        print("ОШИБКА: Токен не найден в .env файле")
        print("Создайте файл .env в той же папке со строкой:")
        print("TINKOFF_TOKEN=t.ваш_реальный_токен")
        return
    
    print(f"Используется токен: {TOKEN[:15]}...")  # Показываем часть токена
    
    bid = ask = bid_lot = ask_lot = None
    def request_iterator():
        yield MarketDataRequest(subscribe_order_book_request=SubscribeOrderBookRequest(
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            instruments=[OrderBookInstrument(figi="BBG004730N88", depth=1)]))

        while True:
            time.sleep(1)
    
    print("Подключаемся к Tinkoff API...")
    while True:
        with SandboxClient(TOKEN) as client:
            for marketdata in client.market_data_stream.market_data_stream(
                    request_iterator()
            ):
                print(f"Вывод в терминал всего сообщения.")
                print(marketdata.orderbook)
                if marketdata.orderbook is not None:
                    if marketdata.orderbook.figi == "BBG004730N88":
                        bid = quotation_to_decimal(marketdata.orderbook.bids[0].price).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        ask = quotation_to_decimal(marketdata.orderbook.asks[0].price).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        bid_lot = marketdata.orderbook.bids[0].quantity
                        ask_lot = marketdata.orderbook.asks[0].quantity

                    if bid is not None and ask is not None and bid_lot is not None and ask_lot is not None:
                        print(f"В стакане акции SBER сейчас лучшие bid и ask:")
                        print(f"bid: цена {bid}, лотов {bid_lot}.")
                        print(f"ask: цена {ask}, лотов {ask_lot}.\n")

if __name__ == "__main__":
    main()
