import pandas as pd
import datetime as dt
import os
import requests
import yfinance as yf
import time

# Устанавливаем диапазон дат
end_date = dt.date.today()
start_date = end_date - dt.timedelta(days=365)

# Списки активов
crypto_ids = [
    "bitcoin", "ethereum", "tether", "binancecoin", "solana"
]

stock_symbols = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL',
    'META', 'BRK-B', 'TSLA', 'LVMUY', 'JPM'
]

# Защита от 429 (сделали 5 крипто токенов, потому что coingecko выдавал лимит запросов и после 5 все ломалось)
def get_with_retry(url, params=None, retries=3, delay=2):
    for _ in range(retries):
        response = requests.get(url, params=params)
        if response.status_code == 429:
            time.sleep(delay)
            delay *= 2
        else:
            return response
    raise Exception("Ошибка 429: превышен лимит запросов")

# Получение исторических цен криптовалют
def fetch_crypto_data():
    crypto_df = pd.DataFrame()

    for crypto_id in crypto_ids:
        url = f'https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart'
        params = {
            'vs_currency': 'usd',
            'days': '365',
            'interval': 'daily'
        }

        response = get_with_retry(url, params=params)
        if response.status_code == 200:
            data = response.json()
            prices = data['prices']
            df = pd.DataFrame(prices, columns=['timestamp', crypto_id])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            crypto_df = pd.concat([crypto_df, df], axis=1)
            time.sleep(1.2)  # Пауза между вызовами API
        else:
            print(f"Ошибка при получении данных для {crypto_id}")
    return crypto_df

# Получение исторических цен акций
def fetch_stock_data():
    stock_df = pd.DataFrame()
    for symbol in stock_symbols:
        df = yf.download(symbol, start=start_date, end=end_date)['Close']
        df.name = symbol
        stock_df = pd.concat([stock_df, df], axis=1)
    stock_df.index.name = 'timestamp'  # Переименовываем индекс
    return stock_df

# Основной скрипт
def main():
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)

    print("Сбор данных криптовалют...")
    crypto_df = fetch_crypto_data()
    crypto_df.to_csv('data/raw/crypto_prices.csv')

    print("Сбор данных акций...")
    stock_df = fetch_stock_data()
    stock_df.to_csv('data/raw/stock_prices.csv')

    print("Объединение и сохранение данных...")
    # Объединение по индексу времени
    merged_df = pd.merge(crypto_df, stock_df, left_index=True, right_index=True, how='inner')
    merged_df.reset_index(inplace=True)  # Возвращаем timestamp как колонку
    merged_df.to_csv('data/processed/merged_prices.csv', index=False)

if __name__ == "__main__":
    main()
