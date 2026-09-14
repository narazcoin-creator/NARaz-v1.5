# NARaz v1.5

NARaz v1.5 — полноценное продолжение базового интерфейса NARaz v1.3, с усиленным Market/Trade разделением.

## Что добавлено
- Отдельный **Market** экран: overview, 24h change, high/low, volume, quote volume, gainers/losers.
- Отдельный **Trade** экран: market/limit orders, внешний публичный стакан, live trades, 1m chart.
- Реальный public market stream Binance через WebSocket:
  - ticker
  - depth20
  - aggTrade
  - kline 1m
- Внутренний **NARaz Network WebSocket** раздаёт нормализованные данные приложению.
- Автоматическое переподключение при обрыве.
- REST Binance остаётся только fallback/initial data source.
- Home получил информационные promo-блоки и NARaz Academy.
- Wallet и Blockchain сохранены.
- Android-приложение WebView включено в проект; v1.5 — versionCode 5.
- GitHub Actions собирает Debug APK.

## Важное разделение
**Market = наблюдение за внешним рынком.**
Данные приходят из Binance public market stream и не рисуются симулятором.

**Trade = тестнет-исполнение NARaz.**
Market/Limit ордера пока меняют локальные testnet balances. Это НЕ отправка реального ордера на Binance и НЕ покупка реального BTC/ETH.

## Схема
Internet
→ Binance Public WebSocket
→ NARaz Market Gateway
→ NARaz Network WebSocket
→ Android App

## Запуск backend
```bash
cd backend
pip install -r requirements.txt
python server.py
```

HTTP API: `8080`
NARaz Network WebSocket: `8081`

На физическом Android устройстве укажи в Settings IP компьютера в LAN, например `http://192.168.1.10:8080`.

## Demo
username: `demo`
password: `demo12345`

## Граница версии
Это функциональный testnet foundation, а не production cryptocurrency exchange. Для production нужны полноценные криптографические подписи транзакций, P2P/консенсус, secure key custody, audit, AML/KYC, real custody/settlement и отдельный production execution gateway.
