# NARaz v1.5 architecture

1. Android WebView loads the NARaz interface from local assets.
2. HTTP API handles authentication, wallet, blockchain and testnet trading.
3. Market Gateway maintains a persistent Binance public WebSocket connection.
4. Gateway normalizes ticker, depth20, aggTrade and 1m kline events.
5. NARaz Network WebSocket broadcasts snapshots and live events to connected clients.
6. Market and Trade are intentionally separate in the UI and in the data model.

External market data is observational. Testnet order execution is local and must not be described as real external execution.
