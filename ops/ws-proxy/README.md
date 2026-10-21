# bybit-ws-proxy

Локальный WebSocket-прокси: к Bybit подключается с `permessage-deflate`,
коллекторам отдаёт обычный WS на `ws://127.0.0.1:8765/{linear|spot}`.

```bash
cd ops/ws-proxy
npm install --omit=dev
sudo cp ops/systemd/system/bybit-ws-proxy.service /etc/systemd/system/
sudo systemctl enable --now bybit-ws-proxy
```

Коллекторы включают прокси через `BYBIT_WS_PROXY=ws://127.0.0.1:8765`.
Если переменная не задана — они ходят напрямую в Bybit (без сжатия).
