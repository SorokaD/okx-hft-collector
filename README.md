# OKX HFT Collector  

upd 20.12.24 Миграция на PostgreSQL/TimescaleDB, удалён ClickHouse
upd 20.12.24 Добавлен канал index-tickers для сбора index price

Высокочастотный коллектор данных с биржи OKX для сбора торговых данных и стаканов заявок в реальном времени.

## 🚀 Возможности

- **WebSocket подключение** к OKX для получения данных в реальном времени
- **Сбор торговых данных** (trades, funding-rate, mark-price, tickers, open-interest, index-tickers)
- **Сбор данных стакана заявок** (orderbook) 
- **Хранение в PostgreSQL/TimescaleDB** с оптимизированной структурой
- **Мониторинг и алерты** через Prometheus + Alertmanager
- **Метрики производительности** в реальном времени
- **Батчинг данных** для оптимизации производительности
- **Автоматическое переподключение** при разрыве соединения

## 📊 Архитектура

```
OKX WebSocket → Collector → PostgreSQL/TimescaleDB (167.86.110.201)
                     ↓
              Prometheus → Alertmanager → Telegram
```

## 🛠 Установка и запуск

### Требования

- Docker и Docker Compose
- PostgreSQL/TimescaleDB (внешний сервер)
- 4+ GB RAM

### Быстрый старт

1. **Клонируйте репозиторий:**
```bash
git clone <repository-url>
cd okx-hft-collector
```

2. **Настройте переменные окружения:**
```bash
cd docker
cp .env.example .env
# Отредактируйте .env файл
```

3. **Запустите систему:**
```bash
docker-compose up -d --build
```

4. **Проверьте статус:**
```bash
docker-compose ps
docker-compose logs -f collector
```

### Компоненты системы

| Сервис | Порт | Описание |
|--------|------|----------|
| Collector | 9108 | Основной сервис сбора данных + /metrics |
| Prometheus | 9104 | Сбор и хранение метрик |
| Alertmanager | 9095 | Обработка и отправка алертов |

## 📈 Мониторинг

### Веб-интерфейсы

- **Prometheus**: http://localhost:9104
- **Alertmanager**: http://localhost:9095  
- **Метрики коллектора**: http://localhost:9108/metrics

### Ключевые метрики

```prometheus
# Количество событий по каналам
events_total{channel="trades",instId="BTC-USDT-SWAP"}

# Переподключения WebSocket
reconnects_total

# Задержка event loop
event_loop_lag_ms
```

### Полезные запросы Prometheus

```promql
# Скорость получения данных
rate(events_total[5m])

# Количество переподключений за час
increase(reconnects_total[1h])

# Задержка обработки событий
event_loop_lag_ms
```

## 🚨 Система алертов

### Настроенные алерты

| Алерт | Условие | Серьезность | Описание |
|-------|---------|-------------|----------|
| WebSocketConnectionDown | `increase(reconnects_total[1m]) > 0` | Critical | Переподключение к OKX |
| NoDataReceived | `rate(events_total[1m]) == 0` | Critical | Отсутствие данных от OKX |
| HighEventLoopLag | `event_loop_lag_ms > 50` | Warning | Высокая задержка обработки |
| CollectorDown | `up{job="okx-collector"} == 0` | Critical | Коллектор недоступен |
| PostgreSQLDown | `pg_up == 0` | Critical | PostgreSQL недоступен |

### Настройка Telegram уведомлений

1. Создайте бота через @BotFather в Telegram
2. Получите токен бота и chat ID
3. Добавьте в `.env`:
```bash
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## 📊 Структура данных (PostgreSQL)

### Таблицы

| Таблица | Описание |
|---------|----------|
| `okx_raw.trades` | Сделки |
| `okx_raw.funding_rates` | Ставки финансирования |
| `okx_raw.mark_prices` | Mark price |
| `okx_raw.tickers` | Тикеры (bid/ask, volume) |
| `okx_raw.open_interest` | Открытый интерес |
| `okx_raw.index_tickers` | Index price (BTC-USDT, ETH-USDT) |
| `okx_raw.orderbook_snapshots` | Снапшоты стакана |
| `okx_raw.orderbook_updates` | Инкрементальные обновления стакана |

### Пример запросов

```sql
-- Последние сделки
SELECT * FROM okx_raw.trades 
WHERE instid = 'BTC-USDT-SWAP' 
ORDER BY ts_event_ms DESC LIMIT 10;

-- Index price
SELECT * FROM okx_raw.index_tickers 
WHERE instid = 'BTC-USDT' 
ORDER BY ts_event_ms DESC LIMIT 10;

-- Количество записей по таблицам
SELECT 'trades' as table_name, COUNT(*) FROM okx_raw.trades
UNION ALL
SELECT 'tickers', COUNT(*) FROM okx_raw.tickers
UNION ALL
SELECT 'index_tickers', COUNT(*) FROM okx_raw.index_tickers;
```

## ⚙️ Конфигурация

### Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `INSTRUMENTS` | `["BTC-USDT-SWAP","ETH-USDT-SWAP"]` | Список инструментов |
| `CHANNELS` | `["trades","funding-rate","mark-price","tickers","open-interest","books"]` | Каналы данных |
| `INDEX_INSTRUMENTS` | `["BTC-USDT","ETH-USDT"]` | Инструменты для index-tickers |
| `INDEX_CHANNELS` | `["index-tickers"]` | Каналы для индексных цен |
| `OKX_WS_URL` | `wss://ws.okx.com:8443/ws/v5/public` | URL WebSocket OKX |
| `POSTGRES_HOST` | `localhost` | Хост PostgreSQL |
| `POSTGRES_PORT` | `6432` | Порт PostgreSQL (PgBouncer) |
| `POSTGRES_USER` | - | Пользователь PostgreSQL |
| `POSTGRES_PASSWORD` | - | Пароль PostgreSQL |
| `POSTGRES_DB` | `okx_hft` | База данных |
| `POSTGRES_SCHEMA` | `okx_raw` | Схема |
| `BATCH_MAX_SIZE` | `5000` | Максимальный размер батча |
| `FLUSH_INTERVAL_MS` | `150` | Интервал принудительной отправки (мс) |
| `METRICS_PORT` | `9108` | Порт для метрик |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

## 🔧 Управление системой

### Основные команды

```bash
# Запуск всех сервисов
docker-compose up -d --build

# Остановка всех сервисов  
docker-compose down

# Перезапуск коллектора
docker-compose restart collector

# Просмотр логов
docker-compose logs -f collector

# Проверка статуса
docker-compose ps
```

### Проверка здоровья системы

```bash
# Проверка метрик коллектора
curl "http://localhost:9108/metrics"

# Проверка PostgreSQL
psql -h 167.86.110.201 -U postgres -d okx_hft -c "SELECT COUNT(*) FROM okx_raw.trades;"
```

## 📝 Логирование

### Уровни логирования

- `DEBUG` - Детальная отладочная информация
- `INFO` - Общая информация о работе (по умолчанию)
- `WARNING` - Предупреждения
- `ERROR` - Ошибки

### Просмотр логов

```bash
# Логи в реальном времени
docker-compose logs -f collector

# Последние N строк
docker-compose logs --tail=100 collector
```

## 📚 Дополнительные ресурсы

- [OKX WebSocket API Documentation](https://www.okx.com/docs-v5/en/#websocket-api)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [Prometheus Documentation](https://prometheus.io/docs/)

## 📄 Лицензия

MIT License
