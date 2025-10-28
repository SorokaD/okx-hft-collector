# OKX HFT Collector
upd 23.10.25 Все работает в связке с infra, настроены алерты в tg при падении collector
upd 25.10.25 Все работает, в clickhouse собираются 3 таблицы 
upd 28.10.25 Все работает, в clickhouse собираются 5 таблиц, работают alerts (тормоз), нет конфликта с портами okx_hft_infra

Высокочастотный коллектор данных с биржи OKX для сбора торговых данных и стаканов заявок в реальном времени.

## 🚀 Возможности

- **WebSocket подключение** к OKX для получения данных в реальном времени
- **Сбор торговых данных** (trades) для BTC-USDT и ETH-USDT
- **Сбор данных стакана заявок** (orderbook) 
- **Хранение в ClickHouse** с оптимизированной структурой
- **Мониторинг и алерты** через Prometheus + Alertmanager
- **Метрики производительности** в реальном времени
- **Батчинг данных** для оптимизации производительности
- **Автоматическое переподключение** при разрыве соединения

## 📊 Архитектура

```
OKX WebSocket → Collector → ClickHouse
                     ↓
              Prometheus → Alertmanager → Уведомления
```

## 🛠 Установка и запуск

### Требования

- Docker и Docker Compose
- 8+ GB свободного места на диске
- 4+ GB RAM

### Быстрый старт

1. **Клонируйте репозиторий:**
```bash
git clone <repository-url>
cd okx-hft-collector
```

2. **Запустите систему:**
```bash
cd docker
docker-compose up -d
```

3. **Проверьте статус:**
```bash
docker-compose ps
```

### Компоненты системы

| Сервис | Порт | Описание |
|--------|------|----------|
| ClickHouse | 8123, 9000 | База данных для хранения рыночных данных |
| Collector | 9108 | Основной сервис сбора данных |
| Prometheus | 9090 | Сбор и хранение метрик |
| Alertmanager | 9093 | Обработка и отправка алертов |

## 📈 Мониторинг

### Веб-интерфейсы

- **Prometheus**: http://localhost:9090
- **Alertmanager**: http://localhost:9093  
- **Метрики коллектора**: http://localhost:9108/metrics
- **ClickHouse**: http://localhost:8123

### Ключевые метрики

```prometheus
# Количество событий по каналам
events_total{channel="trades",instId="BTC-USDT"}

# Переподключения WebSocket
reconnects_total

# Задержка event loop
event_loop_lag_ms

# Количество записей в ClickHouse
SELECT COUNT(*) FROM market_raw.trades
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
| WebSocketConnectionDown | `increase(reconnects_total[5m]) > 0` | Critical | Разрыв соединения с OKX |
| NoDataReceived | `rate(events_total[5m]) == 0` | Critical | Отсутствие данных от OKX |
| HighEventLoopLag | `event_loop_lag_ms > 100` | Warning | Высокая задержка обработки |
| DataInsertionErrors | `increase(events_total{channel="unknown"}[5m]) > 10` | Warning | Ошибки вставки данных |
| CollectorDown | `up{job="okx-collector"} == 0` | Critical | Коллектор недоступен |
| ClickHouseDown | `up{job="clickhouse"} == 0` | Critical | ClickHouse недоступен |

### Настройка уведомлений

#### Telegram (рекомендуется)

1. Создайте бота через @BotFather в Telegram
2. Получите токен бота и chat ID
3. Создайте файл `.env` в папке `docker/`:
```bash
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

4. Запустите webhook для уведомлений:
```bash
docker-compose up -d alert-webhook
```

#### Email уведомления

Отредактируйте `docker/alertmanager.yml`:
```yaml
receivers:
  - name: 'email-alerts'
    email_configs:
      - to: 'admin@example.com'
        subject: 'OKX Collector Alert'
        smtp_config:
          smarthost: 'smtp.gmail.com:587'
          auth_username: 'your_email@gmail.com'
          auth_password: 'your_app_password'
```

## 📊 Структура данных

### Таблица trades

```sql
CREATE TABLE market_raw.trades (
    instId String,           -- Инструмент (BTC-USDT, ETH-USDT)
    ts_event_ms UInt64,      -- Время события от OKX (миллисекунды)
    tradeId String,          -- ID сделки
    px Float64,              -- Цена
    sz Float64,              -- Размер
    side String,             -- Сторона (buy/sell)
    ts_ingest_ms UInt64      -- Время получения данных (миллисекунды)
) ENGINE = MergeTree()
ORDER BY (instId, ts_event_ms, tradeId)
```


## ⚙️ Конфигурация

### Переменные окружения

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `INSTRUMENTS` | `["BTC-USDT","ETH-USDT"]` | Список инструментов |
| `CHANNELS` | `["books-l2-tbt","trades"]` | Каналы данных |
| `OKX_WS_URL` | `wss://ws.okx.com:8443/ws/v5/public` | URL WebSocket OKX |
| `CLICKHOUSE_DSN` | `http://clickhouse:8123` | DSN ClickHouse |
| `BATCH_MAX_SIZE` | `5000` | Максимальный размер батча |
| `FLUSH_INTERVAL_MS` | `150` | Интервал принудительной отправки (мс) |
| `METRICS_PORT` | `9108` | Порт для метрик |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

### Настройка производительности

#### Увеличение пропускной способности

```yaml
# В docker-compose.yml
environment:
  BATCH_MAX_SIZE: "10000"      # Больше записей в батче
  FLUSH_INTERVAL_MS: "100"     # Чаще отправка
```

#### Уменьшение нагрузки

```yaml
# Сбор только торговых данных
CHANNELS: '["trades"]'

# Меньше инструментов
INSTRUMENTS: '["BTC-USDT"]'
```

## 📊 Объем данных

### Ожидаемый объем за сутки

| Тип данных | Записей/сек | Записей/день | Размер |
|------------|-------------|--------------|---------|
| Trades | ~175 | ~15M | ~1.5 GB |
| Orderbook | ~55 | ~4.7M | ~2.4 GB |
| **Итого** | ~230 | ~20M | **~4 GB** |

### За месяц: ~120 GB
### За год: ~1.5 TB

## 🔧 Управление системой

### Основные команды

```bash
# Запуск всех сервисов
docker-compose up -d

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
# Проверка ClickHouse
curl "http://localhost:8123/?query=SELECT%20version()"

# Проверка метрик коллектора
curl "http://localhost:9108/metrics"

# Количество записей в базе
curl "http://localhost:8123/?query=SELECT%20COUNT(*)%20FROM%20market_raw.trades"
```

### Очистка данных

```bash
# Остановка системы
docker-compose down

# Удаление данных ClickHouse
docker volume rm docker_ch-data

# Перезапуск
docker-compose up -d
```

## 🐛 Устранение неполадок

### Проблемы с подключением

1. **Коллектор не подключается к ClickHouse:**
```bash
# Проверьте логи
docker-compose logs collector

# Проверьте статус ClickHouse
curl "http://localhost:8123/?query=SELECT%20version()"
```

2. **Нет данных в ClickHouse:**
```bash
# Проверьте логи коллектора
docker-compose logs --tail=50 collector

# Проверьте метрики
curl "http://localhost:9108/metrics" | grep events_total
```

### Проблемы с производительностью

1. **Высокая задержка event loop:**
   - Увеличьте `BATCH_MAX_SIZE`
   - Уменьшите `FLUSH_INTERVAL_MS`
   - Отключите канал orderbook

2. **Много переподключений:**
   - Проверьте стабильность интернет-соединения
   - Увеличьте таймауты в коде

### Проблемы с алертами

1. **Алерты не приходят:**
```bash
# Проверьте статус Alertmanager
curl "http://localhost:9093/api/v2/status"

# Проверьте правила в Prometheus
# Перейдите в http://localhost:9090/alerts
```

## 📝 Логирование

### Уровни логирования

- `DEBUG` - Детальная отладочная информация
- `INFO` - Общая информация о работе (по умолчанию)
- `WARNING` - Предупреждения
- `ERROR` - Ошибки

### Просмотр логов

```bash
# Все логи
docker-compose logs

# Логи конкретного сервиса
docker-compose logs collector
docker-compose logs clickhouse

# Логи в реальном времени
docker-compose logs -f collector

# Последние N строк
docker-compose logs --tail=100 collector
```

## 🔒 Безопасность

### Рекомендации

1. **Используйте HTTPS** для внешних подключений
2. **Ограничьте доступ** к портам 9090, 9093 (Prometheus, Alertmanager)
3. **Регулярно обновляйте** Docker образы
4. **Мониторьте ресурсы** системы

### Брандмауэр

```bash
# Разрешить только необходимые порты
ufw allow 8123  # ClickHouse (если нужен внешний доступ)
ufw allow 9108  # Метрики коллектора
```

## 📚 Дополнительные ресурсы

- [OKX WebSocket API Documentation](https://www.okx.com/docs-v5/en/#websocket-api)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

## 🤝 Поддержка

При возникновении проблем:

1. Проверьте логи: `docker-compose logs`
2. Проверьте метрики: http://localhost:9108/metrics
3. Проверьте алерты: http://localhost:9093
4. Создайте issue в репозитории

## 📄 Лицензия

MIT License

---

**Версия**: 1.0.0  
**Последнее обновление**: Октябрь 2025