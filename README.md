docker compose up -d  
docker ps -a  
docker compose down  

| Сервис         | URL                                               | Описание                             |
| -------------- | ------------------------------------------------- | ------------------------------------ |
| **Tabix**      | [http://localhost:8080](http://localhost:8080)    | Веб-клиент для ClickHouse            |
| **MLflow**     | [http://localhost:5000](http://localhost:5000)    | Интерфейс экспериментов ML           |
| **Airflow**    | [http://localhost:8081](http://localhost:8081)    | Планировщик пайплайнов               |
| **Superset**   | [http://localhost:8088](http://localhost:8088)    | BI-панель (не стартует из-за ошибки) |
| **ClickHouse** | `native: localhost:9000` / `http: localhost:8123` | SQL-интерфейс / REST                 |
| **MinIo**      | http://localhost:9003 (логин/пароль из .env)      | Консоль                              |
| **MinIo**      | http://localhost:9002                             | S3-endpoint для SDK                  |

