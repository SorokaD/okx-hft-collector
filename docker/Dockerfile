
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir pydantic pydantic-settings aiohttp orjson uvloop prometheus-client httpx asyncpg tenacity structlog

COPY src ./src
ENV PYTHONPATH=/app/src

CMD ["python", "-m", "okx_hft.run"]
