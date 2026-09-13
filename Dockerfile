FROM python:3.11-slim

WORKDIR /app

# Системные зависимости (минимум для aiosqlite и сборки некоторых пакетов)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости отдельно (для кэширования)
COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Открываем порт
EXPOSE 8000

# Запуск бота
CMD ["python", "bot.py"]
