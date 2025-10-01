# Dockerfile
FROM python:3.11-slim

# Установка Cron и утилит для отладки
# Добавляем less и nano для удобного просмотра файлов внутри контейнера
RUN apt-get update && apt-get install -y cron less nano && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Настраиваем среду для cron (важно для корректного запуска скриптов)
ENV PATH="/usr/local/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код
COPY . .

# Разрешаем запуск Python скриптов для cron (не обязательно, но полезно)
RUN chmod +x main.py report_generator.py

# Команда по умолчанию для контейнера listener (она будет переопределена в docker-compose)
CMD ["python", "main.py"]