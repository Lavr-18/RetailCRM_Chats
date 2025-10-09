FROM python:3.11-slim

# Установка утилит для отладки (cron больше не нужен)
RUN apt-get update && apt-get install -y less nano bash && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Настраиваем среду (PATH)
ENV PATH="/usr/local/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код
COPY . .

# Разрешаем запуск Python скриптов
# chmod +x main.py report_generator.py

# Команда по умолчанию для контейнера: запуск главного слушателя,
# который теперь содержит логику отчетов в отдельном потоке.
CMD ["python", "main.py"]
