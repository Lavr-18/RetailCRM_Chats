FROM python:3.11-slim

# Установка Cron и утилит для отладки
# Добавляем less и nano для удобного просмотра файлов внутри контейнера
RUN apt-get update && apt-get install -y cron less nano bash && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Настраиваем среду для cron (важно для корректного запуска скриптов)
ENV PATH="/usr/local/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь остальной код, включая .env, который будет использоваться скриптом
COPY . .

# Разрешаем запуск Python скриптов
RUN chmod +x main.py report_generator.py

# =======================================================
# ШАГ 1: Создание скрипта-обертки run_report.sh
# Этот скрипт принудительно экспортирует все переменные из .env в окружение Bash,
# а затем запускает Python. Это гарантирует, что OPENAI_API_KEY будет доступен.
# =======================================================
RUN echo '#!/bin/bash' > /app/run_report.sh \
    && echo 'set -a' >> /app/run_report.sh \
    && echo 'source /app/.env' >> /app/run_report.sh \
    && echo 'set +a' >> /app/run_report.sh \
    && echo '/usr/local/bin/python /app/report_generator.py' >> /app/run_report.sh \
    && chmod +x /app/run_report.sh

# =======================================================
# ШАГ 2: Настройка CRON на запуск скрипта-обертки
# =======================================================
RUN echo '30 23 * * * cd /app && /bin/bash /app/run_report.sh >> /var/log/cron.log 2>&1' | crontab -

# Команда по умолчанию для контейнера listener (она будет переопределена в docker-compose)
CMD ["python", "main.py"]
