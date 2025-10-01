# main.py

import logging
from dialog_listener import start_listener

# Настройка логирования для главного файла
logging.basicConfig(
    level=logging.DEBUG, # Изменили уровень логирования на DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Запуск приложения...")
    start_listener()
