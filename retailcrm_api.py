import json

import requests
import logging
from typing import Dict, Any

# Импортируем конфиг для доступа к ключам API
try:
    import config
except ImportError:
    logging.error("Не удалось импортировать файл config.py.")
    exit(1)

# Настройка логирования
logger = logging.getLogger(__name__)

# URL для задач: /api/v5/tasks/create
API_URL = config.RETAILCRM_BASE_URL + "/api/v5/tasks/create"
API_KEY = config.RETAILCRM_API_KEY


def create_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Создает задачу в RetailCRM.

    Args:
        task_data (Dict[str, Any]): Словарь с данными задачи (text, datetime, performerId и т.д.).

    Returns:
        Dict[str, Any]: Ответ от RetailCRM API.
    """
    logger.info(f"Отправка запроса на создание задачи для менеджера ID: {task_data.get('performerId')}")

    # RetailCRM API требует передачи данных в виде JSON-строки в параметре 'task'
    # с указанием API-ключа в заголовках.
    headers = {
        'X-Api-Key': API_KEY,
        'Content-Type': 'application/x-www-form-urlencoded'  # RetailCRM часто предпочитает urlencoded
    }

    # Формируем тело запроса
    payload = {
        'task': task_data  # Данные задачи передаются как объект в поле 'task'
    }

    try:
        # Для RetailCRM часто проще передать данные в виде form-data
        response = requests.post(
            API_URL,
            data={'task': json.dumps(task_data)},
            headers={'X-Api-Key': API_KEY},
            timeout=10
        )
        response.raise_for_status()

        result = response.json()

        if result.get('success'):
            logger.info(f"✅ Задача успешно создана. ID: {result.get('id')}")
        else:
            logger.error(f"❌ API-ошибка при создании задачи. Ответ: {result}")

        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка HTTP-запроса при создании задачи: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка при создании задачи: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


# ----------------------------------------------------
# Добавляем функцию-обертку для удобного создания задачи
# (логика из вашего примера)
# ----------------------------------------------------

from datetime import datetime, timedelta
import pytz

# Устанавливаем часовой пояс Москвы, как в основном проекте
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def create_ad_hoc_avito_task(manager_external_id: str):
    """
    Создает задачу в RetailCRM для менеджера через 1 час,
    с уведомлением о новом обращении с Avito.
    RetailCRM API требует ID пользователя, а не external_id, но в RetailCRM
    external_id часто совпадает с ID, если не задан явно.
    Мы будем использовать external_id (строку) в качестве performerId,
    так как это более надежно в рамках Avito-диалога.
    """
    if not manager_external_id:
        logger.error("Невозможно поставить задачу: отсутствует external_id менеджера.")
        return

    # 1. Определяем время задачи: текущее время + 1 час
    now_moscow = datetime.now(MOSCOW_TZ)
    target_dt = now_moscow + timedelta(hours=1)

    # Округляем до ближайшей минуты и форматируем
    target_dt = target_dt.replace(second=0, microsecond=0)
    task_datetime_str = target_dt.strftime('%Y-%m-%d %H:%M')

    task_text = "Обращение с Авито: Создать заказ"
    task_commentary = (
        f"У вас было новое обращение с Авито ({now_moscow.strftime('%Y-%m-%d %H:%M:%S')} МСК). "
        f"Создайте заказ со способом оформления **Авито**."
    )

    logger.info(f"Попытка поставить задачу менеджеру External ID {manager_external_id} на {task_datetime_str} МСК.")

    # 2. Формируем тело задачи
    task_data = {
        'text': task_text,
        'commentary': task_commentary,
        'datetime': task_datetime_str,
        # Используем external_id, который API RetailCRM должен принять как performerId
        'performerId': manager_external_id,
        # Указываем тип задачи
        'type': 'call'  # Обычно используется для простых уведомлений
    }

    # 3. Отправляем запрос
    response = create_task(task_data)

    return response


# --- Тестовый модуль (опционально) ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("retailcrm_api.py запущен.")

    # !!! Тестовый запуск закомментирован.
    create_ad_hoc_avito_task('11')