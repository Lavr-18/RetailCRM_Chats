import os
import re
import logging
from datetime import datetime
from collections import Counter
import openai
import json
import config
from config import PROMPT_TEMPLATE

# Настройка логирования
logger = logging.getLogger(__name__)

# --- Константы и конфигурация для OpenAI ---
try:
    OPENAI_API_KEY = config.OPENAI_API_KEY
    if not OPENAI_API_KEY:
        logger.error("Переменная OPENAI_API_KEY пуста. Пожалуйста, проверьте файл .env.")
        exit(1)
except AttributeError:
    logger.error("В файле config.py отсутствует переменная OPENAI_API_KEY.")
    exit(1)

# Инициализируем клиент OpenAI с новым синтаксисом
client = openai.OpenAI(api_key=OPENAI_API_KEY)

# Рекомендация: gpt-4o-mini - самая надежная и экономичная модель для
# стабильного структурированного вывода (JSON) и резюмирования.
RECOMMENDED_MODEL = "gpt-5-mini"


def parse_openai_response(response: str) -> tuple[dict, str] | None:
    """
    Парсит ответ от OpenAI, разделяя JSON и SUMMARY.
    """
    logger.debug(f"Начало парсинга ответа OpenAI. Длина ответа: {len(response)} символов.")

    # Используем регулярное выражение для поиска блока с JSON
    json_match = re.search(r'```json\s*({.*?})\s*```', response, re.DOTALL)

    if not json_match:
        logger.error("В ответе OpenAI не найден блок JSON.")
        return None, None

    json_part = json_match.group(1).strip()

    # Теперь ищем разделитель для summary
    summary_match = re.search(r'---SUMMARY---(.*)', response, re.DOTALL)
    if not summary_match:
        logger.warning("В ответе OpenAI не найден разделитель '---SUMMARY---'.")
        summary_part = ""
    else:
        summary_part = summary_match.group(1).strip()

    try:
        # Пытаемся загрузить JSON
        json_data = json.loads(json_part)
        logger.debug("JSON успешно загружен.")
        return json_data, summary_part
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError: {e}. Пытаемся исправить JSON, заменяя одинарные кавычки на двойные.")
        # Попытка исправить распространенную ошибку с одинарными кавычками
        json_part = json_part.replace("'", "\"")
        try:
            json_data = json.loads(json_part)
            logger.info("JSON успешно исправлен и загружен.")
            return json_data, summary_part
        except json.JSONDecodeError as e_fixed:
            logger.error(f"Ошибка при исправлении и парсинге JSON: {e_fixed}", exc_info=True)
            return None, None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при парсинге ответа OpenAI: {e}", exc_info=True)
        return None, None


def analyze_dialog(dialog_text: str, categories: list) -> tuple[dict, str] | None:
    """
    Отправляет диалог в OpenAI API для анализа и возвращает структурированные данные и резюме.
    """
    logger.info(f"Начало анализа диалога с помощью OpenAI, модель: {RECOMMENDED_MODEL}.")
    prompt = PROMPT_TEMPLATE.format(
        ", ".join([f"'{cat}'" for cat in categories]),
        dialog_text
    )
    logger.debug(f"Prompt для OpenAI: {prompt[:200]}...")

    try:
        response = client.chat.completions.create(
            model=RECOMMENDED_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info("Запрос к OpenAI успешно выполнен.")

        raw_response = response.choices[0].message.content
        logger.debug(f"Raw OpenAI response:\n{raw_response}")

        json_data, summary = parse_openai_response(raw_response)

        if json_data and summary:
            logger.info("Анализ диалога завершен успешно.")
            return json_data, summary
        else:
            logger.warning("Анализ диалога не дал валидных данных. Возвращаем None.")
            return None, None
    except openai.APIError as e:
        logger.error(f"Ошибка API OpenAI: {e}")
        return None, None
    except openai.APIConnectionError as e:
        logger.error(f"Ошибка подключения к API OpenAI: {e}")
        return None, None
    except openai.RateLimitError as e:
        logger.error(f"Превышен лимит запросов к API OpenAI: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при работе с OpenAI: {e}", exc_info=True)
        return None, None


# --- Вспомогательные функции для работы с файлами ---
def parse_dialog_file(file_path: str) -> str | None:
    """
    Читает текстовый файл диалога и возвращает его содержимое.
    """
    logger.info(f"Начало чтения файла диалога: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.info("Файл диалога успешно прочитан.")
            return content
    except FileNotFoundError:
        logger.error(f"Файл не найден: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}", exc_info=True)
        return None


# --- ТЕСТОВЫЙ МОДУЛЬ (Вывод сырого ответа API) ---
if __name__ == "__main__":
    # Настройка логирования для тестового запуска
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("=======================================")
    logger.info("ЗАПУСК ТЕСТА: ПОЛУЧЕНИЕ СЫРОГО ОТВЕТА API")
    logger.info("=======================================")

    test_dialog = """
[2025-10-11T10:38:00.800851] КЛИЕНТ: Доброе утро, а азалия многолетняя же, как долго в году цветет?
[2025-10-11T10:40:33.993456] МЕНЕДЖЕР: Ирина, доброе утро! Да, все верно! В году она цветет 2-3 раза и ее продолжительность цветения составляет от двух до шести недель🌸
[2025-10-11T10:41:49.463346] КЛИЕНТ: Тогда я бы еще 2 кашпо такие же, как Ольге передала попросила собрать. Чтобы все 3 цвета были, малиновый, розовый и белый.
[2025-10-11T10:42:15.985703] КЛИЕНТ: Прсчитайте, пожалуйста. Все вместе можно будет привезти.
    """

    # Используем статические категории из config.py для теста
    test_categories = ['установление_контакта', 'выявление_потребностей', 'квалификация',
                       'презентация', 'возражение', 'отработка_возражения',
                       'проговорены_договоренности', 'закрытие_на_оплату']

    # Формируем полный промпт, как это делает analyze_dialog
    prompt = PROMPT_TEMPLATE.format(
        ", ".join([f"'{cat}'" for cat in test_categories]),
        test_dialog
    )

    try:
        logger.info(f"Отправка запроса на модель: {RECOMMENDED_MODEL}...")

        # Выполняем прямой вызов API
        response = client.chat.completions.create(
            model=RECOMMENDED_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        logger.info("Запрос к OpenAI успешно выполнен (HTTP 200 OK).")

        raw_response = response.choices[0].message.content

        logger.info("\n--- ПОЛНЫЙ СЫРОЙ ОТВЕТ ОТ API ---")
        # Выводим весь ответ, чтобы увидеть, где находится JSON
        print(raw_response)
        logger.info("--- КОНЕЦ СЫРОГО ОТВЕТА ---\n")

    except Exception as e:
        logger.error(f"Критическая ошибка при получении сырого ответа: {e}", exc_info=True)
