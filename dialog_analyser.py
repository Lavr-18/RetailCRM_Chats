# dialog_analyser.py

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
    logger.info("Начало анализа диалога с помощью OpenAI.")
    prompt = PROMPT_TEMPLATE.format(
        ", ".join([f"'{cat}'" for cat in categories]),
        dialog_text
    )
    logger.debug(f"Prompt для OpenAI: {prompt[:200]}...")
    logger.debug(f"Полный prompt для OpenAI:\n{prompt}")

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
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
    # Обновленные классы исключений для v1.x.x
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