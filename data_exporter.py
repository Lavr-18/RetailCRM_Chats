import logging
import os
import sys
import json
import requests
import datetime
import re

# Добавляем корневую директорию проекта в sys.path для импорта других модулей
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Импортируем модули
from dialog_analyser import analyze_dialog
import config

# Настройка логирования для этого модуля
logger = logging.getLogger(__name__)


# --- Вспомогательные функции ---

def normalize_phone(phone_str: str) -> str:
    """
    Нормализует номер телефона к формату '7XXXXXXXXXX' (только цифры).
    Удаляет все нецифровые символы и заменяет начальную '8' на '7'.
    """
    logger.debug(f"Начало нормализации номера телефона: {phone_str}")
    digits_only = re.sub(r'\D', '', phone_str)
    if digits_only.startswith('8') and len(digits_only) == 11:
        normalized = '7' + digits_only[1:]
        logger.info(f"Номер {phone_str} нормализован в {normalized}.")
        return normalized
    elif digits_only.startswith('7') and len(digits_only) == 11:
        logger.info(f"Номер {phone_str} уже в нужном формате.")
        return digits_only
    # Дополнительная обработка для российских мобильных (без +7)
    elif digits_only.startswith('9') and len(digits_only) == 10:
        normalized = '7' + digits_only
        logger.info(f"Номер {phone_str} нормализован в {normalized}.")
        return normalized
    logger.warning(f"Не удалось нормализовать номер телефона: {phone_str}")
    return ""


def move_dialog_to_closed(dialog_id: int, client_phone: str):
    """
    Перемещает файл диалога из папки 'active' в 'closed'.
    """
    active_dir = 'dialogs/active'
    closed_dir = 'dialogs/closed'
    if not os.path.exists(closed_dir):
        os.makedirs(closed_dir)
        logger.info(f"Создана директория для закрытых диалогов: {closed_dir}")

    # Изменено: теперь имя файла включает номер телефона
    file_name = f'dialog_{dialog_id}_{client_phone}.txt'
    active_path = os.path.join(active_dir, file_name)
    closed_path = os.path.join(closed_dir, file_name)

    if os.path.exists(active_path):
        try:
            os.rename(active_path, closed_path)
            logger.info(f"✅ Диалог {dialog_id} успешно перемещен в закрытые: {closed_path}")
        except Exception as e:
            logger.error(f"❌ Ошибка при перемещении файла {active_path}: {e}", exc_info=True)
    else:
        logger.warning(f"Файл диалога {active_path} не найден. Пропускаем перемещение.")


def get_latest_order_details_from_phone(phone_number: str) -> dict | None:
    """
    Ищет заказы в RetailCRM по номеру телефона и возвращает полный
    JSON-объект самого нового заказа или None, если заказ не найден.
    """
    logger.info(f"Начинаем поиск заказа в RetailCRM по номеру телефона: {phone_number}")
    normalized_phone = normalize_phone(phone_number)
    if not normalized_phone:
        logger.warning("Нормализованный номер телефона пуст. Невозможно найти заказ.")
        return None

    try:
        url = f"{config.RETAILCRM_BASE_URL}/api/v5/orders"
        headers = {
            'X-Api-Key': config.RETAILCRM_API_KEY
        }
        params = {
            'filter[customer]': normalized_phone
        }
        logger.debug(f"Отправка запроса в RetailCRM: URL={url}, params={params}")
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        logger.debug("Запрос к RetailCRM успешен.")

        data = response.json()
        if not data.get('orders'):
            logger.info(f"Заказы для клиента с номером {normalized_phone} не найдены.")
            return None

        # Сортируем заказы по дате создания в убывающем порядке, чтобы получить самый новый
        sorted_orders = sorted(data['orders'], key=lambda x: x.get('createdAt', ''), reverse=True)
        latest_order = sorted_orders[0]

        logger.info(f"Найден самый новый заказ с ID: {latest_order.get('externalId', 'Неизвестно')}")
        return latest_order

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при поиске заказа в RetailCRM: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка при получении ID заказа: {e}", exc_info=True)
        return None


def get_manager_details_from_id(manager_id: int) -> dict | None:
    """
    Получает информацию о менеджере по его ID.
    """
    logger.info(f"Начинаем поиск информации о менеджере по ID: {manager_id}")
    try:
        url = f"{config.RETAILCRM_BASE_URL}/api/v5/users/{manager_id}"
        headers = {
            'X-Api-Key': config.RETAILCRM_API_KEY
        }
        logger.debug(f"Отправка запроса в RetailCRM: URL={url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.debug("Запрос к RetailCRM успешен.")

        data = response.json()
        if not data.get('user'):
            logger.warning(f"Информация о менеджере с ID {manager_id} не найдена.")
            return None

        logger.info(f"Информация о менеджере успешно получена.")
        return data.get('user')

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при поиске информации о менеджере: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка при получении информации о менеджере: {e}", exc_info=True)
        return None


def send_to_google_forms(data: dict):
    """
    Отправляет проанализированные данные в Google-таблицу через Google Forms.
    """
    logger.info("Начало отправки данных в Google Forms.")
    logger.debug(f"Отправляемые данные: {data}")
    try:
        response = requests.post(config.GOOGLE_FORMS_URL, data=data, timeout=10)
        response.raise_for_status()
        logger.info("✅ Данные успешно отправлены в Google Forms.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке данных в Google Forms: {e}", exc_info=True)


def send_to_telegram(summary: str):
    """
    Отправляет краткое резюме диалога в Telegram-группу с поддержкой тем.
    """
    logger.info("Начало отправки резюме в Telegram.")
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'message_thread_id': config.TELEGRAM_TOPIC_ID,
        'text': summary,
        'parse_mode': 'HTML'
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logger.info("✅ Резюме успешно отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке в Telegram: {e}", exc_info=True)


# --- Основная логика обработки и экспорта ---

def process_and_export_data(dialog_id: int, client_phone: str):
    """
    Центральная функция для обработки и экспорта данных закрытого диалога.
    Принимает номер телефона клиента.
    """
    logger.info(f"=== Начало обработки закрытого диалога {dialog_id} ===")

    # Изменено: теперь имя файла включает номер телефона
    file_name = f'dialog_{dialog_id}_{client_phone}.txt'
    active_path = os.path.join('dialogs', 'active', file_name)
    closed_path = os.path.join('dialogs', 'closed', file_name)

    dialog_text = ""
    file_path = None

    # 1. Сначала пытаемся найти файл в папке 'active'
    if os.path.exists(active_path):
        file_path = active_path
        logger.info(f"Загрузка текста диалога из файла {file_path}")
    # 2. Если не нашли, ищем в папке 'closed'
    elif os.path.exists(closed_path):
        file_path = closed_path
        logger.info(f"Загрузка текста диалога из файла {file_path}")
    else:
        logger.warning(f"Файл диалога {file_name} не найден ни в active, ни в closed. Пропускаем обработку.")
        return

    # 3. Загружаем текст диалога из найденного файла
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            dialog_text = f.read()
        logger.info(f"Текст диалога успешно загружен.")
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}", exc_info=True)
        return

    # 4. Получаем детали последнего заказа
    order_details = get_latest_order_details_from_phone(client_phone)
    order_link = 'Неизвестно'
    total_summ = 'Неизвестно'
    customer_type = 'Неизвестно'
    manager_name = 'Неизвестно'

    if order_details:
        order_link = f"{config.RETAILCRM_BASE_URL}/orders/{order_details.get('slug', 'Неизвестно')}/edit"
        total_summ = order_details.get('totalSumm', 'Неизвестно')
        if order_details.get('orderType') == 'b2b':
            customer_type = 'Юридическое лицо'
        else:
            customer_type = 'Физическое лицо'

        manager_id = order_details.get('managerId')
        if manager_id:
            manager_details = get_manager_details_from_id(manager_id)
            if manager_details:
                first_name = manager_details.get('firstName', '')
                last_name = manager_details.get('lastName', '')
                manager_name = f"{first_name} {last_name}".strip()

    # 5. Анализируем диалог с помощью OpenAI
    try:
        openai_json_data, summary = analyze_dialog(dialog_text, config.CATEGORIES)
        if openai_json_data and summary:
            logger.info("Анализ диалога OpenAI завершен. Приступаем к экспорту.")
            # 6. Формируем полную сводку для Telegram с дополнительной информацией
            full_summary_telegram = (
                f"<b>👤 Менеджер:</b> {manager_name}\n"
                f"<b>📱 Телефон клиента:</b> {client_phone}\n"
                f"<b>🔗 Ссылка на заказ:</b> <a href='{order_link}'>Заказ</a>\n\n"
                f"{summary}"
            )

            # 7. Объединяем данные для Google Forms, используя точные entry-ID
            google_forms_data = {
                'entry.408402535': order_link,
                'entry.711063137': total_summ,
                'entry.90684815': customer_type,
                'entry.1744925750': manager_name,
                'entry.1791797075': dialog_text,
                'entry.1213746785': openai_json_data.get('установление_контакта', 0),
                'entry.812648406': openai_json_data.get('выявление_потребностей', 0),
                'entry.567411627': openai_json_data.get('квалификация', 0),
                'entry.154941084': openai_json_data.get('презентация', 0),
                'entry.45434250': openai_json_data.get('возражение', 0),
                'entry.830702183': openai_json_data.get('отработка_возражения', 0),
                'entry.2001468013': openai_json_data.get('проговорены_договоренности', 0),
                'entry.1565546251': openai_json_data.get('закрытие_на_оплату', 0)
            }
            logger.debug(f"Данные для Google Forms подготовлены.")

            # 8. Отправляем данные в Google Forms
            send_to_google_forms(google_forms_data)

            # 9. Отправляем полную сводку в Telegram
            send_to_telegram(full_summary_telegram)
        else:
            logger.error(f"OpenAI не смог проанализировать диалог {dialog_id}. Экспорт невозможен.")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в процессе анализа и экспорта: {e}", exc_info=True)

    # 10. Перемещаем файл после обработки, независимо от результата экспорта
    # Изменено: теперь передаем client_phone
    move_dialog_to_closed(dialog_id, client_phone)
    logger.info(f"=== Обработка диалога {dialog_id} завершена ===")


# --- Тестовый модуль ---

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Замените этот номер телефона на реальный, чтобы протестировать
    test_phone_number = "79777796726"
    test_dialog_id = 99999

    # --------------------------------------- #
    #     ТЕСТ: ПОЛУЧЕНИЕ ИНФОРМАЦИИ ИЗ API
    # --------------------------------------- #
    logger.info("Запуск тестового модуля для получения информации о заказе и менеджере...")
    order_info = get_latest_order_details_from_phone(test_phone_number)
    manager_details = None

    if order_info:
        logger.info(f"✅ Успешно получена информация по номеру {test_phone_number}.")
        if 'managerId' in order_info:
            manager_id = order_info['managerId']
            manager_details = get_manager_details_from_id(manager_id)
            if manager_details:
                logger.info(f"✅ Успешно получена информация о менеджере по ID {manager_id}.")
            else:
                logger.error(f"❌ Не удалось получить информацию о менеджере с ID {manager_id}.")
    else:
        logger.error(f"❌ Не удалось получить информацию о заказе для номера {test_phone_number}.")

    # --------------------------------------- #
    #     ТЕСТ: ОТПРАВКА ДАННЫХ В GOOGLE FORMS
    # --------------------------------------- #
    logger.info("\nЗапуск тестового модуля для проверки отправки данных в Google Forms...")

    # Формируем данные для отправки, используя переменные из API-ответов
    order_link = f"{config.RETAILCRM_BASE_URL}/orders/{order_info.get('slug', '')}/edit" if order_info else 'Неизвестно'
    total_summ = order_info.get('totalSumm', 'Неизвестно') if order_info else 'Неизвестно'
    customer_type = 'Юридическое лицо' if order_info and order_info.get('orderType') == 'b2b' else 'Физическое лицо'
    manager_name = 'Неизвестно'
    if manager_details:
        manager_name = f"{manager_details.get('firstName', '')} {manager_details.get('lastName', '')}".strip()

    test_google_forms_data = {
        'entry.408402535': order_link,
        'entry.711063137': total_summ,
        'entry.90684815': customer_type,
        'entry.1744925750': manager_name,
        'entry.1213746785': '1',  # Оставляем тестовые значения для остальных полей
        'entry.812648406': '1',
        'entry.567411627': '1',
        'entry.154941084': '1',
        'entry.45434250': '1',
        'entry.830702183': '1',
        'entry.2001468013': '1',
        'entry.1565546251': '1'
    }

    try:
        send_to_google_forms(test_google_forms_data)
        logger.info("✅ Тестовые данные успешно отправлены в Google Forms.")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке тестовых данных в Google Forms: {e}", exc_info=True)

    # --------------------------------------- #
    #       ТЕСТ: ОТПРАВКА В ОСНОВНОЙ ТОПИК
    # --------------------------------------- #
    logger.info("\nЗапуск тестового модуля для проверки отправки в основной топик...")

    test_summary = "Это тестовое резюме, имитирующее результат анализа."

    # Формируем полную сводку для тестовой отправки
    full_test_summary = (
        f"<b>👤 Менеджер:</b> {manager_name}\n"
        f"<b>📱 Телефон клиента:</b> {test_phone_number}\n"
        f"<b>🔗 Ссылка на заказ:</b> <a href='{order_link}'>Заказ</a>\n\n"
        f"{test_summary}"
    )

    try:
        send_to_telegram(full_test_summary)
        logger.info("✅ Тестовое резюме для основного топика успешно отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке тестового резюме в Telegram: {e}", exc_info=True)

    # --------------------------------------- #
    #       ТЕСТ: ОТПРАВКА В ТОПИК С ПРЕДУПРЕЖДЕНИЯМИ
    # --------------------------------------- #
    logger.info("\nЗапуск тестового модуля для проверки отправки уведомлений в Telegram...")
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    test_message_warnings = (
        f"🚨 **Тестовое уведомление**\n\n"
        f"Это сообщение отправлено из `data_exporter.py` для проверки настроек.\n"
        f"Если вы видите это, значит, `TELEGRAM_WARNINGS_TOPIC_ID` и "
        f"`TELEGRAM_CHAT_ID` настроены верно."
    )
    special_chars = r'_*[]()~`>#+-=|{}.!'
    for char in special_chars:
        test_message_warnings = test_message_warnings.replace(char, f'\\{char}')
    payload_warnings = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'message_thread_id': config.TELEGRAM_WARNINGS_TOPIC_ID,
        'text': test_message_warnings,
        'parse_mode': 'MarkdownV2'
    }

    try:
        response = requests.post(url, data=payload_warnings, timeout=10)
        response.raise_for_status()
        logger.info("✅ Тестовое уведомление для топика с предупреждениями успешно отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке тестового уведомления в Telegram: {e}", exc_info=True)