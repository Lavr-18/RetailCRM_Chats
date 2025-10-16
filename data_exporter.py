import logging
import os
import sys
import json
import requests
import datetime
import re
from datetime import datetime, timedelta  # <-- ИЗМЕНЕНИЕ: Добавлен импорт для работы с датами

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

    # Имя файла включает номер телефона
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
    Отправляет проанализированные данные в Google-таблицу через Google Forms
    (для диалогов, прошедших фильтрацию и анализ OpenAI).
    """
    logger.info("Начало отправки данных в Google Forms (Полный анализ).")
    logger.debug(f"Отправляемые данные: {data}")
    try:
        response = requests.post(config.GOOGLE_FORMS_URL, data=data, timeout=10)
        response.raise_for_status()
        logger.info("✅ Данные успешно отправлены в Google Forms (Полный анализ).")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке данных в Google Forms (Полный анализ): {e}", exc_info=True)


def send_to_google_forms_free(data: dict):
    """
    Отправляет базовые данные в Google-таблицу через Google Forms
    (для диалогов, не прошедших фильтрацию или не требующих анализа OpenAI).
    """
    logger.info("Начало отправки данных в Google Forms (Базовый экспорт).")
    logger.debug(f"Отправляемые данные: {data}")

    # URL для базового экспорта берется из config.GOOGLE_FORMS_URL_FREE
    try:
        response = requests.post(config.GOOGLE_FORMS_URL_FREE, data=data, timeout=10)
        response.raise_for_status()
        logger.info("✅ Данные успешно отправлены в Google Forms (Базовый экспорт).")
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при отправке данных в Google Forms (Базовый экспорт): {e}", exc_info=True)


def send_to_telegram(summary: str):
    """
    Отправляет краткое резюме диалога в Telegram-группу с поддержкой тем.
    Использует parse_mode='HTML'.
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
    Включает логику фильтрации по статусу, методу заказа и дате создания.
    """
    logger.info(f"=== Начало обработки закрытого диалога {dialog_id} ===")

    # 1. Загрузка текста диалога
    file_name = f'dialog_{dialog_id}_{client_phone}.txt'
    active_path = os.path.join('dialogs', 'active', file_name)
    closed_path = os.path.join('dialogs', 'closed', file_name)

    dialog_text = ""
    file_path = None
    if os.path.exists(active_path):
        file_path = active_path
    elif os.path.exists(closed_path):
        file_path = closed_path
    else:
        logger.warning(f"Файл диалога {file_name} не найден. Пропускаем обработку.")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            dialog_text = f.read()
        logger.info(f"Текст диалога успешно загружен.")
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}", exc_info=True)
        return

    # 2. Получаем детали последнего заказа и инициализируем переменные
    order_details = get_latest_order_details_from_phone(client_phone)
    order_link = 'Неизвестно'
    total_summ = 'Неизвестно'
    customer_type = 'Физическое лицо'  # Дефолтное значение
    manager_name = 'Неизвестно'
    should_analyze = False
    order_number = 'Неизвестно'

    if order_details:
        order_link = f"{config.RETAILCRM_BASE_URL}/orders/{order_details.get('slug', 'Неизвестно')}/edit"
        total_summ = order_details.get('totalSumm', 'Неизвестно')
        order_number = order_details.get('externalId', 'Неизвестно')

        # Определение типа клиента
        customer_type = 'Юридическое лицо' if order_details.get('orderType') == 'b2b' else 'Физическое лицо'

        manager_id = order_details.get('managerId')
        if manager_id:
            manager_details = get_manager_details_from_id(manager_id)
            if manager_details:
                first_name = manager_details.get('firstName', '')
                last_name = manager_details.get('lastName', '')
                manager_name = f"{first_name} {last_name}".strip()

        # --- НОВЫЙ КОД ДЛЯ ПРОВЕРКИ ДАТЫ: Заказ должен быть не старше 2 дней ---
        order_created_at_str = order_details.get('createdAt')
        is_recent_order = False

        if order_created_at_str:
            try:
                # Парсинг даты в формате "YYYY-MM-DD HH:MM:SS"
                order_time = datetime.strptime(order_created_at_str, '%Y-%m-%d %H:%M:%S')

                # Сравниваем с текущим временем минус 2 дня
                two_days_ago = datetime.now() - timedelta(days=2)

                if order_time >= two_days_ago:
                    is_recent_order = True
                else:
                    logger.info(
                        f"Заказ {order_number} создан ({order_time}) более 2-х дней назад. Фильтрация по дате НЕ пройдена.")
            except ValueError as e:
                logger.error(
                    f"❌ Ошибка парсинга даты заказа {order_number} с форматом '{order_created_at_str}': {e}. Пропускаем анализ по дате.",
                    exc_info=True)
            except Exception as e:
                logger.error(f"❌ Непредвиденная ошибка при проверке даты заказа {order_number}: {e}", exc_info=True)
        else:
            logger.warning(f"В заказе {order_number} отсутствует поле 'createdAt'. Считаем НЕАКТУАЛЬНЫМ для анализа.")

        # 3. Проверка условий для полного анализа (Tier 1)
        order_status = order_details.get('status')
        order_method = order_details.get('orderMethod')

        is_valid_status = order_status in config.RETAILCRM_VALID_STATUSES
        is_valid_method = order_method != config.INVALID_ORDER_METHOD

        # ОБЪЕДИНЕНИЕ ВСЕХ УСЛОВИЙ
        if is_valid_status and is_valid_method and is_recent_order:
            should_analyze = True
            logger.info(
                f"Условия фильтрации выполнены (Status: {order_status}, Method: {order_method}, Recent: True). Будет произведен полный анализ OpenAI.")
        else:
            # Обновленное логирование, показывающее причину.
            reasons = []
            if not is_valid_status: reasons.append(f"Status: {order_status}")
            if not is_valid_method: reasons.append(f"Method: {order_method}")
            # Добавляем причину, только если недавний заказ не прошел
            if order_created_at_str and not is_recent_order: reasons.append("Order is OLDER than 2 days")

            # Логируем причины, если они есть
            if reasons:
                logger.info(
                    f"Условия фильтрации НЕ выполнены ({', '.join(reasons)}). Производится базовый экспорт.")
            elif order_details and not order_created_at_str:
                logger.info(
                    "Условия фильтрации НЕ выполнены (Проблема с датой или другое). Производится базовый экспорт.")
            else:
                # По идее, этот else не должен срабатывать, если есть order_details
                logger.info("Условия фильтрации НЕ выполнены. Производится базовый экспорт.")

    else:
        logger.info("Заказ для клиента не найден. Производится базовый экспорт.")

    # --- Ветвление логики экспорта ---

    if should_analyze:
        # 4. Анализируем диалог с помощью OpenAI (Полный анализ - Tier 1)
        try:
            openai_json_data, summary = analyze_dialog(dialog_text, config.CATEGORIES)

            if openai_json_data and summary:
                logger.info("Анализ диалога OpenAI завершен. Приступаем к полному экспорту.")

                # Формируем полную сводку для Telegram
                full_summary_telegram = (
                    f"<b>👤 Менеджер:</b> {manager_name}\n"
                    f"<b>📱 Телефон клиента:</b> {client_phone}\n"
                    f"<b>🔗 Ссылка на заказ:</b> <a href='{order_link}'>Заказ</a>\n\n"
                    f"{summary}"
                )

                # Объединяем данные для Google Forms (с критериями)
                # Теперь мы уверены, что OpenAI использует короткие, фиксированные ключи.
                google_forms_data = {
                    'entry.408402535': order_link,  # Ссылка на заказ
                    'entry.711063137': total_summ,
                    'entry.90684815': customer_type,
                    'entry.1744925750': manager_name,
                    'entry.1791797075': dialog_text,
                    'entry.1213746785': openai_json_data.get('установление_контакта', 0),
                    'entry.812648406': openai_json_data.get('выявление_потребностей', 0),
                    'entry.567411627': openai_json_data.get('квалификация', 0),
                    'entry.154941084': openai_json_data.get('презентация', 0),  # Короткий ключ
                    'entry.45434250': openai_json_data.get('возражение', 0),  # Короткий ключ
                    'entry.830702183': openai_json_data.get('отработка_возражения', 0),
                    'entry.2001468013': openai_json_data.get('проговорить_договоренности', 0),
                    'entry.1565546251': openai_json_data.get('закрытие_на_оплату', 0),
                    'entry.982776944': openai_json_data.get('уточнил_цель_покупки', 0)  # Короткий ключ
                }

                send_to_google_forms(google_forms_data)
                send_to_telegram(full_summary_telegram)
            else:
                logger.error(f"OpenAI не смог проанализировать диалог {dialog_id}. Переход к базовому экспорту.")
                # Если анализ провалился, переходим к базовому экспорту
                should_analyze = False  # Принудительно переключаем на базовый экспорт

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в процессе анализа OpenAI: {e}", exc_info=True)
            should_analyze = False

    if not should_analyze:
        # 5. Экспорт базовых данных (Tier 2) - Таблица Хранение чатов
        # Собираем данные для Google Forms без критериев
        google_forms_data_free = {
            'entry.1563894862': order_link,  # ИЗМЕНЕНИЕ: Отправляем order_link вместо order_number
            'entry.844658380': total_summ,  # Сумма заказа
            'entry.1126205710': customer_type,  # Физ/Юр
            'entry.3334402': dialog_text  # Диалог
        }

        send_to_google_forms_free(google_forms_data_free)
        logger.info("Базовый экспорт данных завершен.")

    # 6. Перемещаем файл после обработки
    move_dialog_to_closed(dialog_id, client_phone)
    logger.info(f"=== Обработка диалога {dialog_id} завершена ===")


# --- Тестовый модуль ---

if __name__ == "__main__":
    # Упрощенный тестовый блок
    logging.basicConfig(level=logging.INFO)
    logger.info("Модуль data_exporter.py запущен. Для проверки логики фильтрации "
                "необходимо запустить систему в рабочем режиме и проверить логи.")
