import logging
import os
import sys
import re
from datetime import datetime, date, time, timedelta
from glob import glob
import requests
from typing import List, Dict, Any

# Добавляем корневую директорию проекта в sys.path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Импортируем модули
import config
from data_exporter import send_to_telegram
from data_exporter import normalize_phone
from data_exporter import process_and_export_data  # <-- ДОБАВЛЕНО: для принудительного закрытия

# Настройка логирования
logger = logging.getLogger(__name__)

# --- Константы и настройки времени (без изменений) ---

# Группа "Новый" для Метрики 2
STATUS_GROUP_NEW = {"new", "gotovo-k-soglasovaniiu", "agree-absence"}
# Группа "Согласование" для Метрики 2
STATUS_GROUP_AGREEMENT = {"client-confirmed", "ne-dozvonilis", "perezvonit-pozdnee", "klient-zhdet-foto-s-zakupki",
                          "vizit-v-shourum",
                          "ozhidaet-oplaty", "gotovim-kp", "soglasovanie-kp", "kp-gotovo-k-zashchite", "proekt-visiak",
                          "soglasovano",
                          "oplacheno", "proverka-nalichiia", "oplata-ne-proshla"}
# Все целевые статусы для актуальных заказов (Метрика 2)
TARGET_STATUSES = STATUS_GROUP_NEW.union(STATUS_GROUP_AGREEMENT)
# Статусы для успешного закрытия на оплату/предоплату (Метрика 6)
PAYMENT_STATUSES = {
    "oplacheno", "novyi-oplachen",  # Полная оплата
    "predoplata-poluchena", "novyi-predoplachen", "prepayed",  # Предоплата
    "servisnoe-obsluzhivanie-oplacheno", "vyezd-biologa-oplachen"  # Добавлены статусы оплаты услуг
}

# Максимальный возраст заказа для метрики 2
MAX_ORDER_AGE_DAYS = 2

# Рабочее время для метрик 3, 4, 5: 9:00 до 20:00
WORK_START_TIME = time(9, 0)
WORK_END_TIME = time(20, 0)
# Время составления отчета
REPORT_END_TIME = time(23, 30)


# --- Вспомогательные функции (без изменений) ---

def format_timedelta(td: timedelta) -> str:
    """Форматирует timedelta в строку вида 'Чч мс'."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours} ч")
    if minutes > 0 or (hours == 0 and seconds > 0):
        parts.append(f"{minutes} м")

    if not parts and seconds > 0:
        return f"{seconds} с"

    return " ".join(parts) or "0 с"


def parse_dialog_line(line: str) -> dict | None:
    match = re.match(r'^\[(.*?)\] (КЛИЕНТ|МЕНЕДЖЕР): (.*)$', line)
    if not match: return None
    timestamp_str, sender, content = match.groups()
    try:
        dt = datetime.fromisoformat(timestamp_str)
    except ValueError:
        return None
    return {'time': dt, 'sender': sender, 'content': content.strip()}


def get_dialog_data(file_path: str) -> dict | None:
    file_name = os.path.basename(file_path)
    match = re.match(r'dialog_(\d+)_(\d+)\.txt', file_name)
    if not match: return None
    dialog_id = int(match.group(1));
    client_phone = match.group(2)
    messages = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parsed_msg = parse_dialog_line(line.strip())
                if parsed_msg: messages.append(parsed_msg)
        if not messages: return None
        return {'dialog_id': dialog_id, 'client_phone': client_phone, 'messages': messages, 'file_path': file_path,
                'first_message_time': messages[0]['time']}
    except Exception as e:
        logger.error(f"Ошибка при чтении или парсинге файла {file_path}: {e}", exc_info=True)
        return None


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ПРИНУДИТЕЛЬНОГО ЗАКРЫТИЯ ---

def close_active_dialogs():
    """
    Принудительно 'закрывает' (анализирует и перемещает) все диалоги,
    оставшиеся в dialogs/active, используя функцию из data_exporter.py.
    """
    active_dir = 'dialogs/active'

    if not os.path.exists(active_dir):
        logger.warning(f"Директория {active_dir} не найдена. Пропуск принудительного закрытия.")
        return

    # Находим все файлы в active/
    active_files = glob(os.path.join(active_dir, 'dialog_*.txt'))

    if not active_files:
        logger.info("В папке active/ нет файлов для принудительного закрытия.")
        return

    closed_count = 0

    for file_path in active_files:
        file_name = os.path.basename(file_path)

        # Извлекаем dialog_id и client_phone из имени файла: dialog_{dialog_id}_{client_phone}.txt
        match = re.match(r'dialog_(\d+)_(\d+)\.txt', file_name)

        if match:
            dialog_id = int(match.group(1))
            client_phone = match.group(2)

            try:
                logger.info(f"Принудительное закрытие диалога {dialog_id} по расписанию...")

                # Вызываем вашу существующую функцию, которая выполнит анализ и перемещение
                process_and_export_data(dialog_id, client_phone)
                closed_count += 1

            except Exception as e:
                logger.error(f"❌ Ошибка при принудительном закрытии диалога {dialog_id}: {e}")
        else:
            logger.warning(f"Не удалось распарсить ID и телефон из имени файла: {file_name}")

    logger.info(f"✅ Успешно принудительно 'закрыто' {closed_count} активных диалогов.")


# --- ОБНОВЛЕННАЯ ФУНКЦИЯ ПОЛУЧЕНИЯ ДИАЛОГОВ (теперь ищет только в closed) ---

def get_current_day_dialogs(target_date: date) -> List[Dict[str, Any]]:
    """
    Ищет диалоги, начатые в target_date, только в папке CLOSED.
    """
    # Ищем только в закрытых диалогах
    all_files = glob('dialogs/closed/dialog_*.txt')
    today_dialogs = []

    for file_path in all_files:
        dialog_data = get_dialog_data(file_path)

        # Фильтруем диалоги по дате их первого сообщения
        if dialog_data and dialog_data['first_message_time'].date() == target_date:
            today_dialogs.append(dialog_data)

    logger.info(f"Найдено {len(today_dialogs)} диалогов, начатых {target_date}, в папке CLOSED.")
    return today_dialogs


# --- Функции для работы с RetailCRM (без изменений) ---

def check_order_modification_today(order_id: int, target_date: date) -> bool:
    """
    Использует GET /api/v5/orders/history для проверки, было ли изменение
    по заказу order_id в течение target_date.
    """
    logger.info(f"Проверяем историю изменений для заказа {order_id} за {target_date}.")
    try:
        url = f"{config.RETAILCRM_BASE_URL}/api/v5/orders/history"
        headers = {'X-Api-Key': config.RETAILCRM_API_KEY}

        # Используем формат с пробелом (Y-m-d H:i:s), requests закодирует его правильно.
        start_dt_str = f"{target_date.strftime('%Y-%m-%d')} 00:00:00"
        end_dt_str = f"{target_date.strftime('%Y-%m-%d')} 23:59:59"

        params = {
            'filter[orderId]': order_id,
            'filter[startDate]': start_dt_str,
            'filter[endDate]': end_dt_str,
        }

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        history_found = len(data.get('history', [])) > 0

        if history_found:
            logger.info(f"Заказ {order_id} был изменен {target_date}.")
        else:
            logger.info(f"Заказ {order_id} не был изменен {target_date}.")

        return history_found

    except requests.exceptions.RequestException as e:
        response = getattr(e, 'response', None)
        if response is not None and response.status_code == 400:
            try:
                error_data = response.json()
                error_msg = error_data.get('errorMsg') or error_data.get('errors') or "Неизвестная ошибка 400"
                logger.error(
                    f"❌ Ошибка 400 при проверке истории заказа {order_id}: {error_msg}. URL-параметры: {params}")
            except requests.exceptions.JSONDecodeError:
                logger.error(
                    f"❌ Ошибка 400 при проверке истории заказа {order_id}: Не удалось декодировать ответ API в JSON. Статус: {e}")
            except Exception:
                logger.error(f"❌ Ошибка при проверке истории заказа {order_id}: {e}", exc_info=True)
        else:
            logger.error(f"❌ Ошибка при проверке истории заказа {order_id}: {e}", exc_info=True)

        return False


def get_relevant_orders_for_client(phone_number: str) -> Dict[str, Any]:
    """
    Ищет строго актуальный (НОВЫЙ) заказ для Метрики 2 И проверяет активность
    самого свежего заказа (для фильтрации ссылок).

    Возвращает: {
        'new_order': dict|None,
        'latest_order': dict|None,
        'is_client_active': bool
    }
    """
    today = datetime.now().date()
    logger.info(f"Начинаем поиск актуальных заказов в RetailCRM по телефону: {phone_number}")
    normalized_phone = normalize_phone(phone_number)

    # Инициализация результата
    result = {'new_order': None, 'latest_order': None, 'is_client_active': False}
    if not normalized_phone: return result

    # --- Шаг 1: Получаем все заказы клиента (до 50 шт) ---
    try:
        url = f"{config.RETAILCRM_BASE_URL}/api/v5/orders"
        headers = {'X-Api-Key': config.RETAILCRM_API_KEY}
        params = {'filter[customer]': normalized_phone, 'limit': 50}
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        all_client_orders = response.json().get('orders', [])

        if not all_client_orders:
            logger.info(f"Найдено 0 заказов для клиента {normalized_phone}.")
            return result

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при получении всех заказов клиента {normalized_phone}: {e}", exc_info=True)
        return result

    latest_order = all_client_orders[0]  # Самый свежий заказ
    result['latest_order'] = latest_order

    # --- Шаг 2: Строгая проверка (НОВЫЕ заказы в целевом статусе) ---
    date_limit = datetime.now() - timedelta(days=MAX_ORDER_AGE_DAYS)

    for order in all_client_orders:
        created_at_str = order.get('createdAt')
        if not created_at_str: continue

        try:
            created_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                created_dt = datetime.strptime(created_at_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue

        status = order.get('status')

        # Если заказ молодой и в целевом статусе - он считается "НОВЫМ"
        if created_dt >= date_limit and status in TARGET_STATUSES:
            result['new_order'] = order
            result['is_client_active'] = True  # Новый заказ всегда считается активным
            logger.info(f"Заказ {order.get('id')} признан строго актуальным (НОВЫЙ).")
            # Не прерываем цикл, чтобы убедиться, что latest_order - самый свежий
            break

            # --- Шаг 3: Проверка активности (старый заказ, измененный сегодня) ---
    # Этот шаг нужен ТОЛЬКО для фильтрации списка "Клиенты без актуального заказа".

    if not result['is_client_active'] and latest_order:
        latest_order_id = latest_order.get('id')
        if latest_order_id and check_order_modification_today(latest_order_id, today):
            result['is_client_active'] = True  # Активный, но не новый
            logger.info(
                f"Клиент {normalized_phone} признан активным, т.к. последний заказ {latest_order_id} был изменен сегодня.")
        else:
            logger.info(f"Клиент {normalized_phone} не имеет актуального (нового/активного) заказа.")

    if result['new_order'] is None:
        logger.info(f"Найдено 0 строго актуальных (новых) заказов для {normalized_phone}.")

    return result


def get_day_in_day_paid_orders(target_date: date) -> List[Dict[str, Any]]:
    # ... (Остается без изменений) ...
    logger.info(f"Начинаем поиск заказов, созданных и оплаченных за {target_date}")

    # Дата в формате Y-m-d
    target_date_str = target_date.strftime('%Y-%m-%d')

    try:
        url = f"{config.RETAILCRM_BASE_URL}/api/v5/orders"
        headers = {'X-Api-Key': config.RETAILCRM_API_KEY}

        # Параметры API для фильтрации день в день
        params = {
            'filter[createdAtFrom]': target_date_str,
            'filter[createdAtTo]': target_date_str,
            'filter[extendedStatus][]': list(PAYMENT_STATUSES),
            'filter[statusUpdatedAtFrom]': target_date_str,
            'filter[statusUpdatedAtTo]': target_date_str,
            'limit': 100
        }

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        day_in_day_orders = data.get('orders', [])

        logger.info(f"Найдено {len(day_in_day_orders)} заказов, созданных и оплаченных в этот день (через API).")
        return day_in_day_orders

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка при поиске оплаченных день в день заказов: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка при получении заказов: {e}", exc_info=True)
        return []


# --- Логика расчета Метрик 3, 4 и 5 (без изменений) ---

def analyze_dialog_speed_and_status(dialog: dict) -> dict:
    messages = dialog['messages']
    response_times = []
    first_client_msg = None
    first_manager_response_time = None
    first_response_too_slow = False

    for i in range(len(messages)):
        msg = messages[i]

        if i > 0 and msg['sender'] == 'МЕНЕДЖЕР' and messages[i - 1]['sender'] == 'КЛИЕНТ':
            response_time = msg['time'] - messages[i - 1]['time']
            response_times.append(response_time)

        if msg['sender'] == 'КЛИЕНТ' and first_client_msg is None:
            first_client_msg = msg

        if first_client_msg and msg['sender'] == 'МЕНЕДЖЕР' and first_manager_response_time is None and msg['time'] > \
                first_client_msg['time']:
            first_manager_response_time = msg['time'] - first_client_msg['time']
            if first_manager_response_time > timedelta(minutes=5):
                first_response_too_slow = True

    last_msg = messages[-1]
    is_unanswered_working = False
    is_unanswered_non_working = False

    if last_msg['sender'] == 'КЛИЕНТ':
        last_msg_time = last_msg['time']

        if WORK_START_TIME <= last_msg_time.time() <= WORK_END_TIME:
            is_unanswered_working = True

        elif WORK_END_TIME < last_msg_time.time() <= REPORT_END_TIME:
            is_unanswered_non_working = True

    return {
        'first_response_too_slow': first_response_too_slow,
        'is_unanswered_working': is_unanswered_working,
        'is_unanswered_non_working': is_unanswered_non_working,
        'response_times': response_times
    }


# --- Логика расчета Метрик 1 и 2 (Обновлена) ---

def process_new_dialogs(dialogs: list) -> dict:
    """
    Обновлено: считает только НОВЫЕ заказы (Метрика 2) и фильтрует
    клиентов без АКТИВНОГО (нового/измененного) заказа.
    """
    total_new_inquiries = len(dialogs)
    fiz_count = 0
    yur_count = 0
    orders_created_count = 0
    clients_without_order_data = []  # Хранит {'phone', 'latest_order_id'}

    for dialog in dialogs:
        phone = dialog['client_phone']
        order_info = get_relevant_orders_for_client(phone)

        new_order = order_info['new_order']  # Для Метрики 2
        is_client_active = order_info['is_client_active']  # Для фильтрации ссылок
        latest_order = order_info['latest_order']

        if new_order:
            # Метрика 2: Считаем только СТРОГО актуальные (НОВЫЕ) заказы
            orders_created_count += 1
            order_type = new_order.get('orderType')

            if order_type == 'b2b':
                yur_count += 1
            else:
                fiz_count += 1

        # Добавляем клиента в список "без актуального заказа", только если он НЕ активен
        if not is_client_active:
            latest_order_id = latest_order.get('id') if latest_order else None
            clients_without_order_data.append({'phone': phone, 'latest_order_id': latest_order_id})

    # Генерация ссылок (Метрика 2 - для не-активных клиентов)
    base_url_path = config.RETAILCRM_BASE_URL.replace('/api/v5', '')
    clients_without_order_links = []

    for client in clients_without_order_data:
        phone = client['phone']
        order_id = client['latest_order_id']

        if order_id:
            # Ссылка на последний заказ в формате /orders/{id}/edit
            link = f"{base_url_path}/orders/{order_id}/edit"
            clients_without_order_links.append(link)
        else:
            # Ссылка на поиск клиента, если заказов нет вообще (Fallback)
            link = f"{base_url_path}/customers?filter[text]={phone}"
            clients_without_order_links.append(link)

    return {
        'total_new_inquiries': total_new_inquiries,
        'fiz_count': fiz_count,
        'yur_count': yur_count,
        'orders_created_count': orders_created_count,
        'clients_without_order_links': clients_without_order_links
    }


# --- Основная логика генерации отчета ---

def generate_daily_report():
    today = datetime.now().date()
    logger.info(f"=== Начало генерации ежедневного отчета за {today} ===")

    # НОВЫЙ ШАГ: Сначала принудительно закрываем (анализируем и перемещаем) все оставшиеся активные чаты
    close_active_dialogs()

    # Шаг 1: Получаем все диалоги, начатые сегодня (теперь только из CLOSED)
    dialogs_for_today = get_current_day_dialogs(today)

    if not dialogs_for_today:
        logger.info("Нет новых диалогов для анализа за сегодняшний день. Отправка отчета пропущена.")
        return

    # --- Агрегация результатов (Метрики 3, 4, 5) ---
    all_response_times_td = []
    slow_first_response_count = 0
    unanswered_working_count = 0
    unanswered_non_working_count = 0

    for dialog in dialogs_for_today:
        speed_and_status = analyze_dialog_speed_and_status(dialog)

        all_response_times_td.extend(speed_and_status['response_times'])

        if speed_and_status['first_response_too_slow']:
            slow_first_response_count += 1

        if speed_and_status['is_unanswered_working']:
            unanswered_working_count += 1

        if speed_and_status['is_unanswered_non_working']:
            unanswered_non_working_count += 1

    # Шаг 2: Расчет метрик 1 и 2
    report_data_1_2 = process_new_dialogs(dialogs_for_today)

    # Шаг 3: Финальный расчет среднего времени (Метрика 3)
    total_avg_response_time = sum(all_response_times_td, timedelta()) / len(
        all_response_times_td) if all_response_times_td else None

    # Шаг 4: Расчет метрики 6: Закрытие день в день

    # 4.1. Получаем уникальные телефоны клиентов, которые обратились сегодня
    today_appeal_phones = {normalize_phone(d['client_phone']) for d in dialogs_for_today}

    # 4.2. Получаем заказы, созданные И оплаченные сегодня
    day_in_day_orders = get_day_in_day_paid_orders(today)

    day_in_day_count = 0
    day_in_day_sum = 0

    for order in day_in_day_orders:
        # Извлекаем телефон клиента из заказа
        customer_phone_number = order.get('customer', {}).get('phones', [{}])[0].get('number')

        if customer_phone_number and normalize_phone(customer_phone_number) in today_appeal_phones:
            # Все 3 условия совпали: день обращения = день создания = день оплаты
            day_in_day_count += 1
            day_in_day_sum += order.get('totalSumm', 0)

    # --- Шаг 5: Формирование отчета ---

    report_summary = (
        f"<b>📊 Ежедневный Отчет по чатам за {today.strftime('%d.%m.%Y')}</b>\n\n"
        # Метрика 1
        f"1. Поступило новых обращений: {report_data_1_2['total_new_inquiries']} "
        f"(Физ. {report_data_1_2['fiz_count']} / Юр. {report_data_1_2['yur_count']})\n\n"

        # Метрика 2
        f"2. Заказов заведено: <b>{report_data_1_2['orders_created_count']}</b>\n"
    )

    if report_data_1_2['clients_without_order_links']:
        links_str = "\n".join(
            [f"• <a href='{link}'>Клиент</a>" for link in report_data_1_2['clients_without_order_links']])
        report_summary += f"   ❌ Клиенты без актуального заказа ({len(report_data_1_2['clients_without_order_links'])}):\n{links_str}\n\n"
    else:
        report_summary += "   ✅ Все новые клиенты с актуальным заказом.\n\n"

    # Метрика 3
    if total_avg_response_time:
        avg_time_str = format_timedelta(total_avg_response_time)
        report_summary += f"3. Скорость ответа (ср. цикл): <b>{avg_time_str}</b>\n"
    else:
        report_summary += "3. Скорость ответа (ср. цикл): <b>Н/Д</b> (недостаточно данных)\n"

    if slow_first_response_count > 0:
        report_summary += f"   🚨 Медленный первый ответ (> 5 мин): <b>{slow_first_response_count} шт.</b>\n"
    else:
        report_summary += f"   ✅ Все первые ответы < 5 мин.\n"

    # Метрика 4 и 5
    report_summary += (
        f"4. Неотвеченных чатов (20:00-23:30): <b>{unanswered_non_working_count} шт.</b>\n"
        f"5. Неотвеченных чатов (раб. время 9:00-20:00): <b>{unanswered_working_count} шт.</b>\n\n"
    )

    # Метрика 6
    report_summary += (
        f"6. Закрытие день в день (шт/сумма): <b>{day_in_day_count} шт. / {day_in_day_sum:,.0f} руб.</b>"
    )

    # send_to_telegram(report_summary, config.TELEGRAM_TOPIC_ID) # Раскомментировать, когда будет настроен cron
    print("\n--- Сгенерированный Отчет ---\n" + report_summary)

    logger.info("=== Генерация отчета завершена. ===")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    generate_daily_report()
    pass