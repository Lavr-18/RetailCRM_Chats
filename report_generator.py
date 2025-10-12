import logging
import os
import sys
import re
import requests
from datetime import datetime, date, timedelta, time as dt_time
from glob import glob
from typing import List, Dict, Any

# Добавляем корневую директорию проекта в sys.path
# Это необходимо, чтобы импортировать config и data_exporter
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Импортируем модули
import config
# Импортируем только необходимые функции из data_exporter
from data_exporter import normalize_phone, process_and_export_data

# Настройка логирования
logger = logging.getLogger(__name__)

# --- Константы и настройки времени ---
DIALOG_DIR_ACTIVE = 'dialogs/active'
DIALOG_DIR_CLOSED = 'dialogs/closed'
DIALOG_FILE_REGEX = r'dialog_(\d+)_(\d+)\.txt'
MAX_DIALOG_AGE_DAYS = 3  # Максимальный возраст диалога для удаления (3 дня)

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
WORK_START_TIME = dt_time(9, 0)
WORK_END_TIME = dt_time(20, 0)
# Время составления отчета (используется для метрик, не для планирования)
REPORT_END_TIME = dt_time(23, 30)


# --- НОВАЯ ФУНКЦИЯ: Отправка отчета в Telegram (вместо использования data_exporter.send_to_telegram) ---
def send_report_to_telegram(text: str, topic_id: str):
    """
    Отправляет уведомление в Telegram-группу с поддержкой тем.
    """
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'message_thread_id': topic_id,
        'text': text,
        'parse_mode': 'HTML'  # Используем HTML, так как в отчете используются теги <b> и <a>
    }

    # Экранирование специальных символов для HTML не требуется в таком объеме, как для MarkdownV2,
    # но нужно убедиться, что текст не содержит неэкранированных HTML-сущностей,
    # которые могут нарушить формат.

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        logger.info("Отчет успешно отправлен в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при отправке отчета в Telegram: {e}")


# --- Вспомогательные функции ---

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
    """Парсит одну строку диалога."""
    match = re.match(r'^\[(.*?)\] (КЛИЕНТ|МЕНЕДЖЕР): (.*)$', line)
    if not match: return None
    timestamp_str, sender, content = match.groups()
    try:
        # Учитываем, что метка времени может содержать микросекунды
        dt = datetime.fromisoformat(timestamp_str)
    except ValueError:
        return None
    return {'time': dt, 'sender': sender, 'content': content.strip()}


def get_dialog_file_details(file_path: str) -> dict | None:
    """
    Извлекает ID, телефон, первое и последнее сообщение из файла диалога.
    Возвращает dict: {'dialog_id', 'client_phone', 'messages', 'file_path',
                     'first_message_time', 'last_message_time'}
    """
    file_name = os.path.basename(file_path)
    match = re.match(DIALOG_FILE_REGEX, file_name)
    if not match: return None

    dialog_id = int(match.group(1))
    client_phone = match.group(2)
    messages = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                parsed_msg = parse_dialog_line(line.strip())
                if parsed_msg: messages.append(parsed_msg)

        if not messages:
            return None

        return {
            'dialog_id': dialog_id,
            'client_phone': client_phone,
            'messages': messages,
            'file_path': file_path,
            'first_message_time': messages[0]['time'],
            'last_message_time': messages[-1]['time']  # <--- ДОБАВЛЕНО: Время последнего сообщения
        }
    except Exception as e:
        logger.error(f"Ошибка при чтении или парсинге файла {file_path}: {e}", exc_info=True)
        return None


def manage_and_get_dialogs(report_date: date) -> List[Dict[str, Any]]:
    """
    Реализует новую логику управления файлами:
    1. Удаляет диалоги, старше 3 дней (по дате последнего сообщения).
    2. Перемещает диалоги, закрытые сегодня, из 'active' в 'closed' и добавляет их в отчет.
    3. Собирает диалоги из 'closed', закрытые сегодня, для отчета.
    """
    # 3 дня назад (для удаления старых файлов)
    deletion_limit_dt = datetime.now() - timedelta(days=MAX_DIALOG_AGE_DAYS)

    all_files_for_check = glob(os.path.join(DIALOG_DIR_ACTIVE, 'dialog_*.txt')) + \
                          glob(os.path.join(DIALOG_DIR_CLOSED, 'dialog_*.txt'))

    today_dialogs_for_report = []

    for file_path in all_files_for_check:
        file_name = os.path.basename(file_path)
        details = get_dialog_file_details(file_path)

        if not details:
            logger.warning(f"Не удалось получить детали для файла: {file_name}. Пропуск.")
            continue

        last_msg_time = details['last_message_time']

        # 1. Проверка на удаление (старше 3 дней)
        if last_msg_time < deletion_limit_dt:
            try:
                os.remove(file_path)
                logger.info(f"🗑️ Удален старый диалог (дата последнего сообщения {last_msg_time.date()}): {file_name}")
            except Exception as e:
                logger.error(f"❌ Ошибка при удалении файла {file_name}: {e}")
            continue

        # 2. Проверка на включение в отчет (последнее сообщение сегодня)
        if last_msg_time.date() == report_date:

            # Если диалог в active, его нужно закрыть (проанализировать и переместить)
            if file_path.startswith(DIALOG_DIR_ACTIVE):
                dialog_id = details['dialog_id']
                client_phone = details['client_phone']

                logger.info(
                    f"Принудительное закрытие (анализ + перемещение) активного диалога {dialog_id} для отчета...")

                # process_and_export_data выполнит анализ и перемещение в 'closed'
                # ВАЖНО: После этого вызова файл может быть уже в 'closed', но объект 'details'
                # остается валидным для включения в отчет.
                try:
                    # Принудительно вызываем анализ и перемещение (как при событии dialog_closed)
                    process_and_export_data(dialog_id, client_phone)

                    # Добавляем детали (сообщения) в список для отчета
                    today_dialogs_for_report.append(details)

                except Exception as e:
                    logger.error(f"❌ Ошибка при принудительном закрытии диалога {dialog_id} для отчета: {e}")

            # Если диалог уже в closed И был закрыт сегодня (по дате последнего сообщения), включаем его в отчет
            elif file_path.startswith(DIALOG_DIR_CLOSED):
                today_dialogs_for_report.append(details)

    logger.info(f"Найдено {len(today_dialogs_for_report)} диалогов с последним сообщением за {report_date}.")
    return today_dialogs_for_report


# --- Остальные функции (без изменений) ---

# ... [Остальные функции: check_order_modification_today, get_relevant_orders_for_client,
# get_day_in_day_paid_orders, analyze_dialog_speed_and_status, process_new_dialogs остаются без изменений] ...


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
    # NOTE: 'today' здесь используется для проверки, был ли последний заказ изменен СЕГОДНЯ,
    # и не зависит от report_date. Это корректно для Метрики 2.
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
    # ИСПРАВЛЕНИЕ #2: Расчет даты
    # Отчет запускается в 23:00 и должен быть за текущий день.
    report_date = datetime.now().date()

    logger.info(f"=== Начало генерации ежедневного отчета за {report_date} ===")

    # НОВЫЙ ШАГ: Управление файлами, удаление старых, закрытие активных и сбор диалогов для отчета
    dialogs_for_today = manage_and_get_dialogs(report_date)

    if not dialogs_for_today:
        logger.info("Нет новых диалогов для анализа за выбранный день. Отправка отчета пропущена.")
        return

    # --- Агрегация результатов (Метрики 3, 4, 5) ---
    all_response_times_td = []
    slow_first_response_count = 0
    unanswered_working_count = 0
    unanswered_non_working_count = 0

    for dialog in dialogs_for_today:
        # Диалоги, которые были в 'active' и принудительно 'закрыты' выше, уже содержат
        # результат анализа (который происходит внутри process_and_export_data).
        # Однако, для расчета метрик 3, 4, 5 нам нужны сообщения.
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
    # Используем len() == 0, чтобы избежать ошибки деления на ноль, если all_response_times_td пуст
    if all_response_times_td:
        total_avg_response_time = sum(all_response_times_td, timedelta()) / len(all_response_times_td)
    else:
        total_avg_response_time = None

    # Шаг 4: Расчет метрики 6: Закрытие день в день

    # 4.1. Получаем уникальные телефоны клиентов, которые обратились сегодня (по последнему сообщению)
    today_appeal_phones = {normalize_phone(d['client_phone']) for d in dialogs_for_today}

    # 4.2. Получаем заказы, созданные И оплаченные сегодня
    day_in_day_orders = get_day_in_day_paid_orders(report_date)

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
        f"<b>📊 Ежедневный Отчет по чатам за {report_date.strftime('%d.%m.%Y')}</b>\n\n"
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

    # ИСПРАВЛЕНИЕ #3: Используем новую функцию, которая корректно обрабатывает токен и тему
    send_report_to_telegram(report_summary, config.TELEGRAM_TOPIC_ID)
    print("\n--- Сгенерированный Отчет ---\n" + report_summary)

    logger.info("=== Генерация отчета завершена. ===")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    generate_daily_report()
    pass
