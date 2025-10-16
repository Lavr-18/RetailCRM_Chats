import json
import logging
import os
import time
import requests
import websocket
import pytz
from threading import Thread
from datetime import datetime, time as dt_time
import re
import urllib.parse

# Импортируем новые модули
from dialog_analyser import analyze_dialog
from data_exporter import move_dialog_to_closed, process_and_export_data
from report_generator import generate_daily_report
# ИМПОРТ НОВОЙ ЛОГИКИ:
from retailcrm_api import create_ad_hoc_avito_task

# Настройка логирования для этого модуля (можно использовать тот же, что и в main.py)
logger = logging.getLogger(__name__)

# --------------------------------------- #
#        Параметры и глобальные переменные
# --------------------------------------- #
try:
    import config

    API_URL = config.RETAILCRM_API_URL
    TOKEN = config.RETAIL_CRM_BOT_TOKEN
    TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID
    TELEGRAM_TOPIC_ID = config.TELEGRAM_TOPIC_ID
    TELEGRAM_WARNINGS_TOPIC_ID = config.TELEGRAM_WARNINGS_TOPIC_ID
except ImportError:
    logger.error("Не удалось импортировать файл config.py. Убедитесь, что он существует и настроен.")
    exit(1)
except AttributeError as e:
    logger.error(f"В файле config.py отсутствуют необходимые переменные: {e}")
    exit(1)

HEADERS = {"X-Bot-Token": TOKEN, "Content-Type": "application/json"}

MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60

# Настройки для планировщика отчетов
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
LAST_REPORT_DATE = None  # Хранит дату последнего запуска отчета (чтобы не запускать его чаще раза в день)

# ГЛОБАЛЬНАЯ ПЕРЕМЕННАЯ ДЛЯ ПРОВЕРКИ ПЕРВОГО СООБЩЕНИЯ С AVITO
# Хранит ID диалогов, для которых уже была поставлена задача
AVITO_TASK_COMPLETED_DIALOGS = set()


# --------------------------------------- #
#           Вспомогательные функции
# --------------------------------------- #

def send_telegram_notification(text: str, topic_id: str):
    """
    Отправляет уведомление в Telegram-группу с поддержкой тем.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'message_thread_id': topic_id,
        'text': text,
        'parse_mode': 'MarkdownV2'
    }

    # Экранируем специальные символы для MarkdownV2
    special_chars = r'_*[]()~`>#+-=|{}.!'
    clean_text = text
    for char in special_chars:
        clean_text = clean_text.replace(char, f'\\{char}')
    payload['text'] = clean_text

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        logger.info("Уведомление успешно отправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при отправке уведомления в Telegram: {e}")


def check_for_unauthorized_links(message_data: dict):
    """
    Проверяет сообщение менеджера на наличие неавторизованных ссылок на оплату.
    """
    try:
        if message_data.get('from', {}).get('type') == 'user':
            message_content = message_data.get('content', '')

            # Регулярное выражение для поиска всех ссылок
            urls = re.findall(r'(https?://[^\s]+)', message_content)

            if not urls:
                return

            manager_name = message_data.get('from', {}).get('name', 'Неизвестный менеджер')
            dialog_id = message_data.get('dialog', {}).get('id')

            for url in urls:
                parsed_url = urllib.parse.urlparse(url)
                domain = parsed_url.netloc.replace('www.', '')

                if any(domain == d for d in config.ALLOWED_PAYMENT_DOMAINS):
                    logger.info(f"Обнаружена разрешенная ссылка от {manager_name} в диалоге {dialog_id}.")
                else:
                    logger.warning(f"Обнаружена подозрительная ссылка от {manager_name} в диалоге {dialog_id}: {url}")
                    notification_text = (
                        f"🚨 Подозрительная активность\n\n"
                        f"Менеджер: {manager_name}\n"
                        f"Диалог ID: {dialog_id}\n"
                        f"Обнаруженная ссылка: {url}\n\n"
                        f"Сообщение: {message_content}"
                    )
                    send_telegram_notification(notification_text, config.TELEGRAM_WARNINGS_TOPIC_ID)

    except Exception as e:
        logger.error(f"Ошибка при проверке ссылок: {e}")


# --------------------------------------- #
#      Функции для работы с диалогами
# --------------------------------------- #
def save_message_to_file(dialog_id: int, client_phone: str, sender_type: str, message_text: str, timestamp: str):
    """
    Сохраняет сообщение в текстовый файл диалога.
    Использует dialog_id и client_phone для уникального имени файла.
    """
    active_dir = 'dialogs/active'
    if not os.path.exists(active_dir):
        os.makedirs(active_dir)

    file_name = f'dialog_{dialog_id}_{client_phone}.txt'
    file_path = os.path.join(active_dir, file_name)
    formatted_message = f"[{timestamp}] {sender_type.upper()}: {message_text}\n"

    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(formatted_message)
        logger.info(f"Сообщение для диалога {dialog_id} сохранено в файл: {file_path}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении сообщения в файл {file_path}: {e}")


# --------------------------------------- #
#   Callbacks WebSocket для RetailCRM
# --------------------------------------- #

def on_message(ws, message):
    """
    Вызывается при входящем сообщении по WebSocket.
    Парсит JSON и сохраняет все сообщения.
    """
    global AVITO_TASK_COMPLETED_DIALOGS

    try:
        data = json.loads(message)
        # --- БЛОК ОТЛАДКИ: Вывод полной информации о сообщении ---
        logger.info(f"Полные данные сообщения: {data}")
        # --- КОНЕЦ БЛОКА ОТЛАДКИ ---

        event_type = data.get("type")

        if event_type == "message_new":
            message_data = data.get("data", {}).get("message", {})
            dialog_id = message_data.get("dialog", {}).get("id")
            sender_type = message_data.get("from", {}).get("type")
            incoming_type = message_data.get("type")
            client_phone = message_data.get("chat", {}).get("customer", {}).get("phone", "Неизвестно")
            timestamp = datetime.now().isoformat()

            # Извлекаем данные о канале и ответственном менеджере
            channel_name = message_data.get("chat", {}).get("channel", {}).get("name")
            responsible_manager_id = message_data.get("chat", {}).get("last_dialog", {}).get("responsible", {}).get(
                "external_id")

            if not dialog_id:
                logger.warning("Отсутствует dialog_id, пропускаем")
                return

            if sender_type in ["user", "customer"]:
                # Проверяем наличие подозрительных ссылок, если отправитель - менеджер (это нелогично, но оставим, если так было задумано)
                # Логичнее проверять, когда sender_type == 'user' (менеджер), но в RetailCRM Chat Messages
                # 'user' означает менеджера, а 'customer' - клиента. В вашем примере 'from.type' = 'user' или 'customer'.
                # В вашем логе с Avito, первое сообщение - 'customer', второе - 'user' (менеджер).
                check_for_unauthorized_links(message_data)

                # --- НОВАЯ ЛОГИКА: ПОСТАНОВКА ЗАДАЧИ ДЛЯ AVITO ---
                if (
                        dialog_id not in AVITO_TASK_COMPLETED_DIALOGS and
                        sender_type == 'customer' and  # Это должно быть первое сообщение от клиента
                        channel_name == 'Avito Авито' and
                        client_phone == 'Неизвестно' and  # Подтверждает, что у клиента нет номера телефона
                        responsible_manager_id  # Должен быть уже назначен ответственный
                ):
                    logger.info(
                        f"Обнаружено первое сообщение с Avito в диалоге {dialog_id} с manager_id {responsible_manager_id}. Ставим задачу.")

                    # Запускаем постановку задачи в отдельном потоке, чтобы не блокировать WebSocket
                    task_thread = Thread(
                        target=create_ad_hoc_avito_task,
                        args=(responsible_manager_id,)
                    )
                    task_thread.start()

                    # Добавляем ID в set, чтобы избежать дублирования задачи
                    AVITO_TASK_COMPLETED_DIALOGS.add(dialog_id)
                # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

                # Сохраняем сообщение в файл
                if incoming_type == "text":
                    content = message_data.get("content", {})
                    message_text = content.get("text") if isinstance(content, dict) else str(content)
                    role = "Клиент" if sender_type == 'customer' else "Менеджер"
                    save_message_to_file(dialog_id, client_phone, role, message_text, timestamp)
                elif incoming_type == "image":
                    # Также сохраняем информацию об изображении
                    message_text = "Изображение"
                    role = "Клиент" if sender_type == 'customer' else "Менеджер"
                    save_message_to_file(dialog_id, client_phone, role, message_text, timestamp)
                else:
                    logger.info(f"Игнорируем сообщение типа {incoming_type}.")
            else:
                logger.info(f"Игнорируем сообщение от '{sender_type}'")

        elif event_type == "dialog_closed":
            dialog_data = data.get('data', {}).get('dialog', {})
            dialog_id = dialog_data.get('id')

            # Извлекаем данные, которые необходимы для data_exporter.py
            client_phone = dialog_data.get('chat', {}).get('customer', {}).get('phone', 'Неизвестно')
            manager_name = dialog_data.get('last_dialog', {}).get('responsible', {}).get('name', 'Неизвестно')

            if dialog_id:
                logger.info(f"Получено событие закрытия для диалога {dialog_id}.")
                # Запускаем обработку в отдельном потоке
                thread = Thread(
                    target=process_and_export_data,
                    args=(dialog_id, client_phone)
                )
                thread.start()

                # После закрытия диалога удаляем его из set, чтобы в будущем,
                # если диалог будет открыт снова, можно было снова поставить задачу
                AVITO_TASK_COMPLETED_DIALOGS.discard(dialog_id)
            else:
                logger.warning("Получено событие dialog_closed, но отсутствует dialog_id.")

    except json.JSONDecodeError:
        logger.error(f"JSONDecodeError: {message}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


def on_error(ws, error):
    logger.error(f"WebSocket ошибка: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket закрыт: {close_status_code} - {close_msg}")


def on_open(ws):
    logger.info("WebSocket соединение установлено")
    ws.reconnect_attempts = 0
    ws.reconnect_delay = RECONNECT_DELAY


# --------------------------------------- #
#   Функции для запуска и переподключения
# --------------------------------------- #

def create_websocket():
    """
    Создаёт WebSocketApp для RetailCRM и возвращает его.
    """
    ws_url = f"{API_URL.replace('https://', 'wss://')}/ws?events=message_new,dialog_closed"
    ws = websocket.WebSocketApp(
        ws_url,
        header=["X-Bot-Token: " + TOKEN],
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    ws.reconnect_attempts = 0
    ws.reconnect_delay = RECONNECT_DELAY
    return ws


def run_with_reconnect(ws):
    """
    Запускает ws.run_forever в цикле, чтобы при обрыве связи
    повторно подключаться.
    """
    while True:
        if ws.reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Достигнуто макс. число попыток переподключения: {MAX_RECONNECT_ATTEMPTS}")
            break

        if ws.reconnect_attempts > 0:
            logger.info(f"Переподключение {ws.reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS}...")
        else:
            logger.info("Старт WebSocket...")

        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"Ошибка в ws.run_forever: {e}")

        current_attempts = getattr(ws, 'reconnect_attempts', 0) + 1
        current_delay = min(getattr(ws, 'reconnect_delay', 5) * 2, MAX_RECONNECT_DELAY)
        logger.info(f"Следующая попытка переподключения через {current_delay} с...")
        time.sleep(current_delay)

        ws_new = create_websocket()
        ws_new.reconnect_attempts = current_attempts
        ws_new.reconnect_delay = current_delay
        ws = ws_new


# --------------------------------------- #
#          Планировщик Отчетов
# --------------------------------------- #

def report_scheduler():
    """
    Неблокирующий планировщик, который запускает генератор отчета
    ежедневно в 23:00 по МСК в отдельном потоке.
    """
    global LAST_REPORT_DATE
    logger.info("Поток планировщика отчетов запущен.")

    while True:
        try:
            # Получаем текущее время в Московском часовом поясе
            now_msk = datetime.now(MOSCOW_TZ)
            current_date = now_msk.date()

            # Целевое время 23:00:00 (end of day)
            target_time = dt_time(23, 0, 0)

            # Сравниваем только время, чтобы определить, наступило ли 23:00
            is_it_time = now_msk.time() >= target_time

            # Проверяем условия:
            # 1. Время >= 23:00:00 МСК
            # 2. Отчет еще не был запущен СЕГОДНЯ (для текущей даты)
            if is_it_time and current_date != LAST_REPORT_DATE:
                logger.info(f"Наступило 23:00 MSK. Запуск генерации ежедневного отчета...")

                # Запуск генератора отчета в отдельном потоке, чтобы не блокировать основной цикл
                # (listener) и этот планировщик.
                report_thread = Thread(target=generate_daily_report, daemon=True)
                report_thread.start()

                # Обновляем дату последнего запуска
                LAST_REPORT_DATE = current_date

                # После запуска отчета переходим в "спящий режим" на 1 час (3600 секунд),
                # чтобы избежать многократной проверки и запуска в течение часа после 23:00.
                time.sleep(3600)

                # Основной цикл проверки: спим 1 минуту
            time.sleep(60)

        except Exception as e:
            logger.error(f"Критическая ошибка в планировщике отчетов: {e}", exc_info=True)
            # При ошибке ждем 5 минут перед следующей попыткой цикла
            time.sleep(300)


# --------------------------------------- #
#         Инициализация и запуск
# --------------------------------------- #

def start_listener():
    """
    Точка входа для запуска слушателя событий и планировщика отчетов.
    """
    try:
        test_request = requests.get(f"{API_URL}/bots", headers=HEADERS)
        if test_request.status_code == 403:
            logger.error("Ошибка авторизации: неверный токен бота.")
            return
    except requests.exceptions.RequestException as e:
        logger.error(f"Не удалось подключиться к API RetailCRM: {e}")
        return

    # 1. Запуск слушателя WebSocket
    ws = create_websocket()
    ws_thread = Thread(target=run_with_reconnect, args=(ws,), daemon=True)
    ws_thread.start()

    # 2. Запуск планировщика отчетов
    report_scheduler_thread = Thread(target=report_scheduler, daemon=True)
    report_scheduler_thread.start()

    try:
        # Основной поток просто ожидает
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем.")