# dialog_listener.py

import json
import logging
import os
import time
import requests
import websocket
from threading import Thread
from datetime import datetime
import re
import urllib.parse

# Импортируем новые модули
from dialog_analyser import analyze_dialog
from data_exporter import move_dialog_to_closed, process_and_export_data

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
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    payload['text'] = text

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

            if not dialog_id:
                logger.warning("Отсутствует dialog_id, пропускаем")
                return

            if sender_type in ["user", "customer"]:
                # Проверяем наличие подозрительных ссылок, если отправитель - менеджер
                check_for_unauthorized_links(message_data)

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
#         Инициализация и запуск
# --------------------------------------- #

def start_listener():
    """
    Точка входа для запуска слушателя событий.
    """
    try:
        test_request = requests.get(f"{API_URL}/bots", headers=HEADERS)
        if test_request.status_code == 403:
            logger.error("Ошибка авторизации: неверный токен бота.")
            return
    except requests.exceptions.RequestException as e:
        logger.error(f"Не удалось подключиться к API RetailCRM: {e}")
        return

    ws = create_websocket()
    ws_thread = Thread(target=run_with_reconnect, args=(ws,), daemon=True)
    ws_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Приложение остановлено пользователем.")