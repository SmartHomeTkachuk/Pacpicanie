#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Telegram-бот расписания Политеха.
Версия 5.19 – окончательная, исправлен запуск.
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import List, Dict, Optional, Tuple, Set, Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import aiofiles
import xml.etree.ElementTree as ET
from flask import Flask, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ============================================================================
#  КОНСТАНТЫ И НАСТРОЙКИ
# ============================================================================

WEEKS_IN_SEMESTER = 20
DAYS_IN_WEEK = 7
MAX_PARTS_PER_MESSAGE = 10
SEMESTER_TYPE = os.getenv("SEMESTER_TYPE", "").lower()

class Settings:
    def __init__(self):
        self.bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.base_url: str = os.getenv("BASE_URL", "https://polytech-shedule.ru/data/")
        self.db_name: str = os.getenv("DB_NAME", "schedule.db")
        self.port: int = int(os.getenv("PORT", "8080"))
        self.max_message_len: int = int(os.getenv("MAX_MESSAGE_LEN", "4000"))
        self.retry_count: int = int(os.getenv("RETRY_COUNT", "3"))
        self.retry_delay: int = int(os.getenv("RETRY_DELAY", "5"))
        self.groups_cache_file: str = os.getenv("GROUPS_CACHE_FILE", "groups_cache.json")
        self.groups_manual_file: str = os.getenv("GROUPS_MANUAL_FILE", "groups_manual.json")
        self.groups_per_page: int = int(os.getenv("GROUPS_PER_PAGE", "12"))
        self.ttl_groups_hours: int = int(os.getenv("TTL_GROUPS_HOURS", "24"))
        self.timezone: str = os.getenv("TZ", "Europe/Moscow")
        self.send_delay: float = 0.1
        self.message_interval: float = 0.05
        self.user_agent: str = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; TelegramBot/1.0)")
        self.max_concurrent_requests: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
        self.batch_size: int = 50
        self.morning_send_hour: int = int(os.getenv("MORNING_SEND_HOUR", "7"))
        self.morning_send_minute: int = int(os.getenv("MORNING_SEND_MINUTE", "0"))
        self.cache_refresh_hour: int = int(os.getenv("CACHE_REFRESH_HOUR", "3"))
        self.cache_refresh_minute: int = int(os.getenv("CACHE_REFRESH_MINUTE", "0"))
        self.cache_refresh_extra_hour: int = int(os.getenv("CACHE_REFRESH_EXTRA_HOUR", "6"))
        self.cache_refresh_extra_minute: int = int(os.getenv("CACHE_REFRESH_EXTRA_MINUTE", "45"))
        self.clean_cache_hour: int = int(os.getenv("CLEAN_CACHE_HOUR", "2"))
        self.clean_cache_minute: int = int(os.getenv("CLEAN_CACHE_MINUTE", "0"))
        self.cleanup_locks_hour: int = int(os.getenv("CLEANUP_LOCKS_HOUR", "1"))
        self.cleanup_locks_minute: int = int(os.getenv("CLEANUP_LOCKS_MINUTE", "0"))
        self.admin_ids: List[int] = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]

settings = Settings()
TZ = ZoneInfo(settings.timezone)
START_TIME = time.time()

# ============================================================================
#  ЛОГИРОВАНИЕ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============================================================================
#  БАЗА ДАННЫХ
# ============================================================================

async def init_db() -> None:
    async with aiosqlite.connect(settings.db_name) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                group_name TEXT NOT NULL,
                subscribed INTEGER DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS schedule_cache (
                group_name TEXT,
                week_number INTEGER,
                day_of_week INTEGER,
                lesson_time TEXT,
                subject TEXT,
                teacher TEXT,
                room TEXT,
                is_dummy INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_name, week_number, day_of_week, lesson_time)
            )
        ''')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_cache_group_week ON schedule_cache(group_name, week_number);')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_name);')
        await db.commit()
    logger.info("База данных инициализирована")

# ---- Функции БД ----

async def get_user_group(user_id: int) -> Optional[str]:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT group_name FROM users WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.error(f"Ошибка получения группы для {user_id}: {e}", exc_info=True)
        return None

async def set_user_group(user_id: int, group_name: str) -> None:
    if not validate_group_name(group_name):
        logger.warning(f"Невалидная группа {group_name} для {user_id}")
        return
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            await db.execute(
                "INSERT INTO users (user_id, group_name) VALUES (?, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET group_name = excluded.group_name",
                (user_id, group_name)
            )
            await db.commit()
            logger.info(f"Пользователь {user_id} выбрал группу {group_name}")
    except Exception as e:
        logger.error(f"Ошибка сохранения группы: {e}", exc_info=True)

async def update_subscription(user_id: int, subscribed: bool) -> None:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT group_name FROM users WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
            if row is None or not row[0] or not validate_group_name(row[0]):
                logger.warning(f"Нельзя изменить подписку для {user_id}: нет группы")
                return
            await db.execute(
                "UPDATE users SET subscribed = ? WHERE user_id = ?",
                (1 if subscribed else 0, user_id)
            )
            await db.commit()
            logger.info(f"Подписка {user_id} -> {subscribed}")
    except Exception as e:
        logger.error(f"Ошибка обновления подписки: {e}", exc_info=True)

async def get_subscribed_users() -> List[Tuple[int, str]]:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute(
                "SELECT user_id, group_name FROM users WHERE subscribed = 1 AND group_name != ''"
            ) as cur:
                rows = await cur.fetchall()
                return [(uid, g) for uid, g in rows if validate_group_name(g)]
    except Exception as e:
        logger.error(f"Ошибка получения подписанных: {e}", exc_info=True)
        return []

async def get_all_groups_from_db() -> List[str]:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT DISTINCT group_name FROM users WHERE group_name != ''") as cur:
                rows = await cur.fetchall()
                return [r[0] for r in rows if validate_group_name(r[0])]
    except Exception as e:
        logger.error(f"Ошибка получения групп из БД: {e}", exc_info=True)
        return []

async def get_user_count() -> int:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"Ошибка подсчёта: {e}", exc_info=True)
        return 0

async def get_subscribed_count() -> int:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT COUNT(*) FROM users WHERE subscribed = 1") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"Ошибка подсчёта: {e}", exc_info=True)
        return 0

async def get_cache_size() -> int:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute("SELECT COUNT(*) FROM schedule_cache") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"Ошибка подсчёта: {e}", exc_info=True)
        return 0

async def delete_user_data(user_id: int) -> None:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            await db.commit()
            logger.info(f"Пользователь {user_id} удалён")
    except Exception as e:
        logger.error(f"Ошибка удаления: {e}", exc_info=True)

# ---- Кеширование с блокировками ----

_cache_update_locks: Dict[str, asyncio.Lock] = {}
_cache_lock_timestamps: Dict[str, float] = {}
_locks_creation_lock = asyncio.Lock()

async def _cleanup_locks() -> None:
    now = time.monotonic()
    for key, ts in list(_cache_lock_timestamps.items()):
        if now - ts > 3600:
            lock = _cache_update_locks.get(key)
            if lock is None or not lock.locked():
                _cache_update_locks.pop(key, None)
                _cache_lock_timestamps.pop(key, None)

async def cleanup_cache_locks_job() -> None:
    await _cleanup_locks()
    logger.debug("Очистка блокировок кеша выполнена")

async def update_cache(group_name: str, week_number: int, schedule_data: Dict[int, List[Dict[str, str]]]) -> None:
    lock_key = f"{group_name}_{week_number}"
    async with _locks_creation_lock:
        if lock_key not in _cache_update_locks:
            _cache_update_locks[lock_key] = asyncio.Lock()
        _cache_lock_timestamps[lock_key] = time.monotonic()
    await _cleanup_locks()

    async with _cache_update_locks[lock_key]:
        async with aiosqlite.connect(settings.db_name) as db:
            try:
                await db.execute("BEGIN")
                await db.execute(
                    "DELETE FROM schedule_cache WHERE group_name = ? AND week_number = ?",
                    (group_name, week_number)
                )
                rows_to_insert = []
                for day in range(1, DAYS_IN_WEEK):
                    lessons = schedule_data.get(day, [])
                    if not lessons:
                        rows_to_insert.append((group_name, week_number, day, "00:00", "Нет пар", "", "", 1))
                    else:
                        for lesson in lessons:
                            rows_to_insert.append((
                                group_name, week_number, day,
                                lesson['time'], lesson['subject'], lesson['teacher'], lesson['room'], 0
                            ))
                await db.executemany('''
                    INSERT OR REPLACE INTO schedule_cache 
                    (group_name, week_number, day_of_week, lesson_time, subject, teacher, room, is_dummy, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', rows_to_insert)
                await db.commit()
                logger.info(f"Кеш обновлён для {group_name} неделя {week_number}")
            except Exception as e:
                try:
                    await db.rollback()
                except Exception as rb_err:
                    logger.error(f"Ошибка rollback: {rb_err}")
                logger.error(f"Ошибка обновления кеша: {e}", exc_info=True)
                raise

async def get_cached_schedule(group_name: str, week_number: int, day_of_week: int) -> List[Dict[str, str]]:
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute('''
                SELECT lesson_time, subject, teacher, room, is_dummy
                FROM schedule_cache 
                WHERE group_name = ? AND week_number = ? AND day_of_week = ?
                ORDER BY lesson_time
            ''', (group_name, week_number, day_of_week)) as cur:
                rows = await cur.fetchall()
                return [{"time": r[0], "subject": r[1], "teacher": r[2], "room": r[3], "is_dummy": r[4]} for r in rows]
    except Exception as e:
        logger.error(f"Ошибка получения кеша: {e}", exc_info=True)
        return []

async def get_cached_schedule_partial(group_name: str, week_number: int) -> Dict[int, List[Dict[str, str]]]:
    result = {}
    try:
        async with aiosqlite.connect(settings.db_name) as db:
            async with db.execute('''
                SELECT day_of_week, lesson_time, subject, teacher, room, is_dummy
                FROM schedule_cache 
                WHERE group_name = ? AND week_number = ?
                ORDER BY day_of_week, lesson_time
            ''', (group_name, week_number)) as cur:
                rows = await cur.fetchall()
                for row in rows:
                    day = row[0]
                    if day not in result:
                        result[day] = []
                    result[day].append({
                        "time": row[1], "subject": row[2], "teacher": row[3], "room": row[4], "is_dummy": row[5]
                    })
        return result
    except Exception as e:
        logger.error(f"Ошибка частичного кеша: {e}", exc_info=True)
        return {}

# ============================================================================
#  ПАРСИНГ РАСПИСАНИЯ
# ============================================================================

async def fetch_schedule(group_name: str, week_number: int, session: aiohttp.ClientSession) -> Optional[Dict[int, List[Dict[str, str]]]]:
    url = f"{settings.base_url}{week_number}.xml"
    headers = {'User-Agent': settings.user_agent}
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                logger.warning(f"HTTP {resp.status} для {url}")
                return None
            text = await resp.text()
            if text.startswith('\ufeff'):
                text = text[1:]
            root = ET.fromstring(text)
            schedule = {i: [] for i in range(1, DAYS_IN_WEEK)}
            found = False
            for elem in root.findall('My'):
                group_elem = elem.find('SPGRUP_NAIM')
                if group_elem is None or group_elem.text != group_name:
                    continue
                found = True
                den_elem = elem.find('DEN')
                time_elem = elem.find('TIME')
                if den_elem is None or time_elem is None:
                    continue
                try:
                    day = int(den_elem.text.strip())
                except (ValueError, AttributeError):
                    continue
                if day not in schedule:
                    logger.warning(f"Некорректный день {day} в XML для {group_name}, пропускаем")
                    continue
                predmet_elem = elem.find('PREDMET')
                famio_elem = elem.find('FAMIO')
                aud_elem = elem.find('AUD')
                lesson_time = time_elem.text.strip() if time_elem.text else "??:??"
                subject = predmet_elem.text.strip() if predmet_elem is not None and predmet_elem.text else "Нет данных"
                teacher = famio_elem.text.strip() if famio_elem is not None and famio_elem.text else "Не указан"
                room = aud_elem.text.strip() if aud_elem is not None and aud_elem.text else "Не указана"
                schedule[day].append({
                    "time": lesson_time,
                    "subject": subject,
                    "teacher": teacher,
                    "room": room
                })
            if not found:
                return None
            if not any(schedule.values()):
                logger.warning(f"Для группы {group_name} в XML не найдено валидных дней")
                return None
            for day in schedule:
                schedule[day].sort(key=lambda x: x["time"])
            return schedule
    except (aiohttp.ClientError, asyncio.TimeoutError, ET.ParseError) as e:
        logger.error(f"Ошибка загрузки/парсинга {url}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка {url}: {e}", exc_info=True)
        return None

async def fetch_schedule_with_retry(group_name: str, week_number: int, session: aiohttp.ClientSession, retries: int = None) -> Optional[Dict[int, List[Dict[str, str]]]]:
    if retries is None:
        retries = settings.retry_count
    for attempt in range(retries):
        result = await fetch_schedule(group_name, week_number, session)
        if result is not None:
            return result
        if attempt < retries - 1:
            await asyncio.sleep(settings.retry_delay)
    return None

# ============================================================================
#  УТИЛИТЫ
# ============================================================================

_logged_semester_warnings: Set[str] = set()

def detect_semester_type() -> str:
    month = datetime.now(TZ).month
    auto = "spring" if 2 <= month <= 8 else "autumn"
    if SEMESTER_TYPE in ("spring", "autumn"):
        if SEMESTER_TYPE != auto:
            warning_key = f"{SEMESTER_TYPE}_{auto}"
            if warning_key not in _logged_semester_warnings:
                logger.warning(f"Ручной выбор семестра '{SEMESTER_TYPE}' не соответствует текущему месяцу (авто: '{auto}')")
                _logged_semester_warnings.add(warning_key)
        return SEMESTER_TYPE
    return auto

def get_semester_start() -> datetime:
    semester = detect_semester_type()
    now = datetime.now(TZ)
    year = now.year
    if semester == "spring":
        start = datetime(year, 2, 1, tzinfo=TZ)
        if now.month >= 9:
            start = datetime(year + 1, 2, 1, tzinfo=TZ)
    else:
        if now.month < 9:
            year -= 1
        start = datetime(year, 9, 1, tzinfo=TZ)
    start += timedelta(days=(0 - start.weekday()) % 7)
    return start

def get_current_week() -> int:
    start = get_semester_start()
    diff = (datetime.now(TZ) - start).days
    if diff < 0:
        return 1
    week = diff // 7 + 1
    return max(1, min(week, WEEKS_IN_SEMESTER))

def get_day_of_week() -> int:
    return datetime.now(TZ).isoweekday()

def format_schedule(lessons: List[Dict[str, str]], day_name: str) -> str:
    real = [l for l in lessons if l.get('is_dummy', 0) == 0]
    if not real:
        return f"📭 {day_name}: Пар нет"
    text = f"📚 {day_name}:\n"
    for lesson in real:
        text += f"⏰ {lesson['time']} | {lesson['subject']}\n"
        text += f"   👨‍🏫 {lesson['teacher']} | 🏫 {lesson['room']}\n"
    return text

def split_long_text(text: str, max_len: int) -> List[str]:
    if len(text) <= max_len:
        return [text]
    parts = []
    lines = text.split('\n')
    current = ""
    for line in lines:
        if not line:
            if current:
                current += "\n"
            continue
        if len(line) > max_len:
            if current:
                parts.append(current)
                current = ""
            words = line.split()
            temp = ""
            for word in words:
                if len(temp) + len(word) + 1 > max_len:
                    if temp:
                        parts.append(temp)
                    if len(word) > max_len:
                        for i in range(0, len(word), max_len):
                            parts.append(word[i:i+max_len])
                        temp = ""
                    else:
                        temp = word
                else:
                    temp += (" " + word) if temp else word
            if temp:
                parts.append(temp)
        else:
            if len(current) + len(line) + 1 > max_len:
                parts.append(current)
                current = line
            else:
                current += ("\n" + line) if current else line
    if current:
        parts.append(current)
    return parts

def split_with_header(header: str, content_parts: List[str], max_len: int) -> List[str]:
    if len(header) + 2 > max_len:
        header = header[:max_len-3] + "..."
    if not content_parts:
        return [header]
    result = []
    for part in content_parts:
        if len(header) + len(part) + 2 > max_len:
            subparts = split_long_text(part, max_len - len(header) - 2)
            for sub in subparts:
                result.append(header + "\n" + sub)
        else:
            if result and len(result[-1]) + len(part) + 1 <= max_len:
                result[-1] += "\n" + part
            else:
                result.append(header + "\n" + part)
    return result

def validate_group_name(group_name: str) -> bool:
    return bool(re.fullmatch(r'\d{8}/\d{4}', group_name))

# ============================================================================
#  СПИСОК ГРУПП
# ============================================================================

_groups_cache_lock = asyncio.Lock()

async def get_group_list(session: aiohttp.ClientSession, force_refresh: bool = False) -> List[str]:
    if os.path.exists(settings.groups_manual_file):
        try:
            async with aiofiles.open(settings.groups_manual_file, 'r') as f:
                content = await f.read()
                manual = json.loads(content)
                if isinstance(manual, list) and manual:
                    valid_groups = [g for g in manual if validate_group_name(g)]
                    if valid_groups:
                        logger.info("Группы из ручного файла (отфильтрованы)")
                        return valid_groups
                    else:
                        logger.warning("Ручной файл групп не содержит валидных групп")
        except Exception as e:
            logger.warning(f"Ошибка ручного файла: {e}")

    async with _groups_cache_lock:
        cache_file = settings.groups_cache_file
        if not force_refresh and os.path.exists(cache_file):
            try:
                async with aiofiles.open(cache_file, 'r') as f:
                    data = json.loads(await f.read())
                    if isinstance(data, dict) and 'timestamp' in data and 'groups' in data:
                        if time.time() - data['timestamp'] < settings.ttl_groups_hours * 3600:
                            logger.debug("Группы из кеша")
                            return data['groups']
            except Exception as e:
                logger.warning(f"Ошибка кеша: {e}")

        try:
            headers = {'User-Agent': settings.user_agent}
            async with session.get("https://polytech-shedule.ru/", headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    groups = re.findall(r'\b\d{8}/\d{4}\b', html)
                    if groups:
                        groups = sorted(set(groups))
                        async with aiofiles.open(cache_file, 'w') as f:
                            await f.write(json.dumps({'timestamp': time.time(), 'groups': groups}))
                        logger.info(f"Загружено {len(groups)} групп с сайта")
                        return groups
        except Exception as e:
            logger.error(f"Ошибка загрузки групп: {e}", exc_info=True)

        if os.path.exists(cache_file):
            try:
                async with aiofiles.open(cache_file, 'r') as f:
                    data = json.loads(await f.read())
                    if 'groups' in data and data['groups']:
                        logger.warning("Используем устаревший кеш групп")
                        return data['groups']
            except:
                pass
        return []

# ============================================================================
#  ОБРАБОТЧИКИ КОМАНД
# ============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
    await update.message.reply_text(
        "👋 Привет! Я бот расписания Политеха.\nНажми кнопку, чтобы выбрать группу.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 Команды:\n"
        "/start – выбор группы\n"
        "/today – на сегодня\n"
        "/tomorrow – на завтра\n"
        "/week [N] – на неделю (можно указать номер)\n"
        "/setgroup – сменить группу\n"
        "/subscribe – включить утреннюю рассылку\n"
        "/unsubscribe – отключить рассылку\n"
        "/reset – очистить мои данные\n"
        "/stats – статистика (админы)\n"
        "/refresh_cache – принудительное обновление (админы)\n"
        "/help – справка"
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in settings.admin_ids:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    uc = await get_user_count()
    sc = await get_subscribed_count()
    cs = await get_cache_size()
    await update.message.reply_text(
        f"📊 Статистика:\nПользователей: {uc}\nПодписаны: {sc}\nКеш: {cs}\nВремя работы: {int(time.time() - START_TIME)}с"
    )

_background_tasks: Set[asyncio.Task] = set()

async def refresh_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in settings.admin_ids:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    await update.message.reply_text("🔄 Обновление кеша запущено в фоне.")
    task = asyncio.create_task(refresh_all_caches(context.application))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    logger.info("Ручное обновление кеша запущено")

async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    session = context.bot_data.get('session')
    groups = await get_group_list(session)
    if not groups:
        await query.edit_message_text("❌ Не удалось загрузить список групп. Попробуйте позже.")
        return

    if query.data.startswith('page_'):
        page = int(query.data.split('_')[1])
        context.user_data['group_page'] = page
    else:
        page = context.user_data.get('group_page', 0)

    total_pages = (len(groups) + settings.groups_per_page - 1) // settings.groups_per_page
    if page >= total_pages:
        page = max(0, total_pages - 1)
        context.user_data['group_page'] = page

    start_idx = page * settings.groups_per_page
    end_idx = min(start_idx + settings.groups_per_page, len(groups))
    page_groups = groups[start_idx:end_idx]

    keyboard = []
    row = []
    for g in page_groups:
        row.append(InlineKeyboardButton(g, callback_data=f"group_{g}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀ Назад", callback_data=f"page_{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Вперёд ▶", callback_data=f"page_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)

    await query.edit_message_text(
        f"📋 Выбери группу (страница {page+1}/{total_pages}):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    group_name = query.data.replace("group_", "")
    user_id = query.from_user.id

    if not validate_group_name(group_name):
        keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
        await query.edit_message_text(
            "❌ Некорректный формат группы. Выберите заново:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    session = context.bot_data.get('session')
    all_groups = await get_group_list(session)

    if not all_groups:
        logger.warning("Список групп пуст, пробуем принудительно обновить...")
        all_groups = await get_group_list(session, force_refresh=True)
        if not all_groups:
            keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
            await query.edit_message_text(
                "❌ Не удалось загрузить список групп. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

    if group_name not in all_groups:
        keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
        await query.edit_message_text(
            f"⚠️ Группа {group_name} не найдена. Выберите другую:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    current_group = await get_user_group(user_id)
    if current_group == group_name:
        await query.edit_message_text(f"✅ Группа {group_name} уже выбрана.")
        return

    week = get_current_week()
    schedule = await fetch_schedule_with_retry(group_name, week, session)
    if schedule is None:
        keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
        await query.edit_message_text(
            "❌ Не удалось загрузить расписание. Попробуйте другую группу или повторите позже:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    await set_user_group(user_id, group_name)
    await update_cache(group_name, week, schedule)
    await query.edit_message_text(f"✅ Группа {group_name} сохранена!\nИспользуй /help.")

async def get_day_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE, delta_days: int) -> None:
    user_id = update.effective_user.id
    group = await get_user_group(user_id)
    if not group:
        await update.message.reply_text("❗ Сначала выбери группу: /start")
        return

    week = get_current_week()
    day = get_day_of_week() + delta_days

    if day == 7:
        await update.message.reply_text("🎉 Воскресенье, пар нет!")
        return

    if day > DAYS_IN_WEEK:
        day = 1
        week += 1
        if week > WEEKS_IN_SEMESTER:
            await update.message.reply_text("📅 Семестр закончился, расписания на эту дату нет.")
            return

    lessons = await get_cached_schedule(group, week, day)
    if not lessons:
        session = context.bot_data.get('session')
        schedule = await fetch_schedule_with_retry(group, week, session)
        if schedule:
            await update_cache(group, week, schedule)
            lessons = schedule.get(day, [])
        else:
            await update.message.reply_text("❌ Ошибка загрузки расписания. Попробуйте позже.")
            return

    days = ["", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    day_name = days[day] if 1 <= day <= 7 else ""
    text = format_schedule(lessons, day_name)
    parts = split_long_text(text, settings.max_message_len)
    for idx, part in enumerate(parts):
        try:
            await update.message.reply_text(part)
            if idx < len(parts) - 1:
                await asyncio.sleep(settings.message_interval)
        except Exception as e:
            if "Forbidden" in str(e):
                logger.warning(f"Пользователь {user_id} заблокировал бота")
                break
            else:
                logger.error(f"Ошибка отправки {user_id}: {e}")

async def today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await get_day_schedule(update, context, 0)

async def tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await get_day_schedule(update, context, 1)

async def week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    group = await get_user_group(user_id)
    if not group:
        await update.message.reply_text("❗ Сначала выбери группу: /start")
        return

    args = context.args
    if args:
        try:
            week = int(args[0])
        except ValueError:
            week = get_current_week()
        else:
            if week < 1 or week > WEEKS_IN_SEMESTER:
                await update.message.reply_text(f"❌ Номер недели от 1 до {WEEKS_IN_SEMESTER}.")
                return
    else:
        week = get_current_week()

    session = context.bot_data.get('session')
    cached = await get_cached_schedule_partial(group, week)
    days_with_cache = set(cached.keys())
    if len(days_with_cache) < DAYS_IN_WEEK - 1:
        full = await fetch_schedule_with_retry(group, week, session)
        if full:
            await update_cache(group, week, full)
            cached = await get_cached_schedule_partial(group, week)
        else:
            await update.message.reply_text("❌ Не удалось загрузить полное расписание. Попробуйте позже.")
            return
    else:
        missing = [d for d in range(1, DAYS_IN_WEEK) if d not in days_with_cache]
        if missing:
            full = await fetch_schedule_with_retry(group, week, session)
            if full:
                await update_cache(group, week, full)
                cached = await get_cached_schedule_partial(group, week)
            else:
                await update.message.reply_text("❌ Не удалось загрузить недостающие дни расписания. Попробуйте позже.")
                return

    days = ["", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]
    day_parts = []
    for day in range(1, DAYS_IN_WEEK):
        lessons = cached.get(day, [])
        day_text = format_schedule(lessons, days[day])
        day_parts.extend(split_long_text(day_text, settings.max_message_len))

    header = f"📅 Расписание на неделю {week} (группа {group}):\n\n"
    final_parts = split_with_header(header, day_parts, settings.max_message_len)

    if not final_parts:
        await update.message.reply_text("📭 Расписание на эту неделю не найдено.")
        return

    warning_text = "⚠️ Показаны не все дни (слишком длинное расписание)."
    if len(final_parts) > MAX_PARTS_PER_MESSAGE:
        trimmed = final_parts[:MAX_PARTS_PER_MESSAGE - 1]
        if len(warning_text) > settings.max_message_len:
            warning_text = warning_text[:settings.max_message_len - 3] + "..."
        trimmed.append(warning_text)
        final_parts = trimmed

    for idx, part in enumerate(final_parts):
        try:
            await update.message.reply_text(part)
            if idx < len(final_parts) - 1:
                await asyncio.sleep(settings.message_interval)
        except Exception as e:
            if "Forbidden" in str(e):
                logger.warning(f"Пользователь {user_id} заблокировал бота")
                break
            else:
                logger.error(f"Ошибка отправки {user_id}: {e}")

async def setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = await get_user_group(user_id)
    msg = "Нажмите кнопку, чтобы выбрать группу."
    if current:
        msg = f"Текущая группа: {current}\n\n{msg}"
    keyboard = [[InlineKeyboardButton("📋 Выбрать группу", callback_data="choose_group")]]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))

async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await get_user_group(user_id):
        await update.message.reply_text("❗ Сначала выбери группу: /start")
        return
    await update_subscription(user_id, True)
    await update.message.reply_text("✅ Подписка включена!")

async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await get_user_group(user_id):
        await update.message.reply_text("❗ Сначала выбери группу: /start")
        return
    await update_subscription(user_id, False)
    await update.message.reply_text("❌ Подписка отключена.")

async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await delete_user_data(update.effective_user.id)
    await update.message.reply_text("🗑️ Данные удалены. Начните с /start.")

# ============================================================================
#  ФОНОВЫЕ ЗАДАЧИ
# ============================================================================

async def refresh_all_caches(app: Application) -> None:
    groups = await get_all_groups_from_db()
    if not groups:
        return
    current_week = get_current_week()
    weeks = {current_week}
    for delta in (-1, 1):
        w = current_week + delta
        if w < 1:
            w = WEEKS_IN_SEMESTER
        elif w > WEEKS_IN_SEMESTER:
            w = 1
        weeks.add(w)

    session = app.bot_data.get('session')
    if session is None:
        logger.error("Нет сессии aiohttp в refresh_all_caches")
        return

    semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def fetch_group(group: str, week: int):
        async with semaphore:
            try:
                sched = await fetch_schedule_with_retry(group, week, session)
                if sched:
                    await update_cache(group, week, sched)
                else:
                    logger.warning(f"Не удалось загрузить {group} неделя {week}")
            except Exception as e:
                logger.error(f"Ошибка {group} неделя {week}: {e}", exc_info=True)
            finally:
                queue.task_done()

    queue = asyncio.Queue()
    for group in groups:
        if validate_group_name(group):
            for week in weeks:
                await queue.put((group, week))

    async def worker():
        while not queue.empty():
            group, week = await queue.get()
            await fetch_group(group, week)

    workers = [asyncio.create_task(worker()) for _ in range(min(settings.max_concurrent_requests, queue.qsize()))]
    await asyncio.gather(*workers, return_exceptions=True)
    await queue.join()
    logger.info("Обновление кеша завершено")

async def send_morning_schedule(app: Application) -> None:
    users = await get_subscribed_users()
    if not users:
        return
    week = get_current_week()
    day = get_day_of_week()
    if day == 7:
        return
    days = ["", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"]

    session = app.bot_data.get('session')
    if session is None:
        logger.error("Нет сессии для рассылки")
        return

    groups_set = {g for _, g in users}
    semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def load_group(group: str):
        async with semaphore:
            try:
                cached = await get_cached_schedule(group, week, day)
                if not cached:
                    sched = await fetch_schedule_with_retry(group, week, session)
                    if sched:
                        await update_cache(group, week, sched)
                next_day = day + 1
                if next_day <= DAYS_IN_WEEK and day != 6:
                    next_week = week
                    cached_tomorrow = await get_cached_schedule(group, next_week, next_day)
                    if not cached_tomorrow:
                        sched2 = await fetch_schedule_with_retry(group, next_week, session)
                        if sched2:
                            await update_cache(group, next_week, sched2)
            except Exception as e:
                logger.error(f"Ошибка предзагрузки {group}: {e}", exc_info=True)

    await asyncio.gather(*(load_group(g) for g in groups_set), return_exceptions=True)

    async def send_with_delay(uid: int, text: str) -> bool:
        for attempt in range(settings.retry_count):
            try:
                await app.bot.send_message(uid, text)
                return True
            except Exception as e:
                if any(k in str(e) for k in ("Forbidden", "blocked", "not found", "deactivated")):
                    logger.warning(f"Чат {uid} недоступен, отписываем")
                    await update_subscription(uid, False)
                    return False
                if attempt < settings.retry_count - 1:
                    await asyncio.sleep(settings.retry_delay)
                else:
                    logger.error(f"Не удалось отправить {uid}: {e}")
                    return False
        return False

    for uid, group in users:
        lessons = await get_cached_schedule(group, week, day)
        if not lessons:
            logger.warning(f"Нет кеша для {group}, пропускаем {uid}")
            continue
        text = f"🌅 Доброе утро!\n{format_schedule(lessons, days[day])}"
        parts = split_long_text(text, settings.max_message_len)
        blocked = False
        for idx, part in enumerate(parts):
            if not blocked:
                blocked = not await send_with_delay(uid, part)
                if idx < len(parts) - 1 and not blocked:
                    await asyncio.sleep(settings.message_interval)
            else:
                break
        await asyncio.sleep(settings.send_delay)

async def clean_old_cache_job() -> None:
    current_week = get_current_week()
    allowed_weeks = {current_week + delta for delta in (-1, 0, 1)}
    allowed_weeks = {w if 1 <= w <= WEEKS_IN_SEMESTER else (WEEKS_IN_SEMESTER if w < 1 else 1) for w in allowed_weeks}

    db = None
    try:
        db_groups = await get_all_groups_from_db()
        db = await aiosqlite.connect(settings.db_name)
        await db.execute("BEGIN")
        if db_groups:
            placeholders = ','.join('?' * len(db_groups))
            await db.execute(
                f"DELETE FROM schedule_cache WHERE group_name NOT IN ({placeholders})",
                db_groups
            )
        else:
            await db.execute("DELETE FROM schedule_cache")
        await db.execute(
            f"DELETE FROM schedule_cache WHERE week_number NOT IN ({','.join('?' * len(allowed_weeks))})",
            tuple(allowed_weeks)
        )
        await db.commit()
        logger.info("Очистка кеша завершена")
    except Exception as e:
        if db is not None:
            try:
                await db.rollback()
            except Exception:
                pass
        logger.error(f"Ошибка очистки кеша: {e}", exc_info=True)
    finally:
        if db is not None:
            await db.close()

# ============================================================================
#  FLASK (МОНИТОРИНГ)
# ============================================================================

flask_app = Flask(__name__)
_global_loop = None

@flask_app.route('/')
def home():
    return "OK", 200

@flask_app.route('/ping')
def ping():
    return "OK", 200

@flask_app.route('/stats')
def stats():
    if _global_loop is None or _global_loop.is_closed():
        return jsonify({'error': 'loop not available'}), 500
    try:
        fut1 = asyncio.run_coroutine_threadsafe(get_user_count(), _global_loop)
        fut2 = asyncio.run_coroutine_threadsafe(get_subscribed_count(), _global_loop)
        fut3 = asyncio.run_coroutine_threadsafe(get_cache_size(), _global_loop)
        return jsonify({
            'users': fut1.result(timeout=5),
            'subscribed': fut2.result(timeout=5),
            'cache_size': fut3.result(timeout=5),
            'uptime': time.time() - START_TIME
        })
    except Exception as e:
        logger.error(f"/stats error: {e}")
        return jsonify({'error': str(e)}), 500

def run_flask():
    try:
        flask_app.run(host='0.0.0.0', port=settings.port, debug=False)
    except Exception as e:
        logger.error(f"Flask не запустился: {e}")

# ============================================================================
#  ЗАПУСК БОТА (исправленный)
# ============================================================================

scheduler = AsyncIOScheduler(timezone=settings.timezone)
_shutdown_in_progress = False

async def shutdown_app(app: Application) -> None:
    global _shutdown_in_progress
    if _shutdown_in_progress:
        return
    _shutdown_in_progress = True
    logger.info("Завершение работы...")
    scheduler.shutdown(wait=True)
    await app.shutdown()
    session = app.bot_data.get('session')
    if session is not None and not session.closed:
        await session.close()
    for task in list(_background_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _background_tasks.clear()
    logger.info("Бот остановлен")

def main():
    global _global_loop

    if not settings.bot_token:
        logger.error("TELEGRAM_BOT_TOKEN не задан!")
        sys.exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _global_loop = loop

    # Инициализация БД и создание сессии
    async def init_and_session():
        await init_db()
        timeout = aiohttp.ClientTimeout(total=30)
        session = aiohttp.ClientSession(timeout=timeout)
        return session

    session = loop.run_until_complete(init_and_session())

    app = Application.builder().token(settings.bot_token).build()
    app.bot_data['session'] = session

    # Регистрация обработчиков
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("tomorrow", tomorrow))
    app.add_handler(CommandHandler("week", week))
    app.add_handler(CommandHandler("setgroup", setgroup))
    app.add_handler(CommandHandler("subscribe", subscribe))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe))
    app.add_handler(CommandHandler("reset", reset))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("refresh_cache", refresh_cache_command))
    app.add_handler(CallbackQueryHandler(choose_group, pattern="^choose_group$|^page_\\d+$"))
    app.add_handler(CallbackQueryHandler(select_group, pattern="^group_"))

    # Планировщик
    async def start_scheduler():
        scheduler.add_job(send_morning_schedule, CronTrigger(hour=settings.morning_send_hour, minute=settings.morning_send_minute, timezone=TZ), args=[app])
        scheduler.add_job(refresh_all_caches, CronTrigger(hour=settings.cache_refresh_hour, minute=settings.cache_refresh_minute, timezone=TZ), args=[app])
        scheduler.add_job(refresh_all_caches, CronTrigger(hour=settings.cache_refresh_extra_hour, minute=settings.cache_refresh_extra_minute, timezone=TZ), args=[app])
        scheduler.add_job(clean_old_cache_job, CronTrigger(hour=settings.clean_cache_hour, minute=settings.clean_cache_minute, timezone=TZ))
        scheduler.add_job(cleanup_cache_locks_job, CronTrigger(hour=settings.cleanup_locks_hour, minute=settings.cleanup_locks_minute, timezone=TZ))
        scheduler.start()
        logger.info("Планировщик запущен")

    loop.run_until_complete(start_scheduler())

    # Flask
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"Flask на порту {settings.port}")

    # Обработка сигналов
    def signal_handler(sig, frame):
        logger.info("Получен сигнал")
        if _global_loop is not None and not _global_loop.is_closed():
            asyncio.run_coroutine_threadsafe(shutdown_app(app), _global_loop)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            _global_loop.add_signal_handler(sig, lambda: signal_handler(sig, None))
    except NotImplementedError:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("✅ Бот запущен!")

    try:
        # Запускаем polling синхронно (он сам создаст цикл)
        app.run_polling(allowed_updates=["message", "callback_query"])
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Ошибка в run_polling: {e}", exc_info=True)
    finally:
        loop.run_until_complete(shutdown_app(app))
        loop.close()

if __name__ == "__main__":
    main()
