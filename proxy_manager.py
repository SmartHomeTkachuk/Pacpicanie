"""
Менеджер прокси для Telegram бота.
Поддерживает ручной список из файла и публичные списки с GitHub как резерв.
Автоматически переключается на следующий прокси при ошибках.
"""

import asyncio
import logging
import random
from typing import List, Optional

import aiohttp
import aiohttp_socks  # Обязательно установите: pip install aiohttp-socks

logger = logging.getLogger(__name__)

class ProxyManager:
    def __init__(self, proxy_file: str = "proxies.txt"):
        self.proxy_file = proxy_file
        self.proxies: List[str] = []
        self.current_index = 0
        self._lock = asyncio.Lock()

    async def load_proxies(self) -> None:
        """
        Загружает прокси из файла (основной источник).
        Если файл пуст или отсутствует, использует публичный список с GitHub.
        """
        async with self._lock:
            # 1. Пытаемся загрузить из файла
            try:
                with open(self.proxy_file, "r") as f:
                    proxies = [line.strip() for line in f if line.strip()]
                if proxies:
                    self.proxies = proxies
                    logger.info(f"Загружено {len(self.proxies)} прокси из файла {self.proxy_file}")
                    return
            except FileNotFoundError:
                logger.warning(f"Файл {self.proxy_file} не найден. Загружаем публичный список.")

            # 2. Резерв: загружаем публичный список (формат socks5://host:port)
            logger.info("Загружаем публичный список прокси с GitHub...")
            public_proxies = await self._fetch_public_proxies()
            if public_proxies:
                self.proxies = public_proxies
                # Сохраняем в файл для кеширования
                with open(self.proxy_file, "w") as f:
                    f.write("\n".join(public_proxies))
                logger.info(f"Загружено {len(self.proxies)} публичных прокси и сохранено в {self.proxy_file}")
            else:
                # Если ничего не загрузилось, используем запасной список
                self.proxies = [
                    "socks5://free.glushilok.net:1080",  # Пример
                    "socks5://mtpro.xyz:1080"
                ]
                logger.warning("Используем запасной список прокси")

    async def _fetch_public_proxies(self) -> List[str]:
        """Загружает список SOCKS5 прокси из публичного репозитория."""
        urls = [
            "https://raw.githubusercontent.com/LoneKingCode/free-proxy-db/main/socks5.txt",
            "https://raw.githubusercontent.com/hookzof/socks5_list/main/socks5.txt",
        ]
        async with aiohttp.ClientSession() as session:
            for url in urls:
                try:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            # Парсим строки вида ip:port или socks5://ip:port
                            proxies = []
                            for line in text.splitlines():
                                line = line.strip()
                                if line and not line.startswith("#"):
                                    if "://" not in line:
                                        line = f"socks5://{line}"
                                    proxies.append(line)
                            if proxies:
                                return proxies
                except Exception as e:
                    logger.warning(f"Ошибка загрузки из {url}: {e}")
        return []

    async def get_proxy(self) -> Optional[str]:
        """Возвращает текущий рабочий прокси или None, если список пуст."""
        async with self._lock:
            if not self.proxies:
                await self.load_proxies()
            if not self.proxies:
                return None
            # Проверяем, не вышел ли индекс за пределы
            if self.current_index >= len(self.proxies):
                self.current_index = 0
            return self.proxies[self.current_index]

    async def mark_failed(self, proxy: str) -> None:
        """
        Отмечает прокси как нерабочий и переключается на следующий.
        Удаляет из списка, чтобы не использовать его снова.
        """
        async with self._lock:
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                logger.warning(f"Прокси {proxy} удалён из списка (не работает)")
                # Обновляем файл
                with open(self.proxy_file, "w") as f:
                    f.write("\n".join(self.proxies))
            # Сброс индекса, если он стал невалидным
            if self.current_index >= len(self.proxies):
                self.current_index = 0

    async def switch_to_next(self) -> Optional[str]:
        """Принудительно переключается на следующий прокси."""
        async with self._lock:
            if not self.proxies:
                return None
            self.current_index = (self.current_index + 1) % len(self.proxies)
            return self.proxies[self.current_index]
