"""Ограничение скорости на всю программу: один «кран» на скачивание и один на заливку.

Все потоки и очереди одного направления делят общий лимит (ведро токенов). 0 — без ограничения.
Скорость задаётся в Мбит/с, как её показывает провайдер: 100 Мбит/с ≈ 12,5 МБ/с.
"""
import threading
import time

SLICE = 256 * 1024          # ждём кусками: «Стоп» и пауза срабатывают не позже чем через долю секунды
BURST_SECONDS = 0.5         # сколько можно «накопить» в простое, чтобы не было рывков


class Bucket:
    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self._rate = 0.0        # байт/с; 0 — без ограничения
        self._tokens = 0.0
        self._stamp = time.monotonic()

    @property
    def mbit(self):
        return round(self._rate * 8 / 1_000_000)

    def set_mbit(self, mbit):
        with self._lock:
            self._rate = max(0, float(mbit or 0)) * 1_000_000 / 8
            self._tokens = 0.0
            self._stamp = time.monotonic()

    def _take(self, n):
        """Сколько секунд подождать, чтобы отправить n байт. Списывает n сразу (в долг)."""
        with self._lock:
            if self._rate <= 0:
                return 0.0
            now = time.monotonic()
            self._tokens = min(self._rate * BURST_SECONDS, self._tokens + (now - self._stamp) * self._rate)
            self._stamp = now
            self._tokens -= n
            return 0.0 if self._tokens >= 0 else -self._tokens / self._rate

    def consume(self, n, should_stop=None):
        """Ждёт, пока можно передать n байт. Возвращает False, если ожидание прервали (стоп/пауза)."""
        while n > 0:
            part = min(n, SLICE)
            n -= part
            wait = self._take(part)
            while wait > 0:
                if should_stop is not None and should_stop():
                    return False
                step = min(wait, 0.2)
                time.sleep(step)
                wait -= step
        return True


DOWN = Bucket('скачивание')
UP = Bucket('заливка')
LIMIT_MAX = 1000             # Мбит/с: больше домашнего канала не бывает


def valid_mbit(value):
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= LIMIT_MAX
