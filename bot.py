"""
FELIX SMART BOT — сигналы по стратегии «Не заходи раньше» (Smart Money / SMC)
на котировках Pocket Option. Только РЕАЛЬНЫЕ (не OTC) основные валютные пары.

ЛОГИКА (старший ТФ H1 + младший ТФ M5):
H1:
 1. Тренд по структуре: HH + HL — восходящий, LH + LL — нисходящий
    (если структура смешанная — запасной вариант по EMA20/EMA50).
 2. Снятие ликвидности: свеча H1 прокалывает последний swing-минимум — откат
    внутри тренда (для покупки) или последний swing-максимум (для продажи).
 3. Импульс с имбалансом: сразу после снятия — сильная свеча по тренду,
    оставившая FVG (разрыв между 1-й и 3-й свечой). Три свечи FVG должны идти
    подряд по времени — «разрыв» через дыру в данных не считается имбалансом.
 4. «Не заходи раньше»: на импульсе НЕ входим, ждём возврата цены в зону FVG.
    FVG считается сломанным, если свеча H1 закрылась за его дальней границей.
M5 (пока цена вернулась в FVG):
 5. Снятие локальной ликвидности — прокол локального минимума/максимума M5
    внутри или у зоны FVG.
 6. Слом структуры (BOS) — закрытие M5 за локальным максимумом/минимумом.
    На закрытии этой свечи приходит сигнал: вход сразу.
    Подтверждение ищется только в свечах ПОСЛЕ последнего перерыва в котировках,
    и последняя закрытая M5 должна быть только что закрывшейся (не старой).

Экспирация сигнала — 30 минут. Бот сам проверяет цену через 15 / 30 / 60 минут
и ведёт статистику (/stats). Один и тот же FVG даёт максимум один сигнал.

Свечи M5 и H1 бот собирает сам из потока котировок + подгружает историю при
старте. Команда /candles показывает, сколько свечей накоплено по каждой паре.

РАСПИСАНИЕ: сигналы — понедельник–пятница, 06:00–22:00 по Бергену (Europe/Oslo).
Свечи в будни копятся круглосуточно (нужно для структуры H1), но сигналы вне
окна не отправляются. Суббота и воскресенье — выходной: бот молчит и не строит свечи.

ЗАЩИТА ОТ ФАЛЬШИВЫХ КОТИРОВОК: в выходные Pocket Option присылает застывшую
последнюю цену. Бот игнорирует тики в субботу/воскресенье, а котировку считает
живой, только если цена реально меняется — иначе «рынок закрыт».
Из подгруженной истории выбрасываются выходные «плоские» свечи (high = low):
иначе они создают ложные равные уровни ликвидности и ломают структуру H1.

НАДЁЖНОСТЬ И ДИАГНОСТИКА:
- фоновая догрузка истории каждые 15 мин для пар, где она не пришла при старте;
  M5-история запрашивается с уменьшающейся глубиной (600 → 288 → 144 свечи);
- если ответ истории H1 пришёл мелкими свечами/тиками, из него же строится M5;
- после паузы в котировках (> 5 мин) бот сам догружает историю, чтобы закрыть дыру;
- сообщение в Telegram, если отдельная пара молчит > 30 мин, а остальные работают;
- лог «[alive]» в Railway: через 5 мин после старта и дальше раз в час;
- лог «[smc]» при каждой смене этапа по паре;
- сообщение в Telegram, если в рабочее время котировки не приходят > 10 мин,
  и сообщение, когда они вернулись;
- ошибки Telegram (в т.ч. Conflict) пишутся в лог коротко, без падения.

Исправления библиотеки pocket_option (перенесены из рабочего кода):
- патч fix_timestamp (TypeError: Unsupported type: <class 'int'>);
- единый ключ актива в хранилище свечей и в анализе;
- подгрузка истории через реальную сигнатуру load_history_period
  с запасными вариантами вызова и таймаутом.
"""

import io
import os
import sys
import time
import inspect
import random
import typing
import asyncio
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from telegram import Update, ReplyKeyboardMarkup
from telegram.error import Conflict, NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from pocket_option import PocketOptionClient
from pocket_option.constants import Regions
from pocket_option.contrib.default_init import default_init
from pocket_option.models import Asset, AuthorizationData, UpdateCloseValueItem


# ---------------------------------------------------------------------------
# ПАТЧ БИБЛИОТЕКИ POCKET OPTION (исправление TypeError в fix_timestamp)
# ---------------------------------------------------------------------------

def _patch_pocket_option_timestamp():
    """Библиотечная fix_timestamp не принимает int, а сервер присылает время
    именно как int. Подменяем функцию во всех модулях pocket_option."""
    try:
        import pocket_option.utils as po_utils
        import pocket_option.middlewares  # noqa: F401 — чтобы модуль точно был загружен
    except Exception as error:
        print(f"[patch] Не удалось импортировать модули pocket_option: {error}")
        return

    original = getattr(po_utils, "fix_timestamp", None)
    if original is None:
        print("[patch] fix_timestamp не найдена — патч не нужен.")
        return
    if getattr(original, "_felix_patched", False):
        return

    def _safe_fix_timestamp(ts):
        if isinstance(ts, (int, float)) and not isinstance(ts, bool):
            for candidate in (float(ts), str(ts)):
                try:
                    return original(candidate)
                except (TypeError, ValueError):
                    pass
            value = float(ts)
            if value > 1e12:  # миллисекунды -> секунды
                value /= 1000
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return original(ts)

    _safe_fix_timestamp._felix_patched = True

    patched_modules = []
    for name, module in list(sys.modules.items()):
        if not name.startswith("pocket_option") or module is None:
            continue
        if getattr(module, "fix_timestamp", None) is original:
            setattr(module, "fix_timestamp", _safe_fix_timestamp)
            patched_modules.append(name)

    print(f"[patch] fix_timestamp подменена в: {', '.join(patched_modules)}")


_patch_pocket_option_timestamp()


# ---------------------------------------------------------------------------
# КОНФИГ
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PO_SESSION = os.getenv("PO_SESSION")
PO_UID = os.getenv("PO_UID")
PO_IS_DEMO = 1

# Основные валютные пары (без золота и индексов). Точные имена в библиотеке
# ищутся при старте — см. resolve_pairs().
PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]

TF_LTF = 5 * 60        # M5 — младший ТФ (подтверждение)
TF_HTF = 60 * 60       # H1 — старший ТФ (тренд, ликвидность, FVG)

MAX_LTF_CANDLES = 600
MAX_HTF_CANDLES = 200
HISTORY_LTF_COUNTS = (600, 288, 144)   # глубина M5-истории: 1-я попытка, 2-я, 3-я и дальше
HISTORY_HTF_COUNT = 150                # сколько свечей H1 просить при подгрузке истории
HISTORY_TIMEOUT_SECONDS = 20

MIN_HTF_CANDLES = 30       # меньше — H1 анализ не запускаем
MIN_LTF_CANDLES = 30       # меньше — M5 анализ не запускаем

SWING_WING = 2                 # свеча-экстремум: выше/ниже 2 соседей с каждой стороны
HTF_SETUP_LOOKBACK = 24        # в скольких последних свечах H1 ищем FVG
HTF_LIQ_LOOKBACK = 20          # последний swing-минимум/максимум ищем не дальше 20 свечей H1 назад
HTF_SWEEP_MAX_BEFORE_IMPULSE = 6   # снятие не раньше чем за 6 свечей до импульса
IMPULSE_BODY_FACTOR = 1.3      # тело импульсной свечи >= 1.3 средних тел

LTF_CONFIRM_WINDOW = 24        # подтверждение ищем в последних 24 свечах M5 (2 часа)
LTF_LIQ_LOOKBACK = 12          # уровень локальной ликвидности — за 12 свечей M5 (1 час)
LTF_STRUCTURE_LOOKBACK = 6     # локальная структура для слома — 6 свечей M5
FVG_TOLERANCE_PCT = 0.0002     # допуск к границам FVG (0.02%)

EXPIRATION_MINUTES = 30
CHECK_HORIZONS = (15, 30, 60)  # через сколько минут проверять цену для статистики

AUTO_ANALYSIS_INTERVAL = 60    # автоанализ раз в минуту
COOLDOWN_MINUTES = 60          # не чаще 1 сигнала в час по паре+направлению

CIRCUIT_BREAKER_WINDOW = 10
CIRCUIT_BREAKER_THRESHOLD_PCT = 40

TIMEZONE = ZoneInfo("Europe/Oslo")
WORK_START_HOUR = 6
WORK_END_HOUR = 22
WEEKEND_DAYS = (5, 6)          # суббота, воскресенье — выходной

STALE_TICKS_SECONDS = 300      # цена не менялась дольше 5 мин — считаем, что рынок закрыт

HISTORY_BG_RETRY_MINUTES = 15  # фоновая догрузка истории для пар, где она не пришла
HEARTBEAT_FIRST_MINUTES = 5    # первый лог «[alive]» после старта
HEARTBEAT_MINUTES = 60         # дальше лог «[alive]» раз в час
TICK_ALERT_SECONDS = 600       # нет котировок > 10 мин в рабочее время — сообщение в Telegram
GAP_BACKFILL_SECONDS = 300     # пауза в котировках > 5 мин — после неё догружаем историю
PAIR_SILENT_SECONDS = 1800     # пара молчит > 30 мин, а остальные работают — сообщение

# ---------------------------------------------------------------------------
# СОСТОЯНИЕ
# ---------------------------------------------------------------------------

ACTIVE_ASSETS: list = []                      # заполняется resolve_pairs()

candle_store: dict[tuple[str, int], deque] = {}
last_tick: dict[str, tuple[float, float]] = {}   # asset_key -> (время последнего ИЗМЕНЕНИЯ цены, цена)
_store_lock = asyncio.Lock()

signal_history: list[dict] = []
used_setups: set[tuple] = set()               # (актив, направление, время FVG)
_history_lock = asyncio.Lock()

_notify_bot = None
_notify_chat_id: int | None = None
_circuit_breaker_active = False

_started_at = time.time()
last_reason: dict[str, str] = {}              # asset_key -> последний этап SMC
_last_analysis_at: float | None = None
_ticks_alert_sent = False
_quotes_gap_started: float | None = None      # когда котировки по всем парам остановились
_silent_pairs_notified: set[str] = set()      # пары, про молчание которых уже написали
_bg_history_round = 0                         # номер фоновой догрузки (для глубины M5)
_background_tasks: list = []                  # ссылки на фоновые задачи, чтобы их не собрал GC


def _asset_key(asset) -> str:
    return asset.value if hasattr(asset, "value") else str(asset)


def _pair_label(asset_key: str) -> str:
    return asset_key.replace("#", "").replace("_", " ").strip()


def _price_fmt(asset_key: str, price: float) -> str:
    return f"{price:.3f}" if "JPY" in asset_key.upper() else f"{price:.5f}"


def _age_text(age: float | None) -> str:
    if age is None:
        return "котировок не было"
    if age < 60:
        return f"тик {int(age)} сек назад"
    return f"тик {int(age // 60)} мин назад"


def resolve_pairs() -> list[str]:
    """Находит в Asset enum реальные (не OTC) пары из PAIRS.
    Возвращает список пар, которые не нашлись."""
    global ACTIVE_ASSETS

    names = [n for n in dir(Asset) if not n.startswith("_")]
    real_names = [n for n in names if "OTC" not in n.upper()]

    def _clean(name: str) -> str:
        return name.upper().replace("#", "").replace("_", "").replace("/", "")

    found = []
    missing = []
    for pair in PAIRS:
        exact = [n for n in real_names if _clean(n) == pair]
        if not exact:
            missing.append(pair)
            continue
        asset_obj = getattr(Asset, exact[0])
        found.append(asset_obj)
        print(f"[pairs] Пара найдена: {pair} -> Asset.{exact[0]}")

    ACTIVE_ASSETS = found

    if missing:
        print(f"[pairs] Не найдены: {', '.join(missing)}")
        print(f"[pairs] Все не-OTC активы в библиотеке ({len(real_names)}): {', '.join(sorted(real_names))}")

    return missing


# ---------------------------------------------------------------------------
# ХРАНИЛИЩЕ СВЕЧЕЙ
# ---------------------------------------------------------------------------

def _maxlen(period: int) -> int:
    return MAX_HTF_CANDLES if period == TF_HTF else MAX_LTF_CANDLES


def is_weekend() -> bool:
    return datetime.now(TIMEZONE).weekday() in WEEKEND_DAYS


def _is_weekend_filler(c: dict) -> bool:
    """Фальшивая выходная свеча: вся суббота по UTC, а также «плоские» свечи
    (high == low) с вечера пятницы и в воскресенье по UTC — рынок закрыт,
    Pocket Option просто повторяет последнюю цену."""
    t = datetime.fromtimestamp(int(c["time"]), timezone.utc)
    wd = t.weekday()
    if wd == 5:
        return True
    if c["high"] == c["low"] and (wd == 6 or (wd == 4 and t.hour >= 20)):
        return True
    return False


async def _push_tick(asset_key: str, price: float, ts: float):
    if is_weekend():
        return                                  # выходные — котировки фальшивые (застывшие)
    async with _store_lock:
        prev = last_tick.get(asset_key)
        if prev is not None and prev[1] == price:
            if ts - prev[0] > STALE_TICKS_SECONDS:
                return                          # цена давно застыла — свечи не строим
        else:
            last_tick[asset_key] = (ts, price)  # время обновляется только при реальном изменении цены
        for period in (TF_LTF, TF_HTF):
            bucket = int(ts // period * period)
            key = (asset_key, period)
            if key not in candle_store:
                candle_store[key] = deque(maxlen=_maxlen(period))
            dq = candle_store[key]
            if dq and dq[-1]["time"] == bucket:
                c = dq[-1]
                c["high"] = max(c["high"], price)
                c["low"] = min(c["low"], price)
                c["close"] = price
            elif not dq or dq[-1]["time"] < bucket:
                dq.append({"time": bucket, "open": price, "high": price, "low": price, "close": price})


async def _seed_candles(asset_key: str, period: int, candles: list[dict]):
    """Вливает историю в хранилище. Для закрытых свечей история точнее
    (живая свеча могла начаться с середины, H1 могла быть собрана из M5),
    поэтому побеждает история. Текущая формирующаяся свеча — всегда живая.
    Выходные «плоские» свечи выбрасываются (и из истории, и из уже накопленного)."""
    key = (asset_key, period)
    current_bucket = int(time.time() // period * period)
    async with _store_lock:
        merged = {c["time"]: c for c in candle_store.get(key, [])}
        for c in candles:
            if c["time"] == current_bucket and c["time"] in merged:
                continue
            merged[c["time"]] = dict(c)
        cleaned = [c for c in merged.values() if not _is_weekend_filler(c)]
        dropped = len(merged) - len(cleaned)
        ordered = sorted(cleaned, key=lambda c: c["time"])[-_maxlen(period):]
        candle_store[key] = deque(ordered, maxlen=_maxlen(period))
    if dropped:
        print(f"[history] {asset_key} {period // 60}м: выброшено выходных «плоских» свечей — {dropped}")


async def _build_htf_from_ltf(asset_key: str):
    """Если H1-история не загрузилась, собираем H1 из свечей M5."""
    async with _store_lock:
        ltf = list(candle_store.get((asset_key, TF_LTF), []))
    if not ltf:
        return 0
    buckets: dict[int, dict] = {}
    for c in ltf:
        b = c["time"] // TF_HTF * TF_HTF
        if b not in buckets:
            buckets[b] = {"time": b, "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"]}
        else:
            h = buckets[b]
            h["high"] = max(h["high"], c["high"])
            h["low"] = min(h["low"], c["low"])
            h["close"] = c["close"]
    await _seed_candles(asset_key, TF_HTF, list(buckets.values()))
    return len(buckets)


async def _get_closed_frame(asset_key: str, period: int) -> pd.DataFrame | None:
    """Только ЗАКРЫТЫЕ свечи — текущая формирующаяся отбрасывается."""
    current_bucket = int(time.time() // period * period)
    async with _store_lock:
        dq = candle_store.get((asset_key, period))
        if not dq:
            return None
        rows = [c for c in dq if c["time"] < current_bucket]
    if not rows:
        return None
    return pd.DataFrame(rows).reset_index(drop=True)


def _tail_after_gap(df: pd.DataFrame, period: int) -> pd.DataFrame:
    """Свечи после последней дыры в данных (перерыв котировок).
    Уровни до перерыва для локального подтверждения M5 не используем."""
    t = df["time"].values
    for idx in range(len(t) - 1, 0, -1):
        if t[idx] - t[idx - 1] > period:
            return df.iloc[idx:].reset_index(drop=True)
    return df


async def _candle_count(asset_key: str, period: int) -> int:
    async with _store_lock:
        return len(candle_store.get((asset_key, period), []))


async def _get_last_price(asset_key: str) -> float | None:
    async with _store_lock:
        tick = last_tick.get(asset_key)
    return tick[1] if tick else None


async def _tick_age(asset_key: str) -> float | None:
    async with _store_lock:
        tick = last_tick.get(asset_key)
    return time.time() - tick[0] if tick else None


# ---------------------------------------------------------------------------
# ОБЩЕЕ
# ---------------------------------------------------------------------------

def is_within_working_hours() -> bool:
    now_local = datetime.now(TIMEZONE)
    if now_local.weekday() in WEEKEND_DAYS:
        return False
    return WORK_START_HOUR <= now_local.hour < WORK_END_HOUR


async def _notify(text: str):
    if _notify_bot is None or _notify_chat_id is None:
        return
    try:
        await _notify_bot.send_message(chat_id=_notify_chat_id, text=text)
    except Exception as error:
        print(f"Не удалось отправить уведомление: {error}")


def _remember_reason(asset_key: str, reason: str):
    """Пишет в лог этап SMC по паре — только когда он меняется, без спама."""
    if last_reason.get(asset_key) != reason:
        last_reason[asset_key] = reason
        print(f"[smc] {_pair_label(asset_key)}: {reason}")


# ---------------------------------------------------------------------------
# SMC-АНАЛИЗ
# Вся логика написана для ПОКУПКИ. Для продажи график зеркалится
# (цены * -1, high <-> low) — тогда нисходящий тренд становится
# восходящим и работают те же правила. Результат зеркалится обратно.
# ---------------------------------------------------------------------------

def _mirror(df: pd.DataFrame) -> pd.DataFrame:
    m = df.copy()
    m["open"] = -df["open"]
    m["close"] = -df["close"]
    m["high"] = -df["low"]
    m["low"] = -df["high"]
    return m


def swing_points(df: pd.DataFrame, wing: int = SWING_WING) -> tuple[list[int], list[int]]:
    highs, lows = [], []
    h = df["high"].values
    lo = df["low"].values
    for i in range(wing, len(df) - wing):
        if h[i] == h[i - wing:i + wing + 1].max():
            highs.append(i)
        if lo[i] == lo[i - wing:i + wing + 1].min():
            lows.append(i)
    return highs, lows


def htf_trend(df: pd.DataFrame) -> tuple[str, str]:
    """Возвращает (направление, основание): направление UP / DOWN / FLAT,
    основание "structure" (HH+HL / LH+LL) или "ema" (запасной вариант)."""
    highs, lows = swing_points(df)
    if len(highs) >= 2 and len(lows) >= 2:
        h_prev, h_last = df["high"].iloc[highs[-2]], df["high"].iloc[highs[-1]]
        l_prev, l_last = df["low"].iloc[lows[-2]], df["low"].iloc[lows[-1]]
        if h_last > h_prev and l_last > l_prev:
            return "UP", "structure"
        if h_last < h_prev and l_last < l_prev:
            return "DOWN", "structure"

    closes = df["close"]
    if len(closes) >= 20:
        ema_fast = closes.ewm(span=20, adjust=False).mean().iloc[-1]
        ema_slow = closes.ewm(span=50, adjust=False).mean().iloc[-1]
        last = closes.iloc[-1]
        if last > ema_fast > ema_slow:
            return "UP", "ema"
        if last < ema_fast < ema_slow:
            return "DOWN", "ema"
    return "FLAT", ""


def trend_label(direction: str, basis: str) -> str:
    if direction == "UP":
        return "восходящий (HH + HL)" if basis == "structure" else "восходящий (EMA20 > EMA50)"
    return "нисходящий (LH + LL)" if basis == "structure" else "нисходящий (EMA20 < EMA50)"


def find_bullish_htf_setup(df: pd.DataFrame) -> dict | None:
    """H1: снятие ликвидности -> импульс -> FVG, который ещё не сломан.
    Возвращает самый свежий подходящий сетап."""
    n = len(df)
    if n < HTF_LIQ_LOOKBACK + 5:
        return None

    op = df["open"].values
    cl = df["close"].values
    hi = df["high"].values
    lo = df["low"].values
    tm = df["time"].values
    avg_body = (df["close"] - df["open"]).abs().tail(50).mean()
    if avg_body <= 0:
        return None

    start = max(HTF_LIQ_LOOKBACK + 2, n - HTF_SETUP_LOOKBACK)
    for i in range(n - 1, start - 1, -1):
        # три свечи FVG должны идти подряд — разрыв через дыру в данных не имбаланс
        if tm[i] - tm[i - 2] != 2 * TF_HTF:
            continue
        # FVG: минимум 3-й свечи выше максимума 1-й
        if lo[i] <= hi[i - 2]:
            continue
        imp = i - 1
        if cl[imp] <= op[imp] or (cl[imp] - op[imp]) < IMPULSE_BODY_FACTOR * avg_body:
            continue

        fvg_bottom, fvg_top = hi[i - 2], lo[i]

        # FVG сломан, если после него была свеча, закрывшаяся ниже нижней границы
        if i + 1 < n and (cl[i + 1:] < fvg_bottom).any():
            continue

        # Снятие ликвидности перед импульсом.
        # Уровень ликвидности — ПОСЛЕДНИЙ подтверждённый swing-минимум (последний
        # откат HL внутри тренда), а не самый низкий минимум за 20 свечей:
        # пробой суточного минимума в восходящем тренде — это уже слом тренда.
        sweep = None
        lower = max(imp - HTF_SWEEP_MAX_BEFORE_IMPULSE, SWING_WING * 2 + 1)
        for j in range(imp, lower - 1, -1):
            level = None
            # swing-минимум k подтверждён, если после него есть SWING_WING свечей до j
            for k in range(j - SWING_WING - 1, max(SWING_WING, j - HTF_LIQ_LOOKBACK) - 1, -1):
                window = lo[k - SWING_WING:k + SWING_WING + 1]
                if lo[k] == window.min():
                    level = lo[k]
                    break
            if level is not None and lo[j] < level:
                sweep = (j, level)
                break
        if sweep is None:
            continue

        j, level = sweep
        return {
            "sweep_index": j,
            "fvg_time": int(df["time"].iloc[i]),
            "fvg_bottom": float(fvg_bottom),
            "fvg_top": float(fvg_top),
            "sweep_level": float(level),
            "sweep_extreme": float(lo[j]),
            "target": float(hi[imp:].max()),   # ближайшая «проблемная зона» — хай импульса
        }
    return None


def find_bullish_ltf_confirmation(df: pd.DataFrame, fvg_bottom: float, fvg_top: float) -> dict | None:
    """M5: снятие локальной ликвидности в зоне FVG и слом структуры
    на ПОСЛЕДНЕЙ закрытой свече (чтобы сигнал был в момент входа)."""
    n = len(df)
    if n < LTF_LIQ_LOOKBACK + 5:
        return None

    cl = df["close"].values
    hi = df["high"].values
    lo = df["low"].values
    tol = abs(fvg_top) * FVG_TOLERANCE_PCT
    b = n - 1  # свеча слома — последняя закрытая

    start = max(LTF_LIQ_LOOKBACK, n - LTF_CONFIRM_WINDOW)
    for s in range(b - 1, start - 1, -1):
        level = lo[s - LTF_LIQ_LOOKBACK:s].min()
        if lo[s] >= level:
            continue                               # локальная ликвидность не снята
        if lo[s] > fvg_top + tol:
            continue                               # снятие не в зоне FVG — цена не вернулась
        if cl[s] < fvg_bottom - tol:
            continue                               # закрылись под FVG — зона не удержала

        struct_high = hi[max(0, s - LTF_STRUCTURE_LOOKBACK):s + 1].max()
        broke_now = cl[b] > struct_high
        broke_earlier = s + 1 < b and (cl[s + 1:b] > struct_high).any()
        if broke_now and not broke_earlier:
            return {
                "sweep_extreme": float(lo[s]),
                "bos_level": float(struct_high),
                "entry": float(cl[b]),
                "stop": float(lo[s]),
            }
    return None


def _time_label(ts: int) -> str:
    return datetime.fromtimestamp(ts, TIMEZONE).strftime("%d.%m %H:%M")


def _trend_text(trend_dir: str, basis: str, sign: int) -> str:
    """Тренд словами в координатах ИСХОДНОГО графика (для продажи график зеркальный)."""
    if trend_dir == "FLAT":
        return "без тренда (структура смешанная)"
    real = trend_dir if sign == 1 else ("DOWN" if trend_dir == "UP" else "UP")
    word = "вверх" if real == "UP" else "вниз"
    how = "по структуре" if basis == "structure" else "по EMA"
    return f"{word} {how}"


async def analyze_pair(asset) -> tuple[dict | None, str]:
    """Возвращает (сигнал или None, короткое объяснение для /signal)."""
    asset_key = _asset_key(asset)

    htf = await _get_closed_frame(asset_key, TF_HTF)
    ltf = await _get_closed_frame(asset_key, TF_LTF)
    htf_n = 0 if htf is None else len(htf)
    ltf_n = 0 if ltf is None else len(ltf)
    if htf_n < MIN_HTF_CANDLES:
        return None, f"мало свечей H1 ({htf_n}/{MIN_HTF_CANDLES})"
    if ltf_n < MIN_LTF_CANDLES:
        return None, f"мало свечей M5 ({ltf_n}/{MIN_LTF_CANDLES})"

    age = await _tick_age(asset_key)
    if age is None or age > STALE_TICKS_SECONDS:
        return None, "нет свежих котировок (рынок закрыт?)"

    # Последняя закрытая M5 должна быть только что закрывшейся. Иначе после
    # перерыва в котировках «слом структуры» проверялся бы на старой свече
    # и сигнал «входи сейчас» пришёл бы по цене двухчасовой давности.
    current_ltf = int(time.time() // TF_LTF * TF_LTF)
    if int(ltf["time"].iloc[-1]) != current_ltf - TF_LTF:
        return None, "M5: ждём первую закрытую свечу после перерыва в котировках"

    # Для подтверждения на M5 берём только свечи после последнего перерыва
    ltf_seg = _tail_after_gap(ltf, TF_LTF)

    # Проверяем обе стороны: покупку на исходном графике, продажу — на зеркальном.
    # Тренд оцениваем по структуре ДО снятия ликвидности: само снятие делает
    # новый минимум (максимум), и если смотреть после него, тренд «ломается».
    candidates = []
    reasons = []
    for direction, sign in (("UP", 1), ("DOWN", -1)):
        h = htf if sign == 1 else _mirror(htf)
        setup = find_bullish_htf_setup(h)
        if setup is None:
            continue
        trend_dir, basis = htf_trend(h.iloc[:setup["sweep_index"]])
        if trend_dir != "UP":      # в зеркале "UP" = нисходящий тренд оригинала
            reasons.append(
                f"FVG {'вверх' if sign == 1 else 'вниз'} от {_time_label(setup['fvg_time'])} есть, "
                f"но тренд H1 до снятия: {_trend_text(trend_dir, basis, sign)}"
            )
            continue
        candidates.append((setup["fvg_time"], direction, sign, setup, trend_label(direction, basis)))

    if not candidates:
        return None, "; ".join(reasons) if reasons else "H1: нет снятия ликвидности + FVG"

    candidates.sort(key=lambda c: c[0], reverse=True)   # самый свежий FVG первым
    signal = None
    reason = ""
    for _, direction, sign, setup, t_label in candidates:
        setup_id = (asset_key, direction, setup["fvg_time"])
        if setup_id in used_setups:
            reason = reason or "по этому FVG сигнал уже был"
            continue
        lt = ltf_seg if sign == 1 else _mirror(ltf_seg)
        conf = find_bullish_ltf_confirmation(lt, setup["fvg_bottom"], setup["fvg_top"])
        if conf is None:
            reason = reason or f"H1 {t_label}, FVG есть — ждём возврата и слома структуры на M5"
            continue
        if await is_on_cooldown(asset_key, direction):
            reason = reason or "cooldown по паре"
            continue
        signal = (direction, sign, setup, conf, t_label, setup_id)
        break

    if signal is None:
        return None, reason

    direction, sign, setup, conf, t_label, setup_id = signal
    # зеркалим значения обратно для продажи
    fvg_a, fvg_b = setup["fvg_bottom"] * sign, setup["fvg_top"] * sign
    signal = {
        "asset": asset_key,
        "direction": direction,
        "setup_id": setup_id,
        "trend_label": t_label,
        "entry": conf["entry"] * sign,
        "fvg_low": min(fvg_a, fvg_b),
        "fvg_high": max(fvg_a, fvg_b),
        "htf_sweep_level": setup["sweep_level"] * sign,
        "ltf_sweep": conf["sweep_extreme"] * sign,
        "bos_level": conf["bos_level"] * sign,
        "stop": conf["stop"] * sign,
        "target": setup["target"] * sign,
    }
    return signal, "✅ сигнал"


def format_signal(s: dict) -> str:
    k = s["asset"]
    p = lambda v: _price_fmt(k, v)  # noqa: E731
    if s["direction"] == "UP":
        head = "ВВЕРХ (CALL) 🟢"
        sweep_word, bos_word = "минимума", "локального максимума"
    else:
        head = "ВНИЗ (PUT) 🔴"
        sweep_word, bos_word = "максимума", "локального минимума"
    return (
        f"🧠 SMC-СИГНАЛ — {_pair_label(k)}\n"
        f"Направление: {head}\n"
        f"Вход: СЕЙЧАС, цена {p(s['entry'])}\n"
        f"Экспирация: {EXPIRATION_MINUTES} мин\n\n"
        f"H1 тренд: {s['trend_label']}\n"
        f"H1 снятие ликвидности: {sweep_word} {p(s['htf_sweep_level'])}\n"
        f"H1 FVG (имбаланс): {p(s['fvg_low'])} – {p(s['fvg_high'])}\n"
        f"M5: снятие локальной ликвидности ({p(s['ltf_sweep'])}) и слом {bos_word} {p(s['bos_level'])}\n\n"
        f"Ориентир стопа: {p(s['stop'])}\n"
        f"Ближайшая цель (проблемная зона): {p(s['target'])}"
    )


# ---------------------------------------------------------------------------
# СТАТИСТИКА (проверка через 15 / 30 / 60 мин) + CIRCUIT BREAKER
# ---------------------------------------------------------------------------

async def is_on_cooldown(asset_key: str, direction: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES)
    async with _history_lock:
        for entry in reversed(signal_history):
            if entry["time"] < cutoff:
                break
            if entry["asset"] == asset_key and entry["direction"] == direction:
                return True
    return False


async def record_signal(signal: dict):
    async with _history_lock:
        used_setups.add(signal["setup_id"])
        signal_history.append({
            "id": len(signal_history) + 1,
            "time": datetime.now(timezone.utc),
            "asset": signal["asset"],
            "direction": signal["direction"],
            "entry": signal["entry"],
            "results": {h: None for h in CHECK_HORIZONS},
        })
    print(f"[signal] {_pair_label(signal['asset'])} {signal['direction']} по {signal['entry']}")


def _judge(direction: str, entry: float, price: float) -> str:
    if price == entry:
        return "draw"
    went_up = price > entry
    return "win" if went_up == (direction == "UP") else "loss"


async def check_pending_outcomes():
    global _circuit_breaker_active
    now = datetime.now(timezone.utc)
    expiry_results_changed = False

    async with _history_lock:
        entries = list(signal_history)

    for entry in entries:
        for horizon in CHECK_HORIZONS:
            if entry["results"][horizon] is not None:
                continue
            if now < entry["time"] + timedelta(minutes=horizon):
                continue
            price = await _get_last_price(entry["asset"])
            if price is None:
                continue
            result = _judge(entry["direction"], entry["entry"], price)
            async with _history_lock:
                entry["results"][horizon] = (result, price)
            if horizon == EXPIRATION_MINUTES:
                expiry_results_changed = True
                icon = {"win": "✅", "loss": "❌", "draw": "➖"}[result]
                word = {"win": "зашёл", "loss": "не зашёл", "draw": "ничья"}[result]
                await _notify(
                    f"{icon} Сигнал #{entry['id']} {_pair_label(entry['asset'])} {word} "
                    f"(через {horizon} мин: {_price_fmt(entry['asset'], entry['entry'])} → "
                    f"{_price_fmt(entry['asset'], price)})"
                )

    if not expiry_results_changed:
        return

    async with _history_lock:
        done = [
            e["results"][EXPIRATION_MINUTES][0] for e in signal_history
            if e["results"][EXPIRATION_MINUTES] is not None
            and e["results"][EXPIRATION_MINUTES][0] != "draw"
        ]
    last_n = done[-CIRCUIT_BREAKER_WINDOW:]
    if len(last_n) < CIRCUIT_BREAKER_WINDOW:
        return
    winrate = sum(1 for r in last_n if r == "win") / len(last_n) * 100

    if winrate < CIRCUIT_BREAKER_THRESHOLD_PCT and not _circuit_breaker_active:
        _circuit_breaker_active = True
        await _notify(
            f"⚠️ Последние {CIRCUIT_BREAKER_WINDOW} сигналов зашли только на {winrate:.0f}% — "
            "стоит пересмотреть параметры или переждать рынок."
        )
    elif winrate >= CIRCUIT_BREAKER_THRESHOLD_PCT and _circuit_breaker_active:
        _circuit_breaker_active = False
        await _notify(f"✅ Точность восстановилась ({winrate:.0f}% из последних {CIRCUIT_BREAKER_WINDOW}).")


def stats_summary() -> str:
    if not signal_history:
        return "Пока не было ни одного сигнала."
    lines = [f"Всего сигналов: {len(signal_history)}"]
    for horizon in CHECK_HORIZONS:
        results = [e["results"][horizon][0] for e in signal_history if e["results"][horizon] is not None]
        decisive = [r for r in results if r != "draw"]
        mark = " (экспирация)" if horizon == EXPIRATION_MINUTES else ""
        if not decisive:
            txt = f"только ничьи ({len(results)})" if results else "пока нет данных"
            lines.append(f"Через {horizon} мин{mark}: {txt}")
            continue
        wins = sum(1 for r in decisive if r == "win")
        draws = len(results) - len(decisive)
        draw_txt = f", ничьих: {draws}" if draws else ""
        lines.append(f"Через {horizon} мин{mark}: {wins}/{len(decisive)} ({round(wins / len(decisive) * 100)}%){draw_txt}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# POCKET OPTION CLIENT
# ---------------------------------------------------------------------------

po_client: PocketOptionClient | None = None
po_connected = False
_was_ever_connected = False

# Логические поля запроса истории и возможные имена этих полей в библиотеке.
_HISTORY_FIELD_ALIASES = {
    "asset": ("asset", "symbol", "active", "asset_name", "pair", "instrument"),
    "period": ("period", "timeframe", "tf", "interval", "candle_period"),
    "time": ("time", "end_time", "timestamp", "to", "end", "end_ts"),
    "offset": ("offset",),
    "count": ("count", "limit", "amount", "size"),
    "index": ("index", "request_id", "req_id", "id"),
}
_history_api_logged = False
_raw_sample_logged: set[int] = set()          # периоды, для которых уже показали пример ответа
_pending_history: deque = deque(maxlen=200)   # (asset_key, period) в порядке запросов


def _take_pending(asset_key: str | None = None, period: int | None = None):
    """Достаёт из очереди ожидающий запрос истории. Если актив/период известны —
    ищет именно его, а не берёт первый попавшийся: иначе запрос без ответа
    (как у NZDUSD) сдвигал бы очередь и ответы приписывались бы не тем парам."""
    if not _pending_history:
        return None
    if asset_key is None and period is None:
        return _pending_history.popleft()
    for i, item in enumerate(_pending_history):
        if (asset_key is None or item[0] == asset_key) and (period is None or item[1] == period):
            del _pending_history[i]
            return item
    return None


def _history_request_model(func):
    """load_history_period принимает ОДИН аргумент — объект запроса.
    Достаём его тип из аннотации функции (обычно это pydantic-модель)."""
    unwrapped = inspect.unwrap(func)
    try:
        params = [p for p in inspect.signature(unwrapped).parameters.values() if p.name != "self"]
    except (TypeError, ValueError):
        return None
    if not params:
        return None
    param = params[0]
    try:
        hints = typing.get_type_hints(unwrapped)
    except Exception:
        hints = {}
    model = hints.get(param.name, param.annotation)
    if model is inspect.Parameter.empty or isinstance(model, str):
        return None
    return model


def _log_history_api(func, model):
    """Один раз пишет в логи, как устроен запрос истории в библиотеке."""
    try:
        print(f"[history] Сигнатура: {inspect.signature(inspect.unwrap(func))}")
    except Exception as error:
        print(f"[history] Сигнатура недоступна: {error}")
    if model is not None:
        fields = getattr(model, "model_fields", None)
        if fields:
            desc = ", ".join(
                f"{name}(alias={info.alias})" if info.alias else name for name, info in fields.items()
            )
            print(f"[history] Модель запроса: {getattr(model, '__name__', model)} — поля: {desc}")
        else:
            print(f"[history] Тип аргумента: {model}")
    try:
        source = inspect.getsource(inspect.unwrap(func)).splitlines()[:25]
        for line in source:
            print(f"[history] | {line}")
    except Exception:
        pass


def _history_values(asset, period: int, count: int, asset_as_str: bool) -> dict:
    now = int(time.time())
    return {
        "asset": _asset_key(asset) if asset_as_str else asset,
        "period": period,
        "time": now,
        "offset": count * period,       # в протоколе Pocket Option offset — глубина в секундах
        "count": count,
        "index": now * 100 + random.randint(0, 99),
    }


def _build_history_payloads(model, asset, period: int, count: int) -> list:
    """Варианты объекта запроса — от самого вероятного к запасным."""
    payloads = []
    fields = getattr(model, "model_fields", None) if model is not None else None

    for asset_as_str in (False, True):
        vals = _history_values(asset, period, count, asset_as_str)
        if fields:
            by_name, by_alias = {}, {}
            for fname, finfo in fields.items():
                keys = {fname.lower()}
                if finfo.alias:
                    keys.add(finfo.alias.lower())
                for logical, aliases in _HISTORY_FIELD_ALIASES.items():
                    if keys & set(aliases):
                        by_name[fname] = vals[logical]
                        by_alias[finfo.alias or fname] = vals[logical]
                        break
            for data in (by_name, by_alias):
                try:
                    payloads.append(model.model_validate(data))
                except Exception:
                    try:
                        payloads.append(model(**data))
                    except Exception:
                        pass
        # запасной вариант — обычный словарь в формате протокола Pocket Option
        payloads.append({
            "asset": vals["asset"] if asset_as_str else _asset_key(asset),
            "index": vals["index"],
            "time": vals["time"],
            "offset": vals["offset"],
            "period": period,
        })
    return payloads


async def _call_load_history(asset, period: int, count: int):
    global _history_api_logged

    func = po_client.emit.load_history_period
    model = _history_request_model(func)
    if not _history_api_logged:
        _history_api_logged = True
        _log_history_api(func, model)

    asset_key = _asset_key(asset)
    last_error = None
    for payload in _build_history_payloads(model, asset, period, count):
        try:
            _pending_history.append((asset_key, period))
            return await asyncio.wait_for(func(payload), timeout=HISTORY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            # запрос ушёл, но ответ не вернулся из функции — возможно, он придёт событием
            return None
        except Exception as error:
            _take_pending(asset_key, period)
            last_error = error
            continue
    raise last_error or RuntimeError("не удалось собрать запрос истории")


def _get_field(obj, *names):
    for name in names:
        value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
        if value is not None:
            return value
    return None


def _to_unix(value) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    value = float(value)
    if value > 1e12:
        value /= 1000
    return int(value)


def _raw_items(result) -> list:
    """Достаёт из ответа истории сам список свечей/тиков."""
    raw = result
    for attr in ("candles", "data", "history"):
        value = result.get(attr) if isinstance(result, dict) else getattr(result, attr, None)
        if value:
            raw = value
            break
    return list(raw) if isinstance(raw, (list, tuple)) else []


def _item_time(item):
    if isinstance(item, dict):
        return item.get("time")
    if isinstance(item, (list, tuple)):
        return item[0] if item else None
    return getattr(item, "time", None)


def _history_step(result) -> int | None:
    """Типичный шаг по времени между элементами ответа (сек): 60 — минутки/тики,
    3600 — часовые свечи. Нужен, чтобы понять, можно ли из ответа собрать M5."""
    times = []
    for item in _raw_items(result)[:300]:
        try:
            t = _item_time(item)
            if t is not None:
                times.append(_to_unix(t))
        except Exception:
            continue
    times = sorted(set(times))
    if len(times) < 3:
        return None
    diffs = sorted(b - a for a, b in zip(times, times[1:]))
    return diffs[len(diffs) // 2]


def _log_raw_sample(result, period: int):
    """Один раз на таймфрейм показывает в логах, как выглядит ответ истории."""
    if period in _raw_sample_logged or result is None:
        return
    _raw_sample_logged.add(period)
    items = _raw_items(result)
    sample = ", ".join(repr(x)[:120] for x in items[:2])
    print(
        f"[history] Пример ответа {period // 60}м: тип {type(result).__name__}, "
        f"элементов {len(items)}, шаг {_history_step(result)} сек, первые: {sample or '—'}"
    )


def _parse_history(result, period: int) -> list[dict]:
    """Понимает разные форматы ответа: объекты/словари свечей,
    списки [time, open, close, high, low] и просто тики [time, price]."""
    raw = _raw_items(result)
    if not raw:
        return []

    candles: dict[int, dict] = {}

    def _add(t, o, h, lo, c):
        bucket = _to_unix(t) // period * period
        if bucket not in candles:
            candles[bucket] = {"time": bucket, "open": o, "high": h, "low": lo, "close": c}
        else:
            cc = candles[bucket]
            cc["high"] = max(cc["high"], h)
            cc["low"] = min(cc["low"], lo)
            cc["close"] = c

    for item in raw:
        try:
            if isinstance(item, dict) or hasattr(item, "open"):
                get = (lambda k: item[k]) if isinstance(item, dict) else (lambda k: getattr(item, k))
                t = get("time")
                o, h, lo, c = (float(get(k)) for k in ("open", "high", "low", "close"))
            elif isinstance(item, (list, tuple)) and len(item) >= 5:
                t = item[0]
                o, c, h, lo = (float(x) for x in item[1:5])
                if h < max(o, c) or lo > min(o, c):          # другой порядок: t, o, h, l, c
                    o, h, lo, c = (float(x) for x in item[1:5])
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                t = item[0]
                o = h = lo = c = float(item[1])              # тик — копим в свечу
            else:
                continue
            _add(t, o, h, lo, c)
        except Exception:
            continue

    return sorted(candles.values(), key=lambda c: c["time"])


async def _ingest_history(asset_key: str, period: int, result, source: str) -> int:
    """Разбирает ответ истории и вливает его в хранилище.
    Если ответ на запрос H1 пришёл мелкими свечами или тиками —
    из него же собирается M5 (M5-история у Pocket Option приходит не всегда)."""
    _log_raw_sample(result, period)
    parsed = _parse_history(result, period)
    if not parsed:
        return 0
    await _seed_candles(asset_key, period, parsed)
    print(f"[history] {source}: подгружено {asset_key} {period // 60}м — свечей {len(parsed)}")

    if period == TF_HTF:
        step = _history_step(result)
        if step is not None and step <= TF_LTF:
            fine = _parse_history(result, TF_LTF)
            if fine:
                await _seed_candles(asset_key, TF_LTF, fine)
                print(f"[history] {asset_key}: M5 собран из той же истории — свечей {len(fine)}")

    if period == TF_LTF and await _candle_count(asset_key, TF_HTF) < MIN_HTF_CANDLES:
        built = await _build_htf_from_ltf(asset_key)
        print(f"[history] {asset_key}: H1 собран из M5 — {built} свечей")
    return len(parsed)


async def _on_history_event(payload):
    """Ответ с историей, пришедший отдельным событием от сервера."""
    try:
        raw_asset = _get_field(payload, "asset", "symbol", "active")
        period = _get_field(payload, "period", "timeframe")
        asset_key = _asset_key(raw_asset) if raw_asset is not None else None
        period = int(period) if period is not None else None

        if asset_key is None or period is None:
            pending = _take_pending(asset_key, period)
            if pending is None:
                print(f"[history] Событие без актива/периода, тип {type(payload).__name__}")
                return
            asset_key = asset_key or pending[0]
            period = period or pending[1]
        else:
            _take_pending(asset_key, period)   # ответ пришёл — убираем запрос из очереди

        got = await _ingest_history(asset_key, period, payload, "Событие")
        if not got:
            print(f"[history] Событие: пусто для {asset_key} {period // 60}м")
    except Exception as error:
        print(f"[history] Ошибка обработки события истории: {error}")


def _register_history_events(client):
    """Подписываемся на все события библиотеки, похожие на ответ с историей."""
    on = client.on
    names = [n for n in dir(on) if not n.startswith("_")]
    print(f"[history] События библиотеки: {', '.join(names)}")
    hooked = []
    for name in names:
        low = name.lower()
        if "history" in low or "candle" in low:
            try:
                getattr(on, name)(_on_history_event)
                hooked.append(name)
            except Exception as error:
                print(f"[history] Не удалось подписаться на {name}: {error}")
    print(f"[history] Подписка на ответы истории: {', '.join(hooked) or 'нет подходящих событий'}")


HISTORY_RETRIES = 3


def _history_count(period: int, attempt: int) -> int:
    """Глубина запроса истории. Для M5 с каждой попыткой меньше:
    сервер может не отдавать слишком глубокую M5-историю."""
    if period == TF_HTF:
        return HISTORY_HTF_COUNT
    return HISTORY_LTF_COUNTS[min(max(attempt, 1) - 1, len(HISTORY_LTF_COUNTS) - 1)]


async def _request_history(asset, period: int, count: int):
    asset_key = _asset_key(asset)
    try:
        result = await _call_load_history(asset, period, count)
        if result is None:
            return
        got = await _ingest_history(asset_key, period, result, "Ответ")
        if not got:
            print(f"[history] Ответ без свечей: {asset_key} {period // 60}м "
                  f"(глубина {count}), тип {type(result).__name__}")
    except Exception as error:
        print(f"[history] Не удалось: {asset_key} {period // 60}м: {error} — копим вживую.")


async def _missing_periods(asset_key: str) -> list[int]:
    missing = []
    if await _candle_count(asset_key, TF_LTF) < MIN_LTF_CANDLES:
        missing.append(TF_LTF)
    if await _candle_count(asset_key, TF_HTF) < MIN_HTF_CANDLES:
        missing.append(TF_HTF)
    return missing


async def _try_preload_history(assets: list):
    """Первая попытка — все пары и оба ТФ (это же закрывает дыры после перерыва),
    дальше — только то, чего не хватает."""
    plan = {_asset_key(a): (a, [TF_LTF, TF_HTF]) for a in assets}

    for attempt in range(1, HISTORY_RETRIES + 1):
        if attempt > 1:
            names = ", ".join(f"{k} ({'/'.join(str(p // 60) + 'м' for p in ps)})" for k, (_, ps) in plan.items())
            print(f"[history] Повторный запрос #{attempt - 1}: {names}")

        for asset, periods in plan.values():
            for period in periods:
                await _request_history(asset, period, _history_count(period, attempt))
                await asyncio.sleep(0.5)   # не заваливаем сервер запросами

        await asyncio.sleep(10)            # даём время прийти ответам-событиям

        next_plan = {}
        for key, (asset, _) in plan.items():
            missing = await _missing_periods(key)
            if missing:
                next_plan[key] = (asset, missing)
        plan = next_plan
        if not plan:
            break

    for asset in assets:
        key = _asset_key(asset)
        m5 = await _candle_count(key, TF_LTF)
        h1 = await _candle_count(key, TF_HTF)
        if h1 < MIN_HTF_CANDLES and m5 > h1:
            h1 = await _build_htf_from_ltf(key)
        ready = "✅" if m5 >= MIN_LTF_CANDLES and h1 >= MIN_HTF_CANDLES else "⏳ копим вживую"
        print(f"[history] Итог {key}: M5 {m5} | H1 {h1} {ready}")


# ---------------------------------------------------------------------------
# ФОНОВЫЕ ПРОВЕРКИ: догрузка истории, лог «жив», контроль котировок
# ---------------------------------------------------------------------------

async def _history_retry_loop():
    """Каждые 15 минут пробует догрузить историю для пар, где её не хватает
    (например NZDUSD). Когда пара готова — сообщает в Telegram."""
    global _bg_history_round
    while True:
        await asyncio.sleep(HISTORY_BG_RETRY_MINUTES * 60)
        try:
            if not po_connected or is_weekend() or not ACTIVE_ASSETS:
                continue
            todo = []
            for a in ACTIVE_ASSETS:
                periods = await _missing_periods(_asset_key(a))
                if periods:
                    todo.append((a, periods))
            if not todo:
                continue

            _bg_history_round += 1
            attempt = (_bg_history_round - 1) % len(HISTORY_LTF_COUNTS) + 1
            names = ", ".join(
                f"{_asset_key(a)} ({'/'.join(str(p // 60) + 'м' for p in ps)})" for a, ps in todo
            )
            print(f"[history] Фоновая догрузка (глубина M5 {_history_count(TF_LTF, attempt)}): {names}")
            for a, periods in todo:
                for period in periods:
                    await _request_history(a, period, _history_count(period, attempt))
                    await asyncio.sleep(0.5)
            await asyncio.sleep(10)

            for a, _ in todo:
                key = _asset_key(a)
                m5 = await _candle_count(key, TF_LTF)
                h1 = await _candle_count(key, TF_HTF)
                if h1 < MIN_HTF_CANDLES and m5 > h1:
                    h1 = await _build_htf_from_ltf(key)
                ready = m5 >= MIN_LTF_CANDLES and h1 >= MIN_HTF_CANDLES
                print(f"[history] После догрузки {key}: M5 {m5} | H1 {h1} {'✅' if ready else '⏳'}")
                if ready:
                    await _notify(f"✅ {_pair_label(key)}: история загрузилась (M5 {m5} | H1 {h1}), пара в работе.")
        except Exception as error:
            print(f"[history] Ошибка фоновой догрузки: {error}")


async def _log_heartbeat():
    now_local = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    if is_weekend():
        window = "выходной"
    elif is_within_working_hours():
        window = "рабочее окно"
    else:
        window = "вне рабочего окна"
    if _last_analysis_at is None:
        analysis = "автоанализ ещё не запускался"
    else:
        analysis = f"последний автоанализ {int(time.time() - _last_analysis_at)} сек назад"
    print(
        f"[alive] {now_local} (Берген) | Pocket Option: {'подключено' if po_connected else 'НЕТ соединения'} | "
        f"{window} | {analysis} | сигналов за сессию: {len(signal_history)}"
    )
    for a in ACTIVE_ASSETS:
        k = _asset_key(a)
        m5 = await _candle_count(k, TF_LTF)
        h1 = await _candle_count(k, TF_HTF)
        age = await _tick_age(k)
        reason = last_reason.get(k, "—")
        print(f"[alive] {_pair_label(k)}: M5 {m5} | H1 {h1} | {_age_text(age)} | {reason}")


async def _heartbeat_loop():
    await asyncio.sleep(HEARTBEAT_FIRST_MINUTES * 60)
    while True:
        try:
            await _log_heartbeat()
        except Exception as error:
            print(f"[alive] Ошибка: {error}")
        await asyncio.sleep(HEARTBEAT_MINUTES * 60)


async def _tick_watchdog_loop():
    """Раз в минуту:
    - в будни отслеживает паузы в котировках по всем парам; когда котировки
      вернулись после паузы > 5 мин — догружает историю, чтобы закрыть дыру;
    - в рабочее время пишет в Telegram, если котировок нет > 10 мин, и когда вернулись;
    - в рабочее время пишет, если отдельная пара молчит > 30 мин, а остальные работают."""
    global _ticks_alert_sent, _quotes_gap_started
    while True:
        await asyncio.sleep(60)
        try:
            if is_weekend() or not ACTIVE_ASSETS:
                continue
            if time.time() - _started_at < TICK_ALERT_SECONDS:
                continue                        # сразу после старта даём время подключиться

            ages = {_asset_key(a): await _tick_age(_asset_key(a)) for a in ACTIVE_ASSETS}
            known = [x for x in ages.values() if x is not None]
            freshest = min(known) if known else None

            # --- паузы в котировках и догрузка истории после них ---
            paused = freshest is None or freshest > GAP_BACKFILL_SECONDS
            if paused and _quotes_gap_started is None:
                _quotes_gap_started = time.time() - (freshest or 0)
                print("[watchdog] Котировки по всем парам остановились.")
            elif not paused and _quotes_gap_started is not None:
                minutes = int((time.time() - _quotes_gap_started) / 60)
                _quotes_gap_started = None
                print(f"[watchdog] Котировки вернулись после паузы ~{minutes} мин — догружаю историю.")
                if po_connected:
                    _background_tasks.append(asyncio.create_task(_try_preload_history(ACTIVE_ASSETS)))

            if not is_within_working_hours():
                continue

            # --- все пары молчат ---
            live = [k for k, x in ages.items() if x is not None and x <= TICK_ALERT_SECONDS]
            if not live and not _ticks_alert_sent:
                _ticks_alert_sent = True
                print("[watchdog] Котировки не приходят больше 10 минут.")
                await _notify(
                    "⚠️ Котировки Pocket Option не приходят больше 10 минут — анализ стоит.\n"
                    f"Pocket Option: {'подключено' if po_connected else 'нет соединения'}.\n"
                    "Если не восстановится — перезапусти деплой в Railway или проверь PO_SESSION."
                )
            elif live and _ticks_alert_sent:
                _ticks_alert_sent = False
                print("[watchdog] Котировки снова идут.")
                await _notify(f"✅ Котировки снова идут ({len(live)}/{len(ACTIVE_ASSETS)} пар).")

            # --- отдельная пара молчит, остальные работают ---
            if time.time() - _started_at < PAIR_SILENT_SECONDS or len(live) * 2 < len(ACTIVE_ASSETS):
                continue
            for k, age in ages.items():
                silent = age is None or age > PAIR_SILENT_SECONDS
                if silent and k not in _silent_pairs_notified:
                    _silent_pairs_notified.add(k)
                    print(f"[watchdog] {k}: котировки не приходят, остальные пары работают.")
                    await _notify(
                        f"⚠️ {_pair_label(k)}: котировки не приходят больше 30 минут, "
                        "хотя остальные пары работают. Похоже, Pocket Option сейчас не даёт "
                        "реальную (не OTC) котировку этой пары — сигналов по ней не будет."
                    )
                elif not silent and k in _silent_pairs_notified:
                    _silent_pairs_notified.discard(k)
                    await _notify(f"✅ {_pair_label(k)}: котировки пошли.")
        except Exception as error:
            print(f"[watchdog] Ошибка: {error}")


async def start_pocket_option_client():
    global po_client, po_connected, _was_ever_connected

    if not PO_SESSION or not PO_UID:
        print("PO_SESSION / PO_UID не заданы — клиент Pocket Option не запущен.")
        return

    missing = resolve_pairs()
    if missing:
        await _notify(f"⚠️ Не нашёл в библиотеке пары: {', '.join(missing)}. Они не отслеживаются.")
    if not ACTIVE_ASSETS:
        await _notify("❌ Не найдено ни одной пары — бот не может работать. Пришли логи [pairs].")
        return

    po_client = PocketOptionClient(logger=True)

    default_init(
        po_client,
        authorization=AuthorizationData.model_validate(
            {
                "session": PO_SESSION,
                "isDemo": PO_IS_DEMO,
                "uid": int(PO_UID),
                "platform": 2,
                "isFastHistory": True,
                "isOptimized": True,
            },
        ),
        sub_assets=ACTIVE_ASSETS,
        sub_period=TF_LTF,
    )

    try:
        _register_history_events(po_client)
    except Exception as error:
        print(f"[history] Не удалось подписаться на события истории: {error}")

    @po_client.on.update_close_value
    async def _on_update_close_value(items: list[UpdateCloseValueItem]):
        ts = time.time()
        for item in items:
            raw_asset = getattr(item, "asset", None) or getattr(item, "symbol", None)
            price = getattr(item, "value", None) or getattr(item, "price", None)
            if raw_asset is None or price is None:
                continue
            try:
                price_f = float(price)
            except (TypeError, ValueError):
                continue
            await _push_tick(_asset_key(raw_asset), price_f, ts)

    @po_client.on.connect
    async def _on_connect():
        global po_connected, _was_ever_connected
        was_reconnect = _was_ever_connected and not po_connected
        po_connected = True
        _was_ever_connected = True
        print("Pocket Option: соединение установлено (демо).")
        if was_reconnect:
            await _notify("✅ Соединение с Pocket Option восстановлено.")
        _background_tasks.append(asyncio.create_task(_try_preload_history(ACTIVE_ASSETS)))

    @po_client.on.disconnect
    async def _on_disconnect():
        global po_connected
        if po_connected:
            await _notify("⚠️ Потеряно соединение с Pocket Option. Пробую переподключиться...")
        po_connected = False
        print("Pocket Option: соединение потеряно.")

    while True:
        try:
            await po_client.connect(Regions.DEMO)
        except Exception as error:
            print(f"Ошибка соединения с Pocket Option: {error}, повтор через 15 сек.")
            po_connected = False
            await asyncio.sleep(15)
            continue
        break


# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------

def _job_name(chat_id: int) -> str:
    return f"smart_analysis_{chat_id}"


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    auto_on = bool(context.job_queue.get_jobs_by_name(_job_name(chat_id)))

    po_line = "🟢 Pocket Option: подключено (демо)" if po_connected else "🔴 Pocket Option: нет соединения"
    auto_line = "🟢 Автоанализ: ВКЛЮЧЁН" if auto_on else "🔴 Автоанализ: ВЫКЛЮЧЕН"
    if is_weekend():
        window = "💤 сегодня выходной (бот молчит)"
    elif is_within_working_hours():
        window = "🟢 сейчас в рабочем окне"
    else:
        window = "🔴 сейчас вне рабочего окна (сигналов нет, свечи копятся)"
    breaker = "\n⚠️ Circuit breaker активен (низкая точность)" if _circuit_breaker_active else ""

    if _last_analysis_at is None:
        analysis_line = "🔎 Автоанализ ещё не запускался"
    else:
        analysis_line = f"🔎 Последний автоанализ: {int(time.time() - _last_analysis_at)} сек назад"

    live = 0
    for a in ACTIVE_ASSETS:
        age = await _tick_age(_asset_key(a))
        if age is not None and age <= STALE_TICKS_SECONDS:
            live += 1
    if is_weekend():
        market_line = "💤 Выходные — рынок закрыт, котировки игнорируются"
    elif live:
        market_line = f"📈 Котировки идут: {live}/{len(ACTIVE_ASSETS)} пар"
    else:
        market_line = "💤 Котировок нет — цена не меняется (рынок закрыт?)"
    pairs = ", ".join(_pair_label(_asset_key(a)) for a in ACTIVE_ASSETS) or "— ещё не найдены"

    uptime_h = (time.time() - _started_at) / 3600

    await update.message.reply_text(
        "✅ FELIX SMART BOT работает\n\n"
        f"{po_line}\n{auto_line}\n{market_line}\n{analysis_line}\n"
        f"⏱ Работает без перезапуска: {uptime_h:.1f} ч | сигналов: {len(signal_history)}\n"
        f"🕐 Сигналы: пн–пт {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 (Берген), сб–вс выходной\n"
        f"{window}{breaker}\n\n"
        f"Пары: {pairs}\n"
        f"Стратегия: «Не заходи раньше» (SMC), H1 + M5\n"
        f"Экспирация: {EXPIRATION_MINUTES} мин | проверка: {', '.join(str(h) for h in CHECK_HORIZONS)} мин\n"
        f"Cooldown: {COOLDOWN_MINUTES} мин на пару\n\n"
        f"Chat ID: {chat_id}"
    )


async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Присылает CSV со всеми свечами H1 и M5 по всем парам — для разбора и бэктеста."""
    lines = ["pair,tf,time_utc,time_bergen,open,high,low,close"]
    total = 0
    async with _store_lock:
        snapshot = {k: list(v) for k, v in candle_store.items()}
    for (asset_key, period), candles in sorted(snapshot.items()):
        tf = "H1" if period == TF_HTF else "M5"
        for c in candles:
            t = int(c["time"])
            utc = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M")
            local = datetime.fromtimestamp(t, TIMEZONE).strftime("%Y-%m-%d %H:%M")
            lines.append(
                f"{_pair_label(asset_key)},{tf},{utc},{local},"
                f"{c['open']},{c['high']},{c['low']},{c['close']}"
            )
            total += 1
    if total == 0:
        await update.message.reply_text("Свечей в памяти пока нет — экспортировать нечего.")
        return
    stamp = datetime.now(TIMEZONE).strftime("%Y%m%d_%H%M")
    data = io.BytesIO("\n".join(lines).encode("utf-8"))
    await update.message.reply_document(
        document=data,
        filename=f"felix_smart_candles_{stamp}.csv",
        caption=f"📦 Свечи H1 и M5 по всем парам ({total} строк). Перешли этот файл в чат с Claude.",
    )


async def candles_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Диагностика: реально ли копятся свечи M5 и H1."""
    if not ACTIVE_ASSETS:
        await update.message.reply_text("Пары ещё не найдены — проверь /status и логи [pairs].")
        return
    lines = ["🕯 Свечи в памяти бота (нужно: H1 ≥ "
             f"{MIN_HTF_CANDLES}, M5 ≥ {MIN_LTF_CANDLES})\n"]
    for a in ACTIVE_ASSETS:
        k = _asset_key(a)
        m5 = await _candle_count(k, TF_LTF)
        h1 = await _candle_count(k, TF_HTF)
        age = await _tick_age(k)
        ready = "✅" if m5 >= MIN_LTF_CANDLES and h1 >= MIN_HTF_CANDLES else "⏳"
        silent = " | 🔇 пара молчит" if k in _silent_pairs_notified else ""
        lines.append(f"{ready} {_pair_label(k)}: M5 {m5} | H1 {h1} | {_age_text(age)}{silent}")
    await update.message.reply_text("\n".join(lines))


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await check_pending_outcomes()
    await update.message.reply_text("📊 Статистика\n\n" + stats_summary())


async def strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Стратегия «Не заходи раньше» (Smart Money):\n\n"
        "H1:\n"
        "1. Тренд по структуре (HH+HL / LH+LL)\n"
        "2. Снятие ликвидности — прокол последнего отката (swing-минимума/максимума)\n"
        "3. Импульс по тренду с имбалансом (FVG)\n"
        "4. Не входим на импульсе — ждём возврата цены в FVG\n\n"
        "M5 (цена в FVG):\n"
        "5. Снятие локальной ликвидности\n"
        "6. Слом структуры — на закрытии этой свечи сигнал, вход сразу\n\n"
        f"Экспирация {EXPIRATION_MINUTES} мин. Статистика через "
        f"{', '.join(str(h) for h in CHECK_HORIZONS)} мин — /stats.\n"
        "Один FVG = максимум один сигнал.\n"
        f"Сигналы: пн–пт {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену, сб–вс выходной."
    )


async def signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not ACTIVE_ASSETS:
        await update.message.reply_text("Пары ещё не найдены — проверь /status.")
        return
    await update.message.reply_text("⏳ Анализирую пары...")
    report = []
    for a in ACTIVE_ASSETS:
        k = _asset_key(a)
        signal, reason = await analyze_pair(a)
        _remember_reason(k, reason)
        if signal:
            await update.message.reply_text(format_signal(signal))
            await record_signal(signal)
        report.append(f"• {_pair_label(k)}: {reason}")
    await update.message.reply_text("Разбор по парам:\n" + "\n".join(report))


async def auto_analysis(context: ContextTypes.DEFAULT_TYPE):
    global _last_analysis_at
    try:
        await check_pending_outcomes()
        if not is_within_working_hours():
            return
        for a in ACTIVE_ASSETS:
            signal, reason = await analyze_pair(a)
            _remember_reason(_asset_key(a), reason)
            if signal:
                await context.bot.send_message(chat_id=context.job.chat_id, text=format_signal(signal))
                await record_signal(signal)
        _last_analysis_at = time.time()
    except Exception as error:
        print(f"Ошибка автоанализа: {error}")


def _start_job(job_queue, chat_id: int, first: int):
    job_queue.run_repeating(
        auto_analysis,
        interval=AUTO_ANALYSIS_INTERVAL,
        first=first,
        chat_id=chat_id,
        name=_job_name(chat_id),
    )


async def auto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if context.job_queue.get_jobs_by_name(_job_name(chat_id)):
        await update.message.reply_text("✅ Автоанализ уже работает.")
        return
    _start_job(context.job_queue, chat_id, first=10)
    await update.message.reply_text(
        f"✅ Автоанализ запущен: проверка каждую минуту, пн–пт "
        f"{WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену."
    )


async def auto_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for job in context.job_queue.get_jobs_by_name(_job_name(update.effective_chat.id)):
        job.schedule_removal()
    await update.message.reply_text("⛔ Автоанализ остановлен.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Ошибки Telegram — коротко в лог, без простыни traceback и без падения."""
    err = context.error
    if isinstance(err, Conflict):
        print(
            "[telegram] Conflict: этим токеном одновременно пользуется другая копия бота. "
            "Пара таких строк во время передеплоя Railway — норма (старый контейнер ещё не выключился). "
            "Если повторяется постоянно — где-то запущена вторая копия."
        )
        return
    if isinstance(err, (NetworkError, TimedOut)):
        print(f"[telegram] Сетевая ошибка (повторим сами): {err}")
        return
    print(f"[telegram] Ошибка: {err!r}")


MAIN_KEYBOARD = [
    ["📊 Статус", "📈 Стратегия"],
    ["🔍 Проверить сигнал", "🕯 Свечи"],
    ["▶️ Автоанализ", "⏹️ Стоп автоанализ"],
    ["📉 Статистика", "📦 Экспорт свечей"],
]


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = ReplyKeyboardMarkup(MAIN_KEYBOARD, resize_keyboard=True)
    await update.message.reply_text("FELIX SMART BOT запущен.\nВыберите действие:", reply_markup=reply_markup)


async def post_init(application: Application):
    global _notify_bot, _notify_chat_id

    _notify_bot = application.bot
    if TELEGRAM_CHAT_ID:
        _notify_chat_id = int(TELEGRAM_CHAT_ID)

    _background_tasks.append(asyncio.create_task(start_pocket_option_client()))
    _background_tasks.append(asyncio.create_task(_history_retry_loop()))
    _background_tasks.append(asyncio.create_task(_heartbeat_loop()))
    _background_tasks.append(asyncio.create_task(_tick_watchdog_loop()))

    if not TELEGRAM_CHAT_ID:
        return

    chat_id = int(TELEGRAM_CHAT_ID)
    if not application.job_queue.get_jobs_by_name(_job_name(chat_id)):
        _start_job(application.job_queue, chat_id, first=30)

    await application.bot.send_message(
        chat_id=chat_id,
        text=(
            "✅ FELIX SMART BOT запущен, автоанализ включён.\n"
            "Стратегия «Не заходи раньше» (SMC): H1 + M5, реальные валютные пары.\n"
            f"Экспирация {EXPIRATION_MINUTES} мин, статистика через "
            f"{', '.join(str(h) for h in CHECK_HORIZONS)} мин.\n"
            f"Сигналы: пн–пт {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену, сб–вс выходной.\n"
            "Свечи в будни копятся круглосуточно — это нужно для структуры H1.\n\n"
            "🕯 Проверить, копятся ли свечи — кнопка «Свечи» или /candles.\n"
            "В выходные бот молчит и не строит свечи из застывших котировок.\n"
            "Если котировки пропадут больше чем на 10 минут в рабочее время — напишу сюда."
        ),
    )


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Не найдена переменная TELEGRAM_BOT_TOKEN")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", menu))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("strategy", strategy))
    application.add_handler(CommandHandler("signal", signal_cmd))
    application.add_handler(CommandHandler("candles", candles_cmd))
    application.add_handler(CommandHandler("export", export_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("auto_start", auto_start))
    application.add_handler(CommandHandler("auto_stop", auto_stop))

    application.add_handler(MessageHandler(filters.Regex("^📊 Статус$"), status))
    application.add_handler(MessageHandler(filters.Regex("^📈 Стратегия$"), strategy))
    application.add_handler(MessageHandler(filters.Regex("^🔍 Проверить сигнал$"), signal_cmd))
    application.add_handler(MessageHandler(filters.Regex("^🕯 Свечи$"), candles_cmd))
    application.add_handler(MessageHandler(filters.Regex("^📦 Экспорт свечей$"), export_cmd))
    application.add_handler(MessageHandler(filters.Regex("^📉 Статистика$"), stats_cmd))
    application.add_handler(MessageHandler(filters.Regex("^▶️ Автоанализ$"), auto_start))
    application.add_handler(MessageHandler(filters.Regex("^⏹️ Стоп автоанализ$"), auto_stop))

    application.add_error_handler(on_error)

    application.run_polling()


if __name__ == "__main__":
    main()
