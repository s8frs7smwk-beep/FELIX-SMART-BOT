PRESS-РЕЖИМ (добавлено отдельной кнопкой, работает на общем движке анализа)

--- БАЗОВЫЙ РЕЖИМ (как было) ---
Факторы уверенности (сумма нормируется к 100%, максимум 110 баллов сырых):
1. Уровни поддержки/сопротивления (макс 30) — по кластеру локальных экстремумов на M5.
   С поправкой на "истощение": пик силы на 3-4 касаниях, после — уровень чаще пробивают,
   а не держит, поэтому балл после 4 касаний снижается, а не растёт бесконечно.
2. Свечной паттерн у уровня (макс 30) — пин-бар, поглощение, доджи.
3. Активность (тики цены за свечу) относительно среднего (макс 25).
4. Совпадение с трендом M15 по EMA20/50 (макс 15, бонус).
5. Качество подхода к уровню (макс 10, бонус) — направленное движение в 3 свечи перед
   сигнальной свечой считается более убедительным, чем касание после бокового шума.

Сигнал уходит только если уверенность >= CONFIDENCE_THRESHOLD (70%).
Экспирация сигнала — 5 минут. Cooldown не даёт слать повторно тот же
актив+направление слишком часто. Бот работает только в заданное окно
времени по Норвегии (Europe/Oslo).

СТАТИСТИКА: бот запоминает каждый отправленный сигнал и через 5 минут
сверяет цену с ценой на момент сигнала — угадал/не угадал. /stats показывает
точность. Circuit breaker: если из последних 10 проверенных сигналов угадано
меньше 40% — бот присылает предупреждение.

--- EXPRESS-РЕЖИМ (добавлено) ---
Отдельная кнопка "🎯 Express" / команда /express — собирает СЕТ из 3 активов
одновременно (Pocket Option требует минимум 3 актива для express-сделки),
используя тот же движок анализа (уровни+паттерн+активность+тренд+подход), но:
- Свой пул активов (акции OTC: Microsoft, Pfizer, Citigroup и т.д.) —
  ключевые слова в EXPRESS_ASSET_KEYWORDS, бот сам находит точные enum-имена
  в библиотеке при старте (не хардкодится, т.к. точные названия заранее
  неизвестны — см. resolve_express_pool()).
- Фильтр по выплате (payout) в диапазоне PAYOUT_MIN..PAYOUT_MAX. Живой поток
  payout от библиотеки не подключён (неизвестно точное событие) — используется
  РУЧНОЙ снэпшот (EXPRESS_PAYOUT_SNAPSHOT), обновляемый командой /setpayout.
- Своя экспирация (EXPRESS_EXPIRATION_SECONDS, 3 мин) и свой интервал
  автоанализа (EXPRESS_INTERVAL_SECONDS, 10 мин) — оба отдельные от обычного
  режима (5 мин / 1 мин), запускаются и останавливаются отдельными кнопками,
  не мешая обычному режиму.
- Если в моменте набралось МЕНЬШЕ 3 активов, прошедших порог уверенности —
  сет не отправляется (лучше пропустить тик).
- Статистика express — отдельно, /stats показывает и обычную точность, и
  отдельно % сетов, где зашли ВСЕ 3 актива (это и есть реальный express-winrate).
"""

import os
import time
import asyncio
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from pocket_option import PocketOptionClient
from pocket_option.constants import Regions
from pocket_option.contrib.default_init import default_init
from pocket_option.models import Asset, AuthorizationData, UpdateCloseValueItem

# ---------------------------------------------------------------------------
# КОНФИГ — БАЗОВЫЙ РЕЖИМ (без изменений)
# ---------------------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PO_SESSION = os.getenv("PO_SESSION")
PO_UID = os.getenv("PO_UID")
PO_IS_DEMO = 1

ASSETS = [
    Asset.EURUSD_otc,
    Asset.GBPUSD_otc,
    Asset.AUDCAD_otc,
]

EXPIRATION_SECONDS = 300  # 5 минут
TF_M5 = 5 * 60
TF_M15 = 15 * 60

AUTO_ANALYSIS_INTERVAL = 60
CONFIDENCE_THRESHOLD = 70
STRONG_SIGNAL_THRESHOLD = 85
SCORE_MAX = 110  # 30 (уровень) + 30 (паттерн) + 25 (активность) + 15 (тренд) + 10 (подход)

LOOKBACK_CANDLES = 50
LEVEL_TOLERANCE_PCT = 0.0007
VOLUME_AVG_WINDOW = 20
MIN_VOLATILITY_RATIO = 0.4

COOLDOWN_MINUTES = 10

CIRCUIT_BREAKER_WINDOW = 10
CIRCUIT_BREAKER_THRESHOLD_PCT = 40

TIMEZONE = ZoneInfo("Europe/Oslo")
WORK_START_HOUR = 6
WORK_END_HOUR = 20

MAX_CANDLES = 80

# ---------------------------------------------------------------------------
# КОНФИГ — EXPRESS-РЕЖИМ (добавлено)
# ---------------------------------------------------------------------------

# Точные имена enum-полей для акций заранее неизвестны — резолвятся при
# старте по ключевым словам (см. resolve_express_pool()).
EXPRESS_ASSET_KEYWORDS = [
    "MICROSOFT",
    "PFIZER",
    "CITIGROUP",
    "MARATHON",       # Marathon Digital Holdings
    "NETFLIX",
    "INTEL",
    "APPLE",
    "AMAZON",
    "BOEING",
    "COINBASE",
]

# Снэпшот выплат с твоего скриншота Pocket Option (экспресс-сделки).
# Стартовое значение, пока не подключён живой поток payout — обнови
# командой /setpayout, если выплата в приложении поменяется.
EXPRESS_PAYOUT_SNAPSHOT = {
    "MICROSOFT": 92,
    "PFIZER": 92,
    "CITIGROUP": 92,
    "MARATHON": 92,
    "NETFLIX": 90,
    "INTEL": 87,
    "APPLE": 83,
    "AMAZON": 83,
    "BOEING": 82,
    "COINBASE": 80,
}

EXPRESS_ASSET_POOL: list[Asset] = []          # заполняется resolve_express_pool()
_keyword_to_asset_key: dict[str, str] = {}

EXPRESS_SET_SIZE = 3
EXPRESS_EXPIRATION_SECONDS = 180               # 3 минуты
EXPRESS_INTERVAL_SECONDS = 600                 # тик анализа раз в 10 минут

PAYOUT_MIN = 89
PAYOUT_MAX = 92

# ---------------------------------------------------------------------------
# ХРАНИЛИЩЕ СВЕЧЕЙ (общее для обоих режимов — уже универсально по asset_key)
# ---------------------------------------------------------------------------

candle_store: dict[tuple[str, int], deque] = {}
_store_lock = asyncio.Lock()

signal_history: list[dict] = []          # сигналы базового режима
_history_lock = asyncio.Lock()

express_set_history: list[dict] = []     # сеты express-режима
_set_history_lock = asyncio.Lock()

payout_store: dict[str, float] = {}      # ручной снэпшот payout (express)
_payout_lock = asyncio.Lock()

_notify_bot = None
_notify_chat_id: int | None = None
_circuit_breaker_active = False


def _asset_key(asset: Asset) -> str:
    return asset.value if hasattr(asset, "value") else str(asset)


def resolve_express_pool() -> list[str]:
    """Ищет в Asset enum поля под ключевые слова EXPRESS_ASSET_KEYWORDS
    (содержит слово + заканчивается на 'otc', без учёта регистра).
    Заполняет EXPRESS_ASSET_POOL и стартовый payout_store по снэпшоту.
    Возвращает список ключевых слов, для которых ничего не нашлось."""
    global EXPRESS_ASSET_POOL, _keyword_to_asset_key

    all_names = [n for n in dir(Asset) if not n.startswith("_")]
    resolved: list[Asset] = []
    missing: list[str] = []

    for keyword in EXPRESS_ASSET_KEYWORDS:
        candidates = [
            n for n in all_names
            if keyword.upper() in n.upper() and n.upper().endswith("OTC")
        ]
        if not candidates:
            missing.append(keyword)
            continue
        chosen_name = min(candidates, key=len)
        asset_obj = getattr(Asset, chosen_name)
        resolved.append(asset_obj)
        _keyword_to_asset_key[keyword] = _asset_key(asset_obj)
        print(f"[express] Актив найден: '{keyword}' -> Asset.{chosen_name}")

    EXPRESS_ASSET_POOL = resolved

    for keyword, payout in EXPRESS_PAYOUT_SNAPSHOT.items():
        asset_key = _keyword_to_asset_key.get(keyword)
        if asset_key:
            payout_store[asset_key] = float(payout)

    return missing


async def _push_candle(asset_key: str, period: int, price: float, bucket_time: int):
    key = (asset_key, period)
    async with _store_lock:
        if key not in candle_store:
            candle_store[key] = deque(maxlen=MAX_CANDLES)
        dq = candle_store[key]
        if dq and dq[-1]["time"] == bucket_time:
            c = dq[-1]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] = c.get("volume", 1) + 1
        else:
            dq.append({
                "time": bucket_time,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,
            })


async def _seed_candles_from_history(asset_key: str, period: int, raw_candles: list[dict]):
    key = (asset_key, period)
    async with _store_lock:
        dq = deque(maxlen=MAX_CANDLES)
        for c in raw_candles[-MAX_CANDLES:]:
            dq.append({
                "time": c["time"],
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": 1,
            })
        if dq:
            candle_store[key] = dq


async def _get_frame(asset_key: str, period: int) -> pd.DataFrame | None:
    key = (asset_key, period)
    async with _store_lock:
        dq = candle_store.get(key)
        if not dq or len(dq) < 10:
            return None
        return pd.DataFrame(list(dq))


async def _get_last_price(asset_key: str) -> float | None:
    key = (asset_key, TF_M5)
    async with _store_lock:
        dq = candle_store.get(key)
        if not dq:
            return None
        return dq[-1]["close"]


async def get_payout(asset_key: str) -> float | None:
    async with _payout_lock:
        return payout_store.get(asset_key)


async def set_payout(asset_key: str, payout: float):
    async with _payout_lock:
        payout_store[asset_key] = payout


def _format_timedelta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    if minutes:
        return f"{minutes} мин {sec} сек"
    return f"{sec} сек"


def is_within_working_hours() -> bool:
    now_local = datetime.now(TIMEZONE)
    return WORK_START_HOUR <= now_local.hour < WORK_END_HOUR


async def _notify(text: str):
    if _notify_bot is None or _notify_chat_id is None:
        return
    try:
        await _notify_bot.send_message(chat_id=_notify_chat_id, text=text)
    except Exception as error:
        print(f"Не удалось отправить уведомление: {error}")


# ---------------------------------------------------------------------------
# АНАЛИЗ: ОБЩИЙ ДВИЖОК (используется обоими режимами, без изменений)
# ---------------------------------------------------------------------------

def is_market_alive(frame: pd.DataFrame) -> bool:
    recent = frame.tail(LOOKBACK_CANDLES)
    if len(recent) < 10:
        return True
    ranges = recent["high"] - recent["low"]
    avg_range = ranges.iloc[:-1].mean()
    last_range = ranges.iloc[-1]
    if avg_range <= 0:
        return True
    return (last_range / avg_range) >= MIN_VOLATILITY_RATIO


def find_levels(frame: pd.DataFrame) -> list[dict]:
    recent = frame.tail(LOOKBACK_CANDLES).reset_index(drop=True)
    if len(recent) < 10:
        return []

    swing_highs = []
    swing_lows = []
    for i in range(2, len(recent) - 2):
        window = recent.iloc[i - 2:i + 3]
        if recent["high"][i] == window["high"].max():
            swing_highs.append(recent["high"][i])
        if recent["low"][i] == window["low"].min():
            swing_lows.append(recent["low"][i])

    def cluster(points: list[float], kind: str) -> list[dict]:
        levels = []
        for p in points:
            matched = False
            for lvl in levels:
                if abs(p - lvl["price"]) / lvl["price"] <= LEVEL_TOLERANCE_PCT:
                    lvl["touches"] += 1
                    lvl["price"] = (lvl["price"] * (lvl["touches"] - 1) + p) / lvl["touches"]
                    matched = True
                    break
            if not matched:
                levels.append({"price": p, "touches": 1, "type": kind})
        return levels

    return cluster(swing_highs, "high") + cluster(swing_lows, "low")


def level_score(touches: int) -> int:
    if touches <= 1:
        return 0
    if touches == 2:
        return 14
    if touches == 3:
        return 26
    if touches == 4:
        return 30
    if touches == 5:
        return 20
    return 12


def classify_candle_pattern(last: pd.Series, prev: pd.Series) -> tuple[str, int, str]:
    body = abs(last["close"] - last["open"])
    full_range = last["high"] - last["low"]
    if full_range <= 0:
        return "нет паттерна", 0, "NONE"

    upper_wick = last["high"] - max(last["close"], last["open"])
    lower_wick = min(last["close"], last["open"]) - last["low"]

    if lower_wick >= body * 2 and lower_wick > upper_wick and body / full_range < 0.35:
        return "пин-бар (бычий)", 30, "UP"
    if upper_wick >= body * 2 and upper_wick > lower_wick and body / full_range < 0.35:
        return "пин-бар (медвежий)", 30, "DOWN"

    prev_body = abs(prev["close"] - prev["open"])
    if (
        last["close"] > last["open"]
        and prev["close"] < prev["open"]
        and last["close"] >= prev["open"]
        and last["open"] <= prev["close"]
        and body > prev_body
    ):
        return "бычье поглощение", 28, "UP"
    if (
        last["close"] < last["open"]
        and prev["close"] > prev["open"]
        and last["open"] >= prev["close"]
        and last["close"] <= prev["open"]
        and body > prev_body
    ):
        return "медвежье поглощение", 28, "DOWN"

    if body / full_range < 0.1:
        return "доджи", 12, "NONE"

    return "нет паттерна", 0, "NONE"


def volume_score(frame: pd.DataFrame) -> tuple[int, str]:
    if "volume" not in frame.columns or len(frame) < VOLUME_AVG_WINDOW + 1:
        return 0, "активность: недостаточно данных"

    recent = frame.tail(VOLUME_AVG_WINDOW + 1)
    avg_ticks = recent["volume"].iloc[:-1].mean()
    last_ticks = recent["volume"].iloc[-1]

    if avg_ticks <= 0:
        return 0, "активность: недостаточно данных"

    ratio = last_ticks / avg_ticks
    if ratio >= 2.0:
        return 25, f"активность x{ratio:.1f} от средней ({int(last_ticks)} тиков)"
    if ratio >= 1.5:
        return 18, f"активность x{ratio:.1f} от средней ({int(last_ticks)} тиков)"
    if ratio >= 1.2:
        return 10, f"активность x{ratio:.1f} от средней ({int(last_ticks)} тиков)"
    return 0, f"активность в норме (x{ratio:.1f})"


def trend_bonus(frame_m15: pd.DataFrame | None, direction: str) -> tuple[int, str]:
    if frame_m15 is None or len(frame_m15) < 10:
        return 0, "тренд M15: недостаточно данных"

    closes = frame_m15["close"]
    ema20 = closes.ewm(span=20, adjust=False).mean().iloc[-1]
    ema50 = closes.ewm(span=min(50, len(closes)), adjust=False).mean().iloc[-1]
    last_close = closes.iloc[-1]

    if last_close > ema20 > ema50:
        m15_trend = "UP"
    elif last_close < ema20 < ema50:
        m15_trend = "DOWN"
    else:
        m15_trend = "FLAT"

    if m15_trend == "FLAT":
        return 0, "тренд M15: боковик"
    if m15_trend == direction:
        return 15, f"тренд M15: {m15_trend} — совпадает ✅"
    return 0, f"тренд M15: {m15_trend} — против сигнала ⚠️"


def approach_bonus(frame: pd.DataFrame, direction: str) -> tuple[int, str]:
    if len(frame) < 6:
        return 0, "подход: недостаточно данных"

    window = frame.iloc[-5:-2]
    if len(window) < 3:
        return 0, "подход: недостаточно данных"

    if direction == "UP":
        directional = (window["close"] < window["open"]).sum()
    else:
        directional = (window["close"] > window["open"]).sum()

    if directional >= 3:
        return 10, "подход: чёткое направленное движение ✅"
    if directional == 2:
        return 5, "подход: частично направленное"
    return 0, "подход: случайный заход"


async def is_on_cooldown(asset_key: str, direction: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES)
    async with _history_lock:
        for entry in reversed(signal_history):
            if entry["time"] < cutoff:
                break
            if entry["asset"] == asset_key and entry["direction"] == direction:
                return True
    return False


async def analyze_market(asset: Asset) -> dict | None:
    asset_key = _asset_key(asset)
    frame = await _get_frame(asset_key, TF_M5)
    if frame is None or len(frame) < LOOKBACK_CANDLES // 2:
        return None

    if not is_market_alive(frame):
        return None

    levels = find_levels(frame)
    if not levels:
        return None

    last = frame.iloc[-2]
    prev = frame.iloc[-3]

    pattern_name, pattern_score, pattern_direction = classify_candle_pattern(last, prev)
    if pattern_direction == "NONE" or pattern_score == 0:
        return None

    price = last["close"]
    nearby_levels = [
        lvl for lvl in levels
        if abs(lvl["price"] - price) / price <= LEVEL_TOLERANCE_PCT * 3
    ]
    if not nearby_levels:
        return None
    strongest = max(nearby_levels, key=lambda lvl: lvl["touches"])

    lvl_score = level_score(strongest["touches"])
    if lvl_score == 0:
        return None

    vol_score, vol_label = volume_score(frame)

    frame_m15 = await _get_frame(asset_key, TF_M15)
    tr_score, tr_label = trend_bonus(frame_m15, pattern_direction)

    ap_score, ap_label = approach_bonus(frame, pattern_direction)

    raw_total = lvl_score + pattern_score + vol_score + tr_score + ap_score
    confidence_pct = round(raw_total / SCORE_MAX * 100)

    if confidence_pct < CONFIDENCE_THRESHOLD:
        return None

    if await is_on_cooldown(asset_key, pattern_direction):
        return None

    return {
        "asset": asset_key,
        "direction": pattern_direction,
        "confidence": confidence_pct,
        "pattern": pattern_name,
        "level_touches": strongest["touches"],
        "level_price": strongest["price"],
        "volume_label": vol_label,
        "trend_label": tr_label,
        "approach_label": ap_label,
        "price": price,
    }


async def analyze_many(assets: list[Asset]) -> list[dict]:
    results = []
    for a in assets:
        signal = await analyze_market(a)
        if signal:
            results.append(signal)
    return results


def format_signal(signal: dict) -> str:
    direction_ru = "ВВЕРХ (CALL) 🟢" if signal["direction"] == "UP" else "ВНИЗ (PUT) 🔴"
    label = signal["asset"].replace("_otc", " OTC")
    prefix = "🔥 СИЛЬНЫЙ СИГНАЛ" if signal["confidence"] >= STRONG_SIGNAL_THRESHOLD else "🔔 СИГНАЛ"
    return (
        f"{prefix} — {label}\n"
        f"Направление: {direction_ru}\n"
        f"Уверенность: {signal['confidence']}%\n"
        f"Экспирация: {EXPIRATION_SECONDS // 60} мин\n\n"
        f"Паттерн: {signal['pattern']}\n"
        f"Уровень: {signal['level_price']:.5f} (касаний: {signal['level_touches']})\n"
        f"{signal['volume_label']}\n"
        f"{signal['trend_label']}\n"
        f"{signal['approach_label']}"
    )


# ---------------------------------------------------------------------------
# EXPRESS-ЛОГИКА (добавлено)
# ---------------------------------------------------------------------------

async def analyze_express_pool() -> list[dict]:
    candidates = []
    for asset in EXPRESS_ASSET_POOL:
        asset_key = _asset_key(asset)

        payout = await get_payout(asset_key)
        if payout is None:
            continue
        if not (PAYOUT_MIN <= payout <= PAYOUT_MAX):
            continue

        signal = await analyze_market(asset)
        if signal:
            signal["payout"] = payout
            candidates.append(signal)

    if len(candidates) < EXPRESS_SET_SIZE:
        return []

    candidates.sort(key=lambda s: s["confidence"], reverse=True)
    return candidates[:EXPRESS_SET_SIZE]


def format_express_set(signal_set: list[dict], set_id: int) -> str:
    lines = [
        f"🎯 EXPRESS-СЕТ #{set_id}",
        f"Экспирация: {EXPRESS_EXPIRATION_SECONDS // 60} мин | активов: {len(signal_set)}",
        "",
    ]
    for s in signal_set:
        direction_ru = "ВВЕРХ (CALL) 🟢" if s["direction"] == "UP" else "ВНИЗ (PUT) 🔴"
        label = s["asset"].replace("_otc", " OTC")
        lines.append(
            f"• {label} — {direction_ru}\n"
            f"  Уверенность: {s['confidence']}% | Выплата: {s['payout']:.0f}%\n"
            f"  Паттерн: {s['pattern']} | {s['trend_label']}"
        )
    avg_conf = round(sum(s["confidence"] for s in signal_set) / len(signal_set))
    lines.append(f"\nСредняя уверенность по сету: {avg_conf}%")
    return "\n".join(lines)


async def record_express_set(signal_set: list[dict]) -> int:
    async with _set_history_lock:
        set_id = len(express_set_history) + 1
        express_set_history.append({
            "id": set_id,
            "time": datetime.now(timezone.utc),
            "assets": [
                {"asset": s["asset"], "direction": s["direction"], "price_at_signal": s["price"]}
                for s in signal_set
            ],
            "checked": False,
            "all_correct": None,
        })
    return set_id


async def check_express_outcomes():
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=EXPRESS_EXPIRATION_SECONDS)
    async with _set_history_lock:
        pending_sets = [s for s in express_set_history if not s["checked"] and s["time"] <= cutoff]

    for signal_set in pending_sets:
        results = []
        incomplete = False
        for item in signal_set["assets"]:
            current_price = await _get_last_price(item["asset"])
            if current_price is None:
                incomplete = True
                break
            went_up = current_price > item["price_at_signal"]
            correct = went_up if item["direction"] == "UP" else not went_up
            results.append(correct)
        if incomplete:
            continue
        async with _set_history_lock:
            signal_set["checked"] = True
            signal_set["all_correct"] = all(results)
        if signal_set["all_correct"]:
            await _notify(f"✅ Express-сет #{signal_set['id']} зашёл полностью (все {len(results)} актива).")
        else:
            hit = sum(results)
            await _notify(f"❌ Express-сет #{signal_set['id']} не зашёл ({hit}/{len(results)} угадано).")


def express_stats_summary() -> str:
    checked_sets = [s for s in express_set_history if s["checked"]]
    if not checked_sets:
        return "Пока нет завершённых express-сетов."
    total_sets = len(checked_sets)
    full_hits = sum(1 for s in checked_sets if s["all_correct"])
    set_winrate = round(full_hits / total_sets * 100)
    return f"Express-сеты (все 3 зашли): {full_hits}/{total_sets} ({set_winrate}%)"


# ---------------------------------------------------------------------------
# СТАТИСТИКА + CIRCUIT BREAKER — БАЗОВЫЙ РЕЖИМ (без изменений)
# ---------------------------------------------------------------------------

async def record_signal(asset_key: str, direction: str, price: float, confidence: int):
    async with _history_lock:
        signal_history.append({
            "time": datetime.now(timezone.utc),
            "asset": asset_key,
            "direction": direction,
            "price_at_signal": price,
            "confidence": confidence,
            "checked": False,
            "correct": None,
        })


async def check_pending_outcomes():
    """Сверяет исходы базового режима (5 мин) и express-сетов (3 мин, своя
    экспирация — см. check_express_outcomes)."""
    global _circuit_breaker_active

    await check_express_outcomes()

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=EXPIRATION_SECONDS)
    async with _history_lock:
        pending = [e for e in signal_history if not e["checked"] and e["time"] <= cutoff]

    for entry in pending:
        current_price = await _get_last_price(entry["asset"])
        if current_price is None:
            continue
        went_up = current_price > entry["price_at_signal"]
        correct = went_up if entry["direction"] == "UP" else not went_up
        async with _history_lock:
            entry["checked"] = True
            entry["correct"] = correct

    if not pending:
        return

    async with _history_lock:
        checked = [e for e in signal_history if e["checked"]]
    last_n = checked[-CIRCUIT_BREAKER_WINDOW:]
    if len(last_n) < CIRCUIT_BREAKER_WINDOW:
        return

    winrate = sum(1 for e in last_n if e["correct"]) / len(last_n) * 100

    if winrate < CIRCUIT_BREAKER_THRESHOLD_PCT and not _circuit_breaker_active:
        _circuit_breaker_active = True
        await _notify(
            f"⚠️ Последние {CIRCUIT_BREAKER_WINDOW} сигналов угаданы только на "
            f"{winrate:.0f}% — возможно, стоит пересмотреть пороги или переждать рынок."
        )
    elif winrate >= CIRCUIT_BREAKER_THRESHOLD_PCT and _circuit_breaker_active:
        _circuit_breaker_active = False
        await _notify(f"✅ Точность сигналов восстановилась ({winrate:.0f}% из последних {CIRCUIT_BREAKER_WINDOW}).")


def stats_summary() -> str:
    checked = [e for e in signal_history if e["checked"]]
    if not checked:
        base_line = "Базовый режим: пока нет завершённых сигналов."
    else:
        total = len(checked)
        correct = sum(1 for e in checked if e["correct"])
        winrate = round(correct / total * 100)
        base_line = f"Базовый режим: {correct}/{total} ({winrate}%)"

    return base_line + "\n" + express_stats_summary()


# ---------------------------------------------------------------------------
# POCKET OPTION CLIENT
# ---------------------------------------------------------------------------

po_client: PocketOptionClient | None = None
po_connected = False
_was_ever_connected = False


async def _try_preload_history(assets_to_preload: list[Asset]):
    for asset in assets_to_preload:
        asset_key = _asset_key(asset)
        for period in (TF_M5, TF_M15):
            try:
                now = int(time.time())
                result = await po_client.emit.load_history_period(
                    asset=asset,
                    period=period,
                    time=now,
                    offset=0,
                )
                raw = getattr(result, "candles", None) or getattr(result, "data", None) or result
                parsed = []
                for c in raw:
                    parsed.append({
                        "time": int(getattr(c, "time", None) or c["time"]),
                        "open": float(getattr(c, "open", None) or c["open"]),
                        "high": float(getattr(c, "high", None) or c["high"]),
                        "low": float(getattr(c, "low", None) or c["low"]),
                        "close": float(getattr(c, "close", None) or c["close"]),
                    })
                if parsed:
                    await _seed_candles_from_history(asset_key, period, parsed)
                    print(f"История подгружена: {asset_key} период {period}с, свечей: {len(parsed)}")
            except Exception as error:
                print(f"Подгрузка истории не удалась для {asset_key}/{period}: {error} — копим вживую.")


async def start_pocket_option_client():
    global po_client, po_connected, _was_ever_connected

    if not PO_SESSION or not PO_UID:
        print("PO_SESSION / PO_UID не заданы — клиент Pocket Option не запущен.")
        return

    missing = resolve_express_pool()
    if missing:
        await _notify(
            "⚠️ Не удалось найти в Pocket Option следующие express-активы по "
            f"ключевым словам: {', '.join(missing)}. Они не будут участвовать "
            "в отборе для express. Обычный режим (EURUSD/GBPUSD/AUDCAD) это не касается."
        )

    all_subscribed_assets = ASSETS + EXPRESS_ASSET_POOL

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
        sub_assets=all_subscribed_assets,
        sub_period=TF_M5,
    )

    @po_client.on.update_close_value
    async def _on_update_close_value(items: list[UpdateCloseValueItem]):
        now = datetime.now(timezone.utc)
        m5_bucket = int(now.timestamp() // TF_M5 * TF_M5)
        m15_bucket = int(now.timestamp() // TF_M15 * TF_M15)
        for item in items:
            asset_key = getattr(item, "asset", None) or getattr(item, "symbol", None)
            price = getattr(item, "value", None) or getattr(item, "price", None)
            if asset_key is None or price is None:
                continue
            await _push_candle(str(asset_key), TF_M5, float(price), m5_bucket)
            await _push_candle(str(asset_key), TF_M15, float(price), m15_bucket)

    @po_client.on.connect
    async def _on_connect():
        global po_connected, _was_ever_connected
        was_reconnect = _was_ever_connected and not po_connected
        po_connected = True
        _was_ever_connected = True
        print("Pocket Option: соединение установлено (демо).")
        if was_reconnect:
            await _notify("✅ Соединение с Pocket Option восстановлено.")
        asyncio.create_task(_try_preload_history(all_subscribed_assets))

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
# TELEGRAM ХЕНДЛЕРЫ — БАЗОВЫЙ РЕЖИМ (без изменений)
# ---------------------------------------------------------------------------

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job_name = f"auto_analysis_{chat_id}"
    express_job_name = f"express_analysis_{chat_id}"
    jobs = context.job_queue.get_jobs_by_name(job_name)
    express_jobs = context.job_queue.get_jobs_by_name(express_job_name)

    po_line = "🟢 Pocket Option: подключено (демо)" if po_connected else "🔴 Pocket Option: нет соединения"
    hours_line = f"🕐 Рабочее окно: {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 (Берген)"
    now_status = "🟢 сейчас в рабочем окне" if is_within_working_hours() else "🔴 сейчас вне рабочего окна (бот молчит)"

    if jobs:
        auto_line = "🟢 Автоанализ (базовый): ВКЛЮЧЁН"
    else:
        auto_line = "🔴 Автоанализ (базовый): ВЫКЛЮЧЕН"

    if express_jobs:
        express_line = "🟢 Автоанализ (express): ВКЛЮЧЁН"
    else:
        express_line = "🔴 Автоанализ (express): ВЫКЛЮЧЕН"

    assets_label = ", ".join(_asset_key(a).replace("_otc", " OTC") for a in ASSETS)
    express_pool_label = ", ".join(_asset_key(a).replace("_otc", " OTC") for a in EXPRESS_ASSET_POOL)
    breaker_line = "\n⚠️ Circuit breaker активен (низкая точность)" if _circuit_breaker_active else ""

    await update.message.reply_text(
        "✅ POCKET TA BOT работает\n\n"
        f"{po_line}\n"
        f"{auto_line}\n"
        f"{express_line}\n"
        f"{hours_line}\n"
        f"{now_status}"
        f"{breaker_line}\n\n"
        f"Базовые активы: {assets_label}\n"
        f"Порог уверенности: {CONFIDENCE_THRESHOLD}% (🔥 от {STRONG_SIGNAL_THRESHOLD}%)\n"
        f"Экспирация (базовый): {EXPIRATION_SECONDS // 60} мин | Cooldown: {COOLDOWN_MINUTES} мин\n\n"
        f"Express-пул: {express_pool_label or '— ещё не резолвлен'}\n"
        f"Фильтр выплаты (express): {PAYOUT_MIN}–{PAYOUT_MAX}% (⚠️ ручной снэпшот, /setpayout)\n"
        f"Экспирация (express): {EXPRESS_EXPIRATION_SECONDS // 60} мин | интервал: {EXPRESS_INTERVAL_SECONDS // 60} мин\n\n"
        f"Chat ID: {chat_id}"
    )


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await check_pending_outcomes()
    await update.message.reply_text("📊 Статистика\n\n" + stats_summary())


async def strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Стратегия POCKET TA (классический технический анализ):\n\n"
        "1. Уровни поддержки/сопротивления — пик силы на 3-4 касаниях (макс 30)\n"
        "2. Свечной паттерн у уровня — пин-бар/поглощение/доджи (макс 30)\n"
        "3. Активность (тики цены за свечу) относительно среднего (макс 25)\n"
        "4. Совпадение с трендом M15 по EMA20/50 — бонус (макс 15)\n"
        "5. Качество подхода к уровню — направленное движение перед сигналом (макс 10)\n\n"
        f"Сигнал шлём только если уверенность >= {CONFIDENCE_THRESHOLD}%.\n"
        f"🔥 отдельная пометка для сигналов от {STRONG_SIGNAL_THRESHOLD}%.\n\n"
        "Базовый режим: экспирация 5 мин, активы EURUSD/GBPUSD/AUDCAD.\n"
        "Express-режим (кнопка 🎯 Express): сет из 3 акционных OTC-активов "
        f"с выплатой {PAYOUT_MIN}-{PAYOUT_MAX}%, экспирация {EXPRESS_EXPIRATION_SECONDS // 60} мин.\n\n"
        f"Рабочее окно: {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену.\n"
        "Статистика по факту точности — команда /stats."
    )


async def signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_within_working_hours():
        await update.message.reply_text(
            f"🔴 Сейчас вне рабочего окна ({WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену)."
        )
        return
    await update.message.reply_text("⏳ Анализирую активы...")
    signals = await analyze_many(ASSETS)
    if not signals:
        await update.message.reply_text(
            f"Сейчас нет сигналов с уверенностью >= {CONFIDENCE_THRESHOLD}%."
        )
        return
    for s in signals:
        await update.message.reply_text(format_signal(s))
        await record_signal(s["asset"], s["direction"], s["price"], s["confidence"])


async def auto_analysis(context: ContextTypes.DEFAULT_TYPE):
    try:
        await check_pending_outcomes()

        if not is_within_working_hours():
            return

        signals = await analyze_many(ASSETS)
        for s in signals:
            await context.bot.send_message(
                chat_id=context.job.chat_id,
                text=format_signal(s),
                disable_notification=False,
            )
            await record_signal(s["asset"], s["direction"], s["price"], s["confidence"])
    except Exception as error:
        print(f"Ошибка автоанализа: {error}")


async def auto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job_name = f"auto_analysis_{chat_id}"

    if context.job_queue.get_jobs_by_name(job_name):
        await update.message.reply_text("✅ Автоанализ (базовый) уже работает.")
        return

    context.job_queue.run_repeating(
        auto_analysis,
        interval=AUTO_ANALYSIS_INTERVAL,
        first=10,
        chat_id=chat_id,
        name=job_name,
    )
    await update.message.reply_text(
        f"✅ Автоанализ (базовый) запущен. Проверка каждую минуту, только в окно "
        f"{WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену."
    )


async def auto_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job_name = f"auto_analysis_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    await update.message.reply_text("⛔ Автоанализ (базовый) остановлен.")


# ---------------------------------------------------------------------------
# TELEGRAM ХЕНДЛЕРЫ — EXPRESS-РЕЖИМ (добавлено)
# ---------------------------------------------------------------------------

async def express_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_within_working_hours():
        await update.message.reply_text(
            f"🔴 Сейчас вне рабочего окна ({WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену)."
        )
        return
    if not EXPRESS_ASSET_POOL:
        await update.message.reply_text("Express-пул ещё не готов (нет соединения с Pocket Option?). Проверь /status.")
        return
    await update.message.reply_text("⏳ Собираю express-сет...")
    signal_set = await analyze_express_pool()
    if not signal_set:
        await update.message.reply_text(
            f"Сейчас не набралось {EXPRESS_SET_SIZE} активов с выплатой {PAYOUT_MIN}-{PAYOUT_MAX}% "
            f"и уверенностью >= {CONFIDENCE_THRESHOLD}%."
        )
        return
    set_id = await record_express_set(signal_set)
    await update.message.reply_text(format_express_set(signal_set, set_id))


async def express_auto_analysis(context: ContextTypes.DEFAULT_TYPE):
    try:
        await check_pending_outcomes()

        if not is_within_working_hours():
            return
        if not EXPRESS_ASSET_POOL:
            return

        signal_set = await analyze_express_pool()
        if not signal_set:
            return

        set_id = await record_express_set(signal_set)
        await context.bot.send_message(
            chat_id=context.job.chat_id,
            text=format_express_set(signal_set, set_id),
            disable_notification=False,
        )
    except Exception as error:
        print(f"Ошибка автоанализа express: {error}")


async def express_auto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job_name = f"express_analysis_{chat_id}"

    if context.job_queue.get_jobs_by_name(job_name):
        await update.message.reply_text("✅ Автоанализ express уже работает.")
        return

    context.job_queue.run_repeating(
        express_auto_analysis,
        interval=EXPRESS_INTERVAL_SECONDS,
        first=10,
        chat_id=chat_id,
        name=job_name,
    )
    await update.message.reply_text(
        f"✅ Автоанализ express запущен. Проверка каждые {EXPRESS_INTERVAL_SECONDS // 60} мин, "
        f"только в окно {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену."
    )


async def express_auto_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    job_name = f"express_analysis_{chat_id}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()
    await update.message.reply_text("⛔ Автоанализ express остановлен.")


async def setpayout_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Использование: /setpayout MICROSOFT 92"""
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            "Использование: /setpayout <ключевое_слово> <значение>\n"
            "Например: /setpayout MICROSOFT 90\n\n"
            f"Доступные ключевые слова: {', '.join(EXPRESS_ASSET_KEYWORDS)}"
        )
        return

    keyword, value_str = args[0].upper(), args[1]
    try:
        value = float(value_str)
    except ValueError:
        await update.message.reply_text("Значение выплаты должно быть числом, например 92")
        return

    asset_key = _keyword_to_asset_key.get(keyword)
    if not asset_key:
        await update.message.reply_text(
            f"Актив '{keyword}' не найден среди резолвленных. Проверь /status."
        )
        return

    await set_payout(asset_key, value)
    await update.message.reply_text(f"✅ Выплата для {keyword} обновлена: {value:.0f}%")


# ---------------------------------------------------------------------------
# ОБЩЕЕ МЕНЮ И ЗАПУСК
# ---------------------------------------------------------------------------

MAIN_KEYBOARD = [
    ["📊 Статус", "📈 Стратегия"],
    ["🔍 Проверить сигнал", "🎯 Express"],
    ["▶️ Автоанализ", "⏹️ Стоп автоанализ"],
    ["▶️ Express авто", "⏹️ Стоп Express авто"],
    ["📉 Статистика"],
]


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = ReplyKeyboardMarkup(MAIN_KEYBOARD, resize_keyboard=True)
    await update.message.reply_text("POCKET TA BOT запущен.\nВыберите действие:", reply_markup=reply_markup)


async def post_init(application: Application):
    global _notify_bot, _notify_chat_id

    _notify_bot = application.bot
    if TELEGRAM_CHAT_ID:
        _notify_chat_id = int(TELEGRAM_CHAT_ID)

    asyncio.create_task(start_pocket_option_client())

    if not TELEGRAM_CHAT_ID:
        return

    chat_id = int(TELEGRAM_CHAT_ID)
    job_name = f"auto_analysis_{chat_id}"

    if not application.job_queue.get_jobs_by_name(job_name):
        application.job_queue.run_repeating(
            auto_analysis,
            interval=AUTO_ANALYSIS_INTERVAL,
            first=30,
            chat_id=chat_id,
            name=job_name,
        )

    await application.bot.send_message(
        chat_id=chat_id,
        text=(
            "✅ POCKET TA BOT запущен, автоанализ (базовый) включён автоматически.\n"
            f"Порог уверенности: {CONFIDENCE_THRESHOLD}% (🔥 от {STRONG_SIGNAL_THRESHOLD}%).\n"
            f"Экспирация {EXPIRATION_SECONDS // 60} мин, cooldown {COOLDOWN_MINUTES} мин.\n"
            f"Рабочее окно: {WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00 по Бергену.\n\n"
            "🎯 Добавлен Express-режим — кнопка внизу или команда /express.\n"
            "Автоанализ express НЕ включается сам — запусти кнопкой '▶️ Express авто', когда будешь готов.\n"
            "⚠️ Выплата (payout) для express — ручной снэпшот, обновляй /setpayout при изменениях.\n\n"
            "Проверить статус — /status, статистику — /stats."
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
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("auto_start", auto_start))
    application.add_handler(CommandHandler("auto_stop", auto_stop))
    application.add_handler(CommandHandler("express", express_cmd))
    application.add_handler(CommandHandler("express_auto_start", express_auto_start))
    application.add_handler(CommandHandler("express_auto_stop", express_auto_stop))
    application.add_handler(CommandHandler("setpayout", setpayout_cmd))

    application.add_handler(MessageHandler(filters.Regex("^📊 Статус$"), status))
    application.add_handler(MessageHandler(filters.Regex("^📈 Стратегия$"), strategy))
    application.add_handler(MessageHandler(filters.Regex("^🔍 Проверить сигнал$"), signal_cmd))
    application.add_handler(MessageHandler(filters.Regex("^📉 Статистика$"), stats_cmd))
    application.add_handler(MessageHandler(filters.Regex("^▶️ Автоанализ$"), auto_start))
    application.add_handler(MessageHandler(filters.Regex("^⏹️ Стоп автоанализ$"), auto_stop))
    application.add_handler(MessageHandler(filters.Regex("^🎯 Express$"), express_cmd))
    application.add_handler(MessageHandler(filters.Regex("^▶️ Express авто$"), express_auto_start))
    application.add_handler(MessageHandler(filters.Regex("^⏹️ Стоп Express авто$"), express_auto_stop))

    application.run_polling()


if __name__ == "__main__":
    main()
