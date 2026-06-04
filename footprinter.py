"""
dom_viewer.py  —  DOM Cluster Viewer
=====================================
Рендер DOM, кластерный чарт (footprint), txt-логгер.

Запуск:
  python dom_viewer.py BTCUSDT
  python dom_viewer.py BTCUSDT 20 30
"""
import requests
import asyncio
import aiohttp
import json
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime

BASE = "https://fapi.binance.com"


def get_usdt_futures_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return sorted([
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
        and s["contractType"] == "PERPETUAL"
    ])


# ── Фильтр: объём >= 500K USDT за 5 минут И диапазон >= 2.5% ─────────────────
FILTER_MIN_VOL_USDT  = 500_000   # минимальный объём за 5m
FILTER_MIN_RANGE_PCT = 2.5       # минимальный диапазон за 5m
FILTER_MAX_SYMBOLS   = 30        # максимум токенов


async def get_active_symbols() -> list[str]:
    """Возвращает топ-30 токенов: объём >=500K и диапазон >=2.5% за 5 минут."""
    all_syms = get_usdt_futures_symbols()
    print(f"Фильтруем {len(all_syms)} токенов (объём≥${FILTER_MIN_VOL_USDT/1e3:.0f}K, диапазон≥{FILTER_MIN_RANGE_PCT}%, 5m)...", flush=True)

    async def check(session, sym):
        try:
            async with session.get(
                f"{BASE}/fapi/v1/klines",
                params={"symbol": sym, "interval": "5m", "limit": 5},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                klines = await r.json()
            high  = max(float(k[2]) for k in klines)
            low   = min(float(k[3]) for k in klines)
            close = float(klines[-1][4])
            vol   = sum(float(k[7]) for k in klines)
            rng   = (high - low) / close * 100 if close > 0 else 0
            return sym, rng, vol
        except Exception:
            return sym, 0.0, 0.0

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50)) as session:
        results = await asyncio.gather(*[check(session, s) for s in all_syms])

    import math
    filtered = [
        (sym, rng, vol, rng * math.log1p(vol))
        for sym, rng, vol in results
        if rng >= FILTER_MIN_RANGE_PCT and vol >= FILTER_MIN_VOL_USDT
    ]
    filtered.sort(key=lambda x: x[3], reverse=True)
    top = filtered[:FILTER_MAX_SYMBOLS]

    print(f"Подходит {len(filtered)} токенов → берём топ {len(top)}:")
    for sym, rng, vol, _ in top:
        vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
        print(f"  {sym:<20} {rng:.2f}%  {vol_str}")
    print()

    return [sym for sym, *_ in top]



class C:
    RESET     = "\033[0m"
    RED       = "\033[91m"
    GREEN     = "\033[92m"
    YELLOW    = "\033[93m"
    BLUE      = "\033[94m"
    CYAN      = "\033[96m"
    WHITE     = "\033[97m"
    BOLD      = "\033[1m"
    DIM       = "\033[2m"
    BG_RED    = "\033[41m"
    BG_BLUE   = "\033[44m"
    BG_YELLOW = "\033[43m"
    BG_GREEN  = "\033[42m"

def red(s):    return f"{C.RED}{s}{C.RESET}"
def green(s):  return f"{C.GREEN}{s}{C.RESET}"
def yellow(s): return f"{C.YELLOW}{s}{C.RESET}"
def blue(s):   return f"{C.BLUE}{s}{C.RESET}"
def cyan(s):   return f"{C.CYAN}{s}{C.RESET}"
def bold(s):   return f"{C.BOLD}{s}{C.RESET}"
def dim(s):    return f"{C.DIM}{s}{C.RESET}"
def bg_red(s):    return f"{C.BG_RED}{C.WHITE}{s}{C.RESET}"
def bg_blue(s):   return f"{C.BG_BLUE}{C.WHITE}{s}{C.RESET}"
def bg_yellow(s): return f"{C.BG_YELLOW}{s}{C.RESET}"
def bg_green(s):  return f"{C.BG_GREEN}{C.WHITE}{s}{C.RESET}"


# ─────────────────────────────────────────────────────────────────────────────
# CLUSTER BAR  (один временной столбик footprint-чарта)
# ─────────────────────────────────────────────────────────────────────────────

class ClusterBar:
    """
    Один «столбик» кластерного чарта.
    За интервал `interval` секунд накапливает объёмы at_bid / at_ask
    на каждом ценовом уровне — ровно то, что видно на скрине (bid x ask).
    """

    def __init__(self, start_ts: float, interval: float = 30.0):
        self.start_ts   = start_ts
        self.interval   = interval
        self.at_bid: dict[float, float] = defaultdict(float)
        self.at_ask: dict[float, float] = defaultdict(float)
        self.open_price  = 0.0
        self.close_price = 0.0
        self.high_price  = 0.0
        self.low_price   = 0.0
        self._trade_count = 0

    # ── добавить сделку ──────────────────────────────────────────────────────
    def add_trade(self, price: float, qty: float, is_sell: bool):
        """
        is_sell=True  → maker-bid (продавец ударил в бид)   → at_bid
        is_sell=False → maker-ask (покупатель ударил в аск)  → at_ask
        Binance aggTrade: m=True → покупатель был маркет-мейкером (бид hit),
        значит это продажа.
        """
        if is_sell:
            self.at_bid[price] += qty
        else:
            self.at_ask[price] += qty

        if self.open_price == 0:
            self.open_price = price
        self.close_price = price
        self.high_price  = max(self.high_price, price) if self.high_price else price
        self.low_price   = min(self.low_price,  price) if self.low_price  else price
        self._trade_count += 1

    # ── вспомогательные ──────────────────────────────────────────────────────
    def is_expired(self, now: float) -> bool:
        return now >= self.start_ts + self.interval

    def delta(self) -> float:
        return sum(self.at_ask.values()) - sum(self.at_bid.values())

    def total_volume(self) -> float:
        return sum(self.at_bid.values()) + sum(self.at_ask.values())

    def all_prices(self) -> list[float]:
        return sorted(set(self.at_bid) | set(self.at_ask), reverse=True)

    def label(self) -> str:
        """Краткая метка времени столбика для отображения."""
        return datetime.fromtimestamp(self.start_ts).strftime("%H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# SEQUENTIAL EXECUTION TRACKER
# ─────────────────────────────────────────────────────────────────────────────

class SequentialTracker:
    """
    Отслеживает КАК именно исполняются ордера между обновлениями стакана.

    Ключевые метрики:
      ofi          — Order Flow Imbalance (изменение bid/ask объёмов)
      ask_consumed — объём ask уровней что исчезли (покупатели съели)
      bid_consumed — объём bid уровней что исчезли (продавцы съели)
      flow_dir     — накопленное направление потока за последние N обновлений
    """

    def __init__(self, window: int = 30):
        self.prev_bids: dict[float, float] = {}
        self.prev_asks: dict[float, float] = {}

        # История за последние N обновлений
        self.ofi_hist         = deque(maxlen=window)
        self.ask_consumed_hist = deque(maxlen=window)
        self.bid_consumed_hist = deque(maxlen=window)
        self.imbalance_hist   = deque(maxlen=window)

    def update(self, bids: dict, asks: dict) -> dict:
        """
        Вызывается после каждого обновления стакана.
        Возвращает словарь с метриками текущего обновления.
        """
        # ── 1. OFI (Order Flow Imbalance) ────────────────────────────────────
        # Взвешенное изменение объёма на ближних уровнях
        # Положительное = бид растёт / аск падает = покупательское давление
        ofi = 0.0
        for i, price in enumerate(sorted(bids.keys(), reverse=True)[:10]):
            w = 1.0 / (i + 1)
            delta = bids[price] - self.prev_bids.get(price, 0.0)
            ofi += w * delta

        for i, price in enumerate(sorted(asks.keys())[:10]):
            w = 1.0 / (i + 1)
            delta = asks[price] - self.prev_asks.get(price, 0.0)
            ofi -= w * delta   # рост аска = продавцы добавляют = минус

        # ── 2. Consumed levels (исчезнувшие уровни) ─────────────────────────
        # Ask уровень исчез = его КУПИЛИ (покупательское давление, давит ВВЕРХ)
        ask_consumed = 0.0
        for price, vol in self.prev_asks.items():
            if price not in asks:                   # уровень исчез
                ask_consumed += vol
            elif asks[price] < vol * 0.5:           # уровень уменьшился > 50%
                ask_consumed += (vol - asks[price])

        # Bid уровень исчез = его ПРОДАЛИ (продавческое давление, давит ВНИЗ)
        bid_consumed = 0.0
        for price, vol in self.prev_bids.items():
            if price not in bids:
                bid_consumed += vol
            elif bids[price] < vol * 0.5:
                bid_consumed += (vol - bids[price])

        # ── 3. Imbalance (мгновенный) ────────────────────────────────────────
        bid_total = sum(list(bids.values())[:10])
        ask_total = sum(list(asks.values())[:10])
        imb = bid_total / max(bid_total + ask_total, 1e-9)

        # ── Сохраняем историю ────────────────────────────────────────────────
        self.ofi_hist.append(ofi)
        self.ask_consumed_hist.append(ask_consumed)
        self.bid_consumed_hist.append(bid_consumed)
        self.imbalance_hist.append(imb)

        self.prev_bids = dict(bids)
        self.prev_asks = dict(asks)

        # ── Агрегированные значения за окно ──────────────────────────────────
        cum_ofi         = sum(self.ofi_hist)
        total_ask_cons  = sum(self.ask_consumed_hist)
        total_bid_cons  = sum(self.bid_consumed_hist)
        avg_imbalance   = sum(self.imbalance_hist) / max(len(self.imbalance_hist), 1)

        # Flow direction score: +1 за каждое обновление где аск>бид съедание
        flow_score = sum(
            1 if a > b else (-1 if b > a else 0)
            for a, b in zip(self.ask_consumed_hist, self.bid_consumed_hist)
        )

        return {
            "ofi":            ofi,
            "cum_ofi":        cum_ofi,
            "ask_consumed":   ask_consumed,
            "bid_consumed":   bid_consumed,
            "total_ask_cons": total_ask_cons,
            "total_bid_cons": total_bid_cons,
            "avg_imbalance":  avg_imbalance,
            "flow_score":     flow_score,       # -window..+window
        }


# ─────────────────────────────────────────────────────────────────────────────
# DOM ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class DOMEngine:
    def __init__(self, symbol: str, levels: int = 20):
        self.symbol  = symbol.upper()
        self.levels  = levels

        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}

        self.at_bid: dict[float, float] = defaultdict(float)
        self.at_ask: dict[float, float] = defaultdict(float)
        self.volume: dict[float, float] = defaultdict(float)

        self.current_price = 0.0
        self.tick_size     = 0.01
        self.last_trade_id = 0
        self.running       = True
        self.update_count  = 0

        # Последовательный трекер
        self.seq = SequentialTracker(window=30)
        self._last_seq_result: dict = {}

        # ── Кластерный чарт (footprint) ─────────────────────────────────────
        self.cluster_interval  = 30.0                   # секунд на столбик
        self.current_bar       = ClusterBar(time.time(), self.cluster_interval)
        self.completed_bars: deque = deque(maxlen=8)    # последние 8 столбиков

        # TXT-лог (живой снимок терминала)
        _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_path = f"dom_{self.symbol.lower()}_{_ts}.txt"
        self._init_txt_log()

    # ── Сетевые запросы ──────────────────────────────────────────────────────

    async def ws_depth_loop(self):
        stream_name = f"{self.symbol.lower()}@depth"
        url = f"wss://fstream.binance.com/ws/{stream_name}"
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        async for msg in ws:
                            if not self.running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                for p, q in data.get("b", []):
                                    fp, fq = float(p), float(q)
                                    if fq == 0:
                                        self.bids.pop(fp, None)
                                    else:
                                        self.bids[fp] = fq
                                for p, q in data.get("a", []):
                                    fp, fq = float(p), float(q)
                                    if fq == 0:
                                        self.asks.pop(fp, None)
                                    else:
                                        self.asks[fp] = fq
                                self.tick_tracker()
            except Exception as e:
                if self.running:
                    await asyncio.sleep(1)

    async def ws_agg_trade_loop(self):
        url = "wss://fstream.binance.com/market/ws"
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        await ws.send_json({
                            "method": "SUBSCRIBE",
                            "params": [f"{self.symbol.lower()}@aggTrade"],
                            "id": 1
                        })
                        async for msg in ws:
                            if not self.running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                t = json.loads(msg.data)
                                if t.get("e") == "aggTrade":
                                    price = round(float(t["p"]) / self.tick_size) * self.tick_size
                                    qty   = float(t["q"])
                                    if t["m"]:
                                        self.at_bid[price] += qty
                                    else:
                                        self.at_ask[price] += qty
                                    self.volume[price] += qty
                                    if int(t["a"]) > self.last_trade_id:
                                        self.last_trade_id = int(t["a"])
                                    self.current_price = float(t["p"])
                                    self.current_bar.add_trade(price, qty, t["m"])
            except Exception as e:
                if self.running:
                    await asyncio.sleep(1)

    async def ws_mark_price_loop(self):
        stream_name = f"{self.symbol.lower()}@markPrice@1s"
        url = f"wss://fstream.binance.com/ws/{stream_name}"
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        async for msg in ws:
                            if not self.running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if data.get("e") == "markPriceUpdate":
                                    self.current_price = float(data["p"])
            except Exception:
                if self.running:
                    await asyncio.sleep(1)

    async def fetch_tick_size(self, session):
        try:
            async with session.get(
                f"{BASE}/fapi/v1/exchangeInfo",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    for s in data["symbols"]:
                        if s["symbol"] == self.symbol:
                            for f in s["filters"]:
                                if f["filterType"] == "PRICE_FILTER":
                                    self.tick_size = float(f["tickSize"])
                                    return
        except:
            pass

    # ── Обновление трекера ───────────────────────────────────────────────────

    def tick_tracker(self):
        """Вызывается после каждого fetch_depth/fetch_trades."""
        self._last_seq_result = self.seq.update(self.bids, self.asks)
        self._rotate_cluster_bar()

    # ── Кластерный чарт: служебные методы ───────────────────────────────────

    def _rotate_cluster_bar(self):
        """Если текущий бар истёк — архивируем, создаём новый."""
        now = time.time()
        if self.current_bar.is_expired(now):
            self.completed_bars.append(self.current_bar)
            self.current_bar = ClusterBar(now, self.cluster_interval)

    def _init_txt_log(self):
        """Создаём TXT-лог с шапкой."""
        try:
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write(f"{'='*80}\n")
                f.write(f"  DOM LOGGER  —  {self.symbol}  —  started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'='*80}\n\n")
        except Exception:
            pass

    def log_txt_snapshot(self, levels, signal, now_str,
                         ata, atb, total_delta, total_vol):
        """
        Записывает живой снимок DOM в TXT-файл.
        Вместо цветов — ASCII-арт:
          * бары объёма по BID/ASK сторонам  (X = крупная стена)
          >цена< маркер текущей цены
          [LONG] / [SHORT] / [FLAT] сигнал
        """
        try:
            lines = []
            W = 84

            # ── Шапка ──────────────────────────────────────────────────────
            lines.append(f"\n{'─'*W}")
            lines.append(
                f"  [{now_str}]  DOM — {self.symbol}"
                f"  цена: {self.current_price:.6f}"
                f"  tick: {self.tick_size}"
                f"  upd#{self.update_count}"
            )
            lines.append(f"  last_trade_id={self.last_trade_id}")
            lines.append(f"{'─'*W}")

            # ── Сигнал ─────────────────────────────────────────────────────
            if signal:
                s       = signal["score"]
                d       = signal["detail"]
                dir_tag = (signal["direction"]
                           .replace("▲", "^").replace("▼", "v").replace("—", "-"))
                bar_w   = 18
                filled  = int(((s + 9) / 18) * bar_w)
                score_bar = "[" + "*" * filled + "." * (bar_w - filled) + "]"

                lines.append(
                    f"  СИГНАЛ: [{dir_tag}]  score={s:+d}/±9"
                    f"  уверенность={signal['confidence']:.0f}%"
                    f"  цель: {signal['target']:.6f}"
                )
                lines.append(f"  скор:        {score_bar}  {'←' if s<0 else ('→' if s>0 else '-')}")

                ae  = d.get("ask_exhaust", 0)
                be  = d.get("bid_exhaust", 0)
                va  = d.get("vacuum_ask",  1)
                vb  = d.get("vacuum_bid",  1)
                fs  = d.get("flow_score",  0)
                oi  = d.get("cum_ofi",     0)
                im  = d.get("avg_imb",     0.5)

                ae_bar = "[" + "*" * int(ae*10) + "." * (10 - int(ae*10)) + "]"
                be_bar = "[" + "*" * int(be*10) + "." * (10 - int(be*10)) + "]"
                im_bar = "[" + "*" * int(im*10) + "." * (10 - int(im*10)) + "]"

                lines.append(f"  {'─'*60}")
                lines.append(
                    f"  A. EXHAUSTION  ask={ae:.0%} {ae_bar}"
                    f"  bid={be:.0%} {be_bar}"
                )
                ask_vac = "СВОБОДЕН" if va < 0.5 else ("СТЕНА!!" if va > 2 else "норма")
                bid_vac = "СВОБОДЕН" if vb < 0.5 else ("СТЕНА!!" if vb > 2 else "норма")
                lines.append(
                    f"  B. VACUUM      ask_путь={ask_vac}({va:.1f}x)"
                    f"  bid_путь={bid_vac}({vb:.1f}x)"
                )
                lines.append(
                    f"  C. FLOW 30upd  direction={fs:+d}"
                    f"  cum_ofi={oi:+.0f}"
                    f"  imbalance={im:.3f} {im_bar}"
                )
                lines.append(f"  {'─'*60}")

            # ── Итоги ──────────────────────────────────────────────────────
            sign = "+" if total_delta >= 0 else ""
            lines.append(
                f"  Δ total: {sign}{total_delta:,.0f}"
                f"  |  @Ask={ata:,.0f}  @Bid={atb:,.0f}"
                f"  |  Volume={total_vol:,.0f}"
            )
            lines.append("")

            # ── DOM-таблица ─────────────────────────────────────────────────
            max_bv = max((l["bid_vol"] for l in levels if l["bid_vol"] > 0), default=1)
            max_av = max((l["ask_vol"] for l in levels if l["ask_vol"] > 0), default=1)
            BAR_W  = 12   # ширина * бара

            hdr = (
                f"  {'BID_BAR':^12} "
                f"{'Bid Vol':>9} {'@Bid':>8} {'Bid':>8}  "
                f"{'Price':^12}  "
                f"{'Ask':>8} {'@Ask':>8} {'Ask Vol':>9} "
                f"{'ASK_BAR':^12} "
                f"{'Delta':>8}"
            )
            sep = "  " + "─" * (len(hdr) - 2)
            lines.append(hdr)
            lines.append(sep)

            cur = round(self.current_price / self.tick_size) * self.tick_size

            for lvl in levels:
                price   = lvl["price"]
                bv      = lvl["bid_vol"]
                av      = lvl["ask_vol"]
                atb_lvl = lvl["at_bid"]
                ata_lvl = lvl["at_ask"]
                delta   = lvl["delta"]

                is_cur = abs(price - cur) < self.tick_size * 0.5

                # BID-бар (растёт вправо к цене → выравнивание вправо)
                if bv > 0:
                    bid_filled = max(1, int((bv / max_bv) * BAR_W))
                    ch = "X" if bv > max_bv * 0.6 else "*"
                    bid_bar = (ch * bid_filled).rjust(BAR_W)
                else:
                    bid_bar = " " * BAR_W

                # ASK-бар (растёт влево от цены → выравнивание влево)
                if av > 0:
                    ask_filled = max(1, int((av / max_av) * BAR_W))
                    ch = "X" if av > max_av * 0.6 else "*"
                    ask_bar = (ch * ask_filled).ljust(BAR_W)
                else:
                    ask_bar = " " * BAR_W

                # Числа
                bv_s  = f"{bv:>9,.0f}"   if bv      > 0 else " " * 9
                atb_s = f"{atb_lvl:>8,.0f}" if atb_lvl > 0 else " " * 8
                bid_s = f"{bv:>8,.0f}"   if bv      > 0 else " " * 8
                av_s  = f"{av:>8,.0f}"   if av      > 0 else " " * 8
                ata_s = f"{ata_lvl:>8,.0f}" if ata_lvl > 0 else " " * 8
                av2_s = f"{av:>9,.0f}"   if av      > 0 else " " * 9

                sign_d = "+" if delta > 0 else ""
                d_s = (f"{sign_d}{delta:,.0f}".rjust(8)
                       if delta != 0 else " " * 8)

                # Цена: текущую выделяем >< стрелками
                if is_cur:
                    p_str = f">{price:.6f}<".center(12)
                else:
                    p_str = f"{price:.6f}".center(12)

                row = (
                    f"  {bid_bar} "
                    f"{bv_s} {atb_s} {bid_s}  "
                    f"{p_str}  "
                    f"{av_s} {ata_s} {av2_s} "
                    f"{ask_bar} "
                    f"{d_s}"
                )
                lines.append(row)

            lines.append(sep)
            lines.append("")

            # ── Cluster footprint ASCII (1:1 с render_clusters) ─────────────
            cols     = self.get_cluster_columns()
            nonempty = [b for b in cols if b.total_volume() > 0]
            if not nonempty:
                if cols:
                    nonempty = [cols[-1]]
            else:
                # текущий бар всегда показываем (даже если пустой)
                if cols and cols[-1] not in nonempty:
                    nonempty.append(cols[-1])

            if nonempty:
                remaining = int(self.current_bar.interval -
                                (time.time() - self.current_bar.start_ts))
                lines.append(
                    f"  CLUSTER CHART  {now_str}"
                    f"  |  {self.cluster_interval:.0f}s/бар"
                    f"  |  след.бар через {max(remaining,0)}s"
                )

                # ── динамическая ширина поля (как в render_clusters) ─────────
                max_cv = max(
                    (v for b in nonempty
                     for v in list(b.at_bid.values()) + list(b.at_ask.values())),
                    default=1.0
                )
                vol_w  = max(len(f"{max_cv:,.0f}"), 2)
                cell_w = vol_w * 2 + 3   # "NNN x NNN"
                col_w  = cell_w + 3      # + бар (|) + пробелы

                # ── ценовой диапазон: ±40 тиков, только цены с объёмом ───────
                cur_p  = self.current_price
                ts     = self.tick_size
                all_p: set = set()
                for b in nonempty:
                    for p in list(b.at_bid) + list(b.at_ask):
                        if abs(p - cur_p) <= 40 * ts:
                            all_p.add(p)

                # ── Метки времени ─────────────────────────────────────────────
                t_line = " " * 12
                for b in nonempty:
                    lbl = b.label() + (" [cur]" if b is self.current_bar else "")
                    t_line += lbl.ljust(col_w)
                lines.append(t_line)
                lines.append("  " + "─" * (10 + col_w * len(nonempty)))

                # ── Ценовые строки ────────────────────────────────────────────
                for price in sorted(all_p, reverse=True):
                    is_cur_p = abs(price - round(cur_p / ts) * ts) < ts * 0.5
                    p_str = (f">{price:.6f}<".rjust(10)
                             if is_cur_p else f"{price:.6f}".rjust(10))
                    row = f"  {p_str}"

                    for b in nonempty:
                        ab = b.at_bid.get(price, 0.0)
                        aa = b.at_ask.get(price, 0.0)
                        in_range = (b.low_price > 0 and
                                    b.low_price <= price <= b.high_price)

                        if ab == 0 and aa == 0:
                            # пустой уровень: вертикальная линия если в диапазоне
                            bar_ch = ("|" if b.delta() >= 0 else ":") if in_range else " "
                            row += "  " + bar_ch + " " * (col_w - 3)
                        else:
                            ab_s = f"{ab:,.0f}" if ab > 0 else "0"
                            aa_s = f"{aa:,.0f}" if aa > 0 else "0"
                            cell = f"{ab_s.rjust(vol_w)} x {aa_s.ljust(vol_w)}"
                            tot  = ab + aa
                            # бар: █ для крупных (≥30% макс), | для остальных
                            bar_ch = "█" if tot >= max_cv * 0.30 else "|"
                            row += " " + cell + bar_ch + " "

                    lines.append(row)

                lines.append("  " + "─" * (10 + col_w * len(nonempty)))

                # ── Дельта под каждым столбиком ───────────────────────────────
                d_line = " " * 12
                for b in nonempty:
                    d_val = b.delta()
                    d_str = f"{d_val:+,.0f}"
                    d_line += d_str.ljust(col_w)
                lines.append(d_line)

            lines.append("")

            # ── Запись в файл ───────────────────────────────────────────────
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")

        except Exception:
            pass  # никогда не ломаем основной поток


    def get_cluster_columns(self) -> list:
        """
        Возвращает список столбиков для отображения.
        Каждый столбик = один ClusterBar (завершённые + текущий последним).
        """
        cols = list(self.completed_bars)
        cols.append(self.current_bar)   # текущий — самый правый
        return cols

    # ── DOM уровни для отображения ───────────────────────────────────────────

    def get_dom_levels(self):
        if not self.bids and not self.asks:
            return []
        all_prices = set()
        for p in list(self.bids.keys())[:self.levels]:
            all_prices.add(p)
        for p in list(self.asks.keys())[:self.levels]:
            all_prices.add(p)

        levels = []
        for price in sorted(all_prices, reverse=True):
            bid_vol = self.bids.get(price, 0)
            ask_vol = self.asks.get(price, 0)
            atb     = self.at_bid.get(price, 0)
            ata     = self.at_ask.get(price, 0)
            delta   = ata - atb
            vol     = self.volume.get(price, 0)
            levels.append({
                "price":   price,
                "bid_vol": bid_vol,
                "ask_vol": ask_vol,
                "at_bid":  atb,
                "at_ask":  ata,
                "delta":   delta,
                "volume":  vol,
            })
        return levels

    def get_totals(self):
        total_at_ask = sum(self.at_ask.values())
        total_at_bid = sum(self.at_bid.values())
        total_delta  = total_at_ask - total_at_bid
        total_vol    = sum(self.volume.values())
        return total_at_ask, total_at_bid, total_delta, total_vol

    # ── ГЛАВНАЯ ЛОГИКА ПРЕДСКАЗАНИЯ ──────────────────────────────────────────

    def get_signal(self):
        """
        Предсказание направления на 0.4% на основе порядка исполнения.

        Три уровня анализа:
          A. EXHAUSTION  — текущий лучший ask/bid почти съеден → цена сдвинется
          B. VACUUM      — что за текущим уровнем? пусто = путь свободен
          C. FLOW        — в каком направлении исчезают уровни последовательно

        Итоговый score: от -9 до +9
          >= +4  → LONG  (вверх на 0.4%)
          <= -4  → SHORT (вниз на 0.4%)
          иначе  → ФЛЭТ
        """
        if not self.bids or not self.asks:
            return None

        bids_sorted = sorted(self.bids.items(), reverse=True)
        asks_sorted = sorted(self.asks.items())

        best_bid_p, best_bid_v = bids_sorted[0]
        best_ask_p, best_ask_v = asks_sorted[0]
        price = self.current_price

        score  = 0
        detail = {}

        # ════════════════════════════════════════════════════════════════════
        # A. EXHAUSTION — насколько съеден текущий лучший уровень?
        #
        # Логика: at_ask[best_ask] — это сколько уже исполнилось прямо
        # на этом уровне. Если > 60% от standing объёма → уровень
        # скоро закончится и цена ДОЛЖНА двинуться вверх (для ask).
        # ════════════════════════════════════════════════════════════════════

        ask_traded   = self.at_ask.get(best_ask_p, 0)
        ask_exhaust  = ask_traded / max(best_ask_v + ask_traded, 1e-9)

        bid_traded   = self.at_bid.get(best_bid_p, 0)
        bid_exhaust  = bid_traded / max(best_bid_v + bid_traded, 1e-9)

        # Лучший ask сильно съеден → следующий уровень станет новым ask
        if ask_exhaust > 0.70:
            score += 3   # сильный сигнал вверх
        elif ask_exhaust > 0.50:
            score += 2
        elif ask_exhaust > 0.30:
            score += 1

        # Лучший bid сильно съеден → следующий уровень станет новым bid
        if bid_exhaust > 0.70:
            score -= 3
        elif bid_exhaust > 0.50:
            score -= 2
        elif bid_exhaust > 0.30:
            score -= 1

        detail["ask_exhaust"] = round(ask_exhaust, 2)
        detail["bid_exhaust"] = round(bid_exhaust, 2)

        # ════════════════════════════════════════════════════════════════════
        # B. VACUUM — анализ следующих N уровней после лучшего
        #
        # Логика последовательного исполнения:
        #   Если за лучшим ask стоит МАЛО объёма (вакуум) → цена пройдёт
        #   этот промежуток быстро без сопротивления.
        #   Если за лучшим bid стоит МАЛО объёма → падение ускорится.
        #
        #   "Стена" = первый уровень с объёмом > avg*3 = цель движения.
        # ════════════════════════════════════════════════════════════════════

        # Вакуум на стороне ask (уровни 2..6 после лучшего ask)
        next_asks = asks_sorted[1:6]
        next_bids = bids_sorted[1:6]

        ask_avg   = sum(v for _, v in asks_sorted[:20]) / max(len(asks_sorted[:20]), 1)
        bid_avg   = sum(v for _, v in bids_sorted[:20]) / max(len(bids_sorted[:20]), 1)

        vacuum_ask = sum(v for _, v in next_asks) / max(len(next_asks) * ask_avg, 1e-9)
        vacuum_bid = sum(v for _, v in next_bids) / max(len(next_bids) * bid_avg, 1e-9)

        # vacuum < 0.5 → плотность следующих уровней ниже 50% от средней = вакуум
        if vacuum_ask < 0.5:
            score += 2   # путь вверх свободен
        elif vacuum_ask < 0.8:
            score += 1
        elif vacuum_ask > 2.0:
            score -= 1   # стена сверху, вверх трудно

        if vacuum_bid < 0.5:
            score -= 2   # путь вниз свободен
        elif vacuum_bid < 0.8:
            score -= 1
        elif vacuum_bid > 2.0:
            score += 1   # стена снизу, вниз трудно

        detail["vacuum_ask"] = round(vacuum_ask, 2)
        detail["vacuum_bid"] = round(vacuum_bid, 2)

        # Цель: первая реальная стена (объём > avg*3)
        target_up   = self._find_wall(asks_sorted, ask_avg, direction="up")
        target_down = self._find_wall(bids_sorted, bid_avg, direction="down")

        # ════════════════════════════════════════════════════════════════════
        # C. SEQUENTIAL FLOW — что говорит трекер последовательности
        #
        # flow_score: за каждое обновление +1 если ask_consumed > bid_consumed
        # Это показывает ТРЕНД кто кого поедает обновление за обновлением
        # ════════════════════════════════════════════════════════════════════

        seq = self._last_seq_result
        if seq:
            flow_score    = seq.get("flow_score", 0)
            window_len    = self.seq.ofi_hist.maxlen

            # Нормируем к [-2, +2]
            flow_norm = flow_score / max(window_len, 1)
            if flow_norm > 0.3:
                score += 2
            elif flow_norm > 0.1:
                score += 1
            elif flow_norm < -0.3:
                score -= 2
            elif flow_norm < -0.1:
                score -= 1

            # OFI накопленный
            cum_ofi = seq.get("cum_ofi", 0)
            if cum_ofi > 0:
                score += 1
            elif cum_ofi < 0:
                score -= 1

            # Imbalance стакана (долгосрочный)
            avg_imb = seq.get("avg_imbalance", 0.5)
            if avg_imb > 0.62:
                score += 1
            elif avg_imb < 0.38:
                score -= 1

            detail["flow_score"]  = flow_score
            detail["cum_ofi"]     = round(cum_ofi, 1)
            detail["avg_imb"]     = round(avg_imb, 3)

        # ════════════════════════════════════════════════════════════════════
        # ИТОГОВОЕ РЕШЕНИЕ
        # ════════════════════════════════════════════════════════════════════

        target_pct = 0.004   # 0.4%

        if score >= 4:
            direction    = "▲ LONG"
            color        = "green"
            target_price = target_up if target_up else price * (1 + target_pct)
        elif score <= -4:
            direction    = "▼ SHORT"
            color        = "red"
            target_price = target_down if target_down else price * (1 - target_pct)
        else:
            direction    = "— ФЛЭТ"
            color        = "dim"
            target_price = price

        confidence = min(abs(score) / 9.0 * 100, 99)

        return {
            "direction":    direction,
            "color":        color,
            "score":        score,            # -9..+9
            "confidence":   confidence,       # 0..99%
            "target":       target_price,
            "detail":       detail,
        }

    def _find_wall(self, levels_sorted, avg_vol, direction="up", threshold=3.0):
        """
        Первый уровень с объёмом > avg * threshold = реальная стена.
        Это и есть цель движения цены по логике исполнения.
        """
        for price, vol in levels_sorted[1:]:   # пропускаем лучший уровень
            if vol > avg_vol * threshold:
                return price
        return None


# ─────────────────────────────────────────────────────────────────────────────
# ОТОБРАЖЕНИЕ
# ─────────────────────────────────────────────────────────────────────────────



def clear():
    os.system("cls" if os.name == "nt" else "clear")

def fmt_num(n: float, width: int = 8) -> str:
    if n == 0:
        return " " * width
    return f"{n:,.0f}".rjust(width)

def fmt_delta(d: float, width: int = 7) -> str:
    if d == 0:
        return " " * width
    s = f"{d:+,.0f}".rjust(width)
    return green(s) if d > 0 else red(s)

def bar(value: float, max_val: float = 1.0, width: int = 10, char="█") -> str:
    """Мини-бар для визуализации exhaust/vacuum."""
    filled = int(min(value / max(max_val, 1e-9), 1.0) * width)
    return char * filled + "░" * (width - filled)


def _cell_text(ab: float, aa: float, vol_w: int) -> str:
    """Форматирует одну ячейку 'bid x ask'."""
    ab_s = f"{ab:,.0f}" if ab > 0 else "0"
    aa_s = f"{aa:,.0f}" if aa > 0 else "0"
    return f"{ab_s.rjust(vol_w)} x {aa_s.ljust(vol_w)}"


def _cell_color(ab: float, aa: float, is_current: bool,
                max_vol: float, bar_color: str) -> callable:
    """Возвращает функцию окраски для ячейки."""
    if is_current:
        return cyan
    dlt = aa - ab
    tot = aa + ab
    if tot == 0:
        return dim
    ratio = dlt / tot                   # -1..+1
    if ratio > 0.25:
        return green                    # покупатели
    if ratio < -0.25:
        return red                      # продавцы
    return dim                          # нейтрально


def render_clusters(dom: DOMEngine):
    """
    Footprint-chart в стиле скрина:

      ─ каждый бар = вертикальная линия (│ красная/зелёная)
      ─ на уровнях с объёмом: числа  bid x ask  (рядом с линией)
      ─ уровни без объёма внутри диапазона High-Low: тонкая линия │
      ─ цвет линии: красный = дельта < 0, зелёный = дельта >= 0
      ─ крупные числа (>30% от макс) — жирные
      ─ текущий бар — cyan
    """
    cols = dom.get_cluster_columns()
    if not cols:
        return

    # ── Фильтруем пустые ────────────────────────────────────────────────────
    nonempty = [b for b in cols if b.total_volume() > 0]
    if not nonempty:
        print(dim("\n  CLUSTER CHART — нет данных ещё..."))
        return
    # текущий бар всегда показываем даже если пустой
    if cols[-1] not in nonempty:
        nonempty.append(cols[-1])

    # ── Ценовой диапазон: объединяем все бары, ±40 тиков от цены ────────────
    cur = dom.current_price
    ts  = dom.tick_size
    half = 40
    all_prices: set[float] = set()
    for bar in nonempty:
        for p in list(bar.at_bid) + list(bar.at_ask):
            if abs(p - cur) <= half * ts:
                all_prices.add(p)   # используем ключи как есть — они уже округлены при записи
    if not all_prices:
        return
    sorted_prices = sorted(all_prices, reverse=True)

    # ── Ширина числового поля ────────────────────────────────────────────────
    max_vol = max(
        (v for bar in nonempty
         for v in list(bar.at_bid.values()) + list(bar.at_ask.values())),
        default=1.0
    )
    vol_w  = max(len(f"{max_vol:,.0f}"), 2)
    cell_w = vol_w * 2 + 3          # "NNN x NNN"
    col_w  = cell_w + 3             # + бар (│) + пробелы

    # ── Заголовок ────────────────────────────────────────────────────────────
    now = datetime.now().strftime("%H:%M:%S")
    remaining = int(dom.current_bar.interval -
                    (time.time() - dom.current_bar.start_ts))
    print(bold(f"\n  CLUSTER CHART  {now}"
               f"  |  {dom.cluster_interval:.0f}s/бар"
               f"  |  текущий бар заканчивается через {max(remaining,0)}s"
               f"  |  лог → {dom.log_path}"))

    # Метки времени
    time_line = " " * 12
    for i, bar in enumerate(nonempty):
        lbl = bar.label()
        is_cur = (bar is dom.current_bar)
        colored = (bold(cyan(lbl)) if is_cur else dim(lbl))
        time_line += colored + " " * (col_w - len(lbl))
    print(time_line)

    print(dim("  " + "─" * (10 + col_w * len(nonempty))))

    # ── Строки: по каждому ценовому уровню сверху вниз ──────────────────────
    for price in sorted_prices:
        is_cur_price = abs(price - round(cur / ts) * ts) < ts * 0.5
        p_str = f"{price:.6f}".rjust(10)
        if is_cur_price:
            p_str = bg_yellow(bold(p_str))
        row = f"  {p_str}"

        for bar in nonempty:
            ab = bar.at_bid.get(price, 0.0)
            aa = bar.at_ask.get(price, 0.0)
            is_current = (bar is dom.current_bar)

            # Цвет вертикальной линии бара
            bar_delta = bar.delta()
            bar_line_fn = green if bar_delta >= 0 else red

            # Проверяем: цена в диапазоне Hi-Lo данного бара?
            in_range = (bar.low_price > 0 and
                        bar.low_price <= price <= bar.high_price)

            if ab == 0 and aa == 0:
                # Нет объёма: тонкая линия если внутри диапазона, иначе пусто
                if in_range:
                    bar_char = bar_line_fn("│")
                    row += "  " + bar_char + " " * (col_w - 3)
                else:
                    row += " " * col_w
            else:
                # Есть объём: числа + линия
                cell = _cell_text(ab, aa, vol_w)
                color_fn = _cell_color(ab, aa, is_current, max_vol,
                                       "green" if bar_delta >= 0 else "red")
                # Жирный если крупный объём
                if (ab + aa) >= max_vol * 0.25:
                    cell = bold(color_fn(cell))
                else:
                    cell = color_fn(cell)
                bar_char = bar_line_fn("█")
                row += " " + cell + bar_char + " "

        print(row)

    print(dim("  " + "─" * (10 + col_w * len(nonempty))))

    # Дельта под каждым столбиком
    delta_line = " " * 12
    for bar in nonempty:
        d = bar.delta()
        d_str = f"{d:+,.0f}"
        is_cur = (bar is dom.current_bar)
        colored = (bold(cyan(d_str)) if is_cur
                   else (green(d_str) if d >= 0 else red(d_str)))
        delta_line += colored + " " * (col_w - len(d_str))
    print(delta_line)

    print(dim("  Зелёный = покупатели | Красный = продавцы | █ = линия бара"))


def render(dom: DOMEngine):
    clear()
    now_str = datetime.now().strftime("%H:%M:%S")

    # ── Кластерный чарт (footprint) ──────────────────────────────────────────
    render_clusters(dom)

    # ── TXT-лог (живой снимок) ───────────────────────────────────────────────
    dom.log_txt_snapshot([], None, now_str, 0, 0, 0, 0)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def main(symbols: str, levels: int = 20, cluster_interval: float = 30.0):
    # Список движков — по одному на символ
    doms = [DOMEngine(sym, levels) for sym in symbols]
    for dom in doms:
        dom.cluster_interval = cluster_interval
        dom.current_bar      = ClusterBar(time.time(), cluster_interval)

    print(f"Подключаемся к Binance Futures... ({', '.join(symbols)})")
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[dom.fetch_tick_size(session) for dom in doms])

        # запускаем WS в фоне
        tasks = []
        for dom in doms:
            dom.tick_tracker()
            tasks.append(asyncio.create_task(dom.ws_agg_trade_loop()))
            tasks.append(asyncio.create_task(dom.ws_depth_loop()))
            tasks.append(asyncio.create_task(dom.ws_mark_price_loop()))

        print(f"tick_size={dom.tick_size}  цена={dom.current_price}")
        print("Запускаем DOM...")
        await asyncio.sleep(1)

        while all(d.running for d in doms):
            try:
                for dom in doms:
                    dom.update_count += 1
                    render(dom)

                await asyncio.sleep(0.5)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Ошибка: {e}")
                await asyncio.sleep(1)


if __name__ == "__main__":
    args = sys.argv[1:]
    sym_args = [a.upper() for a in args if not a.replace(".", "").isdigit()]
    levels   = int(sys.argv[2])    if len(sys.argv) > 2 else 20
    interval = float(sys.argv[3])  if len(sys.argv) > 3 else 30.0

    if sym_args:
        symbols = sym_args
    else:
        symbols = asyncio.run(get_active_symbols())
        if not symbols:
            print("❌ Нет токенов под критерии. Выход.")
            sys.exit(1)

    print(f"DOM Viewer — {len(symbols)} символов | {levels} уровней | cluster={interval:.0f}s")
    try:
        asyncio.run(main(symbols, levels, interval))
    except KeyboardInterrupt:
        print("\nВыход.")