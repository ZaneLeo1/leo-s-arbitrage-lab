#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pulse Cross-Exchange Monitor v3.2
- 数据源： https://pulse.astro-btc.xyz/api/query
- 目标：跨交易所（优先 Binance vs OKX） 价差 & 资金费率 监控
- 1秒抓取、黑色前端、可排序、可配置均值窗口、z-score 偏离
- 健壮解析：自动在返回JSON里查找 *Perp* / *Futures* 段；缺 fundingRate 也可跑
"""

import threading, time, requests, math, os
from collections import defaultdict, deque
from flask import Flask, jsonify, send_from_directory, request
from datetime import datetime, timezone

PULSE_URL = "https://pulse.astro-btc.xyz/api/query"
FETCH_INTERVAL_SEC = 1        # 拉取频率（后端）
WINDOW_SEC_DEFAULT = 300      # 默认均值/方差窗口（前端可调）
HOST = "0.0.0.0"
PORT = 8000
TIMEOUT = 10

# —— 数据结构：保存每个 symbol 的每个交易所 mid 价格与资金费率（环形缓存）
# ring[symbol]['binance'] = {'mid': deque[(ts, price)], 'funding': deque[(ts, rate)]}
ring_prices = defaultdict(lambda: defaultdict(lambda: deque(maxlen=6000)))   # 6000条≈1h 1Hz
ring_funding = defaultdict(lambda: defaultdict(lambda: deque(maxlen=14400))) # 4h
lock = threading.Lock()

def now_utc_ms():
    return int(time.time() * 1000)

def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict): return default
        cur = cur.get(k)
        if cur is None: return default
    return cur

def mid_from_bid_ask(b, a):
    try:
        b = float(b); a = float(a)
        if b > 0 and a > 0 and a >= b:
            return (a + b) / 2.0
    except:
        pass
    return None

def guess_funding(d):
    """
    容错提取资金费率字段：常见key: fundingRate / funding / fr / fundingRate8h（百分比或小数）
    """
    for k in ("fundingRate", "funding", "fr", "funding_rate", "fundingRate8h"):
        v = d.get(k)
        if v is None: 
            continue
        try:
            v = float(v)
            return v
        except:
            try:
                return float(str(v).replace("%",""))/100.0
            except:
                continue
    return None

def parse_perp_sections(j):
    """
    在 /api/query 返回中，尽量找到“永续/期货”段。
    约定（不保证）：key 名里包含 'Perp' 'perp' 'Swap' 'Futures' 等。
    返回 dict: { exchange: [ {name, a, b, funding?}, ...] }
    """
    out = {}
    data = j.get("data") or j  # 某些版本直接顶层给data
    if not isinstance(data, dict):
        return out

    for key, val in data.items():
        if not isinstance(val, dict): 
            continue
        # 仅处理有 ts / list 的结构，且 key 含 perp/swap/futures
        name_lc = key.lower()
        if not any(tag in name_lc for tag in ("perp","swap","futures")):
            continue
        lst = val.get("list")
        if not isinstance(lst, list):
            continue
        # 从 key 猜交易所名：binancePerp -> binance
        exch = name_lc.replace("perp","").replace("swap","").replace("futures","")
        exch = exch.strip("_ ").replace("spot","") or key.lower()
        out[exch] = []
        for it in lst:
            sym = safe_get(it, "name", default=None)
            a = safe_get(it, "a", default=None)
            b = safe_get(it, "b", default=None)
            if not sym or a is None or b is None:
                # 有些结构可能用 ask/bid
                a = it.get("ask", a)
                b = it.get("bid", b)
            # 资金费率尝试
            fr = guess_funding(it)
            out[exch].append({"name": sym, "a": a, "b": b, "funding": fr})
    return out

def fetch_loop():
    """
    后台线程：每秒抓一次 pulse，并更新 ring buffer
    """
    while True:
        try:
            resp = requests.get(PULSE_URL, timeout=TIMEOUT)
            resp.raise_for_status()
            j = resp.json()
            sections = parse_perp_sections(j)

            ts = now_utc_ms()
            with lock:
                for exch, rows in sections.items():
                    for row in rows:
                        name = row["name"]
                        mid = mid_from_bid_ask(row["b"], row["a"])
                        if mid is not None:
                            ring_prices[name][exch].append((ts, mid))
                        if row.get("funding") is not None:
                            ring_funding[name][exch].append((ts, float(row["funding"])))
        except Exception as e:
            # 静默重试，避免刷屏
            pass
        time.sleep(FETCH_INTERVAL_SEC)

def series_stats(series, now_ms, window_sec):
    """
    给定 deque[(ts, val)], 取 window_sec 内数据，返回 (mean, std)（不足2条时 std=0）
    """
    if not series:
        return (None, None, 0)
    cutoff = now_ms - window_sec * 1000
    vals = [v for (t,v) in series if t >= cutoff]
    n = len(vals)
    if n == 0:
        return (None, None, 0)
    mean = sum(vals) / n
    if n < 2:
        return (mean, 0.0, n)
    var = sum((x-mean)**2 for x in vals) / (n-1)
    std = math.sqrt(var)
    return (mean, std, n)

def pick_pair_exchanges(exch_dict):
    """
    给某个 symbol 的 {exch: deque[(ts,mid)]}，优先挑 binance vs okx；否则挑任意两个最新的
    返回 (exA, exB)
    """
    keys = list(exch_dict.keys())
    if "binance" in keys and "okx" in keys:
        return ("binance", "okx")
    # 否则选最近更新时间最新的两个
    if len(keys) >= 2:
        keys.sort(key=lambda k: exch_dict[k][-1][0] if exch_dict[k] else 0, reverse=True)
        return (keys[0], keys[1])
    return (None, None)

app = Flask(__name__, static_url_path="/static", static_folder="static")

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/data")
def api_data():
    """
    返回：
    {
      "ok": true,
      "updated": "...",
      "items": [
        {
          "symbol": "DOGEUSDT",
          "exA": "binance",
          "exB": "okx",
          "midA": 0.20136,
          "midB": 0.20144,
          "spread_pct": 0.0397,             # (midB-midA)/avg*100
          "avg_spread_pct": 0.0123,         # 窗口均值
          "zscore": 1.85,                   # (当前-均值)/std
          "fundingA": 0.0001,               # 单边资金费率（如果有）
          "fundingB": -0.00005,
          "funding_avg": 0.000025,
          "window_sec": 300,
          "samples": 280                    # 用于均值/方差的样本数
        },
        ...
      ]
    }
    支持参数：?window=秒 （默认300）
    """
    try:
        window = int(request.args.get("window", WINDOW_SEC_DEFAULT))
        now_ms = now_utc_ms()
        items = []

        with lock:
            for sym, exch_map in list(ring_prices.items()):
                if len(exch_map) < 2:
                    continue
                exA, exB = pick_pair_exchanges(exch_map)
                if not exA or not exB:
                    continue

                # 取两个交易所的最新 mid
                midA = exch_map[exA][-1][1] if exch_map[exA] else None
                midB = exch_map[exB][-1][1] if exch_map[exB] else None
                if not midA or not midB:
                    continue

                avg_mid = (midA + midB) / 2.0
                if avg_mid <= 0:
                    continue
                spread_pct = (midB - midA) / avg_mid * 100.0

                # 计算历史 “spread_pct” 的均值/方差
                # 构造一条虚拟序列：遍历窗口内的配对采样 —— 取时间对齐近似（用各自队列内的最近值）
                # 为性能简化：直接抽样较近的 N 点（以较短队列为基）
                seriesA = list(exch_map[exA])[-window:]  # 最近 window 条不一定是 window 秒，但够近似
                seriesB = list(exch_map[exB])[-window:]
                n = min(len(seriesA), len(seriesB))
                spreads = []
                for i in range(n):
                    mA = seriesA[-1 - i][1]
                    mB = seriesB[-1 - i][1]
                    avgm = (mA + mB) / 2.0
                    if avgm > 0:
                        spreads.append((mB - mA)/avgm * 100.0)

                if len(spreads) == 0:
                    avg_spread = None; std_spread = None; samples = 0
                elif len(spreads) == 1:
                    avg_spread = spreads[0]; std_spread = 0.0; samples = 1
                else:
                    avg_spread = sum(spreads)/len(spreads)
                    mu = avg_spread
                    var = sum((x-mu)**2 for x in spreads)/(len(spreads)-1)
                    std_spread = math.sqrt(var)
                    samples = len(spreads)

                zscore = None
                if std_spread is not None and std_spread > 1e-9 and avg_spread is not None:
                    zscore = (spread_pct - avg_spread) / std_spread

                # 资金费率（若有）
                fA = None; fB = None; fAvg = None
                f_map = ring_funding.get(sym) or {}
                if f_map.get(exA):
                    fA = f_map[exA][-1][1]
                if f_map.get(exB):
                    fB = f_map[exB][-1][1]
                if isinstance(fA, (int,float)) or isinstance(fB, (int,float)):
                    vals = [v for v in (fA,fB) if isinstance(v, (int,float))]
                    if vals:
                        fAvg = sum(vals)/len(vals)

                items.append({
                    "symbol": sym,
                    "exA": exA, "exB": exB,
                    "midA": round(midA, 10), "midB": round(midB, 10),
                    "spread_pct": round(spread_pct, 6),
                    "avg_spread_pct": None if avg_spread is None else round(avg_spread, 6),
                    "zscore": zscore if zscore is None else round(zscore, 3),
                    "fundingA": fA, "fundingB": fB,
                    "funding_avg": fAvg,
                    "window_sec": window,
                    "samples": samples
                })

        items.sort(key=lambda x: abs(x["spread_pct"]), reverse=True)  # 默认按当前价差绝对值降序
        return jsonify({
            "ok": True,
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "items": items
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/static/<path:path>")
def static_proxy(path):
    return send_from_directory("static", path)

if __name__ == "__main__":
    # 后台拉取线程
    t = threading.Thread(target=fetch_loop, daemon=True)
    t.start()
    print(f"🚀 Pulse v3.2 启动： http://0.0.0.0:{PORT} ；每 {FETCH_INTERVAL_SEC}s 抓取一次")
    app.run(host=HOST, port=PORT, debug=False)
