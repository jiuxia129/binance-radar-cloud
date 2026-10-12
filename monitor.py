# -*- coding: utf-8 -*-
"""
Binance Radar Cloud v1.11.0 —— 云端全市场异动监控（GitHub Actions 定时运行）
=====================================================================
设计目标：电脑关机也能推送。运行在 GitHub Actions 境外服务器，
现货（data-api 镜像）/ 合约（fapi 镜像）与 Bybit/OKX 公共接口均可直连。

v1.11.0 新增（相对 v1.10.0）：
  1. 全平台数据聚合（按币种）：Binance + Gate + OKX + Bybit + Bitget 五家合约 OI、24h 合约成交额、
     24h 现货成交额（近似 CoinGlass 全平台口径，五家为全球最大合约交易所）
  2. WxPusher 微信推送通道（appToken + UID），邮件通道保留，双通道并行
  3. 价格异动改为涨跌都推（窗口 |变化| >= 5%），推送中附加：
       24h 合约成交额：$X.XX亿 / 24h 现货成交额：$X.XX亿 / 全平台 OI：1小时前 X 3小时前 X 现在 X
  4. OI 异动独立监控：全部合约币种，1 小时窗口 OI 变化 |pct| >= OI_PCT 触发，涨跌都推

配置：通过环境变量注入（GitHub Actions Secrets）：
  MAIL_USER / MAIL_PASS / MAIL_TO     QQ 邮箱 SMTP（可选，邮件通道）
  WXPUSHER_APP_TOKEN / WXPUSHER_UID   WxPusher 微信推送（可选，微信通道）
  OI_PCT（可选，默认 10）              OI 异动 1 小时变化阈值 %
  OI_MIN_USD（可选，默认 1000000）    OI 异动最小当前 OI（USD），过滤灰尘币

无第三方依赖，仅用 Python 标准库（urllib + smtplib + concurrent.futures）。
"""
import json
import os
import ssl
import sys
import time
import urllib.request
import urllib.parse
import smtplib
from concurrent.futures import ThreadPoolExecutor
from email.header import Header
from email.mime.text import MIMEText

# ---------------------------------------------------------------- 常量
SPOT_URL = "https://data-api.binance.vision/api/v3/ticker/24hr"   # Binance 现货官方公共镜像（国内可直连）
FUT_URL  = "https://fapi.binance.com/fapi/v1/ticker/24hr"          # Binance 合约官方接口（Actions 境外直连）
OI_URL   = "https://fapi.binance.com/fapi/v1/openInterest"         # Binance 单币当前 OI(数量)
LIQ_URL  = "https://fapi.binance.com/fapi/v1/allForceOrders"       # 合约强平（最近7天，最多1000条）
BYBIT_TICKER = "https://api.bybit.com/v5/market/tickers"
OKX_OI_URL   = "https://www.okx.com/api/v5/public/open-interest"
OKX_TICKER   = "https://www.okx.com/api/v5/market/tickers"
GATE_FUT     = "https://api.gateio.ws/api/v4/futures/usdt/tickers"   # Gate 合约 ticker（自带 total_size/quanto_multiplier）
GATE_SPOT    = "https://api.gateio.ws/api/v4/spot/tickers"           # Gate 现货 ticker（quote_volume）
BITGET_FUT   = "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"  # Bitget 合约（自带 holdingAmount）
BITGET_SPOT  = "https://api.bitget.com/api/v2/spot/market/tickers"   # Bitget 现货（usdtVolume）

WIN_MIN = 5
UP_PCT = 5.0            # 价格异动阈值 %（涨跌都推）
OI_PCT = float(os.environ.get("OI_PCT", "10"))       # OI 异动 1 小时变化阈值 %
OI_MIN_USD = float(os.environ.get("OI_MIN_USD", "1000000"))  # OI 异动最小当前 OI
OI_WIN_H = 1            # OI 异动窗口（小时）
OI_HIST_H = 3           # 推送展示 OI 历史窗口（小时）
COOLDOWN_S = 300
OI_COOLDOWN_S = 1800    # OI 异动同币种冷却 30 分钟
MAX_MAILS_PER_RUN = 3
OI_MAX_PER_RUN = 5
LIQ_MAIL_USD = 500000
LIQ_MAX = 3
LIQ_SEEN_MAX = 4000
HIST_MAX = 60            # 价格序列采样点上限
OI_SNAP_MAX = 40         # 每币 OI 快照点数上限（5分钟一点，约覆盖 3h20m）
BIN_CONC = 32            # Binance OI 并发数

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(BASE_DIR, "state.json")

MAIL_HOST = os.environ.get("MAIL_HOST", "smtp.qq.com")
MAIL_PORT = int(os.environ.get("MAIL_PORT", "465"))
MAIL_USER = os.environ.get("MAIL_USER", "").strip()
MAIL_PASS = os.environ.get("MAIL_PASS", "").strip()
MAIL_TO = os.environ.get("MAIL_TO", MAIL_USER).strip()
WX_TOKEN = os.environ.get("WXPUSHER_APP_TOKEN", "").strip()
WX_UID = os.environ.get("WXPUSHER_UID", "").strip()
WX_URL = "https://wxpusher.zjiecode.com/api/send/message"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36 BinanceRadarCloud/1.11"
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def http_json(url, timeout=25, data=None, headers=None):
    hdrs = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
    if headers:
        hdrs.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


# ---------------------------------------------------------------- 状态持久化
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"hist": {}, "lastAlert": {}, "oiHist": {}, "lastOiAlert": {}, "liqSeen": {}, "lastLiqAlert": {}}


def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


# ---------------------------------------------------------------- 推送（邮件 + WxPusher）
def send_mail(subject, body):
    if not (MAIL_USER and MAIL_PASS):
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(MAIL_TO.split(","))
    s = smtplib.SMTP_SSL(MAIL_HOST, MAIL_PORT, timeout=25)
    try:
        s.login(MAIL_USER, MAIL_PASS)
        s.sendmail(MAIL_USER, [x.strip() for x in MAIL_TO.split(",") if x.strip()], msg.as_string())
    finally:
        try:
            s.quit()
        except Exception:
            pass


def send_wxpusher(content, summary=""):
    if not (WX_TOKEN and WX_UID):
        return
    payload = {
        "appToken": WX_TOKEN,
        "content": content,
        "summary": summary or content[:80],
        "contentType": 1,
        "uids": [WX_UID],
    }
    try:
        j = http_json(WX_URL, timeout=20, data=payload)
        ok = j.get("code") == 1000
        log("WxPusher 推送 %s: %s" % ("成功" if ok else "失败", str(j)[:160]))
        return ok
    except Exception as exc:
        log("WxPusher 推送异常: %s: %s" % (type(exc).__name__, str(exc)[:160]))
        return False


def push(subject, body):
    """双通道推送：邮件 + WxPusher。任一失败不影响另一通道。返回 True 表示至少一路成功。"""
    ok = False
    if MAIL_USER and MAIL_PASS:
        try:
            send_mail(subject, body)
            ok = True
            log("邮件推送成功: %s" % subject)
        except Exception as exc:
            log("邮件推送失败: %s: %s" % (type(exc).__name__, str(exc)[:160]))
    if WX_TOKEN and WX_UID:
        if send_wxpusher(body, subject):
            ok = True
    return ok


# ---------------------------------------------------------------- 数值格式化
def fmt_usd(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "--"
    if f != f:
        return "--"
    if f >= 1e8:
        return "$%.2f亿" % (f / 1e8)
    if f >= 1e4:
        return "$%.2f万" % (f / 1e4)
    return "$%.2f" % f


def fmt_num(v):
    try:
        f = float(v)
        if f != f:
            return "--"
        if f >= 1e8:
            return "%.2f亿" % (f / 1e8)
        if f >= 1e4:
            return "%.2f万" % (f / 1e4)
        return ("%.6f" % f).rstrip("0").rstrip(".") if f < 1 else ("%.4f" % f).rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "--"


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def coin_of_symbol(sym):
    """从交易对标号提取币种：BTCUSDT->BTC, 1000PEPEUSDT->1000PEPE, BTCUSDC->BTC, BTC-PERP->BTC"""
    s = str(sym).upper()
    for q in ("USDT", "USDC", "BUSD", "FDUSD", "PERP", "USD"):
        if s.endswith(q):
            return s[: -len(q)]
    return s


# ---------------------------------------------------------------- 市值查询（CoinGecko，v1.12 新增）
_CG_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
_CG_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
_cg_map = {}
_cg_map_ts = 0.0
_CG_MAP_TTL = 3600.0          # 币种映射缓存 1 小时


def _cg_symbol_map():
    """SYMBOL -> CoinGecko id 映射（缓存）"""
    global _cg_map, _cg_map_ts
    now = time.time()
    if _cg_map and now - _cg_map_ts < _CG_MAP_TTL:
        return _cg_map
    try:
        rows = http_json(_CG_LIST_URL, 25)
        m = {}
        for r in rows or []:
            s = str(r.get("symbol") or "").upper()
            i = str(r.get("id") or "")
            if s and i and s not in m:
                m[s] = i
        if m:
            _cg_map = m
            _cg_map_ts = now
            log("CoinGecko 币种映射: %d 个" % len(m))
    except Exception as exc:
        log("CoinGecko 币种映射获取失败: %s" % str(exc)[:120])
    return _cg_map


def fetch_mcap(coins):
    """批量查市值，入参为归一化币种（BTC/1000PEPE），返回 {COIN: usd}；失败返回 {}"""
    if not coins:
        return {}
    m = _cg_symbol_map()
    want = {}
    for c in coins:
        c = str(c).upper()
        i = m.get(c)
        if i:
            want[c] = i
    if not want:
        return {}
    out = {}
    try:
        url = _CG_PRICE_URL + "?ids=%s&vs_currencies=usd&include_market_cap=true" % ",".join(want.values())
        d = http_json(url, 20)
        for c, i in want.items():
            cap = (d.get(i) or {}).get("usd_market_cap")
            if cap:
                out[c] = float(cap)
    except Exception as exc:
        log("市值查询失败: %s" % str(exc)[:120])
    return out


def fmt_mcap(v):
    if not v:
        return "--"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "--"
    if f != f:
        return "--"
    if f >= 1e12:
        return "$%.2f万亿" % (f / 1e12)
    if f >= 1e8:
        return "$%.2f亿" % (f / 1e8)
    if f >= 1e4:
        return "$%.2f万" % (f / 1e4)
    return "$%.2f" % f


# ---------------------------------------------------------------- 五大所数据聚合（按币种）
def fetch_binance(st):
    """Binance 合约 ticker + 逐币 OI（并发）+ 现货 24h 成交额"""
    info = {}
    rows = http_json(FUT_URL, timeout=30)
    fut = []
    for x in rows or []:
        sym = str(x.get("symbol") or "").strip()
        try:
            price = float(x.get("lastPrice") or 0)
        except (TypeError, ValueError):
            price = 0.0
        if not sym or price <= 0:
            continue
        coin = coin_of_symbol(sym)
        fut.append((coin, sym, price, _to_float(x.get("quoteVolume")), _to_float(x.get("priceChangePercent"))))
    # 逐币 OI（并发）
    syms = [f[1] for f in fut]
    oi_map = {}

    def get_oi(s):
        try:
            j = http_json(OI_URL + "?symbol=" + urllib.parse.quote(s), timeout=15)
            return s, _to_float(j.get("openInterest"))
        except Exception:
            return s, 0.0

    if syms:
        with ThreadPoolExecutor(max_workers=BIN_CONC) as ex:
            for s, oi in ex.map(get_oi, syms):
                oi_map[s] = oi
    for coin, sym, price, vol24, chg24 in fut:
        oi_usd = oi_map.get(sym, 0.0) * price
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["oi"] += oi_usd
        c["futVol24"] += vol24
        c["chg24"] = max(c.get("chg24", 0.0), chg24)
    # Binance 现货 24h 成交额（稳定币计价对）
    try:
        srows = http_json(SPOT_URL, timeout=30)
        for x in srows or []:
            sym = str(x.get("symbol") or "").strip()
            if not sym.endswith(("USDT", "BUSD", "FDUSD", "USDC")):
                continue
            coin = coin_of_symbol(sym)
            c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
            c["spotVol24"] += _to_float(x.get("quoteVolume"))
    except Exception as exc:
        log("Binance 现货成交额失败: %s: %s" % (type(exc).__name__, str(exc)[:120]))
    return info


def fetch_bybit(st):
    """Bybit linear 合约 + spot：tickers 自带 openInterest / turnover24h"""
    info = {}
    j = http_json(BYBIT_TICKER + "?category=linear", timeout=30)
    for x in (j.get("result") or {}).get("list") or []:
        sym = str(x.get("symbol") or "").strip()
        price = _to_float(x.get("lastPrice"))
        if not sym or price <= 0:
            continue
        coin = coin_of_symbol(sym)
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["oi"] += _to_float(x.get("openInterest")) * price
        c["futVol24"] += _to_float(x.get("turnover24h"))
        c["chg24"] = max(c.get("chg24", 0.0), _to_float(x.get("price24hPcnt")) * 100)
    j = http_json(BYBIT_TICKER + "?category=spot", timeout=30)
    for x in (j.get("result") or {}).get("list") or []:
        sym = str(x.get("symbol") or "").strip()
        if not sym or not sym.endswith("USDT"):
            continue
        coin = coin_of_symbol(sym)
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["spotVol24"] += _to_float(x.get("turnover24h"))
    return info


def fetch_okx(st):
    """OKX SWAP OI（批量）+ SWAP/SPOT ticker 成交额"""
    info = {}
    oi_rows = {}
    j = http_json(OKX_OI_URL + "?instType=SWAP", timeout=30)
    for x in (j.get("data") or []):
        inst = str(x.get("instId") or "").upper()
        oi_rows[inst] = _to_float(x.get("oiCcy"))
    j = http_json(OKX_TICKER + "?instType=SWAP", timeout=30)
    swap_meta = {}
    for x in (j.get("data") or []):
        inst = str(x.get("instId") or "").upper()
        # OKX volCcy24h 为币数量，折算成交额 = 数量 * last
        last = _to_float(x.get("last"))
        swap_meta[inst] = (last, _to_float(x.get("volCcy24h")) * last)
    for inst, oi in oi_rows.items():
        # instId 形如 BTC-USDT-SWAP / BTC-USD-SWAP
        coin = inst.split("-")[0] if "-" in inst else inst
        last, vol24 = swap_meta.get(inst, (0.0, 0.0))
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["oi"] += oi * last
        c["futVol24"] += vol24
    j = http_json(OKX_TICKER + "?instType=SPOT", timeout=30)
    for x in (j.get("data") or []):
        inst = str(x.get("instId") or "").upper()
        if not inst.endswith("-USDT"):
            continue
        coin = inst.split("-")[0]
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        # 实测：OKX SPOT 的 volCcy24h 已是计价币(USDT)金额，直接用
        c["spotVol24"] += _to_float(x.get("volCcy24h"))
    return info


def fetch_gate(st):
    """Gate（芝麻开门）合约 + 现货 ticker：自带 total_size / quanto_multiplier / volume_24h_quote"""
    info = {}
    j = http_json(GATE_FUT, timeout=30)
    for x in j or []:
        contract = str(x.get("contract") or "").strip()
        last = _to_float(x.get("last"))
        if not contract or last <= 0:
            continue
        coin = contract.rsplit("_", 1)[0] if "_" in contract else contract
        oi_usd = _to_float(x.get("total_size")) * _to_float(x.get("quanto_multiplier")) * last
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["oi"] += oi_usd
        c["futVol24"] += _to_float(x.get("volume_24h_quote"))
        c["chg24"] = max(c.get("chg24", 0.0), _to_float(x.get("change_percentage")))
    j = http_json(GATE_SPOT, timeout=30)
    for x in j or []:
        pair = str(x.get("currency_pair") or "").strip()
        if not pair.endswith("_USDT"):
            continue
        coin = pair.rsplit("_", 1)[0]
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["spotVol24"] += _to_float(x.get("quote_volume"))
    return info


def fetch_bitget(st):
    """Bitget 合约 + 现货 ticker：合约自带 holdingAmount（币数量）"""
    info = {}
    j = http_json(BITGET_FUT, timeout=30)
    for x in (j.get("data") or []):
        sym = str(x.get("symbol") or "").strip()
        last = _to_float(x.get("lastPr"))
        if not sym or last <= 0:
            continue
        coin = coin_of_symbol(sym)
        oi_usd = _to_float(x.get("holdingAmount")) * last
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["oi"] += oi_usd
        c["futVol24"] += _to_float(x.get("usdtVolume"))
        c["chg24"] = max(c.get("chg24", 0.0), abs(_to_float(x.get("change24h"))) * 100)
    j = http_json(BITGET_SPOT, timeout=30)
    for x in (j.get("data") or []):
        sym = str(x.get("symbol") or "").strip()
        if not sym.endswith("USDT"):
            continue
        coin = coin_of_symbol(sym)
        c = info.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0})
        c["spotVol24"] += _to_float(x.get("usdtVolume"))
    return info


def fetch_aggregated(st):
    """聚合五大所，按币种合并"""
    merged = {}
    for fn in (fetch_binance, fetch_bybit, fetch_okx, fetch_gate, fetch_bitget):
        try:
            part = fn(st)
        except Exception as exc:
            log("数据源 %s 失败: %s: %s" % (fn.__name__, type(exc).__name__, str(exc)[:120]))
            continue
        for coin, c in part.items():
            m = merged.setdefault(coin, {"oi": 0.0, "futVol24": 0.0, "spotVol24": 0.0, "chg24": 0.0})
            m["oi"] += c.get("oi", 0.0)
            m["futVol24"] += c.get("futVol24", 0.0)
            m["spotVol24"] += c.get("spotVol24", 0.0)
            m["chg24"] = max(m.get("chg24", 0.0), c.get("chg24", 0.0))
    # 剔除灰尘币（OI < 1 万 USD）
    return {k: v for k, v in merged.items() if v["oi"] >= 10000}


# ---------------------------------------------------------------- OI 快照与历史查询
def update_oi_snapshot(st, agg):
    """每轮把全平台 OI 快照追加到 state，供 1h/3h 前对比"""
    oi_hist = st.setdefault("oiHist", {})
    now = int(time.time() * 1000)
    for coin, c in agg.items():
        if c["oi"] < 100000:
            continue                                   # 过滤灰尘币，避免 state.json 膨胀
        q = oi_hist.setdefault(coin, [])
        q.append([now, round(c["oi"], 2)])
        if len(q) > OI_SNAP_MAX:
            oi_hist[coin] = q[-OI_SNAP_MAX:]


def oi_at(oi_hist, coin, ago_s):
    """返回 ago_s 秒前最近快照的 OI；无数据返回 None"""
    q = oi_hist.get(coin)
    if not q:
        return None
    target = int(time.time() * 1000) - ago_s * 1000
    best = None
    for ts, v in q:
        if ts <= target:
            best = v
    return best


# ---------------------------------------------------------------- OI 异动检测
def check_oi_moves(st, agg, summary):
    """全部合约币种 OI 异动：1 小时窗口 |变化| >= OI_PCT，涨跌都推"""
    oi_hist = st.get("oiHist", {})
    last = st.setdefault("lastOiAlert", {})
    now = int(time.time() * 1000)
    sent = 0
    hits = []
    for coin, c in agg.items():
        cur = c["oi"]
        if cur < OI_MIN_USD:
            continue
        prev1h = oi_at(oi_hist, coin, 3600)
        if not prev1h or prev1h <= 0:
            continue
        pct = (cur / prev1h - 1.0) * 100.0
        if abs(pct) < OI_PCT:
            continue
        if now - (last.get(coin) or 0) < OI_COOLDOWN_S * 1000:
            continue
        hits.append((coin, cur, prev1h, pct))
    hits.sort(key=lambda h: abs(h[3]), reverse=True)
    for coin, cur, prev1h, pct in hits:
        if sent >= OI_MAX_PER_RUN:
            break
        last[coin] = now
        prev3h = oi_at(oi_hist, coin, 3 * 3600)
        direction = "增加" if pct >= 0 else "减少"
        subject = "[Binance云端] %s OI异动 %s %.1f%%" % (coin, direction, abs(pct))
        lines = [
            "Binance Radar Cloud OI 异动提醒",
            "",
            "币种     : %s" % coin,
            "类型     : OI 全平台持仓异动（1小时窗口）",
            "变化     : %s%.1f%%" % ("+" if pct >= 0 else "", pct),
            "市值     : %s" % fmt_mcap(fetch_mcap([coin]).get(coin)),
            "全平台 OI：1小时前 %s" % fmt_usd(prev1h),
            "          3小时前 %s" % (fmt_usd(prev3h) if prev3h else "--（数据积累中）"),
            "          现在     %s" % fmt_usd(cur),
            "",
            "24h 合约成交额：%s" % fmt_usd(agg[coin]["futVol24"]),
            "24h 现货成交额：%s" % fmt_usd(agg[coin]["spotVol24"]),
            "",
            "推送时间 : %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
            "由云端服务器监控（电脑关机不影响），参考 CoinGlass 全平台口径（Binance+Gate+OKX+Bybit+Bitget 聚合），仅供参考不构成投资建议。",
        ]
        body = "\n".join(lines)
        try:
            if push(subject, body):
                sent += 1
                summary.append("OI %s %s%.1f%% -> 已推送" % (coin, "+" if pct >= 0 else "", pct))
                log("OI异动推送成功: %s %s%.1f%% (%s -> %s)" % (coin, "+" if pct >= 0 else "", pct, fmt_usd(prev1h), fmt_usd(cur)))
        except Exception as exc:
            log("OI异动推送失败: %s -> %s" % (coin, str(exc)[:160]))
    return sent


# ---------------------------------------------------------------- 强平轮询（v1.10 保留）
def liq_level_of(notional):
    if notional >= 1000000:
        return "S"
    if notional >= 750000:
        return "A"
    if notional >= 600000:
        return "B"
    return "C"


def liq_advice(side):
    if str(side).upper() == "SELL":
        return "多头强平抛压，短线偏空，不建议追多，观望为主。"
    if str(side).upper() == "BUY":
        return "空头强平回补，短线偏多，可关注反弹，但勿重仓追高。"
    return "大额强平异动，波动加剧，建议观望，等待方向明朗。"


def check_liquidation(st, summary):
    try:
        rows = http_json(LIQ_URL + "?limit=1000")
    except Exception as exc:
        log("强平数据获取失败: %s: %s" % (type(exc).__name__, str(exc)[:120]))
        return 0
    if not isinstance(rows, list) or not rows:
        return 0
    seen = st.setdefault("liqSeen", {})
    last = st.setdefault("lastLiqAlert", {})
    now = int(time.time() * 1000)
    sent = 0
    new_keys = []
    for x in rows:
        sym = str(x.get("symbol") or "").strip()
        side = str(x.get("side") or "").upper()
        ts = int(x.get("time") or 0)
        avg = _to_float(x.get("averagePrice"))
        qty = _to_float(x.get("executedQty"))
        notional = avg * qty
        if not sym or not side or ts <= 0 or notional <= 0:
            continue
        key = "%s|%s|%s|%s" % (sym, side, ts, qty)
        if key in seen:
            continue
        seen[key] = now
        new_keys.append(key)
        if notional < LIQ_MAIL_USD:
            continue
        if sent >= LIQ_MAX:
            break
        cooldown_key = "%s|%s" % (sym, side)
        if now - (last.get(cooldown_key) or 0) < COOLDOWN_S * 1000:
            continue
        last[cooldown_key] = now
        direction = "多单爆仓" if side == "SELL" else ("空单爆仓" if side == "BUY" else "强平")
        coin = coin_of_symbol(sym)
        mcap = fetch_mcap([coin]).get(coin) if coin else None
        subject = "[Binance云端] %s 大额强平·%s %s" % (sym, direction, fmt_usd(notional))
        body = "\n".join([
            "Binance Radar Cloud 大额强平提醒",
            "",
            "币种     : %s" % sym,
            "市场     : 合约永续",
            "类型     : LIQ 大额强平（5分钟轮询）",
            "方向     : %s" % direction,
            "强平价格 : %s" % fmt_num(avg),
            "成交数量 : %s" % fmt_num(qty),
            "名义价值 : %s USDT" % fmt_usd(notional),
            "市值     : %s" % fmt_mcap(mcap),
            "评分     : %s 级" % liq_level_of(notional),
            "操作建议 : %s" % liq_advice(side),
            "",
            "推送时间 : %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
            "由云端服务器监控（电脑关机不影响），轮询口径与暴涨异动一致，仅供参考不构成投资建议。",
        ])
        try:
            if push(subject, body):
                sent += 1
                summary.append("FUTURES %s 强平 %s -> 已推送" % (sym, direction))
                log("推送成功: %s %s 强平 %s %s" % (sym, direction, fmt_usd(notional), side))
        except Exception as exc:
            log("推送失败: %s %s 强平 -> %s: %s" % (sym, direction, type(exc).__name__, str(exc)[:160]))
    if len(seen) > LIQ_SEEN_MAX:
        drop = sorted(seen.items(), key=lambda kv: kv[1])[: len(seen) - LIQ_SEEN_MAX]
        for k, _v in drop:
            seen.pop(k, None)
    return sent


# ---------------------------------------------------------------- 价格异动（涨跌都推 + 附加 OI）
def level_of(sc):
    return "S" if sc >= 75 else ("A" if sc >= 60 else ("B" if sc >= 45 else "C"))


def advice_of(sc):
    if sc >= 75:
        return "强势异动，可轻仓关注；追涨需谨慎，回踩支撑位再考虑介入。"
    if sc >= 60:
        return "偏强，可关注；注意不跌破参考支撑。"
    if sc >= 45:
        return "强度一般，观望为主，暂不推荐追高。"
    return "偏弱，不推荐追高，等待趋势确认。"


def snapshot(url, market, st):
    try:
        rows = http_json(url)
    except Exception as exc:
        return False, str(exc)
    if not isinstance(rows, list):
        return False, "response not list"
    now = int(time.time() * 1000)
    quote_ok = ("USDT", "BUSD", "FDUSD", "USDC")
    hist = st["hist"]
    info = {}
    for x in rows:
        sym = str(x.get("symbol") or "").strip()
        try:
            price = float(x.get("lastPrice") or 0)
        except (TypeError, ValueError):
            price = 0.0
        if not sym or price <= 0:
            continue
        if market == "spot" and not sym.endswith(quote_ok):
            continue
        key = "%s|%s" % (market, sym)
        dq = hist.get(key)
        if dq is None:
            dq = []
            hist[key] = dq
        if dq and now - dq[-1][0] < 3000:
            dq[-1] = [now, price]
        else:
            dq.append([now, price])
        if len(dq) > HIST_MAX:
            hist[key] = dq[-HIST_MAX:]
        info[key] = {
            "price": price,
            "chg24": _to_float(x.get("priceChangePercent")),
            "quoteVol": _to_float(x.get("quoteVolume")),
        }
    return True, info


def oi_lines_for(agg, oi_hist, coin):
    """构造 OI/成交额 附加行（价格异动推送用）"""
    c = agg.get(coin)
    if not c:
        return []
    cur = c["oi"]
    prev1h = oi_at(oi_hist, coin, 3600)
    prev3h = oi_at(oi_hist, coin, 3 * 3600)
    lines = []
    if cur > 0:
        if prev1h:
            lines.append("全平台 OI：1小时前 %s" % fmt_usd(prev1h))
            lines.append("          3小时前 %s" % (fmt_usd(prev3h) if prev3h else "--（数据积累中）"))
            lines.append("          现在     %s" % fmt_usd(cur))
        else:
            lines.append("全平台 OI：现在 %s（历史对比数据积累中）" % fmt_usd(cur))
    lines.append("24h 合约成交额：%s" % fmt_usd(c["futVol24"]))
    lines.append("24h 现货成交额：%s" % fmt_usd(c["spotVol24"]))
    return lines


def main():
    if not (MAIL_USER or (WX_TOKEN and WX_UID)):
        log("缺少推送配置（MAIL_USER/MAIL_PASS 或 WXPUSHER_APP_TOKEN/WXPUSHER_UID），跳过本次运行")
        sys.exit(1)

    st = load_state()
    now = int(time.time() * 1000)
    win_ms = WIN_MIN * 60000
    summary = []

    # 1) 五大所聚合数据（OI + 24h 成交额，按币种）
    agg = {}
    try:
        agg = fetch_aggregated(st)
        log("聚合数据: 币种=%d 全平台OI=%s" % (len(agg), fmt_usd(sum(v["oi"] for v in agg.values()))))
    except Exception as exc:
        log("聚合数据整体失败: %s" % str(exc)[:160])

    # 2) OI 快照入库
    if agg:
        update_oi_snapshot(st, agg)

    # 3) 价格异动检测（涨跌都推）
    spot_ok, spot_info = snapshot(SPOT_URL, "spot", st)
    fut_ok, fut_info = snapshot(FUT_URL, "futures", st)
    info = {}
    if spot_ok:
        info.update(spot_info)
    if fut_ok:
        info.update(fut_info)

    hits = []
    for key, dq in st["hist"].items():
        if not dq or len(dq) < 2:
            continue
        target = now - win_ms
        if dq[0][0] > target:
            continue
        base = dq[0][1]
        for (ts, p) in dq:
            if ts <= target:
                base = p
            else:
                break
        if base <= 0:
            continue
        chg = (dq[-1][1] / base - 1.0) * 100.0
        if abs(chg) >= UP_PCT:
            hits.append((key, chg))
    hits.sort(key=lambda h: abs(h[1]), reverse=True)

    sent = 0
    for key, chg in hits:
        if now - (st["lastAlert"].get(key) or 0) < COOLDOWN_S * 1000:
            continue
        if sent >= MAX_MAILS_PER_RUN:
            break
        st["lastAlert"][key] = now
        market, sym = key.split("|", 1)
        i = info.get(key) or {}
        coin = coin_of_symbol(sym)
        sc = min(100, int(round(abs(chg) * 6)))
        direction = "暴涨" if chg >= 0 else "暴跌"
        mcap = fetch_mcap([coin]).get(coin) if coin else None
        subject = "[Binance云端] %s 5分钟%s %.2f%%（%s）" % (sym, direction, chg, market.upper())
        body_lines = [
            "Binance Radar Cloud 云端异动提醒",
            "",
            "币种     : %s" % sym,
            "市场     : %s" % ("现货" if market == "spot" else "合约"),
            "类型     : BURST 窗口异动",
            "窗口涨幅 : %+.2f%%（%d 分钟）" % (chg, WIN_MIN),
            "最新价   : %s" % fmt_num(i.get("price", 0.0)),
            "市值     : %s" % fmt_mcap(mcap),
            "24h涨幅  : %+.2f%%" % i.get("chg24", 0.0),
            "评分     : %s 级" % level_of(sc),
            "操作建议 : %s" % advice_of(sc),
        ]
        # 附加 OI 与成交额（v1.11 新增）
        if agg and coin in agg:
            body_lines.append("")
            body_lines.extend(oi_lines_for(agg, st.get("oiHist", {}), coin))
        body_lines.extend([
            "",
            "推送时间 : %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
            "由云端服务器监控（电脑关机不影响）",
        ])
        body = "\n".join(body_lines)
        try:
            if push(subject, body):
                sent += 1
                summary.append("%s %s %+.2f%% -> 已推送" % (market.upper(), sym, chg))
                log("推送成功: %s %s %+.2f%%" % (market, sym, chg))
        except Exception as exc:
            log("推送失败: %s %s -> %s: %s" % (market, sym, type(exc).__name__, str(exc)[:160]))

    # 4) OI 异动独立监控（全部合约币种）
    oi_sent = check_oi_moves(st, agg, summary)

    # 5) 强平轮询
    liq_sent = check_liquidation(st, summary)

    save_state(st)

    watched = len(st["hist"])
    log("本轮完成: spot=%s fut=%s 聚合币种=%d 监测币种=%d 价格命中=%d 推送=%d OI异动推送=%d 强平推送=%d" % (
        "OK" if spot_ok else "FAIL",
        "OK" if fut_ok else "FAIL",
        len(agg), watched, len(hits), sent, oi_sent, liq_sent))
    if summary:
        for line in summary:
            log(line)


if __name__ == "__main__":
    main()
