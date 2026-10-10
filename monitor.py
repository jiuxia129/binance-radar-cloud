# -*- coding: utf-8 -*-
"""
Binance Radar Cloud —— 云端全市场异动监控（GitHub Actions 定时运行）
=====================================================================
设计目标：电脑关机也能推送。本脚本运行在 GitHub Actions 的境外服务器上，
现货（官方公共镜像）与合约（官方 fapi）接口均可直连，无需代理。

检测逻辑（与本地 v1.6 后端监测器一致）：
  - 每轮拉取全市场 24h ticker（现货 + 合约）
  - 维护每币种价格时间序列，计算最近 5 分钟窗口涨幅
  - 窗口涨幅 >= 5% 触发 BURST_UP，即向 QQ 邮箱推送邮件
  - 同币种 5 分钟冷却，每轮最多 3 封，避免刷屏

配置：通过环境变量注入（GitHub Actions Secrets）：
  MAIL_USER / MAIL_PASS / MAIL_TO（QQ 邮箱 SMTP 授权码）

无第三方依赖，仅用 Python 标准库（urllib + smtplib）。
"""
import json
import os
import ssl
import sys
import time
import urllib.request
import smtplib
from email.header import Header
from email.mime.text import MIMEText

# ---------------------------------------------------------------------------
# 常量（与本地 v1.6 后端监测器保持一致）
# ---------------------------------------------------------------------------
SPOT_URL = "https://data-api.binance.vision/api/v3/ticker/24hr"   # 现货官方公共镜像
FUT_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"          # 合约官方接口
WIN_MIN = 5                # 窗口（分钟）
UP_PCT = 5.0               # 窗口涨幅阈值 %
COOLDOWN_S = 300           # 同币种 5 分钟冷却
MAX_MAILS_PER_RUN = 3      # 每轮最多发信数
HIST_MAX = 60              # 每币种最多保留采样点

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(BASE_DIR, "state.json")

MAIL_HOST = os.environ.get("MAIL_HOST", "smtp.qq.com")
MAIL_PORT = int(os.environ.get("MAIL_PORT", "465"))
MAIL_USER = os.environ.get("MAIL_USER", "").strip()
MAIL_PASS = os.environ.get("MAIL_PASS", "").strip()
MAIL_TO = os.environ.get("MAIL_TO", MAIL_USER).strip()

UA = "Mozilla/5.0 BinanceRadarCloud/1.0 (GitHub Actions)"
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,text/plain,*/*"})
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


# ---------------------------------------------------------------------------
# 状态持久化：GitHub Actions 每轮运行结束会把 state.json 提交回仓库，
# 下一轮拉取继续使用，从而跨轮维持价格序列与冷却记录。
# ---------------------------------------------------------------------------
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"hist": {}, "lastAlert": {}}


def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, STATE_PATH)


# ---------------------------------------------------------------------------
# 邮箱推送（QQ 邮箱 SMTP 465）
# ---------------------------------------------------------------------------
def send_mail(subject, body):
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


def level_of(sc):
    return "S" if sc >= 75 else ("A" if sc >= 60 else ("B" if sc >= 45 else "C"))


def advice_of(sc):
    """操作建议：能不能买、推不推荐（机械规则推算，仅供参考）"""
    if sc >= 75:
        return "强势异动，可轻仓关注；追涨需谨慎，回踩支撑位再考虑介入。"
    if sc >= 60:
        return "偏强，可关注；注意不跌破参考支撑。"
    if sc >= 45:
        return "强度一般，观望为主，暂不推荐追高。"
    return "偏弱，不推荐追高，等待趋势确认。"


# ---------------------------------------------------------------------------
# 快照：拉取全市场 ticker，更新价格序列
# ---------------------------------------------------------------------------
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
            continue                                   # 现货只监测稳定币计价对
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


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    if not (MAIL_USER and MAIL_PASS):
        log("缺少 MAIL_USER / MAIL_PASS 环境变量，跳过本次运行")
        sys.exit(1)

    st = load_state()
    now = int(time.time() * 1000)
    win_ms = WIN_MIN * 60000
    summary = []

    spot_ok, spot_info = snapshot(SPOT_URL, "spot", st)
    fut_ok, fut_info = snapshot(FUT_URL, "futures", st)
    info = {}
    if spot_ok:
        info.update(spot_info)
    if fut_ok:
        info.update(fut_info)

    # ---- 检测窗口暴涨 ----
    hits = []
    for key, dq in st["hist"].items():
        if not dq or len(dq) < 2:
            continue
        target = now - win_ms
        if dq[0][0] > target:
            continue                                  # 采样时长不足一个窗口
        base = dq[0][1]
        for (ts, p) in dq:
            if ts <= target:
                base = p
            else:
                break
        if base <= 0:
            continue
        chg = (dq[-1][1] / base - 1.0) * 100.0
        if chg >= UP_PCT:
            hits.append((key, chg))

    # ---- 冷却过滤 + 推送 ----
    sent = 0
    for key, chg in hits:
        if now - (st["lastAlert"].get(key) or 0) < COOLDOWN_S * 1000:
            continue
        if sent >= MAX_MAILS_PER_RUN:
            break
        st["lastAlert"][key] = now
        market, sym = key.split("|", 1)
        i = info.get(key) or {}
        sc = min(100, int(round(max(chg, 0.0) * 6)))
        subject = "[Binance云端] %s 5分钟暴涨 %.2f%%（%s）" % (sym, chg, market.upper())
        body = "\n".join([
            "Binance Radar Cloud 云端异动提醒",
            "",
            "币种     : %s" % sym,
            "市场     : %s" % ("现货" if market == "spot" else "合约"),
            "类型     : BURST_UP 窗口暴涨",
            "窗口涨幅 : +%.2f%%（%d 分钟）" % (chg, WIN_MIN),
            "最新价   : %s" % i.get("price", ""),
            "24h涨幅  : %s%%" % i.get("chg24", ""),
            "24h成交额: %s USDT" % i.get("quoteVol", ""),
            "评分     : %s 级" % level_of(sc),
            "操作建议 : %s" % advice_of(sc),
            "",
            "推送时间 : %s" % time.strftime("%Y-%m-%d %H:%M:%S"),
            "由云端服务器监控（电脑关机不影响）",
        ])
        try:
            send_mail(subject, body)
            sent += 1
            summary.append("%s %s +%.2f%% -> 已推送" % (market.upper(), sym, chg))
            log("推送成功: %s %s +%.2f%%" % (market, sym, chg))
        except Exception as exc:
            log("推送失败: %s %s -> %s: %s" % (market, sym, type(exc).__name__, str(exc)[:160]))

    save_state(st)

    # ---- 运行摘要 ----
    watched = len(st["hist"])
    log("本轮完成: spot=%s fut=%s 监测币种=%d 命中=%d 推送=%d" % (
        "OK" if spot_ok else "FAIL",
        "OK" if fut_ok else "FAIL",
        watched, len(hits), sent))
    if summary:
        for line in summary:
            log(line)


if __name__ == "__main__":
    main()
