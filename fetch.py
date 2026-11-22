"""Macro-Desk data collector.
Collects policy rates / government bond yields (US, KR, JP, EU), USD/KRW, gold, bitcoin.
Writes data/<id>.json and data/meta.json, data/status.json.
One failing source never stops the others.
"""
import bisect
import csv
import datetime as dt
import io
import json
import os
import re
import time
import traceback

import requests

OUT = "data"
KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime.now(KST)
TODAY = NOW.date()
ECOS_KEY = os.environ.get("ECOS_API_KEY", "").strip()
UA = {"User-Agent": "Mozilla/5.0 (Macro-Desk data bot; github.com/bluelagoon1222)"}
STATUS = []

# ---------------------------------------------------------------- series registry
# group: US KR JP EU FX ASSET SPREAD / kind: policy 2Y 5Y 10Y 30Y extra / unit: % KRW USD
REG = [
    ("us_policy", "미국 정책금리 (FF 목표 상단)", "US", "policy", "%", True),
    ("us_eff", "미국 실효 연방기금금리", "US", "extra", "%", False),
    ("us_2y", "미국 국채 2년", "US", "2Y", "%", False),
    ("us_5y", "미국 국채 5년", "US", "5Y", "%", False),
    ("us_10y", "미국 국채 10년", "US", "10Y", "%", False),
    ("us_30y", "미국 국채 30년", "US", "30Y", "%", False),
    ("kr_policy", "한국 기준금리", "KR", "policy", "%", True),
    ("kr_2y", "한국 국고채 2년", "KR", "2Y", "%", False),
    ("kr_3y", "한국 국고채 3년", "KR", "extra", "%", False),
    ("kr_5y", "한국 국고채 5년", "KR", "5Y", "%", False),
    ("kr_10y", "한국 국고채 10년", "KR", "10Y", "%", False),
    ("kr_30y", "한국 국고채 30년", "KR", "30Y", "%", False),
    ("jp_policy", "일본 정책금리 (무담보콜 익일물)", "JP", "policy", "%", False),
    ("jp_2y", "일본 국채 2년", "JP", "2Y", "%", False),
    ("jp_5y", "일본 국채 5년", "JP", "5Y", "%", False),
    ("jp_10y", "일본 국채 10년", "JP", "10Y", "%", False),
    ("jp_30y", "일본 국채 30년", "JP", "30Y", "%", False),
    ("eu_dfr", "ECB 예금금리 (DFR)", "EU", "policy", "%", True),
    ("eu_mro", "ECB 기준금리 (MRO)", "EU", "extra", "%", True),
    ("eu_mlf", "ECB 한계대출금리 (MLF)", "EU", "extra", "%", True),
    ("eu_2y", "독일 국채 2년", "EU", "2Y", "%", False),
    ("eu_5y", "독일 국채 5년", "EU", "5Y", "%", False),
    ("eu_10y", "독일 국채 10년", "EU", "10Y", "%", False),
    ("eu_30y", "독일 국채 30년", "EU", "30Y", "%", False),
    ("fx_usdkrw", "원/달러 환율", "FX", "extra", "원", False),
    ("fx_dxy", "달러인덱스 (DXY)", "FX", "extra", "pt", False),
    ("fx_usdjpy", "엔/달러 환율", "FX", "extra", "엔", False),
    ("fx_eurusd", "달러/유로 환율", "FX", "extra", "달러", False),
    ("fx_usdcny", "위안/달러 환율", "FX", "extra", "위안", False),
    ("gold", "금 (USD/oz, 선물)", "ASSET", "extra", "USD", False),
    ("silver", "은 (USD/oz, 선물)", "ASSET", "extra", "USD", False),
    ("copper", "구리 (USD/lb, 선물)", "ASSET", "extra", "USD", False),
    ("wti", "WTI 유가 (USD/배럴)", "ASSET", "extra", "USD", False),
    ("brent", "브렌트유 (USD/배럴)", "ASSET", "extra", "USD", False),
    ("btc", "비트코인 (USD)", "ASSET", "extra", "USD", False),
    ("cr_us_hy", "미국 하이일드 스프레드", "CREDIT", "extra", "%", False),
    ("cr_us_ig", "미국 투자등급 스프레드", "CREDIT", "extra", "%", False),
    ("cr_us_baa", "미국 Baa 회사채 − 10년 국채 (장기)", "CREDIT", "extra", "%", False),
    ("kr_corp_aa", "한국 회사채 AA- 3년", "CREDIT", "extra", "%", False),
    ("infl_bei10", "미국 10년 기대인플레이션 (BEI)", "INFL", "extra", "%", False),
    ("infl_real10", "미국 10년 실질금리 (TIPS)", "INFL", "extra", "%", False),
    ("vol_vix", "VIX (주식 변동성)", "VOL", "extra", "pt", False),
    ("vol_move", "MOVE (채권 변동성)", "VOL", "extra", "pt", False),
    ("vol_fg", "CNN 공포·탐욕 지수 (0~100)", "VOL", "extra", "pt", False),
    ("eq_spx", "S&P500", "EQ", "extra", "pt", False),
    ("eq_ndx100", "나스닥100", "EQ", "extra", "pt", False),
    ("eq_ndx", "나스닥 종합", "EQ", "extra", "pt", False),
    ("eq_kospi", "KOSPI", "EQ", "extra", "pt", False),
    ("eq_kosdaq", "KOSDAQ", "EQ", "extra", "pt", False),
    ("eq_nikkei", "닛케이225", "EQ", "extra", "pt", False),
    ("eq_sx5e", "유로스톡스50", "EQ", "extra", "pt", False),
    ("m_us_cpi", "미국 CPI (전년비)", "MACRO", "extra", "%", False),
    ("m_us_core", "미국 근원 CPI (전년비)", "MACRO", "extra", "%", False),
    ("m_us_pce", "미국 근원 PCE (전년비)", "MACRO", "extra", "%", False),
    ("m_us_unemp", "미국 실업률", "MACRO", "extra", "%", False),
    ("m_us_nfp", "미국 비농업고용 (전월비, 천명)", "MACRO", "extra", "천명", False),
    ("m_us_m2", "미국 M2 통화량 (전년비)", "MACRO", "extra", "%", False),
    ("m_us_fedbs", "연준 총자산 (조달러)", "MACRO", "extra", "조달러", False),
    ("m_kr_cpi", "한국 CPI (전년비)", "MACRO", "extra", "%", False),
    ("m_kr_core", "한국 근원 CPI (전년비, 농산물·석유류 제외)", "MACRO", "extra", "%", False),
    ("m_kr_exp", "한국 수출 (전년비)", "MACRO", "extra", "%", False),
]
MONTHLY = {"m_us_cpi", "m_us_core", "m_us_pce", "m_us_unemp", "m_us_nfp", "m_us_m2",
           "m_kr_cpi", "m_kr_core", "m_kr_exp"}
SPREADS = [
    ("sp_us_10_2", "미국 10년-2년", "us_10y", "us_2y", False),
    ("sp_kr_10_3", "한국 10년-3년", "kr_10y", "kr_3y", False),
    ("sp_jp_10_2", "일본 10년-2년", "jp_10y", "jp_2y", False),
    ("sp_eu_10_2", "독일 10년-2년", "eu_10y", "eu_2y", False),
    ("sp_kr_us_10", "한-미 10년 금리차", "kr_10y", "us_10y", False),
    ("sp_us_jp_10", "미-일 10년 금리차", "us_10y", "jp_10y", False),
    ("sp_us_de_10", "미-독 10년 금리차", "us_10y", "eu_10y", False),
    ("sp_kr_us_pol", "한-미 정책금리차", "kr_policy", "us_policy", True),
    ("sp_kr_credit", "한국 회사채 AA- − 국고채 3년", "kr_corp_aa", "kr_3y", False),
]
SOURCES = {}
NOTES = {}


# ---------------------------------------------------------------- helpers
def http(url, headers=None, tries=3):
    h = dict(UA)
    if headers:
        h.update(headers)
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, headers=h, timeout=90)
            if r.status_code == 200 and r.content:
                return r
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa
            last = str(e)
        time.sleep(3 * (i + 1))
    raise RuntimeError(f"{last} :: {url[:160]}")


def text_of(r):
    for enc in ("utf-8-sig", "shift_jis", "cp949", "latin-1"):
        try:
            return r.content.decode(enc)
        except UnicodeDecodeError:
            continue
    return r.text


def num(x):
    try:
        x = str(x).strip().replace(",", "")
        if x in ("", ".", "-", "NaN", "nan", "ND"):
            return None
        return float(x)
    except ValueError:
        return None


def clean(pairs):
    d = {}
    for t, v in pairs:
        if v is None or t is None:
            continue
        d[t] = v
    return sorted(d.items())


def log(src, ok, msg=""):
    STATUS.append({"source": src, "ok": ok, "msg": str(msg)[:400]})
    print(("OK  " if ok else "FAIL"), src, msg)


# ---------------------------------------------------------------- sources
def fred(sid):
    r = http(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}")
    rows = list(csv.reader(io.StringIO(text_of(r))))
    return clean((row[0], num(row[1])) for row in rows[1:] if len(row) >= 2)


def ecos_items(stat):
    url = f"https://ecos.bok.or.kr/api/StatisticItemList/{ECOS_KEY}/json/kr/1/2000/{stat}"
    j = http(url).json()
    if "StatisticItemList" not in j:
        raise RuntimeError(json.dumps(j, ensure_ascii=False)[:300])
    return j["StatisticItemList"]["row"]


def ecos_find(stat, name):
    target = name.replace(" ", "")
    rows = [r for r in ecos_items(stat) if (r.get("ITEM_NAME") or "").replace(" ", "") == target]
    if not rows:
        rows = [r for r in ecos_items(stat) if target in (r.get("ITEM_NAME") or "").replace(" ", "")]
    if not rows:
        raise RuntimeError(f"item not found: {stat} {name}")
    rows.sort(key=lambda r: {"D": 0, "M": 1}.get(r.get("CYCLE"), 9))
    return rows[0]


def ecos_series(stat, name):
    it = ecos_find(stat, name)
    cyc = it["CYCLE"]
    start = (it.get("START_TIME") or ("19900101" if cyc == "D" else "199001"))
    end = TODAY.strftime("%Y%m%d") if cyc == "D" else TODAY.strftime("%Y%m")
    out, s, step = [], 1, 10000
    while True:
        url = (f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_KEY}/json/kr/{s}/{s + step - 1}/"
               f"{stat}/{cyc}/{start}/{end}/{it['ITEM_CODE']}")
        j = http(url).json()
        if "StatisticSearch" not in j:
            if out:
                break
            raise RuntimeError(json.dumps(j, ensure_ascii=False)[:300])
        blk = j["StatisticSearch"]
        for r in blk["row"]:
            t = r["TIME"]
            iso = f"{t[:4]}-{t[4:6]}-{t[6:8]}" if len(t) == 8 else f"{t[:4]}-{t[4:6]}-01"
            out.append((iso, num(r["DATA_VALUE"])))
        total = int(blk["list_total_count"])
        s += step
        if s > total:
            break
    return clean(out), f"ECOS {stat} {it['ITEM_CODE']} ({cyc})"


ERA = {"M": 1867, "T": 1911, "S": 1925, "H": 1988, "R": 2018}


def jp_date(x):
    x = x.strip()
    m = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", x)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m = re.match(r"^([MTSHR])(\d{1,2})\.(\d{1,2})\.(\d{1,2})$", x)
    if m:
        e, y, mo, d = m.groups()
        return f"{ERA[e] + int(y):04d}-{int(mo):02d}-{int(d):02d}"
    return None


def mof_jgb():
    """Japan MOF benchmark JGB yields: English CSV, Japanese CSV as fallback."""
    sets = [("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv",
             "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"),
            ("https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv",
             "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv")]
    for urls in sets:
        cols = {}
        for url in urls:
            try:
                rows = list(csv.reader(io.StringIO(text_of(http(url)))))
            except Exception as e:
                log("MOF " + url.rsplit("/", 1)[-1], False, e)
                continue
            hi = next((i for i, r in enumerate(rows) if r and (r[0].strip().lower().startswith("date")
                                                            or "基準日" in r[0])), None)
            if hi is None:
                log("MOF parse " + url.rsplit("/", 1)[-1], False, "header not found: " + str(rows[:2])[:150])
                continue
            head = [h.strip().upper().replace("年", "Y") for h in rows[hi]]
            for r in rows[hi + 1:]:
                if not r:
                    continue
                iso = jp_date(r[0])
                if not iso:
                    continue
                for k in ("2Y", "5Y", "10Y", "30Y"):
                    if k in head and head.index(k) < len(r):
                        v = num(r[head.index(k)])
                        if v is not None:
                            cols.setdefault(k, {})[iso] = v
        if cols.get("10Y"):
            return {k: sorted(v.items()) for k, v in cols.items()}
    return {}


def boj_call():
    """BOJ uncollateralized overnight call rate (daily). Tries BOJ API, falls back to FRED monthly."""
    url = ("https://www.stat-search.boj.or.jp/api/v1/getDataCode?format=json&lang=en"
           "&db=FM01&code=STRDCLUCON&startDate=199801&endDate=" + TODAY.strftime("%Y%m"))
    try:
        j = http(url).json()
        pairs = []

        def walk(o):
            if isinstance(o, dict):
                dates = o.get("SURVEY_DATES") or o.get("DATES")
                vals = o.get("VALUES")
                if isinstance(dates, list) and isinstance(vals, list) and len(dates) == len(vals):
                    for d, v in zip(dates, vals):
                        d = str(d)
                        if len(d) == 8:
                            pairs.append((f"{d[:4]}-{d[4:6]}-{d[6:]}", num(v)))
                for x in o.values():
                    walk(x)
            elif isinstance(o, list):
                for x in o:
                    walk(x)
        walk(j)
        pairs = clean(pairs)
        if len(pairs) > 100:
            return pairs, "BOJ API FM01 STRDCLUCON (daily)"
        raise RuntimeError("unrecognized BOJ response: " + json.dumps(j)[:250])
    except Exception as e:
        log("BOJ API", False, e)
    return fred("IRSTCI01JPM156N"), "FRED IRSTCI01JPM156N (monthly, BOJ API fallback)"


def ecb(key):
    r = http(f"https://data-api.ecb.europa.eu/service/data/{key}?format=csvdata")
    rd = csv.DictReader(io.StringIO(text_of(r)))
    return clean((row.get("TIME_PERIOD"), num(row.get("OBS_VALUE"))) for row in rd)


def bund(m):
    key = f"BBSIS/D.I.ZST.ZI.EUR.S1311.B.A604.R{m:02d}XX.R.A.A._Z._Z.A"
    r = http(f"https://api.statistiken.bundesbank.de/rest/data/{key}",
             headers={"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"})
    rd = csv.DictReader(io.StringIO(text_of(r)))
    pairs = clean((row.get("TIME_PERIOD"), num(row.get("OBS_VALUE"))) for row in rd)
    if pairs:
        return pairs
    r = http(f"https://api.statistiken.bundesbank.de/rest/download/{key.replace('/', '/', 1)}?format=csv&lang=en")
    out = []
    for line in text_of(r).splitlines():
        parts = re.split(r"[;,]", line, maxsplit=1) if ";" not in line else line.split(";")
        if len(parts) >= 2 and re.match(r"^\d{4}-\d{2}-\d{2}$", parts[0].strip()):
            out.append((parts[0].strip(), num(parts[1].strip().replace(",", ".") if ";" in line else parts[1])))
    pairs = clean(out)
    if not pairs:
        raise RuntimeError("empty bundesbank response: " + text_of(r)[:150].replace("\n", " | "))
    return pairs


def yahoo(sym):
    r = http(f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}?period1=0&period2={int(time.time())}"
             f"&interval=1d&events=history")
    res = r.json()["chart"]["result"][0]
    off = res.get("meta", {}).get("gmtoffset", 0) or 0
    ts = res["timestamp"]
    cl = res["indicators"]["quote"][0]["close"]
    return clean((dt.datetime.utcfromtimestamp(t + off).date().isoformat(), c) for t, c in zip(ts, cl))


def yoy(pairs, lag=12):
    out = []
    for i in range(lag, len(pairs)):
        a, b = pairs[i][1], pairs[i - lag][1]
        if a is not None and b:
            out.append((pairs[i][0], round((a / b - 1) * 100, 3)))
    return out


def mdiff(pairs):
    return [(pairs[i][0], pairs[i][1] - pairs[i - 1][1]) for i in range(1, len(pairs))]


def monthly_last(pairs):
    """weekly/daily -> last obs per month (dated YYYY-MM-01)."""
    d = {}
    for t, v in pairs:
        d[t[:7] + "-01"] = v
    return sorted(d.items())


def kr_exports():
    """Find ECOS customs-based monthly export table automatically."""
    j = http(f"https://ecos.bok.or.kr/api/StatisticTableList/{ECOS_KEY}/json/kr/1/10000").json()
    tabs = [t for t in j.get("StatisticTableList", {}).get("row", [])
            if "수출" in (t.get("STAT_NAME") or "") and t.get("SRCH_YN") == "Y"]
    tried = []
    for t in tabs:
        try:
            items = ecos_items(t["STAT_CODE"])
        except Exception:
            continue
        for it in items:
            nm = (it.get("ITEM_NAME") or "").replace(" ", "")
            if it.get("CYCLE") != "M" or not nm.startswith("수출") or len(nm) > 12:
                continue
            tried.append(f"{t['STAT_CODE']} {t['STAT_NAME']} / {it['ITEM_CODE']} {it['ITEM_NAME']}")
            try:
                url = (f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_KEY}/json/kr/1/2000/{t['STAT_CODE']}/M/"
                       f"{it.get('START_TIME') or '196001'}/{TODAY.strftime('%Y%m')}/{it['ITEM_CODE']}")
                rows = http(url).json()["StatisticSearch"]["row"]
                p = clean((f"{r['TIME'][:4]}-{r['TIME'][4:6]}-01", num(r["DATA_VALUE"])) for r in rows)
                if len(p) > 120 and p[-1][0] >= (TODAY - dt.timedelta(days=120)).isoformat():
                    put("m_kr_exp", yoy(p), f"ECOS {t['STAT_CODE']} {t['STAT_NAME']} / {it['ITEM_NAME']} YoY")
                    log("kr_exports candidates", True, " || ".join(tried))
                    return
            except Exception:
                continue
    log("kr_exports candidates", False, " || ".join(tried) or "no table found")
    put("m_kr_exp", yoy(fred("XTEXVA01KRM667S")), "FRED XTEXVA01KRM667S YoY (fallback)")


def cnn_fg():
    """CNN Fear & Greed (unofficial endpoint). History is merged with our saved file so it keeps growing."""
    old = {}
    try:
        with open(f"{OUT}/vol_fg.json", encoding="utf-8") as f:
            j = json.load(f)
        old = dict(zip(j["t"], j["v"]))
    except Exception:
        pass
    hdr = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
           "Accept": "application/json, text/plain, */*", "Referer": "https://edition.cnn.com/",
           "Origin": "https://edition.cnn.com"}
    j, errs = None, []
    for tail in ("/2020-07-15", "/2021-01-01", (TODAY - dt.timedelta(days=360)).strftime("/%Y-%m-%d"), ""):
        try:
            j = http("https://production.dataviz.cnn.io/index/fearandgreed/graphdata" + tail, headers=hdr, tries=2).json()
            if j.get("fear_and_greed_historical", {}).get("data"):
                break
        except Exception as e:
            errs.append(f"{tail or 'nodate'}: {e}")
            j = None
    if not j:
        raise RuntimeError(" | ".join(errs))
    for p in j.get("fear_and_greed_historical", {}).get("data", []):
        d = dt.datetime.utcfromtimestamp(p["x"] / 1000).date().isoformat()
        old[d] = round(float(p["y"]), 1)
    cur = j.get("fear_and_greed", {})
    if cur.get("score") is not None and cur.get("timestamp"):
        old[cur["timestamp"][:10]] = round(float(cur["score"]), 1)
    return sorted(old.items())


# ---------------------------------------------------------------- collect
DATA = {}


def put(sid, pairs, src):
    if pairs and len(pairs) > 5:
        DATA[sid] = pairs
        SOURCES[sid] = src
        log(sid, True, f"{src} | {pairs[0][0]}~{pairs[-1][0]} n={len(pairs)}")
    else:
        log(sid, False, f"{src} empty")


def attempt(sid, fn):
    try:
        fn()
    except Exception as e:
        log(sid, False, f"{e}")
        traceback.print_exc()


def collect():
    # US
    def us_pol():
        old = fred("DFEDTAR")
        new = fred("DFEDTARU")
        cut = new[0][0] if new else "9999"
        put("us_policy", clean([p for p in old if p[0] < cut] + new), "FRED DFEDTAR+DFEDTARU")
    attempt("us_policy", us_pol)
    for sid, fid in (("us_eff", "DFF"), ("us_2y", "DGS2"), ("us_5y", "DGS5"),
                     ("us_10y", "DGS10"), ("us_30y", "DGS30")):
        attempt(sid, lambda sid=sid, fid=fid: put(sid, fred(fid), f"FRED {fid}"))

    # KR (ECOS)
    if not ECOS_KEY:
        log("ECOS", False, "ECOS_API_KEY secret missing")
    else:
        for sid, stat, name in (("kr_policy", "722Y001", "한국은행 기준금리"),
                                ("kr_2y", "817Y002", "국고채(2년)"),
                                ("kr_3y", "817Y002", "국고채(3년)"),
                                ("kr_5y", "817Y002", "국고채(5년)"),
                                ("kr_10y", "817Y002", "국고채(10년)"),
                                ("kr_30y", "817Y002", "국고채(30년)"),
                                ("fx_usdkrw", "731Y001", "원/미국달러(매매기준율)")):
            attempt(sid, lambda sid=sid, stat=stat, name=name: put(sid, *ecos_series(stat, name)))
    if "fx_usdkrw" not in DATA:
        attempt("fx_usdkrw", lambda: put("fx_usdkrw", fred("DEXKOUS"), "FRED DEXKOUS (fallback)"))

    # JP
    def jp():
        c = mof_jgb()
        for k in ("2Y", "5Y", "10Y", "30Y"):
            put("jp_" + k.lower(), c.get(k, []), f"MOF JGB {k}")
    attempt("jp_jgb", jp)
    attempt("jp_policy", lambda: put("jp_policy", *boj_call()))

    # EU policy
    for sid, code in (("eu_dfr", "DFR"), ("eu_mro", "MRR_FR"), ("eu_mlf", "MLFR")):
        def f(sid=sid, code=code):
            try:
                put(sid, ecb(f"FM/D.U2.EUR.4F.KR.{code}.LEV"), f"ECB FM.D.U2.EUR.4F.KR.{code}.LEV")
            except Exception as e:
                log(sid + " D", False, e)
                put(sid, ecb(f"FM/B.U2.EUR.4F.KR.{code}.LEV"), f"ECB FM.B.U2.EUR.4F.KR.{code}.LEV")
        attempt(sid, f)

    # EU bund (fallback: euro area AAA curve)
    for m in (2, 5, 10, 30):
        sid = f"eu_{m}y"

        def f(m=m, sid=sid):
            try:
                put(sid, bund(m), f"Bundesbank BBSIS R{m:02d}XX")
            except Exception as e:
                log(sid + " bundesbank", False, e)
                put(sid, ecb(f"YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{m}Y"), f"ECB AAA SR_{m}Y (fallback)")
                NOTES[sid] = "유로존 AAA 국채 곡선(대체)"
        attempt(sid, f)

    # FX / commodities / credit / inflation / vol (FRED)
    for sid, fid in (("fx_usdjpy", "DEXJPUS"), ("fx_eurusd", "DEXUSEU"), ("fx_usdcny", "DEXCHUS"),
                     ("wti", "DCOILWTICO"), ("brent", "DCOILBRENTEU"),
                     ("cr_us_hy", "BAMLH0A0HYM2"), ("cr_us_ig", "BAMLC0A0CM"), ("cr_us_baa", "BAA10Y"),
                     ("infl_bei10", "T10YIE"), ("infl_real10", "DFII10"), ("vol_vix", "VIXCLS")):
        attempt(sid, lambda sid=sid, fid=fid: put(sid, fred(fid), f"FRED {fid}"))
    # Yahoo
    for sid, sym in (("fx_dxy", "DX-Y.NYB"), ("silver", "SI=F"), ("copper", "HG=F"), ("vol_move", "^MOVE"),
                     ("eq_spx", "^GSPC"), ("eq_ndx100", "^NDX"), ("eq_ndx", "^IXIC"), ("eq_kospi", "^KS11"),
                     ("eq_kosdaq", "^KQ11"),
                     ("eq_nikkei", "^N225"), ("eq_sx5e", "^STOXX50E")):
        attempt(sid, lambda sid=sid, sym=sym: put(sid, yahoo(sym), f"Yahoo {sym}"))
    def fg():
        try:
            put("vol_fg", cnn_fg(), "CNN Fear & Greed")
        except Exception as e:
            # keep previously saved history if CNN blocks this run
            log("vol_fg cnn", False, e)
            with open(f"{OUT}/vol_fg.json", encoding="utf-8") as f:
                j = json.load(f)
            put("vol_fg", list(zip(j["t"], j["v"])), "CNN Fear & Greed (이전 저장분)")
    attempt("vol_fg", fg)
    # US monthly macro
    for sid, fid, fn, lab in (("m_us_cpi", "CPIAUCSL", yoy, "YoY"), ("m_us_core", "CPILFESL", yoy, "YoY"),
                              ("m_us_pce", "PCEPILFE", yoy, "YoY"), ("m_us_unemp", "UNRATE", None, ""),
                              ("m_us_nfp", "PAYEMS", mdiff, "MoM"), ("m_us_m2", "M2SL", yoy, "YoY")):
        attempt(sid, lambda sid=sid, fid=fid, fn=fn, lab=lab:
                put(sid, fn(fred(fid)) if fn else fred(fid), f"FRED {fid} {lab}".strip()))
    attempt("m_us_fedbs", lambda: put("m_us_fedbs", [(t, v / 1e6) for t, v in fred("WALCL")],
                                      "FRED WALCL (weekly, 조달러)"))
    # KR monthly macro + corp bond
    if ECOS_KEY:
        attempt("kr_corp_aa", lambda: put("kr_corp_aa", *ecos_series("817Y002", "회사채(3년, AA-)")))

        def kr_m(sid, stat, name, fb):
            try:
                p, src = ecos_series(stat, name)
                put(sid, yoy(monthly_last(p)), src + " YoY")
            except Exception as e:
                log(sid + " ecos", False, e)
                if not fb:
                    raise
                put(sid, yoy(fred(fb)), f"FRED {fb} YoY (fallback)")
        attempt("m_kr_cpi", lambda: kr_m("m_kr_cpi", "901Y009", "총지수", "KORCPIALLMINMEI"))
        attempt("m_kr_core", lambda: kr_m("m_kr_core", "901Y010", "농산물및석유류제외지수", None))
        attempt("m_kr_exp", kr_exports)
    else:
        attempt("m_kr_cpi", lambda: put("m_kr_cpi", yoy(fred("KORCPIALLMINMEI")), "FRED KORCPIALLMINMEI YoY"))

    # Assets
    def gold():
        try:
            put("gold", yahoo("GC=F"), "Yahoo GC=F")
        except Exception as e:
            log("gold yahoo", False, e)
            r = http("https://stooq.com/q/d/l/?s=xauusd&i=d")
            rows = list(csv.reader(io.StringIO(text_of(r))))
            put("gold", clean((x[0], num(x[4])) for x in rows[1:] if len(x) > 4), "Stooq XAUUSD (fallback)")
    attempt("gold", gold)

    def btc():
        try:
            put("btc", yahoo("BTC-USD"), "Yahoo BTC-USD")
        except Exception as e:
            log("btc yahoo", False, e)
            put("btc", fred("CBBTCUSD"), "FRED CBBTCUSD (fallback)")
    attempt("btc", btc)


# ---------------------------------------------------------------- spreads & stats
def spread(a, b, ffill):
    da, db = dict(DATA[a]), dict(DATA[b])
    if not ffill:
        return [(t, round(da[t] - db[t], 4)) for t in sorted(set(da) & set(db))]
    out, va, vb = [], None, None
    for t in sorted(set(da) | set(db)):
        va = da.get(t, va)
        vb = db.get(t, vb)
        if va is not None and vb is not None:
            out.append((t, round(va - vb, 4)))
    return out


def stats(pairs, step):
    t = [p[0] for p in pairs]
    v = [p[1] for p in pairs]
    ref = TODAY if step else dt.date.fromisoformat(t[-1])

    def at(d):
        i = bisect.bisect_right(t, d.isoformat()) - 1
        return v[i] if i >= 0 else None
    if step:
        last = at(TODAY)
        d1 = at(TODAY - dt.timedelta(days=1))
    else:
        last = v[-1]
        d1 = v[-2] if len(v) > 1 else None
    i52 = bisect.bisect_left(t, (ref - dt.timedelta(days=365)).isoformat())
    w = v[i52:] or v[-1:]
    return {
        "last": last, "date": t[-1],
        "chg": {"1D": d1, "1W": at(ref - dt.timedelta(days=7)), "1M": at(ref - dt.timedelta(days=30)),
                "YTD": at(dt.date(ref.year - 1, 12, 31)), "1Y": at(ref - dt.timedelta(days=365))},
        "hi52": max(w), "lo52": min(w),
        "spark": v[-66:],
    }


def rnd(unit, x):
    if x is None:
        return None
    if unit in ("%", "조달러"):
        return round(x, 3)
    return round(x, 3) if unit in ("엔", "달러", "위안") else (round(x, 0) if abs(x) > 10000 else round(x, 2))


def main():
    os.makedirs(OUT, exist_ok=True)
    collect()
    reg = list(REG)
    for sid, name, a, b, ff in SPREADS:
        if a in DATA and b in DATA:
            p = spread(a, b, ff)
            if p:
                DATA[sid] = p
                SOURCES[sid] = f"계산: {a} - {b}"
        reg.append((sid, name, "SPREAD", "extra", "%", ff))

    meta = []
    for sid, name, grp, kind, unit, step in reg:
        m = {"id": sid, "name": name, "group": grp, "kind": kind, "unit": unit, "step": step,
             "freq": "M" if sid in MONTHLY else ("W" if sid == "m_us_fedbs" else "D"),
             "ok": sid in DATA, "source": SOURCES.get(sid, ""), "note": NOTES.get(sid, "")}
        if sid in DATA:
            pairs = [(d, rnd(unit, x)) for d, x in DATA[sid]]
            with open(f"{OUT}/{sid}.json", "w", encoding="utf-8") as f:
                json.dump({"id": sid, "t": [p[0] for p in pairs], "v": [p[1] for p in pairs]}, f,
                          separators=(",", ":"))
            st = stats(pairs, step)
            m.update({"start": pairs[0][0], "end": pairs[-1][0], "n": len(pairs)})
            m.update(st)
        meta.append(m)

    with open(f"{OUT}/meta.json", "w", encoding="utf-8") as f:
        json.dump({"updated": NOW.strftime("%Y-%m-%d %H:%M KST"), "series": meta}, f,
                  ensure_ascii=False, separators=(",", ":"))
    with open(f"{OUT}/status.json", "w", encoding="utf-8") as f:
        json.dump({"updated": NOW.isoformat(), "log": STATUS}, f, ensure_ascii=False, indent=1)
    ok = sum(1 for m in meta if m["ok"])
    print(f"done: {ok}/{len(meta)} series")


if __name__ == "__main__":
    main()
