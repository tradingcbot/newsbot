"""NewsBot: every run reads market news feeds, asks Claude which new headlines
move gold, silver, oil or the US indices, and emails the ones that matter.

Runs on GitHub Actions every 5 minutes. Settings come from environment
variables (GitHub secrets / variables):
  ANTHROPIC_API_KEY   secret  - your Anthropic API key
  GMAIL_USER          secret  - the Gmail address that sends the email
  GMAIL_APP_PASSWORD  secret  - a Gmail app password for GMAIL_USER
  MAIL_TO             secret  - where alerts go (defaults to GMAIL_USER)
  MODEL               variable - Claude model (default claude-opus-5-5)
  MIN_IMPACT          variable - lowest impact to email: high | medium (default medium)
  DRY_RUN=1                    - print instead of calling Claude / sending email
"""
import calendar
import hashlib
import html
import json
import os
import re
import smtplib
import sys
import time
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import feedparser

STATE_FILE = Path(os.environ.get("STATE_DIR", "state")) / "seen.json"
MODEL = os.environ.get("MODEL") or "claude-opus-5-5"
MIN_IMPACT = (os.environ.get("MIN_IMPACT") or "medium").lower()
DRY_RUN = os.environ.get("DRY_RUN") == "1"
MAX_AGE_H = 3          # ignore items older than this
KEEP_DAYS = 3          # forget seen ids after this
MAX_ITEMS = 40         # most headlines sent to Claude in one run

GN = "https://news.google.com/rss/search?hl=en-US&gl=US&ceid=US:en&q="
# (name, url, always_relevant). Feeds that fail are skipped and logged.
FEEDS = [
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", True),
    ("BLS", "https://www.bls.gov/feed/bls_latest.rss", True),
    ("EIA", "https://www.eia.gov/rss/todayinenergy.xml", True),
    ("OilPrice", "https://oilprice.com/rss/main", False),
    ("CNBC Top", "https://www.cnbc.com/id/100003114/device/rss/rss.html", False),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html", False),
    ("MarketWatch", "https://feeds.marketwatch.com/marketwatch/topstories/", False),
    ("Investing Commodities", "https://www.investing.com/rss/news_11.rss", False),
    ("Investing Stocks", "https://www.investing.com/rss/news_25.rss", False),
    ("Investing Economy", "https://www.investing.com/rss/news_14.rss", False),
    ("FXStreet", "https://www.fxstreet.com/rss/news", False),
    ("Kitco", "https://www.kitco.com/rss/KitcoNews.xml", False),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex", False),
    ("GN Oil", GN + "crude+oil+OR+OPEC+OR+Brent+OR+WTI+when:1h", False),
    ("GN Gold", GN + "gold+price+OR+silver+price+OR+bullion+when:1h", False),
    ("GN Fed", GN + "Federal+Reserve+OR+Powell+OR+FOMC+OR+inflation+when:1h", False),
    ("GN Stocks", GN + "%22S%26P+500%22+OR+Nasdaq+OR+%22Wall+Street%22+when:1h", False),
    ("GN Geo", GN + "tariffs+OR+sanctions+OR+Iran+OR+Hormuz+OR+Russia+oil+when:1h", False),
]

KEYWORDS = re.compile(
    r"\b(oil|crude|brent|wti|opec|barrel|gasoline|refiner|pipeline|inventor|eia|"
    r"gold|silver|bullion|precious|metal|"
    r"fed|fomc|powell|rate (cut|hike)|interest rate|inflation|cpi|ppi|pce|payroll|jobs|"
    r"unemployment|jobless|gdp|recession|treasur|yield|dollar|"
    r"tariff|sanction|embargo|iran|israel|gaza|hormuz|saudi|russia|ukraine|china|"
    r"war|attack|missile|strike|ceasefire|hurricane|"
    r"s&p|nasdaq|dow|stocks|wall street|earnings|nvidia|apple|microsoft|"
    r"shutdown|debt ceiling|stimulus|white house|trump)",
    re.I,
)

SYSTEM = """You screen breaking news for a retail trader who trades these CFDs:
XAUUSD (gold), XAGUSD (silver), WTI (US crude), BRENT (crude), US500 (S&P 500), NAS100 (Nasdaq 100).

You get a numbered list of new headlines. Return only the ones with fresh, market-moving
information for at least one of those instruments: central bank decisions and speeches,
economic data surprises, OPEC/supply news, inventory reports, wars, attacks, sanctions,
tariffs, big-tech earnings or guidance, and similar. Skip opinion pieces, previews, recaps,
"price rises 1%" reports with no new cause, and stories already in the "already alerted" list
(the same event from another outlet counts as already alerted).

For each item you keep give, per affected instrument: direction (bullish / bearish / mixed)
and impact (high = likely to move it well beyond a normal hour's range; medium = noticeable;
low = minor). In "why" write one or two plain sentences on the cause and effect.
Return an empty list when nothing qualifies, which is the usual case."""

SCHEMA = {
    "type": "object",
    "properties": {
        "alerts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item": {"type": "integer"},
                    "why": {"type": "string"},
                    "effects": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "symbol": {"type": "string", "enum": ["XAUUSD", "XAGUSD", "WTI", "BRENT", "US500", "NAS100"]},
                                "direction": {"type": "string", "enum": ["bullish", "bearish", "mixed"]},
                                "impact": {"type": "string", "enum": ["high", "medium", "low"]},
                            },
                            "required": ["symbol", "direction", "impact"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["item", "why", "effects"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["alerts"],
    "additionalProperties": False,
}

RANK = {"low": 0, "medium": 1, "high": 2}


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def load_state():
    try:
        return json.loads(STATE_FILE.read_text()), False
    except (OSError, ValueError):
        return {"seen": {}, "alerted": []}, True


def save_state(state):
    now = time.time()
    state["seen"] = {k: t for k, t in state["seen"].items() if now - t < KEEP_DAYS * 86400}
    state["alerted"] = [a for a in state["alerted"] if now - a["t"] < 12 * 3600]
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state))


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (NewsBot; RSS reader)"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


def clean(text, n):
    text = re.sub(r"<[^>]+>", " ", html.unescape(text or ""))
    return re.sub(r"\s+", " ", text).strip()[:n]


def collect(state):
    now, items, failed = time.time(), [], []
    for name, url, always in FEEDS:
        try:
            feed = feedparser.parse(fetch(url))
        except Exception as e:
            failed.append(f"{name} ({type(e).__name__})")
            continue
        for e in feed.entries:
            title = clean(e.get("title"), 220)
            if not title:
                continue
            key = hashlib.sha1((e.get("link") or title).encode()).hexdigest()[:16]
            tkey = hashlib.sha1(re.sub(r"\W+", "", title.lower()).encode()).hexdigest()[:16]
            if key in state["seen"] or tkey in state["seen"]:
                continue
            state["seen"][key] = state["seen"][tkey] = now
            ts = e.get("published_parsed") or e.get("updated_parsed")
            pub = calendar.timegm(ts) if ts else now
            if now - pub > MAX_AGE_H * 3600:
                continue
            summary = clean(e.get("summary"), 300)
            if not always and not KEYWORDS.search(title + " " + summary):
                continue
            items.append({"source": name, "title": title, "summary": summary,
                          "link": e.get("link", ""), "pub": pub})
    if failed:
        log("feeds failed: " + ", ".join(failed))
    items.sort(key=lambda x: -x["pub"])
    return items[:MAX_ITEMS]


def rate(items, alerted):
    import anthropic
    lines = []
    for i, it in enumerate(items, 1):
        when = time.strftime("%H:%M UTC", time.gmtime(it["pub"]))
        lines.append(f"{i}. [{it['source']}, {when}] {it['title']}" + (f" | {it['summary']}" if it["summary"] else ""))
    already = "\n".join("- " + a["title"] for a in alerted) or "(none)"
    prompt = f"Already alerted in the last 12 hours:\n{already}\n\nNew headlines:\n" + "\n".join(lines)
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=8000,
        system=SYSTEM,
        output_config={"effort": "low", "format": {"type": "json_schema", "schema": SCHEMA}},
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.stop_reason not in ("end_turn", "stop_sequence"):
        raise RuntimeError(f"Claude stopped early: {resp.stop_reason}")
    text = next(b.text for b in resp.content if b.type == "text")
    log(f"Claude: {resp.usage.input_tokens} in / {resp.usage.output_tokens} out tokens")
    return json.loads(text)["alerts"]


def build_email(alerts):
    order = sorted(alerts, key=lambda a: -max(RANK[e["impact"]] for e in a["effects"]))
    top = order[0]
    lvl = max(top["effects"], key=lambda e: RANK[e["impact"]])
    subject = f"[NewsBot] {lvl['impact'].upper()} {lvl['symbol']} {lvl['direction']}: {top['it']['title'][:90]}"
    color = {"bullish": "#1a7f37", "bearish": "#cf222e", "mixed": "#9a6700"}
    rows, plain = [], []
    for a in order:
        it = a["it"]
        eff = " ".join(
            f'<span style="color:{color[e["direction"]]};font-weight:600">{e["symbol"]} '
            f'{e["direction"]} ({e["impact"]})</span>' for e in a["effects"])
        when = time.strftime("%d %b %H:%M UTC", time.gmtime(it["pub"]))
        rows.append(
            f'<tr><td style="padding:10px;border-bottom:1px solid #ddd">'
            f'<a href="{html.escape(it["link"])}"><b>{html.escape(it["title"])}</b></a><br>'
            f'<small>{html.escape(it["source"])} &middot; {when}</small><br>{eff}<br>'
            f'{html.escape(a["why"])}</td></tr>')
        plain.append(f"{it['title']}\n{it['source']}, {when}\n"
                     + "; ".join(f"{e['symbol']} {e['direction']} ({e['impact']})" for e in a["effects"])
                     + f"\n{a['why']}\n{it['link']}\n")
    note = "Awareness only, not a trade signal: headlines are usually priced in within seconds."
    body_html = (f'<div style="font-family:Arial,sans-serif;font-size:14px">'
                 f'<table style="border-collapse:collapse;max-width:700px">{"".join(rows)}</table>'
                 f'<p style="color:#666;font-size:12px">{note}</p></div>')
    return subject, "\n".join(plain) + "\n" + note, body_html


def send(subject, text, body_html):
    user, pwd = os.environ["GMAIL_USER"], os.environ["GMAIL_APP_PASSWORD"]
    to = os.environ.get("MAIL_TO") or user
    msg = MIMEMultipart("alternative")
    msg["Subject"], msg["From"], msg["To"] = subject, user, to
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
        s.login(user, pwd)
        s.sendmail(user, [x.strip() for x in to.split(",")], msg.as_string())


def main():
    state, first_run = load_state()
    items = collect(state)
    log(f"{len(items)} new relevant headlines")
    if first_run:
        log("first run: remembering current headlines without emailing")
        save_state(state)
        return
    if not items:
        save_state(state)
        return
    if DRY_RUN:
        for it in items:
            log(f"  [{it['source']}] {it['title']}")
        save_state(state)
        return
    alerts = rate(items, state["alerted"])
    keep = []
    for a in alerts:
        if not 1 <= a["item"] <= len(items):
            continue
        a["effects"] = [e for e in a["effects"] if RANK[e["impact"]] >= RANK.get(MIN_IMPACT, 1)]
        if a["effects"]:
            a["it"] = items[a["item"] - 1]
            keep.append(a)
    log(f"{len(keep)} alerts at or above {MIN_IMPACT}")
    if keep:
        send(*build_email(keep))
        log("email sent")
        state["alerted"] += [{"title": a["it"]["title"], "t": time.time()} for a in keep]
    save_state(state)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"ERROR {type(e).__name__}: {e}")
        sys.exit(1)
