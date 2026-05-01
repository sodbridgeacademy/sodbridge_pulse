import requests
from datetime import datetime
from pytrends.request import TrendReq
import feedparser
import time
import csv
import os

# -----------------------------
# CONFIG
# -----------------------------
API_KEY = "4942678edb4dcf53c8358ed070161766"
CSV_FILE = "data/sodbridge_pulse_data.csv"

LOCATION = {
    "city": "Lagos",
    "country": "Nigeria",
    "lat": 6.5244,
    "lon": 3.3792
}

TREND_CACHE = {"data": {}, "timestamp": 0}
CACHE_TTL = 6 * 60 * 60

LAST_FX = None  # fallback storage


# -----------------------------
# FX
# -----------------------------
def get_fx():
    global LAST_FX

    try:
        url = f"http://api.exchangerate.host/live?access_key={API_KEY}&currencies=NGN&source=USD"
        data = requests.get(url, timeout=10).json()

        if data.get("success") and "quotes" in data:
            fx = data["quotes"].get("USDNGN")
            LAST_FX = fx
            return fx

        return LAST_FX

    except:
        return LAST_FX


# -----------------------------
# WEATHER
# -----------------------------
def get_weather():
    try:
        url = "https://api.open-meteo.com/v1/forecast?latitude=6.5244&longitude=3.3792&daily=precipitation_probability_max&timezone=Africa%2FLagos"
        data = requests.get(url).json()
        return data["daily"]["precipitation_probability_max"][0]
    except:
        return None


# -----------------------------
# TRENDS (CACHED)
# -----------------------------
def get_trends():
    global TREND_CACHE

    if time.time() - TREND_CACHE["timestamp"] < CACHE_TTL:
        return TREND_CACHE["data"]

    try:
        pytrends = TrendReq(hl='en-US', tz=360)

        kw_list = [
            "fuel price Nigeria",
            "dollar rate Nigeria",
            "food price Nigeria",
            "jobs in Nigeria"
        ]

        pytrends.build_payload(kw_list, timeframe='now 1-d', geo='NG')
        data = pytrends.interest_over_time()

        result = {} if data.empty else data.tail(1).to_dict("records")[0]

        TREND_CACHE = {"data": result, "timestamp": time.time()}
        return result

    except:
        return TREND_CACHE["data"]


# -----------------------------
# NEWS
# -----------------------------
def get_news():
    feeds = {
        "punch": "https://punchng.com/feed/",
        "vanguard": "https://www.vanguardngr.com/feed/",
        "guardian": "https://guardian.ng/feed/"
    }

    news = []

    for source, url in feeds.items():
        feed = feedparser.parse(url)
        for entry in feed.entries[:3]:
            news.append({"source": source, "title": entry.title})

    return news


# -----------------------------
# JOBS
# -----------------------------
def get_jobs():
    try:
        data = requests.get(
            "https://remoteok.com/api",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        ).json()

        jobs = []

        for job in data[1:6]:
            jobs.append({
                "title": job.get("position", ""),
                "company": job.get("company", ""),
                "location": job.get("location", "")
            })

        return jobs

    except:
        return []


# -----------------------------
# SPORTS
# -----------------------------
def get_sports():
    feed = feedparser.parse("https://feeds.bbci.co.uk/sport/rss.xml")

    return [
        {"title": e.title, "source": "bbc_sport"}
        for e in feed.entries[:5]
    ]


# -----------------------------
# PULSE INDEX (CORE)
# -----------------------------
def compute_pulse(fx, rain, news, jobs, trends):
    score = 0

    # FX
    if fx:
        if fx > 1500:
            score += 30
        elif fx > 1350:
            score += 15

    # WEATHER
    if rain and rain > 60:
        score += 15

    # NEWS
    text = " ".join([n["title"].lower() for n in news])
    if "security" in text: score += 10
    if "inflation" in text or "fuel" in text: score += 10

    # JOBS
    if len(jobs) > 5:
        score += 10

    # TRENDS
    if trends:
        active = sum(1 for v in trends.values() if isinstance(v, (int, float)) and v > 0)
        score += active * 3

    return min(score, 100)


# -----------------------------
# SYSTEM STATE
# -----------------------------
def get_system_state(pulse):
    if pulse <= 10:
        return "stable"
    elif pulse <= 18:
        return "moderate_pressure"
    elif pulse <= 25:
        return "high_pressure"
    else:
        return "crisis_signal"


# -----------------------------
# AI WEIGHTED AVERAGE
# -----------------------------
def compute_ai_pulse(fx, rain, news, jobs, trends):
    score = 0

    # ---------------- FX (0–40)
    if fx:
        if fx > 1500:
            score += 40
        elif fx > 1400:
            score += 25
        elif fx > 1300:
            score += 15
        else:
            score += 5

    # ---------------- NEWS (0–20)
    news_text = " ".join([n["title"].lower() for n in news])
    if any(w in news_text for w in ["security", "violence", "attack"]):
        score += 20
    elif any(w in news_text for w in ["inflation", "fuel", "crisis"]):
        score += 15
    else:
        score += 5

    # ---------------- JOBS (0–15)
    if jobs:
        score += min(len(jobs) * 3, 15)

    # ---------------- WEATHER (0–10)
    if rain:
        if rain > 70:
            score += 10
        elif rain > 40:
            score += 5

    # ---------------- TRENDS (0–15)
    if trends and not trends.get("isPartial"):
        for k, v in trends.items():
            if v > 20:
                score += 5

    return min(score, 100)

# -----------------------------
# CSV LOGGER
# -----------------------------
def save_to_csv(row):
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)


# -----------------------------
# GET INSIGHTS
# -----------------------------
def generate_insights(fx, rain, trends, news, jobs, sports):
    insights = []

    if fx and fx > 1500:
        insights.append("🔴 High FX pressure")

    if rain and rain > 60:
        insights.append("🌧️ Heavy rainfall risk")

    news_text = " ".join([n["title"].lower() for n in news])

    if "security" in news_text:
        insights.append("🚨 Security concerns detected")

    if "fuel" in news_text:
        insights.append("⛽ Fuel-related pressure detected")

    if jobs:
        insights.append("💼 Job market activity detected")

    if sports:
        insights.append("⚽ High sports engagement")

    if not insights:
        insights.append("🟢 Stable conditions")

    return insights
# -----------------------------
# MAIN RUN
# -----------------------------
def run_intelligence(run_type="hourly"):
    print("\n📊 SODBRIDGE PULSE SNAPSHOT")
    print("Date:", datetime.now().isoformat())

    fx = get_fx()
    rain = get_weather()
    trends = get_trends()
    news = get_news()
    jobs = get_jobs()
    sports = get_sports()

    pulse = compute_pulse(fx, rain, news, jobs, trends)

    print("\n💱 FX:", fx)
    print("🌧️ Rain:", rain)
    print("📊 Pulse Index:", pulse)

    # ✅ generate insights properly (LIST)
    insights_list = generate_insights(fx, rain, trends, news, jobs, sports)

    # -----------------------------
    # BUILD CSV ROW (ONLY ONCE)
    # -----------------------------
    row = {
        "timestamp": datetime.now().isoformat(),
        "run_type": run_type,
        "location_city": LOCATION["city"],
        "location_country": LOCATION["country"],
        "fx_value": fx,
        "rain_probability": rain,
        "trends_json": str(trends),
        "news_json": str(news),
        "jobs_json": str(jobs),
        "sports_json": str(sports),
        "insights_json": " | ".join(insights_list), 
        "pulse_index": pulse,
        "system_state": get_system_state(pulse),
        "ai_pulse_index": compute_ai_pulse(fx, rain, news, jobs, trends)
    }

    save_to_csv(row)

    print("\n✅ Saved to CSV")


# -----------------------------
# ENTRY
# -----------------------------
if __name__ == "__main__":
    run_intelligence()