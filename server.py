"""
server.py — Web server + API
"""

import os
import threading
import re
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from scraper import get_all_articles, get_connection, setup_database, scrape_all_feeds, USE_POSTGRES, KEYWORDS
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder=".")
CORS(app)

TOPIC_META = {
    "Reproduktive Rechte":    {"icon": "🩺", "color": "#E91E8C"},
    "Lohnlücke & Wirtschaft": {"icon": "💰", "color": "#FFA52C"},
    "LGBTQIA+":               {"icon": "🏳️‍🌈", "color": "#7B2FBE"},
    "Migration & Asyl":       {"icon": "🌍", "color": "#00BCD4"},
    "Menschenrechte":         {"icon": "⚖️", "color": "#FF0018"},
    "Gesundheit & Medizin":   {"icon": "🏥", "color": "#4CAF50"},
    "Recht & Politik":        {"icon": "📜", "color": "#9C27B0"},
    "Politik & Regierung":    {"icon": "🏛️", "color": "#3F51B5"},
    "Kultur & Medien":        {"icon": "🎭", "color": "#FF9800"},
    "Sport":                  {"icon": "⚽", "color": "#008018"},
    "Gewalt & Sicherheit":    {"icon": "🛡️", "color": "#F44336"},
    "Arbeit & Wirtschaft":    {"icon": "💼", "color": "#607D8B"},
}


def setup_subscribers():
    """Create subscribers table if it doesn't exist."""
    conn = get_connection()
    cursor = conn.cursor()
    if USE_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                active BOOLEAN DEFAULT TRUE
            )
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                subscribed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1
            )
        """)
    conn.commit()
    conn.close()


def resolve_time_range(label):
    now = datetime.now()
    if label == "today":
        return now.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "this_week":
        start = now - timedelta(days=now.weekday())
        return start.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "last_week":
        start = now - timedelta(days=now.weekday() + 7)
        return start.replace(hour=0, minute=0, second=0).isoformat()
    elif label == "last_month":
        return (now - timedelta(days=30)).isoformat()
    elif label == "last_year":
        return (now - timedelta(days=365)).isoformat()
    return None


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/articles")
def articles():
    category   = request.args.get("category")
    source     = request.args.get("source")
    country    = request.args.get("country")
    search     = request.args.get("search")
    topic      = request.args.get("topic")
    time_label = request.args.get("time")
    date_from  = request.args.get("date_from")
    date_to    = request.args.get("date_to")
    limit      = int(request.args.get("limit", 200))
    time_range = date_from if date_from else (resolve_time_range(time_label) if time_label else None)
    results = get_all_articles(
        category=category, source=source, search=search,
        topic=topic, country=country, time_range=time_range,
        date_to=date_to, limit=limit
    )
    return jsonify(results)


@app.route("/api/sources")
def sources():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT source FROM articles ORDER BY source")
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/countries")
def countries():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT country FROM articles WHERE country != '' ORDER BY country")
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/topics")
def topics():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    result = []
    for topic_name, meta in TOPIC_META.items():
        cursor.execute(
            f"SELECT COUNT(*) FROM articles WHERE topics LIKE {ph}",
            [f"%{topic_name}%"]
        )
        count = cursor.fetchone()[0]
        result.append({
            "name": topic_name,
            "count": count,
            "icon": meta["icon"],
            "color": meta["color"],
        })
    conn.close()
    result.sort(key=lambda x: x["count"], reverse=True)
    return jsonify(result)


@app.route("/api/stats")
def stats():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    cursor.execute("SELECT COUNT(*) FROM articles")
    total = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM articles WHERE tags LIKE {ph}", ['%lgbtqia+%'])
    lgbtq = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM articles WHERE tags LIKE {ph}", ['%women%'])
    women = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(scraped_at) FROM articles")
    last_scraped = cursor.fetchone()[0]
    conn.close()
    return jsonify({
        "total": total,
        "lgbtqia_plus": lgbtq,
        "women": women,
        "last_scraped": last_scraped
    })


@app.route("/api/analytics/sources")
def analytics_sources():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    cursor.execute(
        f"SELECT source, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY source ORDER BY count DESC",
        [seven_days_ago]
    )
    result = [{"source": row[0], "count": row[1]} for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/analytics/daily")
def analytics_daily():
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    ninety_days_ago = (datetime.now() - timedelta(days=90)).isoformat()
    if USE_POSTGRES:
        cursor.execute(
            f"SELECT DATE(scraped_at::timestamp) as day, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY day ORDER BY day",
            [ninety_days_ago]
        )
    else:
        cursor.execute(
            f"SELECT DATE(scraped_at) as day, COUNT(*) as count FROM articles WHERE scraped_at >= {ph} GROUP BY day ORDER BY day",
            [ninety_days_ago]
        )
    result = [{"date": str(row[0]), "count": row[1]} for row in cursor.fetchall()]
    conn.close()
    return jsonify(result)


@app.route("/api/analytics/keywords")
def analytics_keywords():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT title, summary FROM articles")
    rows = cursor.fetchall()
    conn.close()
    keyword_counts = {}
    for row in rows:
        text = (row[0] + " " + row[1]).lower()
        for kw in KEYWORDS:
            if kw in text and len(kw) > 4:
                keyword_counts[kw] = keyword_counts.get(kw, 0) + 1
    result = [
        {"keyword": k, "count": v}
        for k, v in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    ]
    return jsonify(result)


# ── Newsletter ────────────────────────────────────────────────────────────────

@app.route("/api/newsletter/subscribe", methods=["POST"])
def newsletter_subscribe():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()

    if not email or "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"error": "Bitte gib eine gültige E-Mail-Adresse ein."}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()
        ph = "%s" if USE_POSTGRES else "?"
        cursor.execute(
            f"INSERT INTO subscribers (email) VALUES ({ph})",
            [email]
        )
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": "Erfolgreich angemeldet!"})
    except Exception as e:
        err = str(e).lower()
        if "unique" in err or "duplicate" in err:
            return jsonify({"error": "Diese E-Mail ist bereits angemeldet."}), 409
        return jsonify({"error": "Fehler beim Speichern. Bitte versuch es später."}), 500


@app.route("/api/newsletter/subscribers")
def newsletter_subscribers():
    """Admin endpoint — returns subscriber count and list."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE active = 1")
    count = cursor.fetchone()[0]
    conn.close()
    return jsonify({"count": count})


# ── Scrape ────────────────────────────────────────────────────────────────────

@app.route("/api/scrape")
def trigger_scrape():
    def do_scrape():
        scrape_all_feeds()
    thread = threading.Thread(target=do_scrape)
    thread.start()
    return jsonify({"status": "Scraping gestartet!"})


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


def startup():
    setup_database()
    setup_subscribers()

    def initial_scrape():
        try:
            print("Running initial scrape...", flush=True)
            scrape_all_feeds()
            print("Initial scrape complete!", flush=True)
        except Exception as e:
            print(f"Initial scrape failed: {e}", flush=True)

    thread = threading.Thread(target=initial_scrape)
    thread.start()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: scrape_all_feeds(),
        'interval', hours=12,
        id='scheduled_scrape'
    )
    scheduler.start()
    print("Scheduler aktiv - scrapet alle 12 Stunden.")


startup()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
