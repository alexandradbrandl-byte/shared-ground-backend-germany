"""
server.py — Web server + API
"""

import os
import re
import time
import threading
import hashlib
import requests as http_requests
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from scraper import get_all_articles, get_connection, setup_database, scrape_all_feeds, USE_POSTGRES, KEYWORDS
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder=".")
CORS(app)

BREVO_API_KEY     = os.environ.get("BREVO_API_KEY", "")
BREVO_SENDER      = os.environ.get("BREVO_SENDER_EMAIL", "alexandra.d.brandl@gmail.com")
BREVO_SENDER_NAME = os.environ.get("BREVO_SENDER_NAME", "shared ground")

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

GERMAN_MONTHS = {
    1: "Januar", 2: "Februar", 3: "März", 4: "April",
    5: "Mai", 6: "Juni", 7: "Juli", 8: "August",
    9: "September", 10: "Oktober", 11: "November", 12: "Dezember"
}


def format_german_date(dt):
    return f"{dt.day}. {GERMAN_MONTHS[dt.month]} {dt.year}"


def setup_subscribers():
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


def make_unsubscribe_token(email):
    secret = BREVO_API_KEY[:8] if BREVO_API_KEY else "sg-secret"
    return hashlib.sha256(f"{email}{secret}".encode()).hexdigest()[:16]


def build_newsletter_html(articles, unsubscribe_url):
    date_str = format_german_date(datetime.now())
    rows = ""
    for i, a in enumerate(articles, 1):
        source  = a.get("source", "")
        title   = a.get("title", "")
        link    = a.get("link", "#")
        summary = (a.get("summary") or "")[:220]
        if summary and not summary.endswith("…"):
            summary += "…"
        summary_html = (
            f'<p style="font-size:14px;color:#4a4a4a;line-height:1.7;margin:10px 0 0;">{summary}</p>'
            if summary else ""
        )
        rows += f"""
        <div style="border-left:3px solid #4c1d95;padding:0 0 28px 20px;margin-bottom:28px;border-bottom:1px solid #ece9f0;">
          <p style="font-size:10px;color:#888;text-transform:uppercase;letter-spacing:1.5px;margin:0 0 8px;font-family:sans-serif;">
            {i:02d} &nbsp;&middot;&nbsp; {source}
          </p>
          <h2 style="font-size:19px;font-weight:bold;color:#1a1a1a;margin:0;line-height:1.4;">
            <a href="{link}" style="color:#1a1a1a;text-decoration:none;">{title}</a>
          </h2>
          {summary_html}
          <p style="margin:14px 0 0;">
            <a href="{link}" style="color:#4c1d95;font-size:13px;font-weight:bold;text-decoration:none;border-bottom:1px solid #c4b5fd;padding-bottom:1px;">
              Artikel lesen &#x2192;
            </a>
          </p>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>shared ground &mdash; {date_str}</title>
</head>
<body style="margin:0;padding:0;background:#f0ecf8;font-family:Georgia,serif;">
<div style="max-width:620px;margin:32px auto;background:#ffffff;border-radius:4px;overflow:hidden;box-shadow:0 2px 12px rgba(76,29,149,0.08);">

  <!-- Header -->
  <div style="background:#4c1d95;padding:44px 40px 36px;text-align:center;">
    <h1 style="color:#ffffff;font-size:32px;margin:0;letter-spacing:4px;font-weight:bold;font-family:Georgia,serif;">
      shared ground
    </h1>
    <p style="color:rgba(255,255,255,0.65);font-size:12px;margin:10px 0 0;letter-spacing:1px;">
      deine nachrichten. deine perspektive.
    </p>
    <div style="margin:20px auto 0;width:40px;height:1px;background:rgba(255,255,255,0.3);"></div>
    <p style="color:rgba(255,255,255,0.8);font-size:13px;margin:16px 0 0;font-style:italic;">
      Die Woche im R&uuml;ckblick &mdash; {date_str}
    </p>
  </div>

  <!-- Intro -->
  <div style="padding:32px 40px 8px;border-bottom:1px solid #ece9f0;">
    <p style="font-size:15px;color:#333;line-height:1.8;margin:0;">
      Guten Morgen! Hier sind die wichtigsten Nachrichten aus Feminismus,
      Frauen und LGBTQIA+ der vergangenen Woche &mdash; zusammengestellt von shared ground.
    </p>
  </div>

  <!-- Articles -->
  <div style="padding:32px 40px 8px;">
    <p style="font-size:10px;color:#9f7aea;text-transform:uppercase;letter-spacing:2px;margin:0 0 24px;font-family:sans-serif;">
      Diese Woche
    </p>
    {rows}
  </div>

  <!-- CTA -->
  <div style="background:#f9f7ff;padding:28px 40px;text-align:center;border-top:1px solid #ece9f0;">
    <p style="font-size:14px;color:#555;margin:0 0 16px;">
      Alle Artikel und mehr findest du auf unserer Website:
    </p>
    <a href="https://shared-ground-frontend.vercel.app"
       style="display:inline-block;background:#4c1d95;color:#ffffff;text-decoration:none;padding:12px 28px;font-size:13px;font-weight:bold;letter-spacing:1px;border-radius:2px;font-family:sans-serif;">
      shared ground &ouml;ffnen
    </a>
  </div>

  <!-- Footer -->
  <div style="padding:24px 40px;text-align:center;background:#ffffff;">
    <p style="font-size:11px;color:#aaa;margin:0;line-height:1.8;">
      Du erh&auml;ltst diesen Newsletter weil du dich auf shared ground angemeldet hast.<br>
      <a href="{unsubscribe_url}" style="color:#aaa;text-decoration:underline;">Abmelden</a>
      &nbsp;&middot;&nbsp;
      <a href="https://shared-ground-frontend.vercel.app" style="color:#aaa;text-decoration:underline;">Website</a>
    </p>
  </div>

</div>
</body>
</html>"""


def get_top_articles_this_week(limit=8):
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
    cursor.execute(
        f"""SELECT title, link, summary, source, published_at, scraped_at
            FROM articles
            WHERE scraped_at >= {ph}
            ORDER BY scraped_at DESC
            LIMIT {limit}""",
        [seven_days_ago]
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {"title": r[0], "link": r[1], "summary": r[2], "source": r[3]}
        for r in rows
    ]


def get_active_subscribers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT email FROM subscribers WHERE active = 1")
    emails = [row[0] for row in cursor.fetchall()]
    conn.close()
    return emails


def send_newsletter():
    if not BREVO_API_KEY:
        print("Kein BREVO_API_KEY gesetzt — Newsletter nicht gesendet.")
        return

    articles = get_top_articles_this_week(limit=8)
    if not articles:
        print("Keine Artikel für Newsletter gefunden.")
        return

    subscribers = get_active_subscribers()
    if not subscribers:
        print("Keine Subscribers — Newsletter nicht gesendet.")
        return

    print(f"Sende Newsletter an {len(subscribers)} Subscriber(s)...")
    date_str = format_german_date(datetime.now())
    success = 0

    for email in subscribers:
        token = make_unsubscribe_token(email)
        base_url = os.environ.get("BASE_URL", "https://roundup-briefs-germany.onrender.com")
        unsubscribe_url = f"{base_url}/api/newsletter/unsubscribe?email={email}&token={token}"
        html = build_newsletter_html(articles, unsubscribe_url)

        payload = {
            "sender": {"name": BREVO_SENDER_NAME, "email": BREVO_SENDER},
            "to": [{"email": email}],
            "subject": f"shared ground — Die Woche im Rückblick ({date_str})",
            "htmlContent": html,
        }
        try:
            res = http_requests.post(
                "https://api.brevo.com/v3/smtp/email",
                json=payload,
                headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
                timeout=15,
            )
            if res.status_code in (200, 201):
                success += 1
            else:
                print(f"Fehler bei {email}: {res.status_code} {res.text}")
        except Exception as e:
            print(f"Fehler beim Senden an {email}: {e}")

    print(f"Newsletter gesendet: {success}/{len(subscribers)} erfolgreich.")


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
        cursor.execute(f"INSERT INTO subscribers (email) VALUES ({ph})", [email])
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        err = str(e).lower()
        if "unique" in err or "duplicate" in err:
            return jsonify({"error": "Diese E-Mail ist bereits angemeldet."}), 409
        return jsonify({"error": "Fehler beim Speichern. Bitte versuch es später."}), 500


@app.route("/api/newsletter/unsubscribe")
def newsletter_unsubscribe():
    email = (request.args.get("email") or "").strip().lower()
    token = request.args.get("token", "")
    if not email or token != make_unsubscribe_token(email):
        return "Ungültiger Abmelde-Link.", 400
    try:
        conn = get_connection()
        cursor = conn.cursor()
        ph = "%s" if USE_POSTGRES else "?"
        cursor.execute(f"UPDATE subscribers SET active = 0 WHERE email = {ph}", [email])
        conn.commit()
        conn.close()
        return "<html><body style='font-family:sans-serif;text-align:center;padding:60px'><h2>Abgemeldet</h2><p>Du wurdest erfolgreich vom shared ground Newsletter abgemeldet.</p></body></html>"
    except Exception as e:
        return "Fehler beim Abmelden.", 500


@app.route("/api/newsletter/subscribers")
def newsletter_subscribers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subscribers WHERE active = 1")
    count = cursor.fetchone()[0]
    conn.close()
    return jsonify({"count": count})


@app.route("/api/newsletter/send-now")
def newsletter_send_now():
    thread = threading.Thread(target=send_newsletter)
    thread.start()
    return jsonify({"status": "Newsletter wird gesendet..."})


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


def delete_old_articles():
    """Delete articles older than 90 days to keep the database small."""
    conn = get_connection()
    cursor = conn.cursor()
    ph = "%s" if USE_POSTGRES else "?"
    cutoff = (datetime.now() - timedelta(days=90)).isoformat()
    cursor.execute(f"DELETE FROM articles WHERE scraped_at < {ph}", [cutoff])
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    print(f"Cleanup: deleted {deleted} articles older than 90 days.", flush=True)


def enrich_images_batch(batch_size=15):
    """Fetch og:image for articles that have no image_url yet."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT id, link FROM articles WHERE (image_url IS NULL OR image_url = '') LIMIT {batch_size}"
        )
        rows = cursor.fetchall()
    except Exception as e:
        print(f"Image enrichment DB error: {e}", flush=True)
        conn.close()
        return
    conn.close()

    updated = 0
    for row in rows:
        article_id, link = row[0], row[1]
        try:
            resp = http_requests.get(
                link,
                timeout=8,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SharedGroundBot/1.0)"},
                allow_redirects=True,
            )
            if resp.ok:
                # Try og:image — two attribute orderings
                og_match = re.search(
                    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                    resp.text,
                ) or re.search(
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                    resp.text,
                )
                if og_match:
                    image_url = og_match.group(1).strip()
                    if image_url.startswith("http"):
                        conn2 = get_connection()
                        cur2 = conn2.cursor()
                        ph = "%s" if USE_POSTGRES else "?"
                        cur2.execute(
                            f"UPDATE articles SET image_url = {ph} WHERE id = {ph}",
                            [image_url, article_id],
                        )
                        conn2.commit()
                        conn2.close()
                        updated += 1
        except Exception:
            pass
        time.sleep(1)

    print(f"Image enrichment: {updated}/{len(rows)} articles updated.", flush=True)


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
    scheduler.add_job(
        send_newsletter,
        'cron',
        day_of_week='sun',
        hour=8,
        minute=0,
        id='sunday_newsletter'
    )
    scheduler.add_job(
        delete_old_articles,
        'cron',
        day_of_week='mon',
        hour=3,
        id='weekly_cleanup'
    )
    scheduler.add_job(
        enrich_images_batch,
        'interval',
        minutes=30,
        id='image_enrichment'
    )
    scheduler.start()
    print("Scheduler aktiv — scrapet alle 12h, Newsletter jeden Sonntag um 8 Uhr, Bilder-Anreicherung alle 30 Min.")


startup()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
