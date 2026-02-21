"""
scraper.py
Fetches articles from German news sources and filters by keywords related to
women, feminism, and LGBTQIA+ topics. Saves results to a database.
Supports both SQLite (local) and PostgreSQL (production on Render).
"""

import feedparser
import hashlib
import re
import os
from datetime import datetime, timezone

# ── Database setup: PostgreSQL if DATABASE_URL is set, else SQLite ────────────
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    USE_POSTGRES = True
else:
    import sqlite3
    USE_POSTGRES = False

DB_FILE = "news.db"  # Only used for SQLite fallback


def get_connection():
    """Return a database connection (PostgreSQL or SQLite)."""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        return sqlite3.connect(DB_FILE)


# ─────────────────────────────────────────────────────────────────────────────
#  NEWS SOURCES  — add or remove feeds here
#  Format: "Display Name": {"url": "RSS feed URL", "country": "XX"}
# ─────────────────────────────────────────────────────────────────────────────
FEEDS = {
    # ── Große Tageszeitungen (keyword-gefiltert) ────────────────────────────
    "Spiegel Online":       {"url": "https://www.spiegel.de/schlagzeilen/tops/index.rss",           "country": "Germany"},
    "Zeit Online":          {"url": "https://newsfeed.zeit.de/index",                               "country": "Germany"},
    "FAZ":                  {"url": "https://www.faz.net/rss/aktuell/",                             "country": "Germany"},
    "Süddeutsche Zeitung":  {"url": "https://rss.sueddeutsche.de/rss/Topthemen",                   "country": "Germany"},
    "Die Welt":             {"url": "https://www.welt.de/feeds/latest.rss",                        "country": "Germany"},
    "Tagesspiegel":         {"url": "https://www.tagesspiegel.de/feeds/",                          "country": "Germany"},
    "Stern":                {"url": "https://www.stern.de/feed/standard/alle-nachrichten/",         "country": "Germany"},
    "Focus Online":         {"url": "https://rss.focus.de/fol/XML/rss_folnews.xml",                "country": "Germany"},

    # ── Öffentlich-Rechtliche Medien (keyword-gefiltert) ───────────────────
    "Tagesschau":           {"url": "https://www.tagesschau.de/xml/rss2/",                         "country": "Germany"},
    "ZDF heute":            {"url": "https://www.zdf.de/rss/zdf/nachrichten",                      "country": "Germany"},
    "Deutschlandfunk":      {"url": "https://www.deutschlandfunk.de/nachrichten-100.rss",           "country": "Germany"},
    "BR24":                 {"url": "https://www.br.de/nachrichten/rss/meldungen.xml",              "country": "Germany"},
    "MDR Nachrichten":      {"url": "https://www.mdr.de/nachrichten/index-rss.xml",                "country": "Germany"},
    "NDR Nachrichten":      {"url": "https://www.ndr.de/nachrichten/index-rss.xml",                "country": "Germany"},

    # ── Progressive & taz (keyword-gefiltert) ─────────────────────────────
    "taz":                  {"url": "https://taz.de/!p4608;rss/",                                  "country": "Germany"},
    "Freitag":              {"url": "https://www.freitag.de/feeds/all",                            "country": "Germany"},

    # ── Österreich ─────────────────────────────────────────────────────────
    "Der Standard":         {"url": "https://derstandard.at/?page=rss&ressort=Frontpage",          "country": "Austria"},
    "ORF News":             {"url": "https://rss.orf.at/news.xml",                                 "country": "Austria"},

    # ── Schweiz ────────────────────────────────────────────────────────────
    "NZZ":                  {"url": "https://www.nzz.ch/recent.rss",                               "country": "Switzerland"},
    "SRF News":             {"url": "https://www.srf.ch/news/bnf/rss/1890",                        "country": "Switzerland"},

    # ── Feministische Publikationen (alle Artikel behalten) ────────────────
    "EMMA":                 {"url": "https://www.emma.de/feeds/gesamtinhalt",                      "country": "Germany"},

    # ── LGBTQIA+ Publikationen (alle Artikel behalten) ─────────────────────
    "queer.de":             {"url": "https://www.queer.de/feed.php",                               "country": "Germany"},
    "L-MAG":                {"url": "https://www.l-mag.de/feed/",                                  "country": "Germany"},
}

ALWAYS_INCLUDE_SOURCES = {
    "EMMA",
    "queer.de",
    "L-MAG",
}

# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORDS — Deutsch & Englisch
# ─────────────────────────────────────────────────────────────────────────────
KEYWORDS = [
    # ── Frauen & Feminismus (Deutsch) ───────────────────────────────────────
    "frauen", "frau", "mädchen", "weiblich", "weibliche",
    "feminismus", "feministisch", "feminist",
    "gleichberechtigung", "gleichstellung", "frauenrechte",
    "frauenquote", "geschlechtergleichheit", "geschlechtergerechtigkeit",
    "lohngleichheit", "lohnungleichheit", "lohnlücke", "entgeltungleichheit",
    "entgeltgleichheit", "geschlechtslohnlücke",
    "reproduktive rechte", "abtreibung", "schwangerschaftsabbruch",
    "paragraph 218", "mutterschaft", "schwangerschaft",
    "mutterschutz", "elterngeld", "elternzeit", "väterzeit",
    "sexismus", "misogynie", "patriarchat",
    "häusliche gewalt", "femizid", "frauenmord",
    "sexuelle belästigung", "sexueller übergriff", "vergewaltigung",
    "me-too", "metoo", "#metoo",
    "frauenbewegung", "frauenmarsch",
    "gläserne decke", "frauenförderung",
    "menstruation", "periode", "regelblutung", "menstruationsarmut",
    "verhütung", "verhütungsmittel", "abtreibungspille",
    "geburtenkontrolle", "pille danach",
    "brustkrebs", "gebärmutterhalskrebs", "frauengesundheit",
    "stillzeit", "stillen", "postpartum",
    "trafficking", "menschenhandel",
    "körperbild", "essstörung", "magersucht", "bulimie",
    "mutterschaftsstrafe", "care-arbeit", "unbezahlte arbeit",

    # ── LGBTQIA+ (Deutsch) ──────────────────────────────────────────────────
    "schwul", "lesbisch", "bisexuell",
    "transgender", "transsexuell", "transgeschlechtlich",
    "trans*", "transperson", "transfrauen", "transmänner",
    "nicht-binär", "nichtbinär", "nonbinär", "genderqueer",
    "intergeschlechtlich", "intersexuell",
    "queer", "homosexuell", "homosexualität",
    "coming out", "geoutet", "outing",
    "homophobie", "transphobie", "biphobie",
    "regenbogen", "pride", "christopher street day", "csd",
    "gleichgeschlechtlich", "ehe für alle", "homo-ehe",
    "geschlechtsidentität", "geschlechtsausdruck",
    "pronomen", "misgendering", "deadnaming",
    "drag", "drag queen", "drag king",
    "lsbti", "lsbtiq", "lsbtiq+", "lgbtq",
    "konversionstherapie", "heilungsversuch",
    "geschlechtsangleichung", "geschlechtsangleichende operation",
    "pubertätsblocker",

    # ── Migration & Asyl (Deutsch) ──────────────────────────────────────────
    "flüchtling", "flüchtlinge", "geflüchtete", "asylsuchende",
    "asylbewerber", "asylbewerberin", "asylverfahren",
    "migration", "migrant", "migrantin", "einwanderung", "einwanderer",
    "abschiebung", "rückführung", "aufenthaltsrecht",
    "staatsangehörigkeit", "einbürgerung", "staatsbürgerschaft",
    "fremdenfeindlichkeit", "ausländerfeindlichkeit", "rassismus",
    "diskriminierung", "antirassismus",
    "flucht", "vertreibung", "vertrieben",

    # ── Menschenrechte (Deutsch) ────────────────────────────────────────────
    "menschenrechte", "bürgerrechte",
    "protest", "aktivismus", "aktivist", "aktivistin",
    "zensur", "pressefreiheit", "meinungsfreiheit",
    "minderheitenrechte", "indigene rechte",
    "humanitär", "humanitäre krise",
    "demokratie", "diskriminierung",

    # ── English keywords (kept for international sources) ───────────────────
    "women", "woman", "girl", "girls", "female", "feminism",
    "feminist", "gender equality", "gender gap", "gender pay gap", "equal pay",
    "reproductive rights", "abortion",
    "women's rights", "sexism", "misogyny",
    "domestic violence", "gender violence", "gender-based violence",
    "sexual harassment", "sexual assault", "rape", "metoo", "me too",
    "femicide",
    "lgbt", "lgbtq", "lgbtqia", "queer", "gay", "lesbian", "bisexual",
    "transgender", "trans ", "nonbinary", "non-binary", "intersex",
    "pride", "same-sex", "gay rights", "trans rights",
    "homophobia", "transphobia",
    "immigration", "refugee", "asylum", "migrant",
    "human rights", "civil rights", "discrimination",
]

# ─────────────────────────────────────────────────────────────────────────────
#  TOPIC KEYWORDS (12 Themen, Mehrfachauswahl)
# ─────────────────────────────────────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "Reproduktive Rechte": [
        # Deutsch
        "abtreibung", "schwangerschaftsabbruch", "paragraph 218",
        "reproduktive rechte", "geburtenkontrolle", "verhütung",
        "verhütungsmittel", "pille danach", "schwangerschaft",
        "mutterschaft", "mutterschutz", "elternzeit", "elterngeld",
        "fehlgeburt", "totgeburt", "gebärmutter", "gebärmutterhals",
        "frauengesundheit", "gynäkologie", "hebamme",
        "stillen", "stillzeit", "postpartum", "pränatal",
        "reproduktive gerechtigkeit", "körperliche selbstbestimmung",
        "menstruation", "periode", "menstruationsarmut",
        # English
        "reproductive", "abortion", "pro-choice", "birth control",
        "contraception", "fertility", "ivf", "pregnancy",
        "maternal mortality", "gynecolog", "bodily autonomy",
    ],
    "Lohnlücke & Wirtschaft": [
        # Deutsch
        "lohnlücke", "lohnungleichheit", "entgeltungleichheit",
        "lohngleichheit", "entgeltgleichheit", "geschlechtslohnlücke",
        "gläserne decke", "frauenquote", "frauenförderung",
        "care-arbeit", "unbezahlte arbeit", "mutterschaftsstrafe",
        "elterngeld", "teilzeitfalle",
        # English
        "pay gap", "wage gap", "equal pay", "gender pay", "salary gap",
        "income inequality", "glass ceiling", "gender parity",
        "motherhood penalty", "parental leave",
    ],
    "LGBTQIA+": [
        # Deutsch
        "schwul", "lesbisch", "bisexuell", "transgender", "transsexuell",
        "nicht-binär", "nichtbinär", "intergeschlechtlich", "queer",
        "homosexuell", "coming out", "homophobie", "transphobie",
        "csd", "christopher street day", "ehe für alle", "homo-ehe",
        "drag queen", "drag king", "geschlechtsidentität",
        "pronomen", "lsbtiq", "konversionstherapie",
        "geschlechtsangleichung", "pubertätsblocker",
        # English
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian",
        "trans rights", "homophobia", "transphobia", "pride",
        "same-sex", "gender affirming", "conversion therapy",
    ],
    "Migration & Asyl": [
        # Deutsch
        "flüchtling", "flüchtlinge", "geflüchtete", "asylsuchende",
        "asylbewerber", "migration", "migrant", "einwanderung",
        "abschiebung", "rückführung", "aufenthaltsrecht",
        "einbürgerung", "staatsangehörigkeit",
        "fremdenfeindlichkeit", "flucht", "vertreibung",
        # English
        "immigration", "refugee", "asylum", "migrant",
        "deportation", "border", "citizenship", "diaspora",
        "xenophobia", "trafficking",
    ],
    "Menschenrechte": [
        # Deutsch
        "menschenrechte", "bürgerrechte", "diskriminierung",
        "rassismus", "antirassismus", "protest", "aktivismus",
        "zensur", "pressefreiheit", "meinungsfreiheit",
        "minderheitenrechte", "humanitär",
        # English
        "human rights", "civil rights", "discrimination",
        "racism", "protest", "activism", "censorship", "humanitarian",
    ],
    "Gesundheit & Medizin": [
        # Deutsch
        "gesundheit", "medizin", "krankenhaus", "klinik", "arzt", "ärztin",
        "psychische gesundheit", "therapie", "diagnose", "behandlung",
        "hormon", "hormontherapie", "essstörung", "körperbild",
        "hiv", "krebs", "brustkrebs", "gebärmutterhalskrebs",
        "impfung", "impfstoff", "pandemie",
        "pubertätsblocker", "geschlechtsangleichende",
        # English
        "health", "medical", "healthcare", "mental health", "therapy",
        "hormone", "eating disorder", "hiv", "cancer", "pandemic",
        "vaccination", "gender affirming care",
    ],
    "Recht & Politik": [
        # Deutsch
        "gesetz", "recht", "gericht", "klage", "gesetzgebung",
        "bundesgesetz", "urteil", "richter", "richterinnen", "anwalt", "anwältin",
        "verbot", "reform", "bundesverfassungsgericht", "europäischer gerichtshof",
        "paragraph", "strafgesetzbuch", "grundgesetz",
        # English
        "law", "legal", "court", "lawsuit", "legislation",
        "ruling", "supreme court", "ban", "regulation",
    ],
    "Politik & Regierung": [
        # Deutsch
        "wahl", "abstimmung", "bundestagswahl", "politiker", "politikerin",
        "bundestag", "bundesrat", "bundesregierung", "bundeskanzler", "bundeskanzlerin",
        "minister", "ministerin", "partei", "koalition",
        "wahlkampf", "kandidat", "kandidatin",
        "spd", "cdu", "csu", "grüne", "fdp", "afd", "linke", "bsw",
        "landtag", "kommunalpolitik", "europaparlament",
        # English
        "election", "vote", "parliament", "minister", "president",
        "campaign", "political", "government",
    ],
    "Kultur & Medien": [
        # Deutsch
        "film", "kino", "fernsehen", "serie", "streaming",
        "buch", "roman", "autorin", "autor", "lyrik", "poesie",
        "musik", "konzert", "festival", "album",
        "kunst", "künstlerin", "ausstellung", "museum",
        "mode", "dokumentation", "podcast", "interview",
        "drag queen", "drag king", "preis", "award",
        "repräsentation", "sichtbarkeit",
        # English
        "film", "movie", "television", "book", "novel",
        "music", "art", "exhibition", "fashion", "documentary",
        "representation", "visibility",
    ],
    "Sport": [
        # Deutsch
        "sport", "athletin", "olympia", "wettkampf", "meisterschaft",
        "fußball", "basketball", "tennis", "schwimmen",
        "weltmeisterschaft", "frauen im sport", "mannschaft",
        "trainerin", "liga", "turnier", "medaille",
        "transgender sportlerin", "inklusion im sport",
        "dfb", "dlv", "dsb",
        # English
        "sport", "athlete", "olympic", "competition", "championship",
        "football", "soccer", "basketball", "tennis",
        "world cup", "women in sport", "transgender athlete",
    ],
    "Gewalt & Sicherheit": [
        # Deutsch
        "gewalt", "angriff", "mord", "getötet", "femizid", "frauenmord",
        "häusliche gewalt", "missbrauch", "misshandlung", "opfer", "überlebende",
        "vergewaltigung", "sexuelle gewalt", "belästigung",
        "hassverbrechen", "hassrede", "bedrohung", "stalking",
        "menschenhandel", "frauenhandel",
        "sicherheit", "schutz", "frauenhaus", "schutzorder",
        # English
        "violence", "assault", "murder", "domestic violence",
        "abuse", "rape", "sexual assault", "hate crime",
        "trafficking", "femicide", "safety",
    ],
    "Arbeit & Wirtschaft": [
        # Deutsch
        "arbeitsplatz", "beschäftigung", "arbeitgeber", "arbeitnehmerin",
        "beruf", "karriere", "einstellung", "entlassung", "stellenabbau",
        "geschäftsführerin", "vorstand", "führungsposition",
        "diskriminierung am arbeitsplatz", "belästigung am arbeitsplatz",
        "unternehmerin", "startup", "wirtschaft", "armut",
        "kinderfürsorge", "kinderbetreuung", "kita",
        "elternzeit", "mutterschutz", "work-life-balance",
        # English
        "workplace", "employment", "career", "leadership",
        "ceo", "board", "discrimination at work",
        "entrepreneurship", "economy", "poverty", "childcare",
        "parental leave", "work-life balance",
    ],
}

MAX_ARTICLES_PER_SOURCE = 30


# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def setup_database():
    conn = get_connection()
    cursor = conn.cursor()

    if USE_POSTGRES:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id           SERIAL PRIMARY KEY,
                url_hash     TEXT    UNIQUE,
                title        TEXT,
                link         TEXT,
                summary      TEXT,
                source       TEXT,
                country      TEXT    DEFAULT '',
                category     TEXT,
                tags         TEXT,
                topics       TEXT    DEFAULT '',
                scraped_at   TEXT,
                published_at TEXT    DEFAULT ''
            )
        """)
        cursor.execute("""
            ALTER TABLE articles ADD COLUMN IF NOT EXISTS published_at TEXT DEFAULT ''
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                url_hash     TEXT    UNIQUE,
                title        TEXT,
                link         TEXT,
                summary      TEXT,
                source       TEXT,
                country      TEXT    DEFAULT '',
                category     TEXT,
                tags         TEXT,
                topics       TEXT    DEFAULT '',
                scraped_at   TEXT,
                published_at TEXT    DEFAULT ''
            )
        """)
        try:
            cursor.execute("ALTER TABLE articles ADD COLUMN published_at TEXT DEFAULT ''")
        except Exception:
            pass  # Column already exists

    conn.commit()
    conn.close()
    print("✅ Database ready.")


def url_hash(url):
    return hashlib.md5(url.encode()).hexdigest()


def strip_html(text):
    return re.sub(r'<[^>]+>', '', text or '').strip()


# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORD MATCHING
# ─────────────────────────────────────────────────────────────────────────────
def get_matching_tags(text):
    text_lower = text.lower()
    matched = []
    women_terms = [
        "frauen", "frau", "mädchen", "weiblich", "feminismus", "feministisch",
        "gleichberechtigung", "frauenrechte", "lohnlücke", "entgeltungleichheit",
        "abtreibung", "sexismus", "misogynie", "patriarchat", "femizid",
        "häusliche gewalt", "sexuelle belästigung", "metoo", "me-too",
        "women", "woman", "girl", "girls", "female", "feminism",
        "feminist", "gender", "reproductive", "abortion", "sexism",
        "domestic violence", "femicide",
    ]
    lgbtq_terms = [
        "schwul", "lesbisch", "bisexuell", "transgender", "transsexuell",
        "nicht-binär", "nichtbinär", "intergeschlechtlich", "queer",
        "homosexuell", "homophobie", "transphobie", "csd",
        "ehe für alle", "lsbtiq",
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian", "bisexual",
        "trans ", "nonbinary", "non-binary", "intersex", "pride",
        "homophobia", "transphobia", "conversion therapy",
    ]
    if any(t in text_lower for t in women_terms):
        matched.append("women")
    if any(t in text_lower for t in lgbtq_terms):
        matched.append("lgbtqia+")
    return matched


def matches_keywords(title, summary):
    combined = (title + " " + summary).lower()
    return any(kw in combined for kw in KEYWORDS)


def get_topics(text):
    text_lower = text.lower()
    matched = []
    for topic_name, keywords in TOPIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            matched.append(topic_name)
    return matched


# ─────────────────────────────────────────────────────────────────────────────
#  SCRAPING
# ─────────────────────────────────────────────────────────────────────────────
def scrape_all_feeds():
    total_new = 0
    ph = "%s" if USE_POSTGRES else "?"

    for source_name, feed_info in FEEDS.items():
        feed_url = feed_info["url"]
        country  = feed_info["country"]
        print(f"  📡 Scraping: {source_name}...", flush=True)
        try:
            feed    = feedparser.parse(feed_url)
            entries = feed.entries[:MAX_ARTICLES_PER_SOURCE]
            new_count = 0

            conn   = get_connection()
            cursor = conn.cursor()

            for entry in entries:
                link    = entry.get("link", "")
                title   = strip_html(entry.get("title", "No title"))
                summary = strip_html(entry.get("summary", ""))
                hash_id = url_hash(link)

                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                if pub_parsed:
                    try:
                        published_at = datetime(*pub_parsed[:6]).isoformat()
                    except Exception:
                        published_at = datetime.now().isoformat()
                else:
                    published_at = datetime.now().isoformat()

                always_keep = source_name in ALWAYS_INCLUDE_SOURCES
                if not always_keep and not matches_keywords(title, summary):
                    continue

                tags = get_matching_tags(title + " " + summary)
                if source_name in {"queer.de", "L-MAG"}:
                    tags = list(set(tags + ["lgbtqia+"]))
                elif source_name in {"EMMA"}:
                    tags = list(set(tags + ["women"]))

                category = "lgbtqia+" if "lgbtqia+" in tags else "women"
                tags_str = ", ".join(sorted(set(tags))) if tags else "general"

                topics = get_topics(title + " " + summary)
                if source_name in {"queer.de", "L-MAG"}:
                    topics = list(set(topics + ["LGBTQIA+"]))
                topics_str = ", ".join(sorted(set(topics))) if topics else ""

                try:
                    if USE_POSTGRES:
                        cursor.execute(f"""
                            INSERT INTO articles
                              (url_hash, title, link, summary, source, country,
                               category, tags, topics, scraped_at, published_at)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                            ON CONFLICT (url_hash) DO NOTHING
                        """, (hash_id, title, link, summary, source_name, country,
                              category, tags_str, topics_str, datetime.now().isoformat(),
                              published_at))
                        if cursor.rowcount > 0:
                            new_count += 1
                    else:
                        cursor.execute(f"""
                            INSERT OR IGNORE INTO articles
                              (url_hash, title, link, summary, source, country,
                               category, tags, topics, scraped_at, published_at)
                            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                        """, (hash_id, title, link, summary, source_name, country,
                              category, tags_str, topics_str, datetime.now().isoformat(),
                              published_at))
                        if cursor.rowcount > 0:
                            new_count += 1
                except Exception:
                    pass

            conn.commit()
            conn.close()
            print(f"     ✔  {new_count} new articles from {source_name}", flush=True)
            total_new += new_count

        except Exception as e:
            print(f"     ❌  Error scraping {source_name}: {e}", flush=True)

    print(f"\n🎉 Done! {total_new} new articles saved in total.", flush=True)


def get_all_articles(category=None, source=None, search=None, topic=None,
                     country=None, time_range=None, date_to=None, limit=200):
    conn = get_connection()
    if USE_POSTGRES:
        import psycopg2.extras
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        ph = "%s"
    else:
        conn.row_factory = __import__('sqlite3').Row
        cursor = conn.cursor()
        ph = "?"

    query  = "SELECT * FROM articles WHERE 1=1"
    params = []

    if category:
        query += f" AND (category = {ph} OR tags LIKE {ph})"
        params += [category, f"%{category}%"]
    if source:
        query += f" AND source = {ph}"
        params.append(source)
    if country:
        query += f" AND country = {ph}"
        params.append(country)
    if search:
        if USE_POSTGRES:
            query += f" AND (title ILIKE {ph} OR summary ILIKE {ph})"
        else:
            query += f" AND (title LIKE {ph} OR summary LIKE {ph})"
        params += [f"%{search}%", f"%{search}%"]
    if topic:
        topic_list = [t.strip() for t in topic.split(",")]
        if USE_POSTGRES:
            topic_clauses = " OR ".join([f"topics ILIKE {ph}" for _ in topic_list])
        else:
            topic_clauses = " OR ".join([f"topics LIKE {ph}" for _ in topic_list])
        query += f" AND ({topic_clauses})"
        params += [f"%{t}%" for t in topic_list]
    if time_range:
        query += f" AND scraped_at >= {ph}"
        params.append(time_range)
    if date_to:
        query += f" AND scraped_at <= {ph}"
        params.append(date_to + "T23:59:59")

    query += f" ORDER BY scraped_at DESC LIMIT {ph}"
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


if __name__ == "__main__":
    print("🗞️  Deutsche Medien Scraper startet...\n")
    setup_database()
    scrape_all_feeds()
