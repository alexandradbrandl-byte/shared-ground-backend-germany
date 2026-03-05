"""
scraper.py
Fetches articles from news sources worldwide and filters by keywords related to
women, feminism, and LGBTQIA+ topics. Saves results to a database.
Supports both SQLite (local) and PostgreSQL (production on Render).
Translates non-German titles to German via MyMemory (free, no API key needed).
"""

import feedparser
import hashlib
import re
import os
import requests as http_req
from datetime import datetime, timezone

# ── Database setup ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    import psycopg2
    import psycopg2.extras
    USE_POSTGRES = True
else:
    import sqlite3
    USE_POSTGRES = False

DB_FILE = "news.db"


def get_connection():
    if USE_POSTGRES:
        return psycopg2.connect(DATABASE_URL)
    else:
        return sqlite3.connect(DB_FILE)


def translate_to_german(text, source_lang):
    """Translate text to German via MyMemory (free, no API key needed)."""
    if not text or source_lang == "DE":
        return text
    try:
        lang_pair = f"{source_lang.lower()}|de"
        resp = http_req.get(
            "https://api.mymemory.translated.net/get",
            params={
                "q": text[:500],
                "langpair": lang_pair,
                "de": "alexandra.d.brandl@gmail.com",
            },
            timeout=8,
        )
        if resp.ok:
            data = resp.json()
            if data.get("responseStatus") == 200:
                return data["responseData"]["translatedText"]
    except Exception:
        pass
    return text


# ─────────────────────────────────────────────────────────────────────────────
#  NEWS SOURCES
#  Format: "Display Name": {"url": "...", "country": "...", "language": "XX"}
# ─────────────────────────────────────────────────────────────────────────────
FEEDS = {
    # ── Deutschland ───────────────────────────────────────────────────────────
    "Spiegel Online":       {"url": "https://www.spiegel.de/schlagzeilen/tops/index.rss",              "country": "Germany",       "language": "DE"},
    "Zeit Online":          {"url": "https://newsfeed.zeit.de/index",                                  "country": "Germany",       "language": "DE"},
    "FAZ":                  {"url": "https://www.faz.net/rss/aktuell/",                                "country": "Germany",       "language": "DE"},
    "Sueddeutsche Zeitung": {"url": "https://rss.sueddeutsche.de/rss/Topthemen",                       "country": "Germany",       "language": "DE"},
    "Die Welt":             {"url": "https://www.welt.de/feeds/latest.rss",                            "country": "Germany",       "language": "DE"},
    "Tagesspiegel":         {"url": "https://www.tagesspiegel.de/feeds/",                              "country": "Germany",       "language": "DE"},
    "Focus Online":         {"url": "https://rss.focus.de/fol/XML/rss_folnews.xml",                    "country": "Germany",       "language": "DE"},
    "Tagesschau":           {"url": "https://www.tagesschau.de/xml/rss2/",                             "country": "Germany",       "language": "DE"},
    "ZDF heute":            {"url": "https://www.zdf.de/rss/zdf/nachrichten",                          "country": "Germany",       "language": "DE"},
    "Deutschlandfunk":      {"url": "https://www.deutschlandfunk.de/nachrichten-100.rss",              "country": "Germany",       "language": "DE"},
    "BR24":                 {"url": "https://www.br.de/nachrichten/rss/meldungen.xml",                 "country": "Germany",       "language": "DE"},
    "MDR Nachrichten":      {"url": "https://www.mdr.de/nachrichten/index-rss.xml",                    "country": "Germany",       "language": "DE"},
    "NDR Nachrichten":      {"url": "https://www.ndr.de/nachrichten/index-rss.xml",                    "country": "Germany",       "language": "DE"},
    "taz":                  {"url": "https://taz.de/!p4608;rss/",                                      "country": "Germany",       "language": "DE"},
    "Freitag":              {"url": "https://www.freitag.de/feeds/all",                                "country": "Germany",       "language": "DE"},
    "EMMA":                 {"url": "https://www.emma.de/feeds/gesamtinhalt",                          "country": "Germany",       "language": "DE"},
    "queer.de":             {"url": "https://www.queer.de/feed.php",                                   "country": "Germany",       "language": "DE"},
    "L-MAG":                {"url": "https://www.l-mag.de/feed/",                                      "country": "Germany",       "language": "DE"},

    # ── Österreich ────────────────────────────────────────────────────────────
    "Der Standard":         {"url": "https://derstandard.at/?page=rss&ressort=Frontpage",              "country": "Austria",       "language": "DE"},
    "ORF News":             {"url": "https://rss.orf.at/news.xml",                                     "country": "Austria",       "language": "DE"},
    "Die Presse":           {"url": "https://diepresse.com/rss/",                                      "country": "Austria",       "language": "DE"},
    "Kurier AT":            {"url": "https://kurier.at/rss",                                           "country": "Austria",       "language": "DE"},
    "Kleine Zeitung":       {"url": "https://www.kleinezeitung.at/rss",                                "country": "Austria",       "language": "DE"},
    "profil AT":            {"url": "https://www.profil.at/rss",                                       "country": "Austria",       "language": "DE"},
    "Falter AT":            {"url": "https://www.falter.at/api/rss/feed",                              "country": "Austria",       "language": "DE"},
    "News AT":              {"url": "https://www.news.at/rss",                                         "country": "Austria",       "language": "DE"},
    "Moment AT":            {"url": "https://moment.at/feed/",                                         "country": "Austria",       "language": "DE"},
    "Vienna AT":            {"url": "https://www.vienna.at/feed",                                      "country": "Austria",       "language": "DE"},
    "Wienerin":             {"url": "https://www.wienerin.at/feed",                                    "country": "Austria",       "language": "DE"},
    "Wiener Zeitung":       {"url": "https://www.wienerzeitung.at/rss",                                "country": "Austria",       "language": "DE"},
    "APA OTS":              {"url": "https://www.ots.at/rss",                                          "country": "Austria",       "language": "DE"},

    # ── Schweiz ───────────────────────────────────────────────────────────────
    "NZZ":                  {"url": "https://www.nzz.ch/recent.rss",                                   "country": "Switzerland",   "language": "DE"},
    "SRF News":             {"url": "https://www.srf.ch/news/bnf/rss/1890",                            "country": "Switzerland",   "language": "DE"},
    "Tages-Anzeiger":       {"url": "https://www.tagesanzeiger.ch/rss",                                "country": "Switzerland",   "language": "DE"},
    "20 Minuten CH":        {"url": "https://www.20min.ch/rss/rss.tmpl",                               "country": "Switzerland",   "language": "DE"},
    "Blick CH":             {"url": "https://www.blick.ch/rss",                                        "country": "Switzerland",   "language": "DE"},
    "Watson CH":            {"url": "https://www.watson.ch/rss",                                       "country": "Switzerland",   "language": "DE"},
    "Aargauer Zeitung":     {"url": "https://www.aargauerzeitung.ch/rss",                              "country": "Switzerland",   "language": "DE"},
    "Basler Zeitung":       {"url": "https://www.bazonline.ch/rss",                                    "country": "Switzerland",   "language": "DE"},
    "Der Bund CH":          {"url": "https://www.derbund.ch/rss",                                      "country": "Switzerland",   "language": "DE"},
    "RTS Info":             {"url": "https://www.rts.ch/info/rss",                                     "country": "Switzerland",   "language": "FR"},
    "Le Temps":             {"url": "https://www.letemps.ch/rss.xml",                                  "country": "Switzerland",   "language": "FR"},
    "Tribune de Geneve":    {"url": "https://www.tdg.ch/rss",                                          "country": "Switzerland",   "language": "FR"},
    "Swissinfo EN":         {"url": "https://www.swissinfo.ch/eng/rss",                                "country": "Switzerland",   "language": "EN"},
    "Infosperber CH":       {"url": "https://www.infosperber.ch/feed",                                 "country": "Switzerland",   "language": "DE"},
    "Republik CH":          {"url": "https://www.republik.ch/rss",                                     "country": "Switzerland",   "language": "DE"},

    # ── Spanien ───────────────────────────────────────────────────────────────
    "El Pais":              {"url": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada", "country": "Spain",        "language": "ES"},
    "El Mundo":             {"url": "https://e00-elmundo.uecdn.es/elmundo/rss/portada.xml",             "country": "Spain",        "language": "ES"},
    "La Vanguardia":        {"url": "https://www.lavanguardia.com/rss/home.xml",                        "country": "Spain",        "language": "ES"},
    "El Confidencial":      {"url": "https://rss.elconfidencial.com/espana/",                           "country": "Spain",        "language": "ES"},
    "elDiario.es":          {"url": "https://www.eldiario.es/rss/",                                     "country": "Spain",        "language": "ES"},
    "20minutos ES":         {"url": "https://www.20minutos.es/rss/",                                    "country": "Spain",        "language": "ES"},
    "Publico ES":           {"url": "https://www.publico.es/rss",                                       "country": "Spain",        "language": "ES"},
    "El Periodico":         {"url": "https://www.elperiodico.com/es/rss/rss_portada.xml",               "country": "Spain",        "language": "ES"},
    "RTVE Noticias":        {"url": "https://www.rtve.es/api/noticias.rss",                             "country": "Spain",        "language": "ES"},
    "El Espanol":           {"url": "https://www.elespanol.com/rss/",                                   "country": "Spain",        "language": "ES"},
    "Cadena SER":           {"url": "https://cadenaser.com/feed/",                                      "country": "Spain",        "language": "ES"},
    "infoLibre":            {"url": "https://www.infolibre.es/rss",                                     "country": "Spain",        "language": "ES"},
    "ABC Espana":           {"url": "https://www.abc.es/rss/feeds/abc_ultima_hora.xml",                 "country": "Spain",        "language": "ES"},
    "El Huffpost ES":       {"url": "https://www.huffingtonpost.es/feeds/index.xml",                    "country": "Spain",        "language": "ES"},
    "Mujeres en Red":       {"url": "https://www.mujeresenred.net/spip.php?page=backend",               "country": "Spain",        "language": "ES"},

    # ── Italien ───────────────────────────────────────────────────────────────
    "La Repubblica":        {"url": "https://www.repubblica.it/rss/homepage/rss2.0.xml",               "country": "Italy",        "language": "IT"},
    "Corriere della Sera":  {"url": "https://www.corriere.it/rss/homepage.xml",                         "country": "Italy",        "language": "IT"},
    "ANSA":                 {"url": "https://www.ansa.it/sito/notizie/cronaca/cronaca_rss.xml",         "country": "Italy",        "language": "IT"},
    "Il Fatto Quotidiano":  {"url": "https://www.ilfattoquotidiano.it/feed/",                           "country": "Italy",        "language": "IT"},
    "Il Sole 24 Ore":       {"url": "https://www.ilsole24ore.com/rss/italia--mondo.xml",               "country": "Italy",        "language": "IT"},
    "HuffPost Italia":      {"url": "https://www.huffingtonpost.it/feeds/index.xml",                    "country": "Italy",        "language": "IT"},
    "TGcom24":              {"url": "https://www.tgcom24.mediaset.it/rss/cronaca.xml",                  "country": "Italy",        "language": "IT"},
    "Sky TG24":             {"url": "https://tg24.sky.it/rss/tg24.xml",                                "country": "Italy",        "language": "IT"},
    "Fanpage IT":           {"url": "https://www.fanpage.it/feed/",                                     "country": "Italy",        "language": "IT"},
    "Open Online":          {"url": "https://www.open.online/feed/",                                    "country": "Italy",        "language": "IT"},
    "Il Manifesto":         {"url": "https://ilmanifesto.it/feed",                                      "country": "Italy",        "language": "IT"},
    "Internazionale":       {"url": "https://www.internazionale.it/sito/rss",                           "country": "Italy",        "language": "IT"},
    "AGI":                  {"url": "https://www.agi.it/feed/",                                         "country": "Italy",        "language": "IT"},
    "Rainews":              {"url": "https://www.rainews.it/dl/RaiTV/iphone/rss/rainews/rainews24.xml", "country": "Italy",        "language": "IT"},
    "La Stampa":            {"url": "https://www.lastampa.it/rss.xml",                                  "country": "Italy",        "language": "IT"},

    # ── USA ───────────────────────────────────────────────────────────────────
    "NPR":                  {"url": "https://feeds.npr.org/1001/rss.xml",                               "country": "United States", "language": "EN"},
    "Reuters":              {"url": "https://feeds.reuters.com/reuters/topNews",                        "country": "United States", "language": "EN"},
    "The Guardian US":      {"url": "https://www.theguardian.com/us/rss",                               "country": "United States", "language": "EN"},
    "New York Times":       {"url": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",        "country": "United States", "language": "EN"},
    "CNN":                  {"url": "http://rss.cnn.com/rss/cnn_topstories.rss",                        "country": "United States", "language": "EN"},
    "NBC News":             {"url": "https://feeds.nbcnews.com/nbcnews/public/news",                    "country": "United States", "language": "EN"},
    "CBS News":             {"url": "https://www.cbsnews.com/latest/rss/main",                          "country": "United States", "language": "EN"},
    "ABC News US":          {"url": "https://abcnews.go.com/abcnews/topstories",                        "country": "United States", "language": "EN"},
    "Vox":                  {"url": "https://www.vox.com/rss/index.xml",                                "country": "United States", "language": "EN"},
    "The Atlantic":         {"url": "https://www.theatlantic.com/feed/all/",                            "country": "United States", "language": "EN"},
    "Politico":             {"url": "https://www.politico.com/rss/politicopicks.xml",                   "country": "United States", "language": "EN"},
    "Ms. Magazine":         {"url": "https://msmagazine.com/feed/",                                     "country": "United States", "language": "EN"},
    "Human Rights Watch":   {"url": "https://www.hrw.org/rss.xml",                                     "country": "United States", "language": "EN"},
    "ACLU News":            {"url": "https://www.aclu.org/taxonomy/term/10296/feed",                    "country": "United States", "language": "EN"},
    "Teen Vogue":           {"url": "https://www.teenvogue.com/feed/rss",                               "country": "United States", "language": "EN"},

    # ── China ─────────────────────────────────────────────────────────────────
    "CGTN":                 {"url": "https://www.cgtn.com/subscribe/rss/section/world.xml",             "country": "China",        "language": "EN"},
    "Global Times":         {"url": "https://www.globaltimes.cn/rss/outbrain.xml",                      "country": "China",        "language": "EN"},
    "South China Morning Post": {"url": "https://www.scmp.com/rss/91/feed",                            "country": "China",        "language": "EN"},
    "China Daily":          {"url": "https://www.chinadaily.com.cn/rss/cndy_rss.xml",                   "country": "China",        "language": "EN"},
    "Sixth Tone":           {"url": "https://www.sixthtone.com/feeds/latest_stories",                   "country": "China",        "language": "EN"},
    "ChinaFile":            {"url": "https://www.chinafile.com/rss.xml",                                "country": "China",        "language": "EN"},
    "What's on Weibo":      {"url": "https://www.whatsonweibo.com/feed/",                               "country": "China",        "language": "EN"},
    "Radii China":          {"url": "https://radiichina.com/feed/",                                     "country": "China",        "language": "EN"},
    "Hong Kong Free Press": {"url": "https://hongkongfp.com/feed/",                                     "country": "China",        "language": "EN"},
    "Taiwan News":          {"url": "https://www.taiwannews.com.tw/en/feed",                            "country": "China",        "language": "EN"},
    "Caixin Global":        {"url": "https://www.caixinglobal.com/feeds.rss",                           "country": "China",        "language": "EN"},
    "Xinhua English":       {"url": "http://www.xinhuanet.com/english/rss/worldrss.xml",                "country": "China",        "language": "EN"},
    "People's Daily EN":    {"url": "http://en.people.cn/rss/90001.xml",                                "country": "China",        "language": "EN"},
    "SupChina":             {"url": "https://supchina.com/feed/",                                       "country": "China",        "language": "EN"},
    "China Digital Times":  {"url": "https://chinadigitaltimes.net/feed/",                              "country": "China",        "language": "EN"},

    # ── Uganda ────────────────────────────────────────────────────────────────
    "Daily Monitor UG":     {"url": "https://www.monitor.co.ug/uganda/feed",                            "country": "Uganda",       "language": "EN"},
    "New Vision UG":        {"url": "https://www.newvision.co.ug/rss",                                  "country": "Uganda",       "language": "EN"},
    "Observer Uganda":      {"url": "https://observer.ug/feed",                                         "country": "Uganda",       "language": "EN"},
    "Nile Post":            {"url": "https://nilepost.co.ug/feed/",                                     "country": "Uganda",       "language": "EN"},
    "Chimp Reports":        {"url": "https://chimpreports.com/feed/",                                   "country": "Uganda",       "language": "EN"},
    "The Independent UG":   {"url": "https://www.independent.co.ug/feed/",                              "country": "Uganda",       "language": "EN"},
    "Softpower Uganda":     {"url": "https://softpowerug.com/feed/",                                    "country": "Uganda",       "language": "EN"},
    "URN Uganda":           {"url": "https://ugandaradionetwork.net/feed",                               "country": "Uganda",       "language": "EN"},
    "Kampala Post":         {"url": "https://www.kampalapost.com/feed",                                 "country": "Uganda",       "language": "EN"},
    "African Arguments UG": {"url": "https://africanarguments.org/feed/",                               "country": "Uganda",       "language": "EN"},
    "Bukedde":              {"url": "https://bukedde.co.ug/feed/",                                      "country": "Uganda",       "language": "EN"},
    "NTV Uganda":           {"url": "https://www.ntvuganda.co.ug/feeds",                                "country": "Uganda",       "language": "EN"},
    "Kool FM Uganda":       {"url": "https://kfm.co.ug/feed/",                                          "country": "Uganda",       "language": "EN"},
    "Eagle Online UG":      {"url": "https://www.eagleonline.co.ug/feed/",                              "country": "Uganda",       "language": "EN"},
    "The Tower Post UG":    {"url": "https://thetowerpost.com/feed/",                                   "country": "Uganda",       "language": "EN"},

    # ── Finnland ──────────────────────────────────────────────────────────────
    "Yle News EN":          {"url": "https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_NEWS", "country": "Finland",      "language": "EN"},
    "Yle Uutiset FI":       {"url": "https://feeds.yle.fi/uutiset/v1/majorHeadlines/YLE_UUTISET.rss",   "country": "Finland",      "language": "FI"},
    "Helsingin Sanomat":    {"url": "https://www.hs.fi/rss/tuoreimmat.xml",                              "country": "Finland",      "language": "FI"},
    "Iltalehti":            {"url": "https://www.iltalehti.fi/rss/uutiset.xml",                         "country": "Finland",      "language": "FI"},
    "Ilta-Sanomat":         {"url": "https://www.is.fi/rss/tuoreimmat.xml",                             "country": "Finland",      "language": "FI"},
    "MTV Uutiset":          {"url": "https://www.mtvuutiset.fi/rss/uutiset.xml",                        "country": "Finland",      "language": "FI"},
    "Kauppalehti":          {"url": "https://feeds.kauppalehti.fi/rss/main",                            "country": "Finland",      "language": "FI"},
    "Uusi Suomi":           {"url": "https://www.uusisuomi.fi/feed",                                    "country": "Finland",      "language": "FI"},
    "Aamulehti":            {"url": "https://www.aamulehti.fi/rss.xml",                                 "country": "Finland",      "language": "FI"},
    "Turun Sanomat":        {"url": "https://www.ts.fi/rss",                                            "country": "Finland",      "language": "FI"},
    "Vihrea Lanka":         {"url": "https://www.vihrealanka.fi/feed",                                  "country": "Finland",      "language": "FI"},
    "Maaseudun Tulevaisuus":{"url": "https://www.maaseuduntulevaisuus.fi/rss.xml",                      "country": "Finland",      "language": "FI"},
    "Savon Sanomat":        {"url": "https://www.savonsanomat.fi/rss.xml",                              "country": "Finland",      "language": "FI"},
    "Kaleva FI":            {"url": "https://www.kaleva.fi/rss.xml",                                    "country": "Finland",      "language": "FI"},
    "Taloussanomat":        {"url": "https://www.is.fi/taloussanomat/rss.xml",                          "country": "Finland",      "language": "FI"},

    # ── Türkei ────────────────────────────────────────────────────────────────
    "Hurriyet Daily News":  {"url": "https://www.hurriyetdailynews.com/rss.aspx",                       "country": "Turkey",       "language": "EN"},
    "Daily Sabah":          {"url": "https://www.dailysabah.com/rssFeed/turkey",                        "country": "Turkey",       "language": "EN"},
    "Bianet EN":            {"url": "https://bianet.org/english/rss",                                   "country": "Turkey",       "language": "EN"},
    "Anadolu Agency EN":    {"url": "https://www.aa.com.tr/en/rss/default?cat=trending",                "country": "Turkey",       "language": "EN"},
    "Cumhuriyet":           {"url": "https://www.cumhuriyet.com.tr/rss/son_dakika.xml",                 "country": "Turkey",       "language": "TR"},
    "Hurriyet TR":          {"url": "https://www.hurriyet.com.tr/rss/anasayfa",                         "country": "Turkey",       "language": "TR"},
    "Milliyet":             {"url": "https://www.milliyet.com.tr/rss/rssNew/GuncelRss.xml",             "country": "Turkey",       "language": "TR"},
    "Sabah TR":             {"url": "https://www.sabah.com.tr/rss/anasayfa.xml",                        "country": "Turkey",       "language": "TR"},
    "BirGun":               {"url": "https://www.birgun.net/feed",                                      "country": "Turkey",       "language": "TR"},
    "Gazete Duvar":         {"url": "https://www.gazeteduvar.com.tr/feed",                              "country": "Turkey",       "language": "TR"},
    "T24 TR":               {"url": "https://t24.com.tr/rss",                                           "country": "Turkey",       "language": "TR"},
    "Sozcu":                {"url": "https://www.sozcu.com.tr/rss.xml",                                 "country": "Turkey",       "language": "TR"},
    "Haberturk":            {"url": "https://www.haberturk.com/rss",                                    "country": "Turkey",       "language": "TR"},
    "Bianet TR":            {"url": "https://bianet.org/feeds/genel.rss",                               "country": "Turkey",       "language": "TR"},
    "Karar TR":             {"url": "https://www.karar.com/rss.xml",                                    "country": "Turkey",       "language": "TR"},

    # ── Iran ──────────────────────────────────────────────────────────────────
    "Tehran Times":         {"url": "https://www.tehrantimes.com/rss",                                  "country": "Iran",         "language": "EN"},
    "Iran International":   {"url": "https://www.iranintl.com/en/rss",                                  "country": "Iran",         "language": "EN"},
    "IranWire":             {"url": "https://iranwire.com/en/feed",                                     "country": "Iran",         "language": "EN"},
    "IRNA English":         {"url": "https://en.irna.ir/rss",                                           "country": "Iran",         "language": "EN"},
    "Press TV":             {"url": "https://www.presstv.ir/rssFeed/1.xml",                             "country": "Iran",         "language": "EN"},
    "Financial Tribune":    {"url": "https://financialtribune.com/rss",                                 "country": "Iran",         "language": "EN"},
    "Iran Front Page":      {"url": "https://ifpnews.com/feed/",                                        "country": "Iran",         "language": "EN"},
    "Kayhan London":        {"url": "https://kayhan.london/feed/",                                      "country": "Iran",         "language": "EN"},
    "Iran Human Rights":    {"url": "https://iranhr.net/en/feed/",                                      "country": "Iran",         "language": "EN"},
    "Radio Farda EN":       {"url": "https://en.radiofarda.com/api/epiqq",                              "country": "Iran",         "language": "EN"},
    "BBC Persian":          {"url": "https://www.bbc.com/persian/rss.xml",                              "country": "Iran",         "language": "FA"},
    "VOA Persian":          {"url": "https://www.radiofarda.com/api/zmoqmemyqu",                        "country": "Iran",         "language": "FA"},
    "Manoto News":          {"url": "https://www.manototv.com/news/rss",                                "country": "Iran",         "language": "FA"},
    "Iran Wire FA":         {"url": "https://iranwire.com/fa/feed",                                     "country": "Iran",         "language": "FA"},
    "Zan Iran":             {"url": "https://zaniiran.com/feed/",                                       "country": "Iran",         "language": "FA"},

    # ── Südafrika ─────────────────────────────────────────────────────────────
    "Mail and Guardian":    {"url": "https://mg.co.za/feed/",                                           "country": "South Africa", "language": "EN"},
    "Daily Maverick":       {"url": "https://www.dailymaverick.co.za/feed/",                            "country": "South Africa", "language": "EN"},
    "TimesLive":            {"url": "https://www.timeslive.co.za/rss/",                                 "country": "South Africa", "language": "EN"},
    "News24":               {"url": "https://feeds.news24.com/articles/news24/TopStories/rss",          "country": "South Africa", "language": "EN"},
    "The Citizen ZA":       {"url": "https://citizen.co.za/feed/",                                     "country": "South Africa", "language": "EN"},
    "IOL ZA":               {"url": "https://www.iol.co.za/rss",                                       "country": "South Africa", "language": "EN"},
    "GroundUp":             {"url": "https://www.groundup.org.za/feed/",                                "country": "South Africa", "language": "EN"},
    "Bhekisisa":            {"url": "https://bhekisisa.org/feed/",                                      "country": "South Africa", "language": "EN"},
    "Eyewitness News":      {"url": "https://ewn.co.za/Feed/latest",                                    "country": "South Africa", "language": "EN"},
    "Maverick Citizen":     {"url": "https://www.dailymaverick.co.za/maverickcitizen/feed/",            "country": "South Africa", "language": "EN"},
    "The South African":    {"url": "https://www.thesouthafrican.com/feed/",                            "country": "South Africa", "language": "EN"},
    "Business Day ZA":      {"url": "https://businesslive.co.za/rss/bd/",                               "country": "South Africa", "language": "EN"},
    "African Arguments ZA": {"url": "https://africanarguments.org/feed/",                               "country": "South Africa", "language": "EN"},
    "Daily Sun ZA":         {"url": "https://www.dailysun.co.za/rss.xml",                               "country": "South Africa", "language": "EN"},
    "Feminist SA":          {"url": "https://www.feministsa.org/feed/",                                 "country": "South Africa", "language": "EN"},

    # ── Indien ────────────────────────────────────────────────────────────────
    "The Hindu":            {"url": "https://www.thehindu.com/feeder/default.rss",                      "country": "India",        "language": "EN"},
    "Times of India":       {"url": "https://timesofindia.indiatimes.com/rss.cms",                      "country": "India",        "language": "EN"},
    "NDTV":                 {"url": "https://feeds.feedburner.com/ndtvnews-india-news",                  "country": "India",        "language": "EN"},
    "Indian Express":       {"url": "https://indianexpress.com/feed/",                                  "country": "India",        "language": "EN"},
    "Hindustan Times":      {"url": "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",  "country": "India",        "language": "EN"},
    "The Wire IN":          {"url": "https://thewire.in/feed",                                          "country": "India",        "language": "EN"},
    "Scroll.in":            {"url": "https://scroll.in/feed",                                           "country": "India",        "language": "EN"},
    "The Print":            {"url": "https://theprint.in/feed/",                                        "country": "India",        "language": "EN"},
    "Feminism in India":    {"url": "https://feminisminindia.com/feed/",                                "country": "India",        "language": "EN"},
    "LiveMint":             {"url": "https://www.livemint.com/rss/news",                                "country": "India",        "language": "EN"},
    "The Quint":            {"url": "https://www.thequint.com/feeds/home",                              "country": "India",        "language": "EN"},
    "Outlook India":        {"url": "https://www.outlookindia.com/rss/main/magazine",                   "country": "India",        "language": "EN"},
    "News Laundry":         {"url": "https://www.newslaundry.com/feed",                                 "country": "India",        "language": "EN"},
    "The Caravan IN":       {"url": "https://caravanmagazine.in/feed",                                  "country": "India",        "language": "EN"},
    "Tribune India":        {"url": "https://www.tribuneindia.com/rss/feed.xml",                        "country": "India",        "language": "EN"},
}

ALWAYS_INCLUDE_SOURCES = {
    "EMMA", "queer.de", "L-MAG",
    "Ms. Magazine", "Feminism in India", "Feminist SA",
}

# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────
KEYWORDS = [
    # Deutsch
    "frauen", "frau", "maedchen", "weiblich", "feminismus", "feministisch",
    "gleichberechtigung", "frauenrechte", "lohnluecke", "entgeltungleichheit",
    "abtreibung", "schwangerschaftsabbruch", "sexismus", "misogynie",
    "haeusliche gewalt", "femizid", "sexuelle belaestigung", "metoo",
    "schwul", "lesbisch", "bisexuell", "transgender", "queer",
    "nicht-binaer", "homophobie", "transphobie", "csd", "pride",
    "fluechtling", "migration", "menschenrechte", "diskriminierung",
    "geschlechtergleichheit", "patriarchat", "frauenbewegung",
    "menstruation", "verhuetung", "mutterschaft", "elternzeit",
    "care-arbeit", "glaeserne decke", "frauenquote",
    # Englisch
    "women", "woman", "girl", "girls", "female", "feminism", "feminist",
    "gender equality", "gender gap", "gender pay gap", "equal pay",
    "reproductive rights", "abortion", "women's rights", "sexism", "misogyny",
    "domestic violence", "gender violence", "femicide",
    "sexual harassment", "sexual assault", "rape", "metoo", "me too",
    "lgbt", "lgbtq", "lgbtqia", "queer", "gay", "lesbian", "bisexual",
    "transgender", "trans ", "nonbinary", "non-binary", "intersex",
    "pride", "same-sex", "gay rights", "trans rights",
    "homophobia", "transphobia", "conversion therapy",
    "immigration", "refugee", "asylum", "migrant",
    "human rights", "civil rights", "discrimination", "racism",
    "maternal", "pregnancy", "contraception", "menstruation",
    "gender-based violence", "honor killing", "child marriage",
    "girls education", "women empowerment",
    # Spanisch
    "mujeres", "mujer", "feminismo", "feminista", "igualdad de genero",
    "aborto", "violencia de genero", "acoso sexual",
    "lesbiana", "transgenero", "orgullo", "derechos de la mujer",
    "discriminacion", "refugiada", "migrante",
    # Italienisch
    "donne", "donna", "femminismo", "femminista", "parita di genere",
    "violenza di genere", "molestie sessuali",
    "lesbica", "transgender", "orgoglio", "diritti delle donne",
    "discriminazione", "rifugiata",
    # Finnisch
    "naiset", "nainen", "feminismi", "tasa-arvo",
    "sukupuolisyrjinta", "lgbtq", "lesbo", "transsukupuolinen",
    "ylpeys", "naisten oikeudet", "syrjinta", "pakolainen",
    # Türkisch
    "kadin", "kadinlar", "feminizm", "feminist",
    "toplumsal cinsiyet", "kurtaj", "cinsel taciz",
    "lezbiyen", "onur", "kadin haklari", "ayrimcilik", "multeci",
    # Französisch (für CH)
    "femmes", "feminisme", "feministe", "egalite des genres",
    "avortement", "violence conjugale", "harcelement sexuel",
    "lesbienne", "transgenre", "fierte", "droits des femmes",
    "discrimination", "refugiee", "migrant",
]

# ─────────────────────────────────────────────────────────────────────────────
#  TOPIC KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "Reproduktive Rechte": [
        "abtreibung", "schwangerschaftsabbruch", "reproduktive rechte",
        "geburtenkontrolle", "verhuetung", "schwangerschaft", "mutterschaft",
        "menstruation", "frauengesundheit",
        "reproductive", "abortion", "pro-choice", "birth control",
        "contraception", "fertility", "pregnancy", "maternal",
        "aborto", "kurtaj", "avortement",
    ],
    "Lohnluecke & Wirtschaft": [
        "lohnluecke", "entgeltungleichheit", "glaeserne decke", "frauenquote",
        "care-arbeit", "mutterschaftsstrafe", "elterngeld",
        "pay gap", "wage gap", "equal pay", "gender pay", "glass ceiling",
        "motherhood penalty", "parental leave",
        "brecha salarial", "tetto di cristallo", "ecart de salaire",
    ],
    "LGBTQIA+": [
        "schwul", "lesbisch", "bisexuell", "transgender", "queer",
        "nicht-binaer", "homophobie", "transphobie", "csd", "ehe fuer alle",
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian",
        "trans rights", "homophobia", "transphobia", "pride", "same-sex",
        "lesbiana", "lesbica", "lesbo", "lezbiyen", "lesbienne",
    ],
    "Migration & Asyl": [
        "fluechtling", "gefluechtete", "migration", "abschiebung",
        "immigration", "refugee", "asylum", "migrant", "deportation",
        "refugiada", "rifugiata", "pakolainen", "multeci", "refugiee",
    ],
    "Menschenrechte": [
        "menschenrechte", "buergerrechte", "diskriminierung", "rassismus",
        "human rights", "civil rights", "discrimination", "racism",
        "derechos humanos", "diritti umani", "droits humains",
    ],
    "Gesundheit & Medizin": [
        "gesundheit", "medizin", "psychische gesundheit", "brustkrebs",
        "health", "healthcare", "mental health", "cancer", "gender affirming care",
        "salud", "salute", "terveys", "sante",
    ],
    "Recht & Politik": [
        "gesetz", "gericht", "urteil", "verbot",
        "law", "legal", "court", "lawsuit", "legislation", "ruling", "ban",
        "ley", "legge", "laki", "kanun", "loi",
    ],
    "Politik & Regierung": [
        "wahl", "bundestag", "politikerin", "koalition",
        "election", "parliament", "minister", "president", "government",
        "eleccion", "elezione", "vaali", "secim", "election",
    ],
    "Kultur & Medien": [
        "film", "buch", "musik", "kunst", "repraesentation",
        "film", "book", "music", "art", "representation",
        "pelicula", "elokuva", "film",
    ],
    "Sport": [
        "sport", "athletin", "olympia", "frauen im sport",
        "athlete", "olympic", "world cup", "women in sport", "transgender athlete",
        "deporte", "urheilu", "spor",
    ],
    "Gewalt & Sicherheit": [
        "gewalt", "femizid", "haeusliche gewalt", "vergewaltigung", "menschenhandel",
        "violence", "femicide", "domestic violence", "rape", "trafficking",
        "violencia", "violenza", "vaekivalta", "siddet", "violence",
    ],
    "Arbeit & Wirtschaft": [
        "arbeitsplatz", "karriere", "kinderbetreuung", "elternzeit",
        "workplace", "career", "leadership", "ceo", "childcare", "parental leave",
        "trabajo", "lavoro", "tyo", "is", "travail",
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
            pass
    conn.commit()
    conn.close()
    print("Database ready.")


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
        "frauen", "frau", "weiblich", "feminismus", "feministisch",
        "frauenrechte", "lohnluecke", "abtreibung", "sexismus", "femizid",
        "haeusliche gewalt", "metoo",
        "women", "woman", "girl", "female", "feminism", "feminist",
        "gender", "reproductive", "abortion", "sexism", "domestic violence",
        "mujeres", "mujer", "feminismo", "donne", "donna",
        "naiset", "nainen", "kadin", "femmes", "feminisme",
    ]
    lgbtq_terms = [
        "schwul", "lesbisch", "bisexuell", "transgender", "queer",
        "nicht-binaer", "homophobie", "transphobie", "csd",
        "lgbt", "lgbtq", "lgbtqia", "gay", "lesbian", "bisexual",
        "trans ", "nonbinary", "non-binary", "intersex", "pride",
        "homophobia", "transphobia",
        "lesbiana", "lesbica", "lesbo", "lezbiyen", "lesbienne",
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
        language = feed_info.get("language", "EN")
        print(f"  Scraping: {source_name}...", flush=True)

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

                DACH = {"Germany", "Austria", "Switzerland"}
always_keep = source_name in ALWAYS_INCLUDE_SOURCES or country not in DACH

                # Translate title for keyword matching (non-DE/EN sources)
                title_for_matching = title
                if language not in ("DE", "EN"):
                    title_for_matching = translate_to_german(title, language)

                if not always_keep and not matches_keywords(title_for_matching, summary):
                    continue

                # Use translated title for storage
                stored_title = title_for_matching if language not in ("DE", "EN") else title

                tags = get_matching_tags(title_for_matching + " " + summary)
                if source_name in {"queer.de", "L-MAG"}:
                    tags = list(set(tags + ["lgbtqia+"]))
                elif source_name in {"EMMA"}:
                    tags = list(set(tags + ["women"]))

                category = "lgbtqia+" if "lgbtqia+" in tags else "women"
                tags_str = ", ".join(sorted(set(tags))) if tags else "general"

                topics = get_topics(title_for_matching + " " + summary)
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
                        """, (hash_id, stored_title, link, summary, source_name, country,
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
                        """, (hash_id, stored_title, link, summary, source_name, country,
                              category, tags_str, topics_str, datetime.now().isoformat(),
                              published_at))
                        if cursor.rowcount > 0:
                            new_count += 1
                except Exception:
                    pass

            conn.commit()
            conn.close()
            print(f"     {new_count} new articles from {source_name}", flush=True)
            total_new += new_count

        except Exception as e:
            print(f"     Error scraping {source_name}: {e}", flush=True)

    print(f"\nDone! {total_new} new articles saved.", flush=True)


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
    print("Shared Ground Scraper startet...\n")
    setup_database()
    scrape_all_feeds()
