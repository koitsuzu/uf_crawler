import httpx
import asyncio
import sys
import datetime
import os
import json
import threading
import time
import re
from flask import Flask, render_template_string, request, jsonify
from bs4 import BeautifulSoup
from rich.console import Console
from urllib.parse import urljoin

if sys.stdout.encoding != 'utf-8' and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

app = Flask(__name__)
console = Console()

class UfretCrawler:
    NEW_URL = "https://www.ufret.jp/new.php"
    DATA_DIR = "data"
    DB_GENERAL = os.path.join(DATA_DIR, "general_pipeline.json")
    DB_VIDEO = os.path.join(DATA_DIR, "video_pipeline.json")
    DB_PERMANENT = os.path.join(DATA_DIR, "followed_songs_db.json")
    ARTISTS_FILE = os.path.join(DATA_DIR, "followed_artists.txt")
    FAVORITES_FILE = os.path.join(DATA_DIR, "favorites.txt")
    
    def __init__(self):
        if not os.path.exists(self.DATA_DIR): os.makedirs(self.DATA_DIR)
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        self.lock = threading.RLock()
        self.followed_artists = self.load_txt(self.ARTISTS_FILE)
        self.favorite_urls = self.load_txt(self.FAVORITES_FILE)
        self.db_general = self.load_json(self.DB_GENERAL)
        self.db_video = self.load_json(self.DB_VIDEO)
        self.db_perm = self.load_json(self.DB_PERMANENT)

    def load_txt(self, fn):
        if not os.path.exists(fn): return []
        try:
            with open(fn, "r", encoding="utf-8") as f: return [l.strip() for l in f if l.strip() and not l.startswith("#")]
        except: return []

    def save_txt(self, fn, d):
        with self.lock:
            try:
                with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(d)+"\n")
                return True
            except: return False

    def load_json(self, fn):
        if not os.path.exists(fn): return {}
        try:
            with open(fn, "r", encoding="utf-8") as f: return json.load(f)
        except: return {}

    def save_json(self, fn, d):
        try:
            with open(fn, "w", encoding="utf-8") as f: json.dump(d, f, ensure_ascii=False, indent=2)
            return True
        except: return False

    async def fetch_page(self, client, url):
        try:
            r = await client.get(url, headers=self.headers, follow_redirects=True, timeout=12)
            r.raise_for_status()
            return r.text
        except Exception as e:
            console.print(f"[red]Error {url}: {e}[/red]")
            return None

    def parse_song_item(self, item, base_url):
        try:
            link_tag = item if item.name == "a" else item.find("a")
            if not link_tag: return None
            url = urljoin(base_url, link_tag.get("href", ""))
            
            badges = [t.text.strip() for t in item.select("span.badge")]
            artist_tag = item.find("span", style=lambda s: s and ("font-size:12px" in s or "font-size: 12px" in s))
            artist = artist_tag.get_text().strip() if artist_tag else "Unknown"
            if artist.startswith("-"): artist = artist[1:].strip()

            full_text = link_tag.get_text("|||", strip=True)
            parts = [p.strip() for p in full_text.split("|||") if p.strip()]
            clean_parts = [p for p in parts if p!=artist and p not in badges and "追加" not in p and "NEW" not in p and p!="U-リク" and p!="-"]
            
            raw_title = clean_parts[0] if clean_parts else "Unknown"
            clean_pattern = r"(U-リク|NEW|追加|初心者|動画プラス|ピアノソロ|ソロ|初級|\d{4}/\d{2}/\d{2})"
            title = re.sub(clean_pattern, "", raw_title).strip().lstrip("-").strip()
            if not title: title = "Unknown Song"

            is_piano = any("ピアノ" in t for t in badges)
            is_video = any("動画" in t for t in badges)

            return {"title": title, "artist": artist, "url": url, "tags": badges, "is_piano": is_piano, "is_video": is_video, "discovered_at": datetime.datetime.now().strftime("%Y-%m-%d")}
        except: return None

    def deduplicate_songs(self, songs):
        seen = set()
        unique = []
        for s in songs:
            # Create a unique key based on title and artist
            key = (s["title"], s["artist"])
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    async def scrape_all(self):
        c_followed = self.load_txt(self.ARTISTS_FILE)
        c_favs = self.load_txt(self.FAVORITES_FILE)
        with self.lock:
            self.followed_artists = c_followed
            self.favorite_urls = c_favs
        
        async with httpx.AsyncClient() as client:
            html = await self.fetch_page(client, self.NEW_URL)
            if not html: return []
            soup = BeautifulSoup(html, "html.parser")
            items = soup.select("div.list-group a.list-group-item")
            scraped = []
            for i, item in enumerate(items):
                if i>=100: break # Increased buffer to 100
                s = self.parse_song_item(item, self.NEW_URL)
                if s: scraped.append(s)

            with self.lock:
                new_gen, new_vid = [], []
                for s in scraped:
                    if s["is_piano"]:
                        self.db_perm[s["url"]] = s
                        continue
                    if s["is_video"]:
                        new_vid.append(s)
                        continue
                    new_gen.append(s)
                    if any(f.lower() in s["artist"].lower() for f in self.followed_artists):
                        self.db_perm[s["url"]] = s
                
                # Combine new and old, then deduplicate by content (Title + Artist)
                # We prioritize new items (appearing first in the list)
                raw_gen = new_gen + list(self.db_general.values())
                updated_gen = self.deduplicate_songs(raw_gen)
                self.db_general = {s["url"]: s for s in updated_gen[:50]}
                self.save_json(self.DB_GENERAL, self.db_general)

                raw_vid = new_vid + list(self.db_video.values())
                updated_vid = self.deduplicate_songs(raw_vid)
                self.db_video = {s["url"]: s for s in updated_vid[:20]}
                self.save_json(self.DB_VIDEO, self.db_video)

                self.save_json(self.DB_PERMANENT, self.db_perm)
        return True

    async def add_url_manually(self, url):
        async with httpx.AsyncClient() as client:
            html = await self.fetch_page(client, url)
            if not html: return None
            soup = BeautifulSoup(html, "html.parser")
            h1 = soup.select_one("h1")
            
            # Basic parsing for manual add
            if h1:
                t_span = h1.find("span", style=lambda s: s and "font-weight:bold" in s)
                title = t_span.get_text().strip() if t_span else (h1.find(string=True, recursive=False) or "").strip()
                a_span = h1.find("span", style=lambda s: s and "font-size" in s)
                artist = a_span.get_text().strip() if a_span else "Unknown"
            else:
                title, artist = "Unknown", "Unknown"
            
            song = {
                "title": title, "artist": artist, "url": url, 
                "tags": [], "discovered_at": datetime.datetime.now().strftime("%Y-%m-%d"),
                "is_piano": False, "is_video": False
            }
            
            # Check for tags in manual add
            badges = [t.text.strip() for t in soup.select("span.badge")]
            song["is_piano"] = any("ピアノ" in t for t in badges)
            song["is_video"] = any("動画" in t for t in badges)
            song["tags"] = badges

            with self.lock:
                self.favorite_urls.append(url)
                if url not in self.db_perm:
                    self.db_perm[url] = song
                self.save_txt(self.FAVORITES_FILE, self.favorite_urls)
                self.save_json(self.DB_PERMANENT, self.db_perm)
            return song

    def get_data_for_ui(self):
        with self.lock:
            # Aggregate all known songs to find favorites
            all_known = {**self.db_general, **self.db_video, **self.db_perm}
            
            raw_piano = [s for s in self.db_perm.values() if s.get("is_piano")]
            raw_followed = [s for s in self.db_perm.values() if any(f.lower() in s.get("artist","").lower() for f in self.followed_artists)]
            raw_favorites = [s for s in all_known.values() if s["url"] in self.favorite_urls]

            return {
                "general": list(self.db_general.values()),
                "video": list(self.db_video.values()),
                "piano": self.deduplicate_songs(raw_piano),
                "followed": sorted(self.deduplicate_songs(raw_followed), key=lambda x: x.get("artist", "")),
                "favorites": self.deduplicate_songs(raw_favorites)
            }

crawler = UfretCrawler()

def run_scrape_sync():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(crawler.scrape_all())
    loop.close()

def run_add_url_sync(url):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(crawler.add_url_manually(url))
    loop.close()

def scheduler_thread():
    console.print("[bold blue]Scheduler started...[/bold blue]")
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target: target += datetime.timedelta(days=1)
        time.sleep((target-now).total_seconds())
        run_scrape_sync()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-Hant">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>U-FRETS PRO</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            /* Softer Dark Theme */
            --bg-color: #121212;
            --card-bg: #1e1e20;
            --card-hover: #252528;
            
            --primary-text: #f0f0f0;
            --secondary-text: #a1a1a6;
            
            --accent: #2997ff; 
            --accent-glow: rgba(41, 151, 255, 0.25);
            
            --pill-bg: #2c2c2e;
            --pill-active: #e1e1e1;
            --pill-text-active: #000000;
            
            --danger: #ff453a;   /* Red for Heart/Follow */
            --success: #32d74b;
            --warning: #ffd60a;  /* Yellow for Star/Save */
            
            --input-bg: #1c1c1e;
            --input-border: #333;
        }

        * { box-sizing: border-box; -webkit-font-smoothing: antialiased; }
        body {
            background-color: var(--bg-color);
            color: var(--primary-text);
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            margin: 0;
            padding: 40px;
        }

        .container { max-width: 1400px; margin: 0 auto; }

        header {
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        h1 {
            font-size: 2.5rem;
            font-weight: 700;
            margin: 0;
            letter-spacing: -0.02em;
            color: #fff;
        }

        /* Controls / Tabs */
        .controls {
            display: flex;
            gap: 12px;
            margin-bottom: 20px;
            overflow-x: auto;
            padding-bottom: 5px;
            align-items: center;
        }
        
        .btn {
            background: var(--pill-bg);
            color: #ccc;
            border: 1px solid transparent;
            padding: 10px 22px;
            border-radius: 20px;
            font-size: 1rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
            white-space: nowrap;
        }

        .btn:hover { background: #3a3a3c; color: #fff; }
        .btn.active {
            background: var(--pill-active);
            color: var(--pill-text-active);
            font-weight: 700;
            box-shadow: 0 4px 12px rgba(255,255,255,0.1);
        }

        .btn-sync {
            background: rgba(50, 215, 75, 0.15);
            color: var(--success);
            border: 1px solid rgba(50, 215, 75, 0.3);
            margin-left: auto; /* Push to right */
        }
        .btn-sync:hover { background: rgba(50, 215, 75, 0.25); }
        
        /* Management Bar */
        .mgmt-bar {
            display: flex;
            gap: 15px;
            margin-bottom: 30px;
            background: #18181a;
            padding: 15px;
            border-radius: 12px;
            border: 1px solid #333;
            align-items: center;
            flex-wrap: wrap;
        }
        .mgmt-group { display: flex; gap: 8px; align-items: center; flex: 1; min-width: 300px; }
        .mgmt-label { color: var(--secondary-text); font-size: 0.85rem; font-weight: 600; text-transform: uppercase; width: 80px; }
        
        .input-text {
            flex: 1;
            background: var(--input-bg);
            border: 1px solid var(--input-border);
            color: #fff;
            padding: 10px 15px;
            border-radius: 8px;
            font-family: inherit;
            outline: none;
            transition: border-color 0.2s;
        }
        .input-text:focus { border-color: var(--accent); }
        
        .btn-add {
            background: var(--pill-bg);
            border: 1px solid #444;
            color: #fff;
            padding: 10px 15px;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
        }
        .btn-add:hover { background: #444; }

        /* Grid & Cards */
        .grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr); /* Forced 5 columns */
            gap: 20px;
            animation: fadeIn 0.4s ease-out;
        }

        @media (max-width: 1200px) {
            .grid { grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); }
        }

        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

        .card {
            background: var(--card-bg);
            border-radius: 16px;
            padding: 24px;
            display: flex;
            flex-direction: column;
            transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s;
            position: relative;
            border: 1px solid rgba(255,255,255,0.05);
        }

        .card:hover {
            transform: translateY(-4px);
            box-shadow: 0 12px 40px rgba(0,0,0,0.5);
            background: var(--card-hover);
        }

        /* Highlight Followed Artists */
        .card.highlight {
            border: 1px solid var(--danger);
            box-shadow: 0 4px 20px rgba(255, 69, 58, 0.15);
            background: linear-gradient(180deg, rgba(255, 69, 58, 0.05) 0%, var(--card-bg) 100%);
        }

        .card-tag {
            position: absolute;
            top: 12px; right: 12px;
            font-size: 0.75rem;
            padding: 4px 10px;
            border-radius: 6px;
            font-weight: 700;
            text-transform: uppercase;
        }
        .tag-piano { background: rgba(16, 185, 129, 0.2); color: #10b981; }
        .tag-video { background: rgba(255, 71, 87, 0.2); color: #ff4757; }

        .title {
            font-size: 1.15rem;
            font-weight: 700;
            color: var(--primary-text);
            text-decoration: none;
            margin-bottom: 8px;
            line-height: 1.4;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .title:hover { color: var(--accent); }

        .artist {
            color: var(--secondary-text);
            font-size: 0.95rem;
            font-weight: 500;
            margin-bottom: 20px;
        }
        
        /* Highlight text color for followed artist */
        .highlight .artist { color: var(--danger); font-weight: 600; }

        .actions {
            margin-top: auto;
            display: flex;
            gap: 15px;
            align-items: center;
        }

        .action-icon {
            cursor: pointer;
            font-size: 1.5rem;
            background: transparent;
            border: none;
            padding: 5px;
            transition: transform 0.2s, color 0.2s;
            color: #d1d1d6; /* Default visible white-grey */
            line-height: 1;
        }
        .action-icon:hover { transform: scale(1.15); color: #fff; }
        
        /* Follow = Heart = Red */
        .follow-active { 
            color: var(--danger); 
            filter: drop-shadow(0 0 8px rgba(255, 69, 58, 0.4));
        }
        
        /* Save = Star = Yellow */
        .fav-active { 
            color: var(--warning); 
            filter: drop-shadow(0 0 8px rgba(255, 214, 10, 0.4));
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <div style="font-size:0.8rem; color:var(--secondary-text); margin-bottom:5px; font-weight:600; letter-spacing:0.1em;">DASHBOARD</div>
                <h1>U-FRETS <span style="font-size:0.4em; vertical-align:middle; opacity:0.8; font-weight:400; background:#333; color:#fff; padding:2px 8px; border-radius:6px;">PRO</span></h1>
            </div>
            <div style="text-align:right;">
                <div style="font-size:0.9rem; color:#666;">v18.2.0 Management</div>
            </div>
        </header>
        
        <!-- MANAGEMENT BAR -->
        <div class="mgmt-bar">
            <div class="mgmt-group">
                <span class="mgmt-label">Add Song</span>
                <input type="text" id="input-url" class="input-text" placeholder="Paste U-FRET URL here...">
                <button class="btn-add" onclick="addUrl()">Import</button>
            </div>
            <div class="mgmt-group">
                <span class="mgmt-label">Add Artist</span>
                <input type="text" id="input-artist" class="input-text" placeholder="Artist Name">
                <button class="btn-add" onclick="addArtist()">Follow</button>
            </div>
        </div>

        <div class="controls">
            <button class="btn active" onclick="show('general', this)">New Arrivals <span style="opacity:0.6;font-size:0.9em;margin-left:4px;">{{ gen_count }}</span></button>
            <button class="btn" onclick="show('video', this)">Videos <span style="opacity:0.6;font-size:0.9em;margin-left:4px;">{{ video_count }}</span></button>
            <button class="btn" onclick="show('piano', this)">Piano <span style="opacity:0.6;font-size:0.9em;margin-left:4px;">{{ piano_count }}</span></button>
            <button class="btn" onclick="show('followed', this)">Following <span style="opacity:0.6;font-size:0.9em;margin-left:4px;">{{ follow_count }}</span></button>
            <button class="btn" onclick="show('favorites', this)">Saved <span style="opacity:0.6;font-size:0.9em;margin-left:4px;">{{ fav_count }}</span></button>
            <button class="btn btn-sync" onclick="sync()">Sync Now</button>
        </div>
        
        {% macro card_macro(s) %}
        <div class="card {{ 'highlight' if highlight(s) else '' }}">
            {% if s.get('is_piano') %} <div class="card-tag tag-piano">PIANO</div> {% endif %}
            {% if s.get('is_video') %} <div class="card-tag tag-video">VIDEO</div> {% endif %}
            
            <a href="{{ s.url }}" class="title" target="_blank">{{ s.title }}</a>
            <div class="artist">{{ s.artist }}</div>
            
            <div class="actions">
                <!-- FOLLOW ARTIST = HEART (RED) -->
                <button class="action-icon {{ 'follow-active' if highlight(s) else '' }}" title="Follow Artist" onclick="toggle('follow', '{{ s.artist }}')">
                    {{ '♥' if highlight(s) else '♡' }}
                </button>
                
                <!-- SAVE SONG = STAR (YELLOW) -->
                <button class="action-icon {{ 'fav-active' if is_fav(s) else '' }}" title="Save Song" onclick="toggle('favorite', '{{ s.url }}')">
                    {{ '★' if is_fav(s) else '☆' }}
                </button>
            </div>
        </div>
        {% endmacro %}

        <div id="sec-general" class="grid">{% for s in data.general %} {{ card_macro(s) }} {% endfor %}</div>
        <div id="sec-video" class="grid" style="display:none;">{% for s in data.video %} {{ card_macro(s) }} {% endfor %}</div>
        <div id="sec-piano" class="grid" style="display:none;">{% for s in data.piano %} {{ card_macro(s) }} {% endfor %}</div>
        <div id="sec-followed" class="grid" style="display:none;">{% for s in data.followed %} {{ card_macro(s) }} {% endfor %}</div>
        <div id="sec-favorites" class="grid" style="display:none;">{% for s in data.favorites %} {{ card_macro(s) }} {% endfor %}</div>
    </div>
    
    <script>
        function show(id, btn) {
            document.querySelectorAll('.btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.grid').forEach(g => g.style.display = 'none');
            document.getElementById('sec-'+id).style.display = 'grid';
        }
        async function toggle(type, val) {
            await fetch('/api/'+type, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({value:val})});
            location.reload();
        }
        async function sync() {
            const btn = document.querySelector('.btn-sync');
            btn.innerText = "Syncing...";
            btn.style.opacity = 0.7;
            await fetch('/api/sync', {method:'POST'});
            setTimeout(() => location.reload(), 2000); 
        }
        async function addUrl() {
            const url = document.getElementById('input-url').value;
            if(!url) return;
            const btn = event.target; btn.innerText = "...";
            await fetch('/api/add_url', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:url})});
            location.reload();
        }
        async function addArtist() {
            const name = document.getElementById('input-artist').value;
            if(!name) return;
            await fetch('/api/follow', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({value:name})});
            location.reload();
        }
    </script>
</body>
</html>
"""

def highlight(s): return any(f.lower() in s.get("artist","").lower() for f in crawler.followed_artists)
def is_fav(s): return s["url"] in crawler.favorite_urls

@app.context_processor
def utility_processor(): return dict(highlight=highlight, is_fav=is_fav)

@app.route("/")
def index():
    try:
        # Check if there are any favorites without metadata in DB
        with crawler.lock:
            missing_metadata = [url for url in crawler.favorite_urls if url not in crawler.db_perm]
        
        if missing_metadata:
            console.print(f"[yellow]Found {len(missing_metadata)} favorites with missing metadata. Fetching now...[/yellow]")
            for url in missing_metadata:
                run_add_url_sync(url)
        
        data = crawler.get_data_for_ui()
        data["general"].sort(key=lambda x: x.get("discovered_at", ""), reverse=True)
        return render_template_string(HTML_TEMPLATE, data=data, 
                                     gen_count=len(data["general"]),
                                     video_count=len(data["video"]),
                                     piano_count=len(data["piano"]),
                                     follow_count=len(data["followed"]),
                                     fav_count=len(data["favorites"]))
    except Exception as e: return f"Error: {e}", 500

@app.route("/api/sync", methods=["POST"])
def api_sync():
    threading.Thread(target=run_scrape_sync).start()
    return jsonify({"status": "started"})

@app.route("/api/follow", methods=["POST"])
def api_follow():
    artist = request.json.get("value")
    with crawler.lock:
        c = crawler.load_txt(crawler.ARTISTS_FILE)
        if artist not in c: c.append(artist)
        else: c.remove(artist)
        crawler.save_txt(crawler.ARTISTS_FILE, c)
        crawler.followed_artists = c
    return jsonify({"status": "success"})

@app.route("/api/favorite", methods=["POST"])
def api_favorite():
    url = request.json.get("value")
    with crawler.lock:
        c = crawler.load_txt(crawler.FAVORITES_FILE)
        if url not in c: c.append(url)
        else: c.remove(url)
        crawler.save_txt(crawler.FAVORITES_FILE, c)
        crawler.favorite_urls = c
    return jsonify({"status": "success"})

@app.route("/api/add_url", methods=["POST"])
def api_add_url():
    url = request.json.get("url")
    if url: 
        threading.Thread(target=run_add_url_sync, args=(url,)).start()
    return jsonify({"status": "started"})

if __name__ == "__main__":
    console.print("[bold white on black] U-FRETS PRO v19.1.0 [/bold white on black]")
    threading.Thread(target=scheduler_thread, daemon=True).start()
    app.run(port=5000, debug=False)
