# [START FILE: abs-kosync-enhanced/storyteller_db.py]
import sqlite3
import os
import logging
import time
import json
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class StorytellerDB:
    def __init__(self):
        self.db_path = Path(os.environ.get("STORYTELLER_DB_PATH", "/storyteller_data/storyteller.db"))
        self.conn = None
        self._init_connection()

    def _init_connection(self):
        if not self.db_path.exists():
            logger.warning(f"Storyteller DB not found at {self.db_path}")
            return False
        
        try:
            # FIXED: Opened in Read-Write mode (removed mode=ro)
            # check_same_thread=False allows this connection to be used by the daemon loop
            self.conn = sqlite3.connect(f"file:{self.db_path}", uri=True, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            self.conn.execute("PRAGMA journal_mode=WAL;")
            
            logger.info(f"StorytellerDB: {self.db_path} (WAL mode, leapfrog=10000ms)")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Storyteller DB: {e}")
            return False

    def check_connection(self):
        if not self.conn:
            return self._init_connection()
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT count(*) FROM book")
            count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM position")
            pos_count = cur.fetchone()[0]
            mode = cur.execute("PRAGMA journal_mode").fetchone()[0]
            logger.info(f"✅ Storyteller DB: {count} books, {pos_count} positions (mode={mode})")
            return True
        except Exception as e:
            logger.error(f"Storyteller Check Failed: {e}")
            return False

    def get_progress(self, ebook_filename):
        if not self.conn: return None, None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT uuid FROM book WHERE title LIKE ?", (f"%{Path(ebook_filename).stem}%",))
            row = cursor.fetchone()
            if not row: return None, None
            book_uuid = row['uuid']
            cursor.execute("""
                SELECT locator, timestamp FROM position 
                WHERE book_uuid = ? ORDER BY timestamp DESC LIMIT 1
            """, (book_uuid,))
            pos_row = cursor.fetchone()
            if pos_row and pos_row['locator']:
                data = json.loads(pos_row['locator'])
                pct = data.get('locations', {}).get('totalProgression', 0)
                ts = pos_row['timestamp']
                return float(pct), ts
            return None, None
        except Exception as e:
            logger.error(f"Storyteller Fetch Error: {e}")
            return None, None

    def get_progress_with_fragment(self, ebook_filename):
        if not self.conn: return None, None, None, None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT uuid FROM book WHERE title LIKE ?", (f"%{Path(ebook_filename).stem}%",))
            row = cursor.fetchone()
            if not row: return None, None, None, None
            book_uuid = row['uuid']
            cursor.execute("""
                SELECT locator, timestamp FROM position 
                WHERE book_uuid = ? ORDER BY timestamp DESC LIMIT 1
            """, (book_uuid,))
            pos_row = cursor.fetchone()
            if pos_row and pos_row['locator']:
                data = json.loads(pos_row['locator'])
                pct = data.get('locations', {}).get('totalProgression', 0)
                href = data.get('href', '')
                frag_id = None
                if '#' in href:
                    href_parts = href.split('#')
                    href = href_parts[0]
                    frag_id = href_parts[1]
                return float(pct), pos_row['timestamp'], href, frag_id
            return None, None, None, None
        except Exception as e:
            logger.error(f"Storyteller Fragment Fetch Error: {e}")
            return None, None, None, None


    def update_progress(self, ebook_filename, percentage):
        """
        Update reading progress with aggressive timestamp leapfrog.
        
        Storyteller uses timestamp-based conflict resolution. The app caches positions
        and syncs them back, potentially overwriting our DB changes. To win the race:
        1. Use a timestamp far enough in the future to beat cached app positions
        2. Clear specific anchors (cssSelector, fragments) that override totalProgression
        3. Keep href/type so Storyteller knows which chapter we're in
        """
        try:
            with sqlite3.connect(f"file:{self.db_path}", uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("SELECT uuid FROM book WHERE title LIKE ?", (f"%{Path(ebook_filename).stem}%",))
                row = cursor.fetchone()
                if not row: return False
                book_uuid = row['uuid']
                
                cursor.execute("""
                    SELECT uuid, locator, timestamp FROM position 
                    WHERE book_uuid = ? ORDER BY timestamp DESC LIMIT 1
                """, (book_uuid,))
                pos_row = cursor.fetchone()
                
                if not pos_row:
                    logger.warning(f"No existing position for {ebook_filename}, skipping update")
                    return False
                
                current_ts = pos_row['timestamp'] if pos_row else 0
                now_ms = int(time.time() * 1000)
                
                # AGGRESSIVE LEAPFROG: Jump 10 seconds ahead of both current time AND existing timestamp
                # This ensures we beat any cached position the Storyteller app might sync back
                new_ts = max(now_ms, current_ts) + 10000  # 10 second leap
                
                locator = {}
                if pos_row['locator']:
                    try: locator = json.loads(pos_row['locator'])
                    except: pass
                
                if 'locations' not in locator: locator['locations'] = {}
                
                old_pct = locator.get('locations', {}).get('totalProgression', 0)
                
                # Skip if percentage hasn't changed meaningfully (avoid unnecessary writes)
                if abs(float(percentage) - old_pct) < 0.001:
                    return True
                
                # Update the percentage
                locator['locations']['totalProgression'] = float(percentage)
                
                # CRITICAL: Remove specific anchors that override totalProgression
                # Storyteller prioritizes cssSelector/fragments over totalProgression
                # for positioning. By removing them, we force it to use our percentage.
                for key in ['cssSelector', 'fragments', 'position', 'progression']:
                    if key in locator['locations']:
                        del locator['locations'][key]
                
                # Keep href and type - these tell Storyteller which chapter file,
                # and it will calculate the specific position from totalProgression
                
                cursor.execute(
                    "UPDATE position SET locator = ?, timestamp = ? WHERE uuid = ?",
                    (json.dumps(locator), new_ts, pos_row['uuid'])
                )

                conn.commit()
                logger.info(f"✅ Storyteller: {ebook_filename} → {percentage:.1%} (ts={new_ts}, leap={new_ts - current_ts}ms)")
                return True
        except Exception as e:
            logger.error(f"Storyteller write error: {e}")
            return False


    def get_recent_activity(self, hours=24, min_progress=0.01):
        if not self.conn: return []
        cutoff_ms = int((time.time() - (hours * 3600)) * 1000)
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT DISTINCT b.uuid, b.title, p.locator, p.timestamp
            FROM book b JOIN position p ON b.uuid = p.book_uuid
            WHERE p.timestamp > ? ORDER BY p.timestamp DESC
            """
            cursor.execute(query, (cutoff_ms,))
            rows = cursor.fetchall()
            results = []
            seen_uuids = set()
            for row in rows:
                if row['uuid'] in seen_uuids: continue
                try:
                    locator = json.loads(row['locator']) if row['locator'] else {}
                    pct = locator.get('locations', {}).get('totalProgression', 0.0)
                    if pct >= min_progress:
                        results.append({
                            "id": row['uuid'],
                            "title": row['title'],
                            "progress": pct,
                            "source": "STORYTELLER"
                        })
                        seen_uuids.add(row['uuid'])
                except json.JSONDecodeError: continue
            return results
        except Exception as e:
            logger.error(f"Failed to query Storyteller activity: {e}")
            return []

    def add_to_collection(self, ebook_filename):
        """Placeholder for adding books to a Storyteller collection."""
        # Storyteller collections are managed via the app UI
        # This is a no-op but keeps the interface consistent
        pass

    def get_book_uuid(self, ebook_filename):
        """Get the Storyteller book UUID for a given ebook filename."""
        if not self.conn: return None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT uuid FROM book WHERE title LIKE ?", (f"%{Path(ebook_filename).stem}%",))
            row = cursor.fetchone()
            return row['uuid'] if row else None
        except Exception as e:
            logger.error(f"Error getting book UUID: {e}")
            return None

    def force_position_update(self, ebook_filename, percentage, target_href=None):
        """
        Force update position with maximum aggression.
        
        This method:
        1. Uses a timestamp 60 seconds in the future
        2. Completely rebuilds the locator from scratch
        3. Only keeps the bare minimum needed for Storyteller to work
        
        Use this when normal update_progress isn't winning the sync race.
        """
        try:
            with sqlite3.connect(f"file:{self.db_path}", uri=True, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("SELECT uuid FROM book WHERE title LIKE ?", (f"%{Path(ebook_filename).stem}%",))
                row = cursor.fetchone()
                if not row: return False
                book_uuid = row['uuid']
                
                cursor.execute("""
                    SELECT uuid, locator, timestamp FROM position 
                    WHERE book_uuid = ? ORDER BY timestamp DESC LIMIT 1
                """, (book_uuid,))
                pos_row = cursor.fetchone()
                
                if not pos_row:
                    return False
                
                now_ms = int(time.time() * 1000)
                # EXTREME: 60 second leap into the future
                new_ts = now_ms + 60000
                
                # Build minimal locator - ONLY what Storyteller needs
                old_locator = {}
                if pos_row['locator']:
                    try: old_locator = json.loads(pos_row['locator'])
                    except: pass
                
                locator = {
                    "type": old_locator.get("type", "application/xhtml+xml"),
                    "locations": {
                        "totalProgression": float(percentage)
                    }
                }
                
                # Keep href if we have it, or use provided target
                if target_href:
                    locator["href"] = target_href
                elif old_locator.get("href"):
                    locator["href"] = old_locator["href"]
                
                cursor.execute(
                    "UPDATE position SET locator = ?, timestamp = ? WHERE uuid = ?",
                    (json.dumps(locator), new_ts, pos_row['uuid'])
                )

                conn.commit()
                logger.info(f"⚡ Storyteller FORCE: {ebook_filename} → {percentage:.1%} (ts={new_ts})")
                return True
        except Exception as e:
            logger.error(f"Storyteller force update error: {e}")
            return False
# [END FILE]

