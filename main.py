# [START FILE: abs-kosync-enhanced/main.py]
import os
import time
import json
import schedule
import logging
import sys
from pathlib import Path
from zipfile import ZipFile
import lxml.etree as ET

from json_db import JsonDB
from api_clients import ABSClient, KoSyncClient
from hardcover_client import HardcoverClient
from transcriber import AudioTranscriber
from ebook_utils import EbookParser
from suggestion_manager import SuggestionManager

# Try to import Storyteller API client first, fall back to direct DB
try:
    from storyteller_api import StorytellerDBWithAPI as StorytellerClient
    STORYTELLER_MODE = "api"
except ImportError:
    from storyteller_db import StorytellerDB as StorytellerClient
    STORYTELLER_MODE = "sqlite"

# Logging setup
TRACE_LEVEL_NUM = 5
logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")
def trace(self, message, *args, **kws):
    if self.isEnabledFor(TRACE_LEVEL_NUM): self._log(TRACE_LEVEL_NUM, message, args, **kws)
logging.Logger.trace = trace
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
DATA_DIR = Path("/data")
BOOKS_DIR = Path("/books")
DB_FILE = DATA_DIR / "mapping_db.json"
STATE_FILE = DATA_DIR / "last_state.json"

class SyncManager:
    def __init__(self):
        logger.info("=== Sync Manager Starting (Release 4 - API Support) ===")
        self.abs_client = ABSClient()
        self.kosync_client = KoSyncClient()
        self.hardcover_client = HardcoverClient()
        self.storyteller_db = StorytellerClient()
        self.transcriber = AudioTranscriber(DATA_DIR)
        self.ebook_parser = EbookParser(BOOKS_DIR)
        self.db_handler = JsonDB(DB_FILE)
        self.state_handler = JsonDB(STATE_FILE)
        self.db = self.db_handler.load(default={"mappings": []})
        self.state = self.state_handler.load(default={})
        self.suggestion_manager = SuggestionManager(DATA_DIR, self.ebook_parser, self.abs_client, self.storyteller_db)
        
        self.delta_abs_thresh = float(os.getenv("SYNC_DELTA_ABS_SECONDS", 60))
        self.delta_kosync_thresh = float(os.getenv("SYNC_DELTA_KOSYNC_PERCENT", 1)) / 100.0
        self.delta_char_thresh = float(os.getenv("SYNC_DELTA_KOSYNC_WORDS", 400)) * 5
        self.regression_threshold = float(os.getenv("SYNC_REGRESSION_THRESHOLD", 5)) / 100.0
        
        self.startup_checks()
        self.cleanup_stale_jobs()

    def startup_checks(self):
        logger.info("--- Connectivity Checks ---")
        self.abs_client.check_connection()
        self.kosync_client.check_connection()
        self.storyteller_db.check_connection()
        if self.hardcover_client.token:
            if self.hardcover_client.get_user_id(): logger.info("✅ Connected to Hardcover")
            else: logger.warning("⚠️ Hardcover token failed")

    def cleanup_stale_jobs(self):
        changed = False
        for mapping in self.db.get('mappings', []):
            if mapping.get('status') == 'crashed':
                mapping['status'] = 'active'
                changed = True
        if changed: self.db_handler.save(self.db)

    def _get_abs_title(self, ab):
        """Extract title from audiobook item."""
        media = ab.get('media', {})
        metadata = media.get('metadata', {})
        return metadata.get('title') or ab.get('name', 'Unknown')

    def _automatch_hardcover(self, mapping):
        if not self.hardcover_client.token: return
        item = self.abs_client.get_item_details(mapping['abs_id'])
        if not item: return
        meta = item.get('media', {}).get('metadata', {})
        match = None
        if meta.get('isbn'): match = self.hardcover_client.search_by_isbn(meta.get('isbn'))
        if not match and meta.get('title'): match = self.hardcover_client.search_by_title_author(meta.get('title'), meta.get('authorName'))
        if match:
             mapping.update({'hardcover_book_id': match['book_id'], 'hardcover_edition_id': match.get('edition_id'), 'hardcover_pages': match.get('pages')})
             self.db_handler.save(self.db)
             self.hardcover_client.update_status(match['book_id'], 2, match.get('edition_id'))

    def _sync_to_hardcover(self, mapping, percentage):
        if not self.hardcover_client.token or not mapping.get('hardcover_book_id'): return
        ub = self.hardcover_client.find_user_book(mapping['hardcover_book_id'])
        if ub:
             page_num = int((mapping.get('hardcover_pages') or 0) * percentage)
             self.hardcover_client.update_progress(ub['id'], page_num, mapping.get('hardcover_edition_id'))
             if percentage > 0.99: self.hardcover_client.update_status(mapping['hardcover_book_id'], 3, mapping.get('hardcover_edition_id'))

    def _get_transcript_duration(self, transcript_path):
        try:
            with open(transcript_path, 'r') as f: data = json.load(f)
            if isinstance(data, list) and data: return data[-1].get('end', 0)
            return data.get('duration', 0) if isinstance(data, dict) else 0
        except: return 0

    def _abs_to_percentage(self, abs_seconds, transcript_path):
        duration = self._get_transcript_duration(transcript_path)
        return min(max(abs_seconds / duration, 0.0), 1.0) if duration > 0 else None

    def check_pending_jobs(self):
        self.db = self.db_handler.load(default={"mappings": []})
        for mapping in self.db.get('mappings', []):
            if mapping.get('status') == 'pending':
                logger.info(f"[JOB] Starting: {mapping.get('abs_title')}")
                mapping['status'] = 'processing'
                self.db_handler.save(self.db)
                try:
                    audio_files = self.abs_client.get_audio_files(mapping['abs_id'])
                    if not audio_files: raise Exception("No audio files")
                    transcript_path = self.transcriber.process_audio(mapping['abs_id'], audio_files)
                    self.ebook_parser.extract_text_and_map(mapping['ebook_filename'])
                    mapping.update({'transcript_file': str(transcript_path), 'status': 'active'})
                    self.db_handler.save(self.db)
                    self._automatch_hardcover(mapping)
                except Exception as e:
                    logger.error(f"[FAIL] {mapping.get('abs_title')}: {e}")
                    mapping['status'] = 'failed_retry_later'
                    self.db_handler.save(self.db)

    def run_discovery(self):
        logger.info("🔍 Discovery cycle starting...")
        self.db = self.db_handler.load(default={"mappings": []})
        mapped_ids = [m['abs_id'] for m in self.db['mappings']]
        self.suggestion_manager.run_discovery_cycle(mapped_ids)
        logger.info("✅ Discovery cycle complete.")

    def get_text_from_storyteller_fragment(self, ebook_filename, href, fragment_id):
        """Extract text from EPUB using Storyteller's fragment ID."""
        if not href or not fragment_id: return None
        try:
            epub_path = None
            for f in BOOKS_DIR.rglob(ebook_filename):
                epub_path = f
                break
            if not epub_path: return None
            
            with ZipFile(epub_path, 'r') as zip_ref:
                internal_path = href
                if internal_path not in zip_ref.namelist():
                    matching = [f for f in zip_ref.namelist() if href in f]
                    if matching: internal_path = matching[0]
                    else: return None
                
                with zip_ref.open(internal_path) as f:
                    content = f.read()
                    parser = ET.HTMLParser(encoding='utf-8')
                    tree = ET.fromstring(content, parser)
                    elements = tree.xpath(f"//*[@id='{fragment_id}']")
                    if elements: return "".join(elements[0].itertext()).strip()
        except Exception as e:
            logger.error(f"Fragment extraction error: {e}")
        return None

    def sync_cycle(self):
        logger.debug("--- Sync Cycle ---")
        self.db = self.db_handler.load(default={"mappings": []})
        for mapping in self.db.get('mappings', []):
            if mapping.get('status') != 'active': continue
            abs_id, ko_id, epub = mapping['abs_id'], mapping['kosync_doc_id'], mapping['ebook_filename']
            
            try:
                abs_ts = self.abs_client.get_progress(abs_id) or 0.0
                ko_pct = self.kosync_client.get_progress(ko_id) or 0.0
                st_pct, _ = self.storyteller_db.get_progress(epub)
                st_pct = st_pct or 0.0
                abs_pct = self._abs_to_percentage(abs_ts, mapping.get('transcript_file'))
            except Exception as e:
                logger.debug(f"Error fetching progress: {e}")
                continue

            prev = self.state.get(abs_id, {})
            abs_changed = abs(abs_ts - prev.get('abs_ts', 0)) > self.delta_abs_thresh
            ko_changed = abs(ko_pct - prev.get('kosync_pct', 0)) > self.delta_kosync_thresh
            st_changed = abs(st_pct - prev.get('storyteller_pct', 0)) > self.delta_kosync_thresh
            
            if not any([abs_changed, ko_changed, st_changed]):
                self.state[abs_id] = {'abs_ts': abs_ts, 'abs_pct': abs_pct or 0, 'kosync_pct': ko_pct, 'storyteller_pct': st_pct, 'last_updated': prev.get('last_updated', 0)}
                self.state_handler.save(self.state)
                continue

            progress_map = {'KOSYNC': ko_pct, 'STORYTELLER': st_pct}
            if abs_pct is not None: progress_map['ABS'] = abs_pct
            leader = max(progress_map, key=progress_map.get)
            leader_pct = progress_map[leader]
            
            logger.info(f"[{mapping.get('abs_title')}] Leader: {leader} ({leader_pct:.1%})")
            
            sync_success = False
            final_ts = abs_ts
            final_pct = leader_pct
            
            try:
                if leader == 'ABS':
                    txt = self.transcriber.get_text_at_time(mapping.get('transcript_file'), abs_ts)
                    if txt:
                        match_pct, xpath, _ = self.ebook_parser.find_text_location(epub, txt, hint_percentage=abs_pct)
                        if match_pct:
                            self.kosync_client.update_progress(ko_id, match_pct, xpath)
                            self.storyteller_db.update_progress(epub, match_pct)
                            final_pct = match_pct
                            sync_success = True
                elif leader == 'KOSYNC':
                    txt = self.ebook_parser.get_text_at_percentage(epub, ko_pct)
                    if txt:
                        ts = self.transcriber.find_time_for_text(mapping.get('transcript_file'), txt)
                        if ts:
                            self.abs_client.update_progress(abs_id, ts)
                            self.storyteller_db.update_progress(epub, ko_pct)
                            final_ts = ts
                            sync_success = True
                elif leader == 'STORYTELLER':
                    # Try to get fragment-based text for more accurate sync
                    _, _, href, frag = self.storyteller_db.get_progress_with_fragment(epub)
                    txt = self.get_text_from_storyteller_fragment(epub, href, frag) if frag else None
                    if not txt: txt = self.ebook_parser.get_text_at_percentage(epub, st_pct)
                    if txt:
                        ts = self.transcriber.find_time_for_text(mapping.get('transcript_file'), txt)
                        if ts:
                            self.abs_client.update_progress(abs_id, ts)
                            _, xpath, _ = self.ebook_parser.find_text_location(epub, txt)
                            self.kosync_client.update_progress(ko_id, st_pct, xpath)
                            final_ts = ts
                            sync_success = True
                
                if sync_success and final_pct > 0.01:
                    if not mapping.get('hardcover_book_id'): self._automatch_hardcover(mapping)
                    if mapping.get('hardcover_book_id'): self._sync_to_hardcover(mapping, final_pct)
            except Exception as e:
                logger.error(f"Sync failed: {e}")

            self.state[abs_id] = {'abs_ts': final_ts, 'abs_pct': self._abs_to_percentage(final_ts, mapping.get('transcript_file')) or 0, 'kosync_pct': final_pct, 'storyteller_pct': final_pct, 'last_updated': time.time()}
            self.state_handler.save(self.state)

    def run_daemon(self):
        schedule.every(int(os.getenv("SYNC_PERIOD_MINS", 5))).minutes.do(self.sync_cycle)
        schedule.every(1).minutes.do(self.check_pending_jobs)
        schedule.every(15).minutes.do(self.run_discovery)
        logger.info("Daemon started.")
        self.sync_cycle()
        self.run_discovery()
        while True:
            schedule.run_pending()
            time.sleep(30)

if __name__ == "__main__":
    manager = SyncManager()
    manager.run_daemon()
# [END FILE]
