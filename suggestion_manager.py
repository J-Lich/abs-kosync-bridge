# [START FILE: abs-kosync-enhanced/suggestion_manager.py]
import logging
import time
from rapidfuzz import fuzz
from json_db import JsonDB
from pathlib import Path

logger = logging.getLogger(__name__)

class SuggestionManager:
    def __init__(self, data_dir, ebook_parser, abs_client, storyteller_db):
        self.db_handler = JsonDB(Path(data_dir) / "suggestions.json")
        self.ebook_parser = ebook_parser
        self.abs_client = abs_client
        self.storyteller_db = storyteller_db
        
        # Internal Cache
        self._audiobook_cache = []
        self._cache_timestamp = 0
        
        # Load suggestions
        self.suggestions = self.db_handler.load(default={})

    def _generate_key(self, source, source_id):
        return f"{source}:{source_id}"

    def _calculate_confidence(self, title_a, title_b):
        if not title_a or not title_b: return "Low", 0
        
        # FIXED: Use token_sort_ratio to penalize extra words (like "6")
        score = fuzz.token_sort_ratio(title_a.lower(), title_b.lower())
        
        if score > 92: return "High", score
        elif score > 80: return "Medium", score
        return "Low", score

    def _cleanup_stale_suggestions(self, max_age_days=7):
        now = time.time()
        cutoff = now - (max_age_days * 86400)
        removed = []
        for key, suggestion in list(self.suggestions.items()):
            if suggestion['state'] in ['pending', 'dismissed']:
                if suggestion['timestamp'] < cutoff:
                    removed.append(key)
                    del self.suggestions[key]
        if removed:
            self.db_handler.save(self.suggestions)
            logger.info(f"🧹 Cleaned up {len(removed)} stale suggestions")

    def run_discovery_cycle(self, mapped_abs_ids):
        """Main discovery loop."""
        logger.debug(f"Discovery: Checking for unmapped activity (excluding {len(mapped_abs_ids)} mapped books)")
        new_findings = 0
        
        # Refresh Cache (every hour)
        if not self._audiobook_cache or (time.time() - self._cache_timestamp > 3600):
            logger.info("Refreshing audiobook cache for discovery...")
            self._audiobook_cache = self.abs_client.get_all_audiobooks()
            self._cache_timestamp = time.time()
        
        # 1. ABS -> Ebook Discovery
        active_abs = self.abs_client.get_in_progress()
        for item in active_abs:
            if item['id'] in mapped_abs_ids: continue
            
            key = self._generate_key("ABS", item['id'])
            if key in self.suggestions: continue
            
            best_match = self._find_best_ebook_match(item['title'])
            if best_match:
                self._create_suggestion(key, item, best_match, "ebook")
                new_findings += 1

        # 2. Storyteller -> Audio Discovery
        recent_st = self.storyteller_db.get_recent_activity()
        for item in recent_st:
            best_audio = self._find_best_audio_match(item['title'], self._audiobook_cache)
            if best_audio:
                if best_audio['id'] in mapped_abs_ids: continue
                
                key = self._generate_key("STORYTELLER", item['id'])
                if key in self.suggestions: continue
                
                self._create_suggestion(key, item, best_audio, "audiobook")
                new_findings += 1

        if new_findings > 0:
            self.db_handler.save(self.suggestions)
            logger.info(f"💡 Generated {new_findings} new suggestions")
            
        self._cleanup_stale_suggestions()

    def _find_ebook_by_title(self, title):
        """Find ebook filename from title (for Storyteller suggestions)."""
        try:
            for f in self.ebook_parser.books_dir.rglob("*.epub"):
                # FIXED: Use token_sort_ratio
                if fuzz.token_sort_ratio(title.lower(), f.stem.lower()) > 90:
                    return f.name
        except Exception:
            pass
        return None

    def _find_best_ebook_match(self, title):
        highest_score = 0
        best_file = None
        try:
            for f in self.ebook_parser.books_dir.rglob("*.epub"):
                # FIXED: Use token_sort_ratio
                score = fuzz.token_sort_ratio(title.lower(), f.stem.lower())
                
                if score > highest_score:
                    highest_score = score
                    best_file = f.name
        except Exception: pass
        
        if highest_score > 80:
            return {"filename": best_file, "score": highest_score}
        return None

    def _find_best_audio_match(self, title, all_audiobooks):
        highest_score = 0
        best_ab = None
        for ab in all_audiobooks:
            ab_title = ab.get('media', {}).get('metadata', {}).get('title')
            if not ab_title: ab_title = ab.get('name')
            if ab_title:
                # FIXED: Use token_sort_ratio
                score = fuzz.token_sort_ratio(title.lower(), ab_title.lower())
                
                if score > highest_score:
                    highest_score = score
                    best_ab = ab
        if highest_score > 80:
            return {
                "id": best_ab['id'], 
                "title": best_ab['media']['metadata']['title'],
                "score": highest_score
            }
        return None

    def _create_suggestion(self, key, source_item, match_item, match_type):
        confidence, score = self._calculate_confidence(source_item['title'], match_item.get('title') or match_item.get('filename'))
        
        if confidence == "Low": return

        suggestion_data = {
            "state": "pending",
            "timestamp": time.time(),
            "confidence": confidence,
            "score": score,
            "source_type": source_item['source'],
            "source_title": source_item['title'],
            "source_id": source_item['id'],
            
            "match_type": match_type,
            "match_title": match_item.get('title') or match_item.get('filename'),
            "match_id": match_item.get('id'),
            "match_filename": match_item.get('filename')
        }

        # CRITICAL: Store ebook filename for Storyteller -> ABS mappings
        if source_item['source'] == 'STORYTELLER':
            ebook_file = self._find_ebook_by_title(source_item['title'])
            if ebook_file:
                suggestion_data['source_filename'] = ebook_file
            else:
                # We save it, but mark it potentially problematic in logs
                logger.warning(f"Could not find ebook file for Storyteller book: {source_item['title']}")
                return

        self.suggestions[key] = suggestion_data
# [END FILE]