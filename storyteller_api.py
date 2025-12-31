# [START FILE: abs-kosync-enhanced/storyteller_api.py]
"""
Storyteller API Client - TESTED AND WORKING

Uses Storyteller's REST API to sync reading positions instead of direct SQLite access.
This solves the "8-second overwrite" problem where Storyteller's aggressive sync
would overwrite SQLite changes.

IMPORTANT DISCOVERIES FROM TESTING:
1. Authentication: POST /api/token with form-urlencoded (NOT JSON)
   - Returns: {"access_token": "xxx", "expires_in": xxx, "token_type": "bearer"}
   - Tokens expire VERY quickly - refresh before each operation
   
2. Get books: GET /api/v2/books
   - Returns list with 'id' (numeric), 'uuid', 'title'
   
3. Get position: GET /api/books/{bookUUID}/positions
   - Uses UUID, not numeric ID!
   - Returns: {"locator": {"locations": {"totalProgression": 0.xx}}, "timestamp": xxx}
   
4. Update position: POST /api/books/{bookUUID}/positions
   - Body: {"locator": {"locations": {"totalProgression": 0.xx}}}
   - Returns 204 No Content on success
   - Minimal body works best - no need for href, type, etc.
"""

import os
import time
import logging
import requests
from typing import Optional, Dict, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class StorytellerAPIClient:
    """
    Client for Storyteller's REST API.
    
    This replaces direct SQLite database manipulation with proper API calls,
    ensuring Storyteller's sync mechanism works correctly.
    """
    
    def __init__(self):
        self.base_url = os.environ.get("STORYTELLER_API_URL", "http://localhost:8001").rstrip('/')
        self.username = os.environ.get("STORYTELLER_USER")
        self.password = os.environ.get("STORYTELLER_PASSWORD")
        
        # Cache for book lookups (title -> book info with uuid)
        self._book_cache: Dict[str, Dict] = {}
        self._cache_timestamp = 0
        
        # Token management - tokens expire quickly!
        self._token = None
        self._token_timestamp = 0
        self._token_max_age = 30  # Refresh token every 30 seconds to be safe
        
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json"
        })
    
    def _get_fresh_token(self) -> Optional[str]:
        """
        Get a fresh auth token. Tokens expire quickly so we refresh frequently.
        """
        # Check if current token is still valid
        if self._token and (time.time() - self._token_timestamp) < self._token_max_age:
            return self._token
        
        if not self.username or not self.password:
            logger.warning("Storyteller API: No credentials configured")
            return None
        
        try:
            # IMPORTANT: Use form-urlencoded, NOT JSON
            response = requests.post(
                f"{self.base_url}/api/token",
                data={
                    "username": self.username,
                    "password": self.password
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                self._token = data.get("access_token")
                self._token_timestamp = time.time()
                logger.debug(f"Storyteller API: Got fresh token")
                return self._token
            else:
                logger.error(f"Storyteller login failed: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Storyteller login error: {e}")
            return None
    
    def _make_request(self, method: str, endpoint: str, json_data: dict = None) -> Optional[requests.Response]:
        """
        Make an authenticated request, refreshing token as needed.
        """
        token = self._get_fresh_token()
        if not token:
            return None
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        try:
            url = f"{self.base_url}{endpoint}"
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, timeout=10)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=headers, json=json_data, timeout=10)
            else:
                logger.error(f"Unsupported method: {method}")
                return None
            
            # Check for auth errors
            if response.status_code == 401:
                # Token expired mid-request, force refresh and retry once
                self._token = None
                token = self._get_fresh_token()
                if not token:
                    return None
                headers["Authorization"] = f"Bearer {token}"
                if method.upper() == "GET":
                    response = self.session.get(url, headers=headers, timeout=10)
                else:
                    response = self.session.post(url, headers=headers, json=json_data, timeout=10)
            
            return response
        except Exception as e:
            logger.error(f"Storyteller API request failed: {e}")
            return None
    
    def check_connection(self) -> bool:
        """Check if we can connect to Storyteller API."""
        try:
            # Try to get a token - this validates credentials
            token = self._get_fresh_token()
            if token:
                logger.info(f"✅ Storyteller API connected at {self.base_url}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Storyteller API connection failed: {e}")
            return False
    
    def _refresh_book_cache(self) -> bool:
        """Refresh the cache of books from Storyteller."""
        response = self._make_request("GET", "/api/v2/books")
        
        if response and response.status_code == 200:
            books = response.json()
            self._book_cache = {}
            for book in books:
                title = book.get('title', '').lower()
                # Store both the numeric id and uuid
                self._book_cache[title] = {
                    'id': book.get('id'),
                    'uuid': book.get('uuid'),
                    'title': book.get('title')
                }
            self._cache_timestamp = time.time()
            logger.debug(f"Refreshed Storyteller book cache: {len(self._book_cache)} books")
            return True
        else:
            logger.error(f"Failed to refresh Storyteller book cache")
            return False
    
    def find_book_by_title(self, ebook_filename: str) -> Optional[Dict]:
        """
        Find a book in Storyteller by matching the ebook filename to title.
        
        Returns dict with 'id', 'uuid', and 'title' if found.
        """
        # Refresh cache if stale (older than 1 hour)
        if time.time() - self._cache_timestamp > 3600:
            self._refresh_book_cache()
        
        if not self._book_cache:
            self._refresh_book_cache()
        
        # Extract stem from filename for matching
        stem = Path(ebook_filename).stem.lower()
        
        # Remove common suffixes like "(readaloud)", "[01aDrQq8]", etc.
        import re
        clean_stem = re.sub(r'\s*\([^)]*\)\s*$', '', stem)
        clean_stem = re.sub(r'\s*\[[^\]]*\]\s*$', '', clean_stem)
        clean_stem = clean_stem.strip().lower()
        
        # Try exact match first
        if clean_stem in self._book_cache:
            return self._book_cache[clean_stem]
        
        # Try fuzzy match - check if titles contain each other
        for title, book_info in self._book_cache.items():
            if clean_stem in title or title in clean_stem:
                return book_info
        
        # Try word-based matching
        stem_words = set(clean_stem.split())
        for title, book_info in self._book_cache.items():
            title_words = set(title.split())
            # If most words match, consider it a match
            common = stem_words & title_words
            if len(common) >= min(len(stem_words), len(title_words)) * 0.7:
                return book_info
        
        return None
    
    def get_position(self, book_uuid: str) -> Tuple[Optional[float], Optional[int]]:
        """
        Get current reading position from Storyteller API.
        
        Args:
            book_uuid: The book's UUID (NOT the numeric id!)
        
        Returns: (percentage, timestamp) or (None, None) if not found
        """
        response = self._make_request("GET", f"/api/books/{book_uuid}/positions")
        
        if response and response.status_code == 200:
            try:
                data = response.json()
                locator = data.get('locator', {})
                locations = locator.get('locations', {})
                pct = locations.get('totalProgression', 0)
                ts = data.get('timestamp', 0)
                return float(pct), int(ts)
            except Exception as e:
                logger.error(f"Error parsing position response: {e}")
                return None, None
        elif response and response.status_code == 404:
            # No position yet
            return None, None
        else:
            status = response.status_code if response else "No response"
            logger.warning(f"Storyteller get position failed: {status}")
            return None, None
    
    def update_position(self, book_uuid: str, percentage: float) -> bool:
        """
        Update reading position via Storyteller API.
        
        IMPORTANT: Uses minimal body format that was tested and confirmed working:
        {"locator": {"locations": {"totalProgression": X}}}
        
        Args:
            book_uuid: The book's UUID (NOT the numeric id!)
            percentage: Reading progress as decimal (0.0 - 1.0)
        
        Returns: True if successful
        """
        # Minimal body format - this is what works!
        payload = {
            "locator": {
                "locations": {
                    "totalProgression": float(percentage)
                }
            }
        }
        
        response = self._make_request("POST", f"/api/books/{book_uuid}/positions", payload)
        
        if response and response.status_code == 204:
            logger.info(f"✅ Storyteller API: {book_uuid[:8]}... → {percentage:.1%}")
            return True
        else:
            status = response.status_code if response else "No response"
            text = response.text if response else ""
            logger.error(f"Storyteller update failed: {status} - {text}")
            return False
    
    def get_progress_by_filename(self, ebook_filename: str) -> Tuple[Optional[float], Optional[int]]:
        """
        Get reading progress for a book by its ebook filename.
        
        Returns: (percentage, timestamp) or (None, None)
        """
        book = self.find_book_by_title(ebook_filename)
        if not book:
            logger.debug(f"Book not found in Storyteller: {ebook_filename}")
            return None, None
        
        # Use UUID for position API!
        return self.get_position(book['uuid'])
    
    def update_progress_by_filename(self, ebook_filename: str, percentage: float) -> bool:
        """
        Update reading progress for a book by its ebook filename.
        
        Returns: True if successful
        """
        book = self.find_book_by_title(ebook_filename)
        if not book:
            logger.debug(f"Book not found in Storyteller: {ebook_filename}")
            return False
        
        # Use UUID for position API!
        return self.update_position(book['uuid'], percentage)


class StorytellerDBWithAPI:
    """
    Drop-in replacement for StorytellerDB that uses the API when available,
    falling back to direct SQLite access if API is not configured.
    
    This maintains backward compatibility while enabling proper API-based sync.
    """
    
    def __init__(self):
        self.api_client = None
        self.db_fallback = None
        
        # Try API first - requires URL, username, and password
        api_url = os.environ.get("STORYTELLER_API_URL")
        api_user = os.environ.get("STORYTELLER_USER")
        api_pass = os.environ.get("STORYTELLER_PASSWORD")
        
        if api_url and api_user and api_pass:
            self.api_client = StorytellerAPIClient()
            if self.api_client.check_connection():
                logger.info("Using Storyteller REST API for sync")
            else:
                logger.warning("Storyteller API authentication failed, trying SQLite fallback")
                self.api_client = None
        
        # Fallback to SQLite if API not available
        if not self.api_client:
            try:
                from storyteller_db import StorytellerDB
                self.db_fallback = StorytellerDB()
                logger.info("Using Storyteller SQLite fallback")
            except Exception as e:
                logger.warning(f"Storyteller SQLite fallback not available: {e}")
    
    def check_connection(self) -> bool:
        if self.api_client:
            return self.api_client.check_connection()
        elif self.db_fallback:
            return self.db_fallback.check_connection()
        return False
    
    def get_progress(self, ebook_filename: str) -> Tuple[Optional[float], Optional[int]]:
        if self.api_client:
            return self.api_client.get_progress_by_filename(ebook_filename)
        elif self.db_fallback:
            return self.db_fallback.get_progress(ebook_filename)
        return None, None
    
    def get_progress_with_fragment(self, ebook_filename: str):
        """Get progress with fragment info - API doesn't provide this detail, use fallback."""
        if self.db_fallback:
            return self.db_fallback.get_progress_with_fragment(ebook_filename)
        
        # API fallback - no fragment info available
        pct, ts = self.get_progress(ebook_filename)
        return pct, ts, None, None
    
    def update_progress(self, ebook_filename: str, percentage: float) -> bool:
        if self.api_client:
            return self.api_client.update_progress_by_filename(ebook_filename, percentage)
        elif self.db_fallback:
            return self.db_fallback.update_progress(ebook_filename, percentage)
        return False
    
    def get_recent_activity(self, hours: int = 24, min_progress: float = 0.01):
        """Get recent activity - only available via SQLite."""
        if self.db_fallback:
            return self.db_fallback.get_recent_activity(hours, min_progress)
        return []
    
    def add_to_collection(self, ebook_filename: str):
        """Add to collection - no-op for API, handled differently."""
        if self.db_fallback and hasattr(self.db_fallback, 'add_to_collection'):
            return self.db_fallback.add_to_collection(ebook_filename)
        pass


# For backward compatibility - import this in main.py
def create_storyteller_client():
    """
    Factory function to create the best available Storyteller client.
    
    Priority:
    1. API client if STORYTELLER_API_URL + credentials are set
    2. SQLite client if STORYTELLER_DB_PATH is set
    3. None if neither is available
    """
    return StorytellerDBWithAPI()
# [END FILE]
