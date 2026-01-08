import os
import requests
import logging
import time
import hashlib
import tempfile
from email.message import EmailMessage

logger = logging.getLogger(__name__)

class ABSClient:
    def __init__(self):
        self.base_url = os.environ.get("ABS_SERVER", "").rstrip('/')
        self.token = os.environ.get("ABS_KEY")
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def check_connection(self):
        url = f"{self.base_url}/api/me"
        try:
            r = requests.get(url, headers=self.headers, timeout=5)
            if r.status_code == 200:
                logger.info(f"✅ Connected to Audiobookshelf as user: {r.json().get('username', 'Unknown')}")
                return True
            else:
                logger.error(f"❌ Audiobookshelf Connection Failed: {r.status_code} - {r.text}")
                return False
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ Could not connect to Audiobookshelf at {self.base_url}. Check URL and Docker Network.")
            return False
        except Exception as e:
            logger.error(f"❌ Audiobookshelf Error: {e}")
            return False

    def get_all_audiobooks(self):
        lib_url = f"{self.base_url}/api/libraries"
        try:
            r = requests.get(lib_url, headers=self.headers)
            if r.status_code != 200:
                logger.error(f"Failed to fetch libraries: {r.status_code} - {r.text}")
                return []
            
            libraries = r.json().get('libraries', [])
            all_audiobooks = []

            for lib in libraries:
                logger.info(f"Scanning library: {lib['name']}...")
                lib_id = lib['id']
                items_url = f"{self.base_url}/api/libraries/{lib_id}/items"
                params = {"mediaType": "audiobook"}
                r_items = requests.get(items_url, headers=self.headers, params=params)
                if r_items.status_code == 200:
                    results = r_items.json().get('results', [])
                    all_audiobooks.extend(results)
                else:
                    logger.warning(f"Could not fetch items for library {lib['name']}")

            logger.info(f"Found {len(all_audiobooks)} audiobooks across {len(libraries)} libraries.")
            return all_audiobooks

        except Exception as e:
            logger.error(f"Exception fetching audiobooks: {e}")
            return []

    def get_audio_files(self, item_id):
        url = f"{self.base_url}/api/items/{item_id}"
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                data = r.json()
                files = []
                audio_files = data.get('media', {}).get('audioFiles', [])
                for af in audio_files:
                    stream_url = f"{self.base_url}/api/items/{item_id}/file/{af['ino']}"
                    stream_url += f"?token={self.token}" 
                    
                    extension = af.get('metadata', {}).get('ext') or 'mp3'
                    if not extension.startswith('.'):
                        extension = f".{extension}"
                    
                    files.append({
                        "stream_url": stream_url,
                        "ext": extension
                     })
                return files
            else:
                logger.error(f"Failed to get audio files for {item_id}: {r.status_code} - {r.text}")
                return []
        except Exception as e:
            logger.error(f"Error getting audio files: {e}")
            return []

    def get_progress(self, item_id):
        url = f"{self.base_url}/api/me/progress/{item_id}"
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return r.json().get('currentTime', 0)
        except Exception:
            pass
        return 0.0

    def update_progress(self, item_id, timestamp):
        url = f"{self.base_url}/api/me/progress/{item_id}"
        payload = {
            "currentTime": timestamp,
            "duration": 0, 
            "isFinished": False
        }
        try:
            requests.patch(url, headers=self.headers, json=payload)
        except Exception as e:
            logger.error(f"  Failed to update ABS progress: {e}")

    def get_all_ebooks(self):
        """Fetches all items with mediaType=book"""
        lib_url = f"{self.base_url}/api/libraries"
        try:
            r = requests.get(lib_url, headers=self.headers)
            if r.status_code != 200: return []
            
            libraries = r.json().get('libraries', [])
            all_ebooks = []

            for lib in libraries:
                items_url = f"{self.base_url}/api/libraries/{lib['id']}/items"
                params = {"mediaType": "book"}
                r_items = requests.get(items_url, headers=self.headers, params=params)
                if r_items.status_code == 200:
                    all_ebooks.extend(r_items.json().get('results', []))
            return all_ebooks
        except Exception as e:
            logger.error(f"Error fetching ebooks: {e}")
            return []

    def get_ebook_progress(self, item_id):
        """Gets percentage progress (0.0 - 1.0) for an ebook"""
        url = f"{self.base_url}/api/me/progress/{item_id}"
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                # ABS returns 'progress' for ebooks (0.0 to 1.0) or 'ebookProgress'
                data = r.json()
                return data.get('progress', 0.0) 
        except Exception:
            pass
        return 0.0

    def update_ebook_progress(self, item_id, percentage):
        """Updates ABS ebook progress"""
        url = f"{self.base_url}/api/me/progress/{item_id}"
        payload = {
            "progress": percentage,
            "isFinished": percentage >= 1.0
        }
        try:
            requests.patch(url, headers=self.headers, json=payload)
        except Exception as e:
            logger.error(f"  Failed to update ABS ebook progress: {e}")

    def compute_abs_hash(self, item_id):
        """Downloads ebook to temp, computes KoSync hash, deletes temp."""
        with tempfile.TemporaryDirectory() as temp_dir:
            download_url = f"{self.base_url}/api/items/{item_id}/download"
            try:
                # 1. Download
                with requests.get(download_url, headers=self.headers, stream=True) as r:
                    r.raise_for_status()
                    filename = "temp.epub"
                    if "Content-Disposition" in r.headers:
                        msg = EmailMessage()
                        msg['content-disposition'] = r.headers['Content-Disposition']
                        fname = msg.get_filename()
                        if fname: filename = fname
                    
                    filepath = os.path.join(temp_dir, filename)
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                
                # 2. Hash (KoReader logic)
                md5 = hashlib.md5()
                file_size = os.path.getsize(filepath)
                with open(filepath, 'rb') as f:
                    for i in range(-1, 11): 
                        if i == -1: offset = 0
                        else: offset = 1024 * (4 ** i)
                        if offset >= file_size: break
                        f.seek(offset)
                        chunk = f.read(1024)
                        if not chunk: break    
                        md5.update(chunk)
                return md5.hexdigest()

            except Exception as e:
                logger.error(f"Failed to hash ABS ebook {item_id}: {e}")
                return None

class KoSyncClient:
    def __init__(self):
        self.base_url = os.environ.get("KOSYNC_SERVER", "").rstrip('/')
        self.user = os.environ.get("KOSYNC_USER")
        self.auth_token = hashlib.md5(os.environ.get("KOSYNC_KEY", "").encode('utf-8')).hexdigest()

    def check_connection(self):
        url = f"{self.base_url}/healthcheck"
        headers = {"x-auth-user": self.user, "x-auth-key": self.auth_token, "accept": "application/json"}
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                 logger.info(f"✅ Connected to KoSync Server at {self.base_url}")
                 return True
            
            url_sync = f"{self.base_url}/syncs/progress/test-connection"
            r = requests.get(url_sync, headers=headers, timeout=5)
            logger.info(f"✅ Connected to KoSync Server (Response: {r.status_code})")
            return True
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ Could not connect to KoSync at {self.base_url}. Check URL.")
            return False
        except Exception as e:
            logger.error(f"❌ KoSync Error: {e}")
            return False

    def get_progress(self, doc_id):
        headers = {"x-auth-user": self.user, "x-auth-key": self.auth_token, 'accept': 'application/vnd.koreader.v1+json'}
        url = f"{self.base_url}/syncs/progress/{doc_id}"
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                data = r.json()
                return float(data.get('percentage', 0))
        except Exception:
            pass
        return 0.0

    def update_progress(self, doc_id, percentage, xpath=None):
        headers = {"x-auth-user": self.user, "x-auth-key": self.auth_token, 'accept': 'application/vnd.koreader.v1+json', 'content-type': 'application/json'}
        url = f"{self.base_url}/syncs/progress"
        
        # Use XPath if generated, otherwise fallback to percentage string
        progress_val = xpath if xpath else f"{percentage:.2%}"
        
        payload = {
            "document": doc_id,
            "percentage": percentage,
            "progress": progress_val, 
            "device": "abs-sync-bot",
            "device_id": "abs-sync-bot", 
            "timestamp": int(time.time())
        }
        
        try:
            # Reverted to simple PUT logic
            r = requests.put(url, headers=headers, json=payload)
            
            if r.status_code not in [200, 201]:
                logger.error(f"  KoSync Update Failed: {r.status_code} - {r.text}")
            else:
                logger.info(f"  KoSync updated successfully (HTTP {r.status_code})")
                
        except Exception as e:
            logger.error(f"Failed to update KoSync: {e}")
