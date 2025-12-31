import os
import requests
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class HardcoverClient:
    def __init__(self):
        self.api_url = "https://api.hardcover.app/v1/graphql"
        self.token = os.environ.get("HARDCOVER_TOKEN")
        self.user_id = None
        
        if not self.token:
            logger.warning("HARDCOVER_TOKEN not set")
            return
        
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
    
    def query(self, query: str, variables: Dict = None) -> Optional[Dict]:
        if not self.token:
            return None
        
        try:
            r = requests.post(
                self.api_url,
                json={"query": query, "variables": variables or {}},
                headers=self.headers,
                timeout=10
            )
            
            if r.status_code == 200:
                data = r.json()
                if data.get('data'):
                    return data['data']
                elif data.get('errors'):
                    logger.error(f"GraphQL errors: {data['errors']}")
            else:
                logger.error(f"HTTP {r.status_code}: {r.text}")
        except Exception as e:
            logger.error(f"Hardcover query failed: {e}")
        
        return None
    
    def get_user_id(self) -> Optional[int]:
        if self.user_id:
            return self.user_id
        
        result = self.query("{ me { id } }")
        if result and result.get('me'):
            self.user_id = result['me'][0]['id']
        return self.user_id
    
    def search_by_isbn(self, isbn: str) -> Optional[Dict]:
        """Search by ISBN-13 or ISBN-10."""
        isbn_key = 'isbn_13' if len(str(isbn)) == 13 else 'isbn_10'
        
        # FIXED: Removed unused $userId variable
        query = f"""
        query ($isbn: String!) {{
            editions(where: {{ {isbn_key}: {{ _eq: $isbn }} }}) {{
                id
                pages
                book {{
                    id
                    title
                }}
            }}
        }}
        """
        
        result = self.query(query, {"isbn": str(isbn)})
        if result and result.get('editions') and len(result['editions']) > 0:
            edition = result['editions'][0]
            return {
                'book_id': edition['book']['id'],
                'edition_id': edition['id'],
                'pages': edition['pages'],
                'title': edition['book']['title']
            }
        return None
    
    def search_by_title_author(self, title: str, author: str = None) -> Optional[Dict]:
        """Search by title and author."""
        search_query = f"{title} {author or ''}".strip()
        
        # FIXED: Removed unused $userId variable
        query = """
        query ($query: String!) {
            search(query: $query, per_page: 5, page: 1, query_type: "Book") {
                ids
            }
        }
        """
        
        result = self.query(query, {"query": search_query})
        if not result or not result.get('search') or not result['search'].get('ids'):
            return None
        
        book_ids = result['search']['ids']
        if not book_ids:
            return None
        
        # Hydrate first result (requires userId for permission checks on some fields, but getting basic book info usually doesn't)
        # We'll use the generic lookup here
        book_query = """
        query ($id: Int!) {
            books(where: { id: { _eq: $id }}) {
                id
                title
            }
        }
        """
        
        book_result = self.query(book_query, {"id": book_ids[0]})
        if book_result and book_result.get('books') and len(book_result['books']) > 0:
            book = book_result['books'][0]
            
            # Get default edition for pages
            edition = self.get_default_edition(book['id'])
            
            return {
                'book_id': book['id'],
                'edition_id': edition.get('id') if edition else None,
                'pages': edition.get('pages') if edition else None,
                'title': book['title']
            }
        
        return None
    
    def get_default_edition(self, book_id: int) -> Optional[Dict]:
        """Get default edition for a book."""
        query = """
        query ($bookId: Int!) {
            books_by_pk(id: $bookId) {
                default_ebook_edition {
                    id
                    pages
                }
                default_physical_edition {
                    id
                    pages
                }
            }
        }
        """
        
        result = self.query(query, {"bookId": book_id})
        if result and result.get('books_by_pk'):
            if result['books_by_pk'].get('default_ebook_edition'):
                return result['books_by_pk']['default_ebook_edition']
            elif result['books_by_pk'].get('default_physical_edition'):
                return result['books_by_pk']['default_physical_edition']
        
        return None
    
    def find_user_book(self, book_id: int) -> Optional[Dict]:
        """Find existing user_book. Needs userId."""
        query = """
        query ($bookId: Int!, $userId: Int!) {
            user_books(where: { book_id: { _eq: $bookId }, user_id: { _eq: $userId }}) {
                id
                status_id
                edition_id
                user_book_reads(order_by: {id: desc}, limit: 1) {
                    id
                }
            }
        }
        """
        
        result = self.query(query, {"bookId": book_id, "userId": self.get_user_id()})
        if result and result.get('user_books') and len(result['user_books']) > 0:
            return result['user_books'][0]
        return None
    
    def update_status(self, book_id: int, status_id: int, edition_id: int = None) -> Optional[Dict]:
        """Create/update user_book status."""
        query = """
        mutation ($object: UserBookCreateInput!) {
            insert_user_book(object: $object) {
                error
                user_book {
                    id
                    status_id
                    edition_id
                }
            }
        }
        """
        
        update_args = {
            "book_id": book_id,
            "status_id": status_id,
            "privacy_setting_id": 1
        }
        
        if edition_id:
            update_args["edition_id"] = edition_id
        
        result = self.query(query, {"object": update_args})
        if result and result.get('insert_user_book'):
            return result['insert_user_book'].get('user_book')
        return None
    
    def update_progress(self, user_book_id: int, page: int, edition_id: int = None) -> bool:
        """Update reading progress."""
        # First check if there's an existing read
        read_query = """
        query ($userBookId: Int!) {
            user_book_reads(where: { user_book_id: { _eq: $userBookId }}, order_by: {id: desc}, limit: 1) {
                id
            }
        }
        """
        
        read_result = self.query(read_query, {"userBookId": user_book_id})
        
        if read_result and read_result.get('user_book_reads') and len(read_result['user_book_reads']) > 0:
            # Update existing read
            read_id = read_result['user_book_reads'][0]['id']
            
            query = """
            mutation ($id: Int!, $pages: Int, $editionId: Int) {
                update_user_book_read(id: $id, object: {
                    progress_pages: $pages,
                    edition_id: $editionId
                }) {
                    error
                    user_book_read {
                        id
                    }
                }
            }
            """
            
            result = self.query(query, {"id": read_id, "pages": page, "editionId": edition_id})
            return bool(result and result.get('update_user_book_read'))
        else:
            # Create new read
            query = """
            mutation ($userBookId: Int!, $pages: Int, $editionId: Int) {
                insert_user_book_read(user_book_id: $userBookId, user_book_read: {
                    progress_pages: $pages,
                    edition_id: $editionId
                }) {
                    error
                    user_book_read {
                        id
                    }
                }
            }
            """
            
            result = self.query(query, {"userBookId": user_book_id, "pages": page, "editionId": edition_id})
            return bool(result and result.get('insert_user_book_read'))