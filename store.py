import sqlite3
import math
import pickle
import hashlib
import os
from datetime import datetime
from typing import List, Tuple, Optional
import config as cfg

class TweetStore:
    """
    Tabele:
      tweets(id TEXT PK, text TEXT, created_at TIMESTAMP NULL, fetched_at TIMESTAMP, url TEXT NULL)
      collections(id INTEGER PK AUTOINCREMENT, name TEXT UNIQUE, created_at TIMESTAMP)
      tweet_collections(tweet_id TEXT, collection_id INTEGER, added_at TIMESTAMP, PK(tweet_id, collection_id))
    """
    def __init__(self, sqlite_path: Optional[str] = None):
        self.sqlite_path = sqlite_path or cfg.DB_PATH
        self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._ensure_schema()

    def _ensure_schema(self):
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS tweets(
            id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            created_at TIMESTAMP NULL,
            fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            url TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at);

        CREATE TABLE IF NOT EXISTS collections(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS tweet_collections(
            tweet_id TEXT NOT NULL,
            collection_id INTEGER NOT NULL,
            added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tweet_id, collection_id),
            FOREIGN KEY (tweet_id) REFERENCES tweets(id) ON DELETE CASCADE,
            FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
        );
        """)

    def get_or_create_collection(self, name: str) -> int:
        with self._conn:
            self._conn.execute("INSERT OR IGNORE INTO collections(name) VALUES (?)", (name,))
        cur = self._conn.execute("SELECT id FROM collections WHERE name=?", (name,))
        row = cur.fetchone()
        return row[0]

    def upsert_many(self, rows: List[Tuple[str, str, Optional[str], Optional[str]]]):
        """
        rows: iterable[(id, text, created_at_iso_or_None, url_or_None)]
        """
        if not rows:
            return
        with self._conn:
            self._conn.executemany("""
            INSERT INTO tweets(id, text, created_at, url)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                text=excluded.text,
                created_at=COALESCE(excluded.created_at, tweets.created_at),
                url=COALESCE(excluded.url, tweets.url),
                fetched_at=CURRENT_TIMESTAMP
            """, rows)

    def link_many(self, tweet_ids: List[str], collection_id: int):
        if not tweet_ids:
            return
        with self._conn:
            self._conn.executemany("""
            INSERT OR IGNORE INTO tweet_collections(tweet_id, collection_id) VALUES (?, ?)
            """, ((tid, collection_id) for tid in tweet_ids))

    def fetch_collection_in_range(self, name: str, since: str, until: str):
        """
        Zwróć tweety z kolekcji w oknie [since, until], licząc po created_at, a jak NULL – po fetched_at.
        """
        q = """
        SELECT t.id, t.text, t.created_at, t.url
        FROM tweets t
        JOIN tweet_collections tc ON tc.tweet_id = t.id
        JOIN collections c        ON c.id = tc.collection_id
        WHERE c.name = ?
          AND DATE(COALESCE(t.created_at, t.fetched_at)) BETWEEN DATE(?) AND DATE(?)
        ORDER BY COALESCE(t.created_at, t.fetched_at)
        """
        cur = self._conn.execute(q, (name, since, until))
        return cur.fetchall()

    def stats(self, name: str):
        q = """
        SELECT COUNT(*),
               MIN(COALESCE(created_at, fetched_at)),
               MAX(COALESCE(created_at, fetched_at))
        FROM tweets t
        JOIN tweet_collections tc ON tc.tweet_id = t.id
        JOIN collections c        ON c.id = tc.collection_id
        WHERE c.name=?
        """
        cur = self._conn.execute(q, (name,))
        total, dmin, dmax = cur.fetchone()
        return {"count": total, "from": dmin, "to": dmax}

    def close(self):
        try:
            self._conn.commit()
            self._conn.close()
        except Exception:
            pass


class HybridDeduper:
    """
    Opcjonalny deduper: Bloom (szybkie "raczej nie") + potwierdzenie w SQLite (tweets).
    Trwałość tweetów zapewnia PRIMARY KEY w TweetStore; Bloom jest cachem.
    """
    def __init__(self, sqlite_path: Optional[str] = None, expected_n=500_000, fp_rate=1e-5,
                 load_bloom: Optional[str] = None, table='tweets'):
        self.sqlite_path = sqlite_path or cfg.DB_PATH
        self.table = table
        self.expected_n = expected_n
        self.fp_rate = fp_rate

        self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")

        self.n = expected_n
        self.p = fp_rate
        # m = - (n * ln p) / (ln 2)^2
        self.m = max(8, int(- (self.n * math.log(self.p)) / (math.log(2)**2)))
        self.k = max(1, int(round((self.m / self.n) * math.log(2))))
        self.bitarray = bytearray((self.m + 7) // 8)

        if load_bloom and os.path.exists(load_bloom):
            try:
                data = pickle.load(open(load_bloom, 'rb'))
                if data.get('m') == self.m and data.get('k') == self.k:
                    self.bitarray = bytearray(data['bitarray'])
            except Exception:
                pass

    def _hashes(self, data: bytes):
        for i in range(self.k):
            h = hashlib.sha256(data + i.to_bytes(2, 'big')).digest()
            yield int.from_bytes(h, 'big') % self.m

    def _bloom_contains(self, uid: str):
        b = uid.encode('utf-8')
        for pos in self._hashes(b):
            byte_idx = pos // 8
            bit = pos % 8
            if not (self.bitarray[byte_idx] & (1 << bit)):
                return False
        return True

    def _bloom_add(self, uid: str):
        b = uid.encode('utf-8')
        for pos in self._hashes(b):
            byte_idx = pos // 8
            bit = pos % 8
            self.bitarray[byte_idx] |= (1 << bit)

    def _sqlite_contains(self, uid: str):
        cur = self._conn.execute(f"SELECT 1 FROM {self.table} WHERE id=? LIMIT 1", (uid,))
        return cur.fetchone() is not None

    def contains(self, uid: str):
        if not self._bloom_contains(uid):
            return False
        return self._sqlite_contains(uid)

    def add(self, uid: str):
        self._bloom_add(uid)

    def bulk_add(self, uids):
        for u in uids:
            self._bloom_add(u)

    def save_bloom(self, filename: Optional[str] = None):
        filename = filename or cfg.BLOOM_SERIAL
        try:
            with open(filename, 'wb') as f:
                pickle.dump({'m': self.m, 'k': self.k, 'bitarray': bytes(self.bitarray)}, f)
        except Exception:
            pass

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass
