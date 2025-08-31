from pathlib import Path

# Foldery bazowe
BASE_DIR     = Path.cwd()
BROWSER_DIR  = BASE_DIR / "browser"
DB_DIR       = BASE_DIR / "db"
RESULTS_DIR  = BASE_DIR / "results"
for _d in (BROWSER_DIR, DB_DIR, RESULTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Ścieżki Chrome/Driver (mogą być nadpisane w main)
CHROME_BINARY     = str(BROWSER_DIR / "chrome-win64" / "chrome.exe")
CHROMEDRIVER_PATH = str(BROWSER_DIR / "chromedriver" / "chromedriver.exe")

# Chrome for Testing
CFT_JSON   = "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json"
CFT_OUTDIR = BROWSER_DIR / "chrome_for_testing"
CFT_CHANNEL = "Stable"  # Stable|Beta|Dev|Canary

# Baza danych
DB_PATH = str(DB_DIR / "tweets.sqlite")

# Deduper Bloom (opcjonalny)
USE_BLOOM = False
BLOOM_SERIAL = str(DB_DIR / "tweet_ids_bloom.pickle")

# Rate-limit cooldown (sekundy)
RATE_LIMIT_COOLDOWN = 450

# Checkpointy / progres
RAW_PROGRESS_EVERY_N_TWEETS = 100
RAW_PROGRESS_EVERY_SEC      = 60
AN_PROGRESS_MIN_INTERVAL_SEC = 30
CHECKPOINT_KEEP = 5  # ile plików z timestampem trzymać (na typ)

# Chrome profil (do ominięcia logowania)
# USER_DATA_DIR = r"C:\Users\snipe\AppData\Local\Google\Chrome\User Data"
USER_DATA_DIR = str((Path.cwd() / "browser" / "profile").resolve())

# Headless (logowanie Twitter potrafi być problematyczne)
HEADLESS = False

# ===== Przełączniki zapisów wyników =====
SAVE_CSV = True
SAVE_PARQUET = True