import argparse
from datetime import datetime, timedelta
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

import config as cfg
from twitter_scraper import ensure_chrome_and_driver, set_driver
from analyzer import analyze_and_visualize


# ---------- PRESETY ----------

PRESET_CHOICES = [
    "daily_refresh",   # ostatnie 24h, wymuszone odświeżenie, bloom, dłuższy cooldown
    "rolling7",        # ostatnie 7 dni, top-up bez refresh
    "deep_crawl",      # długi zbiór, resume + bloom + wyższy cooldown
    "db_only",         # analiza tylko z DB (ostatnie 7 dni domyślnie)
    "server_headless", # headless + bloom (idealny na serwer, wymaga user-data-dir)
    "parquet_only"     # zapis tylko do Parquet
]

def _compute_dates(days_back: int):
    today = datetime.now().date()
    since = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")
    return since, until

def _build_preset_defaults(preset: str):
    """
    Zwraca słownik domyślnych wartości argów dla danego presetu.
    Te wartości stają się 'defaultami' parsera: użytkownik może je nadpisać flagami.
    """
    d = {}
    if preset == "daily_refresh":
        s, u = _compute_dates(1)
        d.update({
            "since": s, "until": u, "max_tweets": 1000,
            "use_bloom": True, "refresh": True, "cooldown": 600
        })
    elif preset == "rolling7":
        s, u = _compute_dates(7)
        d.update({
            "since": s, "until": u, "max_tweets": 500,
            "use_bloom": True, "cooldown": 600
        })
    elif preset == "deep_crawl":
        # Ustal zakres w flagach lub interaktywnie; preset ustawia agresywnie parametry taskowe
        d.update({
            "max_tweets": 5000,
            "use_bloom": True, "cooldown": 900,
            "resume": True,           # wznawiaj zawsze
            "analysis_progress_sec": 20,
            "progress_every": 200,
            "progress_sec": 45
        })
    elif preset == "db_only":
        s, u = _compute_dates(7)
        d.update({
            "since": s, "until": u, "max_tweets": 1000,
            "db_only": True
        })
    elif preset == "server_headless":
        s, u = _compute_dates(7)
        d.update({
            "since": s, "until": u, "max_tweets": 800,
            "use_bloom": True, "headless": True,
            "cooldown": 600
        })
    elif preset == "parquet_only":
        s, u = _compute_dates(7)
        d.update({
            "since": s, "until": u, "max_tweets": 500,
            "no_csv": True, "no_parquet": False
        })
    return d


# ---------- DWUETAPOWE PARSOWANIE: najpierw bierzemy preset, potem resztę z jego defaultami ----------

def parse_preset_only():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--preset", choices=PRESET_CHOICES, help="Użyj predefiniowanego profilu parametrów.")
    args, _ = p.parse_known_args()
    return args.preset

def build_parser_with_defaults(preset_defaults=None):
    p = argparse.ArgumentParser(description="Twitter sentiment scraper + analyzer (PL) — kolekcja=korpus, okno dat=analiza.")
    # Preset (pokazujemy w pełnym parserze też, żeby było w --help)
    p.add_argument("--preset", choices=PRESET_CHOICES, help="Predefiniowany profil uruchomienia.")
    # Ścieżki
    p.add_argument("--browser-dir", type=str, help="Folder na Chrome/Driver (default ./browser)")
    p.add_argument("--db-dir", type=str, help="Folder na bazę SQLite (default ./db)")
    p.add_argument("--results-dir", type=str, help="Folder na wyniki (default ./results)")
    # Chrome/driver profil
    p.add_argument("--user-data-dir", type=str, help="Ścieżka do profilu Chrome (by ominąć loginy)")
    p.add_argument("--headless", action="store_true", help="Uruchom Chrome w trybie headless (uwaga: logowanie może nie działać).")
    # Bloom / rate-limit / checkpoint progi
    p.add_argument("--use-bloom", action="store_true", help="Włącz HybridDeduper (Bloom).")
    p.add_argument("--cooldown", type=int, help="Sekundy cooldown przy rate-limit (default 300).")
    p.add_argument("--progress-every", type=int, help="RAW checkpoint co N nowych tweetów (default 100).")
    p.add_argument("--progress-sec", type=int, help="RAW checkpoint co N sekund (default 60).")
    p.add_argument("--analysis-progress-sec", type=int, help="Checkpoint analizy co N sekund (default 30).")
    p.add_argument("--checkpoint-keep", type=int, help="Ile trzymać ostatnich checkpointów z timestampem (default 5).")
    # Resume / refresh
    p.add_argument("--resume", action="store_true", help="Wznów zarówno RAW jak i ANALIZĘ z najnowszych checkpointów.")
    p.add_argument("--resume-raw", action="store_true", help="Wznów tylko scrapowanie RAW.")
    p.add_argument("--resume-analysis", action="store_true", help="Wznów tylko analizę.")
    p.add_argument("--refresh", action="store_true", help="Zmuś dociągnięcie z Twittera w oknie dat (top-up) nawet jeśli DB ma komplet.")
    # Zapis wyników
    p.add_argument("--no-parquet", action="store_true", help="Nie zapisuj wyników do Parquet.")
    p.add_argument("--no-csv", action="store_true", help="Nie zapisuj wyników do CSV.")
    # Parametry merytoryczne
    p.add_argument("--keyword", type=str, help="Słowo kluczowe do wyszukiwania.")
    p.add_argument("--since", type=str, help="Początek zakresu YYYY-MM-DD.")
    p.add_argument("--until", type=str, help="Koniec zakresu YYYY-MM-DD.")
    p.add_argument("--max-tweets", type=int, help="Maksymalna liczba tweetów.")
    p.add_argument("--collection", type=str, help="Nazwa kolekcji (korpusu). Domyślnie = keyword.")
    p.add_argument("--db-only", action="store_true", help="Użyj wyłącznie danych z DB (bez scrapowania).")

    # Jeśli mamy preset defaults – ustaw jako parser defaults (użytkownik nadal może nadpisać flagami)
    if preset_defaults:
        p.set_defaults(**preset_defaults)

    return p

def main():
    # 1) odczytaj preset
    preset = parse_preset_only()
    preset_defaults = _build_preset_defaults(preset) if preset else None

    # 2) zbuduj parser z domyślnymi wg presetu, *potem* sparsuj pełne argumenty
    parser = build_parser_with_defaults(preset_defaults)
    args = parser.parse_args()

    # ---------- ustawienie katalogów ----------
    if args.browser_dir: cfg.BROWSER_DIR = Path(args.browser_dir)
    if args.db_dir:      cfg.DB_DIR = Path(args.db_dir)
    if args.results_dir: cfg.RESULTS_DIR = Path(args.results_dir)
    for _d in (cfg.BROWSER_DIR, cfg.DB_DIR, cfg.RESULTS_DIR): _d.mkdir(parents=True, exist_ok=True)

    # Pochodne
    cfg.DB_PATH = str(cfg.DB_DIR / "tweets.sqlite")
    cfg.BLOOM_SERIAL = str(cfg.DB_DIR / "tweet_ids_bloom.pickle")
    cfg.CFT_OUTDIR = cfg.BROWSER_DIR / "chrome_for_testing"
    cfg.CHROME_BINARY     = str(cfg.BROWSER_DIR / "chrome-win64" / "chrome.exe")
    cfg.CHROMEDRIVER_PATH = str(cfg.BROWSER_DIR / "chromedriver" / "chromedriver.exe")

    # Flagi globalne / config
    if args.use_bloom: cfg.USE_BLOOM = True
    if args.cooldown is not None: cfg.RATE_LIMIT_COOLDOWN = int(args.cooldown)
    if args.progress_every is not None: cfg.RAW_PROGRESS_EVERY_N_TWEETS = int(args.progress_every)
    if args.progress_sec is not None: cfg.RAW_PROGRESS_EVERY_SEC = int(args.progress_sec)
    if args.analysis_progress_sec is not None: cfg.AN_PROGRESS_MIN_INTERVAL_SEC = int(args.analysis_progress_sec)
    if args.checkpoint_keep is not None: cfg.CHECKPOINT_KEEP = max(0, int(args.checkpoint_keep))
    if args.user_data_dir: cfg.USER_DATA_DIR = args.user_data_dir
    if args.headless: cfg.HEADLESS = True

    # Zapisy
    cfg.SAVE_PARQUET = not args.no_parquet
    cfg.SAVE_CSV     = not args.no_csv

    # Resume flags łączone
    resume_raw = args.resume or args.resume_raw
    resume_analysis = args.resume or args.resume_analysis

    # ---------- parametry merytoryczne (interaktywka tylko jeśli nadal brak) ----------
    keyword = args.keyword or input("🔎 Słowo kluczowe: ").strip()

    today   = datetime.now()
    default_since = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    default_until = today.strftime("%Y-%m-%d")

    since = args.since or (input(f"📅 Początek [YYYY-MM-DD] (domyślnie {default_since}): ").strip() or default_since)
    until = args.until or (input(f"📅 Koniec   [YYYY-MM-DD] (domyślnie {default_until}): ").strip() or default_until)

    if args.max_tweets is not None:
        max_t = int(args.max_tweets)
    else:
        max_t = int(input("📈 Maksymalna liczba tweetów: "))

    default_coll = keyword
    collection_name = args.collection or (input(f"🏷️  Nazwa kolekcji (Enter = {default_coll}): ").strip() or default_coll)

    # DB-only – jeśli nie podano flagi, a nie ma presetu ustawiającego, zapytaj interaktywnie
    if args.db_only:
        only_db = True
    else:
        # nie pytaj w trybie z presetem (bo preset już zdecydował) – tylko gdy brak presetu
        if preset is None and args.collection is None:
            only_db = (input("📦 Użyć tylko istniejącej kolekcji z bazy? [y/N]: ").strip().lower() == 'y')
        else:
            only_db = False

    print(f"📚 Preset: {preset or '-'} | Kolekcja: {collection_name} | Okno: {since}..{until} | "
          f"max_tweets={max_t} | {'DB-only' if only_db else 'DB+Twitter'}"
          f"{', refresh' if args.refresh else ''} | "
          f"save: {'CSV' if cfg.SAVE_CSV else ''}{'+' if cfg.SAVE_CSV and cfg.SAVE_PARQUET else ''}"
          f"{'Parquet' if cfg.SAVE_PARQUET else '' or 'none'}")

    # ---------- przeglądarka (tylko gdy nie DB-only) ----------
    drv = None
    if not only_db:
        chrome_bin, chromedriver = ensure_chrome_and_driver(cfg.CHROME_BINARY, cfg.CHROMEDRIVER_PATH)
        options = webdriver.ChromeOptions()
        options.binary_location = chrome_bin
        options.add_argument(f"user-data-dir={cfg.USER_DATA_DIR}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--start-maximized")
        if cfg.HEADLESS:
            options.add_argument("--headless=new")
        drv = webdriver.Chrome(service=Service(chromedriver), options=options)
        set_driver(drv)

        # prosty injection przycisku "KONTYNUUJ" (logowanie)
        inject_js = r'''
        (function(){
          if(window._selenium_continue_injected) return;
          window._selenium_continue_injected = true;
          const btn = document.createElement('button');
          btn.id = 'selenium_continue_button';
          btn.textContent = 'KONTYNUUJ (kliknij po zalogowaniu)';
          btn.style.position = 'fixed';
          btn.style.zIndex = 2147483647;
          btn.style.right = '12px';
          btn.style.bottom = '12px';
          btn.style.padding = '12px 18px';
          btn.style.background = '#1DA1F2';
          btn.style.color = 'white';
          btn.style.border = 'none';
          btn.style.borderRadius = '8px';
          btn.style.boxShadow = '0 4px 12px rgba(0,0,0,0.3)';
          btn.style.fontSize = '14px';
          btn.style.cursor = 'pointer';
          btn.style.fontFamily = 'Arial, sans-serif';
          btn.onclick = function(e){ try { window._selenium_continue_clicked = true; btn.remove(); } catch (err) { window._selenium_continue_clicked = true; } };
          document.body.appendChild(btn);
        })();
        '''
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        import urllib.parse

        # Otwórz wstępny search, wstrzyknij przycisk i czekaj (max 1h)
        search_query = f"{keyword} since:{since} until:{until}"
        search_url = "https://mobile.twitter.com/search?q=" + urllib.parse.quote(search_query)
        print(f"🔗 Otwieram Twitter (search): {search_url}")
        drv.get(search_url)
        try:
            WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass
        drv.execute_script(inject_js)
        print("🔔 Na stronie wstawiono przycisk 'KONTYNUUJ'. Zaloguj się w przeglądarce, a potem kliknij przycisk, by kontynuować.")

        WAIT_TIMEOUT = 3600
        try:
            def _continue_condition(d):
                clicked = d.execute_script("return !!window._selenium_continue_clicked")
                if clicked: return True
                try:
                    els = d.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
                    if els and len(els) > 0: return True
                except Exception: pass
                try:
                    url = d.current_url or ""
                    if 'login' not in url and 'signup' not in url:
                        if 'mobile.twitter.com' in url: return False
                except Exception: pass
                return False
            WebDriverWait(drv, WAIT_TIMEOUT, poll_frequency=1).until(_continue_condition)
            print("✅ Kontynuujemy — kliknięto 'KONTYNUUJ' lub wyniki są dostępne.")
        except Exception as e:
            print(f"⚠️ Timeout/błąd podczas oczekiwania: {e}")
        try:
            drv.execute_script("var b=document.getElementById('selenium_continue_button'); if(b) b.remove();")
        except Exception:
            pass

    # ---------- Analiza (DB-first + top-up) ----------
    analyze_and_visualize(
        keyword, since, until, max_t,
        collection_name=collection_name,
        use_db_only=only_db,
        resume_analysis=resume_analysis,
        refresh=args.refresh
    )

    if drv is not None:
        try: drv.quit()
        except Exception: pass


if __name__ == "__main__":
    main()
