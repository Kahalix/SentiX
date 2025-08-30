# import argparse
# from datetime import datetime, timedelta
# from pathlib import Path

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service

# import config as cfg
# from twitter_scraper import ensure_chrome_and_driver, set_driver
# from analyzer import analyze_and_visualize

# def parse_args():
#     p = argparse.ArgumentParser(description="Twitter sentiment scraper + analyzer (PL) — kolekcja=korpus, okno dat=analiza.")
#     # Ścieżki
#     p.add_argument("--browser-dir", type=str, help="Folder na Chrome/Driver (default ./browser)")
#     p.add_argument("--db-dir", type=str, help="Folder na bazę SQLite (default ./db)")
#     p.add_argument("--results-dir", type=str, help="Folder na wyniki (default ./results)")
#     # Chrome/driver profil
#     p.add_argument("--user-data-dir", type=str, help="Ścieżka do profilu Chrome (by ominąć loginy)")
#     p.add_argument("--headless", action="store_true", help="Uruchom Chrome w trybie headless (uwaga: logowanie może nie działać).")
#     # Bloom / rate-limit / checkpoint progi
#     p.add_argument("--use-bloom", action="store_true", help="Włącz HybridDeduper (Bloom).")
#     p.add_argument("--cooldown", type=int, help="Sekundy cooldown przy rate-limit (default 300).")
#     p.add_argument("--progress-every", type=int, help="RAW checkpoint co N nowych tweetów (default 100).")
#     p.add_argument("--progress-sec", type=int, help="RAW checkpoint co N sekund (default 60).")
#     p.add_argument("--analysis-progress-sec", type=int, help="Checkpoint analizy co N sekund (default 30).")
#     p.add_argument("--checkpoint-keep", type=int, help="Ile trzymać ostatnich checkpointów z timestampem (default 5).")
#     # Resume / refresh
#     p.add_argument("--resume", action="store_true", help="Wznów zarówno RAW jak i ANALIZĘ z najnowszych checkpointów.")
#     p.add_argument("--resume-raw", action="store_true", help="Wznów tylko scrapowanie RAW.")
#     p.add_argument("--resume-analysis", action="store_true", help="Wznów tylko analizę.")
#     p.add_argument("--refresh", action="store_true", help="Zmuś dociągnięcie z Twittera w oknie dat (top-up) nawet jeśli DB ma komplet.")
#     # Zapis wyników
#     p.add_argument("--no-parquet", action="store_true", help="Nie zapisuj wyników do Parquet.")
#     p.add_argument("--no-csv", action="store_true", help="Nie zapisuj wyników do CSV.")
#     # Parametry merytoryczne
#     p.add_argument("--keyword", type=str, help="Słowo kluczowe do wyszukiwania.")
#     p.add_argument("--since", type=str, help="Początek zakresu YYYY-MM-DD.")
#     p.add_argument("--until", type=str, help="Koniec zakresu YYYY-MM-DD.")
#     p.add_argument("--max-tweets", type=int, help="Maksymalna liczba tweetów.")
#     p.add_argument("--collection", type=str, help="Nazwa kolekcji (korpusu). Domyślnie = keyword.")
#     p.add_argument("--db-only", action="store_true", help="Użyj wyłącznie danych z DB (bez scrapowania).")
#     return p.parse_args()

# def main():
#     args = parse_args()

#     # Nadpisz katalogi
#     if args.browser_dir: cfg.BROWSER_DIR = Path(args.browser_dir)
#     if args.db_dir:      cfg.DB_DIR = Path(args.db_dir)
#     if args.results_dir: cfg.RESULTS_DIR = Path(args.results_dir)
#     for _d in (cfg.BROWSER_DIR, cfg.DB_DIR, cfg.RESULTS_DIR): _d.mkdir(parents=True, exist_ok=True)

#     # Pochodne
#     cfg.DB_PATH = str(cfg.DB_DIR / "tweets.sqlite")
#     cfg.BLOOM_SERIAL = str(cfg.DB_DIR / "tweet_ids_bloom.pickle")
#     cfg.CFT_OUTDIR = cfg.BROWSER_DIR / "chrome_for_testing"
#     cfg.CHROME_BINARY     = str(cfg.BROWSER_DIR / "chrome-win64" / "chrome.exe")
#     cfg.CHROMEDRIVER_PATH = str(cfg.BROWSER_DIR / "chromedriver" / "chromedriver.exe")

#     # Flagi
#     if args.use_bloom: cfg.USE_BLOOM = True
#     if args.cooldown is not None: cfg.RATE_LIMIT_COOLDOWN = int(args.cooldown)
#     if args.progress_every is not None: cfg.RAW_PROGRESS_EVERY_N_TWEETS = int(args.progress_every)
#     if args.progress_sec is not None: cfg.RAW_PROGRESS_EVERY_SEC = int(args.progress_sec)
#     if args.analysis_progress_sec is not None: cfg.AN_PROGRESS_MIN_INTERVAL_SEC = int(args.analysis_progress_sec)
#     if args.checkpoint_keep is not None: cfg.CHECKPOINT_KEEP = max(0, int(args.checkpoint_keep))
#     if args.user_data_dir: cfg.USER_DATA_DIR = args.user_data_dir
#     if args.headless: cfg.HEADLESS = True

#     # Zapisy
#     cfg.SAVE_PARQUET = not args.no_parquet
#     cfg.SAVE_CSV     = not args.no_csv

#     resume_raw = args.resume or args.resume_raw
#     resume_analysis = args.resume or args.resume_analysis

#     # Interaktywka jeśli brak parametrów
#     if args.keyword: keyword = args.keyword
#     else: keyword = input("🔎 Słowo kluczowe: ").strip()

#     today   = datetime.now()
#     default_since = (today - timedelta(days=7)).strftime("%Y-%m-%d")
#     default_until = today.strftime("%Y-%m-%d")

#     if args.since: since = args.since
#     else: since = input(f"📅 Początek [YYYY-MM-DD] (domyślnie {default_since}): ").strip() or default_since

#     if args.until: until = args.until
#     else: until = input(f"📅 Koniec   [YYYY-MM-DD] (domyślnie {default_until}): ").strip() or default_until

#     if args.max_tweets is not None: max_t = int(args.max_tweets)
#     else: max_t = int(input("📈 Maksymalna liczba tweetów: "))

#     default_coll = keyword
#     if args.collection: collection_name = args.collection
#     else: collection_name = input(f"🏷️  Nazwa kolekcji (Enter = {default_coll}): ").strip() or default_coll

#     only_db = args.db_only or (input("📦 Użyć tylko istniejącej kolekcji z bazy? [y/N]: ").strip().lower() == 'y' if args.db_only is None and not args.collection else False)

#     print(f"📚 Kolekcja: {collection_name} | Okno: {since}..{until} | max_tweets={max_t} | "
#           f"{'DB-only' if only_db else 'DB+Twitter'}{', refresh' if args.refresh else ''} | "
#           f"save: {'CSV' if cfg.SAVE_CSV else ''}{'+' if cfg.SAVE_CSV and cfg.SAVE_PARQUET else ''}{'Parquet' if cfg.SAVE_PARQUET else '' or 'none'}")

#     # Uruchom przeglądarkę tylko gdy nie DB-only
#     drv = None
#     if not only_db:
#         chrome_bin, chromedriver = ensure_chrome_and_driver(cfg.CHROME_BINARY, cfg.CHROMEDRIVER_PATH)
#         options = webdriver.ChromeOptions()
#         options.binary_location = chrome_bin
#         options.add_argument(f"user-data-dir={cfg.USER_DATA_DIR}")
#         options.add_argument("--profile-directory=Default")
#         options.add_argument("--disable-blink-features=AutomationControlled")
#         options.add_argument("--start-maximized")
#         if cfg.HEADLESS:
#             options.add_argument("--headless=new")
#         drv = webdriver.Chrome(service=Service(chromedriver), options=options)
#         set_driver(drv)

#         # prosty injection przycisku "KONTYNUUJ" (logowanie)
#         inject_js = r'''
#         (function(){
#           if(window._selenium_continue_injected) return;
#           window._selenium_continue_injected = true;
#           const btn = document.createElement('button');
#           btn.id = 'selenium_continue_button';
#           btn.textContent = 'KONTYNUUJ (kliknij po zalogowaniu)';
#           btn.style.position = 'fixed';
#           btn.style.zIndex = 2147483647;
#           btn.style.right = '12px';
#           btn.style.bottom = '12px';
#           btn.style.padding = '12px 18px';
#           btn.style.background = '#1DA1F2';
#           btn.style.color = 'white';
#           btn.style.border = 'none';
#           btn.style.borderRadius = '8px';
#           btn.style.boxShadow = '0 4px 12px rgba(0,0,0,0.3)';
#           btn.style.fontSize = '14px';
#           btn.style.cursor = 'pointer';
#           btn.style.fontFamily = 'Arial, sans-serif';
#           btn.onclick = function(e){ try { window._selenium_continue_clicked = true; btn.remove(); } catch (err) { window._selenium_continue_clicked = true; } };
#           document.body.appendChild(btn);
#         })();
#         '''
#         from selenium.webdriver.support.ui import WebDriverWait
#         from selenium.webdriver.support import expected_conditions as EC
#         from selenium.webdriver.common.by import By
#         import urllib.parse

#         # Otwórz wstępny search, wstrzyknij przycisk i czekaj (max 1h)
#         search_query = f"{keyword} since:{since} until:{until}"
#         search_url = "https://mobile.twitter.com/search?q=" + urllib.parse.quote(search_query)
#         print(f"🔗 Otwieram Twitter (search): {search_url}")
#         drv.get(search_url)
#         try:
#             WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
#         except Exception:
#             pass
#         drv.execute_script(inject_js)
#         print("🔔 Na stronie wstawiono przycisk 'KONTYNUUJ'. Zaloguj się w przeglądarce, a potem kliknij przycisk, by kontynuować.")

#         WAIT_TIMEOUT = 3600
#         try:
#             def _continue_condition(d):
#                 clicked = d.execute_script("return !!window._selenium_continue_clicked")
#                 if clicked: return True
#                 try:
#                     els = d.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
#                     if els and len(els) > 0: return True
#                 except Exception: pass
#                 try:
#                     url = d.current_url or ""
#                     if 'login' not in url and 'signup' not in url:
#                         if 'mobile.twitter.com' in url: return False
#                 except Exception: pass
#                 return False
#             WebDriverWait(drv, WAIT_TIMEOUT, poll_frequency=1).until(_continue_condition)
#             print("✅ Kontynuujemy — kliknięto 'KONTYNUUJ' lub wyniki są dostępne.")
#         except Exception as e:
#             print(f"⚠️ Timeout/błąd podczas oczekiwania: {e}")
#         try:
#             drv.execute_script("var b=document.getElementById('selenium_continue_button'); if(b) b.remove();")
#         except Exception:
#             pass

#     # Analiza (DB-first + top-up)
#     analyze_and_visualize(
#         keyword, since, until, max_t,
#         collection_name=collection_name,
#         use_db_only=only_db,
#         resume_analysis=resume_analysis,
#         refresh=args.refresh
#     )

#     if drv is not None:
#         try: drv.quit()
#         except Exception: pass

# if __name__ == "__main__":
#     main()
