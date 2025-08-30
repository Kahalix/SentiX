import os
import re
import time
import urllib.parse
import requests
import platform as _platform
from zipfile import ZipFile
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from selenium.webdriver.common.keys import Keys

import pandas as pd
from tqdm.auto import tqdm

import config as cfg
from store import TweetStore, HybridDeduper
import checkpoints as ckp

# ====== driver handle + fabryka (do autorestartu) ======
driver = None
_driver_factory = None

def set_driver(drv):  # main.py wywoła
    global driver
    driver = drv

def register_driver_factory(factory):  # main.py wywoła
    global _driver_factory
    _driver_factory = factory


# ====== cleaning helpers ======
def _clean_tweet(text: str) -> str:
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'[^A-Za-z0-9ąćęłńóśźżĄĆĘŁŃÓŚŹŻ ]', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip().lower()

def _text_fallback_id_from_clean(text):
    import hashlib
    h = hashlib.sha1()
    h.update(_clean_tweet(text).encode('utf-8'))
    return "txt_" + h.hexdigest()


# =========================
# Chrome for Testing helpery + progres
# =========================
PLATFORM_MAP = {
    'Windows': {
        'arch_map': {'AMD64': 'win64', 'x86_64': 'win64', 'x86': 'win32', '': 'win64'},
        'exe_names': ['chrome.exe', 'chrome', 'Chromium.app'],
        'driver_names': ['chromedriver.exe', 'chromedriver']
    },
    'Linux': {
        'arch_map': {'x86_64': 'linux64', 'aarch64': 'linux-arm64', 'arm64': 'linux-arm64'},
        'exe_names': ['chrome', 'chromium', 'chromium-browser'],
        'driver_names': ['chromedriver']
    },
    'Darwin': {
        'arch_map': {'x86_64': 'mac-x64', 'arm64': 'mac-arm64'},
        'exe_names': ['Chromium.app', 'chrome', 'Chromium'],
        'driver_names': ['chromedriver']
    }
}

def _detect_platform():
    sys_pl = _platform.system()
    arch = _platform.machine()
    if sys_pl not in PLATFORM_MAP:
        raise RuntimeError(f"Nieobsługiwana platforma: {sys_pl}")
    arch_map = PLATFORM_MAP[sys_pl]['arch_map']
    plat_key = arch_map.get(arch, None) or list(arch_map.values())[0]
    return sys_pl, arch, plat_key

def _fetch_cft_manifest():
    print("🔎 Pobieram manifest chrome-for-testing ...")
    r = requests.get(cfg.CFT_JSON, timeout=30); r.raise_for_status()
    return r.json()

def _find_downloads_for_channel(data, channel_name, desired_platform):
    res = {'version': None, 'chrome_url': None, 'chromedriver_url': None}
    if 'channels' in data and channel_name in data['channels']:
        ch = data['channels'][channel_name]
        res['version'] = ch.get('version') or ch.get('last_known_good_version')
        downloads = ch.get('downloads', {})
        for kind in ('chrome', 'chromedriver'):
            items = downloads.get(kind, []) or []
            for it in items:
                if it.get('platform') == desired_platform:
                    if kind == 'chrome': res['chrome_url'] = it.get('url')
                    else: res['chromedriver_url'] = it.get('url')
    if not res['chrome_url'] or not res['chromedriver_url']:
        def walk(obj):
            if isinstance(obj, dict):
                if 'url' in obj and isinstance(obj.get('url'), str): yield obj
                for v in obj.values(): yield from walk(v)
            elif isinstance(obj, list):
                for el in obj: yield from walk(el)
        for entry in walk(data):
            u = entry.get('url'); plat = entry.get('platform') or entry.get('os') or ''
            if not u: continue
            if desired_platform in plat or f"-{desired_platform}" in u or f"/{desired_platform}/" in u:
                if 'chrome' in u and not res['chrome_url']: res['chrome_url'] = u
                if 'chromedriver' in u and not res['chromedriver_url']: res['chromedriver_url'] = u
    return res

def _download_with_progress(url: str, dest_zip: Path):
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        desc = f"Downloading {Path(url).name}"
        with open(dest_zip, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024, desc=desc) as pbar:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

def _extract_with_progress(zip_path: Path, target_dir: Path):
    target_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path) as z:
        infos = z.infolist()
        with tqdm(total=len(infos), desc=f"Extracting to {target_dir}", unit="file") as pbar:
            for info in infos:
                z.extract(info, target_dir)
                pbar.update(1)

def _download_and_extract(url, target_dir: Path):
    temp_zip = target_dir / "_temp_download.zip"
    _download_with_progress(url, temp_zip)
    _extract_with_progress(temp_zip, target_dir)
    try: temp_zip.unlink()
    except Exception: pass

def _find_file_recursive(base_dir: Path, names):
    for root, dirs, files in os.walk(base_dir):
        for n in names:
            if n in files:
                return Path(root) / n
        for d in dirs:
            if d in names:
                return Path(root) / d
    return None

def _find_executable_in_dir(base: Path, names):
    if base is None: return None
    base = Path(base)
    if base.exists() and base.is_file(): return base
    if base.exists() and base.is_dir():
        for root, dirs, files in os.walk(base):
            rootp = Path(root)
            for n in names:
                if n in files: return rootp / n
            for f in files:
                fn = f.lower()
                if fn.startswith("chromedriver"): return rootp / f
                if fn.startswith("chrome") and (fn.endswith(".exe") or not fn.endswith(".dll")): return rootp / f
    return None

def ensure_chrome_and_driver(chrome_path_hint=None, driver_path_hint=None):
    chrome_path = None; driver_path = None
    if chrome_path_hint:
        p = Path(chrome_path_hint)
        if p.exists():
            chrome_path = _find_executable_in_dir(p, PLATFORM_MAP.get(_platform.system(), {}).get('exe_names', ['chrome.exe']))
            if chrome_path: print(f"Używam CHROME_BINARY (resolved): {chrome_path}")
    if driver_path_hint:
        p = Path(driver_path_hint)
        if p.exists():
            driver_path = _find_executable_in_dir(p, PLATFORM_MAP.get(_platform.system(), {}).get('driver_names', ['chromedriver.exe']))
            if driver_path: print(f"Używam CHROMEDRIVER (resolved): {driver_path}")
    if chrome_path and driver_path:
        return str(chrome_path), str(driver_path)

    candidate_root = Path(cfg.CFT_OUTDIR)
    if candidate_root.exists():
        sys_pl, arch, desired_platform = _detect_platform()
        exe_names = PLATFORM_MAP[sys_pl]['exe_names']; driver_names = PLATFORM_MAP[sys_pl]['driver_names']
        found_chrome = _find_file_recursive(candidate_root, exe_names)
        found_driver = _find_file_recursive(candidate_root, driver_names)
        if found_chrome:
            resolved = _find_executable_in_dir(found_chrome, exe_names)
            if resolved: chrome_path = resolved; print(f"Znaleziono chrome (resolved): {chrome_path}")
        if found_driver:
            resolved_drv = _find_executable_in_dir(found_driver, driver_names)
            if resolved_drv: driver_path = resolved_drv; print(f"Znaleziono chromedriver (resolved): {driver_path}")
        if chrome_path and driver_path:
            return str(chrome_path), str(driver_path)

    sys_pl, arch, desired_platform = _detect_platform()
    manifest = _fetch_cft_manifest()
    info = _find_downloads_for_channel(manifest, cfg.CFT_CHANNEL, desired_platform)
    version = info.get('version') or 'unknown'
    target_base = Path(cfg.CFT_OUTDIR) / f"{cfg.CFT_CHANNEL}_{version}"
    target_base.mkdir(parents=True, exist_ok=True)

    if info.get('chrome_url'):
        chrome_dir = target_base / "chrome"
        _download_and_extract(info['chrome_url'], chrome_dir)
    else:
        print("⚠️ Nie znaleziono linku do pliku 'chrome'.")
    if info.get('chromedriver_url'):
        driver_dir = target_base / "chromedriver"
        _download_and_extract(info['chromedriver_url'], driver_dir)
    else:
        print("⚠️ Nie znaleziono linku do 'chromedriver'.")

    sys_pl, arch, desired_platform = _detect_platform()
    exe_names = PLATFORM_MAP[sys_pl]['exe_names']; driver_names = PLATFORM_MAP[sys_pl]['driver_names']
    found_chrome = _find_file_recursive(target_base, exe_names)
    found_driver = _find_file_recursive(target_base, driver_names)
    if found_chrome:
        resolved_chrome = _find_executable_in_dir(found_chrome, exe_names)
        if resolved_chrome:
            chrome_path = resolved_chrome; print(f"✅ Chrome znaleziono: {chrome_path}")
    else:
        print("❌ Nie znalazłem pliku wykonywalnego Chrome.")
    if found_driver:
        resolved_driver = _find_executable_in_dir(found_driver, driver_names)
        if resolved_driver:
            driver_path = resolved_driver; print(f"✅ Chromedriver znaleziono: {driver_path}")
    else:
        print("❌ Nie znalazłem chromedrivera.")

    if not chrome_path or not driver_path:
        raise RuntimeError("Brakuje 'chrome' lub 'chromedriver' po instalacji.")
    return str(chrome_path), str(driver_path)


# =========================
# overlay / rate-limit / no-results
# =========================
def _find_retry_button():
    X = (
        "//button[.//span[normalize-space()='Retry']] | "
        "//div[@role='button'][.//span[normalize-space()='Retry']] | "
        "//button[.//span[normalize-space()='Reload']] | "
        "//div[@role='button'][.//span[normalize-space()='Reload']]"
    )
    try:
        btns = driver.find_elements(By.XPATH, X)
        return btns[0] if btns else None
    except Exception:
        return None

def _has_error_overlay():
    X = (
        "//span[contains(., 'Something went wrong')] | "
        "//div[contains(., 'Something went wrong')] | "
        "//span[contains(., 'Try reloading')] | "
        "//span[contains(., 'Too many requests')] | "
        "//span[contains(., 'Rate limit')] | "
        "//span[contains(., 'Coś poszło nie tak')]"
    )
    try:
        return len(driver.find_elements(By.XPATH, X)) > 0
    except Exception:
        return False

def _has_no_results():
    X = (
        "//span[contains(., 'No results')] | "
        "//div[contains(., 'No results')] | "
        "//span[contains(., 'Brak wyników')] | "
        "//div[contains(., 'Brak wyników')]"
    )
    try:
        return len(driver.find_elements(By.XPATH, X)) > 0
    except Exception:
        return False

def _robust_click(el):
    try:
        el.click(); return True
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].click();", el); return True
    except Exception:
        pass
    try:
        el.send_keys(Keys.ENTER); return True
    except Exception:
        return False

def _cooldown_with_progress(total_seconds: int, desc: str = "Cooldown (rate limit)"):
    secs = int(max(0, total_seconds))
    if secs <= 0:
        return
    try:
        with tqdm(total=secs, desc=desc, unit="s") as pbar:
            for _ in range(secs):
                time.sleep(1)
                pbar.update(1)
    except KeyboardInterrupt:
        print("⏭️ Przerwano cooldown — próbuję od razu.")
    except Exception:
        time.sleep(secs)

def wait_and_handle_errors(quick_tries=3, quick_interval=3, cooldown_sec=cfg.RATE_LIMIT_COOLDOWN):
    """
    Zwraca: 'ok' | 'no_results' | 'blocked_recovered' | 'blocked_still'
    """
    try:
        if driver.find_elements(By.XPATH, "//div[@data-testid='tweetText']"):
            return 'ok'
    except Exception:
        pass

    if _has_no_results():
        return 'no_results'

    if _has_error_overlay():
        for _ in range(quick_tries):
            btn = _find_retry_button()
            if btn: _robust_click(btn)
            else:
                try: driver.refresh()
                except Exception: pass
            time.sleep(quick_interval)
            try:
                if driver.find_elements(By.XPATH, "//div[@data-testid='tweetText']"):
                    return 'ok'
            except Exception:
                pass
            if _has_no_results():
                return 'no_results'
            if not _has_error_overlay():
                return 'ok'

        print(f"⏳ Podejrzenie blokady/rate-limit — czekam {cooldown_sec//60} min...")
        _cooldown_with_progress(int(cooldown_sec), desc="Cooldown (rate limit)")

        btn = _find_retry_button()
        if btn: _robust_click(btn)
        else:
            try: driver.refresh()
            except Exception: pass
        time.sleep(5)

        try:
            if driver.find_elements(By.XPATH, "//div[@data-testid='tweetText']"):
                return 'blocked_recovered'
        except Exception:
            pass
        if _has_no_results():
            return 'no_results'
        if _has_error_overlay():
            return 'blocked_still'
        return 'blocked_recovered'

    return 'ok'


# =========================
# odporny get (retry + ewentualny restart drivera)
# =========================
def _robust_get(url: str, attempts: int = 3, wait_after: float = 2.0):
    global driver, _driver_factory
    for i in range(1, attempts + 1):
        try:
            driver.get(url)
            time.sleep(wait_after)
            return True
        except Exception as e:
            print(f"⚠️ driver.get timeout/err (próba {i}/{attempts}): {e}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            if i < attempts and _driver_factory is not None:
                try:
                    driver.quit()
                except Exception:
                    pass
                try:
                    driver = _driver_factory()
                    set_driver(driver)
                    print("🔁 Odtworzyłem przeglądarkę i spróbuję ponownie...")
                except Exception as e2:
                    print(f"❌ Nie udało się odtworzyć drivera: {e2}")
            else:
                return False


# =========================
# tweet id + czas + url
# =========================
def _get_tweet_id_and_dt(el):
    tweet_id = None; dt = None; url = None
    try:
        link = el.find_element(By.XPATH, ".//ancestor::article//a[contains(@href,'/status/')]")
        href = link.get_attribute("href") or ""
        url = href
        m = re.search(r'/status/(\d+)', href)
        tweet_id = m.group(1) if m else None
    except Exception:
        url = None
        tweet_id = None
    try:
        time_el = el.find_element(By.XPATH, ".//ancestor::article//time")
        ts = time_el.get_attribute("datetime")
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00')) if ts else None
    except Exception:
        dt = None
    return tweet_id, dt, url


# =========================
# właściwe scrapowanie (1 podzakres)
# =========================
def fetch_tweets(keyword: str, since_incl: str, until_incl: str, max_tweets: int = 200, deduper=None):
    if deduper is None:
        class _Local:
            def __init__(self): self._s = set()
            def contains(self, u): return u in self._s
            def add(self, u): self._s.add(u)
            def bulk_add(self, seq): self._s.update(seq)
            def close(self): pass
        deduper = _Local()

    since_dt = datetime.fromisoformat(since_incl)
    until_dt = datetime.fromisoformat(until_incl)
    until_excl = (until_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    query = f"{keyword} since:{since_dt.strftime('%Y-%m-%d')} until:{until_excl}"
    url   = "https://mobile.twitter.com/search?q=" + urllib.parse.quote(query) + "&f=live"
    print(f"\n🔗 Otwieram: {url}")

    ok = _robust_get(url)
    if not ok:
        print("❌ Nie udało się wczytać strony po próbach — przerywam ten slice.")
        return [], [], [], []

    # wstępne ogarnięcie overlay
    status = wait_and_handle_errors()
    if status == 'no_results':
        print("ℹ️ Brak wyników dla tego zakresu.")
        return [], [], [], []

    texts, dates, ids, urls = [], [], [], []
    seen_ids = set()
    last_h = driver.execute_script("return document.body.scrollHeight")

    while len(texts) < max_tweets:
        st = wait_and_handle_errors()
        if st in ('no_results', 'blocked_still'):
            break

        els = driver.find_elements(By.XPATH, '//div[@data-testid="tweetText"]')
        for el in els:
            try:
                raw = el.text.strip()
            except StaleElementReferenceException:
                continue
            if not raw:
                continue

            tweet_id, dt, href = _get_tweet_id_and_dt(el)
            uid = tweet_id if tweet_id else _text_fallback_id_from_clean(raw)
            if uid in seen_ids or deduper.contains(uid):
                continue

            seen_ids.add(uid); deduper.add(uid)
            texts.append(raw); dates.append(dt); ids.append(uid); urls.append(href)
            if len(texts) >= max_tweets:
                break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            st2 = wait_and_handle_errors(quick_tries=2, quick_interval=2)
            if st2 in ('no_results', 'blocked_still'):
                break
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
        last_h = new_h

    return texts, dates, ids, urls


# =========================
# scrapowanie w podoknach + zapis do DB + checkpoint RAW + ZWRACANIE LIST
# =========================
def fetch_tweets_in_periods(keyword: str, since: str, until: str, max_tweets: int = 200,
                            collection_name: str = None, resume_raw: bool = False):
    start_dt = datetime.fromisoformat(since)
    end_dt   = datetime.fromisoformat(until)
    total_days = (end_dt.date() - start_dt.date()).days + 1

    # dzielimy na miesiące, a miesiące na <=31 slice’y
    periods = []
    cur = start_dt
    while cur <= end_dt:
        nxt_month = (cur + relativedelta(months=1)).replace(day=1)
        last_of = min(nxt_month - timedelta(days=1), end_dt)
        periods.append((cur, last_of))
        cur = nxt_month

    days_per = [(p[1].date() - p[0].date()).days + 1 for p in periods]
    quotas = [max(1, round(max_tweets * d / total_days)) for d in days_per]
    diff = sum(quotas) - max_tweets
    idx_ord = sorted(range(len(quotas)), key=lambda i: quotas[i], reverse=(diff>0))
    for i in range(abs(diff)):
        j = idx_ord[i % len(quotas)]
        quotas[j] -= 1 if diff>0 else -1
        if quotas[j] < 0: quotas[j] = 0

    texts_all, dates_all, ids_all, urls_all = [], [], [], []

    # DB i kolekcja (jeśli jest)
    store = TweetStore(cfg.DB_PATH)
    coll_id = None
    if collection_name:
        try:
            if hasattr(store, "get_or_create_collection"):
                coll_id = store.get_or_create_collection(collection_name)
            elif hasattr(store, "ensure_collection"):
                coll_id = store.ensure_collection(collection_name)
        except Exception as e:
            print(f"⚠️ Nie mogę utworzyć/odczytać kolekcji: {e}")

    # Resume RAW z checkpointa (jeśli ktoś korzysta)
    if resume_raw:
        prev_df = ckp.load_raw_progress_latest(collection_name or keyword, since, until)
        if prev_df is not None and not prev_df.empty:
            try:
                ids_all  = prev_df["id"].tolist()
                texts_all = prev_df["raw_text"].tolist()
                dates_all = list(prev_df["date"])
                urls_all  = prev_df.get("url", pd.Series([None]*len(ids_all))).tolist()
                print(f"↩️ Resume RAW: przywrócono {len(ids_all)} rekordów z checkpointu.")
                # spróbuj dograć do DB
                if coll_id is not None and ids_all:
                    rows = []
                    for _id, _t, _dt, _u in zip(ids_all, texts_all, dates_all, urls_all):
                        dt_iso = _dt.isoformat() if isinstance(_dt, datetime) else (None if pd.isna(_dt) else str(_dt))
                        rows.append((_id, _t, dt_iso, _u))
                    _db_write_bulk(store, rows, coll_id)
            except Exception as e:
                print(f"⚠️ Resume RAW nie powiódł się: {e}")

    # Deduper (Bloom opcjonalnie)
    deduper = None
    if cfg.USE_BLOOM:
        deduper = HybridDeduper(sqlite_path=cfg.DB_PATH.replace(".sqlite","_ids.sqlite"),
                                expected_n=max(1000, max_tweets*2), fp_rate=1e-5,
                                load_bloom=cfg.BLOOM_SERIAL)
        if ids_all:
            try: deduper.bulk_add(ids_all)
            except Exception: pass
    else:
        class _Local:
            def __init__(self): self._s=set()
            def contains(self,u): return u in self._s
            def add(self,u): self._s.add(u)
            def bulk_add(self,seq): self._s.update(seq)
            def close(self): pass
        deduper = _Local()
        if ids_all: deduper.bulk_add(ids_all)

    last_raw_save_ts = time.time()
    pbar = tqdm(total=max_tweets, initial=len(texts_all),
                desc=f"Scraping '{keyword}' [{since}..{until}]", unit="tw")

    try:
        if len(texts_all) < max_tweets:
            for (period, quota) in zip(periods, quotas):
                if len(texts_all) >= max_tweets: break

                p_start = period[0].date()
                p_end   = period[1].date()
                days = (p_end - p_start).days + 1

                slices = min(days, max(1, min(quota, 31)))
                base, rem = days // slices, days % slices
                slice_lengths = [base + (1 if i < rem else 0) for i in range(slices)]
                slice_quotas  = [max(1, round(quota * sl / days)) for sl in slice_lengths]
                sd = sum(slice_quotas) - quota
                idx_ord_s = sorted(range(len(slice_quotas)), key=lambda i: slice_quotas[i], reverse=(sd>0))
                for i in range(abs(sd)):
                    j = idx_ord_s[i % len(slice_quotas)]
                    slice_quotas[j] -= 1 if sd>0 else -1
                    if slice_quotas[j] < 0: slice_quotas[j] = 0

                cur_day = p_start
                for sl_len, sl_q in zip(slice_lengths, slice_quotas):
                    if len(texts_all) >= max_tweets: break
                    if sl_q <= 0:
                        cur_day = cur_day + timedelta(days=sl_len); continue

                    slice_since = cur_day.isoformat()
                    slice_until = (cur_day + timedelta(days=sl_len-1)).isoformat()
                    remaining   = max_tweets - len(texts_all)
                    need_here   = min(sl_q, remaining)
                    if need_here <= 0:
                        cur_day = cur_day + timedelta(days=sl_len); continue

                    attempts = 0
                    while need_here > 0 and attempts < 3 and len(texts_all) < max_tweets:
                        want = min(need_here, max_tweets - len(texts_all))
                        print(f"Pobieram {keyword} {slice_since}..{slice_until} (chcę {want}; próba {attempts+1}/3)")
                        txts, dts, ids, urls = fetch_tweets(keyword, slice_since, slice_until, want, deduper=deduper)

                        # akumulacja
                        texts_all.extend(txts); dates_all.extend(dts); ids_all.extend(ids); urls_all.extend(urls)
                        added = len(txts)
                        if added > 0:
                            pbar.update(added)
                            pbar.set_postfix_str(f"{len(texts_all)}/{max_tweets}")

                            # zapis do DB
                            if collection_name and ids:
                                rows = []
                                for _id, _t, _dt, _u in zip(ids, txts, dts, urls):
                                    dt_iso = _dt.isoformat() if isinstance(_dt, datetime) else None
                                    rows.append((_id, _t, dt_iso, _u))
                                _db_write_bulk(store, rows, coll_id)

                        # checkpoint RAW (tylko gdy wznawiamy)
                        if resume_raw and added > 0:
                            now = time.time()
                            if (now - last_raw_save_ts) >= cfg.RAW_PROGRESS_EVERY_SEC or added >= cfg.RAW_PROGRESS_EVERY_N_TWEETS:
                                df = pd.DataFrame({"id": ids_all, "raw_text": texts_all, "date": dates_all, "url": urls_all})
                                ckp.save_raw_progress(collection_name or keyword, since, until, df)
                                last_raw_save_ts = now

                        need_here -= added
                        print(f"   → Dodano {added}. Pozostało do zebrania w tym slice: {need_here}")

                        attempts += 1
                        if need_here > 0 and attempts < 3:
                            state = wait_and_handle_errors(quick_tries=2, quick_interval=2)
                            if state in ('no_results', 'blocked_still'): break
                            time.sleep([2,5,10][min(attempts-1, 2)])

                    cur_day = cur_day + timedelta(days=sl_len)

        print(f"✅ Zebrano łącznie {len(texts_all)}/{max_tweets} tweetów (w tej operacji).")
    finally:
        pbar.close()
        try:
            if cfg.USE_BLOOM and hasattr(deduper, "close"):
                try:
                    deduper.save_bloom(cfg.BLOOM_SERIAL)
                except Exception:
                    pass
                deduper.close()
        except Exception:
            pass
        store.close()

    # ZWRACAMY LISTY dla analyzer.py
    return texts_all[:max_tweets], dates_all[:max_tweets], ids_all[:max_tweets], urls_all[:max_tweets]


def _db_write_bulk(store: TweetStore, rows, coll_id):
    try:
        if hasattr(store, "upsert_many") and hasattr(store, "link_many"):
            store.upsert_many(rows)
            if coll_id is not None:
                ids = [r[0] for r in rows]
                store.link_many(ids, coll_id)
        elif hasattr(store, "upsert_tweets_bulk"):
            ids = [r[0] for r in rows]
            texts = [r[1] for r in rows]
            dts = [r[2] for r in rows]
            urls = [r[3] for r in rows]
            store.upsert_tweets_bulk(coll_id, ids, texts, dts, urls)
        elif hasattr(store, "insert_tweets_bulk"):
            ids = [r[0] for r in rows]
            texts = [r[1] for r in rows]
            dts = [r[2] for r in rows]
            urls = [r[3] for r in rows]
            store.insert_tweets_bulk(coll_id, ids, texts, dts, urls)
        else:
            # fallback: pojedynczo
            for _id, _t, _dt, _u in rows:
                if hasattr(store, "upsert_tweet"):
                    store.upsert_tweet(coll_id, _id, _t, _dt, _u)
                elif hasattr(store, "insert_tweet"):
                    store.insert_tweet(coll_id, _id, _t, _dt, _u)
    except Exception as e:
        print(f"⚠️ Błąd zapisu do DB: {e}")