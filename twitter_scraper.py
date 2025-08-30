import os
import re
import time
import urllib.parse
import requests
import platform as _platform
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.keys import Keys

import pandas as pd
from tqdm.auto import tqdm  # ✅ progres

import config as cfg
from store import TweetStore, HybridDeduper
import checkpoints as ckp

# ====== driver handle ======
driver = None
def set_driver(drv):
    global driver
    driver = drv

# ====== cleaning helpers ======
def _clean_tweet(text: str) -> str:
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'[^A-Za-z0-9ąćęłńóśźżĄĆĘŁŃÓŚŹŻ ]', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip().lower()

def _text_fallback_id_from_clean(text):
    cleaned = _clean_tweet(text)
    import hashlib
    h = hashlib.sha1()
    h.update(cleaned.encode('utf-8'))
    return "txt_" + h.hexdigest()

# =========================
# Chrome for Testing helpery
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

# ===== Progresowe pobranie i rozpakowanie =====
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
    try:
        temp_zip.unlink()
    except Exception:
        pass

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

    if chrome_path and str(chrome_path).endswith('.app'):
        candidate = Path(chrome_path) / 'Contents' / 'MacOS'
        if candidate.exists():
            for f in candidate.iterdir():
                if f.is_file(): chrome_path = f; break

    if not chrome_path or not driver_path:
        raise RuntimeError("Brakuje 'chrome' lub 'chromedriver' po instalacji.")
    return str(chrome_path), str(driver_path)

# =========================
# detekcja overlay / rate limit / brak wyników
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
        # szybkie próby kliknięcia/refresh
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

        # --- pasek postępu na cooldownie ---
        mins = int(cooldown_sec) // 60
        print(f"⏳ Podejrzenie blokady/rate-limit — czekam {mins} min...")
        _cooldown_with_progress(int(cooldown_sec), desc="Cooldown (rate limit)")

        # po cooldownie spróbuj ponownie
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

def _cooldown_with_progress(total_seconds: int, desc: str = "Cooldown (rate limit)"):
    """
    Odlicza 'total_seconds' w dół z paskiem tqdm.
    Bezpieczny na Ctrl+C – przerwanie skraca czekanie i od razu przechodzimy dalej.
    """
    secs = int(max(0, total_seconds))
    if secs <= 0:
        return
    try:
        from tqdm.auto import tqdm
        with tqdm(total=secs, desc=desc, unit="s") as pbar:
            for _ in range(secs):
                time.sleep(1)
                pbar.update(1)
    except KeyboardInterrupt:
        print("⏭️ Przerwano cooldown — próbuję od razu.")
        return
    except Exception:
        # w razie problemów z tqdm – klasyczny sleep
        time.sleep(secs)

# =========================
# tweet id + czas + url
# =========================
def _get_tweet_id_and_dt(el):
    try:
        link = el.find_element(By.XPATH, ".//ancestor::article//a[contains(@href,'/status/')]")
        href = link.get_attribute("href") or ""
        m = re.search(r'/status/(\d+)', href)
        tweet_id = m.group(1) if m else None
    except Exception:
        tweet_id, href = None, None
    try:
        time_el = el.find_element(By.XPATH, ".//ancestor::article//time")
        ts = time_el.get_attribute("datetime")
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00')) if ts else None
    except Exception:
        dt = None
    return tweet_id, dt, href

# =========================
# właściwe scrapowanie (1 zakres)
# =========================
def fetch_tweets(keyword: str, since_incl: str, until_incl: str, max_tweets: int = 200, deduper: HybridDeduper = None):
    since_dt = datetime.fromisoformat(since_incl)
    until_dt = datetime.fromisoformat(until_incl)
    until_excl = (until_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    query = f"{keyword} since:{since_dt.strftime('%Y-%m-%d')} until:{until_excl}"
    url   = "https://mobile.twitter.com/search?q=" + urllib.parse.quote(query) + "&f=live"
    print(f"\n🔗 Otwieram: {url}")
    driver.get(url); time.sleep(2)

    texts, dates, ids, urls = [], [], [], []
    seen_ids = set()
    last_h = driver.execute_script("return document.body.scrollHeight")

    while len(texts) < max_tweets:
        state = wait_and_handle_errors()
        if state == 'no_results':
            print("ℹ️ Brak wyników dla tego zakresu."); break
        elif state == 'blocked_still':
            print("❌ Wciąż blokada po cooldownie — przerywam."); break

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
            if uid in seen_ids:
                continue

            if deduper is not None:
                if deduper.contains(uid):
                    continue
                else:
                    deduper.add(uid)

            seen_ids.add(uid)
            texts.append(raw); dates.append(dt); ids.append(uid); urls.append(href)

            if len(texts) >= max_tweets:
                break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            state2 = wait_and_handle_errors(quick_tries=2, quick_interval=2)
            if state2 in ('no_results', 'blocked_still'):
                break
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
        last_h = new_h

    return texts, dates, ids, urls

# =========================
# scrapowanie w podoknach + zapis do DB + checkpoint RAW + resume RAW + PROGRESS BAR
# =========================
def fetch_tweets_in_periods(keyword: str, since: str, until: str, max_tweets: int,
                            collection_name: str, resume_raw: bool = False):
    start_dt = datetime.fromisoformat(since)
    end_dt   = datetime.fromisoformat(until)
    total_days = (end_dt.date() - start_dt.date()).days + 1

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

    texts_all, dates_all, ids_all, urls_all = [], [], [], []

    store = TweetStore(cfg.DB_PATH)
    coll_id = store.get_or_create_collection(collection_name)

    # Resume RAW z checkpointa
    if resume_raw:
        pre = ckp.load_raw_progress_latest(collection_name, since, until)
        if pre:
            pre_ids, pre_texts, pre_dates, pre_urls = pre
            ids_all.extend(pre_ids); texts_all.extend(pre_texts); dates_all.extend(pre_dates); urls_all.extend(pre_urls)
            rows = []
            for _id, _t, _dt, _u in zip(pre_ids, pre_texts, pre_dates, pre_urls):
                dt_iso = _dt.isoformat() if isinstance(_dt, datetime) else (None if pd.isna(_dt) else str(_dt))
                rows.append((_id, _t, dt_iso, _u))
            store.upsert_many(rows)
            store.link_many(pre_ids, coll_id)
            print(f"↩️ Resume RAW: {len(pre_ids)} tweetów już w checkpointach — dołączono.")

    deduper = None
    if cfg.USE_BLOOM:
        deduper = HybridDeduper(sqlite_path=cfg.DB_PATH, expected_n=max(1000, max_tweets*2),
                                fp_rate=1e-5, load_bloom=cfg.BLOOM_SERIAL)
        if ids_all:
            deduper.bulk_add(ids_all)

    # Timery checkpointów
    last_save_time = time.time()
    last_saved_count = len(texts_all)

    RETRIES_PER_SLICE = 3
    BACKOFFS = [2, 5, 10]

    # === PROGRESS BAR ===
    pbar = tqdm(total=max_tweets, initial=len(texts_all),
                desc=f"Scraping '{keyword}' [{since}..{until}]",
                unit="tw")

    try:
        if len(texts_all) >= max_tweets:
            print(f"✅ Resume RAW: osiągnięto limit {max_tweets} — nic do scrapowania.")
        else:
            for (period, quota) in zip(periods, quotas):
                if len(texts_all) >= max_tweets:
                    break

                p_start = period[0].date()
                p_end   = period[1].date()
                days = (p_end - p_start).days + 1

                slices = min(days, quota, 31) or 1
                base, rem = days // slices, days % slices
                slice_lengths = [base + (1 if i < rem else 0) for i in range(slices)]
                slice_quotas  = [max(1, round(quota * sl / days)) for sl in slice_lengths]
                sd = sum(slice_quotas) - quota
                idx_ord_s = sorted(range(len(slice_quotas)), key=lambda i: slice_quotas[i], reverse=(sd>0))
                for i in range(abs(sd)):
                    j = idx_ord_s[i % len(slice_quotas)]
                    slice_quotas[j] -= 1 if sd>0 else -1
                    if slice_quotas[j] < 0:
                        slice_quotas[j] = 0

                cur_day = p_start
                for sl_len, sl_q in zip(slice_lengths, slice_quotas):
                    if len(texts_all) >= max_tweets:
                        break
                    if sl_q <= 0:
                        cur_day = cur_day + timedelta(days=sl_len)
                        continue

                    slice_since = cur_day.isoformat()
                    slice_until = (cur_day + timedelta(days=sl_len-1)).isoformat()
                    remaining   = max_tweets - len(texts_all)
                    needed_for_slice = min(sl_q, remaining)
                    if needed_for_slice <= 0:
                        cur_day = cur_day + timedelta(days=sl_len)
                        continue

                    slice_attempt = 0
                    while needed_for_slice > 0 and slice_attempt < RETRIES_PER_SLICE and len(texts_all) < max_tweets:
                        want = min(needed_for_slice, max_tweets - len(texts_all))
                        print(f"Pobieram {keyword} {slice_since}..{slice_until} (chcę {want}; próba {slice_attempt+1}/{RETRIES_PER_SLICE})")
                        txts, dts, ids, urls = fetch_tweets(keyword, slice_since, slice_until, want, deduper=deduper)

                        texts_all.extend(txts); dates_all.extend(dts); ids_all.extend(ids); urls_all.extend(urls)

                        # update progress bar
                        added = len(txts)
                        if added > 0:
                            pbar.update(added)
                            pbar.set_postfix_str(f"{len(texts_all)}/{max_tweets}")

                        # Zapis do DB + link do kolekcji
                        rows = []
                        for _id, _t, _dt, _u in zip(ids, txts, dts, urls):
                            dt_iso = _dt.isoformat() if isinstance(_dt, datetime) else None
                            rows.append((_id, _t, dt_iso, _u))
                        store.upsert_many(rows)
                        store.link_many(ids, coll_id)

                        # RAW checkpoint?
                        now = time.time()
                        got_since_last = len(texts_all) - last_saved_count
                        if got_since_last >= cfg.RAW_PROGRESS_EVERY_N_TWEETS or (now - last_save_time) >= cfg.RAW_PROGRESS_EVERY_SEC:
                            ckp.save_raw_progress(collection_name, since, until, ids_all, texts_all, dates_all, urls_all)
                            last_saved_count = len(texts_all)
                            last_save_time = now

                        needed_for_slice -= added
                        print(f"   → Dodano {added}. Pozostało do zebrania w tym slice: {needed_for_slice}")

                        slice_attempt += 1
                        if needed_for_slice > 0 and slice_attempt < RETRIES_PER_SLICE:
                            state = wait_and_handle_errors(quick_tries=2, quick_interval=2)
                            if state in ('no_results', 'blocked_still'):
                                break
                            time.sleep(BACKOFFS[min(slice_attempt-1, len(BACKOFFS)-1)])

                    cur_day = cur_day + timedelta(days=sl_len)

        print(f"✅ Zebrano łącznie {len(texts_all)}/{max_tweets} tweetów (w tej operacji).")
    finally:
        pbar.close()
        if deduper is not None:
            deduper.save_bloom(cfg.BLOOM_SERIAL)
            deduper.close()
        store.close()

    return texts_all[:max_tweets], dates_all[:max_tweets], ids_all[:max_tweets], urls_all[:max_tweets]
