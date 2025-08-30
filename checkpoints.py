import time
from pathlib import Path
import pandas as pd
from datetime import datetime
import config as cfg

def checkpoint_dir(collection_name: str, since: str, until: str) -> Path:
    root = (collection_name or "collection").replace(" ", "_")
    rng  = f"{since}_to_{until}"
    d = cfg.RESULTS_DIR / root / rng / "_checkpoints"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _timestamp():
    return time.strftime("%Y%m%d-%H%M%S")

def _prune_old(d: Path, prefix: str, keep: int):
    files = sorted(d.glob(f"{prefix}_*.csv"))
    if len(files) > keep:
        for f in files[:len(files)-keep]:
            try: f.unlink()
            except Exception: pass

def save_raw_progress(collection_name, since, until, ids, texts, dates, urls):
    d = checkpoint_dir(collection_name, since, until)
    df = pd.DataFrame({
        'id': ids,
        'text': texts,
        'created_at': [dt.isoformat() if isinstance(dt, datetime) else (None if pd.isna(dt) else str(dt)) for dt in dates],
        'url': urls
    })
    latest = d / "raw_progress_latest.csv"
    stamped = d / f"raw_progress_{_timestamp()}.csv"
    df.to_csv(latest, index=False, encoding="utf-8-sig")
    df.to_csv(stamped, index=False, encoding="utf-8-sig")
    _prune_old(d, "raw_progress", cfg.CHECKPOINT_KEEP)
    print(f"💾 [checkpoint] raw → {latest.name} oraz {stamped.name}")

def save_analysis_progress(collection_name, since, until, df: pd.DataFrame):
    d = checkpoint_dir(collection_name, since, until)
    latest = d / "analysis_progress_latest.csv"
    stamped = d / f"analysis_progress_{_timestamp()}.csv"
    df.to_csv(latest, index=False, encoding="utf-8-sig")
    df.to_csv(stamped, index=False, encoding="utf-8-sig")
    _prune_old(d, "analysis_progress", cfg.CHECKPOINT_KEEP)
    print(f"💾 [checkpoint] analysis → {latest.name} oraz {stamped.name}")

def load_raw_progress_latest(collection_name, since, until):
    d = checkpoint_dir(collection_name, since, until)
    f = d / "raw_progress_latest.csv"
    if not f.exists():
        return None
    df = pd.read_csv(f)
    dates = pd.to_datetime(df.get('created_at'), errors='coerce')
    ids   = df.get('id').astype(str).tolist()
    texts = df.get('text').fillna('').astype(str).tolist()
    urls  = df.get('url').fillna('').astype(str).tolist()
    return ids, texts, list(dates), urls

def load_analysis_progress_latest(collection_name, since, until):
    d = checkpoint_dir(collection_name, since, until)
    f = d / "analysis_progress_latest.csv"
    if not f.exists():
        return None
    try:
        df = pd.read_csv(f)
        return df
    except Exception:
        return None
