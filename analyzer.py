import re
import pandas as pd
from datetime import datetime
import time

from transformers import pipeline
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from tqdm.auto import tqdm

import config as cfg
from store import TweetStore
import checkpoints as ckp
from twitter_scraper import fetch_tweets_in_periods

# ===== Stopwords (PL) =====
try:
    import stopwordsiso as _siso
    STOPWORDS_PL = set(_siso.stopwords("pl") or [])
    print("✅ Załadowano stopwords z pakietu stopwordsiso (pełna lista).")
except Exception:
    STOPWORDS_PL = {
        "i","w","się","na","z","do","że","to","jest","jak","o","od","dla","nie","a","tak","ale",
        "czy","po","przez","jego","jej","ich","mnie","mi","ty","on","ona","ono","my","wy","bez",
        "ten","ta","to","te","tych","tego","tej","jestem","być","byl",
    }
    print("⚠️ Nie znaleziono 'stopwordsiso'. Używam ograniczonego fallbacku.")

def remove_stopwords(text: str, stopwords: set) -> str:
    return " ".join(token for token in text.split() if token not in stopwords)

def clean_tweet(text: str) -> str:
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'[^A-Za-z0-9ąćęłńóśźżĄĆĘŁŃÓŚŹŻ ]', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip().lower()

# ===== Model sentymentu =====
sentiment_pl = pipeline(
    "sentiment-analysis",
    model="bardsai/twitter-sentiment-pl-base",
    tokenizer="bardsai/twitter-sentiment-pl-base",
    device=-1
)

def signed_score_from_label(label, score):
    if label == 'positive': return score
    elif label == 'negative': return -score
    else: return 0.0

# ===== DB-first dataset + top-up z Twittera =====
def prepare_dataset(keyword: str,
                    collection_name: str,
                    since: str,
                    until: str,
                    max_tweets: int,
                    allow_scrape: bool = True,
                    resume_raw: bool = False,
                    refresh: bool = False):
    store = TweetStore(cfg.DB_PATH)

    rows = store.fetch_collection_in_range(collection_name, since, until)
    have = len(rows)

    if allow_scrape:
        need = max_tweets - have
        if refresh:
            need = max(max_tweets, need)
        if need > 0:
            print(f"🔄 Top-up z Twittera: potrzebuję ~{need} tweetów w oknie {since}..{until}")
            fetch_tweets_in_periods(keyword, since, until, need, collection_name=collection_name, resume_raw=resume_raw)
            rows = store.fetch_collection_in_range(collection_name, since, until)

    store.close()

    if len(rows) > max_tweets:
        rows = rows[:max_tweets]

    ids   = [r[0] for r in rows]
    texts = [r[1] for r in rows]
    dates = [pd.to_datetime(r[2]) if r[2] else pd.NaT for r in rows]
    urls  = [r[3] for r in rows]
    return ids, texts, dates, urls

# ===== Analiza i wizualizacja (z resume) + PROGRESS BAR + Parquet/CSV =====
def analyze_and_visualize(keyword, since, until, max_tweets,
                          collection_name=None,
                          use_db_only=False,
                          resume_analysis=False,
                          refresh=False):
    allow_scrape = not use_db_only
    collection_name = collection_name or keyword

    ids, raws, dates, urls = prepare_dataset(
        keyword=keyword,
        collection_name=collection_name,
        since=since,
        until=until,
        max_tweets=max_tweets,
        allow_scrape=allow_scrape,
        resume_raw=resume_analysis,
        refresh=refresh
    )

    if not ids:
        print("❌ Brak tweetów do analizy.")
        return

    root = (collection_name or keyword).replace(" ", "_")
    rng  = f"{since}_to_{until}"
    path = cfg.RESULTS_DIR / root / rng
    path.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame({'id': ids, 'raw_text': raws, 'date': dates, 'url': urls}).drop_duplicates('id')

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['clean']    = df['raw_text'].apply(clean_tweet)
    df['clean_ns'] = df['clean'].apply(lambda txt: remove_stopwords(txt, STOPWORDS_PL))

    for c in ('sentiment','score','polarity'):
        if c not in df.columns:
            df[c] = None

    # Resume analysis (merge po id)
    if resume_analysis:
        chk = ckp.load_analysis_progress_latest(collection_name, since, until)
        if chk is not None and 'id' in chk.columns:
            chk = chk.drop_duplicates(subset='id', keep='last')
            for c in ['sentiment','score','polarity','clean','clean_ns']:
                if c in chk.columns:
                    df = df.merge(chk[['id', c]].rename(columns={c: f'{c}_chk'}), on='id', how='left')
                    if c in df.columns:
                        df[c] = df[c].combine_first(df.get(f'{c}_chk'))
                    else:
                        df[c] = df.get(f'{c}_chk')
                    df.drop(columns=[f'{c}_chk'], inplace=True)
            already = df['sentiment'].notna().sum()
            if already:
                print(f"↩️ Resume ANALYSIS: wykryto {already} już policzonych rekordów.")

    # policz tylko brakujące — PROGRESS BAR
    todo_mask = df['sentiment'].isna() | (df['sentiment'] == '')
    todo_idx = df.index[todo_mask].tolist()

    if len(todo_idx) > 0:
        batch_size = 32
        last_analysis_save = time.time()
        pbar = tqdm(total=len(todo_idx), desc="Analyzing (sentiment)", unit="tw")
        for start in range(0, len(todo_idx), batch_size):
            idxs = todo_idx[start:start+batch_size]
            batch = df.loc[idxs, 'clean_ns'].tolist()
            try:
                out = sentiment_pl(batch)
            except Exception as e:
                print("⚠️ Błąd w transformerze dla batcha:", e)
                out = [{'label':'neutral','score':0.5} for _ in batch]

            for row_i, r in zip(idxs, out):
                df.at[row_i, 'sentiment'] = r['label']
                df.at[row_i, 'score']     = float(r['score'])
                df.at[row_i, 'polarity']  = signed_score_from_label(r['label'], float(r['score']))

            pbar.update(len(idxs))
            pbar.set_postfix_str(f"done {min(start+batch_size, len(todo_idx))}/{len(todo_idx)}")

            now = time.time()
            if (now - last_analysis_save) >= cfg.AN_PROGRESS_MIN_INTERVAL_SEC or (start + batch_size) >= len(todo_idx):
                ckp.save_analysis_progress(collection_name, since, until, df)
                last_analysis_save = now
        pbar.close()
    else:
        print("ℹ️ Nic do policzenia — wszystko już przeanalizowane.")

    # Final + wykresy + zapisy
    csv_file = path / f"{root}_{since}_to_{until}.csv"
    parquet_file = path / f"{root}_{since}_to_{until}.parquet"

    wrote_any = False
    if cfg.SAVE_CSV:
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"💾 CSV zapisane: {csv_file}")
        wrote_any = True
    else:
        print("⏭️ Pomiń zapis CSV (flaga --no-csv).")

    if cfg.SAVE_PARQUET:
        try:
            df.to_parquet(parquet_file, index=False)
            print(f"💾 Parquet zapisany: {parquet_file}")
            wrote_any = True
        except Exception as e:
            print(f"⚠️ Nie udało się zapisać Parquet ({e}). Zainstaluj 'pyarrow' lub 'fastparquet'.")
    else:
        print("⏭️ Pomiń zapis Parquet (flaga --no-parquet).")

    if not wrote_any:
        print("⚠️ Uwaga: wyłączone zapisy CSV i Parquet — wyniki nie zostały zserializowane do plików.")

    counts = df['sentiment'].value_counts().reindex(['positive','neutral','negative']).fillna(0)
    fig, ax = plt.subplots()
    counts.plot.bar(ax=ax)
    ax.set_title("Rozkład nastrojów")
    ax.set_ylabel("Liczba tweetów")
    plt.tight_layout()
    bar = path / "sentiment_distribution.png"
    plt.savefig(bar); plt.close()
    print(f"💾 Wykres zapisany do {bar}")

    df_t = df.dropna(subset=['date']).copy()
    if not df_t.empty:
        df_t['day'] = df_t['date'].dt.date
        trend = df_t.groupby('day')['polarity'].mean()
        fig, ax = plt.subplots()
        ax.plot(trend.index, trend.values, marker='o')
        ax.set_title("Średnia polaryzacja w kolejnych dniach")
        ax.set_ylabel("Polaryzacja (–1 do +1)")
        ax.set_xlabel("Data")
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.xticks(rotation=45, ha='right'); plt.tight_layout()
        tr = path / "polarity_trend.png"
        plt.savefig(tr); plt.close()
        print(f"💾 Wykres zapisany do {tr}")
    else:
        print("⚠️ Brak dat do wykresu trendu polaryzacji.")

    all_txt = " ".join(df['clean_ns'])
    if all_txt.strip():
        wc = WordCloud(width=800, height=400, background_color='white').generate(all_txt)
        fig, ax = plt.subplots(figsize=(10,5))
        ax.imshow(wc, interpolation='bilinear')
        ax.axis('off')
        ax.set_title("Chmura słów (bez stopwords)")
        wc_file = path / "wordcloud.png"
        plt.savefig(wc_file); plt.close()
        print(f"💾 Chmura słów zapisana do {wc_file}")
    else:
        print("⚠️ Brak tekstu po usunięciu stopwords — pomijam chmurę słów.")
