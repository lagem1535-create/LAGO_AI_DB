"""LAGO 뉴스 수집기 — 캐글 노트북과 GitHub Actions에서 같이 씀.

  python collect_news.py                    # 평소: RSS에서 새 기사만 (매일 7시·19시)
  python collect_news.py --backfill-days 30 # 처음 한 번: 지난 30일 치도 모으기

저장: 허깅페이스 lagem/LAGO_AI_DB 의 news/ 폴더 (news.parquet, news.faiss, news.sqlite, news_info.json)
저장하는 것: 제목 + 요약(최대 300자) + 링크 + 날짜 + 언론사. 기사 본문은 저장 안 함.
"""
import argparse, hashlib, html, json, os, re, shutil, sqlite3, subprocess, sys, tempfile, time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests

# ===== 설정 =====
HF_REPO = "lagem/LAGO_AI_DB"
KEEP_DAYS = 30
MAX_SUMMARY = 300
EMB_MODEL = "google/embeddinggemma-300m"
EMB_DIM = 256
KNEWS_REPO = "https://github.com/akngs/knews-rss"   # 국내 언론사 RSS를 매일 저장해 둔 저장소 (CC0) → 지난 기사 채우기용
UA = {"User-Agent": "Mozilla/5.0 (LAGO-AI personal news search)"}
KST = timezone(timedelta(hours=9))

# (언론사, 분야, RSS 주소)
FEEDS = [
    # 국내 (주소 출처: akngs/knews-rss data/feed_specs.csv)
    ("경향신문", "전체", "https://www.khan.co.kr/rss/rssdata/total_news.xml"),
    ("동아일보", "전체", "https://rss.donga.com/total.xml"),
    ("조선일보", "전체", "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml"),
    ("한겨레", "전체", "https://www.hani.co.kr/rss/"),
    ("세계일보", "전체", "http://www.segye.com/Articles/RSSList/segye_recent.xml"),
    ("프레시안", "전체", "https://www.pressian.com/api/v3/site/rss/news"),
    ("시사인", "전체", "https://www.sisain.co.kr/rss/allArticle.xml"),
    ("시사저널", "전체", "http://www.sisajournal.com/rss/allArticle.xml"),
    ("미디어오늘", "전체", "https://www.mediatoday.co.kr/rss/allArticle.xml"),
    ("뉴시스", "정치", "https://newsis.com/RSS/politics.xml"),
    ("뉴시스", "경제", "https://newsis.com/RSS/economy.xml"),
    ("뉴시스", "사회", "https://newsis.com/RSS/society.xml"),
    ("뉴시스", "국제", "https://newsis.com/RSS/international.xml"),
    ("뉴시스", "IT/바이오", "https://newsis.com/RSS/health.xml"),
    ("서울신문", "정치", "https://www.seoul.co.kr/xml/rss/rss_politics.xml"),
    ("서울신문", "경제", "https://www.seoul.co.kr/xml/rss/rss_economy.xml"),
    ("서울신문", "사회", "https://www.seoul.co.kr/xml/rss/rss_society.xml"),
    ("서울신문", "국제", "https://www.seoul.co.kr/xml/rss/rss_international.xml"),
    ("경향신문", "IT", "http://www.khan.co.kr/rss/rssdata/it_news.xml"),
    ("동아일보", "의학과학", "https://rss.donga.com/science.xml"),
    # 해외 — BBC는 공식 RSS
    ("BBC", "Top", "https://feeds.bbci.co.uk/news/rss.xml"),
    ("BBC", "World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC", "Technology", "https://feeds.bbci.co.uk/news/technology/rss.xml"),
    ("BBC", "Science", "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml"),
    ("BBC 코리아", "전체", "https://feeds.bbci.co.uk/korean/rss.xml"),
]
# CNN(RSS가 2024년 이후 멈춤), 내셔널지오그래픽(자체 RSS 없음) → 구글 뉴스 검색 RSS로
# 국민일보: rss.kmib.co.kr 주소가 없어짐(2026-10 확인) → 구글 뉴스 검색으로
GNEWS_SITES = [
    ("CNN", "cnn.com"),
    ("National Geographic", "nationalgeographic.com"),
    ("국민일보", "kmib.co.kr"),
]
KO_SITES = {"kmib.co.kr", "bbc.com/korean"}   # 한국어 구글 뉴스로 검색할 곳
# ================


def log(*a):
    print(*a, flush=True)


def fmt(sec):
    sec = int(max(sec, 0)); h, m = sec // 3600, sec % 3600 // 60
    return f"{h}시간 {m}분" if h else f"{m}분 {sec % 60}초"


def clean(s, limit=None):
    s = re.sub(r"<[^>]+>", " ", html.unescape(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:limit] + "…" if limit and len(s) > limit else s


def norm_url(u):
    """utm_ 같은 추적 꼬리표를 떼서 같은 기사를 한 번만"""
    p = urllib.parse.urlsplit(u.strip())
    q = [(k, v) for k, v in urllib.parse.parse_qsl(p.query) if not k.lower().startswith(("utm_", "fbclid"))]
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, urllib.parse.urlencode(q), ""))


def make_row(pub, sec, title, url, date, summary):
    title = clean(title)
    summary = clean(summary, MAX_SUMMARY)
    if summary.replace(" ", "") in title.replace(" ", "") or title.replace(" ", "") in summary.replace(" ", "")[:len(title)]:
        summary = ""
    url = norm_url(url)
    return {"id": hashlib.sha1(url.encode()).hexdigest()[:16], "source": "news", "publisher": pub, "section": sec,
            "title": title, "url": url, "date": date.astimezone(KST).isoformat(),
            "text": f"[뉴스 {date.astimezone(KST):%Y-%m-%d} {pub}] {title}" + (f". {summary}" if summary else "")}


# ---------- 수집 ----------
def fetch_rss(url):
    import feedparser
    r = requests.get(url, headers=UA, timeout=25)
    r.raise_for_status()
    return feedparser.parse(r.content).entries


def entry_date(e, default):
    t = e.get("published_parsed") or e.get("updated_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else default


def gnews(pub, site, ko, now, rows, report, sec="Google News"):
    """구글 뉴스 검색 RSS로 그 언론사의 최근 하루 기사"""
    q = urllib.parse.quote(f"site:{site} when:1d")
    lang = "hl=ko&gl=KR&ceid=KR:ko" if ko else "hl=en-US&gl=US&ceid=US:en"
    try:
        ents = fetch_rss(f"https://news.google.com/rss/search?q={q}&{lang}")
        for e in ents:
            t = re.sub(rf"\s+-\s+{re.escape(pub)}$", "", clean(e.get("title")), flags=re.I)
            rows.append(make_row(pub, sec, t, e["link"], entry_date(e, now), ""))
        report.append((pub, sec, "OK", len(ents)))
    except Exception as ex:
        report.append((pub, sec, f"실패 {type(ex).__name__}", 0))
    time.sleep(2)


def collect_live(now):
    rows, report, failed = [], [], {}
    for pub, sec, url in FEEDS:
        try:
            ents = fetch_rss(url)
            for e in ents:
                if e.get("link") and e.get("title"):
                    rows.append(make_row(pub, sec, e["title"], e["link"], entry_date(e, now),
                                         e.get("summary") or e.get("description")))
            report.append((pub, sec, "OK", len(ents)))
        except Exception as ex:
            report.append((pub, sec, f"실패 {type(ex).__name__}", 0))
            host = urllib.parse.urlsplit(url).hostname or ""
            failed.setdefault(pub, re.sub(r"^(www|rss|feeds?|news)\.", "", host))
        time.sleep(1)
    for pub, site in GNEWS_SITES:
        gnews(pub, site, site in KO_SITES, now, rows, report)
    # RSS가 막힌 언론사(예: 해외 서버에서 접속 거부)는 구글 뉴스로 대신 받기
    done = {p for p, _ in GNEWS_SITES}
    for pub, site in failed.items():
        if pub not in done and site and "bbc" not in site:
            gnews(pub, site, True, now, rows, report, sec="대신 Google News")
    return rows, report


def backfill_korean(days, now, workdir):
    """knews-rss 저장소의 gh-pages 기록(하루 한 번 RSS 저장)에서 지난 기사 모으기"""
    rows = []
    repo = Path(workdir) / "knews"
    log(f"[지난 기사] 국내: {KNEWS_REPO} 기록 받는 중...")
    subprocess.run(["git", "clone", "-q", "--filter=blob:none", "--no-checkout", "--single-branch", "-b", "gh-pages",
                    KNEWS_REPO, str(repo)], check=True)
    commits = subprocess.run(["git", "-C", str(repo), "log", f"--since={days + 1} days ago", "--format=%H"],
                             capture_output=True, text=True, check=True).stdout.split()
    files = [f for f in subprocess.run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "HEAD", "jsons"],
                                       capture_output=True, text=True).stdout.split() if f.endswith(".json")]
    log(f"  기록 {len(commits)}일 치 × 파일 {len(files)}개")
    t0 = time.time()
    for n, c in enumerate(commits, 1):
        for f in files:
            r = subprocess.run(["git", "-C", str(repo), "show", f"{c}:{f}"], capture_output=True)
            if r.returncode:
                continue
            try:
                items = json.loads(r.stdout)
            except Exception:
                continue
            for it in items:
                try:
                    spec = it.get("spec", {})
                    d = datetime.fromisoformat(it["date"].replace("Z", "+00:00"))
                    rows.append(make_row(spec.get("publisher", {}).get("name", "?"), spec.get("title", ""),
                                         it["title"], it["url"], d, it.get("partialText", "")))
                except Exception:
                    continue
        if n % max(1, len(commits) // 10) == 0 or n == len(commits):
            log(f"  국내 [{'#' * int(n / len(commits) * 30):<30}] {n}/{len(commits)}일  기사 {len(rows):,}개  경과 {fmt(time.time() - t0)}")
    return rows


def backfill_foreign(days, now):
    """BBC·CNN·내셔널지오그래픽: 구글 뉴스 검색을 하루씩 (RSS는 최근 것만 있어서)"""
    sites = [("BBC", "bbc.com"), ("BBC 코리아", "bbc.com/korean")] + GNEWS_SITES
    rows, t0 = [], time.time()
    log(f"[지난 기사] 해외: 구글 뉴스 검색 {days}일 × {len(sites)}곳")
    for i in range(days):
        day = (now - timedelta(days=i + 1)).date()
        for pub, site in sites:
            lang = "hl=ko&gl=KR&ceid=KR:ko" if site in KO_SITES else "hl=en-US&gl=US&ceid=US:en"
            q = urllib.parse.quote(f"site:{site} after:{day} before:{day + timedelta(days=1)}")
            try:
                for e in fetch_rss(f"https://news.google.com/rss/search?q={q}&{lang}"):
                    t = re.sub(r"\s+-\s+[^-]+$", "", clean(e.get("title")))
                    rows.append(make_row(pub, "Google News", t, e["link"], entry_date(e, now), ""))
            except Exception:
                pass
            time.sleep(1.5)
        if (i + 1) % 5 == 0 or i + 1 == days:
            log(f"  해외 [{'#' * int((i + 1) / days * 30):<30}] {i + 1}/{days}일  기사 {len(rows):,}개  경과 {fmt(time.time() - t0)}")
    return rows


# ---------- 저장 ----------
def hf():
    from huggingface_hub import HfApi, hf_hub_download, login
    token = os.environ.get("HF_TOKEN")
    if not token:
        try:
            from kaggle_secrets import UserSecretsClient
            token = UserSecretsClient().get_secret("HF_TOKEN"); os.environ["HF_TOKEN"] = token
        except Exception:
            sys.exit("[오류] HF_TOKEN 없음 (캐글 Secrets 또는 GitHub Secrets에 HF_TOKEN 추가)")
    login(token)
    api = HfApi()
    api.create_repo(HF_REPO, repo_type="dataset", private=True, exist_ok=True)
    return api, hf_hub_download


def main(backfill_days=0):
    import faiss, pyarrow as pa, pyarrow.parquet as pq
    now = datetime.now(KST)
    cut = (now - timedelta(days=KEEP_DAYS)).isoformat()
    log(f"뉴스 수집 시작: {now:%Y-%m-%d %H:%M} (KST)" + (f"  | 지난 {backfill_days}일 채우기" if backfill_days else ""))
    api, dl = hf()
    work = Path(tempfile.mkdtemp(prefix="news_"))

    # 1. 기존 뉴스
    old = []
    if "news/news.parquet" in set(api.list_repo_files(HF_REPO, repo_type="dataset")):
        old = pq.read_table(dl(HF_REPO, "news/news.parquet", repo_type="dataset")).to_pylist()
    kept = [r for r in old if r["date"] >= cut]
    log(f"기존 {len(old):,}개 → {KEEP_DAYS}일 지난 {len(old) - len(kept):,}개 삭제 → {len(kept):,}개 유지")

    # 2. 새로 받기
    cand, report = collect_live(now)
    if backfill_days:
        cand += backfill_korean(backfill_days, now, work)
        cand += backfill_foreign(backfill_days, now)
    log(f"\n{'언론사':<12}{'분야':<14}{'상태':<20}{'목록':>5}")
    for pub, sec, st, n in report:
        log(f"{pub:<12}{sec:<14}{st:<20}{n:>5}")

    seen_url = {r["url"] for r in kept}
    seen_title = {re.sub(r"\s+", "", r["title"]) for r in kept}
    new = []
    for r in sorted(cand, key=lambda r: r["date"], reverse=True):
        key = re.sub(r"\s+", "", r["title"])
        if r["date"] < cut or r["date"] > (now + timedelta(hours=1)).isoformat() or not key:
            continue
        if r["url"] in seen_url or key in seen_title:
            continue
        seen_url.add(r["url"]); seen_title.add(key); new.append(r)
    log(f"\n새 기사 {len(new):,}개 (후보 {len(cand):,}개 중 중복·기간 밖 제외)")

    # 3. 벡터 (CPU)
    if new:
        from sentence_transformers import SentenceTransformer
        import torch
        n_gpu = torch.cuda.device_count()
        dev = "cuda" if n_gpu else "cpu"
        log(f"벡터 만들기: {len(new):,}개, 장치 {dev.upper()} x{max(n_gpu, 1)}" + (" (CPU는 느림: 1만 개에 수십 분)" if dev == "cpu" else ""))
        pool = None
        if n_gpu >= 2:   # GPU 2개 이상: 나눠서 동시에
            emb = SentenceTransformer(EMB_MODEL, device="cpu")
            pool = emb.start_multi_process_pool(target_devices=[f"cuda:{i}" for i in range(n_gpu)])
        else:
            emb = SentenceTransformer(EMB_MODEL, device=dev, truncate_dim=EMB_DIM)

        def enc(batch):
            if pool is None:
                return np.asarray(emb.encode_document(batch), dtype="float32")
            try:
                v = emb.encode_document(batch, pool=pool, batch_size=128, convert_to_numpy=True)
            except TypeError:
                v = emb.encode_multi_process(batch, pool, prompt_name="document", batch_size=128)
            return np.asarray(v, dtype="float32")[:, :EMB_DIM]

        texts, out, t0, nxt = [r["text"] for r in new], [], time.time(), 0.02
        step = max(512, len(texts) // 50) if pool else (256 if dev == "cuda" else 64)   # 2%쯤마다 한 줄
        for s in range(0, len(texts), step):
            out.append(enc(texts[s:s + step]))
            p = min(s + step, len(texts)) / len(texts)
            if p >= nxt - 1e-9:
                el = time.time() - t0
                log(f"  벡터 [{'#' * int(p * 30)}{'-' * (30 - int(p * 30))}] {p * 100:5.1f}%  "
                    f"{min(s + step, len(texts)):,}/{len(texts):,}  경과 {fmt(el)} / 남은 약 {fmt(el / p - el)}")
                while nxt <= p + 1e-9:
                    nxt += 0.02   # 2%마다
        if pool is not None:
            emb.stop_multi_process_pool(pool)
        v = np.concatenate(out); v /= np.linalg.norm(v, axis=1, keepdims=True) + 1e-12
        for r, x in zip(new, v):
            r["vec"] = x.astype("float16").tobytes()

    rows = sorted(kept + new, key=lambda r: r["date"], reverse=True)
    if not rows:
        sys.exit("[중단] 저장할 기사가 없음 (위 표에서 RSS 상태 확인)")

    # 4. 저장 → 허깅페이스 news/
    pq.write_table(pa.Table.from_pylist(rows), work / "news.parquet", compression="zstd")
    mat = np.stack([np.frombuffer(r["vec"], dtype="float16") for r in rows]).astype("float32")
    index = faiss.IndexFlatIP(EMB_DIM); index.add(mat)
    faiss.write_index(index, str(work / "news.faiss"))
    con = sqlite3.connect(work / "news.sqlite")
    con.execute("CREATE TABLE chunks (id INTEGER PRIMARY KEY, source TEXT, title TEXT, url TEXT, text TEXT, date TEXT, publisher TEXT)")
    con.executemany("INSERT INTO chunks VALUES (?, 'news', ?, ?, ?, ?, ?)",
                    ((i, r["title"], r["url"], r["text"], r["date"], r["publisher"]) for i, r in enumerate(rows)))
    con.commit(); con.close()
    json.dump({"emb_model": EMB_MODEL, "emb_dim": EMB_DIM, "updated": now.isoformat(), "count": len(rows),
               "keep_days": KEEP_DAYS}, open(work / "news_info.json", "w"), ensure_ascii=False, indent=2)
    for f in ["news.parquet", "news.faiss", "news.sqlite", "news_info.json"]:
        api.upload_file(path_or_fileobj=str(work / f), path_in_repo=f"news/{f}", repo_id=HF_REPO, repo_type="dataset",
                        commit_message=f"news {now:%Y-%m-%d %H:%M}")

    from collections import Counter
    log(f"\n[완료] 뉴스 {len(rows):,}개 저장 (새로 {len(new):,} / 유지 {len(kept):,} / 삭제 {len(old) - len(kept):,})")
    log(f"  기간: {rows[-1]['date'][:10]} ~ {rows[0]['date'][:10]}")
    log("  언론사별:", ", ".join(f"{k} {v:,}" for k, v in Counter(r["publisher"] for r in rows).most_common()))
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill-days", type=int, default=0)
    main(ap.parse_args().backfill_days)
