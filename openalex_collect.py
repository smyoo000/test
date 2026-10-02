"""
OpenAlex 쿼리 기반 일괄 수집 스크립트 (분석과업 2 학술 코퍼스용) - 시범 버전
  python openalex_collect.py count     # 쿼리별 예상 건수 확인
  python openalex_collect.py collect   # 수집 (시범: 쿼리당 최대 500건)
  python openalex_collect.py merge     # 중복 제거 → out/corpus.csv
"""

import csv, json, os, sys, time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# ============================== CONFIG ==============================
API_KEY = os.environ.get("OPENALEX_API_KEY", "")  # 환경변수로 지정
BASE = "https://api.openalex.org/works"

# (1) 도메인 경계 쿼리: 재난·안전 분야 모집단
DOMAIN = '(disaster OR "emergency management" OR "disaster risk" OR hazard OR "crisis response")'

# (2) 요구기술 사전 (시범: 2개)
REQUIREMENTS = {
    "R01_damage_estimation": ['"damage assessment"', '"damage estimation"', '"loss estimation"'],
    "R02_early_warning":     ['"early warning"', '"alert dissemination"', '"warning system"'],
}

# (3) 공통 필터
FILTERS = {
    "publication_year": "2015-2026",
    "type": "article|review",
    "language": "en",
    "has_abstract": "true",
}
PER_PAGE = 100
MAX_PER_QUERY = 500      # 시범용 상한 (본 수집 시 20000 등으로 상향)
SLEEP = 0.15

OUT = Path("out"); RAW = OUT / "raw"
# ====================================================================

SELECT = ",".join([
    "id", "doi", "title", "publication_year", "type", "language",
    "abstract_inverted_index", "cited_by_count",
    "primary_location", "primary_topic", "keywords",
])


def build_queries():
    return {rid: f"{DOMAIN} AND ({' OR '.join(syns)})" for rid, syns in REQUIREMENTS.items()}


def filter_str():
    return ",".join(f"{k}:{v}" for k, v in FILTERS.items())


def call(params, retries=5):
    if API_KEY:
        params = {**params, "api_key": API_KEY}
    for i in range(retries):
        r = requests.get(BASE, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503):
            time.sleep(2 ** i)
            continue
        raise RuntimeError(f"{r.status_code}: {r.text[:300]}")
    raise RuntimeError("재시도 초과")


def abstract_from_index(inv):
    if not inv:
        return ""
    pos = {p: w for w, ps in inv.items() for p in ps}
    return " ".join(pos[i] for i in sorted(pos))


def flatten(w):
    loc = w.get("primary_location") or {}
    src = (loc.get("source") or {})
    topic = w.get("primary_topic") or {}
    return {
        "openalex_id": w["id"].rsplit("/", 1)[-1],
        "doi": w.get("doi"),
        "title": w.get("title") or "",
        "abstract": abstract_from_index(w.get("abstract_inverted_index")),
        "year": w.get("publication_year"),
        "type": w.get("type"),
        "language": w.get("language"),
        "source": src.get("display_name"),
        "cited_by_count": w.get("cited_by_count"),
        "primary_topic": topic.get("display_name"),
        "field": (topic.get("field") or {}).get("display_name"),
        "keywords": "; ".join(k.get("display_name", "") for k in (w.get("keywords") or [])),
    }


def log(row):
    path = OUT / "query_log.csv"
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        wr = csv.DictWriter(f, fieldnames=list(row))
        if new:
            wr.writeheader()
        wr.writerow(row)


def count():
    for qid, q in build_queries().items():
        meta = call({"search": q, "filter": filter_str(), "per_page": 1})["meta"]
        print(f"{qid:28s} {meta['count']:>8,}")
        time.sleep(SLEEP)


def collect():
    RAW.mkdir(parents=True, exist_ok=True)
    for qid, q in build_queries().items():
        path = RAW / f"{qid}.jsonl"
        if path.exists():
            print(f"[skip] {qid} (이미 수집됨)"); continue
        tmp = path.with_suffix(".part")
        cursor, n, total = "*", 0, None
        with open(tmp, "w", encoding="utf-8") as f:
            while cursor and n < MAX_PER_QUERY:
                data = call({"search": q, "filter": filter_str(), "per_page": PER_PAGE,
                             "cursor": cursor, "select": SELECT})
                total = data["meta"]["count"]
                for w in data["results"]:
                    f.write(json.dumps(flatten(w), ensure_ascii=False) + "\n")
                n += len(data["results"])
                cursor = data["meta"].get("next_cursor")
                if not data["results"]:
                    break
                print(f"\r{qid}: {n:,}/{total:,}", end="")
                time.sleep(SLEEP)
        tmp.rename(path)
        print()
        log({"query_id": qid, "query": q, "filter": filter_str(), "total_hits": total,
             "collected": n, "collected_at": datetime.now().isoformat(timespec="seconds")})


def merge():
    frames = []
    for p in sorted(RAW.glob("*.jsonl")):
        df = pd.read_json(p, lines=True)
        df["matched_query"] = p.stem
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    n_raw = len(df)

    hits = df.groupby("openalex_id")["matched_query"].apply(lambda s: ";".join(sorted(set(s))))
    df = df.drop_duplicates("openalex_id").drop(columns="matched_query").set_index("openalex_id")
    df["matched_queries"] = hits
    df["n_matched"] = df["matched_queries"].str.count(";") + 1

    n_dedup = len(df)
    df = df[df["abstract"].str.split().str.len() >= 50]
    df.reset_index().to_csv(OUT / "corpus.csv", index=False, encoding="utf-8-sig")

    print(f"원자료 {n_raw:,} → 중복제거 {n_dedup:,} → 초록 필터 후 {len(df):,}")
    print(df["n_matched"].value_counts().sort_index().rename("걸린 쿼리 수별 문헌 수"))

    # 적합성 점검용: 요구기술별 샘플 제목 10개
    for rid in REQUIREMENTS:
        sub = df[df["matched_queries"].str.contains(rid)]
        print(f"\n[{rid}] 샘플 제목 (총 {len(sub):,}건)")
        for t in sub["title"].sample(min(10, len(sub)), random_state=1):
            print("  -", t)


if __name__ == "__main__":
    {"count": count, "collect": collect, "merge": merge}[sys.argv[1] if len(sys.argv) > 1 else "count"]()
