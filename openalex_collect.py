#!/usr/bin/env python3
"""
분석과업 2 — 요구기술(R01~R10) 문헌 코퍼스 1차 수집: OpenAlex API

사용 예)
  # 1) 검색식별 건수만 먼저 확인 (검색식 조정 단계)
  python openalex_collect.py --mailto you@snu.ac.kr --count-only

  # 2) 전체 수집
  python openalex_collect.py --mailto you@snu.ac.kr

  # 3) 일부 R만 다시 수집
  python openalex_collect.py --mailto you@snu.ac.kr --only R03,R07

출력 (out/ 폴더, 실행 시각별 하위폴더)
  works.csv        문헌 단위 1행 (중복 제거, matched_R에 걸린 R 목록) + 선별용 빈 칸
  hits.csv         R × 문헌 대응 (어떤 검색식에 걸렸는지 = 수집 경로 기록)
  run_log.json     실행 일시·검색식 원문·총 건수·수집 건수·초록 확보율 (재현성 기록)
  raw/R01.jsonl …  API 원본 응답 (--save-raw 지정 시)

주의
  - matched_R은 "검색식에 걸린 경로"이지 코딩 결과가 아닙니다(부록 D.2).
    R 부여는 전문 문단 코딩에서 별도로 수행합니다.
  - 초록은 OpenAlex의 abstract_inverted_index를 복원한 것으로, 일부 출판사
    문헌은 초록이 비어 있습니다(abstract_available = 0).

필요 패키지: pip install requests pyyaml
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

API = "https://api.openalex.org/works"
SELECT = ",".join([
    "id", "doi", "display_name", "publication_year", "publication_date", "type",
    "language", "primary_location", "open_access", "best_oa_location",
    "authorships", "cited_by_count", "abstract_inverted_index",
    "primary_topic", "keywords", "referenced_works_count",
])
PER_PAGE = 200


# ---------------------------------------------------------------- helpers
def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def build_filter(seed: str, context: str, st: dict) -> str:
    q = norm_ws(f"{seed} AND {context}") if context else norm_ws(seed)
    if "," in q:
        raise ValueError(f"검색식에 콤마(,)가 있으면 안 됩니다: {q}")
    parts = [f"title_and_abstract.search:{q}"]
    if st.get("from_year") or st.get("to_year"):
        parts.append(f"publication_year:{st.get('from_year', '')}-{st.get('to_year', '')}")
    if st.get("types"):
        parts.append("type:" + "|".join(st["types"]))
    if st.get("languages"):
        parts.append("language:" + "|".join(st["languages"]))
    return ",".join(parts)


def rebuild_abstract(inv: dict | None) -> str:
    """abstract_inverted_index {word: [pos,...]} → 원문 순서 문자열"""
    if not inv:
        return ""
    pos = {}
    for word, idxs in inv.items():
        for i in idxs:
            pos[i] = word
    return " ".join(pos[i] for i in sorted(pos))


def short_id(oa_id: str) -> str:
    return (oa_id or "").rsplit("/", 1)[-1]


def get(session: requests.Session, params: dict, max_retry: int = 6) -> dict:
    wait = 2.0
    for attempt in range(max_retry):
        try:
            r = session.get(API, params=params, timeout=60)
        except requests.RequestException as e:
            err = str(e)
        else:
            if r.status_code == 200:
                return r.json()
            err = f"HTTP {r.status_code}: {r.text[:300]}"
            if r.status_code not in (429, 500, 502, 503, 504):
                raise RuntimeError(err)
        print(f"   ! 재시도 {attempt + 1}/{max_retry} ({err}) — {wait:.0f}s 대기", file=sys.stderr)
        time.sleep(wait)
        wait = min(wait * 2, 60)
    raise RuntimeError(f"요청 실패: {params.get('filter')}")


def flatten(w: dict) -> dict:
    pl = w.get("primary_location") or {}
    src = pl.get("source") or {}
    oa = w.get("open_access") or {}
    best = w.get("best_oa_location") or {}
    auths = w.get("authorships") or []
    topic = w.get("primary_topic") or {}
    abstract = rebuild_abstract(w.get("abstract_inverted_index"))
    countries = sorted({c for a in auths for c in (a.get("countries") or [])})
    return {
        "doc_id": short_id(w.get("id")),
        "doi": (w.get("doi") or "").replace("https://doi.org/", ""),
        "title": norm_ws(w.get("display_name")),
        "abstract": abstract,
        "abstract_available": int(bool(abstract)),
        "year": w.get("publication_year"),
        "publication_date": w.get("publication_date"),
        "type": w.get("type"),
        "language": w.get("language"),
        "source_name": src.get("display_name"),
        "source_type_oa": src.get("type"),          # journal / conference / repository …
        "authors": "; ".join(
            (a.get("author") or {}).get("display_name") or "" for a in auths),
        "author_countries": "; ".join(countries),
        "cited_by_count": w.get("cited_by_count"),
        "referenced_works_count": w.get("referenced_works_count"),
        "is_oa": int(bool(oa.get("is_oa"))),
        "oa_status": oa.get("oa_status"),
        "oa_url": oa.get("oa_url"),
        "pdf_url": best.get("pdf_url"),
        "primary_topic": topic.get("display_name"),
        "primary_field": (topic.get("field") or {}).get("display_name"),
        "keywords": "; ".join(k.get("display_name", "") for k in (w.get("keywords") or [])),
        "openalex_url": w.get("id"),
    }


# 부록 D.3 필드에 맞춘 선별·코딩용 빈 칸
SCREEN_COLS = [
    "source_type",        # academic (고정) — 정책·언론 코퍼스와 병합 시 구분
    "screen_ta",          # 제목·초록 선별: include / exclude / maybe
    "exclude_reason",     # 부록 D.1 제외·주의 항목
    "fulltext_status",    # 확보 / 미확보 / 요약만
    "event_id",           # 같은 사건 묶음
    "review_status",      # 검토자·판단유보 사유
    "notes",
]


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser(description="OpenAlex 문헌 수집 (R01~R10)")
    ap.add_argument("--config", default=Path(__file__).with_name("queries.yaml"))
    ap.add_argument("--out", default="out")
    ap.add_argument("--mailto", default=os.getenv("OPENALEX_MAILTO"),
                    help="polite pool 이메일 (권장)")
    ap.add_argument("--api-key", default=os.getenv("OPENALEX_API_KEY"),
                    help="OpenAlex API 키가 있으면 지정")
    ap.add_argument("--only", help="예: R01,R03")
    ap.add_argument("--count-only", action="store_true", help="건수만 조회")
    ap.add_argument("--save-raw", action="store_true", help="원본 JSON 저장")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    st, context, queries = cfg.get("settings", {}), cfg.get("context", ""), cfg["queries"]
    if args.only:
        keep = {x.strip().upper() for x in args.only.split(",")}
        queries = {k: v for k, v in queries.items() if k in keep}

    base = {}
    if args.mailto:
        base["mailto"] = args.mailto
    if args.api_key:
        base["api_key"] = args.api_key

    session = requests.Session()
    session.headers["User-Agent"] = f"ETRI-task2-corpus (mailto:{args.mailto or 'n/a'})"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(args.out) / stamp
    out.mkdir(parents=True, exist_ok=True)
    if args.save_raw:
        (out / "raw").mkdir(exist_ok=True)

    log = {"run_at": datetime.now().isoformat(timespec="seconds"),
           "api": API, "settings": st, "context": norm_ws(context), "queries": {}}
    works: dict[str, dict] = {}
    hits: list[dict] = []

    for rid, q in queries.items():
        flt = build_filter(q["seed"], context, st)
        meta = get(session, {**base, "filter": flt, "per_page": 1, "select": "id"})["meta"]
        total = meta["count"]
        print(f"[{rid}] {q.get('label', '')}  → 총 {total:,}건")
        entry = {"label": q.get("label"), "filter": flt, "total_count": total}
        log["queries"][rid] = entry
        if args.count_only:
            continue

        cap = st.get("max_per_query") or total
        if total > cap:
            print(f"   ! 상한 {cap:,}건까지만 수집 (검색식 좁히기 권장)")
        cursor, n, n_abs = "*", 0, 0
        raw_f = open(out / "raw" / f"{rid}.jsonl", "w", encoding="utf-8") if args.save_raw else None
        while cursor and n < cap:
            data = get(session, {**base, "filter": flt, "per_page": PER_PAGE,
                                 "cursor": cursor, "select": SELECT})
            for w in data["results"]:
                if n >= cap:
                    break
                n += 1
                if raw_f:
                    raw_f.write(json.dumps(w, ensure_ascii=False) + "\n")
                row = flatten(w)
                n_abs += row["abstract_available"]
                did = row["doc_id"]
                if did in works:
                    works[did]["_R"].append(rid)
                else:
                    row["_R"] = [rid]
                    works[did] = row
                hits.append({"R_id": rid, "doc_id": did, "rank": n})
            cursor = data["meta"].get("next_cursor")
            print(f"   … {n:,}/{min(total, cap):,}", end="\r")
            time.sleep(0.15)
        if raw_f:
            raw_f.close()
        entry.update(retrieved=n, with_abstract=n_abs,
                     abstract_rate=round(n_abs / n, 3) if n else None)
        print(f"   수집 {n:,}건 · 초록 확보 {n_abs:,}건 ({entry['abstract_rate']})")

    if not args.count_only:
        cols = list(next(iter(works.values())).keys()) if works else []
        cols = [c for c in cols if c != "_R"] + ["matched_R", "n_matched_R"] + SCREEN_COLS
        with open(out / "works.csv", "w", newline="", encoding="utf-8-sig") as f:
            wr = csv.DictWriter(f, fieldnames=cols)
            wr.writeheader()
            for row in works.values():
                rs = sorted(set(row.pop("_R")))
                row.update({c: "" for c in SCREEN_COLS})
                row.update(matched_R=";".join(rs), n_matched_R=len(rs),
                           source_type="academic")
                wr.writerow(row)
        with open(out / "hits.csv", "w", newline="", encoding="utf-8-sig") as f:
            wr = csv.DictWriter(f, fieldnames=["R_id", "doc_id", "rank"])
            wr.writeheader()
            wr.writerows(hits)
        n_u = len(works)
        n_ua = sum(r["abstract_available"] for r in works.values())
        log["summary"] = {"unique_works": n_u, "unique_with_abstract": n_ua,
                          "hits_total": len(hits)}
        print(f"\n고유 문헌 {n_u:,}건 (초록 있음 {n_ua:,}건) → {out}")

    (out / "run_log.json").write_text(
        json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"로그: {out / 'run_log.json'}")


if __name__ == "__main__":
    main()
