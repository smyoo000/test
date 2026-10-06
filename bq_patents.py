#!/usr/bin/env python3
"""
분석과업 3 — Google Patents Public Data(BigQuery) 키워드·CPC 기반 특허 수집

설계
  공개 테이블(patents-public-data.patents.publications)은 매우 커서 조회할 때마다
  수백 GB가 과금됩니다. 그래서
    stage1  : 기간·국가 + (재난 맥락어 OR 전체 CPC 후보)로 걸러 "후보 테이블"을
              내 데이터셋에 한 번만 저장 (큰 스캔 1회)
    count   : 후보 테이블에서 미래도전기술별 건수만 확인 (저렴)
    extract : 후보 테이블에서 기술별 특허 추출 → CSV·패밀리 단위 정리 (저렴)
  순서로 진행합니다. 검색식을 고쳐도 count/extract만 다시 돌리면 됩니다.
  (단, 새 CPC를 추가했다면 stage1을 다시 실행)

사용 예)
  python bq_patents.py stage1 --dry-run     # 예상 스캔량(GB)만 확인
  python bq_patents.py stage1               # 후보 테이블 생성
  python bq_patents.py count                # 기술별 건수
  python bq_patents.py extract              # 추출 + CSV 저장
  python bq_patents.py extract --only F01,F03   # F 전체 또는 F01_E2처럼 요소기술 지정

준비
  pip install google-cloud-bigquery pandas pyyaml db-dtypes
  gcloud auth application-default login     # 또는 서비스계정 키(GOOGLE_APPLICATION_CREDENTIALS)
  queries 파일의 settings.project 를 본인 GCP 프로젝트로 수정

출력 (out_patents/<실행시각>/)
  F01_E1_publications.csv  요소기술별 공보 단위(국가별 문헌) — 제목·초록·CPC·출원인·일자
  F01_E1_families.csv      요소기술별 패밀리 단위(중복 제거) — 대표 공보, 국가 목록, 최초 우선일
  F01_ALL_families.csv     미래도전기술(F) 단위 합집합
  all_families.csv         전체 통합, 패밀리별 해당 F·요소기술 목록
  summary.csv              F·요소기술별 패밀리 수, KR 수, 맥락어/CPC 매칭 수
  yearly_families_by_F.csv 최초 출원연도별 패밀리 수(출원동향용)
  run_log.json           실행 일시, SQL 원문, 파라미터, 스캔량, 건수 (재현성 기록)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from google.cloud import bigquery

SRC = "patents-public-data.patents.publications"
HERE = Path(__file__).resolve().parent
STATE = HERE / ".stage1_state.json"   # 후보 테이블을 만들 때 사용한 CPC·설정 기록


# ------------------------------------------------------------------ 검색식 → 정규식
def norm_cpc(c: str) -> str:
    return re.sub(r"\s+", "", str(c)).upper()


def flatten_techs(cfg: dict) -> dict:
    """techs(F) → 검색 단위(요소기술) 사전. elements가 없으면 F 자체를 한 단위로 취급"""
    out = {}
    for fid, f in cfg["techs"].items():
        base_cpc = list(f.get("cpc", []))
        els = f.get("elements")
        if not els:
            out[fid] = {"parent": fid, "f_label": f.get("label"), "label": f.get("label"),
                        "linked_R": f.get("linked_R"), "blocks": f.get("blocks", []), "cpc": base_cpc}
            continue
        for eid, e in els.items():
            qid = f"{fid}_{eid}"
            out[qid] = {"parent": fid, "f_label": f.get("label"), "label": e.get("label"),
                        "linked_R": f.get("linked_R"), "blocks": e.get("blocks", []),
                        "cpc": base_cpc + list(e.get("cpc", []))}
    return out


def regex_en(terms: list[str]) -> str:
    """영문 용어 목록 → RE2 정규식 (소문자 비교, 단어 경계, 공백·하이픈 유연)"""
    if not terms:
        return ""
    parts = []
    for t in terms:
        toks = [re.escape(x) for x in re.split(r"[\s\-]+", t.lower().strip()) if x]
        parts.append(r"[\s\-]*".join(toks))   # "heat wave" = heatwave / heat-wave / heat wave
    return r"\b(?:" + "|".join(parts) + r")"


def regex_ko(terms: list[str]) -> str:
    """국문 용어 목록 → 정규식 (부분 일치, 공백 유무 허용)"""
    if not terms:
        return ""
    parts = [r"\s*".join(re.escape(x) for x in t.split()) for t in terms if t.strip()]
    return "(?:" + "|".join(parts) + ")"


# ------------------------------------------------------------------ BigQuery 헬퍼
class BQ:
    def __init__(self, st: dict, dry: bool = False):
        self.st = st
        self.client = bigquery.Client(project=st["project"], location=st.get("location", "US"))
        self.dry = dry
        self.max_bytes = int(float(st.get("max_gb_billed", 600)) * 1e9)
        self.log: list[dict] = []

    def run(self, sql: str, params: list, label: str):
        cfg = bigquery.QueryJobConfig(query_parameters=params, dry_run=True, use_query_cache=False)
        est = self.client.query(sql, job_config=cfg).total_bytes_processed
        gb = est / 1e9
        print(f"[{label}] 예상 스캔량 {gb:,.1f} GB")
        entry = {"label": label, "sql": sql, "est_gb": round(gb, 2),
                 "params": [{"name": p.name, "value": getattr(p, "value", None) or getattr(p, "values", None)}
                            for p in params]}
        self.log.append(entry)
        if self.dry:
            return None
        if est > self.max_bytes:
            sys.exit(f"  ! 상한({self.max_bytes/1e9:.0f} GB) 초과 — settings.max_gb_billed를 조정하거나 범위를 줄이세요.")
        cfg = bigquery.QueryJobConfig(query_parameters=params, maximum_bytes_billed=self.max_bytes)
        job = self.client.query(sql, job_config=cfg)
        res = job.result()
        entry["billed_gb"] = round((job.total_bytes_billed or 0) / 1e9, 2)
        print(f"  완료 (과금 {entry['billed_gb']:,.1f} GB)")
        return res


# ------------------------------------------------------------------ SQL 생성
def cand_table(st):
    return f"`{st['project']}.{st['dataset']}.pat_candidates`"


def stage1_sql(st, cfg) -> tuple[str, list]:
    all_cpc = sorted({norm_cpc(c) for t in flatten_techs(cfg).values() for c in t.get("cpc", [])})
    claims_col = (", (SELECT text FROM UNNEST(claims_localized) WHERE language = 'en' LIMIT 1) AS claims_en"
                  if st.get("search_claims") else ", CAST(NULL AS STRING) AS claims_en")
    claims_ctx = "OR REGEXP_CONTAINS(LOWER(IFNULL(claims_en, '')), @ctx_en)" if st.get("search_claims") else ""
    sql = f"""
CREATE OR REPLACE TABLE {cand_table(st)} AS
WITH base AS (
  SELECT
    publication_number, application_number, country_code, kind_code, family_id,
    filing_date, priority_date, publication_date, grant_date,
    (SELECT text FROM UNNEST(title_localized)    WHERE language = 'en' LIMIT 1) AS title_en,
    (SELECT text FROM UNNEST(title_localized)    WHERE language = 'ko' LIMIT 1) AS title_ko,
    (SELECT text FROM UNNEST(abstract_localized) WHERE language = 'en' LIMIT 1) AS abstract_en,
    (SELECT text FROM UNNEST(abstract_localized) WHERE language = 'ko' LIMIT 1) AS abstract_ko
    {claims_col},
    ARRAY(SELECT DISTINCT c.code FROM UNNEST(cpc) c) AS cpc_codes,
    ARRAY(SELECT DISTINCT i.code FROM UNNEST(ipc) i) AS ipc_codes,
    ARRAY(SELECT a.name FROM UNNEST(assignee_harmonized) a) AS assignees,
    ARRAY(SELECT DISTINCT a.country_code FROM UNNEST(assignee_harmonized) a) AS assignee_countries
  FROM `{SRC}`
  WHERE country_code IN UNNEST(@countries)
    AND filing_date BETWEEN @f_from AND @f_to
)
SELECT * FROM base
WHERE REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(title_en, ''), ' ', IFNULL(abstract_en, ''))), @ctx_en)
   OR REGEXP_CONTAINS(CONCAT(IFNULL(title_ko, ''), ' ', IFNULL(abstract_ko, '')), @ctx_ko)
   {claims_ctx}
   OR EXISTS (SELECT 1 FROM UNNEST(ARRAY_CONCAT(cpc_codes, ipc_codes)) c, UNNEST(@all_cpc) p
              WHERE STARTS_WITH(c, p))
"""
    P = bigquery.ScalarQueryParameter
    params = [
        bigquery.ArrayQueryParameter("countries", "STRING", st["countries"]),
        P("f_from", "INT64", int(st["filing_from"])), P("f_to", "INT64", int(st["filing_to"])),
        P("ctx_en", "STRING", regex_en(cfg["context"].get("en", [])) or "a^"),
        P("ctx_ko", "STRING", regex_ko(cfg["context"].get("ko", [])) or "a^"),
        bigquery.ArrayQueryParameter("all_cpc", "STRING", all_cpc or ["__NONE__"]),
    ]
    return sql, params, all_cpc


def tech_where(tid, tech, cfg, st) -> tuple[str, list]:
    """기술 1개에 대한 WHERE 절과 파라미터"""
    P = bigquery.ScalarQueryParameter
    txt_en = "LOWER(CONCAT(IFNULL(title_en,''),' ',IFNULL(abstract_en,'')" + \
             (",' ',IFNULL(claims_en,'')" if st.get("search_claims") else "") + "))"
    txt_ko = "CONCAT(IFNULL(title_ko,''),' ',IFNULL(abstract_ko,''))"
    conds, params = [], []
    for k, b in enumerate(tech["blocks"]):
        ren, rko = regex_en(b.get("en", [])), regex_ko(b.get("ko", []))
        sub = []
        if ren:
            params.append(P(f"{tid}_b{k}_en", "STRING", ren)); sub.append(f"REGEXP_CONTAINS({txt_en}, @{tid}_b{k}_en)")
        if rko:
            params.append(P(f"{tid}_b{k}_ko", "STRING", rko)); sub.append(f"REGEXP_CONTAINS({txt_ko}, @{tid}_b{k}_ko)")
        if sub:
            conds.append("(" + " OR ".join(sub) + ")")
    params += [P("ctx_en", "STRING", regex_en(cfg["context"].get("en", [])) or "a^"),
               P("ctx_ko", "STRING", regex_ko(cfg["context"].get("ko", [])) or "a^"),
               bigquery.ArrayQueryParameter(f"{tid}_cpc", "STRING",
                                            [norm_cpc(c) for c in tech.get("cpc", [])] or ["__NONE__"])]
    ctx = f"(REGEXP_CONTAINS({txt_en}, @ctx_en) OR REGEXP_CONTAINS({txt_ko}, @ctx_ko))"
    cpc = (f"EXISTS (SELECT 1 FROM UNNEST(ARRAY_CONCAT(cpc_codes, ipc_codes)) c, UNNEST(@{tid}_cpc) p "
           f"WHERE STARTS_WITH(c, p))")
    where = " AND ".join(conds or ["TRUE"]) + f"\n  AND ({ctx} OR {cpc})"
    flags = (f"{ctx} AS hit_context, {cpc} AS hit_cpc")
    return where, flags, params


# ------------------------------------------------------------------ 패밀리 정리
def to_families(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["family_key"] = df["family_id"].where(df["family_id"].astype(str) != "-1", df["publication_number"])
    df["prio"] = df["priority_date"].where(df["priority_date"] > 0, df["filing_date"])
    # 대표 공보: 영문 초록이 있는 것 우선 → 최초 출원
    df["_has_en"] = df["abstract_en"].fillna("").str.len() > 0
    df = df.sort_values(["family_key", "_has_en", "filing_date"], ascending=[True, False, True])
    g = df.groupby("family_key", sort=False)
    fam = g.first()[["publication_number", "title_en", "title_ko", "abstract_en", "abstract_ko",
                     "cpc_codes", "assignees"]].rename(columns={"publication_number": "rep_publication"})
    fam["earliest_priority"] = g["prio"].min()
    fam["earliest_filing_year"] = (g["filing_date"].min() // 10000).astype(int)
    fam["countries"] = g["country_code"].apply(lambda s: ";".join(sorted(set(s))))
    fam["n_publications"] = g.size()
    fam["has_grant"] = g["grant_date"].apply(lambda s: bool((s.fillna(0) > 0).any()))
    for c in ["hit_context", "hit_cpc"]:
        if c in df:
            fam[c] = g[c].any()
    return fam.reset_index()


def listify(df):
    for c in ["cpc_codes", "ipc_codes", "assignees", "assignee_countries"]:
        if c in df:
            df[c] = df[c].apply(lambda v: ";".join(v) if isinstance(v, (list, tuple)) or hasattr(v, "tolist") else v)
    return df


# ------------------------------------------------------------------ main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["stage1", "count", "extract"])
    ap.add_argument("--config", default=HERE / "patent_queries.yaml")
    ap.add_argument("--only", help="예: F01,F03 또는 F01_E2")
    ap.add_argument("--dry-run", action="store_true", help="스캔량만 확인")
    ap.add_argument("--out", default="out_patents")
    a = ap.parse_args()

    cfg = yaml.safe_load(Path(a.config).read_text(encoding="utf-8"))
    st = cfg["settings"]
    if st["project"].startswith("YOUR_"):
        sys.exit("settings.project 를 본인 GCP 프로젝트 ID로 바꿔주세요.")
    techs = flatten_techs(cfg)
    if a.only:
        keep = [x.strip().upper() for x in a.only.split(",")]
        techs = {k: v for k, v in techs.items() if any(k == x or k.startswith(x + "_") for x in keep)}
    print(f"검색 단위 {len(techs)}개: " + ", ".join(techs))
    bq = BQ(st, dry=a.dry_run)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = Path(a.out) / stamp
    log = {"run_at": datetime.now().isoformat(timespec="seconds"), "cmd": a.cmd,
           "settings": st, "source": SRC, "candidate_table": cand_table(st)}

    if a.cmd == "stage1":
        if not a.dry_run:
            ds = bigquery.Dataset(f"{st['project']}.{st['dataset']}")
            ds.location = st.get("location", "US")
            bq.client.create_dataset(ds, exists_ok=True)
        sql, params, all_cpc = stage1_sql(st, cfg)
        bq.run(sql, params, "stage1")
        if not a.dry_run:
            n = list(bq.client.query(f"SELECT COUNT(*) n FROM {cand_table(st)}").result())[0].n
            print(f"  후보 테이블 {n:,}건")
            log["candidate_rows"] = n
            STATE.write_text(json.dumps({"all_cpc": all_cpc, "settings": st, "built_at": log["run_at"]},
                                        ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        if STATE.exists():
            built = set(json.loads(STATE.read_text(encoding="utf-8"))["all_cpc"])
            need = {norm_cpc(c) for t in techs.values() for c in t.get("cpc", [])}
            miss = need - built
            if miss:
                print(f"  ! 후보 테이블에 없는 CPC가 있습니다 {sorted(miss)} → stage1을 다시 실행하세요.")
        else:
            print("  ! stage1 기록이 없습니다. 먼저 stage1을 실행했는지 확인하세요.")
        log["techs"] = {}
        all_rows = []
        if a.cmd == "count":
            groups = {}
            for tid, t in techs.items():
                groups.setdefault(t["parent"], []).append(tid)
            for fid, members in groups.items():
                if len(members) < 2:
                    continue
                ws, ps = [], []
                for tid in members:
                    w, _, p = tech_where(tid, techs[tid], cfg, st)
                    ws.append(f"({w})")
                    ps += [q for q in p if q.name not in {x.name for x in ps}]
                sql = (f"SELECT COUNT(*) AS n_pub, COUNT(DISTINCT family_id) AS n_fam,\n"
                       f"  COUNTIF(country_code='KR') AS n_kr\nFROM {cand_table(st)}\nWHERE " + "\n   OR ".join(ws))
                res = bq.run(sql, ps, f"{fid}(합집합)")
                if res is not None:
                    r = list(res)[0]
                    print(f"  {fid} 합집합 {techs[members[0]]['f_label']}\n     공보 {r.n_pub:,} / 패밀리 {r.n_fam:,} / KR {r.n_kr:,}")
                    log["techs"][f"{fid}_ALL"] = {"label": techs[members[0]]["f_label"], "n_pub": r.n_pub,
                                                  "n_fam": r.n_fam, "n_kr": r.n_kr}
        for tid, t in techs.items():
            where, flags, params = tech_where(tid, t, cfg, st)
            if a.cmd == "count":
                sql = (f"SELECT COUNT(*) AS n_pub, COUNT(DISTINCT family_id) AS n_fam,\n"
                       f"  COUNTIF(country_code='KR') AS n_kr\nFROM {cand_table(st)}\nWHERE {where}")
                res = bq.run(sql, params, tid)
                if res is not None:
                    r = list(res)[0]
                    print(f"  {tid} {t.get('label','')}\n     공보 {r.n_pub:,} / 패밀리 {r.n_fam:,} / KR {r.n_kr:,}")
                    log["techs"][tid] = {"parent": t["parent"], "label": t.get("label"), "n_pub": r.n_pub,
                                         "n_fam": r.n_fam, "n_kr": r.n_kr}
            else:
                sql = f"SELECT *, {flags}\nFROM {cand_table(st)}\nWHERE {where}"
                res = bq.run(sql, params, tid)
                if res is None:
                    continue
                df = res.to_dataframe()
                out.mkdir(parents=True, exist_ok=True)
                fam = to_families(df)
                listify(df).to_csv(out / f"{tid}_publications.csv", index=False, encoding="utf-8-sig")
                listify(fam).to_csv(out / f"{tid}_families.csv", index=False, encoding="utf-8-sig")
                print(f"  {tid}: 공보 {len(df):,} / 패밀리 {len(fam):,} "
                      f"(맥락어 매칭 {int(fam.get('hit_context', pd.Series()).sum()):,}, "
                      f"CPC 매칭 {int(fam.get('hit_cpc', pd.Series()).sum()):,})")
                log["techs"][tid] = {"parent": t["parent"], "label": t.get("label"), "linked_R": t.get("linked_R"),
                                     "n_pub": len(df), "n_fam": len(fam),
                                     "n_fam_kr": int(fam["countries"].str.contains("KR").sum()) if len(fam) else 0,
                                     "fam_hit_context": int(fam.get("hit_context", pd.Series(dtype=bool)).sum()),
                                     "fam_hit_cpc": int(fam.get("hit_cpc", pd.Series(dtype=bool)).sum())}
                fam["F"] = t["parent"]
                fam["element"] = tid
                all_rows.append(fam)
        if all_rows:
            allf = pd.concat(all_rows, ignore_index=True)
            agg = allf.groupby("family_key").agg(
                F=("F", lambda s: ";".join(sorted(set(s)))),
                elements=("element", lambda s: ";".join(sorted(set(s)))),
                hit_context=("hit_context", "max"), hit_cpc=("hit_cpc", "max"))
            first = allf.drop(columns=["F", "element", "hit_context", "hit_cpc"]).drop_duplicates("family_key")
            allfam = first.set_index("family_key").join(agg).reset_index()
            allfam.to_csv(out / "all_families.csv", index=False, encoding="utf-8-sig")
            # F 단위 합집합(요소기술 결과의 패밀리 합집합)
            summ = []
            for fid in sorted(allf["F"].unique()):
                sub = allfam[allfam["F"].str.split(";").apply(lambda xs: fid in xs)]
                sub.to_csv(out / f"{fid}_ALL_families.csv", index=False, encoding="utf-8-sig")
                summ.append({"unit": f"{fid}_ALL", "F": fid, "label": techs[next(k for k in techs if techs[k]["parent"] == fid)]["f_label"],
                             "n_fam": len(sub), "n_fam_kr": int(sub["countries"].str.contains("KR").sum())})
            for tid, v in log["techs"].items():
                summ.append({"unit": tid, "F": v["parent"], "label": v["label"], "n_pub": v["n_pub"],
                             "n_fam": v["n_fam"], "n_fam_kr": v["n_fam_kr"],
                             "fam_hit_context": v["fam_hit_context"], "fam_hit_cpc": v["fam_hit_cpc"]})
            sm = pd.DataFrame(summ)
            for c in ["n_pub", "n_fam", "n_fam_kr", "fam_hit_context", "fam_hit_cpc"]:
                if c in sm:
                    sm[c] = sm[c].astype("Int64")
            sm.sort_values(["F", "unit"]).to_csv(out / "summary.csv", index=False, encoding="utf-8-sig")
            # 연도별 패밀리 수(최초 출원연도 기준) — 출원동향 분석용
            yr = allf.drop_duplicates(["family_key", "F"]).pivot_table(
                index="earliest_filing_year", columns="F", values="family_key", aggfunc="count", fill_value=0)
            yr.to_csv(out / "yearly_families_by_F.csv", encoding="utf-8-sig")
            print(f"\n통합 패밀리 {len(allfam):,}건 → summary.csv, yearly_families_by_F.csv, F별 *_ALL_families.csv")

    log["queries"] = bq.log
    out.mkdir(parents=True, exist_ok=True)
    (out / "run_log.json").write_text(json.dumps(log, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"로그: {out / 'run_log.json'}")


if __name__ == "__main__":
    main()
