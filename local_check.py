"""v2 검색식을 이미 수집된 v1 코퍼스(all_families.csv)에 적용해 미리 점검한다.
BigQuery를 쓰지 않으며, 결과는 '기존 코퍼스 안에서의 적중'일 뿐 DB 전체 건수의 추정치가 아니다.

사용: python local_check.py --corpus all_families.csv [--samples 8] [--out local_check]
출력: local_check/summary.csv, local_check/samples_<요소>.csv
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
import yaml

from query_lib import flatten_techs, norm_cpc, regex_en, regex_ko

HERE = Path(__file__).resolve().parent


def matcher(cfg, d):
    en = (d.title_en.fillna("") + " " + d.abstract_en.fillna("")).str.lower()
    ko = d.title_ko.fillna("") + " " + d.abstract_ko.fillna("")
    cpcs = d.cpc_codes.fillna("").str.upper().str.split(";")
    ctx = en.str.contains(regex_en(cfg["context"]["en"]), regex=True) | ko.str.contains(regex_ko(cfg["context"]["ko"]), regex=True)

    def run(t):
        m = pd.Series(True, index=d.index)
        for b in t["blocks"]:
            sub = pd.Series(False, index=d.index)
            if b.get("en"):
                sub |= en.str.contains(regex_en(b["en"]), regex=True)
            if b.get("ko"):
                sub |= ko.str.contains(regex_ko(b["ko"]), regex=True)
            m &= sub
        pref = [norm_cpc(c) for c in t["cpc"]]
        hit_cpc = cpcs.apply(lambda cs: any(c.startswith(p) for c in cs for p in pref)) if pref else False
        return m & (ctx | hit_cpc), m & ctx, m & hit_cpc
    return run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=HERE / "patent_queries_v2.yaml")
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--samples", type=int, default=8)
    ap.add_argument("--out", default="local_check")
    a = ap.parse_args()
    cfg = yaml.safe_load(Path(a.config).read_text(encoding="utf-8"))
    d = pd.read_csv(a.corpus, low_memory=False)
    cn = d.countries.fillna("") == "CN"
    run = matcher(cfg, d)
    out = Path(a.out); out.mkdir(exist_ok=True)
    rows = []
    for tid, t in flatten_techs(cfg).items():
        m, mctx, mcpc = run(t)
        y = d.earliest_filing_year
        rows.append(dict(unit=tid, F=t["parent"], role=t["role"], approach=t["approach"], label=t["label"],
                         n=int(m.sum()), n_nonCN=int((m & ~cn).sum()), via_context=int(mctx.sum()), via_cpc=int(mcpc.sum()),
                         y2016_19=int((m & y.between(2016, 2019)).sum()), y2020_23=int((m & y.between(2020, 2023)).sum()),
                         v1_tags=";".join(f"{k}:{v}" for k, v in d.loc[m, "elements"].str.split(";").explode().value_counts().head(3).items())))
        s = d[m & ~cn].sort_values("earliest_filing_year", ascending=False).head(a.samples)
        s[["rep_publication", "earliest_filing_year", "assignees", "title_en", "title_ko"]].to_csv(
            out / f"samples_{tid}.csv", index=False, encoding="utf-8-sig")
    sm = pd.DataFrame(rows)
    sm.to_csv(out / "summary.csv", index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 220); pd.set_option("display.max_colwidth", 40)
    print(sm.drop(columns=["label"]).to_string(index=False))


if __name__ == "__main__":
    main()
