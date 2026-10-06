"""BigQuery 없이 bq_patents_v2.py의 extract 후처리(패밀리 통합·요약·연도표)를 점검하는 모의 실행.
v1 코퍼스에서 local_check와 같은 매칭으로 '가짜 공보'를 만들어 BQ.run 대신 돌려준다.
사용: python test_pipeline.py --corpus all_families.csv"""
import argparse, sys, types
from pathlib import Path

import pandas as pd
import yaml

# google.cloud.bigquery 대체 모듈
bq = types.ModuleType("bigquery")
for n in ["Client", "QueryJobConfig", "Dataset"]:
    setattr(bq, n, lambda *a, **k: types.SimpleNamespace(**k))
bq.ScalarQueryParameter = lambda name, typ, val: types.SimpleNamespace(name=name, value=val)
bq.ArrayQueryParameter = lambda name, typ, val: types.SimpleNamespace(name=name, values=val)
g = types.ModuleType("google"); gc = types.ModuleType("google.cloud"); gc.bigquery = bq
sys.modules.update({"google": g, "google.cloud": gc, "google.cloud.bigquery": bq})

import bq_patents_v2 as B  # noqa: E402
from local_check import matcher  # noqa: E402
from query_lib import flatten_techs  # noqa: E402

ap = argparse.ArgumentParser(); ap.add_argument("--corpus", required=True); ap.add_argument("--only", default="F01,F06")
a = ap.parse_args()
cfg = yaml.safe_load(Path(B.HERE / "patent_queries_v2.yaml").read_text(encoding="utf-8"))
cfg["settings"]["project"] = "test-project"
d = pd.read_csv(a.corpus, low_memory=False)
run = matcher(cfg, d)
techs = flatten_techs(cfg)


def fake_pubs(tid):
    m, mctx, mcpc = run(techs[tid])
    s = d[m].copy()
    # 패밀리마다 공보 2건(관할 다름)을 만들어 서지 통합을 시험
    rows = []
    for _, r in s.iterrows():
        for k, cc in enumerate((r.countries or "CN").split(";")[:2] or ["CN"]):
            rows.append(dict(publication_number=f"{r.rep_publication}-{k}", application_number="", country_code=cc,
                             kind_code="A", family_id=str(r.family_key), filing_date=int(r.earliest_filing_year) * 10000 + 101 + k * 10000,
                             priority_date=int(r.earliest_priority) if pd.notna(r.earliest_priority) else 0,
                             publication_date=0, grant_date=0, title_en=r.title_en, title_ko=r.title_ko,
                             abstract_en=r.abstract_en, abstract_ko=r.abstract_ko, claims_en=None,
                             cpc_codes=[c for c in str(r.cpc_codes).split(";") if c and c != "nan"], ipc_codes=[],
                             assignees=[x for x in str(r.assignees).split(";") if x and x != "nan"], assignee_countries=[],
                             hit_context=bool(mctx[_]), hit_cpc=bool(mcpc[_])))
    return pd.DataFrame(rows)


class FakeRes:
    def __init__(self, df): self.df = df
    def to_dataframe(self): return self.df


B.BQ.__init__ = lambda self, st, dry=False: (setattr(self, "dry", dry), setattr(self, "log", []), setattr(self, "client", None))[0]
B.BQ.run = lambda self, sql, params, label: FakeRes(fake_pubs(label))
B.STATE.write_text('{"all_cpc": []}', encoding="utf-8")
tmp = Path("_test_cfg.yaml"); tmp.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
sys.argv = ["bq_patents_v2.py", "extract", "--config", str(tmp), "--only", a.only, "--out", "_test_out"]
B.main()
tmp.unlink(); B.STATE.unlink()
out = sorted(Path("_test_out").iterdir())[-1]
f = pd.read_csv(out / "all_families.csv")
print(f[["family_key", "rep_publication", "countries", "earliest_filing_year", "n_publications", "elements", "core_elements"]].head(8).to_string())
print(pd.read_csv(out / "summary.csv").to_string())
