"""검색식(YAML) → 정규식 변환과 요소 평탄화. bq_patents_v2.py와 local_check.py가 함께 사용한다.
BigQuery의 RE2와 파이썬 re에서 같은 의미로 동작하는 구문만 사용한다."""
from __future__ import annotations

import re


def norm_cpc(c: str) -> str:
    return re.sub(r"\s+", "", str(c)).upper()


def flatten_techs(cfg: dict) -> dict:
    """techs(F) → 검색 단위(요소) 사전. role(base/core)·approach(A/B/C/X)를 함께 보존한다."""
    out = {}
    for fid, f in cfg["techs"].items():
        base_cpc = list(f.get("cpc", []))
        for eid, e in (f.get("elements") or {}).items():
            out[f"{fid}_{eid}"] = {
                "parent": fid, "f_label": f.get("label"), "label": e.get("label"),
                "role": e.get("role", "base"), "approach": e.get("approach", "X"),
                "linked_R": f.get("linked_R"), "blocks": e.get("blocks", []),
                "cpc": base_cpc + list(e.get("cpc", [])),
            }
    return out


def regex_en(terms: list[str]) -> str:
    """영문 용어 → 정규식(소문자 비교, 단어 시작 경계, 공백·하이픈 변형 허용)"""
    if not terms:
        return ""
    parts = []
    for t in terms:
        toks = [re.escape(x) for x in re.split(r"[\s\-]+", str(t).lower().strip()) if x]
        parts.append(r"[\s\-]*".join(toks))
    return r"\b(?:" + "|".join(parts) + r")"


def regex_ko(terms: list[str]) -> str:
    """국문 용어 → 정규식(부분 일치, 공백 유무 허용)"""
    if not terms:
        return ""
    parts = [r"\s*".join(re.escape(x) for x in str(t).split()) for t in terms if str(t).strip()]
    return "(?:" + "|".join(parts) + ")"
