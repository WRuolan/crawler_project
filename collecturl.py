# -*- coding: utf-8 -*-
r"""
collect_shaanxi_yearly_v2.py
============================
陕西省政府网 /irs/front/search 按年抓取 URL（2011-2025）

核心改进：
- 不再用“linkUrl 是否存在”来选结果列表（避免固定 5 条推荐列表）
- 自动选取“最像搜索结果”的 list[dict]（长度最大 + 含 title/documentId 等特征）
- 对每条 item 递归扫描字符串，提取站内详情链接（linkUrl/url/sourceUrl/任意嵌套字段）
- 输出：
  <out_root>\<year>\urls\urls_all.txt
  <out_root>\<year>\metadata\irs_results.csv
  <out_root>\summary_years.csv

运行（先测 2025）：
python collect_shaanxi_yearly_v2.py --out-root "D:\Jupyter\agriculture\corpus\shaanxi_gov" --keyword "区域农业产业协同" --start-year 2025 --end-year 2025 --order-by related --page-size 30 --force
"""

import argparse
import csv
import datetime as dt
import json
import math
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.exceptions import ReadTimeout, ConnectionError as ReqConnectionError, ChunkedEncodingError

API_URL = "https://www.shaanxi.gov.cn/irs/front/search"
BASE = "https://www.shaanxi.gov.cn"

TENANT_ID = "16711"
DATA_TYPE_ID = 17018
SITE_ID = 11
REFERER = "https://www.shaanxi.gov.cn/sxsearch/search.html?tenantId=16711"


def tz8() -> dt.timezone:
    return dt.timezone(dt.timedelta(hours=8))


def year_range_ms(y: int) -> Tuple[int, int]:
    b = dt.datetime(y, 1, 1, 0, 0, 0, tzinfo=tz8())
    nb = dt.datetime(y + 1, 1, 1, 0, 0, 0, tzinfo=tz8())
    return int(b.timestamp() * 1000), int(nb.timestamp() * 1000) - 1


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def build_payload(keyword: str, page_no: int, page_size: int, begin_ms: int, end_ms: int, order_by: str) -> Dict[str, Any]:
    # 保持你最早 cURL 的结构（不要随意删字段）
    return {
        "tenantId": TENANT_ID,
        "configTenantId": TENANT_ID,
        "searchWord": keyword,
        "dataTypeId": DATA_TYPE_ID,
        "historySearchWords": [keyword],
        "orderBy": order_by,
        "searchBy": "all",
        "granularity": "ALL",
        "pageNo": int(page_no),
        "pageSize": int(page_size),
        "beginDateTime": int(begin_ms),
        "endDateTime": int(end_ms),
        "filters": [],
        "customFilter": {
            "operator": "and",
            "properties": [{"property": "site_id", "operator": "eq", "value": SITE_ID}],
            "filters": [],
        },
    }


def post_with_retry(sess: requests.Session, payload: Dict[str, Any], timeout: int, retries: int, backoff: float) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": BASE,
        "Referer": REFERER,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    }
    last = None
    for i in range(retries + 1):
        try:
            r = sess.post(API_URL, headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (ReadTimeout, ReqConnectionError, ChunkedEncodingError, requests.HTTPError) as e:
            last = e
            if i == retries:
                raise
            time.sleep(backoff * (2 ** i) + random.random() * 0.3)
    raise last


def find_total(obj: Any) -> Optional[int]:
    if not isinstance(obj, dict):
        return None
    # 常见 total 位置
    for path in (("data", "total"), ("pager", "total"), ("data", "pager", "total"), ("total",)):
        cur = obj
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok:
            if isinstance(cur, int):
                return cur
            if isinstance(cur, str) and cur.isdigit():
                return int(cur)
    return None


def iter_lists_of_dicts(x: Any):
    """遍历 JSON 中所有 list[dict] 候选。"""
    if isinstance(x, dict):
        for v in x.values():
            yield from iter_lists_of_dicts(v)
    elif isinstance(x, list):
        if x and all(isinstance(it, dict) for it in x):
            yield x
        for v in x:
            yield from iter_lists_of_dicts(v)


def score_list(lst: List[Dict[str, Any]]) -> int:
    """给候选列表打分：优先长度大、含 title/documentId 的更像结果列表。"""
    if not lst:
        return -1
    score = len(lst) * 10
    keys_union = set()
    for it in lst[:10]:
        keys_union |= set(it.keys())
    if "title" in keys_union or "name" in keys_union:
        score += 200
    if "documentId" in keys_union or "docId" in keys_union or "id" in keys_union:
        score += 200
    # 若任何字符串值里出现 shaanxi.gov.cn，加分
    for it in lst[:10]:
        for v in it.values():
            if isinstance(v, str) and "shaanxi.gov.cn" in v:
                score += 200
                break
    return score


def pick_best_items(page_json: Any) -> List[Dict[str, Any]]:
    """在 response 里挑出最可能的分页结果列表。"""
    best = []
    best_score = -1
    for lst in iter_lists_of_dicts(page_json):
        sc = score_list(lst)
        if sc > best_score:
            best_score = sc
            best = lst
    return best


def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return urljoin(BASE, u)
    return u


def is_detail_like(u: str) -> bool:
    """粗过滤：要么带 .html/.shtml/.htm，要么是常见内容路径（zfxxgk/xw/...）"""
    pu = urlparse(u)
    if "shaanxi.gov.cn" not in (pu.netloc or ""):
        return False
    path = (pu.path or "").lower()
    if "sxsearch/search.html" in u.lower():
        return False
    if "/irs/front/search" in u.lower():
        return False
    if any(path.endswith(ext) for ext in [".html", ".shtml", ".htm"]):
        return True
    # 无后缀但像内容页
    if any(seg in path for seg in ["/zfxxgk/", "/xw/", "/zwgk/", "/fw/", "/hd/", "/sj/"]):
        return True
    return False


def walk_strings(x: Any):
    if isinstance(x, dict):
        for v in x.values():
            yield from walk_strings(v)
    elif isinstance(x, list):
        for v in x:
            yield from walk_strings(v)
    elif isinstance(x, str):
        s = x.strip()
        if s:
            yield s


def extract_urls_from_item(it: Dict[str, Any]) -> List[str]:
    """对一个 item 递归提取所有站内详情链接（不再依赖 linkUrl 单字段）。"""
    urls = []
    for s in walk_strings(it):
        if s.startswith(("http://", "https://", "/", "//")):
            u = normalize_url(s)
            if u and is_detail_like(u):
                urls.append(u)
    # 去重
    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def collect_one_year(sess: requests.Session, out_root: Path, year: int, keyword: str, order_by: str,
                     page_size: int, timeout: int, retries: int, backoff: float, sleep_s: float, force: bool):
    ydir = out_root / str(year)
    urls_dir = ydir / "urls"
    meta_dir = ydir / "metadata"
    ensure_dir(urls_dir)
    ensure_dir(meta_dir)

    urls_path = urls_dir / "urls_all.txt"
    meta_csv = meta_dir / "irs_results.csv"

    if (not force) and urls_path.exists() and urls_path.stat().st_size > 100:
        return {"year": year, "skipped": 1}

    begin_ms, end_ms = year_range_ms(year)

    seen_urls = set()
    rows = 0
    total = None
    pages = None

    with meta_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["year", "pageNo", "title", "documentId", "url"])
        w.writeheader()

        # 先取第一页确定 total/pages
        payload1 = build_payload(keyword, 1, page_size, begin_ms, end_ms, order_by)
        j1 = post_with_retry(sess, payload1, timeout, retries, backoff)

        total = find_total(j1) or 0
        pages = max(1, math.ceil(total / page_size)) if total else 1

        def consume(j: Any, pno: int):
            nonlocal rows
            items = pick_best_items(j)
            if not items:
                return 0
            new_cnt = 0
            for it in items:
                urls = extract_urls_from_item(it)
                if not urls:
                    continue
                title = it.get("title") or it.get("name") or ""
                docid = it.get("documentId") or it.get("docId") or it.get("id") or ""
                for u in urls:
                    if u not in seen_urls:
                        seen_urls.add(u)
                        new_cnt += 1
                    w.writerow({"year": year, "pageNo": pno, "title": title, "documentId": docid, "url": u})
                    rows += 1
            return new_cnt

        new1 = consume(j1, 1)

        # 若第一页就只有极少 url（比如 5），我们继续翻页，但设置“连续无新增页”退出，避免死循环
        stagnant = 0
        for pno in range(2, pages + 1):
            payload = build_payload(keyword, pno, page_size, begin_ms, end_ms, order_by)
            jp = post_with_retry(sess, payload, timeout, retries, backoff)
            newp = consume(jp, pno)

            if pno % 10 == 0:
                print(f"  [Y{year}] page {pno}/{pages}, urls={len(seen_urls)}, new={newp}, total={total}")

            if newp == 0:
                stagnant += 1
            else:
                stagnant = 0

            # 连续 5 页无新增，认为后续都是重复/无效分页，停止
            if stagnant >= 5:
                break

            time.sleep(sleep_s + random.random() * 0.2)

    urls_path.write_text("\n".join(sorted(seen_urls)), encoding="utf-8")
    return {"year": year, "skipped": 0, "total": total, "urls": len(seen_urls), "rows": rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--keyword", required=True)
    ap.add_argument("--start-year", type=int, default=2011)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--order-by", default="related")
    ap.add_argument("--page-size", type=int, default=30)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--retries", type=int, default=8)
    ap.add_argument("--backoff", type=float, default=0.8)
    ap.add_argument("--sleep", type=float, default=0.8)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out_root = Path(args.out_root)
    ensure_dir(out_root)
    sess = requests.Session()

    summary = out_root / "summary_years.csv"
    with summary.open("w", encoding="utf-8", newline="") as sf:
        sw = csv.DictWriter(sf, fieldnames=["year", "total_from_api", "urls", "rows", "skipped"])
        sw.writeheader()

        for y in range(args.start_year, args.end_year + 1):
            print(f"[YEAR] {y} ...")
            res = collect_one_year(
                sess, out_root, y, args.keyword, args.order_by,
                args.page_size, args.timeout, args.retries, args.backoff, args.sleep, args.force
            )
            sw.writerow({
                "year": res.get("year"),
                "total_from_api": res.get("total", ""),
                "urls": res.get("urls", ""),
                "rows": res.get("rows", ""),
                "skipped": res.get("skipped", 0),
            })
            print(f"[YEAR] {y} done: total={res.get('total')} urls={res.get('urls')} rows={res.get('rows')}")

    print(f"[DONE] summary: {summary}")
    print(f"[DONE] out_root: {out_root}")


if __name__ == "__main__":
    main()