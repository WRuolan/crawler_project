# collect_yearly_generic.py
import argparse, csv, json, math, random, time
import datetime as dt
from pathlib import Path
import requests

def tz8():
    return dt.timezone(dt.timedelta(hours=8))

def year_range_ms(y: int):
    b = dt.datetime(y, 1, 1, 0, 0, 0, tzinfo=tz8())
    nb = dt.datetime(y + 1, 1, 1, 0, 0, 0, tzinfo=tz8())
    return int(b.timestamp()*1000), int(nb.timestamp()*1000)-1

def get_by_path(obj, path: str):
    # path like "data.list" / "pager.total"
    cur = obj
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def post_json(sess, url, headers, payload, timeout=60, retries=6):
    last = None
    for i in range(retries+1):
        try:
            r = sess.post(url, headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if i == retries:
                raise
            time.sleep(0.8*(2**i) + random.random()*0.3)
    raise last

def build_payload(site, keyword, page, size, begin_ms, end_ms):
    # 以 template 为底，按字段名写入
    p = json.loads(json.dumps(site["payload_template"]))  # deep copy
    p[site["keyword_field"]] = keyword
    p[site["page_field"]] = page
    p[site["size_field"]] = size
    if site.get("begin_field"):
        p[site["begin_field"]] = begin_ms
    if site.get("end_field"):
        p[site["end_field"]] = end_ms
    if site.get("order_field") and site.get("order_value") is not None:
        p[site["order_field"]] = site["order_value"]
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--keyword", required=True)
    ap.add_argument("--start-year", type=int, default=2011)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=120)
    args = ap.parse_args()

    # ===== 只改这里：每个省/每个站点一份配置 =====
    SITE = {
        "name": "shaanxi_gov",
        "api_url": "https://www.shaanxi.gov.cn/irs/front/search",
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://www.shaanxi.gov.cn",
            "Referer": "https://www.shaanxi.gov.cn/sxsearch/search.html?tenantId=16711",
        },
        # payload 结构按 cURL 里的 JSON 原样放进来（其余字段由 build_payload 覆盖）
        "payload_template": {
            "tenantId": "16711",
            "configTenantId": "16711",
            "dataTypeId": 17018,
            "historySearchWords": [],
            "granularity": "ALL",
            "searchBy": "all",
            "filters": [],
            "customFilter": {"operator": "and", "properties": [{"property": "site_id", "operator": "eq", "value": 11}], "filters": []}
        },
        "keyword_field": "searchWord",
        "page_field": "pageNo",
        "size_field": "pageSize",
        "begin_field": "beginDateTime",
        "end_field": "endDateTime",
        "order_field": "orderBy",
        "order_value": "time",       # 不确定就换成 related（以你 Network 为准）
        "total_path": "data.total",  # 需要你按 Response 调整
        "list_path": "data.list",    # 需要你按 Response 调整
        "url_field": "linkUrl",      # 需要你按 item 字段调整
        "title_field": "title",
        "date_field": "time",
    }
    # =======================================

    out_root = Path(args.out_root)
    ensure_dir(out_root)
    sess = requests.Session()

    summary_csv = out_root / "summary_years.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as sf:
        sw = csv.DictWriter(sf, fieldnames=["year","total","urls"])
        sw.writeheader()

        for y in range(args.start_year, args.end_year+1):
            print(f"[YEAR] {y} ...")
            begin_ms, end_ms = year_range_ms(y)

            urls = set()
            page = 1
            total = None

            meta_dir = out_root / str(y) / "metadata"
            urls_dir = out_root / str(y) / "urls"
            ensure_dir(meta_dir); ensure_dir(urls_dir)

            meta_csv = meta_dir / "irs_results.csv"
            with meta_csv.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["year","page","title","date","url"])
                w.writeheader()

                while True:
                    payload = build_payload(SITE, args.keyword, page, args.page_size, begin_ms, end_ms)
                    j = post_json(sess, SITE["api_url"], SITE["headers"], payload, timeout=args.timeout)

                    if total is None:
                        total = get_by_path(j, SITE["total_path"]) or 0
                        print(f"  total={total}")

                    items = get_by_path(j, SITE["list_path"]) or []
                    if not isinstance(items, list) or not items:
                        break

                    new_cnt = 0
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        u = it.get(SITE["url_field"])
                        if isinstance(u, str) and u.strip():
                            u = u.strip()
                            if u not in urls:
                                urls.add(u); new_cnt += 1
                            w.writerow({
                                "year": y,
                                "page": page,
                                "title": it.get(SITE["title_field"], ""),
                                "date": it.get(SITE["date_field"], ""),
                                "url": u
                            })

                    if page % 10 == 0:
                        print(f"  page {page}, urls={len(urls)}, new={new_cnt}")

                    if new_cnt == 0 and page > 1:
                        break
                    if total and len(urls) >= total:
                        break

                    page += 1
                    time.sleep(0.8 + random.random()*0.2)

            (urls_dir / "urls_all.txt").write_text("\n".join(sorted(urls)), encoding="utf-8")
            sw.writerow({"year": y, "total": total, "urls": len(urls)})
            print(f"[YEAR] {y} done: total={total} urls={len(urls)}")

    print(f"[DONE] {summary_csv}")

if __name__ == "__main__":
    main()