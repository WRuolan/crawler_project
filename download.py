# -*- coding: utf-8 -*-
r"""
fetch_only.py
-------------
仅根据 urls_all.txt 下载网页与正文：
- 输入：D:\Jupyter\agriculture\data\urls\urls_all.txt
- 输出：
  - D:\Jupyter\agriculture\data\raw_html\{md5_12}.html
  - D:\Jupyter\agriculture\data\raw_txt\{md5_12}.txt
- 不做分类，不写 articles.csv，不改 metadata/irs_results.csv

运行示例：
  conda activate dinov3
  cd /d D:\Jupyter\agriculture
  python fetch_only.py
  或者直接运行脚本即可

可选参数：
  --urls  指定 urls_all.txt 路径
  --out-dir 输出根目录（默认 D:\Jupyter\agriculture\data）
  --sleep 每篇间隔秒数（默认 0.6）
  --timeout 请求超时（默认 30）
  --retries 重试次数（默认 5）
  --resume 若 txt 已存在且非空则跳过（默认开启）
"""

import argparse
import hashlib
import random
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


DATE_PATTERNS = [
    re.compile(r"(20\d{2}|19\d{2})[-/.](\d{1,2})[-/.](\d{1,2})"),
    re.compile(r"(20\d{2}|19\d{2})年(\d{1,2})月(\d{1,2})日"),
]

# 常见正文容器（政府站模板差异很大，先穷举常见的）
CONTENT_SELECTORS = [
    "article",
    "#zoom", "#Zoom", "#content", "#Content", "#zoomcon", "#printArea",
    ".TRS_Editor", ".trs_editor", ".article", ".article-content",
    ".content", ".content-body", ".wz_content", ".details", ".detail",
    ".main", ".main-content", ".con", ".cont", ".txt", ".view", ".zw",
]


def md5_12(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]


def ensure_dirs(out_dir: Path) -> tuple[Path, Path]:
    raw_html = out_dir / "raw_html"
    raw_txt = out_dir / "raw_txt"
    raw_html.mkdir(parents=True, exist_ok=True)
    raw_txt.mkdir(parents=True, exist_ok=True)
    return raw_html, raw_txt


def load_urls(urls_path: Path) -> list[str]:
    lines = urls_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    urls = []
    seen = set()
    for ln in lines:
        u = (ln or "").strip()
        if not u:
            continue
        if u not in seen:
            seen.add(u)
            urls.append(u)
    return urls


def normalize_response_text(resp: requests.Response) -> str:
    # requests 有时会把中文页识别为 ISO-8859-1，强制用 apparent_encoding 更稳
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def extract_title(soup: BeautifulSoup) -> str:
    # 优先 h1，再退化到 title
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    return ""


def extract_date(text: str) -> str:
    head = (text or "")[:3000]
    for pat in DATE_PATTERNS:
        m = pat.search(head)
        if m:
            y, mo, d = m.group(1), m.group(2), m.group(3)
            try:
                return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            except Exception:
                return f"{y}-{mo}-{d}"
    return ""


def clean_text(s: str) -> str:
    if not s:
        return ""
    # 保留段落结构：先按行切分，再去掉空行/多空格
    lines = [re.sub(r"[ \t\u3000\xa0]+", " ", x).strip() for x in s.splitlines()]
    lines = [x for x in lines if x]
    # 合并相邻短行：尽量形成自然段
    return "\n".join(lines)


def text_chinese_chars_count(s: str) -> int:
    return len(re.findall(r"[\u4e00-\u9fff]", s or ""))


def extract_main_text(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "lxml")

    # 移除干扰
    for tag in soup(["script", "style", "noscript", "iframe"]):
        tag.decompose()

    title = extract_title(soup)

    # 尝试候选容器
    best = ""
    for sel in CONTENT_SELECTORS:
        try:
            node = soup.select_one(sel)
        except Exception:
            node = None
        if not node:
            continue
        txt = clean_text(node.get_text("\n", strip=True))
        # 用中文字符数作为主要评价（中文正文更可靠）
        if text_chinese_chars_count(txt) > text_chinese_chars_count(best):
            best = txt

    # 兜底：找最长 div/p 段
    if text_chinese_chars_count(best) < 80:
        candidates = []
        for node in soup.find_all(["div", "section", "main"]):
            txt = clean_text(node.get_text("\n", strip=True))
            if text_chinese_chars_count(txt) >= 80:
                candidates.append(txt)
        if candidates:
            best = max(candidates, key=text_chinese_chars_count)

    # 全局兜底
    if text_chinese_chars_count(best) < 50:
        best = clean_text(soup.get_text("\n", strip=True))

    date = extract_date(best)
    return title, date, best


def fetch_with_retry(session: requests.Session, url: str, headers: dict, timeout: int, retries: int) -> str:
    last_err = None
    for i in range(retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return normalize_response_text(r)
        except Exception as e:
            last_err = e
            if i == retries:
                raise
            # 指数退避 + 抖动
            time.sleep((0.8 * (2 ** i)) + random.random() * 0.3)
    raise last_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", default=r"D:\Jupyter\agriculture\data\urls\urls_all.txt")
    ap.add_argument("--out-dir", default=r"D:\Jupyter\agriculture\data")
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--resume", action="store_true", default=True)
    args = ap.parse_args()

    urls_path = Path(args.urls)
    out_dir = Path(args.out_dir)
    if not urls_path.exists():
        raise FileNotFoundError(f"urls_all.txt 不存在：{urls_path}")

    raw_html_dir, raw_txt_dir = ensure_dirs(out_dir)
    failed_path = out_dir / "failed_urls.txt"

    urls = load_urls(urls_path)
    print(f"[INFO] urls={len(urls)} from {urls_path}")

    sess = requests.Session()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Connection": "keep-alive",
    }

    done = 0
    skipped = 0
    failed = 0

    for idx, url in enumerate(urls, 1):
        uid = md5_12(url)
        html_path = raw_html_dir / f"{uid}.html"
        txt_path = raw_txt_dir / f"{uid}.txt"

        if args.resume and txt_path.exists() and txt_path.stat().st_size > 200:
            skipped += 1
            if idx % 200 == 0:
                print(f"[INFO] {idx}/{len(urls)} processed, done={done}, skipped={skipped}, failed={failed}")
            continue

        # Referer 给同域更稳一点
        try:
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
        except Exception:
            pass

        try:
            html = fetch_with_retry(sess, url, headers=headers, timeout=args.timeout, retries=args.retries)
            html_path.write_text(html, encoding="utf-8", errors="ignore")

            title, date, text = extract_main_text(html)

            txt_path.write_text(
                f"URL: {url}\nTITLE: {title}\nDATE: {date}\n\n{text}\n",
                encoding="utf-8",
                errors="ignore",
            )
            done += 1

        except Exception as e:
            failed += 1
            with failed_path.open("a", encoding="utf-8") as f:
                f.write(f"{url}\t{repr(e)}\n")

        if idx % 200 == 0:
            print(f"[INFO] {idx}/{len(urls)} processed, done={done}, skipped={skipped}, failed={failed}")

        time.sleep(args.sleep + random.random() * 0.2)

    print(f"[DONE] total={len(urls)}, done={done}, skipped={skipped}, failed={failed}")
    print(f"[DONE] raw_html: {raw_html_dir}")
    print(f"[DONE] raw_txt : {raw_txt_dir}")
    if failed:
        print(f"[WARN] failed list: {failed_path}")


if __name__ == "__main__":
    main()