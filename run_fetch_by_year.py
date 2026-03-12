# -*- coding: utf-8 -*-
r"""
run_fetch_by_year.py
按年份目录结构批量下载 raw_html/raw_txt。

目录结构示例：
D:\Jupyter\agriculture\corpus\shaanxi_gov\
  2011\urls\urls_all.txt
  2012\urls\urls_all.txt
  ...
  2025\urls\urls_all.txt

输出：
  <root>\<year>\raw_html\
  <root>\<year>\raw_txt\
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help=r'年份目录根路径，例如 D:\Jupyter\agriculture\corpus\shaanxi_gov')
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    ap.add_argument("--fetch-script", default="fetch_only.py",
                    help="下载脚本路径（你的 fetch_only.py 或 download.py）。默认当前目录下 fetch_only.py")
    ap.add_argument("--sleep", type=float, default=None)
    ap.add_argument("--timeout", type=int, default=None)
    ap.add_argument("--retries", type=int, default=None)
    args = ap.parse_args()

    root = Path(args.root)
    fetch_script = Path(args.fetch_script)
    if not fetch_script.is_file():
        fetch_script = (Path.cwd() / args.fetch_script).resolve()
    if not fetch_script.is_file():
        raise FileNotFoundError(f"fetch-script 不存在：{fetch_script}")

    ok_years = 0
    skip_years = 0
    fail_years = 0

    for y in range(args.start_year, args.end_year + 1):
        ydir = root / str(y)
        urls_txt = ydir / "urls" / "urls_all.txt"
        if (not urls_txt.exists()) or urls_txt.stat().st_size == 0:
            print(f"[SKIP] {y}: urls_all.txt not found/empty -> {urls_txt}")
            skip_years += 1
            continue

        out_dir = ydir  # 输出到该年份目录下

        cmd = [sys.executable, str(fetch_script), "--urls", str(urls_txt), "--out-dir", str(out_dir)]
        if args.sleep is not None:
            cmd += ["--sleep", str(args.sleep)]
        if args.timeout is not None:
            cmd += ["--timeout", str(args.timeout)]
        if args.retries is not None:
            cmd += ["--retries", str(args.retries)]

        print(f"\n[YEAR {y}] RUN: {' '.join(cmd)}")
        r = subprocess.run(cmd, check=False)
        if r.returncode == 0:
            ok_years += 1
            print(f"[YEAR {y}] DONE")
        else:
            fail_years += 1
            print(f"[YEAR {y}] FAIL (code={r.returncode})")

    print("\n==== SUMMARY ====")
    print(f"OK:   {ok_years}")
    print(f"SKIP: {skip_years}")
    print(f"FAIL: {fail_years}")


if __name__ == "__main__":
    main()