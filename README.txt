文件说明
- irs_api_collect_urls_v4.py : 主脚本（抓 URL -> 下载正文 -> 词典匹配分类）
- requirements.txt      : 依赖列表

建议运行方式（Windows PowerShell）：
  cd D:\Jupyter\agriculture
  python -m venv .venv
  .\.venv\Scripts\activate
  pip install -r requirements.txt

  python crawl_and_classify.py --search-url "<你的搜索链接>" --out-dir "D:\Jupyter\agriculture\data" --mode selenium --headless
