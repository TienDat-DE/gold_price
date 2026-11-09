# File: test_crawler.py (chỉ dùng để test)
from src.crawler.btmc_crawler import BTMCCrawler
from src.crawler.sjc_crawler import SJCCrawler

# Giả lập ID và ngày do Airflow cấp
test_run_id = "test_run_20261109_01"
test_date = "2026-11-09"

print("--- CHẠY BOT BTMC ---")
bot_btmc = BTMCCrawler(run_id=test_run_id, logical_date=test_date)
bot_btmc.run(step_name="CRAWL_BTMC")

print("\n--- CHẠY BOT SJC ---")
bot_sjc = SJCCrawler(run_id=test_run_id, logical_date=test_date)
bot_sjc.run(step_name="CRAWL_SJC")