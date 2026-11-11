import os
import requests
from src.core.base_crawler import BaseCrawler
from src.core.utils import retry_request

class XAUCrawler(BaseCrawler):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="XAU_CRAWLER", run_id=run_id, logical_date=logical_date)
        # Sử dụng endpoint miễn phí và ổn định nhất từ code cũ của bạn
        self.url = "https://api.gold-api.com/price/XAU"

    @retry_request(max_retries=3, delay=5)
    def fetch_api(self):
        self.logger.info(f"Đang fetch giá XAU từ: {self.url}")
        response = requests.get(self.url, timeout=(10, 15))
        
        if response.status_code != 200:
            raise Exception(f"HTTP Status: {response.status_code} - Lỗi kết nối Gold API")
            
        return response.text

    def save_raw_data(self, raw_content: str):
        os.makedirs("data/01_raw", exist_ok=True)
        file_path = f"data/01_raw/xau_{self._logical_date}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
            
        self.logger.info(f"Đã lưu thành công payload XAU vào: {file_path}")