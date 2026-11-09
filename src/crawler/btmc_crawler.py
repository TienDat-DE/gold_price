import os
import urllib.parse
from datetime import datetime
import tls_client
from src.core.base_crawler import BaseCrawler
from src.core.utils import retry_request

class BTMCCrawler(BaseCrawler):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="BTMC_CRAWLER", run_id=run_id, logical_date=logical_date)
        self.base_url = "https://btmc.vn/ProductHome/getGoldDate"

    @retry_request(max_retries=3, delay=5)
    def fetch_api(self):
        """Dùng tls_client để vượt mặt Cloudflare và lấy dữ liệu JSON theo ngày"""
        
        # 1. Chuyển đổi định dạng ngày: '2026-11-09' -> '09/11/2026'
        date_obj = datetime.strptime(self._logical_date, "%Y-%m-%d")
        formatted_date = date_obj.strftime("%d/%m/%Y")
        
        # 2. Mã hóa URL (biến dấu / thành %2F)
        encoded_date = urllib.parse.quote(formatted_date, safe='')
        
        # 3. Nối thành URL hoàn chỉnh
        dynamic_url = f"{self.base_url}?date={encoded_date}"
        self.logger.info(f"Đang fetch dữ liệu từ URL: {dynamic_url}")
        
        # Khởi tạo session giả lập Chrome 120
        session = tls_client.Session(
            client_identifier="chrome_120", 
            random_tls_extension_order=True
        )
        
        response = session.get(dynamic_url, timeout_seconds=15)
        
        if response.status_code != 200:
            raise Exception(f"HTTP Status: {response.status_code} - Không thể kết nối BTMC")
            
        return response.text # Trả về chuỗi JSON thô

    def save_raw_data(self, raw_content: str):
        """Lưu file RAW xuống thư mục data/01_raw/"""
        os.makedirs("data/01_raw", exist_ok=True)
        file_path = f"data/01_raw/btmc_{self._logical_date}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
            
        self.logger.info(f"Đã lưu thành công payload vào: {file_path}")