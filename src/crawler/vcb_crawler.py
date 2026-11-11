import os
import requests
from src.core.base_crawler import BaseCrawler
from src.core.utils import retry_request

class VCBCrawler(BaseCrawler):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="VCB_CRAWLER", run_id=run_id, logical_date=logical_date)
        self.url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"

    @retry_request(max_retries=3, delay=5)
    def fetch_api(self):
        """Gọi API XML của Vietcombank"""
        self.logger.info(f"Đang fetch tỷ giá từ: {self.url}")
        response = requests.get(self.url, timeout=(10, 15))
        
        if response.status_code != 200:
            raise Exception(f"HTTP Status: {response.status_code} - Lỗi kết nối VCB")
            
        return response.text

    def save_raw_data(self, raw_content: str):
        """Lưu file định dạng .xml vào vùng 01_raw"""
        os.makedirs("data/01_raw", exist_ok=True)
        file_path = f"data/01_raw/vcb_{self._logical_date}.xml"
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
            
        self.logger.info(f"Đã lưu thành công payload tỷ giá vào: {file_path}")