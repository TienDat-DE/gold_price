import os
import tls_client
from src.core.base_crawler import BaseCrawler
from src.core.utils import retry_request

class SJCCrawler(BaseCrawler):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="SJC_CRAWLER", run_id=run_id, logical_date=logical_date)
        self.url = 'https://sjc.com.vn/GoldPrice/Services/PriceService.ashx'

    @retry_request(max_retries=3, delay=5)
    def fetch_api(self):
        """Dùng tls_client giả lập Chrome để vượt tường lửa 403 của SJC"""
        self.logger.info(f"Đang fetch dữ liệu từ SJC API: {self.url}")
        
        # Khởi tạo session giả lập trình duyệt Chrome 120
        session = tls_client.Session(
            client_identifier="chrome_120", 
            random_tls_extension_order=True
        )
        
        # Thêm header Referer để đánh lừa SJC rằng ta đang bấm từ trang chủ của họ
        headers = {
            'referer': 'https://sjc.com.vn/'
        }
        
        response = session.get(self.url, headers=headers, timeout_seconds=15)
        
        if response.status_code != 200:
            raise Exception(f"HTTP Status: {response.status_code} - Không thể kết nối SJC")
            
        return response.text # Trả về chuỗi JSON thô

    def save_raw_data(self, raw_content: str):
        """Lưu toàn bộ JSON RAW xuống thư mục data/01_raw/"""
        os.makedirs("data/01_raw", exist_ok=True)
        file_path = f"data/01_raw/sjc_{self._logical_date}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
            
        self.logger.info(f"Đã lưu thành công payload vào: {file_path}")