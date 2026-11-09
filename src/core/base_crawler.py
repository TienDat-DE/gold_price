from abc import abstractmethod
from .base_job import BaseJob

class BaseCrawler(BaseJob):
    """Abstract Class định nghĩa chuẩn cho mọi bot cào dữ liệu."""
    
    @abstractmethod
    def fetch_api(self):
        """Logic gọi API hoặc Request HTML."""
        pass

    @abstractmethod
    def save_raw_data(self, raw_content):
        """Logic lưu dữ liệu thô ra file (JSON/HTML) vào thư mục data/01_raw/."""
        pass

    def execute(self):
        """Khớp nối các bước của khâu Crawler."""
        self.logger.info("Đang gọi API thu thập dữ liệu...")
        raw_content = self.fetch_api()
        
        self.logger.info("Đang lưu dữ liệu nguyên trạng...")
        self.save_raw_data(raw_content)
        self.logger.info("Crawler hoàn tất xuất sắc.")