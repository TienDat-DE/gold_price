from abc import abstractmethod
from .base_job import BaseJob

class BaseStaging(BaseJob):
    """Abstract Class định nghĩa chuẩn cho quá trình Parsing và Chuẩn hóa đơn vị."""
    
    @abstractmethod
    def extract_data(self):
        """Đọc file raw từ khâu trước, dùng BeautifulSoup/Regex bóc tách ra dict/dataframe."""
        pass

    @abstractmethod
    def normalize_data(self, extracted_data):
        """Xử lý tính Đa hình: Ép kiểu, nhân tỷ lệ (VD: SJC x1000) để quy về chuẩn VND."""
        pass
        
    def save_staging_data(self, clean_data):
        """(Hàm dùng chung) Lưu dữ liệu đã chuẩn hóa vào staging_gold_price."""
        # Code thực thi INSERT vào bảng staging_gold_price
        pass

    def execute(self):
        """Khớp nối các bước của khâu Staging."""
        self.logger.info("Đang bóc tách dữ liệu từ file raw...")
        extracted = self.extract_data()
        
        self.logger.info("Đang chuẩn hóa định dạng số/đơn vị...")
        normalized = self.normalize_data(extracted)
        
        self.logger.info("Đang lưu vào vùng Staging...")
        self.save_staging_data(normalized)