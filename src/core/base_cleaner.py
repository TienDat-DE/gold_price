from abc import abstractmethod
from .base_job import BaseJob

class BaseCleaner(BaseJob):
    """Abstract Class xử lý Business Logic, Hashing và SCD Type 2."""
    
    @abstractmethod
    def check_anomaly(self, data):
        """Kiểm tra biến động giá > 10%. Nếu có, log vào bảng anomaly_log."""
        pass

    @abstractmethod
    def generate_hash(self, data):
        """Tạo chuỗi Hash (MD5/SHA256) từ các cột dữ liệu quan trọng để theo dõi thay đổi."""
        pass

    @abstractmethod
    def load_scd2(self, data, current_hash):
        """Truy vấn previous_hash. Nếu khác nhau -> Đóng valid_to dòng cũ, Insert dòng mới."""
        pass

    def execute(self):
        """Khớp nối các bước của khâu Cleaner."""
        # Lấy dữ liệu từ bảng staging_gold_price
        self.logger.info("Đang đọc dữ liệu từ vùng Staging...")
        staging_data = {} # Truy vấn DB lấy ra tại đây
        
        self.logger.info("Đang chạy kiểm tra Anomaly...")
        self.check_anomaly(staging_data)
        
        self.logger.info("Đang mã hóa Hashing...")
        current_hash = self.generate_hash(staging_data)
        
        self.logger.info("Đang cập nhật lịch sử SCD Type 2...")
        self.load_scd2(staging_data, current_hash)