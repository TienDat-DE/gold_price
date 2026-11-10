import os
import json
import hashlib
from abc import abstractmethod
from src.core.base_job import BaseJob

class BaseStaging(BaseJob):
    def read_raw_json(self, file_name: str):
        """Đọc dữ liệu thô từ khâu Crawler"""
        file_path = f"data/01_raw/{file_name}"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @abstractmethod
    def extract_data(self, raw_data):
        """Sub-task 5.1: Định nghĩa cách bóc tách JSON ở lớp con"""
        pass

    @abstractmethod
    def normalize_data(self, extracted_data):
        """Sub-task 5.2: Định nghĩa cách nhân tỷ lệ giá ở lớp con"""
        pass

    def _generate_hash(self, buy_price, sell_price):
        """Tạo chuỗi MD5 từ giá mua và bán để check lịch sử (SCD2) nhanh hơn"""
        hash_string = f"{buy_price}_{sell_price}".encode('utf-8')
        return hashlib.md5(hash_string).hexdigest()

    def validate_and_hash(self, data_list):
        """Sub-task 5.3: Data Quality Rules & Hashing"""
        valid_data = []
        anomaly_data = []

        for row in data_list:
            # Rule 1: Check Null
            if not row.get('gold_type') or row.get('buy_price') is None or row.get('sell_price') is None:
                row['anomaly_reason'] = "Dữ liệu bị Null/Thiếu trường"
                anomaly_data.append(row)
                continue
            
            try:
                # Rule 2: Numeric Format
                buy = float(row['buy_price'])
                sell = float(row['sell_price'])
            except ValueError:
                row['anomaly_reason'] = "Giá trị không phải là số"
                anomaly_data.append(row)
                continue
                
            # Rule 3: Negative/Zero Value
            if buy <= 0 or sell <= 0:
                row['anomaly_reason'] = f"Giá trị âm hoặc bằng 0 (Mua: {buy}, Bán: {sell})"
                anomaly_data.append(row)
                continue
            
            # Data hợp lệ -> Ép kiểu int, tính Hash
            row['buy_price'] = int(buy)
            row['sell_price'] = int(sell)
            row['price_hash'] = self._generate_hash(row['buy_price'], row['sell_price'])
            valid_data.append(row)
            
        return valid_data, anomaly_data

    def save_to_db(self, valid_data, anomaly_data):
        """Đổ dữ liệu vào Staging và Anomaly Log"""
        if valid_data:
            sql_staging = """
                INSERT INTO staging_gold_price (run_id, store_code, gold_type, buy_price, sell_price, logical_date, price_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values_staging = [
                (self._run_id, row['store_code'], row['gold_type'], row['buy_price'], row['sell_price'], self._logical_date, row['price_hash'])
                for row in valid_data
            ]
            self._cursor.executemany(sql_staging, values_staging)
            self.logger.info(f"Đã chèn {self._cursor.rowcount} dòng vào staging_gold_price.")

        if anomaly_data:
            sql_anomaly = """
                INSERT INTO anomaly_log (run_id, store_code, gold_type, buy_price, sell_price, anomaly_reason)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            values_anomaly = [
                (self._run_id, row.get('store_code'), row.get('gold_type'), row.get('buy_price'), row.get('sell_price'), row.get('anomaly_reason'))
                for row in anomaly_data
            ]
            self._cursor.executemany(sql_anomaly, values_anomaly)
            self.logger.warning(f"Phát hiện {len(anomaly_data)} dòng dị thường, đã lưu vào anomaly_log.")
            
        self._db.commit()

    def execute(self):
        """Template Method kết nối luồng Staging"""
        raw_file_name = f"{self._job_name.split('_')[0].lower()}_{self._logical_date}.json"
        
        self.logger.info("Đang đọc file thô và bóc tách dữ liệu...")
        raw_data = self.read_raw_json(raw_file_name)
        extracted = self.extract_data(raw_data)
        
        self.logger.info("Đang chuẩn hóa tỷ lệ giá...")
        normalized = self.normalize_data(extracted)
        
        self.logger.info("Đang quét Data Quality và sinh Hash...")
        valid, anomaly = self.validate_and_hash(normalized)
        
        self.logger.info("Đang lưu vào Database...")
        self.save_to_db(valid, anomaly)