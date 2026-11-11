import os
import json
from src.core.base_job import BaseJob

class XAUStaging(BaseJob):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="XAU_STAGING", run_id=run_id, logical_date=logical_date)

    def execute(self):
        file_path = f"data/01_raw/xau_{self._logical_date}.json"
        
        if not os.path.exists(file_path):
            self.logger.error(f"Không tìm thấy file: {file_path}")
            return

        self.logger.info("Đang bóc tách file JSON XAU...")
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
            
        # Lấy giá USD từ key "price"
        price_usd = float(raw_data.get("price", 0))
        
        if price_usd <= 0:
            self.logger.warning("Lỗi Data Quality: Giá XAU <= 0. Bỏ qua.")
            return

        # Tạo date_key chuẩn (YYYYMMDD) từ logical_date
        date_key = int(self._logical_date.replace("-", ""))
        
        self.logger.info(f"Giá XAU bóc tách thành công: {price_usd} USD/Ounce")
        
        # Load vào DB (Upsert)
        sql = """
            INSERT INTO fact_xau_price (date_key, currency, price_usd)
            VALUES (%s, 'USD', %s)
            ON DUPLICATE KEY UPDATE 
                price_usd = VALUES(price_usd)
        """
        
        try:
            self._cursor.execute(sql, (date_key, price_usd))
            self._db.commit()
            self.logger.info("Đã Load thành công giá XAU vào Data Warehouse.")
        except Exception as e:
            self.logger.error(f"Lỗi Insert DB XAU: {e}")
            self._db.rollback()
            raise