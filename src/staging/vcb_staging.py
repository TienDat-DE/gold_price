import os
import xml.etree.ElementTree as ET
from src.core.base_job import BaseJob

class VCBStaging(BaseJob):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="VCB_STAGING", run_id=run_id, logical_date=logical_date)

    def execute(self):
        """Luồng bóc tách XML và Load thẳng vào Fact Exchange Rate"""
        file_path = f"data/01_raw/vcb_{self._logical_date}.xml"
        
        if not os.path.exists(file_path):
            self.logger.error(f"Không tìm thấy file: {file_path}")
            return

        self.logger.info("Đang parse file XML Vietcombank...")
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        usd_data = None
        
        # Duyệt qua các thẻ <Exrate> trong XML
        for exrate in root.findall('Exrate'):
            if exrate.attrib.get('CurrencyCode') == 'USD':
                # VCB cung cấp Buy, Transfer và Sell. Ta lấy Transfer làm giá mua chuẩn
                transfer_rate = exrate.attrib.get('Transfer', '0').replace(',', '')
                sell_rate = exrate.attrib.get('Sell', '0').replace(',', '')
                
                usd_data = {
                    'buy_rate': int(float(transfer_rate)),
                    'sell_rate': int(float(sell_rate))
                }
                break
                
        if not usd_data:
            self.logger.warning("Không tìm thấy tỷ giá USD trong file XML.")
            return
            
        # Ép logical_date sang date_key dạng INT (YYYYMMDD)
        date_key = int(self._logical_date.replace("-", ""))
        
        self.logger.info(f"Đã bóc tách USD - Mua: {usd_data['buy_rate']}, Bán: {usd_data['sell_rate']}")
        
        # Load vào DB (Sử dụng REPLACE INTO hoặc INSERT IGNORE để Idempotent)
        sql = """
            INSERT INTO fact_exchange_rate (date_key, currency_code, buy_rate, sell_rate, source)
            VALUES (%s, 'USD', %s, %s, 'VIETCOMBANK')
            ON DUPLICATE KEY UPDATE 
                buy_rate = VALUES(buy_rate), 
                sell_rate = VALUES(sell_rate)
        """
        
        try:
            self._cursor.execute(sql, (date_key, usd_data['buy_rate'], usd_data['sell_rate']))
            self._db.commit()
            self.logger.info("Đã Load thành công tỷ giá USD vào Data Warehouse.")
        except Exception as e:
            self.logger.error(f"Lỗi Insert DB tỷ giá: {e}")
            self._db.rollback()
            raise