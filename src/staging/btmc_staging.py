from src.core.base_staging import BaseStaging

class BTMCStaging(BaseStaging):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="BTMC_STAGING", run_id=run_id, logical_date=logical_date)

    def extract_data(self, raw_data):
        """Sub-task 5.1: Parse JSON BTMC"""
        extracted = []
        
        # Payload trả về của BTMC dạng list thẳng hoặc nằm trong key 'DataList'
        data_list = raw_data.get('DataList', raw_data) if isinstance(raw_data, dict) else raw_data
        
        for item in data_list:
            name = item.get('Name', '').upper()
            gold_type = None
            
            # Lọc từ khóa để chuẩn hóa thành 2 sản phẩm lõi
            if 'SJC' in name:
                gold_type = 'VANG_MIENG'
            elif 'NHẪN' in name or 'VRTL' in name:
                gold_type = 'VANG_NHAN'
                
            if gold_type:
                extracted.append({
                    'store_code': 'BTMC',
                    'gold_type': gold_type,
                    'buy_price': item.get('Buy', 0),
                    'sell_price': item.get('Sell', 0)
                })
        return extracted

    def normalize_data(self, extracted_data):
        """Sub-task 5.2: Chuẩn hóa - Làm sạch chuỗi và giữ nguyên tỷ lệ"""
        for row in extracted_data:
            buy_str = str(row['buy_price']).replace('.', '').replace(',', '')
            sell_str = str(row['sell_price']).replace('.', '').replace(',', '')
            
            row['buy_price'] = float(buy_str) if buy_str else 0
            row['sell_price'] = float(sell_str) if sell_str else 0
        return extracted_data