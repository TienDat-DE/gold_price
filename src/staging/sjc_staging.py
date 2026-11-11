from src.core.base_staging import BaseStaging

class SJCStaging(BaseStaging):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="SJC_STAGING", run_id=run_id, logical_date=logical_date)

    def extract_data(self, raw_data):
        """Sub-task 5.1: Parse JSON SJC - Dựa vào Tên thay vì ID để chống gãy (break)"""
        extracted = []
        for item in raw_data.get('data', []):
            type_name = item.get('TypeName', '').upper()
            gold_type = None
            
            # Map dựa trên tên thay vì ID (1 hoặc 49)
            if '1L, 10L, 1KG' in type_name or 'VÀNG SJC 1L' in type_name:
                gold_type = 'VANG_MIENG'
            elif 'VÀNG NHẪN SJC' in type_name:
                gold_type = 'VANG_NHAN'
                
            if gold_type:
                extracted.append({
                    'store_code': 'SJC',
                    'gold_type': gold_type,
                    'buy_price': item.get('BuyValue', 0),
                    'sell_price': item.get('SellValue', 0)
                })
        return extracted

    def normalize_data(self, extracted_data):
        """Sub-task 5.2: Chuẩn hóa - Nhân 1000 cho giá SJC"""
        for row in extracted_data:
            row['buy_price'] = float(row['buy_price']) * 1000
            row['sell_price'] = float(row['sell_price']) * 1000
        return extracted_data