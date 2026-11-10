from src.core.base_staging import BaseStaging

class SJCStaging(BaseStaging):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="SJC_STAGING", run_id=run_id, logical_date=logical_date)

    def extract_data(self, raw_data):
        """Sub-task 5.1: Parse JSON SJC"""
        extracted = []
        for item in raw_data.get('data', []):
            if item['Id'] in [1, 49]:
                extracted.append({
                    'store_code': 'SJC',
                    'gold_type': 'VANG_MIENG' if item['Id'] == 1 else 'VANG_NHAN',
                    'buy_price': item['BuyValue'],
                    'sell_price': item['SellValue']
                })
        return extracted

    def normalize_data(self, extracted_data):
        """Sub-task 5.2: Chuẩn hóa - Nhân 1000 cho giá SJC"""
        for row in extracted_data:
            row['buy_price'] = float(row['buy_price']) * 1000
            row['sell_price'] = float(row['sell_price']) * 1000
        return extracted_data