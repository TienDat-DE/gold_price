from src.core.base_staging import BaseStaging

class SJCStaging(BaseStaging):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="SJC_STAGING", run_id=run_id, logical_date=logical_date)

    def extract_data(self, raw_data):
        """Sub-task 5.1: Parse JSON SJC - Lọc chi nhánh chuẩn & map theo tên"""
        extracted = []
        seen_types = set()

        for item in raw_data.get('data', []):
            branch_name = item.get('BranchName', 'Hồ Chí Minh').upper()
            if 'HỒ CHÍ MINH' not in branch_name:
                continue

            type_name = item.get('TypeName', '').upper()
            gold_type = None

            if '1L, 10L, 1KG' in type_name or 'VÀNG SJC 1L' in type_name:
                gold_type = 'VANG_MIENG'
            elif 'VÀNG NHẪN SJC' in type_name:
                gold_type = 'VANG_NHAN'

            if gold_type and gold_type not in seen_types:
                seen_types.add(gold_type)
                extracted.append({
                    'store_code': 'SJC',
                    'gold_type': gold_type,
                    'buy_price': item.get('BuyValue', 0),
                    'sell_price': item.get('SellValue', 0)
                })
        return extracted

    def normalize_data(self, extracted_data):
        """Sub-task 5.2: BuyValue của SJC đã ở đơn vị VNĐ/Lượng"""
        for row in extracted_data:
            row['buy_price'] = float(row['buy_price'])
            row['sell_price'] = float(row['sell_price'])
        return extracted_data