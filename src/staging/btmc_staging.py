import re
from src.core.base_staging import BaseStaging

class BTMCStaging(BaseStaging):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="BTMC_STAGING", run_id=run_id, logical_date=logical_date)

    @staticmethod
    def _strip_html(value) -> str:
        """Loại bỏ thẻ HTML như <b>13700</b> -> 13700"""
        if value is None:
            return "0"
        return re.sub(r"<[^>]+>", "", str(value)).strip()

    def extract_data(self, raw_data):
        """Sub-task 5.1: Parse JSON BTMC từ object 'Data'"""
        extracted = []
        data = raw_data.get("Data", raw_data) if isinstance(raw_data, dict) else {}

        if not isinstance(data, dict):
            self.logger.warning("Cấu trúc dữ liệu BTMC không hợp lệ.")
            return extracted

        mieng_buy = data.get("btmcvangmiengmua") or data.get("sjcmua")
        mieng_sell = data.get("btmcvangmiengban") or data.get("sjcban")
        if mieng_buy and mieng_sell:
            extracted.append({
                "store_code": "BTMC",
                "gold_type": "VANG_MIENG",
                "buy_price": self._strip_html(mieng_buy),
                "sell_price": self._strip_html(mieng_sell),
            })

        nhan_buy = data.get("btmcvangnhanmua")
        nhan_sell = data.get("btmcvangnhanban")
        if nhan_buy and nhan_sell:
            extracted.append({
                "store_code": "BTMC",
                "gold_type": "VANG_NHAN",
                "buy_price": self._strip_html(nhan_buy),
                "sell_price": self._strip_html(nhan_sell),
            })

        return extracted

    def normalize_data(self, extracted_data):
        """Sub-task 5.2: Quy đổi từ nghìn đồng/chỉ sang VNĐ/Lượng (* 10.000)"""
        for row in extracted_data:
            buy_str = str(row["buy_price"]).replace(".", "").replace(",", "")
            sell_str = str(row["sell_price"]).replace(".", "").replace(",", "")

            row["buy_price"] = (float(buy_str) * 10000) if buy_str else 0.0
            row["sell_price"] = (float(sell_str) * 10000) if sell_str else 0.0
        return extracted_data