from src.core.base_job import BaseJob

class GlobalConverter(BaseJob):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="GLOBAL_CONVERTER", run_id=run_id, logical_date=logical_date)
        if self._cursor:
            self._cursor.close()
        # Bật buffered=True để tránh lỗi "Unread result found" khi chạy nhiều query liên tiếp
        self._cursor = self._db.cursor(dictionary=True, buffered=True)

    def execute(self):
        self._cursor.execute("SELECT store_id FROM dim_store WHERE store_code = 'WORLD'")
        store_row = self._cursor.fetchone()
        
        self._cursor.execute("SELECT gold_type_id FROM dim_gold_type WHERE gold_type_code = 'XAU_VND'")
        type_row = self._cursor.fetchone()

        if not store_row or not type_row:
            self.logger.error("Thiếu dữ liệu WORLD hoặc XAU_VND trong bảng Dimension.")
            return

        store_id = store_row['store_id']
        type_id = type_row['gold_type_id']
        date_key = int(self._logical_date.replace("-", ""))

        # Sub-task 9.1: Truy vấn JOIN lấy XAU và USD theo cùng date_key
        sql_join = """
            SELECT x.price_usd as xau_price, e.buy_rate as usd_rate
            FROM fact_xau_price x
            JOIN fact_exchange_rate e ON x.date_key = e.date_key
            WHERE x.date_key = %s
            LIMIT 1
        """
        self._cursor.execute(sql_join, (date_key,))
        result = self._cursor.fetchone()

        if not result:
            self.logger.warning(f"Bỏ qua: Không đủ dữ liệu XAU hoặc USD của ngày {date_key} để quy đổi.")
            return

        xau_usd = float(result['xau_price'])
        usd_vnd = float(result['usd_rate'])

        # Sub-task 9.2: Thuật toán quy đổi sang VNĐ/Lượng
        converted_price = (xau_usd + 1) * 1.01 * 1.20565 * usd_vnd + 150000
        final_price = int(converted_price)

        self.logger.info(f"Quy đổi: {xau_usd} USD/oz * {usd_vnd} VNĐ -> {final_price} VNĐ/Lượng")

        # Sub-task 9.3: Lưu kết quả vào Fact Gold Price (Idempotent)
        sql_check = "SELECT fact_id FROM fact_gold_price WHERE date_key = %s AND store_id = %s AND gold_type_id = %s"
        self._cursor.execute(sql_check, (date_key, store_id, type_id))
        exists = self._cursor.fetchone()

        if exists:
            self.logger.info("Giá quy đổi của ngày hôm nay đã tồn tại trong Fact. Bỏ qua.")
        else:
            sql_insert = """
                INSERT INTO fact_gold_price (date_key, store_id, gold_type_id, buy_price, sell_price, spread)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self._cursor.execute(sql_insert, (date_key, store_id, type_id, final_price, final_price, 0))
            self._db.commit()
            self.logger.info("Đã lưu thành công Giá Vàng Thế Giới vào DWH.")