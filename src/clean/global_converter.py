from src.core.base_job import BaseJob

class GlobalConverter(BaseJob):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="GLOBAL_CONVERTER", run_id=run_id, logical_date=logical_date)
        # Bật dictionary=True để lấy dữ liệu theo tên cột
        self._cursor = self._db.cursor(dictionary=True)

    def execute(self):
        # Lấy ID của Dimension "Thế Giới"
        self._cursor.execute("SELECT store_id FROM dim_store WHERE store_code = 'WORLD'")
        store_id = self._cursor.fetchone()['store_id']
        
        self._cursor.execute("SELECT gold_type_id FROM dim_gold_type WHERE gold_type_code = 'XAU_VND'")
        type_id = self._cursor.fetchone()['gold_type_id']

        date_key = int(self._logical_date.replace("-", ""))

        # Sub-task 9.1: Truy vấn JOIN lấy XAU và USD theo cùng date_key
        sql_join = """
            SELECT x.price_usd as xau_price, e.buy_rate as usd_rate
            FROM fact_xau_price x
            JOIN fact_exchange_rate e ON x.date_key = e.date_key
            WHERE x.date_key = %s
        """
        self._cursor.execute(sql_join, (date_key,))
        result = self._cursor.fetchone()

        if not result:
            self.logger.warning(f"Bỏ qua: Không đủ dữ liệu XAU hoặc USD của ngày {date_key} để quy đổi.")
            return

        xau_usd = float(result['xau_price'])
        usd_vnd = float(result['usd_rate'])

        # Sub-task 9.2: Thuật toán quy đổi theo chuẩn Business Logic của bạn
        # Công thức: (XAU + 1) * 1.01 * 1.20565 * Tỷ Giá + 150000 (Thuế phí gia công)
        converted_price = (xau_usd + 1) * 1.01 * 1.20565 * usd_vnd + 150000
        final_price = int(converted_price)

        self.logger.info(f"Quy đổi: {xau_usd} USD/oz * {usd_vnd} VNĐ -> {final_price} VNĐ/Lượng")

        # Sub-task 9.3: Lưu kết quả vào Fact Gold Price
        # Kiểm tra xem hôm nay đã lưu giá quy đổi chưa (Idempotent)
        sql_check = "SELECT fact_id FROM fact_gold_price WHERE date_key = %s AND store_id = %s AND gold_type_id = %s"
        self._cursor.execute(sql_check, (date_key, store_id, type_id))
        exists = self._cursor.fetchone()

        if exists:
            self.logger.info("Giá quy đổi của ngày hôm nay đã tồn tại trong Fact. Bỏ qua (Tránh trùng lặp).")
        else:
            # Giá thế giới quy đổi không có giá Mua/Bán riêng biệt, nên ta gán Buy = Sell, Spread = 0
            sql_insert = """
                INSERT INTO fact_gold_price (date_key, store_id, gold_type_id, buy_price, sell_price, spread)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self._cursor.execute(sql_insert, (date_key, store_id, type_id, final_price, final_price, 0))
            self._db.commit()
            self.logger.info(f"Đã lưu thành công Giá Vàng Thế Giới vào DWH. Sẵn sàng cho Dashboard!")