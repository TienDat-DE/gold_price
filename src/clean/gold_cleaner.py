from src.core.base_job import BaseJob


class GoldCleaner(BaseJob):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="GOLD_CLEANER", run_id=run_id, logical_date=logical_date)
        if self._cursor:
            self._cursor.close()
        # Bật dictionary=True và buffered=True để đọc theo tên cột và tránh lỗi "Unread result found"
        self._cursor = self._db.cursor(dictionary=True, buffered=True)

    def _get_dimension_maps(self):
        """Nạp bảng ánh xạ Dimension store_code -> store_id và gold_type_code -> gold_type_id"""
        self._cursor.execute("SELECT store_id, store_code FROM dim_store")
        store_map = {row["store_code"]: row["store_id"] for row in self._cursor.fetchall()}

        self._cursor.execute("SELECT gold_type_id, gold_type_code FROM dim_gold_type")
        type_map = {row["gold_type_code"]: row["gold_type_id"] for row in self._cursor.fetchall()}

        return store_map, type_map

    def _get_latest_fact_price(self, store_id: int, gold_type_id: int):
        """Lấy mức giá gần nhất đã lưu trong Data Warehouse để đối chiếu Idempotent & Anomaly"""
        sql = """
            SELECT fact_id, date_key, buy_price, sell_price
            FROM fact_gold_price
            WHERE store_id = %s AND gold_type_id = %s
            ORDER BY date_key DESC, fact_id DESC
            LIMIT 1
        """
        self._cursor.execute(sql, (store_id, gold_type_id))
        return self._cursor.fetchone()

    def _log_anomaly(self, store_code: str, gold_type: str, old_price: int, new_price: int, pct_change: float):
        """Ghi nhận cảnh báo biến động >10% vào bảng anomaly_log (tự khớp cấu trúc cột trong DB)"""
        reason = f"Biến động {pct_change:.2f}% vượt ngưỡng 10% (Cũ: {old_price:,} -> Mới: {new_price:,})"
        try:
            self._cursor.execute("SHOW COLUMNS FROM anomaly_log")
            columns_info = self._cursor.fetchall()

            field_values = {
                "run_id": self._run_id,
                "logical_date": self._logical_date,
                "store_code": store_code,
                "gold_type": gold_type,
                "gold_type_code": gold_type,
                "old_price": old_price,
                "prev_price": old_price,
                "prev_sell_price": old_price,
                "new_price": new_price,
                "curr_price": new_price,
                "curr_sell_price": new_price,
                "pct_change": round(pct_change, 2),
                "change_pct": round(pct_change, 2),
                "diff_percent": round(pct_change, 2),
                "reason": reason,
                "message": reason,
                "description": reason,
            }

            cols, vals = [], []
            for col in columns_info:
                col_name = col["Field"]
                if "auto_increment" in (col["Extra"] or ""):
                    continue
                if col_name in field_values:
                    cols.append(col_name)
                    vals.append(field_values[col_name])
                elif col["Null"] == "NO" and col["Default"] is None:
                    cols.append(col_name)
                    vals.append(reason)

            if cols:
                placeholders = ", ".join(["%s"] * len(cols))
                col_list = ", ".join(cols)
                sql = f"INSERT INTO anomaly_log ({col_list}) VALUES ({placeholders})"
                self._cursor.execute(sql, tuple(vals))
                self._db.commit()
        except Exception as e:
            self.logger.error(f"Không thể ghi vào anomaly_log: {e}")

    def execute(self):
        self.logger.info(f"Đang đọc dữ liệu mới nhất từ Staging cho run_id = {self._run_id}...")

        # Chỉ lấy bản ghi mới nhất (MAX staging_id) của từng cặp cửa hàng - loại vàng
        # giúp an toàn tuyệt đối khi chạy lại DAG nhiều lần trong cùng một ngày
        sql_staging = """
            SELECT s.*
            FROM staging_gold_price s
            INNER JOIN (
                SELECT store_code, gold_type, MAX(staging_id) AS max_id
                FROM staging_gold_price
                WHERE run_id = %s
                GROUP BY store_code, gold_type
            ) latest ON s.staging_id = latest.max_id
        """
        self._cursor.execute(sql_staging, (self._run_id,))
        staging_data = self._cursor.fetchall()

        if not staging_data:
            self.logger.warning(f"Không tìm thấy dữ liệu Staging cho run_id = {self._run_id}. Bỏ qua.")
            return

        store_map, type_map = self._get_dimension_maps()
        date_key = int(str(self._logical_date)[:10].replace("-", ""))

        for row in staging_data:
            store_code = row["store_code"]
            gold_type = row["gold_type"]
            buy_price = int(float(row["buy_price"]))
            sell_price = int(float(row["sell_price"]))
            spread = sell_price - buy_price

            store_id = store_map.get(store_code)
            type_id = type_map.get(gold_type)

            if not store_id or not type_id:
                self.logger.warning(f"Bỏ qua {store_code} - {gold_type}: Chưa khai báo trong bảng Dimension.")
                continue

            prev_fact = self._get_latest_fact_price(store_id, type_id)

            if prev_fact:
                prev_buy = int(float(prev_fact["buy_price"]))
                prev_sell = int(float(prev_fact["sell_price"]))

                # 1. Kiểm tra Idempotent: Nếu giá không đổi so với bản ghi gần nhất thì bỏ qua
                if prev_buy == buy_price and prev_sell == sell_price:
                    self.logger.info(
                        f"Giá {store_code} - {gold_type} KHÔNG ĐỔI ({buy_price:,} / {sell_price:,}). Bỏ qua."
                    )
                    continue

                # 2. Kiểm tra Anomaly Detection: Chặn nếu giá bán biến động > 10%
                if prev_sell > 0:
                    pct_change = abs(sell_price - prev_sell) / prev_sell * 100
                    if pct_change > 10.0:
                        self.logger.warning(
                            f"Phát hiện Anomaly: Biến động >10% ({pct_change:.2f}%) cho {store_code} - {gold_type} "
                            f"(Cũ: {prev_sell:,} -> Mới: {sell_price:,}). Chặn nạp DWH!"
                        )
                        self._log_anomaly(store_code, gold_type, prev_sell, sell_price, pct_change)
                        continue

            # 3. Nạp bản ghi hợp lệ vào Data Warehouse
            sql_insert = """
                INSERT INTO fact_gold_price (date_key, store_id, gold_type_id, buy_price, sell_price, spread)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self._cursor.execute(sql_insert, (date_key, store_id, type_id, buy_price, sell_price, spread))
            self._db.commit()
            self.logger.info(
                f"Đã nạp DWH: {store_code} - {gold_type} | Mua: {buy_price:,} | Bán: {sell_price:,}"
            )