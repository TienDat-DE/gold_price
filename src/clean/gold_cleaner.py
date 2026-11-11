from src.core.base_cleaner import BaseCleaner

class GoldCleaner(BaseCleaner):
    def __init__(self, run_id: str, logical_date: str):
        super().__init__(job_name="GOLD_CLEANER", run_id=run_id, logical_date=logical_date)
        # Sử dụng dictionary để truy xuất tên cột dễ dàng
        self._cursor = self._db.cursor(dictionary=True)

    def _get_dim_mapping(self):
        """Lấy Mapping ID từ Data Warehouse (Biến Mã Chữ thành Mã Số)"""
        self._cursor.execute("SELECT store_code, store_id FROM dim_store")
        store_map = {row['store_code']: row['store_id'] for row in self._cursor.fetchall()}
        
        self._cursor.execute("SELECT gold_type_code, gold_type_id FROM dim_gold_type")
        type_map = {row['gold_type_code']: row['gold_type_id'] for row in self._cursor.fetchall()}
        return store_map, type_map

    def _get_latest_prices(self):
        """Lấy giá mới nhất hiện có trong DWH để so sánh Anomaly và SCD2"""
        sql = """
            SELECT f.store_id, f.gold_type_id, f.buy_price, f.sell_price
            FROM fact_gold_price f
            INNER JOIN (
                SELECT store_id, gold_type_id, MAX(recorded_at) as max_time
                FROM fact_gold_price
                GROUP BY store_id, gold_type_id
            ) latest ON f.store_id = latest.store_id 
                     AND f.gold_type_id = latest.gold_type_id 
                     AND f.recorded_at = latest.max_time
        """
        self._cursor.execute(sql)
        return {(row['store_id'], row['gold_type_id']): (row['buy_price'], row['sell_price']) for row in self._cursor.fetchall()}

    def check_anomaly(self, staging_data):
        """Thực thi kiểm tra biến động giá > 10%"""
        store_map, type_map = self._get_dim_mapping()
        latest_prices = self._get_latest_prices()
        
        valid_data = []
        anomaly_inserts = []
        
        for row in staging_data:
            store_id = store_map.get(row['store_code'])
            type_id = type_map.get(row['gold_type'])
            
            if not store_id or not type_id:
                continue
                
            new_buy, new_sell = row['buy_price'], row['sell_price']
            latest = latest_prices.get((store_id, type_id))
            
            if latest:
                old_buy, old_sell = latest
                buy_diff = abs(new_buy - old_buy) / old_buy
                sell_diff = abs(new_sell - old_sell) / old_sell
                
                if buy_diff > 0.1 or sell_diff > 0.1:
                    reason = f"Biến động >10%: Mua lệch {buy_diff:.1%}, Bán lệch {sell_diff:.1%}"
                    anomaly_inserts.append((self._run_id, row['store_code'], row['gold_type'], new_buy, new_sell, reason))
                    self.logger.warning(f"Phát hiện Anomaly: {reason}")
                    continue # Bỏ qua dòng lỗi này
            
            # Gắn thêm ID vào data hợp lệ để truyền sang bước sau
            row['store_id'] = store_id
            row['type_id'] = type_id
            valid_data.append(row)
            
        if anomaly_inserts:
            sql = "INSERT INTO anomaly_log (run_id, store_code, gold_type, buy_price, sell_price, anomaly_reason) VALUES (%s, %s, %s, %s, %s, %s)"
            self._cursor.executemany(sql, anomaly_inserts)
            self._db.commit()
            
        return valid_data

    def generate_hash(self, staging_data):
        """Staging đã tính sẵn Hash, ở đây chỉ trích xuất danh sách hash hiện tại[cite: 4]"""
        return [row['price_hash'] for row in staging_data]

    def load_scd2(self, valid_data, current_hash):
        """So sánh giá cũ/mới và Insert dòng mới vào Fact thay vì đóng valid_to[cite: 4]"""
        latest_prices = self._get_latest_prices()
        fact_inserts = []
        
        for row in valid_data:
            store_id, type_id = row['store_id'], row['type_id']
            new_buy, new_sell = row['buy_price'], row['sell_price']
            latest = latest_prices.get((store_id, type_id))
            
            # Nếu giá đã tồn tại và không thay đổi -> Bỏ qua
            if latest and new_buy == latest[0] and new_sell == latest[1]:
                self.logger.info(f"Giá {row['store_code']} - {row['gold_type']} KHÔNG ĐỔI. Bỏ qua.")
                continue
                
            spread = new_sell - new_buy
            date_key = int(row['logical_date'].strftime("%Y%m%d"))
            fact_inserts.append((date_key, store_id, type_id, new_buy, new_sell, spread))
            
        if fact_inserts:
            sql = "INSERT INTO fact_gold_price (date_key, store_id, gold_type_id, buy_price, sell_price, spread) VALUES (%s, %s, %s, %s, %s, %s)"
            self._cursor.executemany(sql, fact_inserts)
            self._db.commit()
            self.logger.info(f"Đã Load thành công {len(fact_inserts)} dòng giá MỚI vào Data Warehouse.")

    def execute(self):
        """Khớp nối các bước của khâu Cleaner[cite: 4] - Ghi đè lại để truyền data xuyên suốt"""
        self.logger.info("Đang đọc dữ liệu từ vùng Staging...[cite: 4]")
        self._cursor.execute("SELECT * FROM staging_gold_price WHERE run_id = %s", (self._run_id,))
        staging_data = self._cursor.fetchall()
        
        if not staging_data:
            self.logger.info("Không có dữ liệu Staging để xử lý.")
            return

        self.logger.info("Đang chạy kiểm tra Anomaly...[cite: 4]")
        valid_data = self.check_anomaly(staging_data)
        
        self.logger.info("Đang mã hóa Hashing...[cite: 4]")
        current_hash_list = self.generate_hash(valid_data)
        
        self.logger.info("Đang cập nhật lịch sử Fact...[cite: 4]")
        self.load_scd2(valid_data, current_hash_list)