import mysql.connector
import logging
import traceback
from datetime import datetime
from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv

load_dotenv()

class BaseJob(ABC):
    def __init__(self, job_name: str, run_id: str, logical_date: str):
        """
        Khởi tạo các biến môi trường và kết nối hệ thống.
        run_id và logical_date sau này sẽ do Airflow tự động truyền vào.
        """
        self._job_name = job_name
        self._run_id = run_id 
        self._logical_date = logical_date
        
        # Sub-task 2.1: Khởi tạo Logger và Database Connector
        self.logger = self._setup_logger()
        self._db = self._connect_db()
        self._cursor = self._db.cursor()
        self._ensure_job_run()

    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(self._job_name)

    def _connect_db(self):
        """Kết nối MySQL sử dụng biến môi trường bảo mật từ file .env"""
        try:
            conn = mysql.connector.connect(
                host=os.getenv("DB_HOST", "localhost"),
                user=os.getenv("DB_USER", "root"),
                password=os.getenv("DB_PASSWORD", "123456"),
                database=os.getenv("DB_NAME", "gold_dwh")
            )
            return conn
        except Exception as e:
            self.logger.error(f"Lỗi kết nối Database: {e}")
            raise

    def _ensure_job_run(self):
        """Đảm bảo run_id từ Airflow đã tồn tại ở bảng cha job_run trước khi ghi log"""
        try:
            self._cursor.execute("SHOW COLUMNS FROM job_run")
            columns_info = self._cursor.fetchall()

            known_defaults = {
                "run_id": self._run_id,
                "logical_date": self._logical_date,
                "data_date": self._logical_date,
                "status": "RUNNING",
                "job_name": self._job_name,
                "dag_id": self._job_name,
            }

            cols, vals = [], []
            for col_name, _col_type, is_null, _key, default_val, extra in columns_info:
                if "auto_increment" in (extra or ""):
                    continue
                if col_name in known_defaults or (is_null == "NO" and default_val is None):
                    cols.append(col_name)
                    vals.append(known_defaults.get(col_name, self._run_id))

            if cols:
                placeholders = ", ".join(["%s"] * len(cols))
                col_list = ", ".join(cols)
                sql = f"INSERT IGNORE INTO job_run ({col_list}) VALUES ({placeholders})"
                self._cursor.execute(sql, tuple(vals))
                self._db.commit()
        except Exception as e:
            self.logger.warning(f"Lưu ý khi khởi tạo job_run: {e}")

    # Sub-task 2.2: Xây dựng phương thức lưu vết (Insert vào pipeline_log và error_log)
    def _log_pipeline(self, step_name: str, status: str, message: str = ""):
        """Ghi nhận tiến độ vào quyển 'sổ trực ban' pipeline_log"""
        try:
            sql = """
                INSERT INTO pipeline_log (run_id, job_name, step_name, status, message)
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (self._run_id, self._job_name, step_name, status, message)
            self._cursor.execute(sql, values)
            self._db.commit()
            self.logger.info(f"[{status}] {step_name} | {message}")
        except Exception as e:
            self.logger.error(f"Không thể ghi log pipeline: {e}")

    def _log_error(self, step_name: str, error_msg: str, stack_trace: str):
        """Ghi chi tiết lỗi sập hệ thống vào error_log để Debug"""
        try:
            sql = """
                INSERT INTO error_log (run_id, job_name, step_name, error_message, stack_trace)
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (self._run_id, self._job_name, step_name, error_msg, stack_trace)
            self._cursor.execute(sql, values)
            self._db.commit()
        except Exception as e:
            self.logger.error(f"Không thể ghi log lỗi: {e}")

    @abstractmethod
    def execute(self):
        """Nơi Class con sẽ viết logic của riêng chúng"""
        pass

    # Sub-task 2.3: Template Method tích hợp Try-Catch & Exception Handling
    def run(self, step_name: str):
        """
        Khung xương cố định: Tự động ghi log Bắt đầu, chạy logic, bắt lỗi và ghi log Kết thúc.
        Class con KHÔNG ĐƯỢC PHÉP ghi đè (override) hàm này.
        """
        self._log_pipeline(step_name, "RUNNING", "Bắt đầu thực thi module...")
        
        try:
            self.execute()
            self._log_pipeline(step_name, "SUCCESS", "Hoàn thành module xuất sắc.")
            
        except Exception as e:
            error_message = str(e)
            stack_trace_str = traceback.format_exc()
            
            self._log_pipeline(step_name, "FAILED", f"Lỗi: {error_message}")
            self._log_error(step_name, error_message, stack_trace_str)
            self.logger.error(f"===== HỆ THỐNG SẬP TẠI {step_name} =====\n{stack_trace_str}")
            raise e
            
        finally:
            if self._db.is_connected():
                self._cursor.close()
                self._db.close()