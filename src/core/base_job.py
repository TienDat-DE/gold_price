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
                password=os.getenv("DB_PASSWORD", "123456"), # Sẽ lấy password thật từ .env
                database=os.getenv("DB_NAME", "gold_dwh")
            )
            return conn
        except Exception as e:
            self.logger.error(f"Lỗi kết nối Database: {e}")
            raise

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
            # Gọi logic nghiệp vụ (Crawl, Staging, Clean...)
            self.execute()
            
            # Nếu code chạy mượt mà đến đây, ghi log Success
            self._log_pipeline(step_name, "SUCCESS", "Hoàn thành module xuất sắc.")
            
        except Exception as e:
            # Tự động tóm (capture) lỗi và Stack Trace
            error_message = str(e)
            stack_trace_str = traceback.format_exc()
            
            # Ghi vào DB để sau này lên Dashboard soi lỗi
            self._log_pipeline(step_name, "FAILED", f"Lỗi: {error_message}")
            self._log_error(step_name, error_message, stack_trace_str)
            self.logger.error(f"===== HỆ THỐNG SẬP TẠI {step_name} =====\n{stack_trace_str}")
            
            # Ném lỗi ra ngoài để Airflow biết mà bôi ĐỎ task
            raise e
            
        finally:
            # Luôn luôn đóng kết nối DB khi chạy xong để tránh tràn RAM/kẹt Session
            if self._db.is_connected():
                self._cursor.close()
                self._db.close()