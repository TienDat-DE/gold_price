import time
from functools import wraps

def retry_request(max_retries=3, delay=5):
    """
    Decorator tự động thử lại request nếu mạng bị chập chờn (Timeout, 50x)
    - max_retries: Số lần thử lại tối đa
    - delay: Thời gian nghỉ (giây) giữa các lần thử
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        self.logger.error(f"Đã thử {max_retries} lần nhưng vẫn thất bại: {e}")
                        raise e # Ném lỗi để BaseJob tóm được và ghi vào error_log
                    
                    self.logger.warning(f"Lỗi mạng: {e}. Đang thử lại lần {attempt}/{max_retries} sau {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator