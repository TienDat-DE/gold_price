from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_DIR = "/Users/tsdeveloper/Desktop/gold_price"
PYTHON_BIN = f"{PROJECT_DIR}/venv/bin/python"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG chạy hàng ngày vào lúc 10:15 (chạy sau Domestic 15 phút)
with DAG(
    dag_id='global_gold_pipeline',
    default_args=default_args,
    description='ETL Pipeline cho Giá vàng XAU và Tỷ giá USD/VND',
    schedule_interval='15 10 * * *',
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['global', 'xau', 'usd', 'elt'],
) as dag:

    # ==========================================
    # LUỒNG 1: GIÁ VÀNG THẾ GIỚI (XAU)
    # ==========================================
    crawl_xau = BashOperator(
        task_id='crawl_xau',
        bash_command=f"""
        cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} {PYTHON_BIN} -c "
from src.crawler.xau_crawler import XAUCrawler
XAUCrawler(run_id='{{{{ run_id }}}}', logical_date='{{{{ ds }}}}').run(step_name='CRAWL_XAU')
        "
        """
    )

    stage_xau = BashOperator(
        task_id='stage_xau',
        bash_command=f"""
        cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} {PYTHON_BIN} -c "
from src.staging.xau_staging import XAUStaging
XAUStaging(run_id='{{{{ run_id }}}}', logical_date='{{{{ ds }}}}').run(step_name='STAGE_XAU')
        "
        """
    )

    # ==========================================
    # LUỒNG 2: TỶ GIÁ NGOẠI TỆ (USD/VND)
    # ==========================================
    crawl_usd = BashOperator(
        task_id='crawl_usd',
        bash_command=f"""
        cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} {PYTHON_BIN} -c "
from src.crawler.vcb_crawler import VCBCrawler
VCBCrawler(run_id='{{{{ run_id }}}}', logical_date='{{{{ ds }}}}').run(step_name='CRAWL_USD')
        "
        """
    )

    stage_usd = BashOperator(
        task_id='stage_usd',
        bash_command=f"""
        cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} {PYTHON_BIN} -c "
from src.staging.vcb_staging import VCBStaging
VCBStaging(run_id='{{{{ run_id }}}}', logical_date='{{{{ ds }}}}').run(step_name='STAGE_USD')
        "
        """
    )

    # ==========================================
    # KHÂU 3: HỘI TỤ & QUY ĐỔI GIÁ (CONVERSION)
    # ==========================================
    convert_global = BashOperator(
        task_id='convert_global_gold',
        bash_command=f"""
        cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR} {PYTHON_BIN} -c "
from src.clean.global_converter import GlobalConverter
GlobalConverter(run_id='{{{{ run_id }}}}', logical_date='{{{{ ds }}}}').run(step_name='CONVERT_GLOBAL')
        "
        """
    )

    # ==========================================
    # THIẾT LẬP CHUỖI PHỤ THUỘC (DEPENDENCIES)
    # ==========================================
    crawl_xau >> stage_xau
    crawl_usd >> stage_usd

    [stage_xau, stage_usd] >> convert_global