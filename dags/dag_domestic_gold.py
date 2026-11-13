from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG chạy hàng ngày vào lúc 10:00 (Giờ vàng cập nhật giá)
with DAG(
    dag_id='domestic_gold_pipeline',
    default_args=default_args,
    description='ETL Pipeline cho Giá vàng trong nước (SJC & BTMC)',
    schedule_interval='0 10 * * *',
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['domestic', 'gold', 'elt'],
) as dag:

    # ==========================================
    # KHÂU 1: CRAWLER (EXTRACTION)
    # ==========================================
    crawl_sjc = BashOperator(
        task_id='crawl_sjc',
        bash_command="""
        python -c "
                from src.crawler.sjc_crawler import SJCCrawler
                SJCCrawler(run_id='{{ run_id }}', logical_date='{{ ds }}').run(step_name='CRAWL_SJC')
        "
        """
    )

    crawl_btmc = BashOperator(
        task_id='crawl_btmc',
        bash_command="""
        python -c "
                from src.crawler.btmc_crawler import BTMCCrawler
                BTMCCrawler(run_id='{{ run_id }}', logical_date='{{ ds }}').run(step_name='CRAWL_BTMC')
        "
        """
    )

    # ==========================================
    # KHÂU 2: STAGING (TRANSFORMATION)
    # ==========================================
    stage_sjc = BashOperator(
        task_id='stage_sjc',
        bash_command="""
        python -c "
                from src.staging.sjc_staging import SJCStaging
                SJCStaging(run_id='{{ run_id }}', logical_date='{{ ds }}').run(step_name='STAGE_SJC')
        "
        """
    )

    stage_btmc = BashOperator(
        task_id='stage_btmc',
        bash_command="""
        python -c "
                from src.staging.btmc_staging import BTMCStaging
                BTMCStaging(run_id='{{ run_id }}', logical_date='{{ ds }}').run(step_name='STAGE_BTMC')
        "
        """
    )

    # ==========================================
    # KHÂU 3: CLEAN & LOAD DWH (BUSINESS LOGIC)
    # ==========================================
    clean_and_load = BashOperator(
        task_id='clean_and_load_gold',
        bash_command="""
        python -c "
                from src.clean.gold_cleaner import GoldCleaner
                GoldCleaner(run_id='{{ run_id }}', logical_date='{{ ds }}').run(step_name='CLEAN_GOLD')
        "
        """
    )

    # ==========================================
    # THIẾT LẬP CHUỖI PHỤ THUỘC (DEPENDENCIES)
    # ==========================================

    
    crawl_sjc >> stage_sjc
    crawl_btmc >> stage_btmc
    [stage_sjc, stage_btmc] >> clean_and_load