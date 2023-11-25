import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

sys.path.append('/home/alaa-haggag/Projects/AirflowProject/')
from DimProduct_insert_update import join_and_detect_new_or_changed_rows

def upload_csv_to_s3():
    s3_hook = S3Hook(aws_conn_id='your_s3_conn_id')
    bucket_name = 'all-products-staging-bucket'
    file_name = '/home/alaa-haggag/Projects/AirflowProject/data/shoes.csv'
    s3_hook.load_file(
        filename=file_name,
        key=file_name,
        bucket_name=bucket_name,
        replace=True
    )

def check_ids_to_update(**context):
    ids_to_update = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if ids_to_update == '':
        return 'check_rows_to_insert'
    else:
        return 'snowflake_update_task'

def check_rows_to_insert(**context):
    rows_to_insert = context['ti'].xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert")
    if rows_to_insert is None:
        return 'skip_snowflake_insert_task'
    else:
        return 'snowflake_insert_task'

def INSERT_INTO_DWH_PRODUCT_DIM(rows_to_insert, batch_size=500):
    print("rows_to_insert:", rows_to_insert)

    # Check if rows_to_insert is empty
    if not rows_to_insert:
        raise ValueError("No rows to insert.")

    # Split rows into batches
    batches = [rows_to_insert[i:i + batch_size] for i in range(0, len(rows_to_insert), batch_size)]

    # Generate INSERT statements for each batch
    sql_statements = []
    for batch in batches:
        # Create a list of formatted values
        formatted_values = [
            f"({', '.join(map(repr, row))})"  # Use repr to ensure proper string formatting
            for row in batch
        ]

        # Join the formatted values
        values = ', '.join(formatted_values)

    sql = f"""
    INSERT INTO iti_airflow.dimensions.DimProduct (id, category, subcategory, likes_count, discount, raw_price, category_avg_price, effective_start_date, effective_end_date, active)
    VALUES {rows_to_insert};
    """
    sql_statements.append(sql)
    return sql_statements

def UPDATE_DWH_PRODUCT_DIM(ids_to_update):
    print("ids_to_update", ids_to_update)
    sql = f"""
    UPDATE iti_airflow.dimensions.DimProduct
    SET effective_end_date = '{datetime.now().date().strftime("%Y-%m-%d")}',
    active = FALSE
    WHERE id IN ({ids_to_update}) AND active=TRUE;
    """
    return sql

# Start the Airflow DAG
with DAG("ETL_Dag", start_date=datetime(2023, 11, 25), catchup=False, schedule='@hourly') as Dag:
    extract_accessories = SqlToS3Operator(
        task_id="extract_accessories",
        sql_conn_id="products-conn",
        aws_conn_id="products-s3-conn",
        query="SELECT * FROM accessories",
        s3_bucket="all-products-staging-bucket",
        s3_key="accessories.csv",
        replace=True
    )

    extract_jewelry = SqlToS3Operator(
        task_id="extract_jewelry",
        sql_conn_id="products-conn",
        aws_conn_id="products-s3-conn",
        query="SELECT * FROM jewelry",
        s3_bucket="all-products-staging-bucket",
        s3_key="jewelry.csv",
        replace=True
    )

    upload_shoes_csv_to_s3 = PythonOperator(
        task_id='upload_csv_to_s3',
        python_callable=upload_csv_to_s3,
    )

    join_and_detect_task = PythonOperator(
        task_id='join_and_detect_task',
        python_callable=join_and_detect_new_or_changed_rows,
    )

    snowflake_insert_task = SnowflakeOperator(
        task_id='snowflake_insert_task',
        sql=INSERT_INTO_DWH_PRODUCT_DIM('{{ ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="rows_to_insert") }}'),
        snowflake_conn_id="snow-airflow-conn",
        trigger_rule="none_failed"
    )

    snowflake_update_task = SnowflakeOperator(
        task_id='snowflake_update_task',
        sql=UPDATE_DWH_PRODUCT_DIM('{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")}}'),
        snowflake_conn_id="snow-airflow-conn",
    )

    check_ids_to_update_task = BranchPythonOperator(
        task_id='check_ids_to_update',
        python_callable=check_ids_to_update,
        provide_context=True
    )

    check_rows_to_insert_task = BranchPythonOperator(
        task_id='check_rows_to_insert',
        python_callable=check_rows_to_insert,
        provide_context=True
    )

    [extract_accessories, extract_jewelry, upload_shoes_csv_to_s3] >> join_and_detect_task >> check_ids_to_update_task >> [snowflake_update_task, check_rows_to_insert_task]
    check_rows_to_insert_task >> snowflake_insert_task
    snowflake_update_task >> snowflake_insert_task
