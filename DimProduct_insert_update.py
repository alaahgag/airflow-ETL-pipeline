import boto3
import psycopg2
import os
import pandas as pd
from datetime import datetime
from snowflake.connector import connect
from airflow.decorators import task

@task(multiple_outputs=True)
def join_and_detect_new_or_changed_rows():
    #### Connect to S3
    AWS_ACCESS_KEY = 'YOUR_AWS_ACCESS_KEY'
    AWS_SECRET_KEY = 'YOUR_AWS_SECRET_KEY'

    os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_KEY

    # Read data from S3
    accessories = pd.read_csv('s3://all-products-staging-bucket/accessories.csv')
    jewelry = pd.read_csv('s3://all-products-staging-bucket/jewelry.csv')
    shoes = pd.read_csv('s3://all-products-staging-bucket/shoes.csv')
    print("Successfully read data from AWS S3")

    # Join all the data
    src_products = pd.concat([accessories, jewelry, shoes], axis=0, ignore_index=True, sort=False)

    # Select the columns that we need
    src_products = src_products[['id', 'category', 'subcategory', 'likes_count', 'discount', 'raw_price']]

    # Define a function for average aggregation
    def avg_agg(df, group_col, df_col, new_col_name):
        # Calculate the average and round to 2 decimals
        df[new_col_name] = df.groupby(group_col)[df_col].transform('mean').round(2)
        return df

    # Apply average aggregation
    src_products = avg_agg(src_products, "category", "raw_price", "category_avg_price")

    #### Connect to DWH
        # Use your Snowflake user credentials to connect
    conn = connect(
            user='YOUR_SNOWFLAKE_USER',
            password='YOUR_SNOWFLAKE_PASSWORD',
            account='YOUR_SNOWFLAKE_ACCOUNT',
            warehouse='YOUR_SNOWFLAKE_WAREHOUSE',
            database= 'iti_airflow',
            schema='DIMENSIONS',
            role='YOUR_SNOWFLAKE_ROLE'
        )
    print("Connected to Snowflake DWH successfully")
    cursor = conn.cursor()

    # Create your SQL command
    sql_query = "select * from iti_airflow.dimensions.DimProduct"

    # Create the cursor object with your SQL command
    cursor.execute(sql_query)
    print("query executed")
    # Convert output to a dataframe
    tgt_pro_df = cursor.fetch_pandas_all()
    print("data fetched")
    cursor.close()
    print("cursor closed")

    # columns renaming
    src_products.columns = ["src_"+col for col in src_products.columns]
    tgt_pro_df.columns = ["tgt_"+col.lower() for col in tgt_pro_df.columns]
    print("columns renamed")

    # Join source & target data and add effective dates and active flag
    src_plus_tgt = pd.merge(src_products, tgt_pro_df, how='left', left_on='src_id', right_on='tgt_id')
    src_plus_tgt['effective_start_date'] = datetime.now().date().strftime("%Y-%m-%d")
    src_plus_tgt['effective_end_date'] = "2999-12-31"
    src_plus_tgt['active'] = True

    # Get new rows only (i.e. rows that don't exist in DWH, all DWH data will be null)
    new_inserts = src_plus_tgt[src_plus_tgt.tgt_id.isna()].copy()

    # Select only source columns and the effective dates and active flag
    cols_to_insert = src_products.columns.tolist() + ['effective_start_date', 'effective_end_date', 'active']

    # Convert result to string
    result_list = new_inserts[cols_to_insert].values.tolist()
    result_tuple = [tuple(row) for row in result_list]
    new_rows_to_insert = str(result_tuple).lstrip('[').rstrip(']')
    print("Found {} new rows".format(len(result_list)))

    # Get changed rows only (i.e. rows that exist in DWH but with different raw_price)
    insert_updates = src_plus_tgt[(src_plus_tgt['src_raw_price'] != src_plus_tgt['tgt_raw_price']) & (~src_plus_tgt.tgt_id.isna())]

    # Convert result to string
    insert_list = insert_updates[cols_to_insert].values.tolist()
    insert_tuples = [tuple(row) for row in insert_list]
    changed_rows_to_insert = str(insert_tuples).lstrip('[').rstrip(']')

    # The resulted string will be sent to the next task (SnowflakeOperator) to use in insert query
    if changed_rows_to_insert == '':
        rows_to_insert = new_rows_to_insert
    else:
        rows_to_insert = new_rows_to_insert + ", " + changed_rows_to_insert

    # Get product_ids to upgrade
    ids_to_update = insert_updates.src_id.tolist()
    print("Found {} changed rows".format(len(ids_to_update)))

    # This result will be sent to the next task (SnowflakeOperator) to use in update query
    ids_to_update = str(ids_to_update).lstrip('[').rstrip(']')

    return {"rows_to_insert": rows_to_insert, "ids_to_update": ids_to_update}
