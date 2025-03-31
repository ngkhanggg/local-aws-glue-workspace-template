# ========================= Imports =========================

import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import col, concat_ws, sha2

from awsglue.context import GlueContext

# ========================= Logger =========================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ========================= Spark & Glue =========================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# ========================= Start =========================

logger.info('Start ETL job')

source_data = glue_context.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths': ['s3://ngkhang-asia-data-lakehouse-landing/bank_customers/bank_customers_chunk_1.csv']
    },
    format='csv',
    format_options={
        'separator': ',',
        'withHeader': True
    }
).toDF()

dest_data = glue_context.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths': ['s3://ngkhang-asia-data-lakehouse-landing/bank_customers/bank_customers_chunk_2.csv']
    },
    format='csv',
    format_options={
        'separator': ',',
        'withHeader': True
    }
).toDF()

primary_key_columns = ['RowNumber', 'CustomerID']
hash_key_columns = ['Surname', 'CreditScore', 'Geography', 'Gender', 'Age', 'Tenure', 'Balance', 'NumOfProducts', 'HasCrCard', 'IsActiveMember', 'EstimatedSalary', 'Exited']

hashed_source_data = source_data \
    .withColumn('primary_key', sha2(concat_ws('', *[col(c) for c in primary_key_columns]), 256)) \
    .withColumn('hash_key', sha2(concat_ws('', *[col(c) for c in hash_key_columns]), 256)) \
    .select(['primary_key'] + primary_key_columns + ['hash_key'])

hashed_dest_data = dest_data \
    .withColumn('primary_key', sha2(concat_ws('', *[col(c) for c in primary_key_columns]), 256)) \
    .withColumn('hash_key', sha2(concat_ws('', *[col(c) for c in hash_key_columns]), 256)) \
    .select(['primary_key'] + primary_key_columns + ['hash_key'])

# Find records whose CustomerID is in hashed_source_data but not in hashed_dest_data
missing_records = hashed_source_data \
    .join(other=hashed_dest_data, on='primary_key', how='left_anti') \
    .orderBy('RowNumber')
missing_records.printSchema()
missing_records.show()

# Find records whose CustomerID is in hashed_source_data and in hashed_dest_data, but with different values
invalid_records = hashed_source_data.alias('source') \
    .join(other=hashed_dest_data.alias('dest'), on='primary_key', how='inner') \
    .filter(col('source.hash_key') != col('dest.hash_key')) \
    .select(
        col('primary_key'),
        col('source.hash_key').alias('source_hash_key'),
        col('dest.hash_key').alias('dest_hash_key')
    )
invalid_records.printSchema()
invalid_records.show()

logger.info('End ETL job')
