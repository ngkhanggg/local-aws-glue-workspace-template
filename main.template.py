# ========================= Imports =========================

import logging
import sys

from pyspark.context import SparkConf, SparkContext

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ========================= Logger =========================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ========================= Args =========================

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'p_account_id', 'p_raw_bucket'])

# ========================= Spark & Glue =========================

account_id = args['p_account_id']
raw_bucket = args['p_raw_bucket']

spark_conf = SparkConf().setAll([
  ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
  ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
  ("spark.sql.catalog.glue_catalog.glue.id", account_id),
  ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
  ("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true"),
  ("spark.sql.catalog.glue_catalog.warehouse", f"s3://{raw_bucket}/warehouse/"),
  ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

  ("spark.sql.adaptive.enabled", "true"),
  ("spark.sql.adaptive.skewJoin.enabled", "true"),
  ("spark.sql.autoBroadcastJoinThreshold", "-1")
])

sc = SparkContext(conf=spark_conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# ========================= Job =========================

job = Job(glue_context)
job.init(args['JOB_NAME'], args)
job.commit()
