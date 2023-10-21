
import os    
import sys
import uuid
import json

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, asc, desc
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.0,' \
              f'spark.serializer=org.apache.spark.serializer.KryoSerializer,' \
              f'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
              f'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'  \
              f'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
              f'pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
