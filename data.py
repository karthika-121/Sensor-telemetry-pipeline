
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("sensor_id", StringType(), False),
    StructField("site_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("temperature_c", DoubleType(), True),
    StructField("pressure_bar", DoubleType(), True),
    StructField("vibration_mm_s", DoubleType(), True),
    StructField("status", StringType(), True),
])

def read_sensor_events(spark: SparkSession, path: str):
    return (
        spark.read
        .schema(SENSOR_SCHEMA)
        .json(path)
    )