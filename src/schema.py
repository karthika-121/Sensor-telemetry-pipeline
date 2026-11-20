from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, DoubleType, StringType
telemetry_schema= StructType ([
    StructField("timestamp",StringType(),True),
    StructField("sensor_id",StringType(),True),
    StructField("equipment_id",StringType(),True),
    StructField("temperature",DoubleType(),True),
    StructField("pressure",DoubleType(),True),
    StructField("vibration",DoubleType(),True),
    StructField("humidity",DoubleType(),True),
    StructField("machine_status",StringType(),True),

])


