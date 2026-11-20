# Cleaning, enrichment, anomaly detection & aggregation
##Pipeline Transformations & Anomaly Detection
from audioop import avg
from turtledemo.sorting_animate import randomize
from typing import cast

from pyspark.sql.functions import broadcast, window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions

from pyspark.sql.functions import lit, max as spark_max, col, count, to_timestamp, when, left,rand,avg

def clean_pipeline(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("timestamp", to_timestamp (col("timestamp")))
          .dropna(subset=["timestamp", "sensor_id"])
        .dropDuplicates()
    )
def anomaly_detection(df: DataFrame) -> DataFrame:
    return( df.withColumn("anomaly_flag",
                  when( (col("temperature") > 140) | (col ("vibration") > 7),"CRITICAL")
                  .when( (col("temperature") > 100) | (col ("vibration") > 4),"WARNING")
                  .otherwise("Normal")
                         )
                  )


def enrich_with_metadata(
        df_telemtry: DataFrame,
        spark:SparkSession
) ->DataFrame:
    """Creates mock metadata and performs a broadcast join."""
    df_metadata= spark.createDataFrame(
        [
            {
                "equipment_id": f"EQ_{i:03d}",
                "plant_id" : f"PLANT_{i % 5 + 1:02d}",
                "location" : "Building_A",
                "machine_type" :"compressor" if i % 2 == 0 else "Pump"
            }
            for i in range(1,101)
        ]
    )
    return (df_telemtry.join (broadcast (df_metadata),["equipment_id"],"left"))

def mitigate_skew_and_aggregate(df: DataFrame) -> DataFrame:
    """Applies salting to skewed sensors and computes hourly equipment health KPIs."""
    df_salted=df.withColumn(
        "salt_key",when(col("sensor_id").isin("SENSOR_001","SENSOR_002"),
                        (rand()*5).cast("int")).otherwise(lit(0))
                        )

    return df_salted.groupBy(
        window(col("timestamp"), "1 hour"),
        (col("equipment_id")),
        (col("plant_id")),
        (col("machine_type"))
    ).agg(
        avg("temperature").alias("avg_temperature"),
        spark_max("temperature").alias("max_temperature"),
        count( when((col("anomaly_flag"))=="CRITICAL",1)).alias("critical_count"),
        count('*').alias("total_count")
    )
