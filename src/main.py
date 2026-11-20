
import os
os.environ["JAVA_HOME"] = r""  # <----- 🔴JAVA PATH🔴
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from schema import telemetry_schema
from generate_data import generate_mockdata
from transform import (clean_pipeline,
                       enrich_with_metadata,
                       anomaly_detection,
                       mitigate_skew_and_aggregate)
def inspect_df(df, name, rows=5):
    print("\n" + "=" * 70)
    print(f"DATAFRAME: {name}")
    print("=" * 70)

    print(f"Rows: {df.count()}")
    print("\nSchema:")
    df.printSchema()

    print(f"\nSample {rows} rows:")
    df.show(rows, truncate=False)


def main():
    #1.starting spark Session
    spark= (SparkSession.builder.appName("IndustrialSensorTelemetryPipeline")
            .config("spark.sql.shuffle.partitions",200)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

    #2.generate mockdata
    generate_mockdata()

    #3.read raw data which generated
    print("Reading telemetry data into Spark...")
    df_rawdata=spark.read.schema(telemetry_schema).option("header","true").csv("../data_gen/sensor_telemetry.csv")


    inspect_df(df_rawdata, "RAW TELEMETRY DATA")

    # 4. Apply Pipeline Transformations
    print("Cleaning telemetry data...")
    df_cleaned = clean_pipeline(df_rawdata)

    inspect_df(df_cleaned, "CLEANED DATA")

    # 5. Enrich with metadata
    print("Enriching with sensor metadata...")
    df_enriched_with_broadcast= enrich_with_metadata(df_cleaned,spark)
    df_enriched_with_broadcast.cache()
    df_enriched_with_broadcast.count()

    df_enriched_with_broadcast.select(
                "sensor_id",
                "equipment_id",
                "plant_id",
                "machine_type",
                "location"
        ).show(10, False)
    inspect_df(df_enriched_with_broadcast, "ENRICHED WITH DATA")

    # 6. Detect anomalies
    print("Running anomaly detection...")
    df_anomaly_detected= anomaly_detection(df_enriched_with_broadcast)

    inspect_df(df_anomaly_detected, "ANOMALY DETECTION OUTPUT")

    print("\nAnomaly Summary:")
    df_anomaly_detected.select(
    "sensor_id",
    "temperature",
    "vibration",
    "anomaly_flag"
    ).show(10, truncate=False)

    print("Repartitioning by equipment_id before KPI aggregation...")
    df_anomaly_detected = df_anomaly_detected.repartition(8,"equipment_id")

    # 7. Aggregate KPIs
    print("Handling data skew and calculating hourly KPIs...")
    df_kpis=mitigate_skew_and_aggregate(df_anomaly_detected)
    inspect_df(df_kpis, "HOURLY KPI OUTPUT")

    print("\n KPI Sample Ordered by Equipment:")
    df_kpis.orderBy("equipment_id").show(20, truncate=False)

    # Asset 1: Hourly KPIs partitioned by plant for operational dashboards
    output_path = "../data_gen"
    df_kpis.write.mode("overwrite").partitionBy("plant_id").parquet(output_path)
    ml_feature_path = os.path.join(output_path, "predictive_maintenance_features.parquet")
    print("Pipeline execution completed successfully!")

    # Asset 2: Clean Historical Telemetry for Predictive Maintenance ML Models
    print(f"Writing ML-ready feature store to: {ml_feature_path}")
    df_anomaly_detected.write.mode("overwrite").partitionBy("machine_type").parquet(ml_feature_path)
    df_enriched_with_broadcast.unpersist()
    spark.stop()
if __name__=="__main__":
    main()
