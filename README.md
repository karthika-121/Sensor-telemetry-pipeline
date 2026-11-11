## Business Problem

Manufacturing plants generate large volumes of sensor telemetry from equipment
such as motors, pumps, compressors, and production machines. Engineers need a
reliable way to identify abnormal equipment behavior, monitor asset health,
and generate operational KPIs before failures affect production.

In this PySpark data engineering pipeline project that:

- Ingests raw industrial sensor telemetry.
- Validates, cleans, deduplicates, and standardizes events.
- Enriches readings with sensor and equipment metadata.
- Detects temperature and vibration anomalies.
- Builds hourly equipment-health KPIs.
- Handles data skew caused by high-frequency sensors.
- Writes curated data in Parquet for analytics and dashboards.
## Business Value

- Detect abnormal equipment behavior earlier.
- Reduce manual investigation of sensor data.
- Produce reliable hourly operational KPIs.
- Demonstrate scalable Spark patterns for large telemetry workloads.
- Provide a foundation for predictive-maintenance analytics.

