## Architecture

```
Raw Sensor Events (CSV/JSON)
             |
             v
      Bronze Layer
  Raw immutable telemetry
             |
             v
      Silver Layer
  Validated, deduplicated,
  enriched sensor readings
             |
             v
       Gold Layer
  Hourly KPIs, anomaly alerts,
  equipment health metrics
             |
             v
       Parquet Output
```

### Pipeline Flow

1. Read raw sensor events using an explicit schema.
2. Validate mandatory fields and business rules.
3. Remove duplicate events.
4. Enrich readings with sensor metadata.
5. Detect anomalies using thresholds and rolling statistics.
6. Aggregate readings into hourly KPIs.
7. Write curated outputs as partitioned Parquet files.
8. Handle data skew using AQE, salting, broadcast joins, and pre-aggregation.