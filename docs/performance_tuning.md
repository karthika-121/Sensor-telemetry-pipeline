# Spark Performance Tuning

## Baseline

- Dataset: 172,800 sensor records.
- Spark mode: local[*].
- Input format: CSV.
- Output format: Parquet.
- Main join key: sensor/machine identifier.
- Known issue: high-frequency sensors can create data skew.

## Skew Detection

- Checked record counts by sensor/machine key.
- Reviewed Spark UI task duration and shuffle metrics.
- Identified long-running tasks and spill behaviour.
- Used explain plans to inspect join strategy.

## Techniques Applied

- Explicit schemas.
- Broadcast join for small metadata.
- AQE skew-join optimisation.
- Salting for extreme hot keys.
- Pre-aggregation before joins.
- Partitioned Parquet output.
- Avoided unnecessary shuffles and UDFs.

## Results

| Metric | Before | After | Notes |
|---|---:|---:|---|
| Job duration | Measured value | Measured value | Local benchmark |
| Longest task duration | Measured value | Measured value | Spark UI |
| Shuffle spill | Measured value | Measured value | Spark UI |

All benchmarks must be reproducible with the commands in this repository.