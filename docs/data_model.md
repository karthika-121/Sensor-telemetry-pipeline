# Data Model

## Source: sensor_data.csv

| Column | Type | Description | Example |
|---|---|---|---|
| timestamp | timestamp | Sensor reading time | 2024-01-01 00:00:00 |
| sensor | string | Sensor measurement type | Temperature |
| unit | string | Unit of measure | °C |
| value | double | Measured value | 72.5 |
| machine_state | integer | Running or stopped | 1 |
| shift | string | Operational shift | Shift 1 |
| safe_min | double | Lower safe threshold | 40 |
| safe_max | double | Upper safe threshold | 90 |
| is_anomaly | integer | Anomaly indicator | 0 or 1 |
| fault_type | string | Fault classification | Spike |

## Derived Fields

| Field | Description |
|---|---|
| event_date | Date extracted from timestamp |
| event_hour | Hour extracted from timestamp |
| threshold_breach | Value outside safe_min/safe_max |
| anomaly_flag | Standardised anomaly indicator |
| reading_status | Normal, warning, or critical |