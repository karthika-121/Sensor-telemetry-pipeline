import os
import random
from datetime import datetime, timedelta

os.makedirs("../data_gen", exist_ok=True)
# 1. Generate Sensor Metadata
with open("../data_gen/sensor_metadata.csv", "w") as f:
    f.write("sensor_id,plant_id,machine_type,location \n")
    for i in range(1,101):
        f.write(f"SENSOR{i:03d},plant_No_01,machine_type{i%5},zone_{i%10}\n")
# 2. Generate Skewed Telemetry Data (Injecting Skew: SENSOR_001 and SENSOR_002 generate 60% of traffic)
print("Generating sample industrial telemetry data with intentional skew...")
with open("../data_gen/sensor_telemetry.csv", "w") as f:
    f.write("timestamp,sensor_id,temperature,pressure,vibration,humidity,machine_status \n")
    base_time=datetime.now() - timedelta(days=1)
    for i in range(500000):
#        Skew injection logic
        random_val=random.random()
        if random_val<0.4 :
            s_id="SENSOR_001"
        elif random_val <0.7 :
            s_id="SENSOR_002"
        else :
            s_id=f"SENSOR_{random.randint(3,100):03d}"
        ts=base_time+ timedelta(random.randint(1,86400))
        temp=round(random.uniform(60,140),2)
        pressure=round(random.uniform(10,60),2)
        vibration=round(random.uniform(0.1,5.0),3)
        humidity = round(random.uniform(20.0, 80.0), 2)
        status='ok' if vibration >5.0 else "warning"
        f.write(f"{ts},{s_id},{temp},{pressure},{vibration},{humidity},{status}\n")
print("Mock industrial dataset generated successfully inside 'data_gen/' folder!")























