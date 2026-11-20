
from datetime import datetime, timedelta
import os
import random
import numpy as np

os.environ["JAVA_HOME"] = r""  # <----- 🔴JAVA PATH🔴
os.environ["HADOOP_HOME"] = r"C:\hadoop"

from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
#from pyspark.sql.types import *
spark = SparkSession.builder.appName("SensorMetadataGeneration").getOrCreate()
# 1. Initialize Spark Session
def generate_mockdata(out_dir="../data_gen"):
    os.makedirs("../data_gen", exist_ok=True)

    # Define reference lists
    sensor_ids = [f"SENSOR_{i:03d}" for i in range(1, 101)]
    equipment_ids = [f"EQ_{i:03d}" for i in range(1, 51)]
    plant_ids = [f"PLANT_{i:02d}" for i in range(1, 6)]
    locations = ["Building_A", "Building_B", "Warehouse_1", "Assembly_Line_2", "Outdoor_Yard"]
    machine_types = ["Compressor", "Pump", "Turbine", "Conveyor", "Generator"]

    # 1. Generate Sensor Metadata using PySpark
    data = []
    for _ in range(1, 101):
        data.append(
            {
                "sensor_id": random.choice(sensor_ids),
                "equipment_id": random.choice(equipment_ids),
                "plant_id": random.choice(plant_ids),
                "machine_type": random.choice(machine_types),
                "location": random.choice(locations),
            }
        )

    df = spark.createDataFrame(data)
    df.show()

    print("Sensor metadata generated successfully!")

    # 2. Generate Skewed Telemetry Data (Injecting Skew: SENSOR_001 and SENSOR_002 generate 60% of traffic)
    print("Generating sample industrial telemetry data with intentional skew...")
    with open("../data_gen/sensor_telemetry.csv", "w") as f:
        f.write("timestamp,sensor_id,equipment_id,temperature,pressure,vibration,humidity,machine_status\n")
        start_time = datetime(2026, 1, 1)

        for _ in range(100000):
            random_val = random.random()
            if random_val < 0.4:
                sensor_id = "SENSOR_001"
            elif random_val < 0.7:
                sensor_id = "SENSOR_002"
            else:
                sensor_id = random.choice(sensor_ids)

            equipment_id = random.choice(equipment_ids)

            # Fixed timestamp calculation (seconds in a year: 365 * 24 * 60 * 60)
            timestamps = start_time + timedelta(seconds=random.randint(0, 365 * 24 * 60 * 60))

            temperature = round(float(np.random.normal(80, 15)), 2)
            pressure = round(float(np.random.normal(35, 8)), 2)
            vibration = round(float(np.random.normal(2.0, 0.8)), 3)
            humidity = round(float(np.random.uniform(20, 80)), 2)

            # Inject anomalies
            if random.random() < 0.03:
                temperature = round(float(random.uniform(150, 200)), 2)
            if random.random() < 0.02:
                vibration = round(float(random.uniform(7, 12)), 3)

            # Inject nulls
            if random.random() < 0.01:
                humidity = ""

            # Machine status
            if temperature > 140 or vibration > 7:
                machine_status = "critical"
            elif temperature > 100 or vibration > 4:
                machine_status = "warning"
            else:
                machine_status = "normal"

            # Write record to CSV file
            f.write(f"{timestamps},{sensor_id},{equipment_id},{temperature},{pressure},{vibration},{humidity},{machine_status}\n")

    print("Mock industrial dataset generated successfully inside 'data_gen/' folder!")
