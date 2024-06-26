import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_cases = [
    # Valid case
    {
        "ts": "2023-06-15T12:00:00",
        "station_id": f"ST{random.randint(1000, 9999)}",
        "sensor0": f"{(round(random.uniform(-100.000, 100.000), 3)):.3f}",
        "sensor1": f"{(round(random.uniform(-100.000, 100.000), 3)):.3f}",
        "sensor2": f"{(round(random.uniform(-100.000, 100.000), 3)):.3f}",
        "sensor3": f"{(round(random.uniform(-100.000, 100.000), 3)):.3f}"
    },
    # Invalid timestamp format
    {
        "ts": "2023/06/15 12:00:00",
        "station_id": "ST0002",
        "sensor0": "12.345",
        "sensor1": "-45.678",
        "sensor2": "100.0",
        "sensor3": "50.123"
    },
    # Invalid station_id format
    {
        "ts": "2023-06-15T12:00:00",
        "station_id": "INVALID",
        "sensor0": "12.345",
        "sensor1": "-45.678",
        "sensor2": "100.0",
        "sensor3": "50.123"
    },
    # Invalid timestamp value
    {
        "ts": "1970-01-01T00:00:00",
        "station_id": "ST0003",
        "sensor0": "12.345",
        "sensor1": "-45.678",
        "sensor2": "100.0",
        "sensor3": "50.123"
    },
    # Invalid sensor row
    {
        "ts": "2023-06-15T12:00:00",
        "station_id": "ST0004",
        "sensor0": "invalid",
        "sensor1": "-45.67",
        "sensor2": "100.00",
        "sensor3": "50.3"
    },
    # Invalid sensor value (sensor3 out of range)
    {
        "ts": "2023-06-15T12:00:00",
        "station_id": "ST0005",
        "sensor0": "12.345",
        "sensor1": "-45.678",
        "sensor2": "100.000",
        "sensor3": "150.000"
    },
    # Invalid sensor value (sensor2 not convertible to float)
    {
        "ts": "2023-06-15T12:00:00",
        "station_id": "ST0006",
        "sensor0": "12.345",
        "sensor1": "-45.678",
        "sensor2": "invalid",
        "sensor3": "50.123"
    },
]

while True:
    for case in test_cases:
      producer.send("raw_data", value=case)
      time.sleep(1)


#producer.flush()