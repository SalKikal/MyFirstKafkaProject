from kafka import KafkaProducer
import json
import time
import random
from enum import Enum

# Validation messages for monitoring topic
class ValidationMessage(Enum):
    TIMESTAMP_VALUE_ERROR = "TimestampValueError"
    NOT_CONVERTIBLE_TO_FLOAT = "NotConvertibleToFloat"
    INVALID_VALUE = "InvalidValue"
    INVALID_ROW = "InvalidRow"

# Kafka configuration
bootstrap_servers = ['localhost:9092']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Temporary data structure for test purpose
data = {
    "ts": "2000-01-01T00:00:00",  
    "station_id": "station_1",
    "sensor0": "Nan",
    "sensor1": "NaN",
    "sensor2": 100,
    "sensor3": 150
}

# Validation rules
def validate_data(data):
    # Rule 1: Ignore data with timestamp value "1970-01-01T00:00:00"
    if data["ts"] == "1970-01-01T00:00:00":
        return False, ValidationMessage.TIMESTAMP_VALUE_ERROR.value
    
    # Rule 4: If all sensor values are Nans, then set the row as invalid
    if ((data["sensor0"] == "Nan") and (data["sensor1"] == "Nan") and (data["sensor2"] == "Nan") and (data["sensor3"] == "Nan")):
        return False, ValidationMessage.INVALID_ROW.value
    
    # Rule 2: If any sensor value is Nan, set it as invalid row
    if ((data["sensor0"] == "Nan") or (data["sensor1"] == "Nan") or (data["sensor2"] == "Nan") or (data["sensor3"] == "Nan")):
        return False, ValidationMessage.NOT_CONVERTIBLE_TO_FLOAT.value
    
    # Rule 3: If value is outside [-100.0, 100.0], then set it as invalid
    if any(data[sensor] < -100.0 or data[sensor] > 100.0 for sensor in data if isinstance(data[sensor], float)):
        return False, ValidationMessage.INVALID_VALUE.value
    
    return True, None

# Validate data and send to appropriate topic
is_valid, validation_message = validate_data(data)
if is_valid:
    # Send data to valid_data topic
    producer.send('valid_data', value=data)
else:
    # Send validation message to monitoring topic
    message = {
        "err_type": validation_message,
        "ts": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),  # Add current datetime
        "sensor_id": None,
        "station_id": data["station_id"]
    }
    producer.send('monitoring', value=message)

# Block until all async messages are sent
producer.flush()
