from kafka import KafkaProducer
import json
import time
from enums import ValidationMessage

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
    "sensor0": 99,
    "sensor1": 89,
    "sensor2": 160,
    "sensor3": 20
}

sensor_keys = ['sensor0', 'sensor1', 'sensor2', 'sensor3']

# Validation rules
def validate_data(data):
    # Rule 1: Ignore data with timestamp value "1970-01-01T00:00:00"
    if data["ts"] == "1970-01-01T00:00:00":
        return False, ValidationMessage.TIMESTAMP_VALUE_ERROR.value
    
    for sensor in sensor_keys:
        value = data[sensor]
        if value < -100.0 or value > 100.0:
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
