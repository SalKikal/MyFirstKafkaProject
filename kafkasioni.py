from kafka import KafkaProducer
import json
import time
from enums import ValidationMessage  # Ensure this is properly defined in your project
import constants  # Ensure this is properly defined in your project
import validations  # Ensure this is properly defined in your project
import math

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
    "station_id": "ST0001",
    "sensor0": 99,
    "sensor1": 89,
    "sensor2": 160,
    "sensor3": 20
}

def validate_data(data):
    if data is None:
        return False, ValidationMessage.NONE.value
    
    if not validations.isValidTsFormat(data["ts"]):
        return False, ValidationMessage.TIMESTAMP_FORMAT_ERROR.value
    
    if not validations.isValidStation_id(data["station_id"]):
        return False, ValidationMessage.STATION_ID.value
    
    if not validations.isValidTsValue(data["ts"]):
        return False, ValidationMessage.TIMESTAMP_VALUE_ERROR.value
    
    if not (validations.isValidSensorValue(data["sensor0"]) and 
            validations.isValidSensorValue(data["sensor1"]) and 
            validations.isValidSensorValue(data["sensor2"]) and 
            validations.isValidSensorValue(data["sensor3"])):
        return False, ValidationMessage.INVALID_ROW.value

    for sensor in constants.sensor_keys:
        value = data[sensor]
        if not validations.isValidSensorValue(value):
            data[sensor] = math.nan
            return False, ValidationMessage.NOT_CONVERTIBLE_TO_FLOAT.value

    for sensor in constants.sensor_keys:
        value = data[sensor]
        if not validations.isValidSensorsRange(value):
            return False, ValidationMessage.INVALID_VALUE.value
    
    return True, None

# Validate data and send to appropriate topic
is_valid, validation_message = validate_data(data)

if is_valid:
    # Send data to valid_data topic
    producer.send('valid_data', value=data)
else:
    # Send validation message to monitoring topic
    if(validation_message != ValidationMessage.NONE):
        message = {
            "err_type": validation_message,
            "ts": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),  # Add current datetime
            "sensor_id": None,
            "station_id": data["station_id"]
        }
        producer.send('monitoring', value=message)

# Block until all async messages are sent
producer.flush()
