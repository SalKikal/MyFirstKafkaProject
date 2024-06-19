import json
import math
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from enums import ValidationMessage
import validations 
import constants


producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='sensor_consumer'
)

consumer.subscribe(topics=['test'])

def getSensorData(msg):

  if msg:
    for _, value in msg.items():
      for val in value:
        try:
          json_data = json.loads(val.value.decode('utf-8'))
          return json_data
        except:
          print("Can not convert to json")
          return None
  else:
    return None

#validate everything
def isValid(data):

  if data is None:
    return False
  
  if not validations.isValidTsFormat(data["ts"]):
    sendToMonitor(ValidationMessage.TIMESTAMP_FORMAT_ERROR, data["station_id"])
    return False

  if not validations.isValidStation_id(data["station_id"]):
    sendToMonitor(ValidationMessage.STATION_ID, data["station_id"])
    return False
  
  if not validations.isValidTsValue(data["ts"]):
    return False, ValidationMessage.TIMESTAMP_VALUE_ERROR.value

  if not (validations.isValidSensorValue(data["sensor0"]) and validations.isValidSensorValue(data["sensor1"]) and validations.isValidSensorValue(data["sensor2"]) and validations.isValidSensorValue(data["sensor3"])):
    sendToMonitor(ValidationMessage.INVALID_ROW, data["station_id"])
    return False

  for sensor in constants.sensor_keys:
        value = data[sensor] 
        if not validations.isValidSensorValue(value):
          data[sensor]  = math.nan
          sendToMonitor(ValidationMessage.NOT_CONVERTIBLE_TO_FLOAT, data["station_id"])
        return False

  for sensor in constants.sensor_keys:
        value = data[sensor]
        if validations.isValidSensorsRange(value):
           return False, ValidationMessage.INVALID_VALUE.value  
        return True, None
  
  return True

#send invalid data to minitoring topic
def sendToMonitor(error, station_id):
  message = {
    "err_type": error,
    "ts": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
    "sensor_id": None,
    "station_id": station_id
  }
  producer.send('monitoring', value=message)

def sendToValidData(data):
  producer.send('valid_data', value=data)

def handleMessage(msg):

  data = getSensorData(msg)

  if isValid(data):
    print(data)
    sendToValidData(data)
    consumer.commit()
  else:
    print("invalid data")

while True:
  msg = consumer.poll(timeout_ms=500)
  handleMessage(msg)