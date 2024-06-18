import json
import math
import re
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
from enums import ValidationMessage


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
  

def isValidTs(ts):
    try:
        datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False

def isValidStation_id(station_id):
    pattern = r'^ST\d{4}$'
    return re.match(pattern, station_id) is not None

def isValidValue(value):
    
  try:
      float(value)
      pattern = r'^[-+]?[0-9]+\.[0-9]{3}$'
      if re.match(pattern, value) is not None:
         return True
      return False
  except (ValueError, TypeError):
      return False

def isValid(data):

  if data is None:
    return False
  
  if not isValidTs(data["ts"]):
    sendToMonitor(ValidationMessage.TIMESTAMP_FORMAT_ERROR, data["station_id"])
    return False

  if not isValidStation_id(data["station_id"]):
    sendToMonitor(ValidationMessage.NOT_CONVERTIBLE_TO_FLOAT, data["station_id"])
    return False
  
  if not (isValidValue(data["sensor0"]) and isValidValue(data["sensor1"]) and isValidValue(data["sensor2"]) and isValidValue(data["sensor3"])):
    sendToMonitor(ValidationMessage.INVALID_ROW, data["station_id"])
    return False

  if not isValidValue(data["sensor0"]):
    data["sensor0"] = math.nan
    sendToMonitor(ValidationMessage.INVALID_VALUE, data["station_id"])
    return False
  
  if not isValidValue(data["sensor1"]):
    data["sensor1"] = math.nan
    sendToMonitor(ValidationMessage.INVALID_VALUE, data["station_id"])
    return False
  
  if not isValidValue(data["sensor2"]):
    data["sensor2"] = math.nan
    sendToMonitor(ValidationMessage.INVALID_VALUE, data["station_id"])
    return False

  if not isValidValue(data["sensor3"]):
    data["sensor3"] = math.nan
    sendToMonitor(ValidationMessage.INVALID_VALUE, data["station_id"])
    return False

  return True

def sendToMonitor(error, station_id):

  message = {
    "err_type": error,
    "ts": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
    "sensor_id": None,
    "station_id": station_id
  }

  producer.send('test', value=message)

def sendToCleanData(data):
  producer.send('test', value=data)

def handleMessage(msg):

  data = getSensorData(msg)

  if isValid(data):
    print(data)
    sendToCleanData(data)
    consumer.commit()
  else:
    print("invalid data")

while True:
  msg = consumer.poll(timeout_ms=500)
  handleMessage(msg)