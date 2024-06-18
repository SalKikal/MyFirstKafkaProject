from kafka import KafkaConsumer, KafkaProducer
import json
import time
from enums import ValidationMessage
from validations import validate_data

# Kafka configuration
bootstrap_servers = ['localhost:9094']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='sensor_consumer'
)

consumer.subscribe(topics=['raw_data'])

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

def sendToMonitor(validation_message, station_id):
  
  if(validation_message != ValidationMessage.NONE):
    message = {
      "err_type": validation_message,
      "ts": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),  # Add current datetime
      "sensor_id": None,
      "station_id": station_id
    }
    producer.send('monitoring', value=message)

def sendToValidData(data):
  producer.send('valid_data', value=data)

def handleMessage(msg):

  data = getSensorData(msg)

  if data is None:
    return

  # Validate data and send to appropriate topic
  is_valid, validation_message = validate_data(data)

  if is_valid:
    # Send data to valid_data topic
    print(data)
    sendToValidData(data)
  else:
    # Send validation message to monitoring topic
    print(validation_message)
    sendToMonitor(validation_message, data["station_id"])

while True:
  msg = consumer.poll(timeout_ms=500)
  handleMessage(msg)
  consumer.commit()

# Block until all async messages are sent
producer.flush()
