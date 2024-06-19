from datetime import datetime
import re
from enums import ValidationMessage
import constants
import validations
import math

#Validate timestamp format (ts)
def isValidTsFormat(ts):
    try:
        datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False

#Validate station_id  
def isValidStation_id(station_id):
    pattern = r'^ST(?!0000)\d{4}$'
    return re.match(pattern, station_id) is not None

#Validate sensors values (if its possible to convert into float)
def isValidSensorValue(value):  
  try:
      float(value)
      pattern = r'^[-+]?[0-9]+\.[0-9]{3}$'
      if re.match(pattern, value) is not None:
         return True
      return False
  except (ValueError, TypeError):
      return False
  
#Validate timestamp value (ts)
def isValidTsValue(value):  
   if value == "1970-01-01T00:00:00": 
     return False
   return True

#Validate sensors' range
def isValidSensorsRange(value):
   if float(value) < -100.0 or float(value) > 100.0:
      return False
   return True
  

def validate_data(data):

    if not validations.isValidTsFormat(data["ts"]):
        return False, ValidationMessage.TIMESTAMP_FORMAT_ERROR.value
    
    if not validations.isValidStation_id(data["station_id"]):
        return False, ValidationMessage.STATION_ID.value
    
    if not validations.isValidTsValue(data["ts"]):
        return False, ValidationMessage.TIMESTAMP_VALUE_ERROR.value
    
    if not (validations.isValidSensorValue(data["sensor0"]) or 
            validations.isValidSensorValue(data["sensor1"]) or 
            validations.isValidSensorValue(data["sensor2"]) or 
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