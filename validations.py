
from datetime import datetime
import re

#Validate timestamp format (ts)
def isValidTsFormat(ts):
    try:
        datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False

#Validate station_id  
def isValidStation_id(station_id):
    pattern = r'^ST\d{4}$'
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
   if value < -100.0 or value > 100.0:
      return False
   return True
  

