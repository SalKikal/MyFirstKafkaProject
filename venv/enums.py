from enum import Enum

# Validation messages for monitoring topic
class ValidationMessage(Enum):
  TIMESTAMP_VALUE_ERROR = "TimestampValueError"
  NOT_CONVERTIBLE_TO_FLOAT = "NotConvertibleToFloat"
  INVALID_VALUE = "InvalidValue"
  INVALID_ROW = "InvalidRow"
  TIMESTAMP_FORMAT_ERROR = "TimestampFormatError"
  STATION_ID = "StationIdError"
