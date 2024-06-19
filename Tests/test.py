import unittest
import validations
from enums import ValidationMessage

class TestValidations(unittest.TestCase):

    def test_isValidTsFormat(self):
        self.assertTrue(validations.isValidTsFormat("2023-06-15T12:00:00"))
        self.assertFalse(validations.isValidTsFormat("2023/06/15 12:00:00"))

    def test_isValidStation_id(self):
        self.assertTrue(validations.isValidStation_id("ST0001"))
        self.assertFalse(validations.isValidStation_id("ST0000"))
        self.assertFalse(validations.isValidStation_id("INVALID"))

    def test_isValidSensorValue(self):
        self.assertTrue(validations.isValidSensorValue("12.345"))
        self.assertFalse(validations.isValidSensorValue("invalid"))
        self.assertFalse(validations.isValidSensorValue("100.0"))
        self.assertFalse(validations.isValidSensorValue("100.10"))
        self.assertFalse(validations.isValidSensorValue("50.12"))

    def test_isValidTsValue(self):
        self.assertTrue(validations.isValidTsValue("2023-06-15T12:00:00"))
        self.assertFalse(validations.isValidTsValue("1970-01-01T00:00:00"))

    def test_isValidSensorsRange(self):
        self.assertTrue(validations.isValidSensorsRange(50.0))
        self.assertFalse(validations.isValidSensorsRange(150.0))
        self.assertTrue(validations.isValidSensorsRange(-50.0))
        self.assertFalse(validations.isValidSensorsRange(-150.0))

    def test_valid_case(self):
        data = {
            "ts": "2023-06-15T12:00:00",
            "station_id": "ST0001",
            "sensor0": "12.345",
            "sensor1": "-45.678",
            "sensor2": "100.000",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertTrue(valid)
        self.assertIsNone(message)

    def test_invalid_timestamp_format(self):
        data = {
            "ts": "2023/06/15 12:00:00",
            "station_id": "ST0002",
            "sensor0": "12.345",
            "sensor1": "-45.678",
            "sensor2": "100.0",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.TIMESTAMP_FORMAT_ERROR.value)

    def test_invalid_station_id(self):
        data = {
            "ts": "2023-06-15T12:00:00",
            "station_id": "INVALID",
            "sensor0": "12.345",
            "sensor1": "-45.678",
            "sensor2": "100.0",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.STATION_ID.value)

    def test_invalid_timestamp_value(self):
        data = {
            "ts": "1970-01-01T00:00:00",
            "station_id": "ST0003",
            "sensor0": "12.345",
            "sensor1": "-45.678",
            "sensor2": "100.0",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.TIMESTAMP_VALUE_ERROR.value)

    def test_invalid_sensor_value(self):
        data = {
            "ts": "2023-06-15T12:00:00",
            "station_id": "ST0004",
            "sensor0": "invalid",
            "sensor1": "-45.678",
            "sensor2": "100.000",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.NOT_CONVERTIBLE_TO_FLOAT.value)

    def test_invalid_sensor_range(self):
        data = {
            "ts": "2023-06-15T12:00:00",
            "station_id": "ST0005",
            "sensor0": "12.345",
            "sensor1": "-45.678",
            "sensor2": "1000.000",
            "sensor3": "50.123"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.INVALID_VALUE.value)

    def test_multiple_invalid_sensors(self):
        data = {
            "ts": "2023-06-15T12:00:00",
            "station_id": "ST0006",
            "sensor0": "invalid",
            "sensor1": "-45.67",
            "sensor2": "NaN",
            "sensor3": "150.0"
        }
        valid, message = validations.validate_data(data)
        self.assertFalse(valid)
        self.assertEqual(message, ValidationMessage.INVALID_ROW.value)

if __name__ == '__main__':
    unittest.main()