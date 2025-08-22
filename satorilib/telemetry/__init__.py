from .telemetry_logger_client import TelemetryLoggerClient, LogEntry
from .simple_config import create_logger, PRODUCTION_CONFIG, TESTING_CONFIG

__version__ = "1.0.0"
__all__ = [
    "TelemetryLoggerClient", 
    "LogEntry", 
    "create_logger", 
    "PRODUCTION_CONFIG", 
    "TESTING_CONFIG"
]