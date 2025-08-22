"""
Simple configuration for TelemetryLoggerClient
"""

from typing import Optional
from .telemetry_logger_client import TelemetryLoggerClient

# Production defaults
PRODUCTION_CONFIG = {
    'buffer_time_seconds': 3600,  # 1 hour
    'batch_size': 100,
    'max_buffer_size': 10000,
    'compression': True,
    'timeout': 5.0,
    'retry_attempts': 3,
    'retry_delay': 1.0
}

# Testing defaults (shorter times)
TESTING_CONFIG = {
    'buffer_time_seconds': 10,  # 10 seconds
    'batch_size': 5,
    'max_buffer_size': 1000,
    'compression': True,
    'timeout': 5.0,
    'retry_attempts': 2,
    'retry_delay': 0.5
}

def create_logger(
    server_url: str,
    client_id: str,
    production: bool = True,
    **overrides
) -> TelemetryLoggerClient:
    """
    Create a telemetry logger
    
    Args:
        server_url: Your server URL (e.g., "http://100.80.145.59:8080")
        client_id: Unique identifier for this client
        production: Use production settings (True) or testing settings (False)
        **overrides: Override any specific settings
    
    Returns:
        Configured TelemetryLoggerClient
    
    Example:
        # Production with Tailscale HTTPS
        logger = create_logger(
            "https://jc-wsl.tailf2c1dd.ts.net",
            "my-app",
            production=True
        )
        
        # Testing with custom buffer time
        logger = create_logger(
            "https://jc-wsl.tailf2c1dd.ts.net",
            "test-app",
            production=False,
            buffer_time_seconds=5
        )
    """
    config = PRODUCTION_CONFIG.copy() if production else TESTING_CONFIG.copy()
    config['server_url'] = server_url
    config['client_id'] = client_id
    config.update(overrides)
    return TelemetryLoggerClient(**config)