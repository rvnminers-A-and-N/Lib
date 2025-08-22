#!/usr/bin/env python3
import json
import gzip
import time
import threading
import queue
import logging
import requests
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, asdict
import uuid
import atexit
from collections import deque

logger = logging.getLogger(__name__)

@dataclass
class LogEntry:
    timestamp: float
    level: str
    message: str
    data: Optional[Dict[str, Any]] = None
    
    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v is not None}

class TelemetryLoggerClient:
    def __init__(
        self,
        server_url: str,
        client_id: Optional[str] = None,
        buffer_time_seconds: int = 3600,  # 1 hour
        max_buffer_size: int = 10000,
        batch_size: int = 100,
        compression: bool = True,
        timeout: float = 5.0,
        retry_attempts: int = 3,
        retry_delay: float = 1.0
    ):
        self.server_url = server_url.rstrip('/')
        self.client_id = client_id or str(uuid.uuid4())
        self.buffer_time_seconds = buffer_time_seconds
        self.max_buffer_size = max_buffer_size
        self.batch_size = batch_size
        self.compression = compression
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        self._buffer = deque(maxlen=max_buffer_size)
        self._buffer_lock = threading.Lock()
        self._last_flush_time = time.time()
        
        self._transmission_queue = queue.Queue()
        self._shutdown_event = threading.Event()
        
        self._transmission_thread = threading.Thread(
            target=self._transmission_worker,
            daemon=True,
            name=f"TelemetryLogger-{self.client_id[:8]}"
        )
        self._transmission_thread.start()
        
        self._flush_timer = None
        self._schedule_flush()
        
        atexit.register(self.shutdown)
        
        logger.info(f"TelemetryLoggerClient initialized with ID: {self.client_id}")
    
    def log(self, level: str, message: str, data: Optional[Dict[str, Any]] = None):
        try:
            entry = LogEntry(
                timestamp=time.time(),
                level=level.upper(),
                message=message,
                data=data
            )
            
            with self._buffer_lock:
                self._buffer.append(entry)
                
                if len(self._buffer) >= self.batch_size:
                    self._trigger_flush()
            
        except Exception as e:
            logger.error(f"Error adding log entry: {e}")
    
    def debug(self, message: str, data: Optional[Dict[str, Any]] = None):
        self.log("DEBUG", message, data)
    
    def info(self, message: str, data: Optional[Dict[str, Any]] = None):
        self.log("INFO", message, data)
    
    def warning(self, message: str, data: Optional[Dict[str, Any]] = None):
        self.log("WARNING", message, data)
    
    def error(self, message: str, data: Optional[Dict[str, Any]] = None):
        self.log("ERROR", message, data)
    
    def critical(self, message: str, data: Optional[Dict[str, Any]] = None):
        self.log("CRITICAL", message, data)
    
    def _schedule_flush(self):
        if not self._shutdown_event.is_set():
            self._flush_timer = threading.Timer(
                self.buffer_time_seconds,
                self._scheduled_flush
            )
            self._flush_timer.daemon = True
            self._flush_timer.start()
    
    def _scheduled_flush(self):
        self._trigger_flush()
        self._schedule_flush()
    
    def _trigger_flush(self):
        with self._buffer_lock:
            if not self._buffer:
                return
            
            batch = list(self._buffer)
            self._buffer.clear()
            self._last_flush_time = time.time()
        
        try:
            self._transmission_queue.put_nowait(batch)
        except queue.Full:
            logger.warning(f"Transmission queue full, dropping {len(batch)} log entries")
    
    def _transmission_worker(self):
        while not self._shutdown_event.is_set():
            try:
                batch = self._transmission_queue.get(timeout=1.0)
                
                if batch is None:  # Shutdown signal
                    break
                
                self._transmit_batch(batch)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in transmission worker: {e}")
    
    def _transmit_batch(self, batch: List[LogEntry]):
        if not batch:
            return
        
        payload = {
            'logs': [entry.to_dict() for entry in batch]
        }
        
        headers = {
            'Content-Type': 'application/json',
            'X-Client-ID': self.client_id
        }
        
        data = json.dumps(payload).encode('utf-8')
        
        if self.compression:
            data = gzip.compress(data)
            headers['Content-Encoding'] = 'gzip'
        
        url = f"{self.server_url}/logs"
        
        for attempt in range(self.retry_attempts):
            try:
                response = requests.post(
                    url,
                    data=data,
                    headers=headers,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    logger.debug(f"Successfully transmitted {len(batch)} log entries")
                    return
                else:
                    logger.warning(
                        f"Server returned status {response.status_code}: {response.text}"
                    )
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}/{self.retry_attempts}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error on attempt {attempt + 1}/{self.retry_attempts}")
            except Exception as e:
                logger.error(f"Unexpected error transmitting logs: {e}")
                break
            
            if attempt < self.retry_attempts - 1:
                time.sleep(self.retry_delay * (attempt + 1))
        
        logger.error(f"Failed to transmit {len(batch)} log entries after {self.retry_attempts} attempts")
    
    def flush(self):
        self._trigger_flush()
    
    def shutdown(self, timeout: float = 5.0):
        if self._shutdown_event.is_set():
            return
        
        logger.info("Shutting down TelemetryLoggerClient")
        
        self._shutdown_event.set()
        
        if self._flush_timer:
            self._flush_timer.cancel()
        
        self.flush()
        
        self._transmission_queue.put(None)
        
        self._transmission_thread.join(timeout=timeout)
        
        remaining = self._transmission_queue.qsize()
        if remaining > 0:
            logger.warning(f"Shutdown with {remaining} batches still in queue")
    
    def get_stats(self) -> Dict[str, Any]:
        with self._buffer_lock:
            buffer_size = len(self._buffer)
        
        return {
            'client_id': self.client_id,
            'buffer_size': buffer_size,
            'queue_size': self._transmission_queue.qsize(),
            'last_flush_time': self._last_flush_time,
            'time_since_flush': time.time() - self._last_flush_time
        }