"""
Scheduler for Market Surveillance Data Ingestion
Manages periodic data fetching with market hours awareness
"""

import logging
import time
import signal
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, Dict, Any
import threading
from enum import Enum

from ..shared.config import Config
from .sqs_producer import MarketDataProducer

logger = logging.getLogger(__name__)

class ScheduleMode(Enum):
    CONTINUOUS = "continuous"
    MARKET_HOURS_ONLY = "market_hours_only"
    WEEKDAYS_ONLY = "weekdays_only"
    BUSINESS_HOURS = "business_hours"

class MarketDataScheduler:
    def __init__(self, producer: Optional[MarketDataProducer] = None):
        self.config = Config
        self.producer = producer or MarketDataProducer()
        
        # Scheduler state
        self.is_running = False
        self.should_stop = False
        self.current_thread: Optional[threading.Thread] = None
        
        # Configuration
        self.interval_seconds = self.config.PRODUCER_INTERVAL
        self.mode = ScheduleMode.CONTINUOUS
        self.symbols = self.config.MARKET_DATA_SYMBOLS
        
        # Statistics
        self.stats = {
            'scheduler_start_time': None,
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run_time': None,
            'last_successful_run': None,
            'next_scheduled_run': None,
            'mode': self.mode.value,
            'interval_seconds': self.interval_seconds
        }
        
        # Thread safety
        self.stats_lock = threading.Lock()
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        logger.info(f"Market Data Scheduler initialized with mode: {self.mode.value}")
    
    def configure(self, interval_seconds: Optional[int] = None, 
                 mode: Optional[ScheduleMode] = None,
                 symbols: Optional[list] = None) -> None:
        """
        Configure scheduler parameters
        
        Args:
            interval_seconds: Interval between runs
            mode: Scheduling mode
            symbols: List of symbols to process
        """
        if interval_seconds is not None:
            self.interval_seconds = max(10, interval_seconds)  # Minimum 10 seconds
            logger.info(f"Interval set to {self.interval_seconds} seconds")
        
        if mode is not None:
            self.mode = mode
            logger.info(f"Mode set to {self.mode.value}")
        
        if symbols is not None:
            self.symbols = symbols
            logger.info(f"Symbols set to {len(symbols)} symbols")
        
        # Update stats
        with self.stats_lock:
            self.stats['mode'] = self.mode.value
            self.stats['interval_seconds'] = self.interval_seconds
    
    def start(self, blocking: bool = True) -> None:
        """
        Start the scheduler
        
        Args:
            blocking: If True, run in current thread (blocking). If False, run in background thread.
        """
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        logger.info(f"Starting Market Data Scheduler in {self.mode.value} mode")
        
        with self.stats_lock:
            self.stats['scheduler_start_time'] = datetime.now(timezone.utc).isoformat()
        
        if blocking:
            self._run_scheduler_loop()
        else:
            self.current_thread = threading.Thread(
                target=self._run_scheduler_loop,
                name="MarketDataScheduler",
                daemon=True
            )
            self.current_thread.start()
            logger.info("Scheduler started in background thread")
    
    def stop(self, timeout: float = 30.0) -> bool:
        """
        Stop the scheduler gracefully
        
        Args:
            timeout: Maximum time to wait for shutdown
            
        Returns:
            True if stopped successfully, False if timeout
        """
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return True
        
        logger.info("Stopping scheduler...")
        self.should_stop = True
        
        # Stop the producer if it's running continuously
        self.producer.stop_continuous_production()
        
        # Wait for thread to finish if running in background
        if self.current_thread and self.current_thread.is_alive():
            logger.info(f"Waiting up to {timeout}s for scheduler thread to stop...")
            self.current_thread.join(timeout=timeout)
            
            if self.current_thread.is_alive():
                logger.error(f"Scheduler thread did not stop within {timeout}s timeout")
                return False
        
        logger.info("Scheduler stopped successfully")
        return True
    
    def _run_scheduler_loop(self) -> None:
        """Main scheduler loop"""
        self.is_running = True
        self.should_stop = False
        
        logger.info("Scheduler loop started")
        
        try:
            while not self.should_stop:
                try:
                    # Check if we should run based on mode
                    if self._should_run_now():
                        logger.info("Starting scheduled market data production...")
                        
                        # Update next run time
                        next_run = datetime.now(timezone.utc) + timedelta(seconds=self.interval_seconds)
                        with self.stats_lock:
                            self.stats['next_scheduled_run'] = next_run.isoformat()
                        
                        # Run production
                        result = self._run_production()
                        
                        # Update statistics
                        self._update_run_stats(result)
                        
                        # Log results
                        if result.get('error'):
                            logger.error(f"Production run failed: {result['error']}")
                        else:
                            sent = result.get('messages_sent', 0)
                            failed = result.get('messages_failed', 0)
                            logger.info(f"Production run completed: {sent} sent, {failed} failed")
                    
                    else:
                        logger.debug(f"Skipping run (mode: {self.mode.value}, current time not suitable)")
                        
                        # Still update next run time for monitoring
                        next_run = self._calculate_next_suitable_run_time()
                        with self.stats_lock:
                            self.stats['next_scheduled_run'] = next_run.isoformat()
                    
                    # Wait for next interval
                    if not self.should_stop:
                        self._wait_for_next_run()
                        
                except Exception as e:
                    error_msg = f"Scheduler loop error: {str(e)}"
                    logger.error(error_msg)
                    
                    # Update failed run stats
                    with self.stats_lock:
                        self.stats['total_runs'] += 1
                        self.stats['failed_runs'] += 1
                        self.stats['last_run_time'] = datetime.now(timezone.utc).isoformat()
                    
                    # Wait a bit before retrying to avoid rapid failures
                    if not self.should_stop:
                        time.sleep(min(60, self.interval_seconds))  # Wait up to 1 minute
                        
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")
        finally:
            self.is_running = False
            logger.info("Scheduler loop ended")
    
    def _should_run_now(self) -> bool:
        """Check if production should run based on current mode"""
        now = datetime.now(timezone.utc)
        
        if self.mode == ScheduleMode.CONTINUOUS:
            return True
        
        elif self.mode == ScheduleMode.WEEKDAYS_ONLY:
            # Monday = 0, Sunday = 6
            return now.weekday() < 5  # Monday through Friday
        
        elif self.mode == ScheduleMode.MARKET_HOURS_ONLY:
            return self._is_market_hours(now)
        
        elif self.mode == ScheduleMode.BUSINESS_HOURS:
            return self._is_business_hours(now)
        
        return True  # Default to continuous
    
    def _is_market_hours(self, dt: datetime) -> bool:
        """Check if datetime is during market hours (EST)"""
        # Convert to EST (approximate - doesn't handle DST perfectly)
        est_time = dt - timedelta(hours=5)
        
        # Check weekday (Monday=0, Sunday=6)
        if est_time.weekday() >= 5:  # Weekend
            return self.config.WEEKEND_OPERATION
        
        # Check time (9:00 AM to 4:00 PM EST)
        hour = est_time.hour
        return self.config.MARKET_OPEN_HOUR <= hour < self.config.MARKET_CLOSE_HOUR
    
    def _is_business_hours(self, dt: datetime) -> bool:
        """Check if datetime is during business hours (8 AM - 6 PM EST, weekdays only)"""
        est_time = dt - timedelta(hours=5)
        
        # Check weekday
        if est_time.weekday() >= 5:  # Weekend
            return False
        
        # Check time (8 AM to 6 PM EST)
        hour = est_time.hour
        return 8 <= hour < 18
    
    def _calculate_next_suitable_run_time(self) -> datetime:
        """Calculate next suitable run time based on mode"""
        now = datetime.now(timezone.utc)
        
        if self.mode == ScheduleMode.CONTINUOUS:
            return now + timedelta(seconds=self.interval_seconds)
        
        # For other modes, find next suitable time
        next_time = now + timedelta(seconds=self.interval_seconds)
        
        # Keep incrementing until we find a suitable time
        max_iterations = 1440  # Don't search more than 24 hours ahead
        iterations = 0
        
        while iterations < max_iterations:
            if self.mode == ScheduleMode.WEEKDAYS_ONLY and next_time.weekday() >= 5:
                # Skip to Monday
                days_to_monday = 7 - next_time.weekday()
                next_time = next_time.replace(hour=9, minute=0, second=0) + timedelta(days=days_to_monday)
                break
            
            elif self.mode == ScheduleMode.MARKET_HOURS_ONLY:
                if self._is_market_hours(next_time):
                    break
                # Move to next market open time
                next_time += timedelta(hours=1)
            
            elif self.mode == ScheduleMode.BUSINESS_HOURS:
                if self._is_business_hours(next_time):
                    break
                # Move to next business hour
                next_time += timedelta(hours=1)
            
            else:
                break  # Unknown mode, use current time
            
            iterations += 1
        
        return next_time
    
    def _run_production(self) -> Dict[str, Any]:
        """Run market data production"""
        try:
            # Use parallel batch for better performance
            result = self.producer.produce_parallel_batch(
                symbols=self.symbols,
                max_workers=5
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Production run failed: {str(e)}")
            return {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
                'messages_sent': 0,
                'messages_failed': len(self.symbols) if self.symbols else 0
            }
    
    def _wait_for_next_run(self) -> None:
        """Wait for next scheduled run with early termination support"""
        wait_time = self.interval_seconds
        sleep_interval = min(5, wait_time)  # Check for stop signal every 5 seconds or less
        
        elapsed = 0
        while elapsed < wait_time and not self.should_stop:
            time.sleep(min(sleep_interval, wait_time - elapsed))
            elapsed += sleep_interval
    
    def _update_run_stats(self, result: Dict[str, Any]) -> None:
        """Update run statistics"""
        with self.stats_lock:
            self.stats['total_runs'] += 1
            self.stats['last_run_time'] = datetime.now(timezone.utc).isoformat()
            
            if result.get('error'):
                self.stats['failed_runs'] += 1
            else:
                self.stats['successful_runs'] += 1
                if result.get('messages_sent', 0) > 0:
                    self.stats['last_successful_run'] = datetime.now(timezone.utc).isoformat()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.stop()
            sys.exit(0)
        
        # Handle common termination signals
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination request
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status and statistics"""
        with self.stats_lock:
            status = self.stats.copy()
        
        # Add current runtime status
        status.update({
            'is_running': self.is_running,
            'should_stop': self.should_stop,
            'current_timestamp': datetime.now(timezone.utc).isoformat(),
            'thread_alive': self.current_thread.is_alive() if self.current_thread else False,
            'mode': self.mode.value,
            'symbols_count': len(self.symbols),
            'symbols': self.symbols[:10] if len(self.symbols) > 10 else self.symbols,  # Limit for readability
            'producer_stats': self.producer.get_statistics()
        })
        
        # Calculate uptime if running
        if status['scheduler_start_time']:
            start_time = datetime.fromisoformat(status['scheduler_start_time'].replace('Z', '+00:00'))
            uptime = datetime.now(timezone.utc) - start_time
            status['uptime_seconds'] = int(uptime.total_seconds())
            status['uptime_formatted'] = str(timedelta(seconds=int(uptime.total_seconds())))
        
        # Calculate success rate
        if status['total_runs'] > 0:
            status['success_rate'] = status['successful_runs'] / status['total_runs']
        else:
            status['success_rate'] = 0.0
        
        return status
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'HEALTHY',
            'scheduler': {
                'is_running': self.is_running,
                'mode': self.mode.value,
                'next_run_suitable': self._should_run_now()
            }
        }
        
        # Check producer health
        producer_health = self.producer.health_check()
        health['producer'] = producer_health
        
        # Determine overall status
        if not self.is_running:
            health['status'] = 'STOPPED'
        elif producer_health.get('status') != 'HEALTHY':
            health['status'] = 'DEGRADED'
        elif self.stats.get('failed_runs', 0) > self.stats.get('successful_runs', 0):
            health['status'] = 'DEGRADED'
        
        return health