"""Adaptive rate limiter for Google Ads API operations."""

import time
import logging

logger = logging.getLogger(__name__)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that adjusts delays based on success/failure patterns.

    Starts with a minimum delay and increases on errors, decreases on successes.
    This helps maximize throughput while respecting rate limits.
    """

    def __init__(self,
                 initial_delay: float = 1.0,
                 min_delay: float = 0.5,
                 max_delay: float = 10.0,
                 increase_factor: float = 2.0,
                 decrease_factor: float = 0.9):
        """
        Initialize adaptive rate limiter.

        Args:
            initial_delay: Starting delay in seconds
            min_delay: Minimum delay in seconds
            max_delay: Maximum delay in seconds
            increase_factor: Multiply delay by this on error (e.g., 2.0 = double)
            decrease_factor: Multiply delay by this on success (e.g., 0.9 = reduce 10%)
        """
        self.current_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self.success_count = 0
        self.error_count = 0

    def wait(self):
        """Wait for the current delay period."""
        if self.current_delay > 0:
            time.sleep(self.current_delay)

    def on_success(self):
        """Called after successful operation - reduces delay."""
        self.success_count += 1
        old_delay = self.current_delay
        self.current_delay = max(self.min_delay, self.current_delay * self.decrease_factor)

        if self.success_count % 10 == 0:  # Log every 10 successes
            logger.debug(f"Rate limiter: {self.success_count} successes, delay: {old_delay:.2f}s -> {self.current_delay:.2f}s")

    def on_error(self, error_type: str = "unknown"):
        """Called after failed operation - increases delay."""
        self.error_count += 1
        old_delay = self.current_delay
        self.current_delay = min(self.max_delay, self.current_delay * self.increase_factor)

        logger.warning(
            f"Rate limiter: Error ({error_type}), delay: {old_delay:.2f}s -> {self.current_delay:.2f}s "
            f"(errors: {self.error_count}, successes: {self.success_count})"
        )

    def get_stats(self) -> dict:
        """Get current statistics."""
        return {
            'current_delay': self.current_delay,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'success_rate': self.success_count / max(1, self.success_count + self.error_count)
        }

    def reset(self):
        """Reset to initial state."""
        self.current_delay = self.min_delay
        self.success_count = 0
        self.error_count = 0
