"""Retry logic with exponential backoff."""

import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Any
from google.ads.googleads.errors import GoogleAdsException
from google.api_core.exceptions import ServiceUnavailable

logger = logging.getLogger(__name__)


def async_retry(max_attempts: int = 5, delay: float = 2.0, backoff: float = 2.0):
    """Decorator for async functions with exponential backoff retry.

    Handles 503 errors with extended delays (60s, 180s, 540s, 1620s).
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)

                except ServiceUnavailable as e:
                    # Handle 503 errors with much longer delays
                    last_exception = e

                    if attempt < max_attempts:
                        # Use much longer delays for 503 errors (exponential: 60s, 180s, 540s, 1620s)
                        retry_delay = 60 * (3 ** (attempt - 1))
                        logger.warning(
                            f"503 Service Unavailable in {func.__name__}. "
                            f"Attempt {attempt}/{max_attempts}. "
                            f"Waiting {retry_delay}s before retry... Error: {str(e)}"
                        )
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__} due to 503 errors")
                        raise last_exception

                except GoogleAdsException as e:
                    last_exception = e

                    # Check for CONCURRENT_MODIFICATION errors
                    is_concurrent_modification = False
                    if hasattr(e, 'failure') and e.failure:
                        for error in e.failure.errors:
                            if hasattr(error.error_code, 'database_error'):
                                error_type = str(error.error_code.database_error)
                                if 'CONCURRENT_MODIFICATION' in error_type:
                                    is_concurrent_modification = True
                                    break

                    # Check if error is retryable
                    if hasattr(e, 'failure') and e.failure:
                        # Don't retry on certain errors (like invalid credentials, quota exceeded permanently)
                        error_codes = [err.error_code for err in e.failure.errors]
                        non_retryable = ['AUTHENTICATION_ERROR', 'AUTHORIZATION_ERROR', 'QUOTA_ERROR']

                        for error in e.failure.errors:
                            error_name = str(error.error_code).split('.')[0]
                            if error_name in non_retryable:
                                logger.error(f"Non-retryable error in {func.__name__}: {error_name}")
                                raise

                    if attempt < max_attempts:
                        # Use longer delays with jitter for CONCURRENT_MODIFICATION
                        if is_concurrent_modification:
                            # Base delay: 5s, 10s, 20s, 40s, 80s with random jitter ±20%
                            base_delay = 5.0 * (2 ** (attempt - 1))
                            jitter = random.uniform(-0.2, 0.2) * base_delay
                            retry_delay = base_delay + jitter
                            logger.warning(
                                f"CONCURRENT_MODIFICATION detected in {func.__name__}. "
                                f"Attempt {attempt}/{max_attempts}. "
                                f"Waiting {retry_delay:.1f}s with jitter before retry..."
                            )
                            await asyncio.sleep(retry_delay)
                        else:
                            # Standard exponential backoff
                            logger.warning(
                                f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                                f"Retrying in {current_delay}s... Error: {str(e)[:100]}"
                            )
                            await asyncio.sleep(current_delay)
                            current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        raise last_exception

                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise

            raise last_exception

        return wrapper
    return decorator


def sync_retry(max_attempts: int = 5, delay: float = 2.0, backoff: float = 2.0):
    """Decorator for sync functions with exponential backoff retry.

    Handles 503 errors with extended delays (60s, 180s, 540s, 1620s).
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import time
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except ServiceUnavailable as e:
                    # Handle 503 errors with much longer delays
                    last_exception = e

                    if attempt < max_attempts:
                        # Use much longer delays for 503 errors (exponential: 60s, 180s, 540s, 1620s)
                        retry_delay = 60 * (3 ** (attempt - 1))
                        logger.warning(
                            f"503 Service Unavailable in {func.__name__}. "
                            f"Attempt {attempt}/{max_attempts}. "
                            f"Waiting {retry_delay}s before retry... Error: {str(e)}"
                        )
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__} due to 503 errors")
                        raise last_exception

                except GoogleAdsException as e:
                    last_exception = e

                    # Check for CONCURRENT_MODIFICATION errors
                    is_concurrent_modification = False
                    if hasattr(e, 'failure') and e.failure:
                        for error in e.failure.errors:
                            if hasattr(error.error_code, 'database_error'):
                                error_type = str(error.error_code.database_error)
                                if 'CONCURRENT_MODIFICATION' in error_type:
                                    is_concurrent_modification = True
                                    break

                    if attempt < max_attempts:
                        # Use longer delays with jitter for CONCURRENT_MODIFICATION
                        if is_concurrent_modification:
                            # Base delay: 5s, 10s, 20s, 40s, 80s with random jitter ±20%
                            base_delay = 5.0 * (2 ** (attempt - 1))
                            jitter = random.uniform(-0.2, 0.2) * base_delay
                            retry_delay = base_delay + jitter
                            logger.warning(
                                f"CONCURRENT_MODIFICATION detected in {func.__name__}. "
                                f"Attempt {attempt}/{max_attempts}. "
                                f"Waiting {retry_delay:.1f}s with jitter before retry..."
                            )
                            time.sleep(retry_delay)
                        else:
                            # Standard exponential backoff
                            logger.warning(
                                f"Attempt {attempt}/{max_attempts} failed for {func.__name__}. "
                                f"Retrying in {current_delay}s..."
                            )
                            time.sleep(current_delay)
                            current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
                        raise last_exception

                except Exception as e:
                    last_exception = e
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise

            raise last_exception

        return wrapper
    return decorator
