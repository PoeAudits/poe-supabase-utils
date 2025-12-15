from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Iterator, Optional

from supabase import Client, create_client

from poe_supabase_utils.config import get_config

logger = logging.getLogger(__name__)

ITERATIONS_MAX = 1000


def _validate_table_name(table_name: str) -> None:
    if not isinstance(table_name, str):
        raise TypeError(f"table_name must be str, got {type(table_name).__name__}")
    if not table_name.strip():
        raise ValueError("table_name cannot be empty")


def _validate_positive_int(value: int, name: str) -> None:
    if not isinstance(value, int):
        raise TypeError(f"{name} must be int, got {type(value).__name__}")
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")


def _validate_non_negative_float(value: float, name: str) -> None:
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")


@dataclass(frozen=True)
class DatabaseClient:
    """Wrapper around Supabase client with validation."""

    client: Client

    def __init__(self, config: Optional[object] = None) -> None:
        """Initialize DatabaseClient.

        Args:
            config: Optional SupabaseConfig. If None, loads from environment.

        Raises:
            ValueError: If configuration is invalid.
        """
        if config is None:
            config = get_config()

        supa_client: Client = create_client(config.url, config.service_role_key)
        object.__setattr__(self, "client", supa_client)

    def __enter__(self) -> DatabaseClient:
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[object],
    ) -> None:
        pass


_db: Optional[DatabaseClient] = None


def get_db() -> DatabaseClient:
    """Get or create the shared DatabaseClient instance."""
    global _db
    if _db is None:
        _db = DatabaseClient()
    return _db


def supabase_get_all_messages(
    table_name: str,
    *,
    client: DatabaseClient,
    sleep_time_s: float = 0.5,
    limit_per_request: int = 1000,
) -> list[dict]:
    """Retrieve all messages from a table with pagination.

    Args:
        table_name: Name of the table to query.
        client: DatabaseClient instance.
        sleep_time_s: Sleep duration between requests in seconds.
        limit_per_request: Maximum rows per request.

    Returns:
        List of all records from the table.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
        RuntimeError: If iteration limit exceeded.
    """
    _validate_table_name(table_name)
    _validate_non_negative_float(sleep_time_s, "sleep_time_s")
    _validate_positive_int(limit_per_request, "limit_per_request")

    all_records: list[dict] = []
    offset = 0

    for _ in range(ITERATIONS_MAX):
        response = (
            client.client.table(table_name)
            .select("*")
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )

        if not response.data:
            logger.debug("No more records found")
            break

        all_records.extend(response.data)
        if len(response.data) < limit_per_request:
            break

        offset += limit_per_request
        time.sleep(sleep_time_s)
    else:
        raise RuntimeError(
            f"Exceeded {ITERATIONS_MAX} iterations fetching from {table_name}"
        )

    return all_records


def supabase_stream_all_messages(
    table_name: str,
    *,
    client: DatabaseClient,
    sleep_time_s: float = 0.5,
    limit_per_request: int = 1000,
) -> Iterator[dict]:
    """Stream all messages from a table with pagination.

    Args:
        table_name: Name of the table to query.
        client: DatabaseClient instance.
        sleep_time_s: Sleep duration between requests in seconds.
        limit_per_request: Maximum rows per request.

    Yields:
        Individual records from the table.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
        RuntimeError: If iteration limit exceeded.
    """
    _validate_table_name(table_name)
    _validate_non_negative_float(sleep_time_s, "sleep_time_s")
    _validate_positive_int(limit_per_request, "limit_per_request")

    offset = 0

    for _ in range(ITERATIONS_MAX):
        response = (
            client.client.table(table_name)
            .select("*")
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            return

        for row in rows:
            yield row

        if len(rows) < limit_per_request:
            return

        offset += limit_per_request
        time.sleep(sleep_time_s)

    raise RuntimeError(
        f"Exceeded {ITERATIONS_MAX} iterations streaming from {table_name}"
    )


def supabase_stream_recent_messages(
    table_name: str,
    messages_count: int,
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
) -> Iterator[dict]:
    """Stream the most recent messages from a table.

    Args:
        table_name: Name of the table to query.
        messages_count: Number of messages to retrieve.
        client: DatabaseClient instance.
        batch_size: Maximum rows per request.

    Yields:
        Individual records, most recent first.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    _validate_table_name(table_name)
    _validate_positive_int(messages_count, "messages_count")
    _validate_positive_int(batch_size, "batch_size")

    emitted = 0
    offset = 0

    while emitted < messages_count:
        current_batch_size = min(batch_size, messages_count - emitted)

        response = (
            client.client.table(table_name)
            .select("*")
            .order("timestamp", desc=True)
            .range(offset, offset + current_batch_size - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            return

        for row in rows:
            yield row
            emitted += 1
            if emitted >= messages_count:
                return

        if len(rows) < current_batch_size:
            return

        offset += current_batch_size


def supabase_get_recent_messages(
    table_name: str,
    messages_count: int,
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
) -> list[dict]:
    """Retrieve the most recent messages from a table.

    Args:
        table_name: Name of the table to query.
        messages_count: Number of messages to retrieve.
        client: DatabaseClient instance.
        batch_size: Maximum rows per request.

    Returns:
        List of records, most recent first.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    _validate_table_name(table_name)
    _validate_positive_int(messages_count, "messages_count")
    _validate_positive_int(batch_size, "batch_size")

    all_records: list[dict] = []
    offset = 0

    while len(all_records) < messages_count:
        remaining = messages_count - len(all_records)
        current_batch_size = min(batch_size, remaining)

        response = (
            client.client.table(table_name)
            .select("*")
            .order("timestamp", desc=True)
            .range(offset, offset + current_batch_size - 1)
            .execute()
        )

        if not response.data:
            logger.debug("No more records found")
            break

        all_records.extend(response.data)
        if len(response.data) < current_batch_size:
            break

        offset += current_batch_size

    return all_records[:messages_count]


def supabase_stream_messages_by_timedelta(
    table_name: str,
    time_delta: timedelta,
    *,
    client: DatabaseClient,
    limit_per_request: int = 1000,
    sleep_time_s: float = 0.1,
) -> Iterator[dict]:
    """Stream messages within a time window.

    Args:
        table_name: Name of the table to query.
        time_delta: Time window from now to look back.
        client: DatabaseClient instance.
        limit_per_request: Maximum rows per request.
        sleep_time_s: Sleep duration between requests in seconds.

    Yields:
        Individual records within the time window.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
        RuntimeError: If iteration limit exceeded.
    """
    _validate_table_name(table_name)
    if not isinstance(time_delta, timedelta):
        raise TypeError(
            f"time_delta must be timedelta, got {type(time_delta).__name__}"
        )
    if time_delta.total_seconds() <= 0:
        raise ValueError(f"time_delta must be positive, got {time_delta}")
    _validate_positive_int(limit_per_request, "limit_per_request")
    _validate_non_negative_float(sleep_time_s, "sleep_time_s")

    time_cutoff = datetime.now() - time_delta
    timestamp_filter = time_cutoff.isoformat()

    offset = 0

    for _ in range(ITERATIONS_MAX):
        response = (
            client.client.table(table_name)
            .select("*")
            .gte("timestamp", timestamp_filter)
            .order("timestamp", desc=True)
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            return

        for row in rows:
            yield row

        if len(rows) < limit_per_request:
            return

        offset += limit_per_request
        time.sleep(sleep_time_s)

    raise RuntimeError(
        f"Exceeded {ITERATIONS_MAX} iterations streaming from {table_name}"
    )


def supabase_get_messages_by_timedelta(
    table_name: str,
    time_delta: timedelta,
    *,
    client: DatabaseClient,
    limit_per_request: int = 1000,
    sleep_time_s: float = 0.1,
) -> list[dict]:
    """Retrieve all messages within a time window.

    Args:
        table_name: Name of the table to query.
        time_delta: Time window from now to look back.
        client: DatabaseClient instance.
        limit_per_request: Maximum rows per request.
        sleep_time_s: Sleep duration between requests in seconds.

    Returns:
        List of records within the time window.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
        RuntimeError: If iteration limit exceeded.
    """
    _validate_table_name(table_name)
    if not isinstance(time_delta, timedelta):
        raise TypeError(
            f"time_delta must be timedelta, got {type(time_delta).__name__}"
        )
    if time_delta.total_seconds() <= 0:
        raise ValueError(f"time_delta must be positive, got {time_delta}")
    _validate_positive_int(limit_per_request, "limit_per_request")
    _validate_non_negative_float(sleep_time_s, "sleep_time_s")

    time_cutoff = datetime.now() - time_delta
    timestamp_filter = time_cutoff.isoformat()

    logger.info("Fetching messages since: %s", timestamp_filter)

    all_records: list[dict] = []
    offset = 0

    for _ in range(ITERATIONS_MAX):
        response = (
            client.client.table(table_name)
            .select("*")
            .gte("timestamp", timestamp_filter)
            .order("timestamp", desc=True)
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )

        if not response.data:
            logger.debug("No more records found")
            break

        all_records.extend(response.data)
        logger.debug("Fetched %d messages so far", len(all_records))

        if len(response.data) < limit_per_request:
            break

        offset += limit_per_request
        time.sleep(sleep_time_s)
    else:
        raise RuntimeError(
            f"Exceeded {ITERATIONS_MAX} iterations fetching from {table_name}"
        )

    logger.info("Total messages retrieved: %d", len(all_records))
    return all_records


def supabase_upload_messages(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
) -> bool:
    """Upload messages to a table.

    Args:
        table_name: Name of the table to insert into.
        messages: List of message dictionaries to upload.
        client: DatabaseClient instance.

    Returns:
        True if upload succeeded, False otherwise.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    _validate_table_name(table_name)
    if not isinstance(messages, list):
        raise TypeError(f"messages must be list, got {type(messages).__name__}")

    if not messages:
        logger.info("No messages to upload")
        return True

    response = client.client.table(table_name).insert(messages).execute()

    if response.data:
        logger.info(
            "Successfully uploaded %d messages to %s", len(messages), table_name
        )
        return True

    logger.error("Error uploading messages - no data returned")
    return False


def supabase_upsert_messages(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
    conflict_columns: Optional[list[str]] = None,
    batch_size: int = 500,
) -> bool:
    """Upsert messages to a table with deduplication.

    Args:
        table_name: Name of the table to upsert into.
        messages: List of message dictionaries to upsert.
        client: DatabaseClient instance.
        conflict_columns: Columns to use for conflict resolution.
        batch_size: Maximum rows per batch.

    Returns:
        True if all batches succeeded, False otherwise.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    if conflict_columns is None:
        conflict_columns = ["text"]

    _validate_table_name(table_name)
    if not isinstance(messages, list):
        raise TypeError(f"messages must be list, got {type(messages).__name__}")
    if not isinstance(conflict_columns, list):
        raise TypeError(
            f"conflict_columns must be list, got {type(conflict_columns).__name__}"
        )
    if not conflict_columns:
        raise ValueError("conflict_columns cannot be empty")
    _validate_positive_int(batch_size, "batch_size")

    if not messages:
        logger.info("No messages to upsert")
        return True

    seen_identifiers: set[tuple] = set()
    deduplicated_messages: list[dict] = []
    duplicates_count = 0

    for message in messages:
        identifier_tuple = tuple(message.get(col) for col in conflict_columns)
        if identifier_tuple not in seen_identifiers:
            seen_identifiers.add(identifier_tuple)
            deduplicated_messages.append(message)
        else:
            duplicates_count += 1

    if duplicates_count > 0:
        logger.info(
            "Removed %d internal duplicates from incoming batch", duplicates_count
        )

    messages_count = len(deduplicated_messages)
    batches_count = (messages_count + batch_size - 1) // batch_size
    batches_successful = 0

    logger.info(
        "Processing %d messages in %d batches of %d",
        messages_count,
        batches_count,
        batch_size,
    )

    on_conflict_val = ",".join(conflict_columns)

    for i in range(0, messages_count, batch_size):
        batch = deduplicated_messages[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        try:
            response = (
                client.client.table(table_name)
                .upsert(batch, on_conflict=on_conflict_val)
                .execute()
            )

            if response.data:
                batches_successful += 1
                logger.info(
                    "Batch %d/%d: Successfully upserted %d messages",
                    batch_num,
                    batches_count,
                    len(batch),
                )
            else:
                logger.error(
                    "Batch %d/%d: Error upserting %d messages",
                    batch_num,
                    batches_count,
                    len(batch),
                )
                return False

        except Exception as batch_error:
            logger.error(
                "Batch %d/%d: Error - %s", batch_num, batches_count, batch_error
            )
            return False

    logger.info(
        "Successfully completed %d/%d batches", batches_successful, batches_count
    )
    return True


def supabase_batch_upload_with_retry(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
    retries_max: int = 3,
) -> bool:
    """Upload messages in batches with retry logic.

    Args:
        table_name: Name of the table to insert into.
        messages: List of message dictionaries to upload.
        client: DatabaseClient instance.
        batch_size: Maximum rows per batch.
        retries_max: Maximum retry attempts per batch.

    Returns:
        True if all batches succeeded, False otherwise.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    _validate_table_name(table_name)
    if not isinstance(messages, list):
        raise TypeError(f"messages must be list, got {type(messages).__name__}")
    _validate_positive_int(batch_size, "batch_size")
    _validate_positive_int(retries_max, "retries_max")

    if not messages:
        logger.info("No messages to upload")
        return True

    batches_count = (len(messages) + batch_size - 1) // batch_size
    batches_successful = 0

    for i in range(0, len(messages), batch_size):
        batch = messages[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        logger.info(
            "Uploading batch %d/%d (%d messages)", batch_num, batches_count, len(batch)
        )

        for attempt in range(retries_max):
            try:
                response = client.client.table(table_name).insert(batch).execute()

                if response.data:
                    batches_successful += 1
                    logger.info("Batch %d uploaded successfully", batch_num)
                    break

                logger.warning("Batch %d failed - attempt %d", batch_num, attempt + 1)

            except Exception as e:
                logger.warning(
                    "Batch %d error (attempt %d): %s", batch_num, attempt + 1, e
                )

            if attempt == retries_max - 1:
                logger.error(
                    "Batch %d failed after %d attempts", batch_num, retries_max
                )
                return False

    logger.info(
        "Successfully uploaded %d/%d batches", batches_successful, batches_count
    )
    return batches_successful == batches_count


def supabase_check_message_exists(
    message_id: int,
    *,
    client: DatabaseClient,
    table_name: str = "raw_data",
) -> bool:
    """Check if a message exists in the database.

    Args:
        message_id: ID of the message to check.
        client: DatabaseClient instance.
        table_name: Name of the table to query.

    Returns:
        True if message exists, False otherwise.

    Raises:
        TypeError: If arguments have wrong types.
        ValueError: If arguments have invalid values.
    """
    if not isinstance(message_id, int):
        raise TypeError(f"message_id must be int, got {type(message_id).__name__}")
    if message_id < 0:
        raise ValueError(f"message_id must be non-negative, got {message_id}")
    _validate_table_name(table_name)

    try:
        response = (
            client.client.table(table_name)
            .select("message_id")
            .eq("message_id", message_id)
            .execute()
        )

        return len(response.data) > 0 if response.data else False

    except Exception as e:
        logger.error("Error checking message existence: %s", e)
        return False


# Convenience partials using lazy-loaded shared client
def _get_db_partial(func):  # type: ignore[no-untyped-def]
    """Create a partial that lazily binds the shared db client."""

    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        return func(*args, client=get_db(), **kwargs)

    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__.replace("supabase_", "")
    return wrapper


get_all_messages = _get_db_partial(supabase_get_all_messages)
get_recent_messages = _get_db_partial(supabase_get_recent_messages)
get_messages_by_timedelta = _get_db_partial(supabase_get_messages_by_timedelta)
upload_messages = _get_db_partial(supabase_upload_messages)
upsert_messages = _get_db_partial(supabase_upsert_messages)
batch_upload_with_retry = _get_db_partial(supabase_batch_upload_with_retry)
check_message_exists = _get_db_partial(supabase_check_message_exists)
stream_all_messages = _get_db_partial(supabase_stream_all_messages)
stream_recent_messages = _get_db_partial(supabase_stream_recent_messages)
stream_messages_by_timedelta = _get_db_partial(supabase_stream_messages_by_timedelta)
