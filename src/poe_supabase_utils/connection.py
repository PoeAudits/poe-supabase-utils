from __future__ import annotations

# import os
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from functools import partial
from typing import Optional, Iterator
from supabase import Client, create_client
from poe_supabase_utils.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY


@dataclass(frozen=True)
class DatabaseClient:
    client: Client

    def __init__(self) -> None:
        url: Optional[str] = SUPABASE_URL
        service_key: Optional[str] = SUPABASE_SERVICE_ROLE_KEY
        assert url is not None, "SUPABASE_URL environment variable must be set"
        assert service_key is not None, (
            "SUPABASE_SERVICE_ROLE_KEY environment variable must be set"
        )
        assert len(url.strip()) > 0, "SUPABASE_URL cannot be empty string"
        assert len(service_key.strip()) > 0, (
            "SUPABASE_SERVICE_ROLE_KEY cannot be empty string"
        )
        if not url or not service_key:
            raise ValueError(
                "Supabase URL and Key must be set as environment variables."
            )
        supa_client: Client = create_client(url, service_key)
        assert supa_client is not None, "Failed to create Supabase client"
        object.__setattr__(self, "client", supa_client)


# Single shared DatabaseClient instance for partials
db = DatabaseClient()


# ---------- Prefixed functions that accept DatabaseClient ----------


def supabase_get_all_messages(
    table_name: str,
    *,
    client: DatabaseClient,
    sleep_time: float = 0.5,
    limit_per_request: int = 1000,
) -> list[dict]:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(sleep_time, (int, float)), (
        f"sleep_time must be numeric, got {type(sleep_time)}"
    )
    assert sleep_time >= 0, f"sleep_time must be non-negative, got {sleep_time}"
    assert isinstance(limit_per_request, int), (
        f"limit_per_request must be int, got {type(limit_per_request)}"
    )
    assert limit_per_request > 0, (
        f"limit_per_request must be positive, got {limit_per_request}"
    )

    try:
        all_records: list[dict] = []
        offset = 0

        while True:
            response = (
                client.client.table(table_name)
                .select("*")
                .range(offset, offset + limit_per_request - 1)
                .execute()
            )
            assert response is not None, "Database response cannot be None"

            if response.data:
                assert isinstance(response.data, list), (
                    f"response.data must be list, got {type(response.data)}"
                )
                all_records.extend(response.data)
                if len(response.data) < limit_per_request:
                    break
                offset += limit_per_request
                time.sleep(sleep_time)
            else:
                print("Error retrieving data or no more records.")
                break

        assert isinstance(all_records, list), (
            f"all_records must be list, got {type(all_records)}"
        )
        assert all(isinstance(record, dict) for record in all_records), (
            "All records must be dictionaries"
        )
        return all_records

    except Exception as e:
        raise e


def supabase_stream_all_messages(
    table_name: str,
    *,
    client: DatabaseClient,
    sleep_time: float = 0.5,
    limit_per_request: int = 1000,
) -> Iterator[dict]:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(sleep_time, (int, float)), (
        f"sleep_time must be numeric, got {type(sleep_time)}"
    )
    assert sleep_time >= 0, f"sleep_time must be non-negative, got {sleep_time}"
    assert isinstance(limit_per_request, int), (
        f"limit_per_request must be int, got {type(limit_per_request)}"
    )
    assert limit_per_request > 0, (
        f"limit_per_request must be positive, got {limit_per_request}"
    )

    offset = 0
    while True:
        response = (
            client.client.table(table_name)
            .select("*")
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )
        assert response is not None, "Database response cannot be None"

        rows = response.data or []
        assert isinstance(rows, list), f"rows must be list, got {type(rows)}"
        if not rows:
            break

        for row in rows:
            assert isinstance(row, dict), f"Each row must be dict, got {type(row)}"
            yield row

        if len(rows) < limit_per_request:
            break

        offset += limit_per_request
        time.sleep(sleep_time)


def supabase_stream_recent_messages(
    table_name: str,
    number_of_messages: int,
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
) -> Iterator[dict]:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(number_of_messages, int), (
        f"number_of_messages must be int, got {type(number_of_messages)}"
    )
    assert number_of_messages > 0, (
        f"number_of_messages must be positive, got {number_of_messages}"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(batch_size, int), (
        f"batch_size must be int, got {type(batch_size)}"
    )
    assert batch_size > 0, f"batch_size must be positive, got {batch_size}"

    emitted = 0
    offset = 0

    while emitted < number_of_messages:
        current_batch_size = min(batch_size, number_of_messages - emitted)
        assert current_batch_size > 0, (
            f"current_batch_size must be positive, got {current_batch_size}"
        )

        response = (
            client.client.table(table_name)
            .select("*")
            .order("timestamp", desc=True)
            .range(offset, offset + current_batch_size - 1)
            .execute()
        )
        assert response is not None, "Database response cannot be None"

        rows = response.data or []
        assert isinstance(rows, list), f"rows must be list, got {type(rows)}"
        if not rows:
            break

        for row in rows:
            assert isinstance(row, dict), f"Each row must be dict, got {type(row)}"
            yield row
            emitted += 1
            if emitted >= number_of_messages:
                return

        if len(rows) < current_batch_size:
            break

        offset += current_batch_size


def supabase_get_recent_messages(
    table_name: str,
    number_of_messages: int,
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
) -> list[dict]:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(number_of_messages, int), (
        f"number_of_messages must be int, got {type(number_of_messages)}"
    )
    assert number_of_messages > 0, (
        f"number_of_messages must be positive, got {number_of_messages}"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(batch_size, int), (
        f"batch_size must be int, got {type(batch_size)}"
    )
    assert batch_size > 0, f"batch_size must be positive, got {batch_size}"

    try:
        all_records: list[dict] = []
        offset = 0

        while len(all_records) < number_of_messages:
            remaining_messages = number_of_messages - len(all_records)
            current_batch_size = min(batch_size, remaining_messages)
            assert current_batch_size > 0, (
                f"current_batch_size must be positive, got {current_batch_size}"
            )

            response = (
                client.client.table(table_name)
                .select("*")
                .order("timestamp", desc=True)
                .range(offset, offset + current_batch_size - 1)
                .execute()
            )
            assert response is not None, "Database response cannot be None"

            if response.data:
                assert isinstance(response.data, list), (
                    f"response.data must be list, got {type(response.data)}"
                )
                all_records.extend(response.data)
                offset += current_batch_size
                if len(response.data) < current_batch_size:
                    break
            else:
                print("Error retrieving data or no more records.")
                break

        result = all_records[:number_of_messages]
        assert isinstance(result, list), f"result must be list, got {type(result)}"
        assert all(isinstance(record, dict) for record in result), (
            "All records must be dictionaries"
        )
        assert len(result) <= number_of_messages, (
            f"Result length {len(result)} exceeds requested {number_of_messages}"
        )
        return result

    except Exception as e:
        raise e


def supabase_stream_messages_by_timedelta(
    table_name: str,
    time_delta: timedelta,
    *,
    client: DatabaseClient,
    limit_per_request: int = 1000,
    sleep_time: float = 0.1,
) -> Iterator[dict]:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(time_delta, timedelta), (
        f"time_delta must be timedelta, got {type(time_delta)}"
    )
    assert time_delta.total_seconds() > 0, (
        f"time_delta must be positive, got {time_delta}"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(limit_per_request, int), (
        f"limit_per_request must be int, got {type(limit_per_request)}"
    )
    assert limit_per_request > 0, (
        f"limit_per_request must be positive, got {limit_per_request}"
    )
    assert isinstance(sleep_time, (int, float)), (
        f"sleep_time must be numeric, got {type(sleep_time)}"
    )
    assert sleep_time >= 0, f"sleep_time must be non-negative, got {sleep_time}"

    time_cutoff = datetime.now() - time_delta
    timestamp_filter = time_cutoff.isoformat()
    assert isinstance(timestamp_filter, str), (
        f"timestamp_filter must be string, got {type(timestamp_filter)}"
    )
    assert len(timestamp_filter) > 0, "timestamp_filter cannot be empty"

    offset = 0
    while True:
        response = (
            client.client.table(table_name)
            .select("*")
            .gte("timestamp", timestamp_filter)
            .order("timestamp", desc=True)
            .range(offset, offset + limit_per_request - 1)
            .execute()
        )
        assert response is not None, "Database response cannot be None"

        rows = response.data or []
        assert isinstance(rows, list), f"rows must be list, got {type(rows)}"
        if not rows:
            break

        for row in rows:
            assert isinstance(row, dict), f"Each row must be dict, got {type(row)}"
            yield row

        if len(rows) < limit_per_request:
            break

        offset += limit_per_request
        time.sleep(sleep_time)


def supabase_get_messages_by_timedelta(
    table_name: str,
    time_delta: timedelta,
    *,
    client: DatabaseClient,
    limit_per_request: int = 1000,
    sleep_time: float = 0.1,
) -> list[dict]:
    """
    Retrieve all messages from the last `minutes` minutes using batching logic.
    """
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(time_delta, timedelta), (
        f"time_delta must be timedelta, got {type(time_delta)}"
    )
    assert time_delta.total_seconds() > 0, (
        f"time_delta must be positive, got {time_delta}"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(limit_per_request, int), (
        f"limit_per_request must be int, got {type(limit_per_request)}"
    )
    assert limit_per_request > 0, (
        f"limit_per_request must be positive, got {limit_per_request}"
    )
    assert isinstance(sleep_time, (int, float)), (
        f"sleep_time must be numeric, got {type(sleep_time)}"
    )
    assert sleep_time >= 0, f"sleep_time must be non-negative, got {sleep_time}"

    try:
        time_cutoff = datetime.now() - time_delta
        timestamp_filter = time_cutoff.isoformat()
        assert isinstance(timestamp_filter, str), (
            f"timestamp_filter must be string, got {type(timestamp_filter)}"
        )
        assert len(timestamp_filter) > 0, "timestamp_filter cannot be empty"

        all_records: list[dict] = []
        offset = 0

        print(f"Fetching messages since: {timestamp_filter}")

        while True:
            response = (
                client.client.table(table_name)
                .select("*")
                .gte("timestamp", timestamp_filter)
                .order("timestamp", desc=True)
                .range(offset, offset + limit_per_request - 1)
                .execute()
            )
            assert response is not None, "Database response cannot be None"

            if response.data:
                assert isinstance(response.data, list), (
                    f"response.data must be list, got {type(response.data)}"
                )
                all_records.extend(response.data)
                print(f"Fetched {len(all_records)} messages so far...")
                if len(response.data) < limit_per_request:
                    break
                offset += limit_per_request
                time.sleep(sleep_time)
            else:
                print("No more records found or error occurred.")
                break

        print(f"Total messages retrieved: {len(all_records)}")
        assert isinstance(all_records, list), (
            f"all_records must be list, got {type(all_records)}"
        )
        assert all(isinstance(record, dict) for record in all_records), (
            "All records must be dictionaries"
        )
        return all_records

    except Exception as e:
        raise e


def supabase_upload_messages(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
) -> bool:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(messages, list), f"messages must be list, got {type(messages)}"
    assert all(isinstance(msg, dict) for msg in messages), (
        "All messages must be dictionaries"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )

    try:
        if not messages:
            print("No messages to upload")
            return True

        response = client.client.table(table_name).insert(messages).execute()
        assert response is not None, "Database response cannot be None"

        if response.data:
            assert isinstance(response.data, list), (
                f"response.data must be list, got {type(response.data)}"
            )
            print(f"Successfully uploaded {len(messages)} messages to {table_name}")
            return True

        print("Error uploading messages - no data returned")
        return False

    except Exception as e:
        print(f"Error uploading messages to {table_name}: {e}")
        return False


def supabase_upsert_messages(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
    conflict_columns: list[str] = ["text"],
    batch_size: int = 500,
) -> bool:
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(messages, list), f"messages must be list, got {type(messages)}"
    assert all(isinstance(msg, dict) for msg in messages), (
        "All messages must be dictionaries"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(conflict_columns, list), (
        f"conflict_columns must be list, got {type(conflict_columns)}"
    )
    assert all(isinstance(col, str) for col in conflict_columns), (
        "All conflict_columns must be strings"
    )
    assert len(conflict_columns) > 0, "conflict_columns cannot be empty"
    assert isinstance(batch_size, int), (
        f"batch_size must be int, got {type(batch_size)}"
    )
    assert batch_size > 0, f"batch_size must be positive, got {batch_size}"

    try:
        if not messages:
            print("No messages to upsert")
            return True

        seen_identifiers: set[tuple] = set()
        deduplicated_messages: list[dict] = []
        internal_duplicates = 0

        for message in messages:
            assert isinstance(message, dict), (
                f"Each message must be dict, got {type(message)}"
            )
            identifier_tuple = tuple(message.get(col) for col in conflict_columns)
            if identifier_tuple not in seen_identifiers:
                seen_identifiers.add(identifier_tuple)
                deduplicated_messages.append(message)
            else:
                internal_duplicates += 1

        if internal_duplicates > 0:
            print(
                f"Removed {internal_duplicates} internal duplicates from incoming batch"
            )

        total_messages = len(deduplicated_messages)
        total_batches = (total_messages + batch_size - 1) // batch_size
        successful_batches = 0
        assert total_messages >= 0, (
            f"total_messages must be non-negative, got {total_messages}"
        )
        assert total_batches >= 0, (
            f"total_batches must be non-negative, got {total_batches}"
        )

        print(
            f"Processing {total_messages} messages in {total_batches} batches of "
            f"{batch_size}"
        )

        on_conflict_val = ",".join(conflict_columns)
        assert len(on_conflict_val) > 0, "on_conflict_val cannot be empty"

        for i in range(0, total_messages, batch_size):
            batch = deduplicated_messages[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            assert isinstance(batch, list), f"batch must be list, got {type(batch)}"
            assert len(batch) > 0, f"batch cannot be empty"
            assert len(batch) <= batch_size, (
                f"batch size {len(batch)} exceeds maximum {batch_size}"
            )

            try:
                response = (
                    client.client.table(table_name)
                    .upsert(batch, on_conflict=on_conflict_val)
                    .execute()
                )
                assert response is not None, "Database response cannot be None"

                if response.data:
                    assert isinstance(response.data, list), (
                        f"response.data must be list, got {type(response.data)}"
                    )
                    successful_batches += 1
                    print(
                        f"Batch {batch_num}/{total_batches}: Successfully upserted "
                        f"{len(batch)} messages"
                    )
                else:
                    print(
                        f"Batch {batch_num}/{total_batches}: Error upserting "
                        f"{len(batch)} messages"
                    )
                    return False

            except Exception as batch_error:
                print(f"Batch {batch_num}/{total_batches}: Error - {batch_error}")
                return False

        assert successful_batches <= total_batches, (
            f"successful_batches {successful_batches} cannot exceed total_batches {total_batches}"
        )
        print(f"Successfully completed {successful_batches}/{total_batches} batches")
        return True

    except Exception as e:
        print(f"Error in batch upsert to {table_name}: {e}")
        return False


def supabase_batch_upload_with_retry(
    table_name: str,
    messages: list[dict],
    *,
    client: DatabaseClient,
    batch_size: int = 1000,
    max_retries: int = 3,
) -> bool:
    """
    Upload messages in batches with retry logic for large datasets.
    """
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"
    assert isinstance(messages, list), f"messages must be list, got {type(messages)}"
    assert all(isinstance(msg, dict) for msg in messages), (
        "All messages must be dictionaries"
    )
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(batch_size, int), (
        f"batch_size must be int, got {type(batch_size)}"
    )
    assert batch_size > 0, f"batch_size must be positive, got {batch_size}"
    assert isinstance(max_retries, int), (
        f"max_retries must be int, got {type(max_retries)}"
    )
    assert max_retries > 0, f"max_retries must be positive, got {max_retries}"

    if not messages:
        print("No messages to upload")
        return True

    total_batches = (len(messages) + batch_size - 1) // batch_size
    successful_batches = 0
    assert total_batches >= 0, (
        f"total_batches must be non-negative, got {total_batches}"
    )

    for i in range(0, len(messages), batch_size):
        batch = messages[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        assert isinstance(batch, list), f"batch must be list, got {type(batch)}"
        assert len(batch) > 0, f"batch cannot be empty"
        assert len(batch) <= batch_size, (
            f"batch size {len(batch)} exceeds maximum {batch_size}"
        )
        assert all(isinstance(msg, dict) for msg in batch), (
            "All batch messages must be dictionaries"
        )

        print(f"Uploading batch {batch_num}/{total_batches} ({len(batch)} messages)")

        for attempt in range(max_retries):
            try:
                response = client.client.table(table_name).insert(batch).execute()
                assert response is not None, "Database response cannot be None"

                if response.data:
                    assert isinstance(response.data, list), (
                        f"response.data must be list, got {type(response.data)}"
                    )
                    successful_batches += 1
                    print(f"Batch {batch_num} uploaded successfully")
                    break
                else:
                    print(f"Batch {batch_num} failed - attempt {attempt + 1}")

            except Exception as e:
                print(f"Batch {batch_num} error (attempt {attempt + 1}): {e}")

            if attempt == max_retries - 1:
                print(f"Batch {batch_num} failed after {max_retries} attempts")
                return False

    assert successful_batches <= total_batches, (
        f"successful_batches {successful_batches} cannot exceed total_batches {total_batches}"
    )
    print(f"Successfully uploaded {successful_batches}/{total_batches} batches")
    return successful_batches == total_batches


def supabase_check_message_exists(
    message_id: int,
    *,
    client: DatabaseClient,
    table_name: str = "raw_data",
) -> bool:
    """
    Check if a message already exists in the database.
    """
    assert isinstance(message_id, int), (
        f"message_id must be int, got {type(message_id)}"
    )
    assert message_id >= 0, f"message_id must be non-negative, got {message_id}"
    assert isinstance(client, DatabaseClient), (
        f"client must be DatabaseClient, got {type(client)}"
    )
    assert isinstance(table_name, str), (
        f"table_name must be string, got {type(table_name)}"
    )
    assert len(table_name.strip()) > 0, "table_name cannot be empty"

    try:
        response = (
            client.client.table(table_name)
            .select("message_id")
            .eq("message_id", message_id)
            .execute()
        )
        assert response is not None, "Database response cannot be None"
        assert hasattr(response, "data"), "Response must have data attribute"
        assert isinstance(response.data, list), (
            f"response.data must be list, got {type(response.data)}"
        )

        result = len(response.data) > 0
        assert isinstance(result, bool), f"result must be bool, got {type(result)}"
        return result

    except Exception as e:
        print(f"Error checking message existence: {e}")
        return False


get_all_messages = partial(supabase_get_all_messages, client=db)
get_recent_messages = partial(supabase_get_recent_messages, client=db)
get_messages_by_timedelta = partial(supabase_get_messages_by_timedelta, client=db)
upload_messages = partial(supabase_upload_messages, client=db)
upsert_messages = partial(supabase_upsert_messages, client=db)
batch_upload_with_retry = partial(supabase_batch_upload_with_retry, client=db)
check_message_exists = partial(supabase_check_message_exists, client=db)
stream_all_messages = partial(supabase_stream_all_messages, client=db)
stream_recent_messages = partial(supabase_stream_recent_messages, client=db)
stream_messages_by_timedelta = partial(supabase_stream_messages_by_timedelta, client=db)
