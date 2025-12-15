# poe-supabase-utils

Supabase utilities for Poe audits project, providing convenient client and functions for message management.

## Features

- **DatabaseClient**: Frozen dataclass wrapping a Supabase client
- **Message operations**: Get, stream, upload, and upsert messages
- **Batch operations**: Upload with retry logic and conflict resolution
- **Timedelta filtering**: Retrieve messages from the last N minutes/hours/days

## Installation

```bash
uv add poe-supabase-utils
```

## Environment Variables

Required:
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role API key

Optional:
- `SUPABASE_ANON_KEY`: Public API key for client-side operations

## Usage

```python
from poe_supabase_utils import (
    get_all_messages,
    upload_messages,
    upsert_messages,
    DatabaseClient,
)
from datetime import timedelta

# Use default client (reads from environment)
messages = get_all_messages("my_table")

# Stream large datasets
for message in stream_all_messages("my_table"):
    process(message)

# Get recent messages
recent = get_recent_messages("my_table", number_of_messages=100)

# Get messages from last 24 hours
recent_24h = get_messages_by_timedelta("my_table", timedelta(hours=24))

# Upload messages
success = upload_messages("my_table", messages=[{"text": "hello"}])

# Upsert with conflict resolution
success = upsert_messages(
    "my_table",
    messages=[{"text": "hello", "timestamp": "2024-01-01T00:00:00"}],
    conflict_columns=["text"],
)

# Check if message exists
exists = check_message_exists(message_id=123)
```

## API Reference

### Functions

All functions support both:
1. **Simple usage** (uses shared `DatabaseClient` instance):
   ```python
   messages = get_all_messages("table_name")
   ```

2. **Custom client usage** (pass your own `DatabaseClient`):
   ```python
   custom_client = DatabaseClient()
   messages = supabase_get_all_messages("table_name", client=custom_client)
   ```

#### `get_all_messages(table_name, **kwargs) -> list[dict]`
Retrieve all messages from a table with batching.

#### `stream_all_messages(table_name, **kwargs) -> Iterator[dict]`
Stream all messages without loading them all into memory.

#### `get_recent_messages(table_name, number_of_messages, **kwargs) -> list[dict]`
Get the most recent N messages, ordered by timestamp descending.

#### `stream_recent_messages(table_name, number_of_messages, **kwargs) -> Iterator[dict]`
Stream the most recent N messages.

#### `get_messages_by_timedelta(table_name, time_delta, **kwargs) -> list[dict]`
Get messages from the last N time units.

#### `stream_messages_by_timedelta(table_name, time_delta, **kwargs) -> Iterator[dict]`
Stream messages from the last N time units.

#### `upload_messages(table_name, messages, **kwargs) -> bool`
Insert messages into a table. Returns `True` on success.

#### `upsert_messages(table_name, messages, *, conflict_columns=["text"], batch_size=500, **kwargs) -> bool`
Upsert messages with conflict resolution. Automatically deduplicates and batches.

#### `batch_upload_with_retry(table_name, messages, *, batch_size=1000, max_retries=3, **kwargs) -> bool`
Upload messages in batches with retry logic for large datasets.

#### `check_message_exists(message_id, *, table_name="raw_data", **kwargs) -> bool`
Check if a message already exists by ID.
