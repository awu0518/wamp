"""
This file deals with our state-level data.
"""
import csv
import io
import json
import time
from random import randint
from typing import Optional
import data.db_connect as dbc
import validation

MIN_ID_LEN = 1
STATE_COLLECTION = 'states'

ID = 'id'
NAME = 'name'
STATE_CODE = 'state_code'
ABBREVIATION = 'abbreviation'  # Alias for state_code
REVIEW_COUNT = 'review_count'

state_cache = {}
CACHE_EXPIRY_SECONDS = 300  # 5 minutes

SAMPLE_STATE = {
    NAME: 'New York',
    STATE_CODE: 'NY',
    REVIEW_COUNT: 0,
}


def db_connect(success_ratio: int) -> bool:
    """
    Return True if connected, False if not.
    Simulates database connection with configurable success rate.
    Returns:
        bool: True if connection succeeds, False otherwise
    """
    return randint(1, success_ratio) % success_ratio == 0


# Returns if a given ID is a valid state ID.
def is_valid_id(_id: str) -> bool:
    if not isinstance(_id, str):
        return False
    if len(_id) < MIN_ID_LEN:
        return False
    return True


def num_states() -> int:
    return len(read())


def read() -> dict:
    """Read all states from MongoDB as a dictionary."""
    states = dbc.read_dict(STATE_COLLECTION, key=NAME)

    # Add metadata to each state
    for name, data in states.items():
        if REVIEW_COUNT not in data:
            data[REVIEW_COUNT] = 0

    # Update cache with timestamp
    current_time = time.time()
    for name, data in states.items():
        state_cache[name] = {
            'data': data,
            'timestamp': current_time
        }

    return states


def read_paginated(page: int = 1, limit: int = 50,
                   sort_by: str = NAME, order: str = 'asc') -> dict:
    """
    Return paginated states list with metadata.
    """
    direction = -1 if isinstance(order, str) and order.lower() == 'desc' else 1
    return dbc.find_paginated(
        collection=STATE_COLLECTION,
        db=dbc.SE_DB,
        sort=[(sort_by, direction)],
        page=page,
        limit=limit,
        no_id=True
    )


def read_one(state_id: str) -> dict:
    """
    Retrieve a single state by ID (name).
    Returns a copy of the state data.
    """
    # Check cache and expiration
    if state_id in state_cache:
        entry = state_cache[state_id]
        if time.time() - entry['timestamp'] < CACHE_EXPIRY_SECONDS:
            return entry['data'].copy()
        else:
            del state_cache[state_id]  # Expired

    states = read()
    if state_id not in states:
        raise ValueError(f'No such state: {state_id}')
    return dict(states[state_id])


def find_by_state_code(state_code: str) -> Optional[dict]:
    """
    Find a state by its state code (case-insensitive).
    Returns a copy of the state data if found, otherwise None.
    """
    if not isinstance(state_code, str) or not state_code:
        return None
    target = state_code.strip().upper()
    states = read()
    for rec in states.values():
        code = rec.get(STATE_CODE)
        if isinstance(code, str) and code.upper() == target:
            return dict(rec)
    return None


def create(flds: dict) -> str:
    """Create a new state."""
    # Validate required fields
    validation.validate_required_fields(flds, [NAME, STATE_CODE])

    # Validate no extra fields
    validation.validate_no_extra_fields(flds, [NAME, STATE_CODE])

    # Validate name
    validation.validate_string_length(flds[NAME], 'name',
                                      min_length=1, max_length=100)

    # Validate state code format (2 uppercase letters)
    validation.validate_state_code(flds[STATE_CODE], 'state_code')

    create_doc = {
        NAME: flds[NAME],
        STATE_CODE: flds[STATE_CODE],
        REVIEW_COUNT: 0,
    }

    new_id = dbc.create(STATE_COLLECTION, create_doc)

    # Update cache
    if NAME in create_doc:
        state_cache[create_doc[NAME]] = {
            'data': create_doc.copy(),
            'timestamp': time.time()
        }

    return str(new_id.inserted_id)


def delete(state_id: str) -> bool:
    """Delete a state by ID (name)."""
    states = read()
    if state_id not in states:
        raise ValueError(f'No such state: {state_id}')
    ret = dbc.delete(STATE_COLLECTION, {NAME: state_id})
    if ret < 1:
        raise ValueError(f'State not found: {state_id}')

    # Remove from cache
    if state_id in state_cache:
        del state_cache[state_id]

    return True


def increment_review_count(state_name: str, increment_by: int = 1) -> bool:
    """
    Increment review_count for a state identified by name.

    Args:
        state_name: State name key
        increment_by: Positive increment amount (default 1)

    Returns:
        True if increment succeeds
    """
    if not isinstance(state_name, str) or not state_name.strip():
        raise ValueError('state_name must be a non-empty string')
    if not isinstance(increment_by, int) or increment_by < 1:
        raise ValueError('increment_by must be a positive integer')

    normalized_name = state_name.strip()
    states = read()
    if normalized_name not in states:
        raise ValueError(f'No such state: {normalized_name}')

    if normalized_name in state_cache:
        state_cache[normalized_name]['data'][REVIEW_COUNT] = (
            state_cache[normalized_name]['data'].get(REVIEW_COUNT, 0)
            + increment_by
        )
        state_cache[normalized_name]['timestamp'] = time.time()

    try:
        client = dbc.get_client()
        result = client[dbc.SE_DB][STATE_COLLECTION].update_one(
            {NAME: normalized_name},
            {'$inc': {REVIEW_COUNT: increment_by}}
        )
        if normalized_name not in state_cache and result.matched_count < 1:
            raise ValueError(f'State not found: {normalized_name}')
    except Exception:
        if normalized_name in state_cache:
            return True
        raise

    return True


def update(state_id: str, flds: dict) -> bool:
    """
    Update an existing state with new field values.
    Returns True on success.
    """
    # Validate input type
    if not isinstance(flds, dict):
        raise ValueError(f'Bad type for {type(flds)=}')

    # Validate no extra fields
    allowed = [NAME, STATE_CODE]
    validation.validate_no_extra_fields(flds, allowed)

    # Validate name if present
    if NAME in flds:
        validation.validate_string_length(flds[NAME], 'name',
                                          min_length=1, max_length=100)

    # Validate state code if present
    if STATE_CODE in flds:
        validation.validate_state_code(flds[STATE_CODE], 'state_code')

    states = read()
    if state_id not in states:
        raise ValueError(f'No such state: {state_id}')

    # Update cache
    if state_id in state_cache:
        # If name is changed, we need to handle key change
        if NAME in flds and flds[NAME] != state_id:
            del state_cache[state_id]
            # We don't have the full new record to re-cache safely
            # without a read, so just letting it be a cache miss next time
            # is safer/simpler
        else:
            # Update existing cache entry
            if state_id in state_cache:
                state_cache[state_id]['data'].update(flds)
                state_cache[state_id]['timestamp'] = time.time()

    dbc.update(STATE_COLLECTION, {NAME: state_id}, flds)
    return True


def search(name: str = None, state_code: str = None) -> dict:
    """
    Search states by name and/or state_code (case-insensitive).

    Args:
        name: State name substring to search for
        state_code: State code to filter by (exact match)
    Returns:
        Dictionary of matching states
    """
    states = read()
    results = {}

    for state_name, state_data in states.items():
        match = True

        if name and name.lower() not in state_data.get(NAME, '').lower():
            match = False

        if state_code:
            data_code = state_data.get(STATE_CODE, '').upper()
            if data_code != state_code.upper():
                match = False

        if match:
            results[state_name] = state_data

    return results


def bulk_create(records: list) -> dict:
    """
    Create multiple states at once.

    Args:
        records: List of state dictionaries to create

    Returns:
        Dictionary with success count, failure count, errors, and created IDs
    """
    if not isinstance(records, list):
        raise ValueError("Records must be a list")

    results = {
        "success": 0,
        "failed": 0,
        "errors": [],
        "ids": []
    }

    for index, record in enumerate(records):
        try:
            new_id = create(record)
            results["ids"].append(new_id)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "index": index,
                "record": record.get(NAME, f"record_{index}"),
                "error": str(e)
            })

    return results


def bulk_update(updates: list) -> dict:
    """
    Update multiple states at once.

    Args:
        updates: List of update dictionaries with 'id' and 'fields'
                 Example: [{"id": "New York",
                            "fields": {"state_code": "NY"}}, ...]

    Returns:
        Dictionary with success count, failure count, and errors
    """
    if not isinstance(updates, list):
        raise ValueError("Updates must be a list")

    results = {
        "success": 0,
        "failed": 0,
        "errors": []
    }

    for update_item in updates:
        if not isinstance(update_item, dict):
            results["failed"] += 1
            results["errors"].append({
                "id": "unknown",
                "error": "Update item must be a dictionary"
            })
            continue

        state_id = update_item.get('id')
        fields = update_item.get('fields')

        if not state_id or not fields:
            results["failed"] += 1
            results["errors"].append({
                "id": state_id or "unknown",
                "error": "Update item must have 'id' and 'fields'"
            })
            continue

        try:
            update(state_id, fields)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": state_id,
                "error": str(e)
            })

    return results


def bulk_delete(ids: list) -> dict:
    """
    Delete multiple states at once.

    Args:
        ids: List of state IDs (names) to delete

    Returns:
        Dictionary with success count, failure count, and errors
    """
    if not isinstance(ids, list):
        raise ValueError("IDs must be a list")

    results = {
        "success": 0,
        "failed": 0,
        "errors": []
    }

    for state_id in ids:
        try:
            delete(state_id)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": state_id,
                "error": str(e)
            })

    return results


def export_to_json(states_data: dict = None, indent: int = 2) -> str:
    """
    Export states data to JSON format.

    Args:
        states_data: Dictionary of states to export.
                     If None, exports all states.
        indent: Number of spaces for JSON indentation (default 2)

    Returns:
        JSON string representation of the states data
    """
    if states_data is None:
        states_data = read()

    # Convert to list format for cleaner JSON output
    states_list = []
    for name, data in states_data.items():
        state_record = {NAME: name}
        state_record.update({k: v for k, v in data.items() if k != '_id'})
        states_list.append(state_record)

    return json.dumps(states_list, indent=indent)


def export_to_csv(states_data: dict = None) -> str:
    """
    Export states data to CSV format.

    Args:
        states_data: Dictionary of states to export.
                     If None, exports all states.

    Returns:
        CSV string representation of the states data
    """
    if states_data is None:
        states_data = read()

    if not states_data:
        return ""

    # Determine all possible fields from the data
    all_fields = set()
    for data in states_data.values():
        all_fields.update(data.keys())

    # Remove MongoDB _id field and ensure NAME is first
    all_fields.discard('_id')
    all_fields.discard(NAME)
    fieldnames = [NAME] + sorted(all_fields)

    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=fieldnames, extrasaction='ignore'
    )
    writer.writeheader()

    for name, data in states_data.items():
        row = {NAME: name}
        row.update({k: v for k, v in data.items() if k != '_id'})
        writer.writerow(row)

    return output.getvalue()
