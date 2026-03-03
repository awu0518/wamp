"""
This file deals with our city-level data.
"""
from random import randint
import time
import data.db_connect as dbc
import validation

MIN_ID_LEN = 1
CITY_COLLECTION = 'cities'

ID = 'id'
NAME = 'name'
STATE_CODE = 'state_code'
REVIEW_COUNT = 'review_count'

# Cache configuration
CACHE_MAX_SIZE = 100  # Maximum number of cities to cache
CACHE_EXPIRY_SECONDS = 300  # 5 minutes
city_cache = {}  # {city_name: {'data': city_data, 'timestamp': time}}

SAMPLE_CITY = {
    NAME: 'New York',
    STATE_CODE: 'NY',
    REVIEW_COUNT: 0,
}


# db connection placeholder
def db_connect(success_ratio: int) -> bool:
    """
    Return True if connected, False if not.
    Simulates database connection with configurable success rate.
    Returns:
        bool: True if connection succeeds, False otherwise
    """
    return randint(1, success_ratio) % success_ratio == 0


# Returns if a given ID is a valid city ID.
def is_valid_id(_id: str) -> bool:
    if not isinstance(_id, str):
        return False
    if len(_id) < MIN_ID_LEN:
        return False
    return True


def _is_cache_entry_valid(city_name: str) -> bool:
    """Check if a specific cache entry is still valid."""
    if city_name not in city_cache:
        return False
    current_time = time.time()
    valid_time = current_time - city_cache[city_name]['timestamp']
    return valid_time < CACHE_EXPIRY_SECONDS


def _evict_oldest_cache_entry():
    """Remove the oldest cache entry when cache is full."""
    if not city_cache:
        return
    keys = city_cache.keys()
    oldest_key = min(keys, key=lambda k: city_cache[k]['timestamp'])
    del city_cache[oldest_key]


def _cache_city(city_name: str, city_data: dict):
    """Add a city to cache, evicting old entries if necessary."""
    if len(city_cache) >= CACHE_MAX_SIZE:
        _evict_oldest_cache_entry()
    city_cache[city_name] = {
        'data': dict(city_data),  # Store a copy
        'timestamp': time.time()
    }


def _invalidate_cache_entry(city_name: str):
    """Remove a specific city from cache."""
    if city_name in city_cache:
        del city_cache[city_name]


def num_cities() -> int:
    return len(read())


def read() -> dict:
    """
    Return all cities using cache-first approach.
    """
    # For bulk read, we'll still go to database to ensure freshness
    # but cache individual entries for subsequent read_one calls
    cities = dbc.read_dict(CITY_COLLECTION, key=NAME)
    # Add metadata to each city
    for city_name, city_data in cities.items():
        if REVIEW_COUNT not in city_data:
            city_data[REVIEW_COUNT] = 0
        _cache_city(city_name, city_data)
    return cities


def read_paginated(page: int = 1,
                   limit: int = 50,
                   sort_by: str = NAME,
                   order: str = 'asc') -> dict:
    """
    Return paginated cities list with metadata.
    """
    cond = isinstance(order, str) and order.lower() == 'desc'
    direction = -1 if cond else 1
    return dbc.find_paginated(
        collection=CITY_COLLECTION,
        db=dbc.SE_DB,
        sort=[(sort_by, direction)],
        page=page,
        limit=limit,
        no_id=True
    )


def create(flds: dict) -> str:
    # Validate input
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

    new_id = dbc.create(CITY_COLLECTION, create_doc)
    # Invalidate cache entry if it exists (unlikely but possible)
    city_name = flds.get(NAME)
    if city_name:
        _invalidate_cache_entry(city_name)
    return str(new_id.inserted_id)


def delete(name: str, state_code: str) -> bool:
    ret = dbc.delete(CITY_COLLECTION, {NAME: name, STATE_CODE: state_code})
    if ret < 1:
        raise ValueError(f'City not found: {name}, {state_code}')
    # Invalidate cache entry
    _invalidate_cache_entry(name)
    return ret


def increment_review_count(city_name: str, state_code: str,
                           increment_by: int = 1) -> bool:
    """
    Increment review_count for a city identified by name and state_code.

    Args:
        city_name: City name key
        state_code: Two-letter state code
        increment_by: Positive increment amount (default 1)

    Returns:
        True if increment succeeds
    """
    if not isinstance(city_name, str) or not city_name.strip():
        raise ValueError('city_name must be a non-empty string')
    validation.validate_state_code(state_code, 'state_code')
    if not isinstance(increment_by, int) or increment_by < 1:
        raise ValueError('increment_by must be a positive integer')

    normalized_name = city_name.strip()
    normalized_state_code = state_code.strip().upper()
    query_filter = {NAME: normalized_name, STATE_CODE: normalized_state_code}

    if normalized_name in city_cache:
        cache_data = city_cache[normalized_name].get('data', {})
        if cache_data.get(STATE_CODE) == normalized_state_code:
            current_count = cache_data.get(REVIEW_COUNT, 0)
            cache_data[REVIEW_COUNT] = current_count + increment_by
            city_cache[normalized_name]['timestamp'] = time.time()

    try:
        client = dbc.get_client()
        result = client[dbc.SE_DB][CITY_COLLECTION].update_one(
            query_filter,
            {'$inc': {REVIEW_COUNT: increment_by}}
        )
        if result.matched_count < 1:
            raise ValueError(
                f'City not found: {normalized_name}, {normalized_state_code}'
            )
    except Exception:
        raise

    return True


def read_one(city_id: str) -> dict:
    """
    Retrieve a single city by ID with cache-first lookup.
    Returns a copy of the city data.
    """
    # Check cache first
    if _is_cache_entry_valid(city_id):
        return dict(city_cache[city_id]['data'])  # Return a copy
    # Cache miss - fetch from database
    cities = read()
    if city_id not in cities:
        raise ValueError(f'No such city: {city_id}')
    city_data = cities[city_id]
    _cache_city(city_id, city_data)
    return dict(city_data)


def update(city_id: str, flds: dict) -> bool:
    """
    Update an existing city with new field values.
    Returns True on success.
    """
    # Validate input type
    if not isinstance(flds, dict):
        raise ValueError(f'Bad type for {type(flds)=}')

    # Validate no extra fields
    validation.validate_no_extra_fields(flds, [NAME, STATE_CODE])

    # Validate name if present
    if NAME in flds:
        validation.validate_string_length(flds[NAME], 'name',
                                          min_length=1, max_length=100)

    # Validate state code if present
    if STATE_CODE in flds:
        validation.validate_state_code(flds[STATE_CODE], 'state_code')

    cities = read()
    if city_id not in cities:
        raise ValueError(f'No such city: {city_id}')
    # If updating the name, we need to handle the key change
    if NAME in flds and flds[NAME] != city_id:
        # Delete old record and create new one with updated name
        old_city = cities[city_id].copy()
        old_city.update(flds)
        # Remove metadata fields before creating
        old_city.pop(REVIEW_COUNT, None)
        delete(city_id, old_city.get(STATE_CODE, ''))
        create(old_city)
        _invalidate_cache_entry(city_id)
    else:
        # Regular update
        dbc.update(CITY_COLLECTION, {NAME: city_id}, flds)
        _invalidate_cache_entry(city_id)
    return True


def search(name: str = None, state_code: str = None) -> dict:
    """
    Search cities by name and/or state_code (case-insensitive).
    Args:
        name: City name substring to search for
        state_code: State code to filter by
    Returns:
        Dictionary of matching cities
    """
    cities = read()
    results = {}
    for city_name, city_data in cities.items():
        match = True
        if name and name.lower() not in city_data.get(NAME, '').lower():
            match = False
        cond = city_data.get(STATE_CODE, '').lower() != state_code.lower()
        if state_code and cond:
            match = False
        if match:
            results[city_name] = city_data
    return results


def bulk_create(records: list) -> dict:
    """
    Create multiple cities at once.

    Args:
        records: List of city dictionaries to create

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
    Update multiple cities at once.

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

        city_id = update_item.get('id')
        fields = update_item.get('fields')

        if not city_id or not fields:
            results["failed"] += 1
            results["errors"].append({
                "id": city_id or "unknown",
                "error": "Update item must have 'id' and 'fields'"
            })
            continue

        try:
            update(city_id, fields)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": city_id,
                "error": str(e)
            })

    return results


def bulk_delete(deletes: list) -> dict:
    """
    Delete multiple cities at once.

    Args:
        deletes: List of delete dictionaries with 'name' and 'state_code'
                 Example: [{"name": "New York", "state_code": "NY"}, ...]

    Returns:
        Dictionary with success count, failure count, and errors
    """
    if not isinstance(deletes, list):
        raise ValueError("Deletes must be a list")

    results = {
        "success": 0,
        "failed": 0,
        "errors": []
    }

    for delete_item in deletes:
        if not isinstance(delete_item, dict):
            results["failed"] += 1
            results["errors"].append({
                "id": "unknown",
                "error": "Delete item must be a dictionary"
            })
            continue

        city_name = delete_item.get('name')
        state_code = delete_item.get('state_code')

        if not city_name or not state_code:
            results["failed"] += 1
            results["errors"].append({
                "id": city_name or "unknown",
                "error": "Delete item must have 'name' and 'state_code'"
            })
            continue

        try:
            delete(city_name, state_code)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": f"{city_name}, {state_code}",
                "error": str(e)
            })

    return results
