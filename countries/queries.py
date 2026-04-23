"""
This file deals with our country-level data.
"""
import csv
import io
import json
from random import randint
from typing import Optional
import data.db_connect as dbc
import validation

MIN_ID_LEN = 1
COUNTRY_COLLECTION = 'countries'

ID = 'id'
NAME = 'name'
ISO_CODE = 'iso_code'
REVIEW_COUNT = 'review_count'

COUNTRY_ISO_UNIQUE_INDEX = 'countries_iso_code_unique_idx'
_indexes_initialized = False

# In-memory cache for testing
country_cache = {}
_next_id = 1

SAMPLE_COUNTRY = {
    NAME: 'United States',
    ISO_CODE: 'US',
    REVIEW_COUNT: 0,
}


def _normalize_iso_code(value: str) -> str:
    """Normalize and validate ISO code to a canonical uppercase form."""
    if not isinstance(value, str):
        raise ValueError('iso_code must be a string')
    normalized = value.strip().upper()
    validation.validate_iso_code(normalized, 'iso_code')
    return normalized


def _stable_country_sort_key(item: tuple[str, dict]) -> tuple[str, str]:
    """Deterministic order for lookup stability across cache and DB results."""
    rec_key, rec = item
    name = rec.get(NAME, rec_key)
    name_key = str(name).strip().lower() if name is not None else ''
    rec_key_norm = str(rec_key).strip().lower()
    return (name_key, rec_key_norm)


def _ensure_indexes() -> None:
    """Best-effort unique index on iso_code for DB-backed deployments."""
    global _indexes_initialized
    if _indexes_initialized:
        return

    try:
        client = dbc.get_client()
        client[dbc.SE_DB][COUNTRY_COLLECTION].create_index(
            [(ISO_CODE, 1)],
            name=COUNTRY_ISO_UNIQUE_INDEX,
            unique=True,
            background=True,
        )
    except Exception:
        # Keep this non-fatal for test/local modes that don't use Mongo.
        pass

    _indexes_initialized = True


def _find_iso_matches(
    iso_code: str,
    use_cache_only: bool = False,
) -> list[tuple[str, dict]]:
    """Return all records matching ISO code, sorted deterministically."""
    target = _normalize_iso_code(iso_code)
    countries = dict(country_cache) if use_cache_only else read()
    matches = []

    for rec_key, rec in countries.items():
        code = rec.get(ISO_CODE)
        if isinstance(code, str) and code.strip().upper() == target:
            matches.append((rec_key, rec))

    matches.sort(key=_stable_country_sort_key)
    return matches


# db connection placeholder
def db_connect(success_ratio: int) -> bool:
    """
    Return True if connected, False if not.
    Simulates database connection with configurable success rate.
    """
    return randint(1, success_ratio) % success_ratio == 0


# Returns if a given ID is a valid country ID.
def is_valid_id(_id: str) -> bool:
    if not isinstance(_id, str):
        return False
    if len(_id) < MIN_ID_LEN:
        return False
    return True


def num_countries() -> int:
    return len(read())


def read() -> dict:
    """Read all countries from cache and/or MongoDB as a dictionary."""
    # Return cache if it has data (for testing)
    if country_cache:
        return dict(country_cache)
    # Otherwise, read from database
    try:
        _ensure_indexes()
        countries = dbc.read_dict(COUNTRY_COLLECTION, key=NAME)
        # Add metadata to each country
        for name, country in countries.items():
            if REVIEW_COUNT not in country:
                country[REVIEW_COUNT] = 0
            if ISO_CODE in country and isinstance(country[ISO_CODE], str):
                country[ISO_CODE] = country[ISO_CODE].strip().upper()
        # Deterministic dictionary ordering for stable iteration behavior.
        return dict(sorted(countries.items(), key=_stable_country_sort_key))
    except Exception:
        return {}


def read_paginated(page: int = 1, limit: int = 50,
                   sort_by: str = NAME, order: str = 'asc') -> dict:
    """
    Return paginated countries list with metadata.
    """
    direction = -1 if isinstance(order, str) and \
        order.lower() == 'desc' else 1
    return dbc.find_paginated(
        collection=COUNTRY_COLLECTION,
        db=dbc.SE_DB,
        sort=[(sort_by, direction)],
        page=page,
        limit=limit,
        no_id=True
    )


def read_one(country_id: str) -> dict:
    """
    Retrieve a single country by ID.
    Returns a copy of the country data.
    """
    if not isinstance(country_id, str) or not country_id.strip():
        raise ValueError('country_id must be a non-empty string')

    countries = read()
    if country_id not in countries:
        raise ValueError(f'No such country: {country_id}')
    return dict(countries[country_id])


def read_one_by_iso_code(iso_code: str) -> dict:
    """
    Retrieve a single country by ISO code (case-insensitive).

    Returns a copy of the first deterministic match.

    Raises:
        ValueError: If iso_code is invalid or no country is found.
    """
    matches = _find_iso_matches(iso_code)
    if not matches:
        normalized_iso = _normalize_iso_code(iso_code)
        raise ValueError(f'No such country with iso_code: {normalized_iso}')
    return dict(matches[0][1])


def find_by_iso_code(iso_code: str) -> Optional[dict]:
    """
    Find a country by its ISO code (case-insensitive).
    Returns a copy of the country data if found, otherwise None.
    """
    if not isinstance(iso_code, str) or not iso_code.strip():
        return None

    try:
        return read_one_by_iso_code(iso_code)
    except ValueError:
        return None


def create(flds: dict) -> str:
    global _next_id
    # Validate input
    validation.validate_required_fields(flds, [NAME, ISO_CODE])

    # Validate no extra fields
    validation.validate_no_extra_fields(flds, [NAME, ISO_CODE])

    # Validate name
    validation.validate_string_length(flds[NAME], 'name',
                                      min_length=1, max_length=100)

    normalized_iso = _normalize_iso_code(flds[ISO_CODE])

    # Enforce ISO uniqueness against the same in-memory store used by create.
    if _find_iso_matches(normalized_iso, use_cache_only=True):
        raise ValueError(
            f'Country with iso_code already exists: {normalized_iso}'
        )

    create_doc = {
        NAME: flds[NAME].strip(),
        ISO_CODE: normalized_iso,
        REVIEW_COUNT: 0,
    }

    _ensure_indexes()

    # Use cache for testing
    new_id = str(_next_id)
    _next_id += 1
    country_cache[new_id] = dict(create_doc)

    # Also store in database if available
    # Pass a copy to avoid modifying the original flds dict
    try:
        dbc.create(COUNTRY_COLLECTION, dict(create_doc))
    except Exception:
        pass  # Ignore DB errors during testing

    return new_id


def increment_review_count(country_name: str, increment_by: int = 1) -> bool:
    """
    Increment review_count for a country identified by name.

    Args:
        country_name: Country name key
        increment_by: Positive increment amount (default 1)

    Returns:
        True if increment succeeds
    """
    if not isinstance(country_name, str) or not country_name.strip():
        raise ValueError('country_name must be a non-empty string')
    if not isinstance(increment_by, int) or increment_by < 1:
        raise ValueError('increment_by must be a positive integer')

    normalized_name = country_name.strip()
    countries = read()
    if normalized_name not in countries:
        raise ValueError(f'No such country: {normalized_name}')

    if normalized_name in country_cache:
        country_cache[normalized_name][REVIEW_COUNT] = (
            country_cache[normalized_name].get(REVIEW_COUNT, 0) + increment_by
        )

    try:
        client = dbc.get_client()
        result = client[dbc.SE_DB][COUNTRY_COLLECTION].update_one(
            {NAME: normalized_name},
            {'$inc': {REVIEW_COUNT: increment_by}}
        )
        if normalized_name not in country_cache and result.matched_count < 1:
            raise ValueError(f'Country not found: {normalized_name}')
    except Exception:
        if normalized_name in country_cache:
            return True
        raise

    return True


def delete(country_id: str) -> bool:
    if not isinstance(country_id, str) or not country_id.strip():
        raise ValueError('country_id must be a non-empty string')

    countries = read()
    if country_id not in countries:
        raise ValueError(f'No such country: {country_id}')

    # Remove from cache if present
    if country_id in country_cache:
        del country_cache[country_id]
        return True

    # Otherwise try to delete from database
    ret = dbc.delete(COUNTRY_COLLECTION, {NAME: country_id})
    if ret < 1:
        raise ValueError(f'Country not found: {country_id}')
    return True


def update(country_id: str, flds: dict) -> bool:
    """
    Update an existing country with new field values.
    Returns True on success.
    """
    if not isinstance(country_id, str) or not country_id.strip():
        raise ValueError('country_id must be a non-empty string')

    # Validate input type
    if not isinstance(flds, dict):
        raise ValueError(f'Bad type for {type(flds)=}')

    if not flds:
        raise ValueError('No fields provided for update')

    # Validate no extra fields
    validation.validate_no_extra_fields(flds, [NAME, ISO_CODE])

    # Validate name if present
    if NAME in flds:
        validation.validate_string_length(flds[NAME], 'name',
                                          min_length=1, max_length=100)

    normalized_iso = None
    # Validate ISO code if present
    if ISO_CODE in flds:
        normalized_iso = _normalize_iso_code(flds[ISO_CODE])

    countries = read()
    if country_id not in countries:
        raise ValueError(f'No such country: {country_id}')

    current = countries[country_id]
    current_iso = str(current.get(ISO_CODE, '')).strip().upper()

    # Enforce uniqueness if ISO is changing.
    if normalized_iso and normalized_iso != current_iso:
        existing = _find_iso_matches(
            normalized_iso,
            use_cache_only=(country_id in country_cache),
        )
        conflict = any(match_key != country_id for match_key, _ in existing)
        if conflict:
            raise ValueError(
                f'Country with iso_code already exists: {normalized_iso}'
            )

    update_doc = dict(flds)
    if NAME in update_doc and isinstance(update_doc[NAME], str):
        update_doc[NAME] = update_doc[NAME].strip()
    if normalized_iso:
        update_doc[ISO_CODE] = normalized_iso

    _ensure_indexes()

    # Update cache if present
    if country_id in country_cache:
        country_cache[country_id].update(update_doc)
        return True

    # Otherwise update database
    dbc.update(COUNTRY_COLLECTION, {NAME: country_id}, update_doc)
    return True


def search(name: str = None, iso_code: str = None) -> dict:
    """
    Search countries by name and/or ISO code (case-insensitive).

    Args:
        name: Country name substring to search for
        iso_code: ISO code to filter by

    Returns:
        Dictionary of matching countries
    """
    countries = read()
    results = {}
    normalized_iso = None
    if iso_code is not None:
        try:
            normalized_iso = _normalize_iso_code(iso_code)
        except Exception:
            # Search should return empty results for invalid iso filters.
            return {}

    for country_name, country_data in countries.items():
        match = True

        if name and name.lower() not in \
                country_data.get(NAME, '').lower():
            match = False

        if (
            normalized_iso
            and str(country_data.get(ISO_CODE, '')).strip().upper()
            != normalized_iso
        ):
            match = False

        if match:
            results[country_name] = country_data

    return results


def bulk_create(records: list) -> dict:
    """
    Create multiple countries at once.

    Args:
        records: List of country dictionaries to create

    Returns:
        Dictionary with success count, failure count, errors, and created IDs
        {
            "success": int,
            "failed": int,
            "errors": [{"index": int, "error": str}],
            "ids": [str]
        }
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
    Update multiple countries at once.

    Args:
        updates: List of update dictionaries with 'id' and 'fields'
                 Example: [{"id": "USA", "fields": {"iso_code": "US"}}, ...]

    Returns:
        Dictionary with success count, failure count, and errors
        {
            "success": int,
            "failed": int,
            "errors": [{"id": str, "error": str}]
        }
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

        country_id = update_item.get('id')
        fields = update_item.get('fields')

        if not country_id or not fields:
            results["failed"] += 1
            results["errors"].append({
                "id": country_id or "unknown",
                "error": "Update item must have 'id' and 'fields'"
            })
            continue

        try:
            update(country_id, fields)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": country_id,
                "error": str(e)
            })

    return results


def bulk_delete(ids: list) -> dict:
    """
    Delete multiple countries at once.

    Args:
        ids: List of country IDs (names) to delete

    Returns:
        Dictionary with success count, failure count, and errors
        {
            "success": int,
            "failed": int,
            "errors": [{"id": str, "error": str}]
        }
    """
    if not isinstance(ids, list):
        raise ValueError("IDs must be a list")

    results = {
        "success": 0,
        "failed": 0,
        "errors": []
    }

    for country_id in ids:
        try:
            delete(country_id)
            results["success"] += 1
        except (ValueError, validation.ValidationError) as e:
            results["failed"] += 1
            results["errors"].append({
                "id": country_id,
                "error": str(e)
            })

    return results


def export_to_json(countries_data: dict = None, indent: int = 2) -> str:
    """
    Export countries data to JSON format.

    Args:
        countries_data: Dictionary of countries to export.
                        If None, exports all countries.
        indent: Number of spaces for JSON indentation (default 2)

    Returns:
        JSON string representation of the countries data
    """
    if countries_data is None:
        countries_data = read()

    # Convert to list format for cleaner JSON output
    countries_list = []
    for name, data in countries_data.items():
        country_record = {NAME: name}
        country_record.update({k: v for k, v in data.items() if k != '_id'})
        countries_list.append(country_record)

    return json.dumps(countries_list, indent=indent)


def export_to_csv(countries_data: dict = None) -> str:
    """
    Export countries data to CSV format.

    Args:
        countries_data: Dictionary of countries to export.
                        If None, exports all countries.

    Returns:
        CSV string representation of the countries data
    """
    if countries_data is None:
        countries_data = read()

    if not countries_data:
        return ""

    # Determine all possible fields from the data
    all_fields = set()
    for data in countries_data.values():
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

    for name, data in countries_data.items():
        row = {NAME: name}
        row.update({k: v for k, v in data.items() if k != '_id'})
        writer.writerow(row)

    return output.getvalue()
