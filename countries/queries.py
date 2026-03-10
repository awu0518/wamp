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

# In-memory cache for testing
country_cache = {}
_next_id = 1

SAMPLE_COUNTRY = {
    NAME: 'United States',
    ISO_CODE: 'US',
    REVIEW_COUNT: 0,
}


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
        countries = dbc.read_dict(COUNTRY_COLLECTION, key=NAME)
        # Add metadata to each country
        for name, country in countries.items():
            if REVIEW_COUNT not in country:
                country[REVIEW_COUNT] = 0
        return countries
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
    countries = read()
    if country_id not in countries:
        raise ValueError(f'No such country: {country_id}')
    return dict(countries[country_id])


def find_by_iso_code(iso_code: str) -> Optional[dict]:
    """
    Find a country by its ISO code (case-insensitive).
    Returns a copy of the country data if found, otherwise None.
    """
    if not isinstance(iso_code, str) or not iso_code:
        return None
    target = iso_code.strip().lower()
    countries = read()
    for rec in countries.values():
        code = rec.get(ISO_CODE)
        if isinstance(code, str) and code.lower() == target:
            return dict(rec)
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

    # Validate ISO code format (2-3 uppercase letters)
    validation.validate_iso_code(flds[ISO_CODE], 'iso_code')

    create_doc = {
        NAME: flds[NAME],
        ISO_CODE: flds[ISO_CODE],
        REVIEW_COUNT: 0,
    }

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
    # Validate input type
    if not isinstance(flds, dict):
        raise ValueError(f'Bad type for {type(flds)=}')

    # Validate no extra fields
    validation.validate_no_extra_fields(flds, [NAME, ISO_CODE])

    # Validate name if present
    if NAME in flds:
        validation.validate_string_length(flds[NAME], 'name',
                                          min_length=1, max_length=100)

    # Validate ISO code if present
    if ISO_CODE in flds:
        validation.validate_iso_code(flds[ISO_CODE], 'iso_code')

    countries = read()
    if country_id not in countries:
        raise ValueError(f'No such country: {country_id}')

    # Update cache if present
    if country_id in country_cache:
        country_cache[country_id].update(flds)
        return True

    # Otherwise update database
    dbc.update(COUNTRY_COLLECTION, {NAME: country_id}, flds)
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

    for country_name, country_data in countries.items():
        match = True

        if name and name.lower() not in \
                country_data.get(NAME, '').lower():
            match = False

        if iso_code and country_data.get(ISO_CODE, '').lower() != \
                iso_code.lower():
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
