"""
This file deals with journal entry data.
All journal interactions with MongoDB should be through this file.
"""
from datetime import datetime
from typing import Optional
from bson import ObjectId
import data.db_connect as dbc
import validation

JOURNAL_COLLECTION = 'journals'

# Field names
ID = '_id'
USER_ID = 'user_id'
TITLE = 'title'
BODY = 'body'
LOCATION_TYPE = 'location_type'
LOCATION_NAME = 'location_name'
STATE_CODE = 'state_code'
ISO_CODE = 'iso_code'
LAT = 'lat'
LNG = 'lng'
VISITED_AT = 'visited_at'
CREATED_AT = 'created_at'
UPDATED_AT = 'updated_at'

VALID_LOCATION_TYPES = ['country', 'state', 'city']
REQUIRED_FIELDS = [TITLE, LOCATION_TYPE, LOCATION_NAME]
ALLOWED_FIELDS = [
    TITLE, BODY, LOCATION_TYPE, LOCATION_NAME,
    STATE_CODE, ISO_CODE, LAT, LNG, VISITED_AT,
]
UPDATABLE_FIELDS = [TITLE, BODY, VISITED_AT]

SAMPLE_JOURNAL = {
    USER_ID: '507f1f77bcf86cd799439011',
    TITLE: 'My trip to NYC',
    BODY: 'Visited Central Park and Times Square.',
    LOCATION_TYPE: 'city',
    LOCATION_NAME: 'New York',
    STATE_CODE: 'NY',
}


@dbc.require_connection
def create(user_id: str, fields: dict) -> str:
    """
    Create a new journal entry for the given user.

    Args:
        user_id: The user's ID (from JWT).
        fields: Dict with title, location_type, location_name, and
                optional body, state_code, iso_code, lat, lng, visited_at.

    Returns:
        The new document's _id as a string.

    Raises:
        ValueError: On missing/invalid fields.
    """
    validation.validate_required_fields(fields, REQUIRED_FIELDS)
    validation.validate_no_extra_fields(fields, ALLOWED_FIELDS)
    validation.validate_string_length(
        fields[TITLE], 'title', min_length=1, max_length=200)
    validation.validate_enum(
        fields[LOCATION_TYPE], 'location_type', VALID_LOCATION_TYPES)
    validation.validate_string_length(
        fields[LOCATION_NAME], 'location_name', min_length=1, max_length=200)

    if BODY in fields and fields[BODY] is not None:
        validation.validate_string_length(
            fields[BODY], 'body', min_length=0, max_length=5000)

    now = datetime.utcnow()
    doc = {
        USER_ID: user_id,
        TITLE: fields[TITLE].strip(),
        BODY: fields.get(BODY, ''),
        LOCATION_TYPE: fields[LOCATION_TYPE],
        LOCATION_NAME: fields[LOCATION_NAME].strip(),
        STATE_CODE: fields.get(STATE_CODE, ''),
        ISO_CODE: fields.get(ISO_CODE, ''),
        LAT: fields.get(LAT),
        LNG: fields.get(LNG),
        VISITED_AT: fields.get(VISITED_AT, ''),
        CREATED_AT: now,
        UPDATED_AT: now,
    }

    result = dbc.create(JOURNAL_COLLECTION, doc)
    return str(result.inserted_id)


@dbc.require_connection
def read_by_user(
    user_id: str,
    location_type: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    """
    Return paginated journal entries for a user.

    Args:
        user_id: The user's ID.
        location_type: Optional filter ('country', 'state', 'city').
        page: 1-based page number.
        limit: Items per page.

    Returns:
        Paginated dict with 'items', 'page', 'limit', 'total', etc.
    """
    filt = {USER_ID: user_id}
    if location_type:
        validation.validate_enum(
            location_type, 'location_type', VALID_LOCATION_TYPES)
        filt[LOCATION_TYPE] = location_type

    return dbc.find_paginated(
        collection=JOURNAL_COLLECTION,
        filt=filt,
        sort=[(CREATED_AT, -1)],
        page=page,
        limit=limit,
        no_id=False,
    )


@dbc.require_connection
def read_one(journal_id: str, user_id: str) -> dict:
    """
    Retrieve a single journal entry, verifying ownership.

    Args:
        journal_id: The journal document's _id as a string.
        user_id: The requesting user's ID.

    Returns:
        The journal document dict.

    Raises:
        ValueError: If not found or not owned by user.
    """
    try:
        obj_id = ObjectId(journal_id)
    except Exception:
        raise ValueError(f'Invalid journal ID: {journal_id}')

    doc = dbc.read_one(JOURNAL_COLLECTION, {ID: obj_id})
    if not doc:
        raise ValueError(f'Journal not found: {journal_id}')
    if doc.get(USER_ID) != user_id:
        raise ValueError(f'Journal not found: {journal_id}')
    return doc


@dbc.require_connection
def update(journal_id: str, user_id: str, fields: dict) -> bool:
    """
    Update a journal entry's title, body, or visited_at.

    Args:
        journal_id: The journal document's _id as a string.
        user_id: The requesting user's ID.
        fields: Dict of fields to update (title, body, visited_at).

    Returns:
        True on success.

    Raises:
        ValueError: If journal not found, not owned, or invalid fields.
    """
    if not isinstance(fields, dict) or not fields:
        raise ValueError('No update fields provided')
    validation.validate_no_extra_fields(fields, UPDATABLE_FIELDS)

    if TITLE in fields:
        validation.validate_string_length(
            fields[TITLE], 'title', min_length=1, max_length=200)
    if BODY in fields and fields[BODY] is not None:
        validation.validate_string_length(
            fields[BODY], 'body', min_length=0, max_length=5000)

    doc = read_one(journal_id, user_id)

    update_dict = {}
    for key in UPDATABLE_FIELDS:
        if key in fields:
            update_dict[key] = fields[key]
    update_dict[UPDATED_AT] = datetime.utcnow()

    dbc.update(
        JOURNAL_COLLECTION,
        {ID: ObjectId(doc[ID])},
        update_dict,
    )
    return True


@dbc.require_connection
def delete(journal_id: str, user_id: str) -> bool:
    """
    Delete a journal entry, verifying ownership.

    Args:
        journal_id: The journal document's _id as a string.
        user_id: The requesting user's ID.

    Returns:
        True on success.

    Raises:
        ValueError: If not found or not owned by user.
    """
    doc = read_one(journal_id, user_id)
    ret = dbc.delete(JOURNAL_COLLECTION, {ID: ObjectId(doc[ID])})
    if ret < 1:
        raise ValueError(f'Journal not found: {journal_id}')
    return True


@dbc.require_connection
def count_by_user(user_id: str) -> int:
    """Return the total number of journals for a user."""
    client = dbc.get_client()
    return client[dbc.SE_DB][JOURNAL_COLLECTION].count_documents(
        {USER_ID: user_id}
    )
