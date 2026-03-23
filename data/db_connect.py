"""
All interaction with MongoDB should be through this file!
We may be required to use a new database at any point.
"""
import os
import pymongo as pm
import time
from datetime import datetime
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError
from functools import wraps
from typing import Callable, TypeVar, Any, Optional
from bson import ObjectId

LOCAL = "0"
CLOUD = "1"

SE_DB = 'seDB'

client = None

MONGO_ID = '_id'


F = TypeVar('F', bound=Callable[..., Any])


def require_connection(func: F) -> F:
    """
    Decorator to ensure a Mongo client connection exists before DB operations.
    Calls connect_db() lazily when needed and preserves the wrapped signature.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        global client
        if client is None:
            connect_db()
        else:
            # Validate existing connection; reconnect if needed.
            try:
                client.admin.command('ping')
            except (ServerSelectionTimeoutError, PyMongoError):
                client = None
                connect_db()
        return func(*args, **kwargs)
    return wrapper  # type: ignore[return-value]


def _build_mongo_uri() -> Optional[str]:
    """
    Build a MongoDB connection URI from environment variables if provided.
    Priority:
      1) MONGO_URI
      2) CLOUD_MONGO == CLOUD -> construct from password (legacy behavior)
      3) None (let MongoClient use defaults, typically localhost)
    """
    # Highest priority: explicit URI
    explicit_uri = os.environ.get('MONGO_URI')
    if explicit_uri:
        return explicit_uri

    # Legacy cloud toggle behavior
    if os.environ.get('CLOUD_MONGO', LOCAL) == CLOUD:
        password = os.environ.get('MONGO_PASSWD')
        if not password:
            raise ValueError('You must set MONGO_PASSWD to use Mongo in the cloud.')
        # return (f'mongodb+srv://gcallah:{password}'
        #         + '@koukoumongo1.yud9b.mongodb.net/'
        #         + '?retryWrites=true&w=majority')
        return (f'mongodb+srv://limjiannn_db_user:{password}'
                + '@cluster0.o6ypt6r.mongodb.net/'
                + '?appName=Cluster0')

    # Fallback to None -> default local connection
    return None


def connect_db():
    """
    This provides a uniform way to connect to the DB across all uses.
    Returns a mongo client object... maybe we shouldn't?
    Also set global client variable.
    We should probably either return a client OR set a
    client global.
    """
    global client
    if client is None:  # not connected yet!
        print('Setting client because it is None.')
        max_retries = int(os.environ.get('MONGO_MAX_RETRIES', '3'))
        backoff_ms = int(os.environ.get('MONGO_RETRY_MS', '200'))
        timeout_ms = int(os.environ.get('MONGO_TIMEOUT_MS', '2000'))

        uri = _build_mongo_uri()
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                if uri:
                    print(f'Connecting to Mongo via URI (attempt {attempt}/{max_retries}).')
                    client = pm.MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
                else:
                    print(f'Connecting to Mongo locally (attempt {attempt}/{max_retries}).')
                    client = pm.MongoClient(serverSelectionTimeoutMS=timeout_ms)

                # Verify connectivity with a fast ping
                client.admin.command('ping')
                print('Mongo connection established and verified.')
                break
            except (ServerSelectionTimeoutError, PyMongoError) as err:
                last_error = err
                print(f'Mongo connection attempt {attempt} failed: {err}')
                client = None
                if attempt < max_retries:
                    time.sleep(backoff_ms / 1000.0)
        else:
            # All retries exhausted
            raise RuntimeError(f'Failed to connect to Mongo after {max_retries} attempts.') from last_error
    return client


def convert_mongo_id(doc: dict):
    if MONGO_ID in doc:
        # Convert mongo ID to a string so it works as JSON
        doc[MONGO_ID] = str(doc[MONGO_ID])


def deep_convert_object_ids(obj: Any) -> Any:
    """
    Recursively convert non-JSON-serializable MongoDB types (ObjectId,
    datetime) into their string representations.

    This returns a new object and does not mutate the input.
    """
    if isinstance(obj, ObjectId):
        return str(obj)

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {k: deep_convert_object_ids(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [deep_convert_object_ids(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(deep_convert_object_ids(v) for v in obj)

    return obj


def get_client():
    """
    Ensure a connected client and return it.
    """
    global client
    if client is None:
        connect_db()
    return client


def ping() -> dict:
    """
    Returns a diagnostic dict describing Mongo connectivity.
    {
      'ok': bool,
      'round_trip_ms': float | None,
      'error': str | None
    }
    """
    start = time.time()
    try:
        get_client().admin.command('ping')
        rtt_ms = (time.time() - start) * 1000.0
        return {'ok': True, 'round_trip_ms': rtt_ms, 'error': None}
    except Exception as err:  # noqa: BLE001
        return {'ok': False, 'round_trip_ms': None, 'error': str(err)}


@require_connection
def create(collection, doc, db=SE_DB):
    """
    Insert a single doc into collection.
    """
    print(f'{db=}')
    return client[db][collection].insert_one(doc)


@require_connection
def read_one(collection, filt, db=SE_DB):
    """
    Find with a filter and return on the first doc found.
    Return None if not found.
    """
    for doc in client[db][collection].find(filt):
        convert_mongo_id(doc)
        return doc


@require_connection
def delete(collection: str, filt: dict, db=SE_DB):
    """
    Delete one document matching the filter. Returns deleted count.
    """
    print(f'{filt=}')
    del_result = client[db][collection].delete_one(filt)
    return del_result.deleted_count


@require_connection
def update(collection, filters, update_dict, db=SE_DB):
    return client[db][collection].update_one(filters, {'$set': update_dict})


@require_connection
def read(collection, db=SE_DB, no_id=True) -> list:
    """
    Returns a list from the db.
    """
    ret = []
    for doc in client[db][collection].find():
        if no_id:
            del doc[MONGO_ID]
        else:
            convert_mongo_id(doc)
        ret.append(doc)
    return ret


@require_connection
def find_paginated(
    collection: str,
    db: str = SE_DB,
    filt: Optional[dict] = None,
    projection: Optional[dict] = None,
    sort: Optional[list[tuple[str, int]]] = None,
    page: int = 1,
    limit: int = 50,
    no_id: bool = True
) -> dict:
    """
    Paginated find helper that returns items and pagination metadata.
    Args:
        collection: Mongo collection name.
        db: Database name.
        filt: Query filter dict.
        projection: Fields projection.
        sort: List of (field, direction) where direction is 1 (asc) or -1 (desc).
        page: 1-based page number.
        limit: Items per page.
        no_id: When True, remove '_id' from each document.
    Returns:
        {
          'items': [docs...],
          'page': int,
          'limit': int,
          'total': int,
          'pages': int,
          'has_next': bool,
          'has_prev': bool
        }
    """
    # Sanitize paging params
    try:
        page = int(page)
    except Exception:  # noqa: BLE001
        page = 1
    try:
        limit = int(limit)
    except Exception:  # noqa: BLE001
        limit = 50
    if page < 1:
        page = 1
    if limit < 1:
        limit = 1

    effective_filter = filt or {}
    total = client[db][collection].count_documents(effective_filter)
    pages = max(1, (total + limit - 1) // limit)
    skip = (page - 1) * limit

    cursor = client[db][collection].find(effective_filter, projection)
    if sort:
        cursor = cursor.sort(sort)
    cursor = cursor.skip(skip).limit(limit)

    items: list[dict] = []
    for doc in cursor:
        if no_id:
            if MONGO_ID in doc:
                del doc[MONGO_ID]
        else:
            convert_mongo_id(doc)
        items.append(doc)

    return {
        'items': items,
        'page': page,
        'limit': limit,
        'total': total,
        'pages': pages,
        'has_next': page < pages,
        'has_prev': page > 1,
    }


@require_connection
def read_dict(collection, key, db=SE_DB, no_id=True) -> dict:
    recs = read(collection, db=db, no_id=no_id)
    recs_as_dict = {}
    for rec in recs:
        recs_as_dict[rec[key]] = rec
    return recs_as_dict


@require_connection
def fetch_all_as_dict(key, collection, db=SE_DB):
    ret = {}
    for doc in client[db][collection].find():
        del doc[MONGO_ID]
        ret[doc[key]] = doc
    return ret
