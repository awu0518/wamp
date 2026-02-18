"""
This file deals with user account data and operations.
All user interactions with MongoDB should be through this file.
"""
from typing import Optional
from datetime import datetime
import re
from bson import ObjectId
import data.db_connect as dbc

USERS_COLLECTION = 'users'

# Field names
ID = '_id'
EMAIL = 'email'
PASSWORD = 'password'
USERNAME = 'username'
CREATED_AT = 'created_at'
UPDATED_AT = 'updated_at'

# Password requirements
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128

SAMPLE_USER = {
    EMAIL: 'user@example.com',
    USERNAME: 'john_doe',
    PASSWORD: 'hashed_password_here',
}


@dbc.require_connection
def create_user(email: str, username: str, password_hash: str) -> dict:
    """
    Create a new user in the database.

    Args:
        email: User's email address (must be unique)
        username: User's username (must be unique)
        password_hash: Hashed password

    Returns:
        dict: Created user document with _id

    Raises:
        ValueError: If email or username already exists
    """
    # Check for duplicates
    if user_exists(email=email):
        raise ValueError(f'User with email {email} already exists')
    if user_exists(username=username):
        raise ValueError(f'Username {username} already taken')

    # Create user document
    user_doc = {
        EMAIL: email.lower().strip(),
        USERNAME: username.strip(),
        PASSWORD: password_hash,
        CREATED_AT: datetime.utcnow(),
        UPDATED_AT: datetime.utcnow(),
    }

    result = dbc.create(USERS_COLLECTION, user_doc)
    user_doc[ID] = str(result.inserted_id)
    return user_doc


@dbc.require_connection
def get_user_by_email(email: str) -> Optional[dict]:
    """
    Retrieve a user by email address.

    Args:
        email: User's email address

    Returns:
        dict: User document if found, None otherwise
    """
    user = dbc.read_one(USERS_COLLECTION, {EMAIL: email.lower().strip()})
    if user:
        dbc.convert_mongo_id(user)
    return user


@dbc.require_connection
def get_user_by_id(user_id: str) -> Optional[dict]:
    """
    Retrieve a user by their ID.

    Args:
        user_id: User's MongoDB ObjectId as string

    Returns:
        dict: User document if found, None otherwise
    """
    try:
        obj_id = ObjectId(user_id)
        user = dbc.read_one(USERS_COLLECTION, {ID: obj_id})
        if user:
            dbc.convert_mongo_id(user)
        return user
    except Exception:
        return None


@dbc.require_connection
def user_exists(email: str = None, username: str = None) -> bool:
    """
    Check if a user exists by email or username.

    Args:
        email: User's email address
        username: User's username

    Returns:
        bool: True if user exists, False otherwise
    """
    if email:
        user = dbc.read_one(USERS_COLLECTION, {EMAIL: email.lower().strip()})
        if user:
            return True
    if username:
        user = dbc.read_one(USERS_COLLECTION, {USERNAME: username.strip()})
        if user:
            return True
    return False


def validate_email(email: str) -> tuple[bool, str]:
    """
    Validate email format.

    Args:
        email: Email to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not email or not isinstance(email, str):
        return False, "Email is required"

    email = email.strip()

    if len(email) < 3 or len(email) > 254:
        return False, "Email must be between 3 and 254 characters"

    # Basic email regex
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False, "Invalid email format"

    return True, ""


def validate_username(username: str) -> tuple[bool, str]:
    """
    Validate username format.

    Args:
        username: Username to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not username or not isinstance(username, str):
        return False, "Username is required"

    username = username.strip()

    if len(username) < 3 or len(username) > 30:
        return False, "Username must be between 3 and 30 characters"

    # Alphanumeric and underscore only
    if not re.match(r'^[a-zA-Z0-9_]+$', username):
        return (False,
                "Username can only contain letters, numbers, "
                "and underscores")

    return True, ""


def validate_password(password: str) -> tuple[bool, str]:
    """
    Validate password meets security requirements.

    Args:
        password: Password to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not password or not isinstance(password, str):
        return False, "Password is required"

    if len(password) < MIN_PASSWORD_LENGTH:
        return (False,
                f"Password must be at least {MIN_PASSWORD_LENGTH} characters")

    if len(password) > MAX_PASSWORD_LENGTH:
        return (False,
                f"Password must be at most {MAX_PASSWORD_LENGTH} characters")

    # Check for at least one uppercase letter
    if not re.search(r'[A-Z]', password):
        return (False,
                "Password must contain at least one uppercase letter")

    # Check for at least one lowercase letter
    if not re.search(r'[a-z]', password):
        return (False,
                "Password must contain at least one lowercase letter")

    # Check for at least one digit
    if not re.search(r'\d', password):
        return False, "Password must contain at least one number"

    return True, ""
