"""
Authentication and token management for user accounts.
Handles login, JWT token generation, and token validation.
"""
from typing import Optional
from datetime import datetime, timedelta
import users.queries as uq
import bcrypt
import os
import jwt
from dotenv import load_dotenv

load_dotenv()
SECRET_KEY = os.getenv(
    'JWT_SECRET', 'your-default-secret-change-in-production')


def hash_password(password: str) -> str:
    """
    Hash a plaintext password for secure storage.

    Args:
        password: Plaintext password

    Returns:
        str: Hashed password
    """

    s = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), s)  # Hash password
    return hashed.decode('utf-8')


def authenticate_user(email: str, password: str) ->\
        Optional[tuple[str, dict]]:
    """
    Authenticate a user with email and password.

    Args:
        email: User's email address
        password: User's plaintext password

    Returns:
        tuple: (token, user_data) if authentication successful,
               None otherwise
    """
    # 1. Get user from database
    user = uq.get_user_by_email(email)
    if not user:
        return None  # User doesn't exist

    # 2. Verify password (bcrypt.checkpw needs bytes)
    stored_hash = (user['password'].encode()
                   if isinstance(user['password'], str)
                   else user['password'])
    if not bcrypt.checkpw(password.encode(), stored_hash):
        return None  # Password wrong

    # 3. Generate token
    token = generate_token(str(user['_id']), email)

    # 4. Return token and user (without password!)
    user_data = {k: v for k, v in user.items() if k != 'password'}
    return (token, user_data)


def generate_token(user_id: str, email: str, expires_in: int = 3600) -> str:
    payload = {
        'user_id': user_id,
        'email': email,
        'exp': datetime.utcnow() + timedelta(seconds=expires_in),
        'iat': datetime.utcnow()
    }

    return jwt.encode(payload, SECRET_KEY, algorithm='HS256')


def validate_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
    except jwt.InvalidTokenError:
        return None


def verify_token_header(authorization_header: str) -> Optional[dict]:
    """
    Extract and validate token from HTTP Authorization header.

    Args:
        authorization_header: Value of Authorization header
                              (format: "Bearer <token>")

    Returns:
        dict: Token payload if valid, None otherwise
    """
    if not authorization_header:
        return None

    try:
        # Format: "Bearer <token>"
        parts = authorization_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return None

        token = parts[1]
        return validate_token(token)
    except (IndexError, ValueError):
        return None
