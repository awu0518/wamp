"""
Authentication and token management for user accounts.
Handles login, JWT token generation, and token validation.
"""
from typing import Optional
from datetime import datetime, timedelta
import users.queries as uq
