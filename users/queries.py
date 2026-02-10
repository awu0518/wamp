"""
This file deals with user account data and operations.
All user interactions with MongoDB should be through this file.
"""
# from typing import Optional
# import data.db_connect as dbc
# import validation

USERS_COLLECTION = 'users'

# Field names
ID = '_id'
EMAIL = 'email'
PASSWORD = 'password'
USERNAME = 'username'
CREATED_AT = 'created_at'
UPDATED_AT = 'updated_at'

SAMPLE_USER = {
    EMAIL: 'user@example.com',
    USERNAME: 'john_doe',
    PASSWORD: 'hashed_password_here',
}
