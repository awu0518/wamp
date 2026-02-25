"""
Tests for users.auth module
"""
import time

import pytest

import data.db_connect as dbc
import users.auth as auth
import users.queries as uq


@pytest.fixture(scope='function')
def temp_user():
	"""Create a temporary user and clean up after test."""
	stamp = int(time.time())
	email = f"auth_{stamp}@example.com"
	username = f"authuser_{stamp}"
	password = "ValidPass123"
	password_hash = auth.hash_password(password)

	user = uq.create_user(email, username, password_hash)
	yield user, password

	dbc.delete(uq.USERS_COLLECTION, {uq.EMAIL: user[uq.EMAIL]})

def test_authenticate_user_success(temp_user):
	"""Test authenticating with correct credentials."""
	user, password = temp_user
	result = auth.authenticate_user(user[uq.EMAIL], password)

	assert result is not None
	token, user_data = result
	assert token
	assert user_data[uq.EMAIL] == user[uq.EMAIL]
	assert uq.PASSWORD not in user_data


def test_authenticate_user_wrong_password(temp_user):
	"""Test authentication fails with wrong password."""
	user, _password = temp_user
	result = auth.authenticate_user(user[uq.EMAIL], "WrongPass123")
	assert result is None


def test_authenticate_user_not_found():
	"""Test authentication fails when user doesn't exist."""
	result = auth.authenticate_user("missing@example.com", "AnyPass123")
	assert result is None
