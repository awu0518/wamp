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


def test_generate_token_success():
	"""Test generating a valid token."""
	token = auth.generate_token("user123", "user@example.com")
	assert isinstance(token, str)
	assert token


def test_validate_token_valid():
	"""Test validating a valid token."""
	token = auth.generate_token("user123", "user@example.com")
	payload = auth.validate_token(token)

	assert payload is not None
	assert payload.get("user_id") == "user123"
	assert payload.get("email") == "user@example.com"


def test_validate_token_tampered():
	"""Test validating a tampered token returns None."""
	token = auth.generate_token("user123", "user@example.com")
	parts = token.split(".")
	assert len(parts) == 3
	# Corrupt the payload segment so the signature no longer matches.
	payload = parts[1]
	payload = payload[:-1] + ("a" if payload[-1] != "a" else "b")
	parts[1] = payload
	tampered = ".".join(parts)
	payload = auth.validate_token(tampered)
	assert payload is None


def test_validate_token_missing():
	"""Test validating a missing/empty token returns None."""
	payload = auth.validate_token("")
	assert payload is None

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


def test_verify_token_header_valid():
	"""Test verifying valid Authorization header."""
	token = auth.generate_token("user123", "user@example.com")
	payload = auth.verify_token_header(f"Bearer {token}")

	assert payload is not None
	assert payload.get("user_id") == "user123"


def test_verify_token_header_missing_bearer():
	"""Test verifying header with missing Bearer prefix."""
	token = auth.generate_token("user123", "user@example.com")
	payload = auth.verify_token_header(f"Token {token}")
	assert payload is None


def test_verify_token_header_empty():
	"""Test verifying empty Authorization header."""
	payload = auth.verify_token_header("")
	assert payload is None
