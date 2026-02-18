"""
Tests for users.queries module
"""
import pytest
import time
import users.queries as uq
import users.auth as auth
import data.db_connect as dbc


@pytest.fixture(scope='function')
def temp_user():
    """Create a temporary test user and clean up after test"""
    email = f"test_{int(time.time())}@example.com"
    username = f"testuser_{int(time.time())}"
    password_hash = auth.hash_password("TestPassword123")

    user = uq.create_user(email, username, password_hash)
    yield user

    # Cleanup: delete directly using dbc
    dbc.delete(uq.USERS_COLLECTION, {uq.EMAIL: user[uq.EMAIL]})


def test_create_user_success():
    """Test creating a new user successfully"""
    email = f"newuser_{int(time.time())}@example.com"
    username = f"newuser_{int(time.time())}"
    password_hash = auth.hash_password("ValidPass123")

    user = uq.create_user(email, username, password_hash)

    assert user is not None
    assert uq.EMAIL in user
    assert user[uq.EMAIL] == email.lower()
    assert user[uq.USERNAME] == username
    assert uq.ID in user
    assert len(user[uq.ID]) == 24  # MongoDB ObjectId length

    # Cleanup
    dbc.delete(uq.USERS_COLLECTION, {uq.EMAIL: email.lower()})


def test_create_user_duplicate_email(temp_user):
    """Test creating a user with duplicate email raises error"""
    with pytest.raises(ValueError, match="already exists"):
        uq.create_user(
            temp_user[uq.EMAIL],
            "different_username",
            "somehash"
        )


def test_create_user_duplicate_username(temp_user):
    """Test creating a user with duplicate username raises error"""
    with pytest.raises(ValueError, match="already taken"):
        uq.create_user(
            "different@email.com",
            temp_user[uq.USERNAME],
            "somehash"
        )


def test_get_user_by_email_found(temp_user):
    """Test retrieving user by email when user exists"""
    user = uq.get_user_by_email(temp_user[uq.EMAIL])

    assert user is not None
    assert user[uq.EMAIL] == temp_user[uq.EMAIL]
    assert user[uq.USERNAME] == temp_user[uq.USERNAME]


def test_get_user_by_email_not_found():
    """Test retrieving user by email when user doesn't exist"""
    user = uq.get_user_by_email("nonexistent@example.com")
    assert user is None


def test_get_user_by_email_case_insensitive(temp_user):
    """Test email lookup is case insensitive"""
    user = uq.get_user_by_email(temp_user[uq.EMAIL].upper())
    assert user is not None
    assert user[uq.EMAIL] == temp_user[uq.EMAIL].lower()


def test_get_user_by_id_found(temp_user):
    """Test retrieving user by ID when user exists"""
    user = uq.get_user_by_id(temp_user[uq.ID])

    assert user is not None
    assert user[uq.ID] == temp_user[uq.ID]
    assert user[uq.EMAIL] == temp_user[uq.EMAIL]


def test_get_user_by_id_not_found():
    """Test retrieving user by ID when user doesn't exist"""
    fake_id = "507f1f77bcf86cd799439011"  # Valid format, non-existent
    user = uq.get_user_by_id(fake_id)
    assert user is None


def test_get_user_by_id_invalid_format():
    """Test retrieving user with invalid ObjectId format"""
    user = uq.get_user_by_id("invalid_id")
    assert user is None


def test_user_exists_by_email(temp_user):
    """Test checking if user exists by email"""
    assert uq.user_exists(email=temp_user[uq.EMAIL]) is True
    assert uq.user_exists(email="nonexistent@example.com") is False


def test_user_exists_by_username(temp_user):
    """Test checking if user exists by username"""
    assert uq.user_exists(username=temp_user[uq.USERNAME]) is True
    assert uq.user_exists(username="nonexistent_user") is False


def test_user_not_exists():
    """Test checking if non-existent user returns False"""
    assert uq.user_exists(
        email="nobody@example.com",
        username="nobody"
    ) is False


def test_validate_email_valid():
    """Test valid email passes validation"""
    valid, msg = uq.validate_email("user@example.com")
    assert valid is True
    assert msg == ""


def test_validate_email_invalid_format():
    """Test invalid email format fails validation"""
    valid, msg = uq.validate_email("not-an-email")
    assert valid is False
    assert "Invalid email format" in msg


def test_validate_email_missing():
    """Test missing email fails validation"""
    valid, msg = uq.validate_email("")
    assert valid is False
    assert "required" in msg


def test_validate_username_valid():
    """Test valid username passes validation"""
    valid, msg = uq.validate_username("valid_user123")
    assert valid is True
    assert msg == ""


def test_validate_username_too_short():
    """Test username too short fails validation"""
    valid, msg = uq.validate_username("ab")
    assert valid is False
    assert "between 3 and 30" in msg


def test_validate_username_invalid_chars():
    """Test username with invalid characters fails"""
    valid, msg = uq.validate_username("user@name")
    assert valid is False
    assert "letters, numbers, and underscores" in msg


def test_validate_password_valid():
    """Test valid password passes validation"""
    valid, msg = uq.validate_password("ValidPass123")
    assert valid is True
    assert msg == ""


def test_password_too_short():
    """Test password fails minimum length requirement"""
    valid, msg = uq.validate_password("Short1")
    assert valid is False
    assert "at least" in msg


def test_password_no_uppercase():
    """Test password without uppercase fails"""
    valid, msg = uq.validate_password("lowercase123")
    assert valid is False
    assert "uppercase" in msg


def test_password_no_lowercase():
    """Test password without lowercase fails"""
    valid, msg = uq.validate_password("UPPERCASE123")
    assert valid is False
    assert "lowercase" in msg


def test_password_no_numbers():
    """Test password without numbers fails"""
    valid, msg = uq.validate_password("NoNumbers")
    assert valid is False
    assert "number" in msg

