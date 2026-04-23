"""
Tests for countries.queries module
"""
from unittest.mock import patch
import pytest

import countries.queries as cq


# FIXTURE: Clear the cache around each test
@pytest.fixture(scope='function')
def clear_country_cache():
    cq.country_cache.clear()
    cq._next_id = 1  # Reset the ID counter
    yield
    cq.country_cache.clear()


# FIXTURE: Create a temporary country and yield its id
@pytest.fixture(scope='function')
def temp_country(clear_country_cache):
    flds = {cq.NAME: "Freedonia", cq.ISO_CODE: "FD"}
    cid = cq.create(flds)
    return cid


# PATCH via fixture: mock randint used by db_connect
@pytest.fixture(scope='function')
def mock_randint():
    with patch('countries.queries.randint') as mock_rand:
        yield mock_rand


def test_db_connect_success(mock_randint):
    mock_randint.return_value = 1
    assert cq.db_connect(1) is True
    mock_randint.assert_called_once_with(1, 1)


def test_db_connect_failure(mock_randint):
    mock_randint.return_value = 1
    assert cq.db_connect(2) is False
    mock_randint.assert_called_once_with(1, 2)


def test_create_success(clear_country_cache):
    flds = {cq.NAME: "Narnia", cq.ISO_CODE: "NA"}
    new_id = cq.create(flds)
    assert new_id == "1"
    expected = {cq.NAME: "Narnia", cq.ISO_CODE: "NA", cq.REVIEW_COUNT: 0}
    assert cq.country_cache["1"] == expected
    assert cq.num_countries() == 1


def test_create_multiple(clear_country_cache):
    c1 = {cq.NAME: "Narnia", cq.ISO_CODE: "NA"}
    c2 = {cq.NAME: "Oz", cq.ISO_CODE: "OZ"}
    id1 = cq.create(c1)
    id2 = cq.create(c2)
    assert id1 == "1"
    assert id2 == "2"
    assert cq.num_countries() == 2


def test_create_raises_on_duplicate_iso(clear_country_cache):
    cq.create({cq.NAME: "Freedonia", cq.ISO_CODE: "FD"})
    with pytest.raises(ValueError, match="iso_code already exists"):
        cq.create({cq.NAME: "Other Freedonia", cq.ISO_CODE: "FD"})


def test_create_normalizes_iso_to_uppercase(clear_country_cache):
    cid = cq.create({cq.NAME: "Lowercase Land", cq.ISO_CODE: "ll"})
    country = cq.read_one(cid)
    assert country[cq.ISO_CODE] == "LL"


def test_find_by_iso_code_case_insensitive(temp_country):
    found = cq.find_by_iso_code("fd")
    assert found is not None
    assert found[cq.NAME] == "Freedonia"


def test_find_by_iso_code_none_on_blank(clear_country_cache):
    assert cq.find_by_iso_code("   ") is None


def test_read_one_by_iso_code_success(temp_country):
    found = cq.read_one_by_iso_code("fd")
    assert found[cq.NAME] == "Freedonia"
    assert found[cq.ISO_CODE] == "FD"


def test_read_one_by_iso_code_raises_on_missing(clear_country_cache):
    with pytest.raises(ValueError, match="No such country with iso_code"):
        cq.read_one_by_iso_code("ZZ")


def test_read_one_by_iso_code_raises_on_bad_type(clear_country_cache):
    with pytest.raises(ValueError, match="iso_code must be a string"):
        cq.read_one_by_iso_code(123)


# WITH RAISES: invalid inputs
def test_create_raises_on_bad_type(clear_country_cache):
    with pytest.raises(ValueError, match="Request body must be a JSON object"):
        cq.create(["not", "a", "dict"])


def test_create_raises_on_missing_name(clear_country_cache):
    with pytest.raises(ValueError, match="Missing required fields"):
        cq.create({cq.ISO_CODE: "XX"})


def test_read_one_success(temp_country):
    country = cq.read_one(temp_country)
    assert country[cq.NAME] == "Freedonia"
    assert country[cq.ISO_CODE] == "FD"


def test_read_one_raises_on_missing(clear_country_cache):
    with pytest.raises(ValueError, match="No such country"):
        cq.read_one("999")


def test_read_one_raises_on_invalid_id(clear_country_cache):
    with pytest.raises(ValueError, match="country_id must be a non-empty"):
        cq.read_one("   ")


def test_read_one_returns_copy(temp_country):
    # Verify that modifying the returned dict doesn't affect the cache
    country = cq.read_one(temp_country)
    country[cq.NAME] = "Modified Name"
    # Original should be unchanged
    assert cq.read_one(temp_country)[cq.NAME] == "Freedonia"


def test_delete_success(temp_country):
    assert cq.delete(temp_country) is True
    assert temp_country not in cq.read()


def test_delete_raises_on_missing(clear_country_cache):
    with pytest.raises(ValueError, match="No such country"):
        cq.delete("999")


def test_delete_raises_on_invalid_id(clear_country_cache):
    with pytest.raises(ValueError, match="country_id must be a non-empty"):
        cq.delete("")


def test_update_country_success(temp_country):
    # Update the country's name
    new_data = {cq.NAME: "New Freedonia"}
    assert cq.update(temp_country, new_data) is True
    updated_country = cq.read()[temp_country]
    assert updated_country[cq.NAME] == "New Freedonia"
    # ISO_CODE should remain unchanged
    assert updated_country[cq.ISO_CODE] == "FD"


def test_update_country_rejects_duplicate_iso(clear_country_cache):
    id1 = cq.create({cq.NAME: "Freedonia", cq.ISO_CODE: "FD"})
    id2 = cq.create({cq.NAME: "Sylvania", cq.ISO_CODE: "SY"})
    with pytest.raises(ValueError, match="iso_code already exists"):
        cq.update(id2, {cq.ISO_CODE: "FD"})

    # Ensure original values unchanged after failed update
    assert cq.read_one(id1)[cq.ISO_CODE] == "FD"
    assert cq.read_one(id2)[cq.ISO_CODE] == "SY"


def test_update_country_raises_on_missing(clear_country_cache):
    with pytest.raises(ValueError, match="No such country"):
        cq.update("999", {cq.NAME: "Ghost Country"})


def test_update_country_raises_on_bad_type(temp_country):
    with pytest.raises(ValueError, match="Bad type"):
        cq.update(temp_country, ["not", "a", "dict"])


def test_update_country_raises_on_invalid_id(clear_country_cache):
    with pytest.raises(ValueError, match="country_id must be a non-empty"):
        cq.update("", {cq.NAME: "Ghost Country"})


def test_update_country_raises_on_empty_fields(temp_country):
    with pytest.raises(ValueError, match="No fields provided for update"):
        cq.update(temp_country, {})


# PATCH: patch db_connect directly
@patch('countries.queries.db_connect', return_value=True, autospec=True)
def test_db_connect_patched(mock_db_connect):
    assert cq.db_connect(10) is True
    mock_db_connect.assert_called_once()