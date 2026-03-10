"""
Tests for cities.queries module
"""
from unittest.mock import patch
import pytest
import time

import cities.queries as cq
from copy import deepcopy

def get_temp_rec():
    return deepcopy(cq.SAMPLE_CITY)

@pytest.fixture(scope='function')
def temp_city_no_del():
    temp_rec = get_temp_rec()
    cq.create(get_temp_rec())
    return temp_rec

@pytest.fixture(scope='function')
def mock_randint():
    """Fixture to mock randint for predictable testing"""
    with patch('cities.queries.randint') as mock_rand:
        yield mock_rand


def test_db_connect_success(mock_randint):
    """Test db_connect with guaranteed success"""
    mock_randint.return_value = 1 
    
    result = cq.db_connect(1)
    assert result is True
    mock_randint.assert_called_once_with(1, 1)


def test_db_connect_failure(mock_randint):
    """Test db_connect with guaranteed failure"""
    mock_randint.return_value = 1
    
    result = cq.db_connect(2)
    assert result is False
    mock_randint.assert_called_once_with(1, 2)

# create tests
@pytest.fixture(scope='function')
def clear_city_cache():
    """Fixture to clear city_cache before each test"""
    cq.city_cache.clear()
    yield
    cq.city_cache.clear()

@pytest.fixture(scope='function')
def temp_city(clear_city_cache):
    """Create a temporary city and yield its name (used as ID)"""
    flds = {cq.NAME: "Boston", cq.STATE_CODE: "MA"}
    cq.create(flds)
    return flds[cq.NAME]  # Return city name, which is used as the key

def test_create_success(clear_city_cache):
    """Test create function with valid input"""
    # Use unique name to avoid conflicts
    test_city = {cq.NAME: f"TestCreateCity_{int(time.time())}", cq.STATE_CODE: "NY"}

    result = cq.create(test_city)

    # Should return a valid ID
    assert result is not None
    # Cache should contain the city after create
    assert test_city[cq.NAME] in cq.city_cache
    # And the city should exist in the database
    cities = cq.read()
    assert test_city[cq.NAME] in cities

    # Clean up
    cq.delete(test_city[cq.NAME], test_city[cq.STATE_CODE])

def test_create_multiple_cities(clear_city_cache):
    """Test creating multiple cities"""
    # Use unique names to avoid conflicts
    timestamp = int(time.time())
    city1 = {cq.NAME: f"TestCity1_{timestamp}", cq.STATE_CODE: "NY"}
    city2 = {cq.NAME: f"TestCity2_{timestamp}", cq.STATE_CODE: "CA"}
    
    id1 = cq.create(city1)
    id2 = cq.create(city2)
    
    # Should return valid MongoDB ObjectIds (not simple incremental IDs)
    assert len(id1) == 24  # MongoDB ObjectId length
    assert len(id2) == 24
    assert id1 != id2  # Should be different IDs
    assert cq.num_cities() >= 2  # At least 2 cities should exist
    
    # Clean up test cities
    cq.delete(city1[cq.NAME], city1[cq.STATE_CODE])
    cq.delete(city2[cq.NAME], city2[cq.STATE_CODE])

@patch('cities.queries.db_connect', return_value=True, autospec=True)
def test_delete(mock_db_connect, clear_city_cache):
    # Create a test city first
    test_city = {cq.NAME: "Test City", cq.STATE_CODE: "TC"}
    cq.create(test_city)
    
    ret = cq.delete(test_city[cq.NAME], test_city[cq.STATE_CODE])
    assert ret >= 1  # Should delete at least 1 record

@pytest.mark.skip('revive once all functions are cutover!')
def test_read(temp_city):
    cities = cq.read()
    assert isinstance(cities, dict)
    assert temp_city in cities

def test_read_one_success(temp_city):
    """Test read_one function with valid city ID and caching"""
    # First call should fetch from database and cache it
    city = cq.read_one(temp_city)
    assert city[cq.NAME] == "Boston"
    assert city[cq.STATE_CODE] == "MA"
    
    # Verify city is now in cache
    assert temp_city in cq.city_cache
    
    # Second call should use cache
    city2 = cq.read_one(temp_city)
    assert city2[cq.NAME] == "Boston"
    assert city2[cq.STATE_CODE] == "MA"


def test_read_one_raises_on_missing(clear_city_cache):
    """Test read_one raises error for non-existent city"""
    with pytest.raises(ValueError, match="No such city"):
        cq.read_one("999")


def test_read_one_returns_copy(temp_city):
    """Verify that modifying the returned dict doesn't affect the cache"""
    city = cq.read_one(temp_city)
    city[cq.NAME] = "Modified Name"
    # Original should be unchanged
    assert cq.read_one(temp_city)[cq.NAME] == "Boston"

def test_update_city_success(temp_city):
    """Test update function with valid input"""
    # Update the city's name
    new_data = {cq.NAME: "New Boston"}
    assert cq.update(temp_city, new_data) is True
    # After name change, the city is now keyed by the new name
    cities = cq.read()
    assert "New Boston" in cities
    updated_city = cities["New Boston"]
    assert updated_city[cq.NAME] == "New Boston"
    # STATE_CODE should remain unchanged
    assert updated_city[cq.STATE_CODE] == "MA"


def test_update_city_raises_on_missing(clear_city_cache):
    """Test update raises error for non-existent city"""
    with pytest.raises(ValueError, match="No such city"):
        cq.update("999", {cq.NAME: "Ghost City"})


def test_update_city_raises_on_bad_type(temp_city):
    """Test update raises error for invalid input type"""
    with pytest.raises(ValueError, match="Bad type"):
        cq.update(temp_city, ["not", "a", "dict"])

def test_num_cities():
    # get the count
    old_count = cq.num_cities()
    print(f"Initial count: {old_count}, type: {type(old_count)}")
    # add a record with a unique name to avoid duplicates
    unique_city = {cq.NAME: f"TestCity_{int(time.time())}", cq.STATE_CODE: "XX"}
    cq.create(unique_city)
    new_count = cq.num_cities()
    print(f"New count: {new_count}, type: {type(new_count)}")
    print(f"Expected: {old_count + 1}")
    assert new_count == old_count + 1
    # Clean up the test city
    cq.delete(unique_city[cq.NAME], "XX")

def test_read(temp_city):
    cities = cq.read()
    assert isinstance(cities, dict)
    # Check that our temp city is in the results
    assert temp_city in cities
    assert cities[temp_city][cq.NAME] == "Boston"

@pytest.mark.skip('revive once all functions are cutover!')
def test_read_cant_connect():
    with pytest.raises(ConnectionError):
        cities = cq.read()
    assert isinstance(cities, list)
    assert get_temp_rec() in cities