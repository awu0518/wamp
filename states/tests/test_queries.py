"""
Tests for states.queries module
"""
from unittest.mock import patch
import pytest
import time
import json

import states.queries as sq
from copy import deepcopy


def get_temp_rec():
    return deepcopy(sq.SAMPLE_STATE)


@pytest.fixture(scope='function')
def mock_randint():
    """Fixture to mock randint for predictable testing"""
    with patch('states.queries.randint') as mock_rand:
        yield mock_rand


def test_db_connect_success(mock_randint):
    """Test db_connect with guaranteed success"""
    mock_randint.return_value = 1 
    
    result = sq.db_connect(1)
    assert result is True
    mock_randint.assert_called_once_with(1, 1)


def test_db_connect_failure(mock_randint):
    """Test db_connect with guaranteed failure"""
    mock_randint.return_value = 1
    
    result = sq.db_connect(2)
    assert result is False
    mock_randint.assert_called_once_with(1, 2)


@pytest.fixture(scope='function')
def temp_state():
    """Create a temporary state and yield its name (used as ID)"""
    flds = {
        sq.NAME: "TestState",
        sq.STATE_CODE: "TS",
        sq.COUNTRY_ISO_CODE: "US"
    }
    sq.create(flds)
    yield flds[sq.NAME]
    # Cleanup
    try:
        sq.delete(flds[sq.NAME])
    except ValueError:
        pass  # Already deleted


def test_create_success():
    """Test create function with valid input"""
    timestamp = int(time.time())
    test_state = {
        sq.NAME: f"TestState_{timestamp}",
        sq.STATE_CODE: "TS",
        sq.COUNTRY_ISO_CODE: "US"
    }
    
    result = sq.create(test_state)
    
    # Should return a valid MongoDB ObjectId
    assert result is not None
    assert len(result) == 24  # MongoDB ObjectId length
    
    # State should exist in the database
    states = sq.read()
    assert test_state[sq.NAME] in states
    
    # Clean up
    sq.delete(test_state[sq.NAME])


def test_create_multiple_states():
    """Test creating multiple states"""
    timestamp = int(time.time())
    state1 = {
        sq.NAME: f"State1_{timestamp}",
        sq.STATE_CODE: "AA",
        sq.COUNTRY_ISO_CODE: "US",
    }
    state2 = {
        sq.NAME: f"State2_{timestamp}",
        sq.STATE_CODE: "BB",
        sq.COUNTRY_ISO_CODE: "CA",
    }
    
    id1 = sq.create(state1)
    id2 = sq.create(state2)
    
    # Should return valid MongoDB ObjectIds
    assert len(id1) == 24
    assert len(id2) == 24
    assert id1 != id2
    assert sq.num_states() >= 2
    
    # Clean up
    sq.delete(state1[sq.NAME])
    sq.delete(state2[sq.NAME])


def test_delete(temp_state):
    """Test delete function"""
    ret = sq.delete(temp_state)
    assert ret is True
    
    # Verify it's gone
    states = sq.read()
    assert temp_state not in states


def test_delete_nonexistent():
    """Test delete raises error for non-existent state"""
    with pytest.raises(ValueError, match="No such state"):
        sq.delete("NonExistentState")


def test_read(temp_state):
    """Test read function"""
    states = sq.read()
    assert isinstance(states, dict)
    assert temp_state in states
    assert states[temp_state][sq.NAME] == "TestState"


def test_read_one_success(temp_state):
    """Test read_one function with valid state ID"""
    state = sq.read_one(temp_state)
    assert state[sq.NAME] == "TestState"
    assert state[sq.STATE_CODE] == "TS"
    assert state[sq.COUNTRY_ISO_CODE] == "US"


def test_read_one_raises_on_missing():
    """Test read_one raises error for non-existent state"""
    with pytest.raises(ValueError, match="No such state"):
        sq.read_one("NonExistentState")


def test_read_one_returns_copy(temp_state):
    """Verify that modifying the returned dict doesn't affect the original"""
    state = sq.read_one(temp_state)
    state[sq.NAME] = "Modified Name"
    # Original should be unchanged
    assert sq.read_one(temp_state)[sq.NAME] == "TestState"


def test_update_state_success(temp_state):
    """Test update function with valid input"""
    new_data = {sq.STATE_CODE: "TT"}
    assert sq.update(temp_state, new_data) is True
    
    # Verify update
    states = sq.read()
    assert states[temp_state][sq.STATE_CODE] == "TT"
    # Other fields should remain unchanged
    assert states[temp_state][sq.NAME] == "TestState"


def test_update_state_raises_on_missing():
    """Test update raises error for non-existent state"""
    with pytest.raises(ValueError, match="No such state"):
        sq.update("NonExistentState", {sq.STATE_CODE: "ZZ"})


def test_update_state_raises_on_bad_type(temp_state):
    """Test update raises error for invalid input type"""
    with pytest.raises(ValueError, match="Bad type"):
        sq.update(temp_state, ["not", "a", "dict"])


def test_num_states():
    """Test num_states function"""
    old_count = sq.num_states()
    unique_state = {
        sq.NAME: f"CountTest_{int(time.time())}",
        sq.STATE_CODE: "CT",
        sq.COUNTRY_ISO_CODE: "US"
    }
    sq.create(unique_state)
    new_count = sq.num_states()
    assert new_count == old_count + 1
    sq.delete(unique_state[sq.NAME])


def test_find_by_state_code(temp_state):
    """Test find_by_state_code function"""
    state = sq.find_by_state_code("TS")
    assert state is not None
    assert state[sq.NAME] == "TestState"
    assert state[sq.STATE_CODE] == "TS"


def test_find_by_country_iso_code(temp_state):
    """Test find_by_country_iso_code function"""
    results = sq.find_by_country_iso_code("us")
    assert temp_state in results
    assert results[temp_state][sq.COUNTRY_ISO_CODE] == "US"


def test_find_by_state_code_case_insensitive(temp_state):
    """Test find_by_state_code is case-insensitive"""
    state = sq.find_by_state_code("ts")
    assert state is not None
    assert state[sq.STATE_CODE] == "TS"


def test_find_by_state_code_not_found():
    """Test find_by_state_code returns None for non-existent code"""
    state = sq.find_by_state_code("ZZ")
    assert state is None


def test_search_by_name(temp_state):
    """Test search function with name parameter"""
    results = sq.search(name="Test")
    assert len(results) >= 1
    assert temp_state in results


def test_search_by_state_code(temp_state):
    """Test search function with state_code parameter"""
    results = sq.search(state_code="TS")
    assert len(results) >= 1
    assert temp_state in results


def test_search_combined(temp_state):
    """Test search function with multiple parameters"""
    results = sq.search(name="Test", state_code="TS")
    assert len(results) >= 1
    assert temp_state in results


def test_search_by_country_iso_code(temp_state):
    """Test search function with country_iso_code parameter"""
    results = sq.search(country_iso_code="US")
    assert len(results) >= 1
    assert temp_state in results


def test_search_no_results():
    """Test search returns empty dict when no matches"""
    results = sq.search(name="NonExistentStateName12345")
    assert results == {}


def test_is_valid_id():
    """Test is_valid_id function"""
    assert sq.is_valid_id("NY") is True
    assert sq.is_valid_id("California") is True
    assert sq.is_valid_id("") is False
    assert sq.is_valid_id(123) is False


# ==================== Export Feature Tests ====================

def test_export_to_json():
    """Test export_to_json function"""
    test_data = {
        "TestState1": {sq.NAME: "TestState1", sq.STATE_CODE: "T1"},
        "TestState2": {sq.NAME: "TestState2", sq.STATE_CODE: "T2"}
    }
    result = sq.export_to_json(test_data)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert len(parsed) == 2


def test_export_to_json_with_indent():
    """Test export_to_json with custom indent"""
    test_data = {
        "TestState": {sq.NAME: "TestState", sq.STATE_CODE: "TS"}
    }
    result = sq.export_to_json(test_data, indent=4)
    # Check that it's properly indented (4 spaces)
    assert "    " in result


def test_export_to_json_excludes_mongo_id():
    """Test export_to_json excludes MongoDB _id field"""
    test_data = {
        "TestState": {
            sq.NAME: "TestState",
            sq.STATE_CODE: "TS",
            "_id": "some_mongo_id"
        }
    }
    result = sq.export_to_json(test_data)
    parsed = json.loads(result)
    assert "_id" not in parsed[0]


def test_export_to_csv():
    """Test export_to_csv function"""
    test_data = {
        "TestState1": {sq.NAME: "TestState1", sq.STATE_CODE: "T1"},
        "TestState2": {sq.NAME: "TestState2", sq.STATE_CODE: "T2"}
    }
    result = sq.export_to_csv(test_data)

    assert isinstance(result, str)
    lines = result.strip().split('\n')
    assert len(lines) == 3  # header + 2 rows
    assert "name" in lines[0]
    assert "state_code" in lines[0]


def test_export_to_csv_empty():
    """Test export_to_csv with empty data"""
    result = sq.export_to_csv({})
    assert result == ""


def test_export_to_csv_excludes_mongo_id():
    """Test export_to_csv excludes MongoDB _id field"""
    test_data = {
        "TestState": {
            sq.NAME: "TestState",
            sq.STATE_CODE: "TS",
            "_id": "some_mongo_id"
        }
    }
    result = sq.export_to_csv(test_data)
    assert "_id" not in result



