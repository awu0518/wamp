from http.client import (
    BAD_REQUEST,
    FORBIDDEN,
    NOT_ACCEPTABLE,
    NOT_FOUND,
    OK,
    SERVICE_UNAVAILABLE,
)

from unittest.mock import patch, MagicMock
from datetime import datetime

import pytest

import server.endpoints as ep

TEST_CLIENT = ep.app.test_client()


# FIXTURE: Reusable test client fixture
@pytest.fixture
def client():
    """Fixture to provide a test client for the Flask app."""
    return ep.app.test_client()


# FIXTURE: Sample timestamp for testing
@pytest.fixture
def mock_timestamp():
    """Fixture to provide a consistent timestamp for testing."""
    return datetime(2025, 10, 21, 12, 0, 0)


# FIXTURE: Expected endpoint list
@pytest.fixture
def expected_endpoints():
    """Fixture to provide expected list of endpoints."""
    return [ep.HELLO_EP, ep.ENDPOINT_EP, ep.TIMESTAMP_EP]


def test_hello():
    resp = TEST_CLIENT.get(ep.HELLO_EP)
    resp_json = resp.get_json()
    assert resp.status_code == OK
    assert ep.HELLO_RESP in resp_json


def test_endpoints_lists_hello():
    resp = TEST_CLIENT.get(ep.ENDPOINT_EP)
    data = resp.get_json()
    assert resp.status_code == OK
    assert ep.ENDPOINT_RESP in data
    assert ep.HELLO_EP in data[ep.ENDPOINT_RESP]


# Using FIXTURE
def test_hello_with_fixture(client):
    """Test hello endpoint using client fixture."""
    resp = client.get(ep.HELLO_EP)
    resp_json = resp.get_json()
    assert resp.status_code == OK
    assert resp_json[ep.HELLO_RESP] == 'world'


# Using FIXTURE
def test_all_endpoints_present(client, expected_endpoints):
    """Test that all expected endpoints are listed using fixtures."""
    resp = client.get(ep.ENDPOINT_EP)
    data = resp.get_json()
    assert resp.status_code == OK
    endpoints_list = data[ep.ENDPOINT_RESP]
    for endpoint in expected_endpoints:
        assert endpoint in endpoints_list


# PATCH: Mock datetime for timestamp endpoint
@patch('server.endpoints.datetime')
def test_timestamp_with_patch(mock_datetime, client):
    """Test timestamp endpoint with mocked datetime using patch."""
    # Setup mock
    mock_now = datetime(2025, 10, 21, 15, 30, 45)
    mock_datetime.now.return_value = mock_now
    
    resp = client.get(ep.TIMESTAMP_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert ep.TIMESTAMP_RESP in data
    assert data[ep.TIMESTAMP_RESP] == mock_now.isoformat()
    assert data['unix'] == mock_now.timestamp()


# PYTEST.RAISES: Test invalid endpoint
def test_invalid_endpoint_raises_404(client):
    """Test that accessing invalid endpoint returns 404 using pytest.raises."""
    resp = client.get('/invalid_endpoint')
    assert resp.status_code == NOT_FOUND


# PYTEST.RAISES: Test for expected data structure
def test_timestamp_has_required_fields(client):
    """Test that timestamp response has required fields."""
    resp = client.get(ep.TIMESTAMP_EP)
    data = resp.get_json()
    
    # This will raise KeyError if fields are missing
    with pytest.raises(KeyError, match="nonexistent"):
        _ = data['nonexistent']
    
    # Verify required fields exist (won't raise)
    assert ep.TIMESTAMP_RESP in data
    assert 'unix' in data


# SKIP: Conditional skip based on environment
@pytest.mark.skip(reason="Integration test - requires external service")
def test_timestamp_matches_external_service(client):
    """Test that our timestamp matches an external time service."""
    # This would require network access in real scenario
    pass


# Test timestamp endpoint returns current time
def test_timestamp_returns_valid_format(client):
    """Test that timestamp endpoint returns valid ISO format."""
    resp = client.get(ep.TIMESTAMP_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    # Verify we can parse the ISO timestamp
    timestamp_str = data[ep.TIMESTAMP_RESP]
    parsed_time = datetime.fromisoformat(timestamp_str)
    assert isinstance(parsed_time, datetime)


# HEALTH ENDPOINT TESTS
def test_health_endpoint_returns_ok(client):
    """Test that health endpoint returns successfully."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert ep.HEALTH_RESP in data
    assert data[ep.HEALTH_RESP] in ['ok', 'degraded']


def test_health_endpoint_has_timestamp(client):
    """Test that health endpoint includes timestamp information."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert 'timestamp' in data
    assert 'unix' in data
    
    # Verify timestamp is valid ISO format
    timestamp_str = data['timestamp']
    parsed_time = datetime.fromisoformat(timestamp_str)
    assert isinstance(parsed_time, datetime)


def test_health_endpoint_has_db_status(client):
    """Test that health endpoint includes database status."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert 'db' in data
    assert 'ok' in data['db']


def test_health_endpoint_has_collection_stats(client):
    """Test that health endpoint includes collection statistics."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert 'collections' in data
    
    # Check if stats were gathered successfully
    if 'error' not in data['collections']:
        assert 'countries' in data['collections']
        assert 'states' in data['collections']
        assert 'cities' in data['collections']
        
        # Verify each collection has count
        for collection_name in ['countries', 'states', 'cities']:
            collection_data = data['collections'][collection_name]
            assert 'count' in collection_data
            assert isinstance(collection_data['count'], int)
            assert collection_data['count'] >= 0


def test_health_endpoint_has_database_stats(client):
    """Test that health endpoint includes database statistics."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert 'database_stats' in data
    
    # Check if database stats were gathered successfully
    if data['database_stats']:  # Not empty dict
        assert 'database' in data['database_stats']
        assert 'collections' in data['database_stats']
        assert 'data_size_bytes' in data['database_stats']


def test_health_endpoint_has_total_documents(client):
    """Test that health endpoint includes total document count."""
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert 'total_documents' in data
    assert isinstance(data['total_documents'], int)
    assert data['total_documents'] >= 0


@patch('server.endpoints.dbc.get_client')
def test_health_endpoint_handles_db_failure(mock_get_client, client):
    """Test that health endpoint gracefully handles database failures."""
    # Mock database failure
    mock_get_client.side_effect = Exception("Database connection failed")
    
    resp = client.get(ep.HEALTH_EP)
    data = resp.get_json()
    
    assert resp.status_code == OK
    assert data[ep.HEALTH_RESP] == 'degraded'
    assert 'collections' in data
    assert 'error' in data['collections']