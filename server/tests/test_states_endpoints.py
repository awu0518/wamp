"""
Basic tests for states endpoints in the Flask API.
Tests essential CRUD operations and basic functionality.
"""

from http.client import (
    BAD_REQUEST,
    CREATED,
    NOT_FOUND,
    OK,
)

from unittest.mock import patch
import pytest
import json

import server.endpoints as ep
import states.queries as stq


@pytest.fixture
def client():
    """Fixture to provide a test client for the Flask app."""
    return ep.app.test_client()


@pytest.fixture
def sample_state():
    """Fixture providing sample state data."""
    return {
        'name': 'Test State',
        'state_code': 'TS'
    }


def test_get_states_success(client):
    """Test successful retrieval of states."""
    with patch.object(stq, 'read') as mock_read:
        mock_states = {
            'New York': {'name': 'New York', 'state_code': 'NY'},
            'Massachusetts': {'name': 'Massachusetts', 'state_code': 'MA'}
        }
        mock_read.return_value = mock_states

        resp = client.get(ep.STATES_EP)
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.STATES_RESP in data
        assert 'count' in data
        assert data['count'] == 2
        assert data[ep.STATES_RESP] == mock_states


def test_create_state_success(client, sample_state):
    """Test successful state creation."""
    with patch.object(stq, 'create') as mock_create:
        mock_create.return_value = "507f1f77bcf86cd799439011"

        resp = client.post(
            ep.STATES_EP,
            data=json.dumps(sample_state),
            content_type='application/json'
        )
        data = resp.get_json()

        assert resp.status_code == CREATED
        assert ep.MESSAGE in data
        assert 'created successfully' in data[ep.MESSAGE]
        assert 'id' in data
        assert data['id'] == "507f1f77bcf86cd799439011"


def test_get_state_by_id_success(client):
    """Test successful retrieval of a state by ID."""
    with patch.object(stq, 'read_one') as mock_read_one:
        mock_state = {'name': 'New York', 'state_code': 'NY'}
        mock_read_one.return_value = mock_state

        resp = client.get(f'{ep.STATES_EP}/New York')
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.STATES_RESP in data
        assert data[ep.STATES_RESP] == mock_state


def test_delete_state_success(client):
    """Test successful state deletion."""
    with patch.object(stq, 'delete') as mock_delete:
        mock_delete.return_value = 1

        resp = client.delete(f'{ep.STATES_EP}/Test State')
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.MESSAGE in data
        assert 'Test State deleted successfully' in data[ep.MESSAGE]


def test_search_states_by_name(client):
    """Test state search by name."""
    with patch.object(stq, 'search') as mock_search:
        mock_results = [
            {'name': 'New York', 'state_code': 'NY'},
            {'name': 'New Mexico', 'state_code': 'NM'}
        ]
        mock_search.return_value = mock_results

        resp = client.get(f'{ep.STATES_SEARCH_EP}?name=New')
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.STATES_RESP in data
        assert 'count' in data
        assert data['count'] == 2
        assert data[ep.STATES_RESP] == mock_results


def test_search_states_missing_params(client):
    """Test state search with no query params."""
    resp = client.get(ep.STATES_SEARCH_EP)
    data = resp.get_json()

    assert resp.status_code == BAD_REQUEST
    assert 'error' in data
    assert 'Provide at least one parameter' in data['error']


def test_create_state_missing_name(client):
    """Test state creation with missing name field."""
    invalid_state = {'state_code': 'TS'}

    with patch.object(stq, 'create') as mock_create:
        mock_create.side_effect = ValueError("Missing required field: name")

        resp = client.post(
            ep.STATES_EP,
            data=json.dumps(invalid_state),
            content_type='application/json'
        )
        data = resp.get_json()

        assert resp.status_code == BAD_REQUEST
        assert 'error' in data
        assert 'Missing required field: name' in data['error']


def test_get_state_by_id_not_found(client):
    """Test retrieval of a missing state."""
    with patch.object(stq, 'read_one') as mock_read_one:
        mock_read_one.side_effect = ValueError("State not found")

        resp = client.get(f'{ep.STATES_EP}/Missing State')
        data = resp.get_json()

        assert resp.status_code == NOT_FOUND
        assert 'error' in data
        assert 'State not found' in data['error']