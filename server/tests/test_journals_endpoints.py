"""
Tests for journal endpoints in the Flask API.
"""
from http.client import (
    BAD_REQUEST,
    CREATED,
    NOT_FOUND,
    OK,
    UNAUTHORIZED,
)

from unittest.mock import patch
import pytest
import json

import server.endpoints as ep
import journals.queries as jq
import users.auth as auth

FAKE_USER_ID = 'aaaaaaaaaaaaaaaaaaaaaaaa'
FAKE_JOURNAL_ID = 'bbbbbbbbbbbbbbbbbbbbbbbb'


@pytest.fixture
def client():
    """Fixture to provide a test client for the Flask app."""
    return ep.app.test_client()


@pytest.fixture
def auth_header():
    """Fixture providing a valid Authorization header."""
    token = auth.generate_token(FAKE_USER_ID, 'test@example.com')
    return {'Authorization': f'Bearer {token}'}


@pytest.fixture
def sample_journal():
    """Fixture providing sample journal data."""
    return {
        'title': 'Test Trip',
        'body': 'Had a great time.',
        'location_type': 'city',
        'location_name': 'New York',
        'state_code': 'NY',
    }


def test_get_journals_requires_auth(client):
    """Test that GET /journals without auth returns 401."""
    resp = client.get(ep.JOURNALS_EP)
    assert resp.status_code == UNAUTHORIZED


def test_get_journals_success(client, auth_header):
    """Test successful retrieval of user journals."""
    mock_data = {
        'items': [
            {
                '_id': FAKE_JOURNAL_ID,
                'user_id': FAKE_USER_ID,
                'title': 'Trip',
                'location_type': 'city',
                'location_name': 'NYC',
            }
        ],
        'page': 1,
        'limit': 50,
        'total': 1,
        'pages': 1,
        'has_next': False,
        'has_prev': False,
    }
    with patch.object(jq, 'read_by_user') as mock_read:
        mock_read.return_value = mock_data
        resp = client.get(ep.JOURNALS_EP, headers=auth_header)
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.JOURNALS_RESP in data
        assert data['count'] == 1
        assert 'pagination' in data


def test_create_journal_requires_auth(client, sample_journal):
    """Test that POST /journals without auth returns 401."""
    resp = client.post(
        ep.JOURNALS_EP,
        data=json.dumps(sample_journal),
        content_type='application/json',
    )
    assert resp.status_code == UNAUTHORIZED


def test_create_journal_success(client, auth_header, sample_journal):
    """Test successful journal creation."""
    with patch.object(jq, 'create') as mock_create:
        mock_create.return_value = FAKE_JOURNAL_ID
        resp = client.post(
            ep.JOURNALS_EP,
            data=json.dumps(sample_journal),
            content_type='application/json',
            headers=auth_header,
        )
        data = resp.get_json()

        assert resp.status_code == CREATED
        assert ep.MESSAGE in data
        assert 'created successfully' in data[ep.MESSAGE]
        assert data['id'] == FAKE_JOURNAL_ID
        mock_create.assert_called_once_with(FAKE_USER_ID, sample_journal)


def test_create_journal_validation_error(client, auth_header):
    """Test journal creation with invalid data returns 400."""
    invalid = {'body': 'no title or location'}
    with patch.object(jq, 'create') as mock_create:
        mock_create.side_effect = ValueError('Missing required fields: title')
        resp = client.post(
            ep.JOURNALS_EP,
            data=json.dumps(invalid),
            content_type='application/json',
            headers=auth_header,
        )
        data = resp.get_json()

        assert resp.status_code == BAD_REQUEST
        assert 'error' in data


def test_get_journal_by_id_requires_auth(client):
    """Test that GET /journals/<id> without auth returns 401."""
    resp = client.get(f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}')
    assert resp.status_code == UNAUTHORIZED


def test_get_journal_by_id_success(client, auth_header):
    """Test successful single journal retrieval."""
    mock_doc = {
        '_id': FAKE_JOURNAL_ID,
        'user_id': FAKE_USER_ID,
        'title': 'Trip',
        'location_type': 'city',
        'location_name': 'NYC',
    }
    with patch.object(jq, 'read_one') as mock_read:
        mock_read.return_value = mock_doc
        resp = client.get(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            headers=auth_header,
        )
        data = resp.get_json()

        assert resp.status_code == OK
        assert ep.JOURNALS_RESP in data
        mock_read.assert_called_once_with(FAKE_JOURNAL_ID, FAKE_USER_ID)


def test_get_journal_by_id_not_found(client, auth_header):
    """Test GET for non-existent journal returns 404."""
    with patch.object(jq, 'read_one') as mock_read:
        mock_read.side_effect = ValueError('Journal not found')
        resp = client.get(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            headers=auth_header,
        )
        assert resp.status_code == NOT_FOUND


def test_update_journal_requires_auth(client):
    """Test that PUT /journals/<id> without auth returns 401."""
    resp = client.put(
        f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
        data=json.dumps({'title': 'New'}),
        content_type='application/json',
    )
    assert resp.status_code == UNAUTHORIZED


def test_update_journal_success(client, auth_header):
    """Test successful journal update."""
    with patch.object(jq, 'update') as mock_update:
        mock_update.return_value = True
        resp = client.put(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            data=json.dumps({'title': 'Updated'}),
            content_type='application/json',
            headers=auth_header,
        )
        data = resp.get_json()

        assert resp.status_code == OK
        assert 'updated successfully' in data[ep.MESSAGE]
        mock_update.assert_called_once_with(
            FAKE_JOURNAL_ID, FAKE_USER_ID, {'title': 'Updated'})


def test_update_journal_not_found(client, auth_header):
    """Test updating a non-existent journal returns 404."""
    with patch.object(jq, 'update') as mock_update:
        mock_update.side_effect = ValueError('Journal not found')
        resp = client.put(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            data=json.dumps({'title': 'X'}),
            content_type='application/json',
            headers=auth_header,
        )
        assert resp.status_code == NOT_FOUND


def test_delete_journal_requires_auth(client):
    """Test that DELETE /journals/<id> without auth returns 401."""
    resp = client.delete(f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}')
    assert resp.status_code == UNAUTHORIZED


def test_delete_journal_success(client, auth_header):
    """Test successful journal deletion."""
    with patch.object(jq, 'delete') as mock_delete:
        mock_delete.return_value = True
        resp = client.delete(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            headers=auth_header,
        )
        data = resp.get_json()

        assert resp.status_code == OK
        assert 'deleted successfully' in data[ep.MESSAGE]
        mock_delete.assert_called_once_with(
            FAKE_JOURNAL_ID, FAKE_USER_ID)


def test_delete_journal_not_found(client, auth_header):
    """Test deleting a non-existent journal returns 404."""
    with patch.object(jq, 'delete') as mock_delete:
        mock_delete.side_effect = ValueError('Journal not found')
        resp = client.delete(
            f'{ep.JOURNALS_EP}/{FAKE_JOURNAL_ID}',
            headers=auth_header,
        )
        assert resp.status_code == NOT_FOUND
