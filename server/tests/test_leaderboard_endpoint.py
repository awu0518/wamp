from http.client import (
    INTERNAL_SERVER_ERROR,
    OK,
)

from unittest.mock import patch
import pytest

import server.endpoints as ep
import journals.queries as jq


@pytest.fixture
def client():
    """Fixture to provide a test client for the Flask app."""
    return ep.app.test_client()


def test_get_leaderboard_success(client):
    fake_leaderboard = {
        "rankings": [
            {
                "user_id": "1",
                "username": "alice",
                "placesVisited": 3,
            }
        ],
        "popularDestinations": [
            {
                "name": "New York",
                "count": 5,
            }
        ],
    }

    with patch.object(jq, 'get_leaderboard') as mock_get_leaderboard:
        mock_get_leaderboard.return_value = fake_leaderboard

        resp = client.get(ep.LEADERBOARD_EP)
        data = resp.get_json()

        assert resp.status_code == OK
        assert data == fake_leaderboard
        assert 'rankings' in data
        assert 'popularDestinations' in data


def test_get_leaderboard_failure(client):
    with patch.object(jq, 'get_leaderboard') as mock_get_leaderboard:
        mock_get_leaderboard.side_effect = Exception('Database error')

        resp = client.get(ep.LEADERBOARD_EP)
        data = resp.get_json()

        assert resp.status_code == INTERNAL_SERVER_ERROR
        assert 'error' in data
        assert 'Database error' in data['error']