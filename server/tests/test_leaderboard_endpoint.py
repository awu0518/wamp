from http.client import OK, INTERNAL_SERVER_ERROR
from unittest.mock import patch

from server.endpoints import LEADERBOARD_EP


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

    with patch("endpoints.jq.get_leaderboard", return_value=fake_leaderboard):
        response = client.get(LEADERBOARD_EP)

    assert response.status_code == OK
    assert response.get_json() == fake_leaderboard


def test_get_leaderboard_failure(client):
    with patch("endpoints.jq.get_leaderboard", side_effect=Exception("db down")):
        response = client.get(LEADERBOARD_EP)

    assert response.status_code == INTERNAL_SERVER_ERROR
    assert "error" in response.get_json()