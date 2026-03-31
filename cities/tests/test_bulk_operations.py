"""
Tests for bulk operations in cities module.
Uses mocks to avoid DB access and focuses on aggregation logic.
"""
import pytest
from unittest.mock import patch
import cities.queries as cq


@pytest.fixture(scope="function")
def clear_city_cache():
    cq.city_cache.clear()
    yield
    cq.city_cache.clear()


def test_bulk_create_success(clear_city_cache):
    records = [
        {cq.NAME: "Miami", cq.STATE_CODE: "FL"},
        {cq.NAME: "Austin", cq.STATE_CODE: "TX"},
    ]
    with patch.object(cq, "create", side_effect=["id1", "id2"]):
        result = cq.bulk_create(records)
    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["ids"] == ["id1", "id2"]
    assert result["errors"] == []


def test_bulk_create_with_failures(clear_city_cache):
    records = [
        {cq.NAME: "Miami", cq.STATE_CODE: "FL"},
        {cq.NAME: "Austin", cq.STATE_CODE: "TX"},
    ]
    with patch.object(cq, "create") as mock_create:
        mock_create.side_effect = ["id1", ValueError("Duplicate city")]
        result = cq.bulk_create(records)
    assert result["success"] == 1
    assert result["failed"] == 1
    assert result["ids"] == ["id1"]
    assert len(result["errors"]) == 1
    assert result["errors"][0]["index"] == 1
    assert "Duplicate" in result["errors"][0]["error"]


def test_bulk_create_invalid_input(clear_city_cache):
    with pytest.raises(ValueError, match="Records must be a list"):
        cq.bulk_create({"not": "a list"})


def test_bulk_update_success(clear_city_cache):
    updates = [
        {"id": "Miami", "fields": {cq.STATE_CODE: "FL"}},
        {"id": "Austin", "fields": {cq.STATE_CODE: "TX"}},
    ]
    with patch.object(cq, "update", return_value=True):
        result = cq.bulk_update(updates)
    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["errors"] == []


def test_bulk_update_with_failures(clear_city_cache):
    updates = [
        {"id": "Miami", "fields": {cq.STATE_CODE: "FL"}},
        {"id": "Austin", "fields": {cq.STATE_CODE: "TX"}},
    ]
    with patch.object(cq, "update") as mock_update:
        mock_update.side_effect = [True, ValueError("No such city: Austin")]
        result = cq.bulk_update(updates)
    assert result["success"] == 1
    assert result["failed"] == 1
    assert len(result["errors"]) == 1
    assert result["errors"][0]["id"] == "Austin"
    assert "No such city" in result["errors"][0]["error"]


def test_bulk_update_invalid_input(clear_city_cache):
    with pytest.raises(ValueError, match="Updates must be a list"):
        cq.bulk_update({"not": "a list"})


def test_bulk_delete_success(clear_city_cache):
    deletes = [
        {"name": "Miami", "state_code": "FL"},
        {"name": "Austin", "state_code": "TX"},
    ]
    with patch.object(cq, "delete", return_value=True):
        result = cq.bulk_delete(deletes)
    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["errors"] == []


def test_bulk_delete_with_failures(clear_city_cache):
    deletes = [
        {"name": "Miami", "state_code": "FL"},
        {"name": "Austin", "state_code": "TX"},
    ]
    with patch.object(cq, "delete") as mock_delete:
        mock_delete.side_effect = [True, ValueError("No such city: Austin")]
        result = cq.bulk_delete(deletes)
    assert result["success"] == 1
    assert result["failed"] == 1
    assert len(result["errors"]) == 1
    assert "Austin" in result["errors"][0]["id"]
    assert "No such city" in result["errors"][0]["error"]


def test_bulk_delete_invalid_input(clear_city_cache):
    with pytest.raises(ValueError, match="Deletes must be a list"):
        cq.bulk_delete({"not": "a list"})


def test_bulk_delete_country_iso_fallback(clear_city_cache):
    deletes = [
        {"name": "Tokyo", "country_iso_code": "JP"},
        {"name": "Berlin", "country_iso_code": "DE"},
    ]
    with patch.object(cq, "delete", return_value=True):
        result = cq.bulk_delete(deletes)
    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["errors"] == []
