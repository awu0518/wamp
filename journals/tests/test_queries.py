"""
Tests for journals.queries module.
"""
import pytest
import time

from bson import ObjectId

import journals.queries as jq
import data.db_connect as dbc


FAKE_USER_ID = 'aaaaaaaaaaaaaaaaaaaaaaaa'
ALT_USER_ID = 'bbbbbbbbbbbbbbbbbbbbbbbb'


@pytest.fixture(scope='function')
def temp_journal():
    """Create a temporary journal entry and clean up after test."""
    fields = {
        jq.TITLE: 'Test Journal',
        jq.BODY: 'Some body text.',
        jq.LOCATION_TYPE: 'city',
        jq.LOCATION_NAME: 'Test City',
        jq.STATE_CODE: 'TC',
    }
    journal_id = jq.create(FAKE_USER_ID, fields)
    yield journal_id

    try:
        dbc.delete(
            jq.JOURNAL_COLLECTION,
            {jq.ID: ObjectId(journal_id)},
        )
    except Exception:
        pass


def test_create_journal_success():
    """Test creating a journal entry with valid fields."""
    fields = {
        jq.TITLE: f'Trip {int(time.time())}',
        jq.BODY: 'Had a great time.',
        jq.LOCATION_TYPE: 'country',
        jq.LOCATION_NAME: 'France',
        jq.ISO_CODE: 'FR',
    }
    journal_id = jq.create(FAKE_USER_ID, fields)
    assert journal_id is not None
    assert len(journal_id) == 24

    dbc.delete(
        jq.JOURNAL_COLLECTION,
        {jq.ID: ObjectId(journal_id)},
    )


def test_create_journal_missing_title():
    """Test creating a journal without title raises error."""
    fields = {
        jq.LOCATION_TYPE: 'city',
        jq.LOCATION_NAME: 'Boston',
    }
    with pytest.raises(Exception, match='Missing required fields'):
        jq.create(FAKE_USER_ID, fields)


def test_create_journal_invalid_location_type():
    """Test creating a journal with invalid location_type raises error."""
    fields = {
        jq.TITLE: 'Bad Type',
        jq.LOCATION_TYPE: 'planet',
        jq.LOCATION_NAME: 'Mars',
    }
    with pytest.raises(Exception, match='location_type'):
        jq.create(FAKE_USER_ID, fields)


def test_create_journal_extra_fields():
    """Test creating a journal with extra fields raises error."""
    fields = {
        jq.TITLE: 'Extra',
        jq.LOCATION_TYPE: 'city',
        jq.LOCATION_NAME: 'NYC',
        'secret': 'should not be here',
    }
    with pytest.raises(Exception, match='Unexpected fields'):
        jq.create(FAKE_USER_ID, fields)


def test_read_by_user_returns_results(temp_journal):
    """Test reading journals for a user returns the created journal."""
    data = jq.read_by_user(FAKE_USER_ID)
    assert 'items' in data
    assert data['total'] >= 1
    ids = [item[jq.ID] for item in data['items']]
    assert temp_journal in ids


def test_read_by_user_filters_by_location_type(temp_journal):
    """Test filtering by location_type returns matching entries."""
    data = jq.read_by_user(FAKE_USER_ID, location_type='city')
    assert data['total'] >= 1
    for item in data['items']:
        assert item[jq.LOCATION_TYPE] == 'city'


def test_read_by_user_empty_for_other_user(temp_journal):
    """Test that another user cannot see this user's journals."""
    data = jq.read_by_user(ALT_USER_ID)
    ids = [item[jq.ID] for item in data['items']]
    assert temp_journal not in ids


def test_read_one_success(temp_journal):
    """Test reading a single journal by ID."""
    doc = jq.read_one(temp_journal, FAKE_USER_ID)
    assert doc is not None
    assert doc[jq.TITLE] == 'Test Journal'
    assert doc[jq.LOCATION_TYPE] == 'city'


def test_read_one_wrong_user(temp_journal):
    """Test that another user cannot read this journal."""
    with pytest.raises(ValueError, match='not found'):
        jq.read_one(temp_journal, ALT_USER_ID)


def test_read_one_bad_id():
    """Test reading with an invalid ID raises error."""
    with pytest.raises(ValueError, match='Invalid journal ID'):
        jq.read_one('not-a-valid-id', FAKE_USER_ID)


def test_read_one_nonexistent():
    """Test reading a non-existent journal raises error."""
    fake_id = 'cccccccccccccccccccccccc'
    with pytest.raises(ValueError, match='not found'):
        jq.read_one(fake_id, FAKE_USER_ID)


def test_update_success(temp_journal):
    """Test updating a journal's title."""
    jq.update(temp_journal, FAKE_USER_ID, {jq.TITLE: 'Updated Title'})
    doc = jq.read_one(temp_journal, FAKE_USER_ID)
    assert doc[jq.TITLE] == 'Updated Title'


def test_update_body(temp_journal):
    """Test updating a journal's body."""
    jq.update(temp_journal, FAKE_USER_ID, {jq.BODY: 'New body.'})
    doc = jq.read_one(temp_journal, FAKE_USER_ID)
    assert doc[jq.BODY] == 'New body.'


def test_update_wrong_user(temp_journal):
    """Test that another user cannot update this journal."""
    with pytest.raises(ValueError, match='not found'):
        jq.update(temp_journal, ALT_USER_ID, {jq.TITLE: 'Hacked'})


def test_update_no_fields(temp_journal):
    """Test updating with empty fields raises error."""
    with pytest.raises(ValueError, match='No update fields'):
        jq.update(temp_journal, FAKE_USER_ID, {})


def test_update_disallowed_field(temp_journal):
    """Test updating a non-updatable field raises error."""
    with pytest.raises(Exception, match='Unexpected fields'):
        jq.update(temp_journal, FAKE_USER_ID,
                  {jq.LOCATION_NAME: 'Somewhere'})


def test_delete_success(temp_journal):
    """Test deleting a journal entry."""
    result = jq.delete(temp_journal, FAKE_USER_ID)
    assert result is True

    with pytest.raises(ValueError, match='not found'):
        jq.read_one(temp_journal, FAKE_USER_ID)


def test_delete_wrong_user(temp_journal):
    """Test that another user cannot delete this journal."""
    with pytest.raises(ValueError, match='not found'):
        jq.delete(temp_journal, ALT_USER_ID)


def test_count_by_user(temp_journal):
    """Test counting journals for a user."""
    count = jq.count_by_user(FAKE_USER_ID)
    assert isinstance(count, int)
    assert count >= 1
