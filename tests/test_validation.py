"""
Comprehensive tests for the validation module.
"""
import pytest
from validation import (
    ValidationError,
    validate_required_fields,
    validate_string_length,
    validate_exact_length,
    validate_pattern,
    validate_alphanumeric,
    validate_alpha,
    validate_uppercase,
    validate_enum,
    validate_integer,
    validate_positive_integer,
    validate_type,
    validate_iso_code,
    validate_state_code,
    validate_no_extra_fields,
    normalize_upper_code,
    normalize_state_code,
    normalize_iso_code,
)


class TestRequiredFields:
    def test_valid_data(self):
        data = {'name': 'Test', 'code': 'TC'}
        validate_required_fields(data, ['name', 'code'])
        # Should not raise

    def test_missing_field(self):
        data = {'name': 'Test'}
        with pytest.raises(ValidationError) as exc:
            validate_required_fields(data, ['name', 'code'])
        assert 'Missing required fields: code' in str(exc.value)

    def test_empty_string(self):
        data = {'name': '', 'code': 'TC'}
        with pytest.raises(ValidationError) as exc:
            validate_required_fields(data, ['name', 'code'])
        assert 'Fields cannot be empty: name' in str(exc.value)

    def test_none_value(self):
        data = {'name': None, 'code': 'TC'}
        with pytest.raises(ValidationError) as exc:
            validate_required_fields(data, ['name', 'code'])
        assert 'Fields cannot be empty: name' in str(exc.value)

    def test_whitespace_only(self):
        data = {'name': '   ', 'code': 'TC'}
        with pytest.raises(ValidationError) as exc:
            validate_required_fields(data, ['name', 'code'])
        assert 'Fields cannot be empty: name' in str(exc.value)

    def test_not_dict(self):
        with pytest.raises(ValidationError) as exc:
            validate_required_fields(['name'], ['name'])
        assert 'must be a JSON object' in str(exc.value)


class TestStringLength:
    def test_valid_length(self):
        validate_string_length('Test', 'name', min_length=1, max_length=10)
        # Should not raise

    def test_too_long(self):
        with pytest.raises(ValidationError) as exc:
            validate_string_length('Test' * 100, 'name', max_length=10)
        assert 'must be at most 10 characters' in str(exc.value)

    def test_too_short(self):
        with pytest.raises(ValidationError) as exc:
            validate_string_length('T', 'name', min_length=2, max_length=10)
        assert 'must be at least 2 characters' in str(exc.value)

    def test_not_string(self):
        with pytest.raises(ValidationError) as exc:
            validate_string_length(123, 'name')
        assert 'must be a string' in str(exc.value)


class TestExactLength:
    def test_valid_length(self):
        validate_exact_length('US', 'iso_code', 2)
        # Should not raise

    def test_too_long(self):
        with pytest.raises(ValidationError) as exc:
            validate_exact_length('USA', 'iso_code', 2)
        assert 'must be exactly 2 characters' in str(exc.value)

    def test_too_short(self):
        with pytest.raises(ValidationError) as exc:
            validate_exact_length('U', 'iso_code', 2)
        assert 'must be exactly 2 characters' in str(exc.value)


class TestPattern:
    def test_valid_pattern(self):
        validate_pattern('ABC123', 'code', r'^[A-Z0-9]+$', 'uppercase alphanumeric')
        # Should not raise

    def test_invalid_pattern(self):
        with pytest.raises(ValidationError) as exc:
            validate_pattern('abc123', 'code', r'^[A-Z0-9]+$', 'uppercase alphanumeric')
        assert 'must match uppercase alphanumeric' in str(exc.value)


class TestAlphanumeric:
    def test_valid_alphanumeric(self):
        validate_alphanumeric('Test123', 'code')
        # Should not raise

    def test_with_spaces_allowed(self):
        validate_alphanumeric('Test 123', 'code', allow_spaces=True)
        # Should not raise

    def test_with_spaces_not_allowed(self):
        with pytest.raises(ValidationError) as exc:
            validate_alphanumeric('Test 123', 'code', allow_spaces=False)
        assert 'must contain only letters, numbers' in str(exc.value)

    def test_special_characters(self):
        with pytest.raises(ValidationError) as exc:
            validate_alphanumeric('Test-123', 'code')
        assert 'must contain only letters, numbers' in str(exc.value)


class TestAlpha:
    def test_valid_alpha(self):
        validate_alpha('Test', 'name')
        # Should not raise

    def test_with_spaces_allowed(self):
        validate_alpha('New York', 'name', allow_spaces=True)
        # Should not raise

    def test_with_spaces_not_allowed(self):
        with pytest.raises(ValidationError) as exc:
            validate_alpha('New York', 'name', allow_spaces=False)
        assert 'must contain only letters' in str(exc.value)

    def test_with_numbers(self):
        with pytest.raises(ValidationError) as exc:
            validate_alpha('Test123', 'name')
        assert 'must contain only letters' in str(exc.value)


class TestUppercase:
    def test_valid_uppercase(self):
        validate_uppercase('TEST', 'code')
        # Should not raise

    def test_lowercase(self):
        with pytest.raises(ValidationError) as exc:
            validate_uppercase('Test', 'code')
        assert 'must be uppercase' in str(exc.value)

    def test_mixed_case(self):
        with pytest.raises(ValidationError) as exc:
            validate_uppercase('TeSt', 'code')
        assert 'must be uppercase' in str(exc.value)


class TestEnum:
    def test_valid_value(self):
        validate_enum('asc', 'order', ['asc', 'desc'])
        # Should not raise

    def test_invalid_value(self):
        with pytest.raises(ValidationError) as exc:
            validate_enum('invalid', 'order', ['asc', 'desc'])
        assert 'must be one of: asc, desc' in str(exc.value)


class TestInteger:
    def test_valid_integer(self):
        validate_integer(42, 'count')
        # Should not raise

    def test_with_min_value(self):
        validate_integer(10, 'count', min_value=5)
        # Should not raise

    def test_below_min_value(self):
        with pytest.raises(ValidationError) as exc:
            validate_integer(3, 'count', min_value=5)
        assert 'must be at least 5' in str(exc.value)

    def test_with_max_value(self):
        validate_integer(10, 'count', max_value=20)
        # Should not raise

    def test_above_max_value(self):
        with pytest.raises(ValidationError) as exc:
            validate_integer(25, 'count', max_value=20)
        assert 'must be at most 20' in str(exc.value)

    def test_not_integer(self):
        with pytest.raises(ValidationError) as exc:
            validate_integer('42', 'count')
        assert 'must be an integer' in str(exc.value)

    def test_float(self):
        with pytest.raises(ValidationError) as exc:
            validate_integer(42.5, 'count')
        assert 'must be an integer' in str(exc.value)

    def test_boolean(self):
        with pytest.raises(ValidationError) as exc:
            validate_integer(True, 'count')
        assert 'must be an integer' in str(exc.value)


class TestPositiveInteger:
    def test_valid_positive(self):
        validate_positive_integer(10, 'count')
        # Should not raise

    def test_zero(self):
        with pytest.raises(ValidationError) as exc:
            validate_positive_integer(0, 'count')
        assert 'must be at least 1' in str(exc.value)

    def test_negative(self):
        with pytest.raises(ValidationError) as exc:
            validate_positive_integer(-5, 'count')
        assert 'must be at least 1' in str(exc.value)


class TestType:
    def test_valid_type(self):
        validate_type('test', 'name', str)
        validate_type(42, 'count', int)
        validate_type({'key': 'value'}, 'data', dict)
        # Should not raise

    def test_invalid_type(self):
        with pytest.raises(ValidationError) as exc:
            validate_type('test', 'count', int)
        assert 'must be of type int' in str(exc.value)


class TestISOCode:
    def test_valid_two_letter(self):
        validate_iso_code('US', 'iso_code')
        # Should not raise

    def test_valid_three_letter(self):
        validate_iso_code('USA', 'iso_code')
        # Should not raise

    def test_lowercase(self):
        with pytest.raises(ValidationError) as exc:
            validate_iso_code('us', 'iso_code')
        assert 'must be 2-3 uppercase letters' in str(exc.value)

    def test_too_long(self):
        with pytest.raises(ValidationError) as exc:
            validate_iso_code('USAA', 'iso_code')
        assert 'must be 2-3 uppercase letters' in str(exc.value)

    def test_too_short(self):
        with pytest.raises(ValidationError) as exc:
            validate_iso_code('U', 'iso_code')
        assert 'must be 2-3 uppercase letters' in str(exc.value)

    def test_with_numbers(self):
        with pytest.raises(ValidationError) as exc:
            validate_iso_code('U2', 'iso_code')
        assert 'must be 2-3 uppercase letters' in str(exc.value)


class TestStateCode:
    def test_valid_state_code(self):
        validate_state_code('NY', 'state_code')
        validate_state_code('CA', 'state_code')
        # Should not raise

    def test_lowercase(self):
        with pytest.raises(ValidationError) as exc:
            validate_state_code('ny', 'state_code')
        assert 'must be exactly 2 uppercase letters' in str(exc.value)

    def test_too_long(self):
        with pytest.raises(ValidationError) as exc:
            validate_state_code('NYC', 'state_code')
        assert 'must be exactly 2 uppercase letters' in str(exc.value)

    def test_too_short(self):
        with pytest.raises(ValidationError) as exc:
            validate_state_code('N', 'state_code')
        assert 'must be exactly 2 uppercase letters' in str(exc.value)

    def test_with_numbers(self):
        with pytest.raises(ValidationError) as exc:
            validate_state_code('N2', 'state_code')
        assert 'must be exactly 2 uppercase letters' in str(exc.value)


class TestNormalizationHelpers:
    def test_normalize_upper_code(self):
        assert normalize_upper_code(' ny ', 'state_code') == 'NY'

    def test_normalize_state_code_none(self):
        assert normalize_state_code(None, 'state_code') == ''

    def test_normalize_iso_code(self):
        assert normalize_iso_code(' us ', 'iso_code') == 'US'

    def test_normalize_raises_on_non_string(self):
        with pytest.raises(ValidationError) as exc:
            normalize_iso_code(123, 'iso_code')
        assert 'iso_code must be a string' in str(exc.value)


class TestNoExtraFields:
    def test_valid_no_extra(self):
        data = {'name': 'Test', 'code': 'TC'}
        validate_no_extra_fields(data, ['name', 'code'])
        # Should not raise

    def test_subset_of_allowed(self):
        data = {'name': 'Test'}
        validate_no_extra_fields(data, ['name', 'code'])
        # Should not raise

    def test_extra_fields(self):
        data = {'name': 'Test', 'code': 'TC', 'extra': 'field'}
        with pytest.raises(ValidationError) as exc:
            validate_no_extra_fields(data, ['name', 'code'])
        assert 'Unexpected fields: extra' in str(exc.value)

    def test_multiple_extra_fields(self):
        data = {'name': 'Test', 'extra1': 'field1', 'extra2': 'field2'}
        with pytest.raises(ValidationError) as exc:
            validate_no_extra_fields(data, ['name'])
        assert 'Unexpected fields:' in str(exc.value)
        assert 'extra1' in str(exc.value)
        assert 'extra2' in str(exc.value)
