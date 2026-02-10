"""
Simple validation utilities for API input validation.
"""
import re
from typing import Any, List, Optional


class ValidationError(ValueError):
    """Custom exception for validation errors."""
    pass


def validate_required_fields(data: dict, required_fields: list) -> None:
    """
    Check if all required fields are present and not empty.

    Args:
        data: Dictionary to validate
        required_fields: List of required field names

    Raises:
        ValidationError: If any required field is missing or empty
    """
    if not isinstance(data, dict):
        raise ValidationError("Request body must be a JSON object")

    missing = []
    empty = []

    for field in required_fields:
        if field not in data:
            missing.append(field)
        elif (data[field] is None or
              (isinstance(data[field], str) and
               not data[field].strip())):
            empty.append(field)

    if missing:
        raise ValidationError(f"Missing required fields: {', '.join(missing)}")
    if empty:
        raise ValidationError(f"Fields cannot be empty: {', '.join(empty)}")


def validate_string_length(value: str, field_name: str,
                           min_length: int = 0,
                           max_length: int = 100) -> None:
    """
    Validate string length.

    Args:
        value: String to validate
        field_name: Name of the field
        min_length: Minimum allowed length
        max_length: Maximum allowed length

    Raises:
        ValidationError: If string length is invalid
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if len(value) < min_length:
        raise ValidationError(
            f"{field_name} must be at least {min_length} characters"
        )
    if len(value) > max_length:
        raise ValidationError(
            f"{field_name} must be at most {max_length} characters"
        )


def validate_exact_length(value: str, field_name: str, length: int) -> None:
    """
    Validate exact string length.

    Args:
        value: String to validate
        field_name: Name of the field
        length: Required exact length

    Raises:
        ValidationError: If string length doesn't match
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if len(value) != length:
        raise ValidationError(
            f"{field_name} must be exactly {length} characters"
        )


def validate_pattern(value: str, field_name: str, pattern: str,
                     description: str = "valid format") -> None:
    """
    Validate string against regex pattern.

    Args:
        value: String to validate
        field_name: Name of the field
        pattern: Regex pattern
        description: Human-readable description of expected format

    Raises:
        ValidationError: If string doesn't match pattern
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if not re.match(pattern, value):
        raise ValidationError(
            f"{field_name} must match {description}"
        )


def validate_alphanumeric(value: str, field_name: str,
                          allow_spaces: bool = False) -> None:
    """
    Validate that string contains only alphanumeric characters.

    Args:
        value: String to validate
        field_name: Name of the field
        allow_spaces: Whether to allow spaces

    Raises:
        ValidationError: If string contains invalid characters
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    pattern = r'^[a-zA-Z0-9\s]+$' if allow_spaces else r'^[a-zA-Z0-9]+$'
    if not re.match(pattern, value):
        msg = (f"{field_name} must contain only letters, numbers" +
               (", and spaces" if allow_spaces else ""))
        raise ValidationError(msg)


def validate_alpha(value: str, field_name: str,
                   allow_spaces: bool = True) -> None:
    """
    Validate that string contains only alphabetic characters.

    Args:
        value: String to validate
        field_name: Name of the field
        allow_spaces: Whether to allow spaces

    Raises:
        ValidationError: If string contains invalid characters
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    pattern = r'^[a-zA-Z\s]+$' if allow_spaces else r'^[a-zA-Z]+$'
    if not re.match(pattern, value):
        msg = (f"{field_name} must contain only letters" +
               (", and spaces" if allow_spaces else ""))
        raise ValidationError(msg)


def validate_uppercase(value: str, field_name: str) -> None:
    """
    Validate that string is uppercase.

    Args:
        value: String to validate
        field_name: Name of the field

    Raises:
        ValidationError: If string is not uppercase
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if value != value.upper():
        raise ValidationError(f"{field_name} must be uppercase")


def validate_enum(value: Any, field_name: str,
                  allowed_values: List[Any]) -> None:
    """
    Validate that value is in allowed set.

    Args:
        value: Value to validate
        field_name: Name of the field
        allowed_values: List of allowed values

    Raises:
        ValidationError: If value not in allowed set
    """
    if value not in allowed_values:
        allowed_str = ', '.join(map(str, allowed_values))
        raise ValidationError(
            f"{field_name} must be one of: {allowed_str}"
        )


def validate_integer(value: Any, field_name: str,
                     min_value: Optional[int] = None,
                     max_value: Optional[int] = None) -> None:
    """
    Validate integer value and range.

    Args:
        value: Value to validate
        field_name: Name of the field
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)

    Raises:
        ValidationError: If value is not valid integer or out of range
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError(f"{field_name} must be an integer")

    if min_value is not None and value < min_value:
        raise ValidationError(
            f"{field_name} must be at least {min_value}"
        )
    if max_value is not None and value > max_value:
        raise ValidationError(
            f"{field_name} must be at most {max_value}"
        )


def validate_positive_integer(value: Any, field_name: str) -> None:
    """
    Validate positive integer.

    Args:
        value: Value to validate
        field_name: Name of the field

    Raises:
        ValidationError: If value is not a positive integer
    """
    validate_integer(value, field_name, min_value=1)


def validate_pagination_params(page: Any, limit: Any,
                               field_page: str = 'page',
                               field_limit: str = 'limit',
                               max_limit: int = 1000) -> tuple[int, int]:
    try:
        page_int = int(page)
    except Exception:
        raise ValidationError(f"{field_page} must be an integer")
    if page_int < 1:
        raise ValidationError(f"{field_page} must be at least 1")

    try:
        limit_int = int(limit)
    except Exception:
        raise ValidationError(f"{field_limit} must be an integer")
    if limit_int < 1:
        raise ValidationError(f"{field_limit} must be at least 1")
    if limit_int > max_limit:
        raise ValidationError(f"{field_limit} must be at most {max_limit}")

    return page_int, limit_int


def validate_type(value: Any, field_name: str, expected_type: type) -> None:
    """
    Validate value type.

    Args:
        value: Value to validate
        field_name: Name of the field
        expected_type: Expected type

    Raises:
        ValidationError: If value type doesn't match
    """
    if not isinstance(value, expected_type):
        raise ValidationError(
            f"{field_name} must be of type {expected_type.__name__}"
        )


def validate_iso_code(value: str, field_name: str = "iso_code") -> None:
    """
    Validate ISO country code format (2-3 uppercase letters).

    Args:
        value: String to validate
        field_name: Name of the field

    Raises:
        ValidationError: If not a valid ISO code format
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if not re.match(r'^[A-Z]{2,3}$', value):
        raise ValidationError(
            f"{field_name} must be 2-3 uppercase letters"
        )


def validate_state_code(value: str, field_name: str = "state_code") -> None:
    """
    Validate US state code format (2 uppercase letters).

    Args:
        value: String to validate
        field_name: Name of the field

    Raises:
        ValidationError: If not a valid state code format
    """
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be a string")

    if not re.match(r'^[A-Z]{2}$', value):
        raise ValidationError(
            f"{field_name} must be exactly 2 uppercase letters"
        )


def validate_no_extra_fields(data: dict, allowed_fields: List[str]) -> None:
    """
    Validate that data contains no unexpected fields.

    Args:
        data: Dictionary to validate
        allowed_fields: List of allowed field names

    Raises:
        ValidationError: If data contains extra fields
    """
    if not isinstance(data, dict):
        raise ValidationError("Request body must be a JSON object")

    extra = set(data.keys()) - set(allowed_fields)
    if extra:
        raise ValidationError(
            f"Unexpected fields: {', '.join(sorted(extra))}"
        )
