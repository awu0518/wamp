import pytest

import data.postalcode as pc


TEST_CODE = '99999'


def test_abc_base():
    with pytest.raises(TypeError):
        pc.PostalCode(TEST_CODE)


def test_construct_us_code():
    us_code = pc.USPostalCode(TEST_CODE)
    assert isinstance(us_code, pc.USPostalCode)


def test_construct_us_code_bad_type():
    # must be a string!
    with pytest.raises(TypeError):
        pc.USPostalCode(42)


def test_construct_us_code_bad_length():
    with pytest.raises(ValueError):
        pc.USPostalCode('kjshfjkshdjfkhdjkfhdjkhfjksdhfjk')


def test_construct_us_code_bad_chars():
    with pytest.raises(ValueError):
        pc.USPostalCode('k' * pc.US_POSTCODE_LEN)


def test_construct_us_code_bad_chars2():
    """
    Make sure no char is not a number.
    """
    with pytest.raises(ValueError):
        pc.USPostalCode('9999k')


def test_str():
    us_code = pc.USPostalCode(TEST_CODE)
    code = str(us_code)
    assert code == TEST_CODE


def test_construct_uk_code():
    uk_code = pc.UKPostalCode(pc.TEST_UK_CODE)
    assert isinstance(uk_code, pc.UKPostalCode)


def test_construct_uk_code_short():
    uk_code = pc.UKPostalCode(pc.TEST_UK_CODE_SHORT)
    assert isinstance(uk_code, pc.UKPostalCode)


def test_construct_uk_code_letters_are_lower():
    uk_code = pc.UKPostalCode(pc.TEST_UK_CODE_LOWER)
    assert isinstance(uk_code, pc.UKPostalCode)


def test_construct_uk_code_too_long():
    with pytest.raises(ValueError):
        pc.UKPostalCode('KJSHFJKSHDJFKHDJKFHDJKHFJKSDHFJK')


def test_construct_uk_code_too_short():
    with pytest.raises(ValueError):
        pc.UKPostalCode('KJSHF')


def test_construct_uk_code_bad_pattern1():
    with pytest.raises(ValueError):
        pc.UKPostalCode('09hj kj09')


def test_construct_uk_code_bad_pattern2():
    with pytest.raises(ValueError):
        pc.UKPostalCode('KH7682WD')
