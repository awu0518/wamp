"""
Backfill country_iso_code for existing state records.
"""
import argparse
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

import data.db_connect as dbc
import validation

STATE_COLLECTION = 'states'
COUNTRY_ISO_CODE = 'country_iso_code'
DEFAULT_COUNTRY_ISO_CODE = 'US'


def _normalize_country_iso_code(country_iso_code: str) -> str:
    if not isinstance(country_iso_code, str):
        raise ValueError('country_iso_code must be a string')

    normalized = country_iso_code.strip().upper()
    validation.validate_iso_code(normalized, COUNTRY_ISO_CODE)
    return normalized


def _build_backfill_filter() -> dict:
    return {
        '$or': [
            {COUNTRY_ISO_CODE: {'$exists': False}},
            {COUNTRY_ISO_CODE: None},
            {COUNTRY_ISO_CODE: ''},
        ]
    }


def backfill_state_country_iso_code(country_iso_code: str = DEFAULT_COUNTRY_ISO_CODE) -> int:
    """
    Backfill missing country_iso_code values on the states collection.

    Returns the number of documents modified.
    """
    normalized_country_iso_code = _normalize_country_iso_code(country_iso_code)
    collection = dbc.get_client()[dbc.SE_DB][STATE_COLLECTION]
    result = collection.update_many(
        _build_backfill_filter(),
        {'$set': {COUNTRY_ISO_CODE: normalized_country_iso_code}},
    )
    return result.modified_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Backfill country_iso_code for existing state records.'
    )
    parser.add_argument(
        '--country-iso-code',
        default=DEFAULT_COUNTRY_ISO_CODE,
        help='Country ISO code to set on missing state records (default: US).',
    )
    args = parser.parse_args()

    modified_count = backfill_state_country_iso_code(args.country_iso_code)
    print(f'Updated {modified_count} state record(s).')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())