"""
Backfill country_iso_code for existing city records.
"""
import argparse
import json
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.append(str(Path(__file__).resolve().parents[1]))

import data.db_connect as dbc

CITY_COLLECTION = 'cities'
STATE_COLLECTION = 'states'

NAME = 'name'
STATE_CODE = 'state_code'
COUNTRY_ISO_CODE = 'country_iso_code'
REVIEW_COUNT = 'review_count'

DEFAULT_REPORT_PATH = Path(__file__).resolve().with_name(
    'city_country_iso_code_backfill_report.json'
)


def _normalize_state_code(state_code: str) -> str:
    if not isinstance(state_code, str):
        raise ValueError('state_code must be a string')
    normalized = state_code.strip().upper()
    if len(normalized) != 2:
        raise ValueError('state_code must be exactly 2 characters')
    return normalized


def _normalize_country_iso_code(country_iso_code: str) -> str:
    if not isinstance(country_iso_code, str):
        raise ValueError('country_iso_code must be a string')
    normalized = country_iso_code.strip().upper()
    if len(normalized) < 2 or len(normalized) > 3:
        raise ValueError('country_iso_code must be 2-3 characters')
    return normalized


def _is_missing_country_iso_code(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _build_state_country_map() -> dict[str, str]:
    states = dbc.read(STATE_COLLECTION)
    state_country_map: dict[str, str] = {}

    for state in states:
        state_code = state.get(STATE_CODE)
        country_iso_code = state.get(COUNTRY_ISO_CODE)
        if not isinstance(state_code, str) or not isinstance(country_iso_code, str):
            continue

        normalized_state_code = state_code.strip().upper()
        normalized_country_iso_code = country_iso_code.strip().upper()
        if not normalized_state_code or not normalized_country_iso_code:
            continue

        state_country_map[normalized_state_code] = normalized_country_iso_code

    return state_country_map


def backfill_city_country_iso_code(report_path: str = None) -> dict:
    """
    Backfill missing country_iso_code values on city records.

    Cities are matched to states by state_code. Rows that cannot be resolved
    are returned in the report payload and optionally written to disk.
    """
    state_country_map = _build_state_country_map()
    cities = dbc.read(CITY_COLLECTION)

    updated_count = 0
    unresolved_rows = []

    for city in cities:
        city_name = city.get(NAME)
        state_code = city.get(STATE_CODE)
        country_iso_code = city.get(COUNTRY_ISO_CODE)

        if not isinstance(city_name, str) or not city_name.strip():
            unresolved_rows.append({
                NAME: city_name,
                STATE_CODE: state_code,
                COUNTRY_ISO_CODE: country_iso_code,
                'reason': 'missing city name',
            })
            continue

        if not _is_missing_country_iso_code(country_iso_code):
            continue

        if not isinstance(state_code, str) or not state_code.strip():
            unresolved_rows.append({
                NAME: city_name,
                STATE_CODE: state_code,
                COUNTRY_ISO_CODE: country_iso_code,
                'reason': 'missing state_code',
            })
            continue

        normalized_state_code = _normalize_state_code(state_code)
        matched_country_iso_code = state_country_map.get(normalized_state_code)
        if not matched_country_iso_code:
            unresolved_rows.append({
                NAME: city_name,
                STATE_CODE: normalized_state_code,
                COUNTRY_ISO_CODE: country_iso_code,
                'reason': 'no matching state found',
            })
            continue

        normalized_country_iso_code = _normalize_country_iso_code(
            matched_country_iso_code
        )
        dbc.update(
            CITY_COLLECTION,
            {NAME: city_name},
            {COUNTRY_ISO_CODE: normalized_country_iso_code},
        )
        updated_count += 1

    report = {
        'collection': CITY_COLLECTION,
        'updated_count': updated_count,
        'unresolved_count': len(unresolved_rows),
        'unresolved_rows': unresolved_rows,
    }

    if report_path:
        report_file = Path(report_path)
        report_file.write_text(json.dumps(report, indent=2), encoding='utf-8')

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            'Backfill country_iso_code for city records using the matching '\
            'state country code.'
        )
    )
    parser.add_argument(
        '--report-path',
        default=str(DEFAULT_REPORT_PATH),
        help=(
            'Path to write the unresolved-row report JSON file. '
            'Defaults to a file beside this script.'
        ),
    )
    args = parser.parse_args()

    report = backfill_city_country_iso_code(args.report_path)
    print(
        'Updated '
        f"{report['updated_count']} city record(s); "
        f"{report['unresolved_count']} unresolved row(s)."
    )
    if report['unresolved_rows']:
        print(f"Unresolved report written to: {args.report_path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())