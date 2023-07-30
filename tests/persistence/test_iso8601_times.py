import datetime

import pytest

from kopf._core.actions.progression import format_iso8601, parse_iso8601

UTC = datetime.timezone.utc
WEST11 = datetime.timezone(datetime.timedelta(hours=-11))
EAST11 = datetime.timezone(datetime.timedelta(hours=11))


@pytest.mark.parametrize('timestamp, expected', [
    (None, None),
    (datetime.datetime(2000, 1, 1), '2000-01-01T00:00:00.000000'),
    (datetime.datetime(2000, 1, 1, 9, 8, 7, 654321), '2000-01-01T09:08:07.654321'),
    (datetime.datetime(2000, 1, 1, tzinfo=UTC), '2000-01-01T00:00:00.000000+00:00'),
    (datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=UTC), '2000-01-01T09:08:07.654321+00:00'),
    (datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=WEST11), '2000-01-01T00:00:00.000000-11:00'),
    (datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=WEST11), '2000-01-01T09:08:07.654321-11:00'),
    (datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=EAST11), '2000-01-01T00:00:00.000000+11:00'),
    (datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=EAST11), '2000-01-01T09:08:07.654321+11:00'),
])
def test_iso8601_formatting(timestamp, expected):
    result = format_iso8601(timestamp)
    assert result == expected


@pytest.mark.parametrize('timestamp, expected', [
    (None, None),
    ('2000-01-01T00:00:00.000000', datetime.datetime(2000, 1, 1)),
    ('2000-01-01T09:08:07.654321', datetime.datetime(2000, 1, 1, 9, 8, 7, 654321)),
    ('2000-01-01T00:00:00.000000Z', datetime.datetime(2000, 1, 1, tzinfo=UTC)),
    ('2000-01-01T09:08:07.654321Z', datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=UTC)),
    ('2000-01-01T00:00:00.000000+00:00', datetime.datetime(2000, 1, 1, tzinfo=UTC)),
    ('2000-01-01T09:08:07.654321+00:00', datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=UTC)),
    ('2000-01-01T00:00:00.000000-11:00', datetime.datetime(2000, 1, 1, tzinfo=WEST11)),
    ('2000-01-01T09:08:07.654321-11:00', datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=WEST11)),
    ('2000-01-01T00:00:00.000000+11:00', datetime.datetime(2000, 1, 1, tzinfo=EAST11)),
    ('2000-01-01T09:08:07.654321+11:00', datetime.datetime(2000, 1, 1, 9, 8, 7, 654321, tzinfo=EAST11)),
])
def test_iso8601_parsing(timestamp, expected):
    result = parse_iso8601(timestamp)
    assert result == expected
