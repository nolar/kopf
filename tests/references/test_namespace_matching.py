import re

import pytest

from kopf.structs.references import match_namespace


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', 'ns1', True),
    ('ns1', 'ns', False),
    ('ns1', '', False),
])
def test_exact_values(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', '*', True),
    ('ns1', 'ns*', True),
    ('ns1', 'ns?', True),
    ('ns1', 'ns??', False),
    ('ns1', 'n*1', True),
    ('ns1', 'n?1', True),
    ('ns1', 'n??1', False),
    ('ns1', '*1', True),
    ('ns1', '?1', False),
    ('ns1', '??1', True),
    ('ns1', 'ns*x', False),
])
def test_glob_patterns(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', 'ns1,ns2', True),
    ('ns1', 'ns2,ns1', False),  # the follow-up patterns are for re-inclusion only
])
def test_multiple_patterns(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', ' ns1', True),
    ('ns1', 'ns1 ', True),
    ('ns1', ' ns1 ', True),
    ('ns1', ' ns1 , ns2 ', True),
    ('ns1', ' ns2 , ns1 ', False),  # the follow-up patterns are for re-inclusion only
])
def test_spaces_are_ignored(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', 'ns*,!ns1', False),
    ('ns1', 'ns*,!ns2', True),
    ('ns1', 'ns*,!ns2', True),
    ('ns1', 'ns*,!ns2', True),
])
def test_exclusions_after_inclusion(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', '!ns1', False),
    ('ns1', '!ns2', True),
    ('ns1', '!ns*', False),
])
def test_initial_exclusion_implies_catchall_inclusion(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', 'ns*,!ns1,ns1', True),
    ('ns1', 'ns*,!ns*,ns1', True),
    ('ns1', '!ns*,ns1', True),
    ('ns1', '!ns*,ns?', True),
])
def test_reinclusion_after_exclusion(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected


@pytest.mark.parametrize('name, pattern, expected', [
    ('ns1', re.compile(r'ns1'), True),
    ('ns1', re.compile(r'ns'), False),
    ('ns1', re.compile(r's1'), False),
    ('ns1', re.compile(r'.+s1'), True),
    ('ns1', re.compile(r'ns.+'), True),

])
def test_regexps_with_full_matching(name, pattern, expected):
    result = match_namespace(name=name, pattern=pattern)
    assert result == expected
