import re

from kopf.structs.references import select_specific_namespaces


def test_empty_pattern_list():
    names = select_specific_namespaces([])
    assert not names


def test_included_empty_string():
    names = select_specific_namespaces([''])
    assert names == {''}


def test_included_exact_strings():
    names = select_specific_namespaces(['ns2', 'ns1'])
    assert names == {'ns1', 'ns2'}


def test_excluded_multipatterns():
    names = select_specific_namespaces(['ns1,ns2'])
    assert not names


def test_excluded_globs():
    names = select_specific_namespaces(['n*s', 'n?s'])
    assert not names


def test_excluded_regexps():
    names = select_specific_namespaces([re.compile(r'ns1')])
    assert not names
