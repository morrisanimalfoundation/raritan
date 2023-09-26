import pytest

from raritan.context import context

"""
Tests context getting.
"""

# Load the test settings module.
context.set_settings_module('raritan.test_settings')

settings = context.get_settings()


def test_context() -> None:
    """
    Tests the options for getting data sources from context.
    """
    for letter in ('a', 'b', 'c', 'd'):
        context.set_data_reference(f'data_source_{letter}', f'Here is data for letter, {letter}')
    context.set_data_reference('a_random_one', 'Nope, not a pattern follower.')
    data = context.get_data_reference('data_source_a')
    assert data == 'Here is data for letter, a'
    data = context.get_data_reference('^data_source_*')
    assert type(data) == dict
    assert len(data.keys()) == 4
    data = context.get_data_reference(['a_random_one', 'data_source_c'])
    assert type(data) == dict
    assert len(data.keys()) == 2
    bad_requests = ('Noids', 7, ['Still', 'Nope'])
    for bad_request in bad_requests:
        with pytest.raises(Exception):
            context.get_data_reference(bad_request)

