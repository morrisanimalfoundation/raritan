import datetime

from raritan.decorators import _get_formatted_duration

"""
Test the duration formatting function.
"""


def test_formatting_some_durations() -> None:
    """
    Tests the duration formatter.
    """
    start = datetime.datetime.now()
    end = start + datetime.timedelta(seconds=30)
    duration = _get_formatted_duration(start, end)
    assert duration == '30s'
    end = start + datetime.timedelta(seconds=550)
    duration = _get_formatted_duration(start, end)
    assert duration == '9m10s'
    end = start + datetime.timedelta(seconds=4000)
    duration = _get_formatted_duration(start, end)
    assert duration == '1h6m40s'
    end = start
    duration = _get_formatted_duration(start, end)
    assert duration == '<1s'
