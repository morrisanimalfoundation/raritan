import glob
import os.path
import re
from time import sleep

import pytest

from raritan.context import context
from raritan.decorators import flow, input_data, output_data, task
from raritan.logger import console, error

"""
Tests decorators and other basic functionality.
"""

# Load the test settings module.
context.set_settings_module('raritan.test_settings')

settings = context.get_settings()


@input_data
def get_data() -> dict:
    """
    A @input_data implementation.

    Returns
    -------
    data: dict
      A dict of data to load into context.
    """
    return {
        settings.data_dir: {
            'test_fixture': 'test.txt',
            'a_test_fixture': 'test.txt',
            'b_test_fixture': 'test.txt',
            'another_fixture': 'test.txt',
        }
    }


@input_data
def missing_input_data() -> dict:
    """
    A bad @input_data implementation to test the unhappy path.

    Returns
    -------
    data: dict
      A dict of data to load into context.
    """
    return {
        settings.data_dir: {
            'test_fixture': 'velcro.txt'
        }
    }


@task
def transform_data(a: int, b: int) -> tuple:
    """
    A @task implementation.

    Returns
    -------
    data: tuple
      A sum and product of the provideed values.
    """
    sleep(1)
    return a + b, a * b


@output_data
def dump_data() -> dict:
    """
    An @output_data implementation.

    Returns
    -------
    data: tuple
      Data to dump from the context object.
    """
    return {
        settings.data_dir: {
            'another_fixture': {
                'formats': ('csv', 'sql'),
                'output_kwargs': {
                    'fee': True,
                    'fi': False,
                    'fo': True,
                },
            },
            'fixture_group': {
                'data': '.*_test_fixture$',
                'formats': ('csv',),
                'output_kwargs': {
                    'fee': True,
                    'fi': False,
                },
            },
            'different_fixture_group': {
                'data': ['.*_test_fixture$', 'another'],
                'formats': ('csv',),
                'output_kwargs': {
                    'fee': True,
                    'fi': False,
                },
            },
        },
    }


@output_data
def missing_dump_data() -> dict:
    """
    A bad @output_data implementation to test the unhappy path.

    Returns
    -------
    data: tuple
      Data to dump from the context object.
    """
    return {
        settings.data_dir: {
            'missing_some_stuff': {
                'formats': ('csv', 'sql'),
                'output_kwargs': {},
            },
        },
    }


@flow
def run_flow() -> None:
    """
    A @flow implementation that puts all the above together.
    """
    get_data()
    transform_data(2, 3)
    dump_data()


def test_input_decorator() -> None:
    """
    Tests the input_data decorator, both good and bad.
    """
    with console.capture() as capture:
        get_data()
        with pytest.raises(AssertionError):
            missing_input_data()
    fixture = context.get_data_reference('test_fixture')
    log_output = capture.get()
    assert 'Handling asset: test.txt' in log_output
    assert 'Loaded asset: ./raritan/tests/fixture/test.txt <1s 9bbb4fc759'
    assert fixture
    assert 'A tiny fixture for testing IO.' in fixture


def test_task_decorator() -> None:
    """
    Tests the task decorator.
    """
    with console.capture() as capture:
        a_sum, a_product = transform_data(2, 3)
        b_sum, b_product = transform_data(4, 5, task_description='something_else')
    log_output = capture.get()
    assert 'Beginning task: transform_data' in log_output
    assert 'Completed task: transform_data 1s' in log_output
    assert 'Beginning task: something_else' in log_output
    assert 'Completed task: something_else 1s' in log_output
    assert a_sum == 5
    assert a_product == 6
    assert b_sum == 9
    assert b_product == 20


def test_error_message_output() -> None:
    with console.capture() as capture:  # Place console capture context manager here
        try:
            get_data()
            fixture = context.get_data_reference('test_fixture')
            fixture['non_existent_column']
        except TypeError as e:
            error(f"Error occurred: {e}")  # Log the exception using the error() function
    log_output = capture.get()  # Get the captured output, including the exception message
    # The error message has colors, so we need to strip them
    log_output_stripped = re.sub(r'\x1b\[[0-9;]*[mK]', '', log_output)
    assert 'File "/workspace/raritan/tests/test_decorators.py", line 180, in test_error_message_output' in log_output_stripped
    assert "Error occurred: string indices must be integers, not 'str'" in log_output_stripped
    assert "Corrupt Code: fixture['non_existent_column']" in log_output_stripped


def test_output_decorator() -> None:
    """
    Tests the output decorator both good and bad.
    """
    with console.capture() as capture:
        get_data()
        dump_data()
        with pytest.raises(Exception):
            missing_dump_data()
    log_output = capture.get()
    # Test the one to one output.
    assert 'Beginning output: another_fixture in format csv' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.csv <1s 9bbb4fc759' in log_output
    assert 'Beginning output: another_fixture in format sql' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.sql <1s 9bbb4fc759' in log_output
    # Test the many-to-one output.
    assert 'Beginning output: fixture_group in format csv' in log_output
    assert 'Finished output: ./raritan/tests/fixture/fixture_group.zip <1s bc63b64f5a' in log_output
    assert 'Beginning output: different_fixture_group in format csv' in log_output
    assert 'Finished output: ./raritan/tests/fixture/different_fixture_group.zip <1s' in log_output
    assert '8a79d95e42' in log_output
    assert os.path.isfile(f'{settings.data_dir}/another_fixture.csv')
    assert os.path.isfile(f'{settings.data_dir}/another_fixture.sql')
    assert os.path.isfile(f'{settings.data_dir}/fixture_group.zip')
    assert os.path.isfile(f'{settings.data_dir}/different_fixture_group.zip')


def test_flow_decorator() -> None:
    """
    Tests the flow decorator.
    """
    with console.capture() as capture:
        run_flow()
    log_output = capture.get()
    assert 'Beginning flow: test_decorators' in log_output
    assert 'Started' in log_output
    assert 'Loaded asset' in log_output
    assert 'Beginning task: transform_data' in log_output
    assert 'Completed task: transform_data 1s' in log_output
    assert 'Beginning output: another_fixture in format csv' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.csv <1s' in log_output
    assert 'Beginning output: another_fixture in format sql' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.sql <1s' in log_output
    assert 'Completed flow run!' in log_output
    assert 'Total duration 1s' in log_output


def setup_function() -> None:
    """
     Performs setup steps.
    """
    context.clear_data_references()


def teardown_function() -> None:
    """
    Removes any leftover output files.
    """
    fixtures = []
    for extension in ('csv', 'sql', 'zip'):
        fixtures += glob.glob(f'{settings.data_dir}/*.{extension}')
    for item in fixtures:
        if os.path.isfile(item):
            os.remove(item)
