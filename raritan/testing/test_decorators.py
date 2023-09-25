import os.path
from time import sleep

import pytest

from raritan.context import context
from raritan.decorators import flow, input_data, output_data, task
from raritan.logger import console

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
            'test_fixture': 'test.txt'
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
            'finalized_fixture': {
                'formats': ('csv', 'sql'),
                'output_kwargs': {
                    'fee': True,
                    'fi': False,
                    'fo': True,
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
    Tests the input_data decorator, both good adn bad.
    """
    with console.capture() as capture:
        get_data()
        with pytest.raises(FileNotFoundError):
            missing_input_data()
    fixture = context.get_data_reference('test_fixture')
    log_output = capture.get()
    assert 'Handling asset: ./raritan/testing/fixture/test.txt' in log_output
    assert 'Loaded asset: ./raritan/testing/fixture/test.txt <1s 321d34bc9a'
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


def test_output_decorator() -> None:
    """
    Tests the output decorator both good and bad.
    """
    context.set_data_reference('finalized_fixture', 'here is the final output')
    with console.capture() as capture:
        dump_data()
        with pytest.raises(RuntimeError):
            missing_dump_data()
    log_output = capture.get()
    assert 'Beginning output: ./raritan/testing/fixture/finalized_fixture.csv' in log_output
    assert 'Finished output: ./raritan/testing/fixture/finalized_fixture.csv <1s 321d34bc9a' in log_output
    assert 'Beginning output: ./raritan/testing/fixture/finalized_fixture.sql' in log_output
    assert 'Finished output: ./raritan/testing/fixture/finalized_fixture.sql <1s 321d34bc9a' in log_output
    assert os.path.isfile(f'{settings.data_dir}/finalized_fixture.csv')
    assert os.path.isfile(f'{settings.data_dir}/finalized_fixture.sql')


def test_flow_decorator():
    """
    Tests the flow decorator.
    """
    with console.capture() as capture:
        run_flow()
    log_output = capture.get()
    assert 'Beginning flow: test_decorators' in log_output
    assert 'Started' in log_output
    assert 'Beginning task: transform_data' in log_output
    assert 'Completed task: transform_data 1s' in log_output
    assert 'Beginning output: ./raritan/testing/fixture/finalized_fixture.csv' in log_output
    assert 'Finished output: ./raritan/testing/fixture/finalized_fixture.csv <1s' in log_output
    assert 'Beginning output: ./raritan/testing/fixture/finalized_fixture.sql' in log_output
    assert 'Finished output: ./raritan/testing/fixture/finalized_fixture.sql <1s' in log_output
    assert 'Completed flow run!' in log_output
    assert 'Total duration 1s' in log_output


def teardown_function():
    """
    Removes any leftover output files.
    """
    for item in (f'{settings.data_dir}/finalized_fixture.csv', f'{settings.data_dir}/finalized_fixture.sql'):
        if os.path.isfile(item):
            os.remove(item)
