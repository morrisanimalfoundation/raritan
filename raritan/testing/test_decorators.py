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
    return {
        settings.data_dir: {
            'test_fixture': 'test.txt'
        }
    }


@input_data
def missing_input_data():
    return {
        settings.data_dir: {
            'test_fixture': 'velcro.txt'
        }
    }


@task
def transform_data(a, b) -> tuple:
    sleep(1)
    return a + b, a * b


@output_data
def dump_data() -> dict:
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
    get_data()
    transform_data(2, 3)
    dump_data()


def test_input_decorator() -> None:
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
    for item in (f'{settings.data_dir}/finalized_fixture.csv', f'{settings.data_dir}/finalized_fixture.sql'):
        if os.path.isfile(item):
            os.remove(item)
