import glob
import os.path
import re
from time import sleep

import pandas as pd

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
        },
    }


@input_data(parallel=False)
def get_missing_nonoptional_file() -> dict:
    """
    Retrieves a dictionary describing assets, including a missing non-optional file.

    Returns
    -------
    dict
        A dictionary describing the assets to be loaded.
    """
    return {
        settings.data_dir: {
            'missing_nonoptional': {'file': 'missing_nonoptional.csv', 'optional': False},
        }
    }


poison_exposure_dictionary = {'input_substance': 'string', 'output_substance': 'string', 'drop': 'Int64', 'resolved': 'Int64'}
poison_exposure_dictionary_df = pd.DataFrame(columns=poison_exposure_dictionary.keys()).astype(poison_exposure_dictionary)


@input_data(parallel=False)
def get_missing_optional_file_with_schema() -> dict:
    """
    Retrieves a dictionary describing assets, including missing optional files with and without default dictionaries.

    Returns
    -------
    dict
        A dictionary describing the assets to be loaded.
    """
    return {
        settings.data_dir: {
            'poison_exposure_dictionary': {
                'optional': True,
                'file': 'poison_substance_dictionary.csv',
                'default_dictionary': poison_exposure_dictionary_df
            }
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
    context.print_all_data_references()
    dump_data()


def test_input_decorator() -> None:
    """
    Tests the input_data decorator, both good and bad.
    """
    with console.capture() as capture:  # Place console capture context manager here
        try:
            get_data()
        except Exception as e:
            error(f"Error occurred: {e}")  # Log the exception using the error() function
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


def test_input_dictionary_messages() -> None:
    """
    Test function to verify error message output when encountering missing optional files.

    This test captures console output while attempting to retrieve missing optional files
    with and without default dictionaries. If an exception occurs during the attempt, it is
    logged using the error() function.

    Raises
    ------
    AssertionError
        If the expected error messages are not found in the log output.
    """
    with console.capture() as capture:  # Place console capture context manager here
        try:
            get_missing_optional_file_with_schema()
        except Exception as e:
            error(f"Error occurred: {e}")  # Log the exception using the error() function
    log_output = remove_ansi_escape_sequences(capture.get())
    assert 'Handling asset: missing_optional_no_default.txt' in log_output
    assert 'Optional file missing: missing_optional_no_default.txt, using default' in log_output
    assert 'dictionary.' in log_output
    assert 'Error occurred: No default dictionary provided.' in log_output


def test_input_nonoptional_messages() -> None:
    """
    Test case to validate handling of missing non-optional files.

    This test captures console output while attempting to get a missing non-optional file.
    If an exception occurs during the attempt, it is logged using the error() function.
    The captured output is then checked for the presence of specific log messages.

    Raises:
        AssertionError: If the expected log messages are not found in the captured output.
    """
    with console.capture() as capture:  # Place console capture context manager here
        try:
            get_missing_nonoptional_file()
        except Exception as e:
            error(f"Error occurred: {e}")  # Log the exception using the error() function
    log_output = remove_ansi_escape_sequences(capture.get())
    assert 'Handling asset: missing_nonoptional.csv' in log_output
    # Check if the log message "Non-Optional file missing: missing_nonoptional.csv" is present
    assert 'Error occurred: Non-Optional file missing: missing_nonoptional.csv' in log_output


def remove_ansi_escape_sequences(text):
    """
    Removes ANSI escape sequences (color codes and formatting) from a given text string.

    Parameters
    ----------
    text: str
        The input text containing ANSI escape sequences.

    Returns
    -------
    str
        A new string with ANSI escape sequences removed.
    """
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def test_output_decorator() -> None:
    """
    Tests the output decorator both good and bad.
    """
    with console.capture() as capture:
        get_data()
        context.print_all_data_references()
        dump_data()
    log_output = remove_ansi_escape_sequences(capture.get())
    # Test the one to one output.
    assert 'Beginning output: another_fixture in format csv' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.csv <1s 9bbb4fc759' in log_output
    assert 'Beginning output: another_fixture in format sql' in log_output
    assert 'Finished output: ./raritan/tests/fixture/another_fixture.sql <1s 9bbb4fc759' in log_output
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
    log_output = remove_ansi_escape_sequences(capture.get())
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
