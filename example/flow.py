import pandas as pd

import settings
from raritan.context import context
from raritan.decorators import flow, input_data, output_data, task

"""
A very simple sample flow that requires some additional setup to actually run.
"""


@input_data(parallel=False)
def get_data() -> dict:
    """
    Gets our flow data and stores it inside the context object.
    """
    return {
        settings.input_dir: {
            'labs_ongoing': 'labs_ongoing.csv',
            'labs_historical': 'labs_historical.csv',
        },
        settings.dictionary_dir: {
            'labs_dictionary': 'multi_column_dictionary_labs.csv'
        }
    }


@task
def transform_data():
    """
    A place to do some work.

    Notes
    -----
    If this work becomes lengthy, we try to use a separate cleaning module.
    """
    data = [
        context.get_data_reference('labs_ongoing'),
        context.get_data_reference('labs_historical'),
    ]
    complete_labs = pd.concat(data, ignore_index=True)
    context.set_data_reference('complete_labs', complete_labs)


@output_data
def output_data() -> dict:
    """
    Outputs the data from context with csv and sql strategies.
    """
    return {
        settings.output_dir: {
            'complete_labs': {
                'formats': ('csv', 'sql'),
                'output_kwargs': {},
            },
        },
    }


@flow
def run_flow() -> None:
    """
    Executes our flow in the defined sequence.
    """
    get_data()
    transform_data()
    output_data()


if __name__ == '__main__':
    run_flow()
