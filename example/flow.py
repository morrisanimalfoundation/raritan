import pandas as pd

import settings
from raritan.context import context
from raritan.decorators import flow, input_data, output_data, task


@input_data(parallel=False)
def get_data() -> dict:
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
    data = [
        context.get_data_reference('labs_ongoing'),
        context.get_data_reference('labs_historical'),
    ]
    complete_labs = pd.concat(data, ignore_index=True)
    context.set_data_reference('complete_labs', complete_labs)


@output_data
def output_data() -> dict:
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
    get_data()
    transform_data()
    output_data()


if __name__ == '__main__':
    run_flow()
