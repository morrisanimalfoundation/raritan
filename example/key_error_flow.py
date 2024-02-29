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
            'sup_med': 'medications_supplements.csv'
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
        context.get_data_reference('sup_med'),
    ]
    # This will throw error
    data['boop']


@output_data
def output_data() -> dict:
    """
    Outputs the data from context with csv and sql strategies.
    """
    return {
        settings.output_dir: {
            'sup_med': {
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


"""
Current Error Message
sup_med:      subject_id  year_in_study  ...    status                             medication_ingredients
0    094-000026            7.0  ...   Current  Supplement - Joint Supplement (Glucosamine HCl...
1    094-000035            8.0  ...   Current  Supplement - Joint Supplement (Chondroitin Sul...
2    094-000035            8.0  ...   Current  Supplement - Multisystem Supplement (Brain, Ey...
3    094-000035            8.0  ...   Current                     Supplement - Immune Supplement
4    094-000035            8.0  ...  Previous              Supplement - Joint Supplement (Other)
..          ...            ...  ...       ...                                                ...
249  094-032904            8.0  ...   Current  Supplement - Joint Supplement (Chondroitin Sul...
250  094-032924            8.0  ...   Current  Supplement - Joint Supplement (Chondroitin Sul...
251  094-033611            6.0  ...   Current  Supplement - Joint Supplement (Chondroitin Sul...
252  094-034430            6.0  ...   Current                               Supplement - Omega 3
253  094-034430            7.0  ...   Current  Supplement - Joint Supplement (Chondroitin Sul...

[254 rows x 19 columns]
None
where is the none coming from
Error Message: Error occurred: list indices must be integers or slices, not str
File "/workspace/example/index_error_flow.py", line 36, in transform_data
Corrupt Code: data['boop']
"""