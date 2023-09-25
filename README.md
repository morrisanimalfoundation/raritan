# Raritan
This repository houses core utilities for Morris Animal Foundation data projects. It's named after the [Raritan Hospital for Animals](https://www.morrisanimalfoundation.org/article/building-history-foundation-celebrates-70-years), a veterinary practice run by Dr. Mark L. Morris Sr. In that facility and community Dr. Morris learned and grew his skills, while making several innovative breakthroughs in companion animal health. It is our goal to embody that spirit of learning, growth and innovation. 

These utilities are meant to be very lightweight and allow the ETL itself to implement most of the heavy lifting. Much of the value provided is in lending ETL flows a narrative structure that is easy to read and understand.

## Context
An object that stores information about the current flow run. It is meant to operate as a singleton that is accessible via `raritan.context.context`. The `@input_data` and `@output_data` decorators write data to and read data from context respectively.

## Decorators
Four decorators are provided to construct flows with.

**flow** - The flow decorator represents a single complete data transformation process. It is the parent decorator in which the others nest. In some cases the flow id (accessed via context) may be used to key files (like database schemas for instance). By default the flow_id is determined via the flow's filename. The `flow_id` kwarg may  be passed to the flow to control the id independently.

**task** - The task decorator represents a step within the flow. A flow may have as many tasks as desired. The signature and return objects of the task are up to the implementor of the flow. For situations where a task is called multiple times with different arguments a `task_description` kwarg is provided. This allows more meaningful information to be shared in the logs.

**input_data** - The input_data decorator loads data or creates pointers to data within the context object. It provides a singular point for listing all data assets. Implementations of input_data should return a dictionary of hierarchically nested data assets. In the case of reading directly from the file system, this would be directories keyed to files. The settings.py implementation is expected to provide a `input_handler` function. The input_data decorator will call this handler for each asset.

Sample Implementation:
```
@input_data
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
```
The above dictionary loads two files from the specified input_directory and one from the dictionary director into the cotext object.

They may then be accessed via `context.get_data_reference` e.g. `context.get_data_reference('labs_ongoing')`

**output_data** - The output_data decorator reads data from context and performs the given output actions. The settings.py implementation is expected to provide a `output_handler` function.  The output_data decorator returns a dictionary of hierarchically nested data assets. The output_data decorator will call this handler for each asset. Each asset may specify kwargs to pass to the output_handler, as well as extensions that represent the types of output created by the flow.

Sample Implementation:
```
@output_data
def output_data() -> dict:
    return {
        settings.output_dir: {
            'clinical_labs': {
                # Optional separate control for dataset vs file name.
                'data': 'complete_labs',
                'formats': ('csv', 'sql'),
                'output_kwargs': {},
            },
        },
    }
```


## Logger
The logger is a simple module that wraps some [rich library](https://github.com/Textualize/rich) console commands for consistency purposes.
