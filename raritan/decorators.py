import os
import random
import sys
from datetime import datetime
from functools import wraps

from raritan import logger
from raritan.context import context
from raritan.logger import error

"""
Provides decorators for our ETL processes.
"""


def flow(*args, **kwargs):
    """
    Logs flow run messages and sets the context.flow_name.

    Notes
    -----
    This method uses *args and **kwargs, so the decorator may be called and receive kwargs of its own.
    Examples:

    Decorator not called.

    ```
    @flow
    some_flow():
      pass
    ```

    Decorator called.

    ```
    @flow(flow_id='bob')
    some_flow():
      pass
    ```

    Keyword Arguments
    -----------------
    flow_id: str
        A flow name override.

    Returns
    -------
    A callable which is the original function with decoration.
    """

    def _flow(original_function):
        name = kwargs.get('flow_id', _get_file_name_from_function(original_function))
        context.set_flow_id(name)
        settings = context.get_settings()
        if hasattr(settings, 'release_spec'):
            context.set_release_spec_name(settings.release_spec)

        # Make sure the original function's docstring is available through help.
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            start = datetime.now()
            logger.info(f'Beginning flow: [bold]{name}[/bold]')
            if context.release_spec_name:
                logger.info(f'Release spec: [bold]{context.release_spec_name}[/bold]')
            start_string = start.strftime('%Y-%m-%d %H:%m:%S')
            logger.info(f'Started: {start_string}')
            try:
                duration = _time_function(original_function, *args, **kwargs)[0]
                emoji = random.choice(('cat', 'dog', 'horse', 'gorilla'))
                logger.success(f'Completed flow run! :{emoji}:')
                logger.info(f'Total duration {duration}')
            except Exception as e:
                error(f"Error occurred: {e}")  # Log the error message
                # We want all the flows to run even if one fails.
                # After the build is complete we scan the output for `Traceback` and if the key word is found,
                # it will throw a fail on Jenkins.
                quit()

        return wrapper_function

    # If no arguments are passed to the decorator, return the wrapper one level down.
    if len(args) > 0 and callable(args[0]):
        return _flow(args[0])
    return _flow


def task(*args, **kwargs):
    """
    Logs task run messages and sets the context.current_task.

    Notes
    -----
    This method uses *args and **kwargs, so the decorator may be called and receive kwargs of its own.
    See core.decorators.flow for examples.

    Task implements an optional kwarg task_description. This kwarg overrides the default task name.
    It is meant for use cases where a task is being called dynamically.

    Keyword Arguments
    -----------------
    task_description: str
       Optionally override the name of the task output to the terminal.

    Returns
    -------
    A callable which is the original function with decoration.
    """

    def _task(original_function):
        # Make sure the original function's docstring is available through help.
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            task_description = kwargs.get('task_description', original_function.__name__)
            if kwargs.get('task_description'):
                del kwargs['task_description']
            logger.info(f'Beginning task: {task_description}')
            context.set_current_task(original_function.__name__)
            try:
                duration, output = _time_function(original_function, *args, **kwargs)
                logger.success(f'Completed task: {task_description} {duration}')
                return output
            except Exception as e:
                error(f"Error occurred: {e}")  # Log the error message
                # We want all the flows to run even if one fails.
                # After the build is complete we scan the output for `Traceback` and if the key word is found,
                # it will throw a fail on Jenkins.
                quit()

        return wrapper_function

    # If no arguments are passed to the decorator, return the wrapper one level down.
    if len(args) > 0 and callable(args[0]):
        return _task(args[0])
    return _task


def input_data(*args, **kwargs):
    """
    Loads a dictionary of assets into the context object for the run.

    Keyword Arguments
    -----------------
    analyze: str
       Optionally provide the host repo the option to analyze assets.

    Returns
    -------
    A callable which is the original function with decoration.
    """
    analyze = kwargs.get('analyze', True)
    optional_flag = kwargs.get('optional', False)
    filters = kwargs.get('filters', None)
    default_dictionary = kwargs.get('default_dictionary', dict())

    def _input(original_function):
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            nonlocal filters  # Declare filters as nonlocal to modify the outer variable
            nonlocal optional_flag  # Declare optional_flag as nonlocal to modify the outer variable
            nonlocal analyze  # Declare optional_flag as nonlocal to modify the outer variable
            nonlocal default_dictionary  # Declare optional_flag as nonlocal to modify the outer variable
            settings = context.get_settings()
            # Get the dictionary describing our input data.
            sources = original_function(*args, **kwargs)
            # Assets are listed in two tiers.
            for group, assets in sources.items():
                for key, name in assets.items():
                    if isinstance(name, dict):
                        # If assets is a dictionary, iterate over its items
                        inner_optional_flag = name.get('optional', False)
                        # Grab the filters
                        filters = name.get('filters', None)
                        # Grab the dictionary schema
                        default_dictionary = name.get('default_dictionary', None)
                        name = name.get('file')
                    else:
                        inner_optional_flag = optional_flag  # Use the default optional_flag
                    logger.info(f'Handling asset: {name}')
                    # Check the optional flag
                    if not os.path.exists(group + '/' + name):
                        # It is not optional
                        if not inner_optional_flag:
                            raise FileNotFoundError(f"Non-Optional file missing: {name}")
                        # It is optional, using a dictionary provided to make an empty dataframe with column names.
                        else:
                            print("it went in here")
                            logger.info(f"Optional file missing: {name}, using default dictionary.")
                            print("what is default dictionary ", default_dictionary)
                            if not default_dictionary:
                                raise Exception('No default dictionary provided.')
                            context.set_data_reference(key, default_dictionary)
                            message = f'Loaded default dictionary for {name}'
                            logger.success(message)
                    else:
                        duration, data = _time_function(settings.input_handler, *[group, name])
                        if filters is not None:
                            try:
                                for filter_function, value in filters.items():
                                    data = filter_function(data, value)
                            except Exception as e:
                                error(f"Something went wrong with the filter function: {e}")  # Log the error message
                                # We want all the flows to run even if one fails.
                                quit()
                        context.set_data_reference(key, data)
                        message = ''
                        # Allow an analyze_asset_handler to ensure integrity and/or write the logging.
                        if analyze and hasattr(settings, 'analyze_asset_handler'):
                            message = settings.analyze_asset_handler(group, name, None, data, duration, 'input')
                        if message is None or len(message) == 0:
                            message = f'Loaded asset: {name} {duration}'
                        logger.success(message)
        return wrapper_function

    # If no arguments are passed to the decorator, return the wrapper one level down.
    if len(args) > 0 and callable(args[0]):
        return _input(args[0])
    return _input


def output_data(*args, **kwargs):
    """
    Output a dictionary of assets from the context object in a variety of formats.

    Keyword Arguments
    -----------------
    analyze: str
       Optionally provide the host repo the option to analyze assets.

    Returns
    -------
    A callable which is the original function with decoration.
    """
    analyze = kwargs.get('analyze', True)

    def _output(original_function):
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            settings = context.get_settings()
            # Get the dict, describing the assets to output.
            output_map = original_function(*args, **kwargs)
            # They should be grouped by two tiers.
            # We are file-centric, but this could work for databases and tables as well.
            for group, assets in output_map.items():
                for key, asset in assets.items():
                    reference_name = asset['data'] if 'data' in asset.keys() else key
                    data = context.get_data_reference(reference_name)
                    # Iterate over the extensions and allow each one to be processed by the output handler.
                    for asset_format in asset['formats']:
                        logger.info(f'Beginning output: {key} in format {asset_format}')
                        duration = _time_function(settings.output_handler, *[group, key, asset_format, data],
                                                  **asset['output_kwargs'])[0]
                        # Allow an analyze_asset_handler to ensure integrity and/or write the logging.
                        message = ''
                        if analyze and hasattr(settings, 'analyze_asset_handler'):
                            message = settings.analyze_asset_handler(group, key, asset_format, data, duration, 'output')
                        if message is None or len(message) == 0:
                            message = f'Finished output: {key} in format {asset_format} {duration}'
                        logger.success(message)

        return wrapper_function

    if len(args) > 0 and callable(args[0]):
        return _output(args[0])
    return _output


def _get_file_name_from_function(function: callable) -> str:
    """
    Gets the parent module's file name, minus extension for a given function.

    Parameters
    ----------
    function: callable
        The function to get the module filename from.

    Returns
    -------
    The name of the function's parent module, minus file extension
    """
    flow_file = sys.modules[function.__module__].__file__
    flow_file = os.path.basename(flow_file)
    flow_file = os.path.splitext(flow_file)
    return flow_file[0]


def _time_function(func: callable, *args, **kwargs) -> tuple:
    """
    Times the execution of a function.

    Parameters
    ----------
    func: callable
      A function to time.
    args: list
      Args to pass to the function.
    kwargs: dict
      Kwargs to pass to the function.

    Returns
    -------
    A tuple the first element of which is a formatted string of duration.
      The second element is the return value of the provided function.
    """
    start = datetime.now()
    output = func(*args, **kwargs)
    end = datetime.now()
    return _get_formatted_duration(start, end), output


def _get_formatted_duration(start: datetime, end: datetime) -> str:
    """
    Formats the difference between two datetimes in hours, minutes and seconds.

    Parameters
    ----------
    start: datetime
      The start datetime.
    end: datetime
      The end datetime.

    Returns
    -------
    duration: str
      The formatted difference between the datetimes.
    """
    output = ''
    seconds = (end - start).total_seconds()
    hours = int(seconds // 3600)
    minutes = int((seconds - (hours * 3600)) // 60)
    seconds = int(round(seconds - ((hours * 3600) + (minutes * 60))))
    if hours > 0:
        output += f'{hours}h'
    if minutes > 0:
        output += f'{minutes}m'
    if seconds > 0:
        output += f'{seconds}s'
    if hours + minutes + seconds == 0:
        output = '<1s'
    return output
