import os
import random
import sys
from datetime import datetime
from functools import wraps

from raritan import logger
from raritan.context import context

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
                duration, output = _time_function(original_function, *args, **kwargs)
                emoji = random.choice(('cat', 'dog', 'horse', 'gorilla'))
                logger.success(f'Completed flow run! :{emoji}:')
                logger.info(f'Total duration {duration}')
                return output
            except Exception:
                logger.console.print_exception(show_locals=True)
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
            except Exception:
                logger.console.print_exception(show_locals=True, max_frames=1)
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

    def _input(original_function):
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            settings = context.get_settings()
            # Get the dictionary describing our input data.
            sources = original_function(*args, **kwargs)
            # Assets are listed in two tiers.
            # We currently expect these to be files.
            for path, assets in sources.items():
                for name, file_name in assets.items():
                    full_path = f'{path}/{file_name}'
                    if not os.path.isfile(full_path):
                        message = f'Missing expected input file: {full_path}'
                        raise FileNotFoundError(message)
                    path_bits = os.path.splitext(full_path)
                    extension = path_bits[1] if path_bits[1] else ''
                    extension = extension.replace('.', '')
                    logger.info(f'Handling asset: {full_path}')
                    # Pass them on to the input handler.
                    duration, data = _time_function(settings.input_handler, *[full_path, extension])
                    context.set_data_reference(name, data)
                    # Allow an analyze_asset_handler to ensure integrity and/or write the logging.
                    if analyze and hasattr(settings, 'analyze_asset_handler'):
                        message = settings.analyze_asset_handler(full_path, extension, data, duration, 'input')
                        logger.success(message)
                    else:
                        logger.success(f'Loaded asset: {full_path} {duration}')
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
            for path, assets in output_map.items():
                for file_name, asset in assets.items():
                    reference_name = asset['data'] if 'data' in asset.keys() else file_name
                    data = context.get_data_reference(reference_name)
                    # Iterate over the extensions and allow each one to be processed by the output handler.
                    for extension in asset['formats']:
                        full_path = f'{path}/{file_name}.{extension}'
                        logger.info(f'Beginning output: {file_name} in format {extension}')
                        duration, output = _time_function(settings.output_handler, *[full_path, extension, data],
                                                          **asset['output_kwargs'])
                        # Allow an analyze_asset_handler to ensure integrity and/or write the logging.
                        if analyze and hasattr(settings, 'analyze_asset_handler'):
                            message = settings.analyze_asset_handler(full_path, extension, data, duration, 'output')
                            logger.success(message)
                        else:
                            logger.success(f'Finished output: {full_path} {duration}')
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
    hours = seconds // 3600
    minutes = (seconds - (hours * 3600)) // 60
    seconds = round(seconds - ((hours * 3600) + (minutes * 60)))
    if hours > 0:
        output += f'{hours}h'
    if minutes > 0:
        output += f'{minutes}m'
    if seconds > 0:
        output += f'{seconds}s'
    if hours + minutes + seconds == 0:
        output = '<1s'
    return output
