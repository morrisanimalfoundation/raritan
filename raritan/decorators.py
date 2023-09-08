import importlib
import os
import random
import sys
import types
from functools import wraps

from rich.console import Console

from raritan.context import context
from raritan.logger import get_logger

"""
Provides decorators for our ETL processes.
"""

# Create a rich console object for us to format stack traces.
console = Console()


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
    name: str
        A flow name override.

    Returns
    -------
    A callable which is the original function with decoration.
    """

    def _flow(original_function):
        name = kwargs.get('name', _get_file_name_from_function(original_function))
        context.set_flow_id(name)
        settings = _get_settings()
        context.set_release_spec_name(settings.release_spec)

        # Make sure the original function's docstring is available through help.
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            logger = get_logger()
            logger.info('Beginning flow: [bold]%s[/bold]', name, extra={"markup": True})
            logger.info('Release spec: [bold]%s[/bold]', context.release_spec_name, extra={"markup": True})
            try:
                output = original_function(*args, **kwargs)
                emoji = random.choice(('cat', 'dog', 'horse', 'gorilla'))
                logger.success(f'Completed flow run! :{emoji}:', extra={"markup": True})
                return output
            except Exception:
                console.print_exception(show_locals=True)
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
            logger = get_logger()
            logger.info('Beginning task: %s', task_description)
            context.set_current_task(original_function.__name__)
            try:
                output = original_function(*args, **kwargs)
                # Same as above.
                # Only log if we are in a flow context.
                logger.success('Completed task: %s', original_function.__name__)
                return output
            except Exception:
                console.print_exception(show_locals=True, max_frames=1)
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
            settings = _get_settings()
            logger = get_logger()
            sources = original_function(*args, **kwargs)
            # todo validate structure here.
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
                    data = settings.input_handler(full_path, extension)
                    context.set_data_reference(name, data)
                    if analyze and hasattr(settings, 'analyze_asset_handler'):
                        settings.analyze_asset_handler(full_path, extension, data)
                    else:
                        logger.success(f'Finished asset: {full_path}')
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
            settings = _get_settings()
            logger = get_logger()
            output_map = original_function(*args, **kwargs)
            # todo validate this.
            for path, assets in output_map.items():
                for file_name, asset in assets.items():
                    data = context.get_data_reference(asset['data'] if 'data' in asset.keys() else file_name)
                    for extension in asset['formats']:
                        full_path = f'{path}/{file_name}.{extension}'
                        logger.info(f'Beginning output: {full_path}')
                        settings.output_handler(full_path, extension, data, **asset['output_kwargs'])
                        if analyze and hasattr(settings, 'analyze_asset_handler'):
                            settings.analyze_asset_handler(full_path, extension, data)
                        else:
                            logger.info(f'Finished output: {full_path}')
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


def _get_settings() -> types.ModuleType:
    """
    Get the settings module and provide a helpful message, if not found.

    Returns
    -------
    The active settings module.
    """
    try:
        settings = importlib.import_module('settings')
    except ModuleNotFoundError:
        message = 'Missing required module settings (settings.py). Check out default_settings.py for an example.'
        raise ModuleNotFoundError(message)
    return settings
