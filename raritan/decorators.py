import importlib
import os
from threading import Thread
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
        name = kwargs.get('name', get_file_name_from_function(original_function))
        context.set_flow_id(name)
        settings = get_settings()
        context.set_release_spec_name(settings.release_spec)

        # Make sure the original function's docstring is available through help.
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            logger = get_logger()
            logger.info('Beginning flow: [bold]%s[/bold]', name, extra={"markup": True})
            logger.info('Release spec: [bold]%s[/bold]', context.release_spec_name, extra={"markup": True})
            try:
                output = original_function(*args, **kwargs)
                logger.success('Completed flow run! :dog:', extra={"markup": True})
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
                console.print_exception(show_locals=True)
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
    is_parallel = kwargs.get('parallel', False)

    def _input(original_function):
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            settings = get_settings()
            data_sources = []
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
                    data_sources.append({
                        'name': name,
                        'full_path': full_path,
                        'extension': extension,
                    })
            if is_parallel:
                processes = []
                for asset in data_sources:
                    this_process = Thread(target=_load_asset_set_context, args=(settings, context, asset))
                    this_process.start()
                    processes.append(this_process)
                for process in processes:
                    process.join()
            else:
                for asset in data_sources:
                    _load_asset_set_context(settings, context, asset)
        return wrapper_function

    # If no arguments are passed to the decorator, return the wrapper one level down.
    if len(args) > 0 and callable(args[0]):
        return _input(args[0])
    return _input


def _load_asset_set_context(settings, active_context, asset):
    data = settings.input_handler(asset['full_path'], asset['extension'])
    active_context.set_data_reference(asset['name'], data)


def output_data(*args, **kwargs):
    is_parallel = kwargs.get('parallel', False)

    def _output(original_function):
        @wraps(original_function)
        def wrapper_function(*args, **kwargs):
            settings = get_settings()
            output_map = original_function(*args, **kwargs)
            # todo validate this.
            flattened_assets = []
            for path, assets in output_map.items():
                for file_name, asset in assets.items():
                    asset['full_path'] = f'{path}/{file_name}'
                    asset['data'] = context.get_data_reference(asset['data'] if 'data' in asset.keys() else file_name)
                    flattened_assets.append(asset)
            if is_parallel:
                processes = []
                for asset in flattened_assets:
                    for extension in asset['formats']:
                        this_process = Thread(target=_output_data, args=(settings, asset, extension))
                        this_process.start()
                        processes.append(this_process)
                for process in processes:
                    process.join()
            else:
                for asset in flattened_assets:
                    for extension in asset['formats']:
                        _output_data(settings, asset, extension)

        return wrapper_function
    if len(args) > 0 and callable(args[0]):
        return _output(args[0])
    return _output


def _output_data(settings, asset, extension):
    settings.output_handler(asset['full_path'], extension, asset['data'], **asset['output_kwargs'])


def get_file_name_from_function(function: callable) -> str:
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


def get_settings() -> types.ModuleType:
    try:
        settings = importlib.import_module('settings')
    except ModuleNotFoundError:
        message = 'Missing required module settings (settings.py). Check out default_settings.py for an example.'
        raise ModuleNotFoundError(message)
    return settings

