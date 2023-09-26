import hashlib
import json
import os
import shutil

"""
A settings file for running the test suite.
"""

data_dir = './raritan/tests/fixture'


def input_handler(file: str, extension: str) -> str:
    """
    An input handler for our testing purposes.

    Parameters
    ----------
    file: str
      The full path to the resource.
    extension: str
      The extension for pivoting handling.

    Returns
    -------
    A string in this case, though it could be anything.
    """
    assert extension == 'txt'
    with open(f'{file}', 'r') as file:
        return file.read()


def output_handler(file: str, extension: str, data: str | dict, **kwargs) -> None:
    """
    An output handler for our testing purposes

    Parameters
    ----------
    file: str
      The full path to the resource.
    extension: str
      The extension for pivoting handling.
    data: string|dict
      The data to handle, a string in this case.
    kwargs: dict
      Any kwargs along for the ride.
    """
    assert kwargs.get('fee')
    assert not kwargs.get('fi')
    data_type = type(data)
    if data_type is not dict:
        _write_file(file, data)
    elif data_type is dict:
        directory = os.path.splitext(file)[0]
        if not os.path.isdir(directory):
            os.mkdir(directory)
        for name, item in data.items():
            _write_file(f'{directory}/{name}.{extension}', item)
        shutil.make_archive(directory, 'zip', directory)
        shutil.rmtree(directory)
    else:
        raise RuntimeError(f'Data must be string or dict, received {data_type}')


def _write_file(name: str, data: str) -> None:
    """
    Writes a file with the open context.
    Parameters
    ----------
    name: str
      The filename.
    data: str
      The data.
    """
    with open(f'{name}', 'w') as file:
        file.write(data)


def analyze_asset_handler(file: str, extension: str, data: str | dict, duration: str, operation: str) -> str:
    """
    An analysis handler for our testing purposes.

    Parameters
    ----------
    file: str
      The full path to the resource.
    extension: str
      The extension for pivoting handling.
    data: str
      The data to handle, a string in this case.
    duration: str
      The duration the job ran.
    operation

    Returns
    -------
    output: str
      A string that adds context to the asset.
    """
    if type(data) is dict:
        data = json.dumps(data)
        file = file.replace(extension, 'zip')
    checksum = hashlib.sha1(data.encode('utf-8')).hexdigest()
    checksum = checksum[0:10]
    if operation == 'input':
        return f'Loaded asset {file} {duration} {checksum}'
    if operation == 'output':
        return f'Finished output: {file} {duration} {checksum}'
