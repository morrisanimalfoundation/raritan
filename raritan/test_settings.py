import hashlib

"""
A settings file for running the test suite.
"""

data_dir = './raritan/testing/fixture'


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


def output_handler(file: str, extension: str, data, **kwargs) -> None:
    """
    An output handler for our testing purposes

    Parameters
    ----------
    file: str
      The full path to the resource.
    extension: str
      The extension for pivoting handling.
    data: string
      The data to handle, a string in this case.
    kwargs: dict
      Any kwargs along for the ride.
    """
    assert kwargs.get('fee')
    assert not kwargs.get('fi')
    with open(f'{file}', 'w') as file:
        file.write(data)


def analyze_asset_handler(file, extension, data, duration, operation):
    """
    An analysis handler for our testing purposes.

    Parameters
    ----------
    file: str
      The full path to the resource.
    extension: str
      The extension for pivoting handling.
    data: string
      The data to handle, a string in this case.
    duration: string
      The duration the job ran.
    operation

    Returns
    -------
    output: str
      A string that adds context to the asset.
    """
    checksum = hashlib.sha1(data.encode('utf-8')).hexdigest()
    checksum = checksum[0:10]
    if operation == 'input':
        return f'Loaded asset {file} {duration} {checksum}'
    if operation == 'output':
        return f'Finished output: {file} {duration} {checksum}'
