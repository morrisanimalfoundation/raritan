import hashlib
import os
from raritan.logger import error
import pandas as pd
import pandas.api.types
import sqlalchemy

import settings
from raritan.context import context
from utility.release_spec import get_sql_schema_file_for_spec

"""
Handles various input and output operations.
"""


def input_handler(path: str, file: str, **kwargs) -> pd.DataFrame:
    """
    An input handler for the raritan @input_data decorator.

    Parameters
    ----------
    path: str
      The path to the file.
    file: str
      The file name.

    kwargs: dict
        optional: Boolean
        Whether to raise an error if it cannot find the file.

    Returns
    -------
    data: pd.DataFrame
      The dataframe.
    """
    # If file does not exist
    if ~os.path.isfile(path + '/' + file):
        # It is not optional
        if not kwargs['optional_flag']:
            error(f"Missing the file: {file}")  # Log the error message
        else:
            return pd.DataFrame()
    sep = ',' if 'csv' in file else '\t'
    # Here it needs to handle the optional flag
    return pd.read_csv(f'{path}/{file}', sep=sep, low_memory=False)


def output_handler(path: str, file: str, extension: str, data: pd.DataFrame | dict, **kwargs):
    """
    An output handler for the raritan @output_data decorator.

    Parameters
    ----------
    path: str
      The path to the resource.
    file: str
      The name of the item.
    extension: str
      The extension for pivoting handling.
    data: string|dict
      The data to handle, a string in this case.
    kwargs: dict
      Any kwargs along for the ride.
    """
    if extension == 'csv':
        if type(data) is dict:
            csv_path = path
        else:
            csv_path = f'{path}/{file}.{extension}'
        output_csv(csv_path, data)
    if extension in ('sql', 'sql.gz') and settings.output_sql_dump:
        do_gzip = True if '.gz' in extension else False
        schema = get_sql_schema_file_for_spec(context.flow_id)
        output_sql(f'{path}/{file}.{extension}', data, schema, do_gzip)


def output_csv(file: str, data: pd.DataFrame | dict) -> None:
    """
    Outputs a csv file or files.

    Parameters
    ----------
    file: str
      The path to output data.
    data: pd.DataFrame | dict
      A dataset or list of datasets.
    """
    file = file.replace('.csv', '')
    if type(data) is pd.DataFrame:
        data.to_csv(f'{file}.csv', index=False)
    if type(data) is dict:
        for name, item in data.items():
            item.to_csv(f'{file}/{name}.csv', index=False)


def output_sql(file: str, data: pd.DataFrame | dict, schema_file: str, gzip: bool) -> None:
    """
    Outputs a mysql file.

    Parameters
    ----------
    file: str
      The path to output data.
    data: pd.DataFrame | dict
      A dataset or list of datasets.
    gzip: bool
      Whether to gzip the output.
    """
    file_name = os.path.basename(file)
    name = file_name.replace('.sql.gz', '')
    name = name.replace('.sql', '')

    # Try starting the mysql service.
    command = 'sudo service mariadb start'
    os.system(command)

    # Create the database.
    command = f'mysql -u {settings.sql_user} -p{settings.sql_password} -e "DROP DATABASE IF EXISTS {settings.sql_database_name}; CREATE DATABASE {settings.sql_database_name};"'
    os.system(command)
    # Apply the schema.
    command = f'mysql -u {settings.sql_user} -p{settings.sql_password} {settings.sql_database_name} < {schema_file}'
    os.system(command)

    # Make the SQL connection.
    engine = get_sqlalchemy_engine()
    # If we just have a single dataframe, make it iterable.
    if type(data) is pd.DataFrame:
        data = {name: data}
    for dataset_name, dataset in data.items():
        # Loop over subjects and insert.
        # Attempt to introspect the correct "subject_id" data type column for each dataset.
        # Every subject should not require multiple schemas per release tier, just because of "subject_id".
        if 'subject_id' in dataset.columns:
            # There are only two options, numeric and non-numeric.
            # Ignore what's in the schema file and change it based on dtype here.
            # The "subject_id" column should always be NOT NULL.
            if pandas.api.types.is_numeric_dtype(dataset['subject_id']):
                alter_query = f'ALTER TABLE {dataset_name} MODIFY subject_id INT(11) NOT NULL;'
            else:
                alter_query = f'ALTER TABLE {dataset_name} MODIFY subject_id VARCHAR(25) NOT NULL;'
            alter_query = sqlalchemy.text(alter_query)
            command = f'mysql -u {settings.sql_user} -p{settings.sql_password} {settings.sql_database_name} -e "{alter_query}"'
            os.system(command)
            # Insert the dataframe's contents into the table.
            dataset.to_sql(dataset_name, engine, if_exists='append', index=False, chunksize=1000)
    gzip_command = ''
    extension = 'sql'
    if gzip:
        gzip_command = '| gzip'
        extension += '.gz'
    if extension not in file:
        file = f'{file}.{extension}'
    # Create the sql dump.
    command = f'mysqldump --skip-dump-date -u {settings.sql_user} -p{settings.sql_password} {settings.sql_database_name} {gzip_command} > {file}'
    os.system(command)


def get_sqlalchemy_engine() -> sqlalchemy.engine.base.Engine:
    """
    Gets a configured sqlalchemy engine.

    Returns
    -------
    engine: sqlalchemy.engine.base.Engine
        A sqlalchemy engine, ready for use.
    """
    sql_url = f'mysql+pymysql://{settings.sql_user}:{settings.sql_password}@localhost:3306/{settings.sql_database_name}'
    return sqlalchemy.create_engine(sql_url)


def output_sql_create_statements_from_dataframes(output_dir: str, data: dict) -> None:
    """
    Outputs a crude SQLLite schema, which may be useful for creating the real MySQL schema.

    Parameters
    ----------
    output_dir : str
        The directory to place the output in.
    data : dict
         A subject key, dataframe value dict.
    """
    engine = get_sqlalchemy_engine()
    schema = []
    for subject, output_df in data.items():
        schema.append(pd.io.sql.get_schema(output_df, subject, con=engine))
    schema = '\n'.join(schema)
    file_name = 'schema_{flow_name}.sql'.format(flow_name=context.flow_id)
    f = open(output_dir + '/' + file_name, 'w')
    f.write(schema)
    f.close()


def analyze_asset_handler(path: str, file: str, extension: str, data: str | dict, duration: str, operation: str) -> str:
    """
    An analysis handler for the @input_data and @output_data decorators.

    Parameters
    ----------
    path: str
      The path to the resource
    file: str
      The filename for the resource.
    extension: str
      The extension for pivoting handling.
    data: pd.DataFrame | dict
      The data to handle.
    duration: str
      The duration the job ran.
    operation

    Returns
    -------
    output: str
      A string that adds context to the asset.
    """
    if operation == 'input':
        checksum = get_checksum(f'{path}/{file}', 8096)
        checksum = checksum[0:10]
        return f'Loaded asset {file} {duration} {checksum} {data.shape}'
    if operation == 'output':
        if 'sql' in extension and not settings.output_sql_dump:
            return 'MySQL processing is disabled. No related assets were created.'
        if type(data) is pd.DataFrame:
            checksum = get_checksum(f'{path}/{file}.{extension}', 8096)
            checksum = checksum[0:10]
            return f'Finished output: {file}.{extension} {duration} {checksum} {data.shape}'
        elif type(data) is dict:
            if 'gz' in extension:
                checksum = get_checksum(f'{path}/{file}.{extension}', 8096)
                checksum = checksum[0:10]
                message = f'Finished output: {file}.{extension} {duration} {checksum}'
                for name, item in data.items():
                    message += f'\n\t {name} {item.shape}'
            else:
                message = f'Finished output: {file}.{extension} {duration}'
                for name, item in data.items():
                    checksum = get_checksum(f'{path}/{name}.{extension}', 8096)
                    checksum = checksum[0:10]
                    message += f'\n\t {name} {checksum} {item.shape}'
            return message


def get_checksum(path: str, size: int) -> str:
    """
    Creates a sha1 checksum of a file.

    Parameters
    ----------
    path: str
        The path to the file.
    size: int
        The number of bytes to hold in the buffer.

    Returns
    -------
    checksum: str
        The checksum.
    """
    file_hash = hashlib.sha1()
    with open(path, 'rb') as f:
        file_bytes = f.read(size)
        while len(file_bytes) > 0:
            file_hash.update(file_bytes)
            file_bytes = f.read(size)
    return file_hash.hexdigest()
