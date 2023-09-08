"""
Sample settings file for describing the location of resources external to the repository and configuring the ETL.
"""

# A smattering of directories that are meaningful.
project_path = '.'
data_dir = project_path + '/data'
testing_dir = project_path + '/testing'
input_dir = data_dir + '/input'
output_dir = data_dir + '/output'
metadata_dir = data_dir + '/metadata'
schema_dir = data_dir + '/schema'
dictionary_dir = data_dir + '/dictionaries'

# The following are some examples of useful variables for the MAF ETL.
# Raritan requires a release specification.
# This is a pivot point to allow different versions of the same data to be published.
release_spec = 'example'

# The following variables are optional.
# The study year to include data through.
embargo_study_year = 1

# Whether to output CSV files.
output_csvs = True

# Whether to output a sqldump.
output_sql_dump = False
# If output_sql_dump is True, the below are required.
# The name of the database to transact with.
sql_database_name = 'database_name'
# The user for the sql transactions.
sql_user = 'root'
# The password for the sql transactions.
sql_password = '<password>'

# Whether to output a semi-useless schema file.
output_sql_schema = False

# Additional salting string to add to hashed columns.
# Used to obfuscate columns with private or proprietary information.
hash_salt = ''
# The hashing algorithm to use on the column.
hashing_algorithm = 'sha256'

# The following handlers work with the input_data and output_data decorators.
# In most cases it probably makes sense for this to live in a separate module that is imported here.


def input_handler(file: str, extension: str):
    """
    Handles loading the asset for the ETL.

    Parameters
    ----------
    file: str
      The path to the resource or potentially a connection string.
    extension: str
      The extension of resource.

    Returns
    -------
      The loaded resource.
    """
    pass


def output_handler(file: str, extension: str, data, **kwargs):
    """
    Handles outputting the asset for the ETL.

    Parameters
    ----------
    file: str
      The path to the resource or potentially a connection string.
    extension: str
      The extension of resource.
    data
      The data of an unknown type, most typically a dataframe.
    kwargs: dict
      Any kwargs passed along from the output_data function.
    """
    pass


def analyze_asset_handler(file: str, extension: str, data):
    """
    Provides an opportunity for the ETL to perform any analysis on data after it is input or before it is output.

    Parameters
    ----------
    file: str
      The path to the resource or potentially a connection string.
    extension: str
      The extension of resource.
    data
      The data of an unknown type, most typically a dataframe.
    """
    pass
