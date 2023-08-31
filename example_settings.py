"""
Sample settings file for describing the location of resources external to the repository.
"""

project_path = '.'
data_dir = project_path + '/data'
testing_dir = project_path + '/testing'
input_dir = data_dir + '/input'
output_dir = data_dir + '/output'
metadata_dir = data_dir + '/metadata'
schema_dir = data_dir + '/schema'
dictionary_dir = data_dir + '/dictionaries'
ubc_dir = input_dir + '/ubc_releases/2020-07'
historical_dir = input_dir + '/historical_data_commons'
testing_input_dir = testing_dir + '/data/input'
testing_output_dir = testing_dir + '/data/output'
data_check_release = testing_dir + '/datacheck_release'
data_check_feature = testing_dir + '/datacheck_feature'

# The currently active release spec.
release_spec = 'example'

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
# Used in to obfuscate columns like tubes, public barcode and diet food brand, public brand.
hash_salt = ''
# The hashing algorithm to use on the column.
hashing_algorithm = 'sha256'
