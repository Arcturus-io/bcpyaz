import subprocess
import os
from io import StringIO
import hashlib

import pandas as pd
from datetime import datetime, timedelta

from azure.storage.blob import BlobClient, generate_container_sas, BlobSasPermissions

def sha512(text, encoding='utf-8'):
    """Converts an input string to its sha512 hash
    """
    if not isinstance(text, bytes):
        if isinstance(text, str):
            text = text.encode(encoding)
        else:
            raise ValueError('Invalid input. Cannot compute hash.')
    return hashlib.sha512(text).hexdigest()


def bcp(sql_table, flat_file, batch_size):
    """Runs the bcp command to transfer the input flat file to the input
    SQL Server table.
    :param sql_table: The destination Sql Server table
    :type sql_table: SqlTable
    :param flat_file: Source flat file
    :type flat_file: FlatFile
    :param batch_size: Batch size (chunk size) to send to SQL Server
    :type batch_size: int
    """
    if sql_table.with_krb_auth:
        auth = ['-T']
    else:
        auth = ['-U', sql_table.username, '-P', sql_table.password]
    full_table_string = \
        f'{sql_table.schema}.{sql_table.table}'
    try:
        bcp_command = ['bcp', full_table_string, 'IN', flat_file.path, '-f',
                       flat_file.get_format_file_path(), '-S',
                       sql_table.server, '-d', sql_table.database, '-b', str(batch_size)] + auth
    except Exception as e:
        args_clean = list()
        for arg in e.args:
            if isinstance(arg, str):
                arg = arg.replace(sql_table.password,
                                  sha512(sql_table.password))
            args_clean.append(arg)
        e.args = tuple(args_clean)
        raise e
    if flat_file.file_has_header_line:
        bcp_command += ['-F', '2', '-q', '-k']
    result = subprocess.run(bcp_command, stderr=subprocess.PIPE)
    if result.returncode:
        raise Exception(
            f'Bcp command failed. Details:\n{result}')

def parse_blob_connection_str(conn_str):
    """
    :param conn_str: A Blob Storage connection str
    :type conn_str: str
    Returns a dict of the components making up the string
    """
    conn_str = conn_str.rstrip(";")
    conn_settings = [s.split("=", 1) for s in conn_str.split(";")]
    if any(len(tup) != 2 for tup in conn_settings):
        raise ValueError("Connection string is either blank or malformed.")
    return dict(conn_settings)

def bcpaz(sql_table, flat_file, azure_storage_connection_string, azure_temp_storage_container):
    """Runs the bcp command to transfer the input flat file to the input
    SQL Server table.
    :param sql_table: The destination Sql Server table
    :type sql_table: SqlTable
    :param flat_file: Source flat file
    :type flat_file: FlatFile
    :param batch_size: Batch size (chunk size) to send to SQL Server
    :type batch_size: int
    :param azure_storage_connection_string: Azure String connection string
    :type azure_storage_connection_string: string
    :param azure_temp_storage_container: Name of a Blob Container to use for temp folder
    :type azure_temp_storage_container: string
    """
    # First upload flat_file.path to Azure blob storage
    conn_string = azure_storage_connection_string
    if conn_string[-1] != '/':
        conn_string += '/'

    container_name = azure_temp_storage_container

    blob_name = os.path.basename(flat_file.path)
    
    blob = BlobClient.from_connection_string(conn_str=conn_string, container_name=container_name, blob_name=blob_name)

    with open(flat_file.path, "rb") as data:
        blob.upload_blob(data)

    # Generate Shared Access Secret for Synapse to access the container

    blob_conn = parse_blob_connection_str(conn_string)

    perms = BlobSasPermissions(read=True, add=True, create=True, write=True, delete=True, delete_previous_version=True, tag=True)

    sas = generate_container_sas(blob_conn['AccountName'], 
        account_key=blob_conn['AccountKey'],
        container_name=container_name,
        permission=perms,
        expiry=datetime.now() + timedelta(hours=24))

    sql = """
    COPY INTO [{schema_name}].[{table_name}]
    FROM 'https://arcturusdevstgacc.blob.core.windows.net/{container_name}/{blob_name}'
    WITH (
        FILE_TYPE = 'CSV',
        CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{sas}'),
        FIELDQUOTE = '"',
        FIELDTERMINATOR=',',
        FIRSTROW=2, -- Skip header
        ROWTERMINATOR='0X0A',
        ENCODING = 'UTF8',
        MAXERRORS = 0,
        ERRORFILE = '/errorsfolder_{blob_name}' --path starting from the storage container
    )
    """.format(
        table_name=sql_table.table,
        schema_name=sql_table.schema,
        blob_name=blob_name,
        container_name=container_name,
        sas=sas
        )

    # Tell Synapse to read the Blob into Table

    sqlcmd(
        server=sql_table.server,
        database=sql_table.database,
        command=sql,
        username=sql_table.username,
        password=sql_table.password)

    # Delete the temp blob

    blob.delete_blob()


def sqlcmd(server, database, command, username=None, password=None):
    """Runs the input command against the database and returns the output if it
     is a table.
    Leave username and password to None if you intend to use
    Kerberos integrated authentication
    :param server: SQL Server
    :type server: str
    :param database: Name of the default database for the script
    :type database: str
    :param command: SQL command to be executed against the server
    :type command: str
    :param username: Username to use for login
    :type username: str
    :param password: Password to use for login
    :type password: str
    :return: Returns a table if the command has an output. Returns None
             if the output does not return anything.
    :rtype: Pandas.DataFrame
    """
    if not username or not password:
        auth = ['-E']
    else:
        auth = ['-U', username, '-P', password]
    command = 'set nocount on;' + command
    sqlcmd_command = ['sqlcmd', '-S', server, '-d', database, '-b'] + auth + \
                     ['-I', '-s,', '-W', '-Q', command]
    result = subprocess.run(sqlcmd_command, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode:
        result_dump = str(result).replace(password, sha512(password))
        raise Exception(f'Sqlcmd command failed. Details:\n{result_dump}')
    output = StringIO(result.stdout.decode('ascii'))
    first_line_output = output.readline().strip()
    if first_line_output == '':
        header = None
    else:
        header = 'infer'
    output.seek(0)
    try:
        result = pd.read_csv(
            filepath_or_buffer=output,
            skiprows=[1],
            header=header)
    except pd.errors.EmptyDataError:
        result = None
    return result
