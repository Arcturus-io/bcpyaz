# bcpyaz

## What is it?

This package is a wrapper for Microsoft's SQL Server bcp utility. Current database drivers available in Python are not fast enough for transferring millions of records (yes, I have tried [pyodbc fast_execute_many](https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#fast_executemany)). Despite the IO hits, the fastest option by far is saving the data to a CSV file in file system (preferably /dev/shm tmpfs) and using the bcp utility to transfer the CSV file to SQL Server.

### Azure Synapse / Blob extensions

If the following env vars are set (with examples):
```
  AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=myblogstorageacct;AccountKey=jhadsfas235876dha90/17891hfh25125/12h2udfjs347512==;EndpointSuffix=core.windows.net"
  AZURE_TEMP_STORAGE_CONTAINER="tempcontainer"
```

Then instead of using the bcp command line utility, the CSV file will be copied to the Azure Storage Blob temporarily and 
[COPY](https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql) will be used to 
transfer it from there into the Synapse Database.

You may need to 
```
GRANT ADMINISTER DATABASE BULK OPERATIONS TO <user>
```
on Synapse to the user that is connecting.

The sqlcmd utility will still be required.

## How Can I Install It?

Make sure your computeer has the [requirements](#requirements).

Install locally from a git clone:
```bash
pip install -e .
```

or via requirements.txt:
```
-e git+https://git@github.com/Arcturus-io/bcpyaz.git#egg=bcpyaz
```

## Examples

Following examples show you how to load (1) flat files and (2) DataFrame objects to SQL Server using this package.

### Flat File

Following example assumes that you have a comma separated file with no qualifier in path 'tests/data1.csv'. The code below sends the the file to SQL Server.

```python
import bcpy


sql_config = {
    'server': 'sql_server_hostname',
    'database': 'database_name',
    'username': 'test_user',
    'password': 'test_user_password1234'
}
sql_table_name = 'test_data1'
csv_file_path = 'tests/data1.csv'
flat_file = bcpy.FlatFile(qualifier='', path=csv_file_path)
sql_table = bcpy.SqlTable(sql_config, table=sql_table_name)
flat_file.to_sql(sql_table)
```

### DataFrame

The following example creates a DataFrame with 100 rows and 4 columns populated with random data and then it sends it to SQL Server.

```python
import bcpy
import numpy as np
import pandas as pd


sql_config = {
    'server': 'sql_server_hostname',
    'database': 'database_name',
    'username': 'test_user',
    'password': 'test_user_password1234'
}
table_name = 'test_dataframe'
df = pd.DataFrame(np.random.randint(-100, 100, size=(100, 4)),
                  columns=list('ABCD'))
bdf = bcpy.DataFrame(df)
sql_table = bcpy.SqlTable(sql_config, table=table_name)
bdf.to_sql(sql_table)
```

## Requirements

You need a working version of Microsoft bcp installed in your system. Your PATH environment variable should contain the directory of the bcp utility. Following are the installation tutorials for different operating systems.

- [Dockerfile (Ubuntu 18.04)](./bcp.Dockerfile)
- [Linux](https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools)
- [Mac](https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-2017#macos)
- [Windows](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)
