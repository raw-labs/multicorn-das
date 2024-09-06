# DAS Foreign Data Wrapper (FDW)

This repository provides a Foreign Data Wrapper (FDW) implementation for the Data Access Service (DAS) protocol using gRPC. The wrapper is implemented in Python and built on top of the [Multicorn](https://github.com/Kozea/Multicorn) extension for PostgreSQL. It enables PostgreSQL to interact with DAS-compliant systems, allowing the execution of SQL queries and other data operations through the DAS protocol.

## Features

- Uses `IMPORT FOREIGN SCHEMA` to dynamically retrieve tables and schema information from a DAS-compliant service.
- Supports querying remote data sources through the DAS protocol over gRPC.
- Dynamic table definition retrieval based on the data source type (e.g., Salesforce).
- Automatic DAS registration when `das_id` is not provided.

## Requirements

- Python 3.8+
- PostgreSQL 13+
- Multicorn extension for PostgreSQL
- gRPC libraries for Python

## Installation

1. Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

2.	Install and configure the [Multicorn2 extension](https://github.com/raw-labs/multicorn2) in your PostgreSQL instance. You must use Multicorn2 fork by RAW Labs as it contains several extensions/fixes that are required.
3.	Add this FDW as an extension to PostgreSQL.

## Configuration

To configure this FDW to work with your PostgreSQL instance, you will use `IMPORT FOREIGN SCHEMA` to dynamically retrieve table definitions from the DAS server. This involves:

	•	`wrapper`: The Python module implementing the FDW, typically 'multicorn_das.DASFdw'.
	•	`das_url`: The gRPC endpoint of the DAS server.
	•	`das_type`: The type of the data source (e.g., Salesforce, SQL, etc.).
	•	Additional options (such as credentials) that are passed directly to the DAS server to connect to the remote data source.

Note: The `das_table_name` and `das_startup_cost` are internal options that are handled automatically when the schema is imported and do not need to be manually specified.

### Example: Salesforce FDW Configuration

Below is an example of how to configure the FDW for connecting to a Salesforce instance via DAS:

```sql
-- Drop the server and schema if they exist
DROP SERVER IF EXISTS multicorn_das CASCADE;
DROP SCHEMA IF EXISTS test CASCADE;

-- Create the server with the necessary connection details
CREATE SERVER multicorn_das FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url '<das gRPC endpoint>',
  das_type 'salesforce',
  username '<salesforce_username>',
  password '<salesforce_password>',
  client_id '<salesforce_client_id>',
  client_secret '<salesforce_client_secret>',
  security_token '<salesforce_security_token>',
  api_version '<salesforce_api_version>',
  url '<salesforce_url>',
  dynamic_objects '<salesforce_dynamic_object_id>'
);

-- Create a new schema for the imported tables
CREATE SCHEMA test;

-- Import tables from the DAS source
IMPORT FOREIGN SCHEMA foo FROM SERVER multicorn_das INTO test;
```

### Key Options

- `das_url`: The gRPC endpoint where the DAS server is hosted.
- `das_type`: The type of data source (e.g., salesforce, sql).
- Additional key-value options: Credentials and other parameters specific to the data source type, passed to the DAS server. For example, Salesforce requires username, password, client ID, client secret, and other API details.
- `das_id`: Optional. If not provided, the DAS server will handle registration and assign a `das_id` during the schema import.

## Usage

Once the foreign schema is imported, you can query the foreign tables just like any regular PostgreSQL table. The queries will be executed via the DAS protocol.

```sql
SELECT * FROM test.my_salesforce_table LIMIT 1;
```

## Schema Import Process

The `IMPORT FOREIGN SCHEMA` command triggers communication with the DAS server to dynamically retrieve table definitions from the specified data source. During this process:

	1.	The FDW passes the connection details (including credentials) to the DAS server.
	2.	If `das_id` is not specified, the DAS server will register a new DAS and assign a `das_id` for future operations.
	3.	The DAS server returns the table definitions, which are then used to create foreign tables in PostgreSQL.

## Logging

The FDW provides logging via the PostgreSQL log system. Logs are written for all key operations, including connecting to the DAS service, importing schemas, executing queries, and handling errors.

## Limitations

-	Bulk insert functionality is not currently implemented.
-	The specific options required in the CREATE SERVER command depend on the type of data source you are connecting to (e.g., Salesforce, SQL, etc.).

## Contributing

Contributions are welcome! Please open an issue or submit a pull request to contribute.
