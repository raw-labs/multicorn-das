"""A foreign data wrapper for DAS."""

import builtins
from multicorn import *
from multicorn.utils import log_to_postgres
from datetime import datetime, date, time
from decimal import Decimal
from logging import DEBUG, INFO, WARNING # Never use ERROR or CRITICAL in log_to_post as it relaunches an exception

from com.rawlabs.protocol.das.v1.common.das_pb2 import DASId, DASDefinition
from com.rawlabs.protocol.das.v1.tables.tables_pb2 import TableId, Row, Column
from com.rawlabs.protocol.das.v1.types.values_pb2 import Value, ValueNull, ValueByte, ValueShort, ValueInt, ValueFloat, ValueDouble, ValueDecimal, ValueBool, ValueString, ValueBinary, ValueString, ValueDate, ValueTime, ValueTimestamp, ValueInterval, ValueRecord, ValueRecordAttr, ValueList
from com.rawlabs.protocol.das.v1.query.quals_pb2 import Qual as DASQual, SimpleQual as DASSimpleQual, IsAnyQual, IsAllQual
from com.rawlabs.protocol.das.v1.query.operators_pb2 import Operator
from com.rawlabs.protocol.das.v1.query.query_pb2 import SortKey as DASSortKey, Query
from com.rawlabs.protocol.das.v1.services.tables_service_pb2 import GetTableDefinitionsRequest, GetTableEstimateRequest, GetTableSortOrdersRequest, GetTablePathKeysRequest, ExplainTableRequest, ExecuteTableRequest, GetTableUniqueColumnRequest, InsertTableRequest, BulkInsertTableRequest, UpdateTableRequest, DeleteTableRequest, GetBulkInsertTableSizeRequest
from com.rawlabs.protocol.das.v1.services.registration_service_pb2 import RegisterRequest
from com.rawlabs.protocol.das.v1.services.health_service_pb2 import HealthCheckRequest

from com.rawlabs.protocol.das.v1.services.tables_service_pb2_grpc import TablesServiceStub
from com.rawlabs.protocol.das.v1.services.registration_service_pb2_grpc import RegistrationServiceStub
from com.rawlabs.protocol.das.v1.services.health_service_pb2_grpc import HealthCheckServiceStub

import json
import base64
import grpc
from time import sleep

class DASFdw(ForeignDataWrapper):

    # This is the default cost of starting up the FDW. The actual value is set in constructor __init__.
    _startup_cost = 20


    def __init__(self, fdw_options, fdw_columns):
        super(DASFdw, self).__init__(fdw_options, fdw_columns)
        log_to_postgres(f'Initializing DASFdw with options: {fdw_options}', DEBUG)
        self.url = fdw_options['das_url']
        log_to_postgres(f'Connecting to {self.url}', DEBUG)
        self.__create_channel()
        self.table_service = TablesServiceStub(self.channel)

        self.das_id = DASId(id=fdw_options['das_id'])
        self.table_id = TableId(name=fdw_options['das_table_name'])

        # This is only used by re__register_das
        self.fdw_options = fdw_options
        
        # Set the startup cost for ForeignDataWrapper plugin to use.
        self._startup_cost = int(fdw_options.get('das_startup_cost', '20'))
        
        log_to_postgres(f'Initialized DASFdw with DAS ID: {self.das_id.id}, Table ID: {self.table_id.name}', DEBUG)

    def __create_channel(self):
        self.channel = grpc.insecure_channel(self.url, options=[
            ('grpc.max_receive_message_length', 4194304),
            ('grpc.max_send_message_length', 10 * 1024 * 1024),  # Optional: Adjust send size if needed
        ])

    # This is used for crash recovery.
    # First off, it will wait for the server to come back alive if it's a server unavailable error.
    # Then, it will re-register the DAS if it was not defined in fdw_options.
    def __crash_recovery(self, e):

        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            # This is how user visible errors are returned. We raise the exception.
            raise e

        # First wait for server to come back alive if it's a server unavailable error
        attempts = 30
        while True:
            if not isinstance(e, grpc.RpcError):
                # Not a grpc error, so propagate it
                raise e
            elif e.code() == grpc.StatusCode.UNAVAILABLE:
                # Wait for server to be available again
                log_to_postgres(f'Server unavailable, retrying...', WARNING)
                sleep(0.5)
                self.__create_channel()

                self.health_service = HealthCheckServiceStub(self.channel)
                try:
                    response = self.health_service.Check(HealthCheckRequest())
                    # If we reach here, the server is available again
                    break
                except Exception as ex:
                    log_to_postgres(f'Error in health_service.Check: {ex}', WARNING)
                    e = ex
                    attempts -= 1
                    if attempts == 0:
                        # Give up after attempts are exhausted
                        raise e
            else:
                # Not a server unavailable error
                break

        # Now that the server is available, there are two options:
        # - either the DAS was defined in fdw_options, in which case there's nothing for us to do;
        # - or the DAS was not defined, in which case we need to re-register the DAS with the same settings as before.
        if 'das_type' not in self.fdw_options:
            # Nothing more to do; we recovered from a server unavailable error
            return

        log_to_postgres(f'Re-registering DAS with type: {self.fdw_options["das_type"]}', DEBUG)

        registration_service = RegistrationServiceStub(self.channel)

        das_definition = DASDefinition(
            type=self.fdw_options['das_type'],
            options=self.fdw_options
        )

        # Pass the same DAS ID as before
        request = RegisterRequest(definition=das_definition, id=self.das_id)

        try:
            response = registration_service.Register(request)
        except Exception as e:
            log_to_postgres(f'Error in registration_service.Register: {e}', WARNING)
            raise e
        
        if response.error:
            raise builtins.ValueError(response.error)
                
        log_to_postgres(f'Re-registered DAS with ID: {self.das_id.id}', DEBUG)


    def get_rel_size(self, quals, columns):
        log_to_postgres(f'Getting rel size for table {self.table_id} with quals: {quals}, columns: {columns}', DEBUG)
        
        grpc_quals = multicorn_quals_to_grpc_quals(quals)
        grpc_columns = columns

        request = GetTableEstimateRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            quals=grpc_quals,
            columns=grpc_columns
        )
        log_to_postgres(f'GetTableEstimateRequest: {request}', DEBUG)

        try:
            response = self.table_service.GetTableEstimate(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetTableEstimate for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.get_rel_size(quals, columns)

        log_to_postgres(f'Got rel size: {response.rows} rows, {response.bytes} bytes for table {self.table_id}', DEBUG)
        return (response.rows, response.bytes)


    def can_sort(self, sortkeys):
        log_to_postgres(f'Checking if can sort for table {self.table_id} with sortkeys: {sortkeys}', DEBUG)

        grpc_sort_keys = multicorn_sortkeys_to_grpc_sortkeys(sortkeys)

        request = GetTableSortOrdersRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            sort_keys=grpc_sort_keys
        )
        log_to_postgres(f'GetTableSortOrdersRequest: {request}', DEBUG)

        try:
            response = self.table_service.GetTableSortOrders(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetTableSortOrders for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.can_sort(sortkeys)
        
        log_to_postgres(f'Can sort: {response} for table {self.table_id}', DEBUG)

        out = []
        for sk in response.sort_keys:
            out.append(SortKey(attname=sk.name, attnum=sk.pos, is_reversed=sk.isReversed, nulls_first=sk.nullsFirst, collate=sk.collate))
        return out


    def get_path_keys(self):
        log_to_postgres(f'Getting path keys for table {self.table_id}', DEBUG)

        request = GetTablePathKeysRequest(
            das_id=self.das_id,
            table_id=self.table_id
        )
        log_to_postgres(f'GetTablePathKeysRequest: {request}', DEBUG)

        try:
            response = self.table_service.GetTablePathKeys(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetTablePathKeys for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.get_path_keys()
        
        log_to_postgres(f'Got path keys: {response.path_keys} for table {self.table_id}', DEBUG)

        out = []
        for pk in response.path_keys:
            out.append((pk.key_columns, pk.expected_rows))
        return out


    def execute(self, quals, columns, sortkeys=None, limit=None, planid=None):
        log_to_postgres(f'Executing for table {self.table_id} with quals: {quals}, columns: {columns}, sortkeys: {sortkeys}, limit: {limit}, planid: {planid}', DEBUG)

        grpc_quals = multicorn_quals_to_grpc_quals(quals)
        grpc_columns = columns
        grpc_sort_keys = multicorn_sortkeys_to_grpc_sortkeys(sortkeys)
        grpc_max_batch_size = 4194304

        query = Query(
            quals=grpc_quals,
            columns=grpc_columns,
            sort_keys=grpc_sort_keys
        )

        # Create an ExecuteRequest message
        request = ExecuteTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            query=query,
            plan_id=str(planid),
            max_batch_size_bytes=grpc_max_batch_size,
            max_cache_age_seconds=3
        )
        log_to_postgres(f'ExecuteTableRequest request: {request}', DEBUG)

        # Make the RPC call
        try:
            rows_stream = self.table_service.ExecuteTable(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.ExecuteTable for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.execute(quals, columns, sortkeys=sortkeys, limit=limit, planid=planid)

        # Iterate over the streamed responses and generate multicorn rows
        return GrpcStreamIterator(self.table_id, rows_stream)

    @property
    def modify_batch_size(self):
        log_to_postgres(f'Getting modify batch size {self.table_id}', DEBUG)

        request = GetBulkInsertTableSizeRequest(
            das_id=self.das_id,
            table_id=self.table_id
        )
        log_to_postgres(f'GetBulkInsertTableSizeRequest: {request}', DEBUG)

        try:
            response = self.table_service.GetBulkInsertTableSize(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetBulkInsertTableSize for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.modify_batch_size

        log_to_postgres(f'Got modify batch size: {response.size} for table {self.table_id}', DEBUG)
        return response.size


    @property
    def rowid_column(self):
        log_to_postgres(f'Getting rowid column for table {self.table_id}', DEBUG)

        request = GetTableUniqueColumnRequest(
            das_id=self.das_id,
            table_id=self.table_id
        )
        log_to_postgres(f'GetTableUniqueColumnRequest: {request}', DEBUG)

        try:
            response = self.table_service.GetTableUniqueColumn(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetTableUniqueColumn for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.rowid_column

        log_to_postgres(f'Got unique column: {response.column} for table {self.table_id}', DEBUG)

        return response.column


    def insert(self, values):
        log_to_postgres(f'Inserting values: {values} into table {self.table_id}', DEBUG)

        # Convert the values to RAW values
        columns = []
        for name, value in values.items():
            columns.append(Column(name=name, data=python_value_to_raw(value)))

        # Create an InsertRequest message
        request = InsertTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row=Row(columns=columns)
        )
        log_to_postgres(f'InsertTableRequest: {request}', DEBUG)

        # Make the RPC call
        try:
            response = self.table_service.InsertTable(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.InsertTable for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.insert(values)

        output_row = {}
        for col in response.row.columns:
            name = col.name
            data = col.data
            output_row[name] = raw_value_to_python(data)

        log_to_postgres(f'Inserted row: {output_row} into table {self.table_id}', DEBUG)

        return output_row


    def bulk_insert(self, all_values):
        log_to_postgres(f'Bulk inserting values: {all_values} into table {self.table_id}', DEBUG)
        rows = []
        for row in all_values:
            columns = []
            for name, value in row.items():
                columns.append(Column(name=name, data=python_value_to_raw(value)))
            rows.append(Row(columns=columns))

        # Create an InsertRequest message
        request = BulkInsertTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            rows=rows
        )
        log_to_postgres(f'BulkInsertTableRequest: {request}', DEBUG)

        # Make the RPC call
        try:
            response = self.table_service.BulkInsertTable(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.BulkInsertTable for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.bulk_insert(all_values)

        output_rows = [{col.name: raw_value_to_python(col.data) for col in row.columns} for row in response.rows]
        log_to_postgres(f'Bulk insert of {len(all_values)} into table {self.table_id} returned row: {output_rows}', DEBUG)

        return output_rows

    
    def update(self, rowid, new_values):
        log_to_postgres(f'Updating rowid: {rowid} with new values: {new_values} in table {self.table_id}', DEBUG)

        # Convert the values to RAW values
        columns = []
        for name, value in new_values.items():
            columns.append(Column(name=name, data=python_value_to_raw(value)))

        # Create an UpdateRequest message
        request = UpdateTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row_id=python_value_to_raw(rowid),
            new_row=Row(columns=columns)
        )
        log_to_postgres(f'UpdateTableRequest: {request}', DEBUG)

        # Make the RPC call
        try:
            response = self.table_service.UpdateTable(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.UpdateTable for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.update(rowid, new_values)
        
        output_row = {}
        for col in response.row.columns:
            name = col.name
            data = col.data
            output_row[name] = raw_value_to_python(data)
        
        log_to_postgres(f'Updated row: {output_row} in table {self.table_id}', DEBUG)

        return output_row
    

    def delete(self, rowid):
        log_to_postgres(f'Deleting rowid: {rowid} from table {self.table_id}', DEBUG)

        # Create a DeleteRequest message
        request = DeleteTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row_id=python_value_to_raw(rowid)
        )
        log_to_postgres(f'DeleteTableRequest: {request}', DEBUG)

        # Make the RPC call
        try:
            self.table_service.DeleteTable(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.DeleteTable for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.delete(rowid)

        log_to_postgres(f'Deleted row: {rowid} from table {self.table_id}', DEBUG)


    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type, restricts):
        # Since it's a classmethod, we need to connect to the server just for this call.
        url = srv_options['das_url']
        log_to_postgres(f'Import schema for {url}', DEBUG)

        with grpc.insecure_channel(url) as channel:

            # If das_id is defined, then we can just call GetDefinitions.
            # Otherwise, we need to create a new DAS and get its ID.
            # In that case, we need to pass the DAS type and all other available options.
            if 'das_id' in srv_options:
                log_to_postgres(f'Using das_id: {srv_options["das_id"]}', DEBUG)
                das_id = DASId(id=srv_options['das_id'])
            else:
                log_to_postgres(f'Creating new DAS', DEBUG)
                if 'das_type' not in srv_options:
                    raise builtins.ValueError('das_type is required when das_id is not provided')
                
                log_to_postgres(f'Creating new DAS with type: {srv_options["das_type"]}', DEBUG)

                registration_service = RegistrationServiceStub(channel)

                das_definition = DASDefinition(
                    type=srv_options['das_type'],
                    options=srv_options | options
                )

                request = RegisterRequest(definition=das_definition)

                try:
                    response = registration_service.Register(request)
                except Exception as e:
                    log_to_postgres(f'Error in registration_service.Register: {e}', WARNING)
                    raise e

                if response.error:
                    raise builtins.ValueError(response.error)

                das_id = response.id
                log_to_postgres(f'Created new DAS with ID: {das_id.id}', DEBUG)

            table_service = TablesServiceStub(channel)

            log_to_postgres(f'Getting definitions for DAS ID: {das_id.id}', DEBUG)
            request = GetTableDefinitionsRequest(das_id=das_id)
            try:
                response = table_service.GetTableDefinitions(request)
            except Exception as e:
                log_to_postgres(f'Error in table_service.GetTableDefinitions: {e}', WARNING)
                raise e
            log_to_postgres(f'Got definitions: {response.definitions}', DEBUG)

            table_definitions = []

            for table in response.definitions:
                log_to_postgres(f'About to do {table.table_id.name}', DEBUG)
                column_definitions = []

                table_name = table.table_id.name
                columns = table.columns
                log_to_postgres(f'Hello a', DEBUG)
                for column in columns:
                    log_to_postgres(f'Hello b {column.name} {column.type}', DEBUG)
                    column_definitions.append(ColumnDefinition(column.name, type_name=raw_type_to_postgresql(column.type)))
                
                log_to_postgres(f'Hello c', DEBUG)
                if 'das_type' in srv_options:
                    log_to_postgres(f'Hello d', DEBUG)
                    table_options = dict(url=url, das_id=das_id.id, das_table_name=table_name, das_startup_cost=str(table.startup_cost), das_type=srv_options['das_type'])
                else:
                    log_to_postgres(f'Hello e', DEBUG)
                    table_options = dict(url=url, das_id=das_id.id, das_table_name=table_name, das_startup_cost=str(table.startup_cost))

                log_to_postgres(f'Hello f', DEBUG)
                table_definitions.append(TableDefinition(table_name, columns=column_definitions, options=table_options))
                log_to_postgres(f'Did table {table_name}', DEBUG)
            
            log_to_postgres(f'Table definitions are {table_definitions}', DEBUG)

            return table_definitions
    

def raw_type_to_postgresql(t):
    type_name = t.WhichOneof('type')
    if type_name == 'any':
        return 'JSONB'
    elif type_name == 'byte':
        return 'SMALLINT' + (' NULL' if t.byte.nullable else '') # Postgres does not have a BYTE type, so SMALLINT is closest
    elif type_name == 'short':
        return 'SMALLINT' + (' NULL' if t.short.nullable else '')
    elif type_name == 'int':
        return 'INTEGER' + (' NULL' if t.int.nullable else '')
    elif type_name == 'long':
        return 'BIGINT' + (' NULL' if t.long.nullable else '')
    elif type_name == 'float':
        return 'REAL' + (' NULL' if t.float.nullable else '')
    elif type_name == 'double':
        return 'DOUBLE PRECISION' + (' NULL' if t.double.nullable else '')
    elif type_name == 'decimal':
        return 'DECIMAL' + (' NULL' if t.decimal.nullable else '')
    elif type_name == 'bool':
        return 'BOOLEAN' + (' NULL' if t.bool.nullable else '')
    elif type_name == 'string':
        return 'TEXT' + (' NULL' if t.string.nullable else '')
    elif type_name == 'binary':
        return 'BYTEA' + (' NULL' if t.binary.nullable else '')
    elif type_name == 'date':
        return 'DATE' + (' NULL' if t.date.nullable else '')
    elif type_name == 'time':
        return 'TIME' + (' NULL' if t.time.nullable else '')
    elif type_name == 'timestamp':
        return 'TIMESTAMP' + (' NULL' if t.timestamp.nullable else '')
    elif type_name == 'interval':
        return 'INTERVAL' + (' NULL' if t.interval.nullable else '')
    elif type_name == 'record':
        # Records were initially advertised as HSTORE, which works only for records in
        # which all values are strings. For backward compatibility, we keep that
        # logic.
        att_types = [f.tipe.WhichOneof('type') for f in t.record.atts]
        if any([att_type != 'string' for att_type in att_types]):
            return 'JSONB'
        else:
            return 'HSTORE'
    elif type_name == 'list':
        inner_type = t.list.inner_type
        # Postgres arrays can always hold NULL values. Their inner type IS nullable.
        inner_type_str = raw_type_to_postgresql(inner_type)
        # When declaring the array type, the syntax doesn't accept that NULLABLE is specified.
        # We remove ' NULL' if found.
        if inner_type_str.endswith(' NULL'):
            inner_type_str = inner_type_str[:-5]
        column_schema = f'{inner_type_str}[]' + (' NULL' if t.list.nullable else '')
        return column_schema
    else:
        raise builtins.ValueError(f"Unsupported Type: {type_name}")


def raw_value_to_python(v):
    value_name = v.WhichOneof('value')
    if value_name == 'null':
        return None
    elif value_name == 'byte':
        return v.byte.v
    elif value_name == 'short':
        return v.short.v
    elif value_name == 'int':
        return v.int.v
    elif value_name == 'long':
        return v.long.v
    elif value_name == 'float':
        return v.float.v
    elif value_name == 'double':
        return v.double.v
    elif value_name == 'decimal':
        return v.decimal.v
    elif value_name == 'bool':
        return v.bool.v
    elif value_name == 'string':
        return v.string.v
    elif value_name == 'binary':
        return v.binary.v
    elif value_name == 'date':
        try:
            return date(v.date.year, v.date.month, v.date.day)
        except builtins.ValueError as exc:
            log_to_postgres(f'Unsupported date: {v.date}: {exc}', WARNING)
            return None
    elif value_name == 'time':
        return time(v.time.hour, v.time.minute, v.time.second, v.time.nano // 1000)
    elif value_name == 'timestamp':
        try:
            return datetime(v.timestamp.year, v.timestamp.month, v.timestamp.day, v.timestamp.hour, v.timestamp.minute, v.timestamp.second, v.timestamp.nano // 1000)
        except builtins.ValueError as exc:
            log_to_postgres(f'Unsupported timestamp: {v.timestamp} {exc}', WARNING)
            return None
    elif value_name == 'interval':
        # There is no Python type for intervals, so we return a custom dictionary.
        return {
            'years': v.interval.years,
            'months': v.interval.months,
            'days': v.interval.days,
            'hours': v.interval.hours,
            'minutes': v.interval.minutes,
            'seconds': v.interval.seconds,
            'micros': v.interval.micros
        }
    elif value_name == 'record':
        record = {}
        for f in v.record.atts:
            record[f.name] = raw_value_to_python(f.value)
        return record
    elif value_name == 'list':
        return [raw_value_to_python(i) for i in v.list.values]
    else:
        raise Exception(f"Unknown RAW value: {value_name}")


def operator_to_grpc_operator(operator):
    if operator == '=':
        return Operator.EQUALS
    elif operator in ['<>', '!=']:
        return Operator.NOT_EQUALS
    elif operator == '<':
        return Operator.LESS_THAN
    elif operator == '<=':
        return Operator.LESS_THAN_OR_EQUAL
    elif operator == '>':
        return Operator.GREATER_THAN
    elif operator == '>=':
        return Operator.GREATER_THAN_OR_EQUAL
    elif operator == 'LIKE':
        return Operator.LIKE
    elif operator == 'NOT LIKE':
        return Operator.NOT_LIKE
    elif operator == 'ILIKE':
        return Operator.ILIKE
    elif operator == 'NOT ILIKE':
        return Operator.NOT_ILIKE
    elif operator == '+':
        return Operator.PLUS
    elif operator == '-':
        return Operator.MINUS
    elif operator == '*':
        return Operator.TIMES
    elif operator == '/':
        return Operator.DIV
    elif operator == '%':
        return Operator.MOD
    elif operator.upper() == 'OR':  # Ensure case-insensitive match
        return Operator.OR
    elif operator.upper() == 'AND':  # Ensure case-insensitive match
        return Operator.AND
    else:
        # Unsupported operator
        return None


def multicorn_quals_to_grpc_quals(quals):
    grpc_quals = []
    for qual in quals:
        log_to_postgres(f'Processing qual: {qual}', DEBUG)
        log_to_postgres(f'Processing qual11: {qual}', DEBUG)
        field_name = qual.field_name
        log_to_postgres(f'Hello 0', DEBUG)
        if qual.is_list_operator:
            log_to_postgres(f'Hello 1', DEBUG)
            skip = False
            raw_values = []
            for value in qual.value:
                raw_value = python_value_to_raw(value)
                if raw_value is None:
                    # Unsupported value; skip this qualifier
                    skip = True
                    break
                raw_values.append(raw_value)
            if not skip:
                # All values are supported
                operator = operator_to_grpc_operator(qual.operator[0])
                if operator is None:
                    log_to_postgres(f'Unsupported operator: {qual.operator[0]}', WARNING)
                    continue
                if qual.list_any_or_all == ANY:
                    grpc_qual = IsAnyQual(values=raw_values, operator=operator)
                    grpc_quals.append(DASQual(name=field_name, is_any_qual=grpc_qual))
                else:
                    grpc_qual = IsAllQual(values=raw_values, operator=operator)
                    grpc_quals.append(DASQual(name=field_name, is_all_qual=grpc_qual))
        else:
            log_to_postgres(f'Hello 2', DEBUG)
            operator = operator_to_grpc_operator(qual.operator)
            log_to_postgres(f'Hello 3', DEBUG)
            if operator is None:
                log_to_postgres(f'Unsupported operator: {qual.operator}', WARNING)
                continue

            log_to_postgres(f'Hello 4', DEBUG)
            raw_value = python_value_to_raw(qual.value)
            log_to_postgres(f'Hello 5 {raw_value}', DEBUG)
            if raw_value is not None:
                # Can pushdown this qualifier
                log_to_postgres(f'Hello 6', DEBUG)
                grpc_qual = DASSimpleQual(operator=operator, value=raw_value)
                log_to_postgres(f'Hello 7', DEBUG)
                grpc_quals.append(DASQual(name=field_name, simple_qual=grpc_qual))
                log_to_postgres(f'Hello 8', DEBUG)

    log_to_postgres(f'Converted quals: {grpc_quals}', DEBUG)
    return grpc_quals


# Convert a Python value to a RAW Value.
# Returns a Value message or None if the value is not supported.
def python_value_to_raw(v):
    log_to_postgres(f'Converting Python value to RAW: {v} (Python type: {type(v)})', DEBUG)
    if v is None:
        return Value(null=ValueNull())
    elif isinstance(v, bool):
        # To be tested before 'int' since booleans are also instances of 'int'
        return Value(bool=ValueBool(v=v))
    elif isinstance(v, int):
        return Value(int=ValueInt(v=v))
    elif isinstance(v, float):
        return Value(double=ValueDouble(v=v))
    elif isinstance(v, str):
        return Value(string=ValueString(v=v))
    elif isinstance(v, bytes):
        return Value(binary=ValueBinary(v=v))
    elif isinstance(v, Decimal):
        return Value(decimal=ValueDecimal(v=str(v)))
    elif isinstance(v, time):
        return Value(time=ValueTime(
            hour=v.hour,
            minute=v.minute,
            second=v.second,
            nano=v.microsecond * 1000
        ))
    # 'datetime' objects are instances of 'date', test this first.
    elif isinstance(v, datetime):
        return Value(timestamp=ValueTimestamp(
            year=v.year,
            month=v.month,
            day=v.day,
            hour=v.hour,
            minute=v.minute,
            second=v.second,
            nano=v.microsecond * 1000
        ))
    elif isinstance(v, date):
        return Value(date=ValueDate(
            year=v.year,
            month=v.month,
            day=v.day
        ))
    elif isinstance(v, dict):
        # Check if it's an interval. No more than one of each field is allowed. No other fields are allowed
        if v.keys() == {'years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'millis'}:
            return Value(interval=ValueInterval(
                years=v['years'],
                months=v['months'],
                weeks=v['weeks'],
                days=v['days'],
                hours=v['hours'],
                minutes=v['minutes'],
                seconds=v['seconds'],
                millis=v['millis']
            ))
        else:
            # Otherwise, it's a user record
            atts = []
            for name, value in v.items():
                atts.append(ValueRecordAttr(name=name, value=python_value_to_raw(value)))
            return Value(record=ValueRecord(atts=atts))
    elif isinstance(v, list):
        inner_values = []
        for value in v:
            inner_values.append(python_value_to_raw(value))
        return Value(list=ValueList(values=inner_values))
    else:
        log_to_postgres(f'Unsupported value: {v}', WARNING)
        return None
    

def multicorn_serialize_as_json(obj):
    def default_serializer(obj):
        if isinstance(obj, time):
            return obj.isoformat()
        if isinstance(obj, date):
            # `date` also catches `datetime`
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        else:
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    return json.dumps(obj, default=default_serializer)


def multicorn_sortkeys_to_grpc_sortkeys(sortkeys):
    grpc_sort_keys_list = []

    if sortkeys: # Sortkeys can be None
        for sortkey in sortkeys:
            log_to_postgres(f'Processing sortkey: {sortkey}', DEBUG)
            attname = sortkey.attname
            attnum = sortkey.attnum
            is_reversed = sortkey.is_reversed
            nulls_first = sortkey.nulls_first
            collate = sortkey.collate
            grpc_sort_key = DASSortKey(name=attname, pos=attnum, is_reversed=is_reversed, nulls_first=nulls_first, collate=collate)
            grpc_sort_keys_list.append(grpc_sort_key)
    
    log_to_postgres(f'Converted sortkeys: {grpc_sort_keys_list}', DEBUG)
    return grpc_sort_keys_list


class GrpcStreamIterator:
    """
    A closeable iterator over a gRPC stream of row chunks.

    - Once the iterator is closed, further iteration stops.
    - If an error occurs while reading rows, the iterator is closed automatically.
    - If the iterator completes naturally (StopIteration), it is also closed.

    Usage:
        rows_stream = grpc_stub.GetRows(...)  # some gRPC method returning a stream

        with GrpcStreamIterator("mytable", rows_stream) as it:
            for row in it:
                # 'row' is a dict mapping column name -> Python value
                do_something_with(row)
    """

    def __init__(self, table_id, rows_stream):
        self._table_id = table_id
        self._stream = rows_stream   # The gRPC stream
        self._iterator = None        # Will hold our Python generator
        self._closed = False         # Track whether we've called close()

    def __enter__(self):
        """
        Optional: Support `with` context management.
        """
        self._iterator = self._rows_generator()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        On exiting the `with` block, ensure the stream is closed.
        """
        self.close()

    def _rows_generator(self):
        """
        A generator that iterates over chunks in the stream, then yields each row.
        Exits gracefully if we detect that the iterator has been closed.
        """
        try:
            for chunk in self._stream:
                # If user called .close(), exit early
                if self._closed:
                    break

                # Each chunk has multiple rows
                for row in chunk.rows:
                    yield build_row(row)

        except GeneratorExit:
            # The generator got force-closed (e.g., if someone forcibly closed
            # the Python generator). We'll just let it end.
            pass

    def __iter__(self):
        """
        Ensure this object is iterable. If we haven't created the generator,
        do so now.
        """
        if self._iterator is None:
            self._iterator = self._rows_generator()
        return self

    def __next__(self):
        """
        Get the next row from the generator, or close if we've reached the end.
        """
        if self._closed:
            # If .close() was called, we don't return anything further.
            raise StopIteration("Iterator already closed.")

        try:
            return next(self._iterator)
        except StopIteration:
            # Natural end of the generator => close and re-raise
            self.close()
            raise
        except Exception:
            # Any other error (including gRPC errors) => ensure we close, then re-raise
            self.close()
            raise

    def close(self):
        """
        Close the iterator and cancel the gRPC stream (if still open).
        """
        if not self._closed:
            self._closed = True
            log_to_postgres(
                f"Cancelling gRPC stream for table {self._table_id}",
                WARNING
            )
            try:
                self._stream.cancel()
            except Exception as e:
                # Log and continue. Typically we don't want a cancel() error
                # to overshadow the original cause of a shutdown or error.
                log_to_postgres(
                    f"Error canceling gRPC stream for table {self._table_id}: {e}",
                    WARNING
                )


def build_row(row):
    """
    Convert the row into a standard Python dict.
    """
    output_row = {}
    for col in row.columns:
        name = col.name
        data = col.data
        log_to_postgres(f'ExecuteTableRequest col {name} data {data}', DEBUG)
        output_row[name] = raw_value_to_python(data)
    return output_row


