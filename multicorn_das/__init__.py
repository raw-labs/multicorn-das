"""A foreign data wrapper for DAS."""

from multicorn import *
from multicorn.utils import log_to_postgres
from datetime import datetime, date, time
from decimal import Decimal
from logging import DEBUG, INFO, WARNING # Never use ERROR or CRITICAL in log_to_post as it relaunches an exception

from com.rawlabs.protocol.das.services.tables_service_pb2 import GetDefinitionsRequest, GetRelSizeRequest, CanSortRequest, GetPathKeysRequest, ExecuteRequest, UniqueColumnRequest, InsertRequest, BulkInsertRequest, UpdateRequest, DeleteRequest, ModifyBatchSizeRequest
from com.rawlabs.protocol.das.services.registration_service_pb2 import RegisterRequest
from com.rawlabs.protocol.das.services.health_service_pb2 import HealthCheckRequest
from com.rawlabs.protocol.das.das_pb2 import DASId, DASDefinition
from com.rawlabs.protocol.das.tables_pb2 import TableId, SortKeys as DASSortKeys, SortKey as DASSortKey, Qual as DASQual, SimpleQual as DASSimpleQual, ListQual as DASListQual, Operator, Equals, NotEquals, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Row
from com.rawlabs.protocol.das.services.tables_service_pb2_grpc import TablesServiceStub
from com.rawlabs.protocol.das.services.registration_service_pb2_grpc import RegistrationServiceStub
from com.rawlabs.protocol.das.services.health_service_pb2_grpc import HealthCheckServiceStub
from com.rawlabs.protocol.raw.values_pb2 import *

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
        self.channel = grpc.insecure_channel(self.url)
        self.table_service = TablesServiceStub(self.channel)

        self.das_id = DASId(id=fdw_options['das_id'])
        self.table_id = TableId(name=fdw_options['das_table_name'])

        # This is only used by re__register_das
        self.fdw_options = fdw_options
        
        # Set the startup cost for ForeignDataWrapper plugin to use.
        self._startup_cost = int(fdw_options.get('das_startup_cost', '20'))
        
        log_to_postgres(f'Initialized DASFdw with DAS ID: {self.das_id.id}, Table ID: {self.table_id.name}', DEBUG)


    # This is used for crash recovery.
    # First off, it will wait for the server to come back alive if it's a server unavailable error.
    # Then, it will re-register the DAS if it was not defined in fdw_options.
    def __crash_recovery(self, e):

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
                attempts -= 1

                self.health_service = HealthCheckServiceStub(self.channel)
                try:
                    response = self.health_service.Check(HealthCheckRequest())
                    # If we reach here, the server is available again
                    break
                except Exception as ex:
                    log_to_postgres(f'Error in health_service.Check: {e}', WARNING)
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
            registration_service.Register(request)
        except Exception as e:
            log_to_postgres(f'Error in registration_service.Register: {e}', WARNING)
            raise e
        
        log_to_postgres(f'Re-registered DAS with ID: {self.das_id.id}', DEBUG)


    def get_rel_size(self, quals, columns):
        log_to_postgres(f'Getting rel size for table {self.table_id} with quals: {quals}, columns: {columns}', DEBUG)
        
        grpc_quals = multicorn_quals_to_grpc_quals(quals)
        grpc_columns = columns

        request = GetRelSizeRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            quals=grpc_quals,
            columns=grpc_columns
        )

        try:
            response = self.table_service.GetRelSize(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetRelSize for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.get_rel_size(quals, columns)

        log_to_postgres(f'Got rel size: {response.rows} rows, {response.bytes} bytes for table {self.table_id}', DEBUG)
        return (response.rows, response.bytes)


    def can_sort(self, sortkeys):
        log_to_postgres(f'Checking if can sort for table {self.table_id} with sortkeys: {sortkeys}', DEBUG)

        grpc_sort_keys = multicorn_sortkeys_to_grpc_sortkeys(sortkeys)
        log_to_postgres(f'Converted sortkeys: {grpc_sort_keys}', DEBUG)


        request = CanSortRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            sortKeys=grpc_sort_keys
        )
        log_to_postgres(f'Can sort request: {request}', DEBUG)

        try:
            response = self.table_service.CanSort(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.CanSort for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.can_sort(sortkeys)
        
        log_to_postgres(f'Can sort: {response} for table {self.table_id}', DEBUG)

        out = []
        for sk in response.sortKeys.sortKeys:
            out.append(SortKey(attname=sk.name, attnum=sk.pos, is_reversed=sk.isReversed, nulls_first=sk.nullsFirst, collate=sk.collate))
        return out


    def get_path_keys(self):
        log_to_postgres(f'Getting path keys for table {self.table_id}', DEBUG)

        request = GetPathKeysRequest(
            dasId=self.das_id,
            tableId=self.table_id
        )

        try:
            response = self.table_service.GetPathKeys(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.GetPathKeys for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.get_path_keys()
        
        log_to_postgres(f'Got path keys: {response.pathKeys} for table {self.table_id}', DEBUG)

        out = []
        for pk in response.pathKeys:
            out.append((pk.keyColumns, pk.expectedRows))
        return out


    def execute(self, quals, columns, sortkeys=None, limit=None, planid=None):
        log_to_postgres(f'Executing for table {self.table_id} with quals: {quals}, columns: {columns}, sortkeys: {sortkeys}, limit: {limit}, planid: {planid}', DEBUG)

        grpc_quals = multicorn_quals_to_grpc_quals(quals)
        grpc_columns = columns

        grpc_sort_keys = multicorn_sortkeys_to_grpc_sortkeys(sortkeys) if sortkeys else None

        # Create an ExecuteRequest message
        request = ExecuteRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            quals=grpc_quals,
            columns=grpc_columns,
            sortKeys=grpc_sort_keys,
            limit=limit,
            planId=str(planid)
        )

        # Make the RPC call
        try:
            rows_stream = self.table_service.Execute(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.Execute for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.execute(quals, columns, sortkeys=sortkeys, limit=limit, planid=planid)

        # Iterate over the streamed responses and generate multicorn rows
        for chunk in rows_stream:
            #log_to_postgres(f'Got chunk with {len(chunk.rows)} rows for table {self.table_id}', DEBUG)
            for row in chunk.rows:
                output_row = {}
                for name, value in row.data.items():
                    output_row[name] = raw_value_to_python(value)
                log_to_postgres(f'Yielding row: {output_row} for table {self.table_id}', DEBUG)
                yield output_row


    @property
    def modify_batch_size(self):
        log_to_postgres(f'Getting modify batch size {self.table_id}', DEBUG)
        request = ModifyBatchSizeRequest(
            dasId=self.das_id,
            tableId=self.table_id
        )
        try:
            response = self.table_service.ModifyBatchSize(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.ModifyBatchSize for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.modify_batch_size

        log_to_postgres(f'Got modify batch size: {response.size} for table {self.table_id}', DEBUG)
        return response.size

    @property
    def rowid_column(self):
        log_to_postgres(f'Getting rowid column for table {self.table_id}', DEBUG)

        request = UniqueColumnRequest(
            dasId=self.das_id,
            tableId=self.table_id
        )

        try:
            response = self.table_service.UniqueColumn(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.UniqueColumns for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.rowid_column

        log_to_postgres(f'Got unique column: {response.column} for table {self.table_id}', DEBUG)

        return response.column


    def insert(self, values):
        log_to_postgres(f'Inserting values: {values} into table {self.table_id}', DEBUG)

        # Convert the values to RAW values
        row = {}
        for name, value in values.items():
            row[name] = python_value_to_raw(value)

        # Create an InsertRequest message
        request = InsertRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            values=Row(data=row)
        )

        # Make the RPC call
        try:
            response = self.table_service.Insert(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.Insert for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.insert(values)

        output_row = {}
        for name, value in response.row.data.items():
            output_row[name] = raw_value_to_python(value)

        log_to_postgres(f'Inserted row: {output_row} into table {self.table_id}', DEBUG)

        return output_row


    def bulk_insert(self, all_values):
        log_to_postgres(f'Bulk inserting values: {all_values} into table {self.table_id}', DEBUG)
        rows = [{name: python_value_to_raw(value) for name, value in row.items()} for row in all_values]

        # Create an InsertRequest message
        request = BulkInsertRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            values=[Row(data=row) for row in rows]
        )

        # Make the RPC call
        try:
            response = self.table_service.BulkInsert(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.BulkInsert for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.bulk_insert(values)

        output_rows = [{name: raw_value_to_python(value) for name, value in row.data.items()} for row in response.rows]
        log_to_postgres(f'Bulk insert of {len(all_values)} into table {self.table_id} returned row: {output_rows}', DEBUG)

        return output_rows

    

    def update(self, rowid, new_values):
        log_to_postgres(f'Updating rowid: {rowid} with new values: {new_values} in table {self.table_id}', DEBUG)

        # Convert the values to RAW values
        row = {}
        for name, value in new_values.items():
            row[name] = python_value_to_raw(value)

        # Create an UpdateRequest message
        request = UpdateRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            rowId=python_value_to_raw(rowid),
            newValues=Row(data=row)
        )

        # Make the RPC call
        try:
            response = self.table_service.Update(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.Update for table {self.table_id}: {e}', WARNING)
            self.__crash_recovery(e)
            return self.update(rowid, new_values)
        
        output_row = {}
        for name, value in response.row.data.items():
            output_row[name] = raw_value_to_python(value)
        
        log_to_postgres(f'Updated row: {output_row} in table {self.table_id}', DEBUG)

        return output_row
    

    def delete(self, rowid):
        log_to_postgres(f'Deleting rowid: {rowid} from table {self.table_id}', DEBUG)

        # Create a DeleteRequest message
        request = DeleteRequest(
            dasId=self.das_id,
            tableId=self.table_id,
            rowId=python_value_to_raw(rowid)
        )

        # Make the RPC call
        try:
            self.table_service.Delete(request)
        except Exception as e:
            log_to_postgres(f'Error in table_service.Delete for table {self.table_id}: {e}', WARNING)
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
                    raise ValueError('das_type is required when das_id is not provided')
                
                log_to_postgres(f'Creating new DAS with type: {srv_options["das_type"]}', DEBUG)

                registration_service = RegistrationServiceStub(channel)

                das_definition = DASDefinition(
                    type=srv_options['das_type'],
                    options=srv_options | options
                )

                request = RegisterRequest(definition=das_definition)

                try:
                    das_id = registration_service.Register(request)
                except Exception as e:
                    log_to_postgres(f'Error in registration_service.Register: {e}', WARNING)
                    raise e
                log_to_postgres(f'Created new DAS with ID: {das_id.id}', DEBUG)

            table_service = TablesServiceStub(channel)

            log_to_postgres(f'Getting definitions for DAS ID: {das_id.id}', DEBUG)
            request = GetDefinitionsRequest(dasId=das_id)
            try:
                response = table_service.GetDefinitions(request)
            except Exception as e:
                log_to_postgres(f'Error in table_service.GetDefinitions: {e}', WARNING)
                raise e
            log_to_postgres(f'Got definitions: {response.definitions}', DEBUG)

            table_definitions = []

            for table in response.definitions:
                column_definitions = []

                table_name = table.tableId.name
                columns = table.columns
                for column in columns:
                    column_definitions.append(ColumnDefinition(column.name, type_name=raw_type_to_postgresql(column.type)))
                
                if 'das_type' in srv_options:
                    table_options = dict(url=url, das_id=das_id.id, das_table_name=table_name, das_startup_cost=str(table.startupCost), das_type=srv_options['das_type'])
                else:
                    table_options = dict(url=url, das_id=das_id.id, das_table_name=table_name, das_startup_cost=str(table.startupCost))

                table_definitions.append(TableDefinition(table_name, columns=column_definitions, options=table_options))
            
            return table_definitions
    

def raw_type_to_postgresql(t):
    type_name = t.WhichOneof('type')
    if type_name == 'undefined':
        if t.nullable: return 'TEXT'
        else: return 'UNKNOWN'
    elif type_name == 'byte':
        assert(t.byte.triable == False, "Triable types are not supported")
        return 'SMALLINT' + (' NULL' if t.byte.nullable else '') # Postgres does not have a BYTE type, so SMALLINT is closest
    elif type_name == 'short':
        assert(t.short.triable == False, "Triable types are not supported")
        return 'SMALLINT' + (' NULL' if t.short.nullable else '')
    elif type_name == 'int':
        assert(t.int.triable == False, "Triable types are not supported")
        return 'INTEGER' + (' NULL' if t.int.nullable else '')
    elif type_name == 'long':
        assert(t.long.triable == False, "Triable types are not supported")
        return 'BIGINT' + (' NULL' if t.long.nullable else '')
    elif type_name == 'float':
        assert(t.float.triable == False, "Triable types are not supported")
        return 'REAL' + (' NULL' if t.float.nullable else '')
    elif type_name == 'double':
        assert(t.double.triable == False, "Triable types are not supported")
        return 'DOUBLE PRECISION' + (' NULL' if t.double.nullable else '')
    elif type_name == 'decimal':
        assert(t.decimal.triable == False, "Triable types are not supported")
        return 'DECIMAL' + (' NULL' if t.decimal.nullable else '')
    elif type_name == 'bool':
        assert(t.bool.triable == False, "Triable types are not supported")
        return 'BOOLEAN' + (' NULL' if t.bool.nullable else '')
    elif type_name == 'string':
        assert(t.string.triable == False, "Triable types are not supported")
        return 'TEXT' + (' NULL' if t.string.nullable else '')
    elif type_name == 'binary':
        assert(t.binary.triable == False, "Triable types are not supported")
        return 'BYTEA' + (' NULL' if t.binary.nullable else '')
    elif type_name == 'date':
        assert(t.date.triable == False, "Triable types are not supported")
        return 'DATE' + (' NULL' if t.date.nullable else '')
    elif type_name == 'time':
        assert(t.time.triable == False, "Triable types are not supported")
        return 'TIME' + (' NULL' if t.time.nullable else '')
    elif type_name == 'timestamp':
        assert(t.timestamp.triable == False, "Triable types are not supported")
        return 'TIMESTAMP' + (' NULL' if t.timestamp.nullable else '')
    elif type_name == 'interval':
        assert(t.interval.triable == False, "Triable types are not supported")
        return 'INTERVAL' + (' NULL' if t.interval.nullable else '')
    elif type_name == 'record':
        assert(t.record.triable == False, "Triable types are not supported")
        return 'HSTORE'
    elif type_name == 'list':
        assert(t.list.triable == False, "Triable types are not supported")
        return f'{raw_type_to_postgresql(t.list.innerType)}[]' + (' NULL' if t.list.nullable else '')
    else:
        raise ValueError(f"Unsupported Type: {type_name}")


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
        return date(v.date.year, v.date.month, v.date.day)
    elif value_name == 'time':
        return time(v.time.hour, v.time.minute, v.time.second, v.time.nano // 1000)
    elif value_name == 'timestamp':
        return datetime(v.timestamp.year, v.timestamp.month, v.timestamp.day, v.timestamp.hour, v.timestamp.minute, v.timestamp.second, v.timestamp.nano // 1000)
    elif value_name == 'interval':
        # There is no Python type for intervals, so we return a custom dictionary.
        return {
            'years': v.interval.years,
            'months': v.interval.months,
            'weeks': v.interval.weeks,
            'days': v.interval.days,
            'hours': v.interval.hours,
            'minutes': v.interval.minutes,
            'seconds': v.interval.seconds,
            'millis': v.interval.millis
        }
    elif value_name == 'record':
        record = {}
        for f in v.record.fields:
            record[f.name] = raw_value_to_python(f.value)
        return record
    else:
        raise Exception(f"Unknown RAW value: {value_name}")


def operator_to_grpc_operator(operator):
    if operator == '=':
        return Operator(equals=Equals())
    elif operator == '<':
        return Operator(lessThan=LessThan())
    elif operator == '>':
        return Operator(greaterThan=GreaterThan())
    elif operator == '<=':
        return Operator(lessThanOrEqual=LessThanOrEqual())
    elif operator == '>=':
        return Operator(greaterThanOrEqual=GreaterThanOrEqual())
    elif operator in ['<>', '!=']:
        return Operator(notEquals=NotEquals())
    else:
        # Unsupported operator
        return None


def multicorn_quals_to_grpc_quals(quals):
    grpc_quals = []
    for qual in quals:
        log_to_postgres(f'Processing qual: {qual}', DEBUG)
        field_name = qual.field_name
        if qual.is_list_operator:
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
                is_any = qual.list_any_or_all == ANY
                operator =  operator_to_grpc_operator(qual.operator[0])
                if not operator:
                    log_to_postgres(f'Unsupported operator: {qual.operator[0]}', WARNING)
                    continue
                grpc_qual = DASListQual(operator=operator, isAny=is_any, values=raw_values)
                grpc_quals.append(DASQual(fieldName=field_name, listQual=grpc_qual))
        else:
            operator = operator_to_grpc_operator(qual.operator)
            if not operator:
                log_to_postgres(f'Unsupported operator: {qual.operator}', WARNING)
                continue

            raw_value = python_value_to_raw(qual.value)
            if raw_value is not None:
                # Can pushdown this qualifier
                grpc_qual = DASSimpleQual(operator=operator, value=raw_value)
                grpc_quals.append(DASQual(fieldName=field_name, simpleQual=grpc_qual))

    log_to_postgres(f'Converted quals: {grpc_quals}', DEBUG)
    return grpc_quals


# Convert a Python value to a RAW Value.
# Returns a Value message or None if the value is not supported.
def python_value_to_raw(v):
    log_to_postgres(f'Converting Python value to RAW: {v} (Python type: {type(v)})', DEBUG)
    if v is None:
        return Value(null=ValueNull())
    elif isinstance(v, int):
        return Value(int=ValueInt(v=v))
    elif isinstance(v, float):
        return Value(double=ValueDouble(v=v))
    elif isinstance(v, str):
        return Value(string=ValueString(v=v))
    elif isinstance(v, bytes):
        return Value(binary=ValueBinary(v=v))
    elif isinstance(v, bool):
        return Value(bool=ValueBool(v=v))
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
            fields = []
            for name, value in v.items():
                fields.append(ValueRecordField(name=name, value=python_value_to_raw(value)))
            return Value(record=ValueRecord(fields=fields))
    elif isinstance(v, list):
        inner_values = []
        for value in v:
            inner_values.append(python_value_to_raw(value))
        return Value(list=ValueList(values=inner_values))
    else:
        log_to_postgres(f'Unsupported value: {v}', WARNING)
        return None


def multicorn_sortkeys_to_grpc_sortkeys(sortkeys):
    grpc_sort_keys_list = []

    for sortkey in sortkeys:
        log_to_postgres(f'Processing sortkey: {sortkey}', DEBUG)
        attname = sortkey.attname
        attnum = sortkey.attnum
        is_reversed = sortkey.is_reversed
        nulls_first = sortkey.nulls_first
        collate = sortkey.collate
        grpc_sort_key = DASSortKey(name=attname, pos=attnum, isReversed=is_reversed, nullsFirst=nulls_first, collate=collate)
        grpc_sort_keys_list.append(grpc_sort_key)
    
    log_to_postgres(f'Converted sortkeys: {grpc_sort_keys_list}', DEBUG)
    return DASSortKeys(sortKeys=grpc_sort_keys_list)
