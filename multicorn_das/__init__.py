"""
A foreign data wrapper for DAS.
"""

import builtins
import json
import grpc
from datetime import datetime, date, time
import base64
from decimal import Decimal
from logging import ERROR, INFO, DEBUG, WARNING, CRITICAL
from time import sleep

# Multicorn imports
from multicorn import ForeignDataWrapper, ForeignFunction, SortKey, ColumnDefinition, TableDefinition
from multicorn.utils import log_to_postgres, MulticornException

# Protobuf imports
from com.rawlabs.protocol.das.v1.common.das_pb2 import DASId, DASDefinition
from com.rawlabs.protocol.das.v1.common.environment_pb2 import Environment
from com.rawlabs.protocol.das.v1.functions.functions_pb2 import FunctionId
from com.rawlabs.protocol.das.v1.tables.tables_pb2 import TableId, Row, Column
from com.rawlabs.protocol.das.v1.types.values_pb2 import (
    Value, ValueNull, ValueByte, ValueShort, ValueInt, ValueFloat, ValueDouble,
    ValueDecimal, ValueBool, ValueString, ValueBinary, ValueDate, ValueTime,
    ValueTimestamp, ValueInterval, ValueRecord, ValueRecordAttr, ValueList
)
from com.rawlabs.protocol.das.v1.query.quals_pb2 import (
    Qual as DASQual,
    SimpleQual as DASSimpleQual,
    IsAnyQual,
    IsAllQual
)
from com.rawlabs.protocol.das.v1.query.operators_pb2 import Operator
from com.rawlabs.protocol.das.v1.query.query_pb2 import SortKey as DASSortKey, Query
from com.rawlabs.protocol.das.v1.services.functions_service_pb2 import GetFunctionDefinitionsRequest, NamedArgument, Argument, ExecuteFunctionRequest
from com.rawlabs.protocol.das.v1.services.tables_service_pb2 import (
    GetTableDefinitionsRequest, GetTableEstimateRequest, GetTableSortOrdersRequest,
    GetTablePathKeysRequest, ExplainTableRequest, ExecuteTableRequest,
    GetTableUniqueColumnRequest, InsertTableRequest, BulkInsertTableRequest,
    UpdateTableRequest, DeleteTableRequest, GetBulkInsertTableSizeRequest
)
from com.rawlabs.protocol.das.v1.services.registration_service_pb2 import RegisterRequest
from com.rawlabs.protocol.das.v1.services.health_service_pb2 import HealthCheckRequest

# gRPC stubs
from com.rawlabs.protocol.das.v1.services.functions_service_pb2_grpc import FunctionsServiceStub
from com.rawlabs.protocol.das.v1.services.tables_service_pb2_grpc import TablesServiceStub
from com.rawlabs.protocol.das.v1.services.registration_service_pb2_grpc import RegistrationServiceStub
from com.rawlabs.protocol.das.v1.services.health_service_pb2_grpc import HealthCheckServiceStub

class DASBase:
    """
    Base class for connecting to and interacting with a DAS gRPC server.
    Manages the gRPC channel, stubs, and some shared error-handling utilities.
    """

    def __init__(self, das_id, das_name, das_type, das_url, resource_name, fdw_options):
        """
        Initialize the base with DAS connection info.

        :param das_id: The DAS ID.
        :param das_name: A user-friendly name or fallback to das_url.
        :param das_type: The DAS 'type' (can be None if unknown).
        :param das_url:  The gRPC address (e.g. 'localhost:50051').
        :param resource_name: A short descriptor for logging (table or function name, etc.).
        :param fdw_options: Dictionary of FDW or function options (e.g. credentials).
        """
        self.das_id = DASId(id=das_id)
        self.das_name = das_name
        self.das_type = das_type
        self.das_url = das_url
        self.resource_name = resource_name  # e.g. table_name, function_name, or None
        self.fdw_options = fdw_options

        self.channel = None
        self.health_service = None
        self.registration_service = None

    # ---------- Error-raising helpers ----------
    @staticmethod
    def _raise_error(code, message, das_name=None, das_type=None, das_url=None, resource_name=None, cause=None):
        error_struct = {'code': code, 'message': message, 'das_name': das_name, 'das_type': das_type, 'das_url': das_url, 'resource_name': resource_name, 'cause': str(cause)}
        raise MulticornException(message, detail=json.dumps(error_struct))

    @staticmethod
    def _raise_registration_failed(message, das_name, das_type, das_url, resource_name=None, cause=None):
        return DASBase._raise_error('REGISTRATION_FAILED', message, das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_unavailable(das_name, das_type, das_url, resource_name, cause=None):
        return DASBase._raise_error('UNAVAILABLE', 'Server unavailable', das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_unauthenticated(message, das_name, das_type, das_url, resource_name, cause=None):
        return DASBase._raise_error('UNAUTHENTICATED', message, das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_permission_denied(message, das_name, das_type, das_url, resource_name, cause=None):
        return DASBase._raise_error('PERMISSION_DENIED', message, das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_invalid_argument(message, das_name, das_type, das_url, resource_name, cause=None):
        return DASBase._raise_error('INVALID_ARGUMENT', message, das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_unsupported_operation(message, das_name, das_type, das_url, resource_name, cause=None):
        return DASBase._raise_error('UNSUPPORTED_OPERATION', message, das_name=das_name, das_type=das_type, das_url=das_url, resource_name=resource_name, cause=cause)

    @staticmethod
    def _raise_internal_error(message, cause=None):
        return DASBase._raise_error('INTERNAL', message, cause=cause)


    # ---------- Re-creating channel & stubs ----------
    def _create_channel_and_stubs(self):
        """
        (Re)Create the insecure channel and stubs for normal FDW usage.
        """
        self.channel = grpc.insecure_channel(self.das_url, options=[
            ('grpc.max_receive_message_length', 4 * 1024 * 1024),
            ('grpc.max_send_message_length', 10 * 1024 * 1024),  # Adjust if needed
        ])
        self.health_service = HealthCheckServiceStub(self.channel)
        self.registration_service = RegistrationServiceStub(self.channel)

    # ---------- gRPC call helpers ----------

    @staticmethod
    def _health_check(stub: HealthCheckServiceStub):
        """
        Try a gRPC health check to confirm the server is alive.
        """
        log_to_postgres('Performing health check...', DEBUG)
        stub.Check(HealthCheckRequest())
        log_to_postgres('Health check succeeded.', DEBUG)

    @staticmethod
    def _grpc_call_internal(das_name, das_type, das_url, resource_name,
                            stub_caller, request, *,
                            attempts=30, health_stub=None,
                            recreate_channel_callback=None,
                            reregister_callback=None):
        """
        Unified wrapper for unary (non-streaming) gRPC calls.

        Retry logic:
          - If server is UNAVAILABLE, do a health check, re-create channel, and retry
            up to `attempts` times. If fail, raise corresponding exception.
          - If we get NOT_FOUND, call reregister_callback() ONCE, then retry.
          - If UNAUTHENTICATION, PERMISSION_DENIED or INVALID_ARGUMENT, raise corresponding error.
          - Otherwise, raise a generic internal error.

        Parameters:
          stub_caller               A function that calls the correct method on the stub.
          request                   The protobuf request object.
          attempts                  Max attempts for UNAVAILABLE errors.
          health_stub               If provided, do a health check with this stub on UNAVAILABLE.
          recreate_channel_callback Called to re-create stubs if UNAVAILABLE is encountered.
          reregister_callback       Called if NOT_FOUND is encountered (only once).

        Returns:
          The gRPC response from the stub method.
        """
        allow_reregister = True

        for attempt in range(1, attempts + 1):
            try:
                return stub_caller(request)

            except grpc.RpcError as e:
                code = e.code()
                log_to_postgres(f'gRPC error (attempt {attempt}/{attempts}): {code} - {e}', WARNING)

                if code == grpc.StatusCode.UNAVAILABLE:
                    # Out of attempts?
                    if attempt == attempts:
                        DASBase._raise_unavailable(das_name, das_type, das_url, resource_name, cause=e)
                    # Sleep, recreate channel, health-check
                    sleep(0.5)
                    if recreate_channel_callback:
                        recreate_channel_callback()
                    if health_stub:
                        try:
                            DASBase._health_check(health_stub)
                        except Exception:
                            # If health-check fails, wait a moment longer and keep looping
                            sleep(1)
                            continue
                    continue

                if code == grpc.StatusCode.NOT_FOUND:
                    if allow_reregister and reregister_callback is not None:
                        log_to_postgres('gRPC attempting registration and retry...', WARNING)
                        reregister_callback()
                        allow_reregister = False
                        # Try the call again after re-registration
                        continue
                    # We either have no reregister_callback or we already tried it once
                    DASBase._raise_registration_failed(
                        "registration failed (DAS not found)",
                        das_name=das_name,
                        das_type=das_type,
                        das_url=das_url,
                        resource_name=resource_name,
                        cause=e
                    )

                if code == grpc.StatusCode.UNAUTHENTICATED:
                    DASBase._raise_unauthenticated(e.details(), das_name, das_type, das_url, resource_name, cause=e)

                if code == grpc.StatusCode.PERMISSION_DENIED:
                    DASBase._raise_permission_denied(e.details(), das_name, das_type, das_url, resource_name, cause=e)

                if code == grpc.StatusCode.INVALID_ARGUMENT:
                    DASBase._raise_invalid_argument(e.details(), das_name, das_type, das_url, resource_name, cause=e)

                if code == grpc.StatusCode.UNIMPLEMENTED:
                    DASBase._raise_unsupported_operation(e.details(), das_name, das_type, das_url, resource_name, cause=e)

                # Anything else => generic error
                DASBase._raise_internal_error("gRPC error calling remote DAS server", cause=e)

            except Exception as ex:
                log_to_postgres(f"Non-gRPC error calling remote DAS server: {ex}", WARNING)

                # Non-gRPC error
                DASBase._raise_internal_error("Non-gRPC error calling remote DAS server", cause=ex)

        # Should never get here, but just in case:
        DASBase._raise_internal_error("exhausted all attempts in gRPC call loop")


    # ---------- Re-registration helper ----------
    def _maybe_reregister_das(self):
        """
        If 'das_type' is in FDW options, re-register the DAS with the same ID using
        the RegistrationService.
        """
        if not self.das_type:
            log_to_postgres('DAS type not in FDW options; skipping re-registration.', DEBUG)
            return

        log_to_postgres(f'Re-registering DAS with type: {self.das_type}', DEBUG)

        request = RegisterRequest(
            definition=DASDefinition(type=self.das_type, options=self.fdw_options),
            id=self.das_id
        )

        # Use our new _grpc_registration_call to call "Register" on the registration stub
        response = self._grpc_registration_call(
            "Register",
            request,
            reregister_callback=None  # prevent recursion
        )

        if response.error:
            self._raise_registration_failed(response.error, das_name=self.das_name, das_type=self.das_type, das_url=self.das_url)

        log_to_postgres(f'Re-registered DAS with ID: {self.das_id.id}', DEBUG)


    def _grpc_registration_call(self, method_name, request, attempts=30,
                                health_stub=None, recreate_channel_callback=None,
                                reregister_callback=None):
        """
        Similar convenience wrapper for the registration_service stub.
        """
        if health_stub is None:
            health_stub = self.health_service
        if recreate_channel_callback is None:
            recreate_channel_callback = self._create_channel_and_stubs
        # Usually we don't want to re-register again inside a re-registration call
        # to avoid an infinite loop. So pass `reregister_callback=None`.
        # Or you can let it call the same callback, but be careful about loops.
        if reregister_callback is None:
            reregister_callback = None

        def stub_caller(req):
            method = getattr(self.registration_service, method_name)
            return method(req)

        return self._grpc_call_internal(
            self.das_name,
            self.das_type,
            self.das_url,
            self.resource_name,
            stub_caller,
            request,
            attempts=attempts,
            health_stub=health_stub,
            recreate_channel_callback=recreate_channel_callback,
            reregister_callback=reregister_callback
        )




class DASFdw(DASBase, ForeignDataWrapper):
    """
    Main FDW class for table-like DAS interaction. Inherits from DASBase for
    connectivity and from ForeignDataWrapper for Multicorn FDW integration.
    """

    _startup_cost = 20  # Default cost of starting up the FDW

    def __init__(self, fdw_options, fdw_columns):
        das_id = fdw_options['das_id']
        # For backward compatibility, in case 'das_name' is not defined
        das_name = fdw_options.get('das_name', fdw_options['das_url'])
        das_type = fdw_options.get('das_type', None)
        das_url = fdw_options['das_url']
        table_name = fdw_options['das_table_name']  # resource name for logs

        DASBase.__init__(self, das_id, das_name, das_type, das_url, table_name, fdw_options)
        ForeignDataWrapper.__init__(self, fdw_options, fdw_columns)

        # Additional FDW init
        self.table_id = TableId(name=table_name)

        # For backward compatibility
        self.table_name = table_name

        # e.g. set the startup cost
        self._startup_cost = int(fdw_options.get('das_startup_cost', '20'))

        # Create channel & stubs, including table_service
        self._create_channel_and_stubs()

        log_to_postgres(f"Initialized DASFdw with table {self.table_name}, options={fdw_options}", DEBUG)


    def _create_channel_and_stubs(self):
        """
        Override to also create a TablesServiceStub after the base is set up.
        """
        super()._create_channel_and_stubs()
        self.table_service = TablesServiceStub(self.channel)

    def _grpc_table_call(self, method_name, request, attempts=30,
                         health_stub=None, recreate_channel_callback=None,
                         reregister_callback=None):
        """
        Convenience wrapper that calls `method_name` on self.table_service using the
        standard `_grpc_call_internal`.
        """
        if health_stub is None:
            health_stub = self.health_service
        if recreate_channel_callback is None:
            recreate_channel_callback = self._create_channel_and_stubs
        if reregister_callback is None:
            reregister_callback = self._maybe_reregister_das

        def stub_caller(req):
            method = getattr(self.table_service, method_name)
            return method(req)

        return self._grpc_call_internal(
            self.das_name,
            self.das_type,
            self.das_url,
            self.table_name,
            stub_caller,
            request,
            attempts=attempts,
            health_stub=health_stub,
            recreate_channel_callback=recreate_channel_callback,
            reregister_callback=reregister_callback
        )

    def _grpc_registration_call(self, method_name, request, attempts=30,
                                health_stub=None, recreate_channel_callback=None,
                                reregister_callback=None):
        """
        Similar convenience wrapper for the registration_service stub.
        """
        if health_stub is None:
            health_stub = self.health_service
        if recreate_channel_callback is None:
            recreate_channel_callback = self._create_channel_and_stubs
        # Usually we don't want to re-register again inside a re-registration call
        # to avoid an infinite loop. So pass `reregister_callback=None`.
        # Or you can let it call the same callback, but be careful about loops.
        if reregister_callback is None:
            reregister_callback = None

        def stub_caller(req):
            method = getattr(self.registration_service, method_name)
            return method(req)

        return self._grpc_call_internal(
            self.das_name,
            self.das_type,
            self.das_url,
            self.table_name,
            stub_caller,
            request,
            attempts=attempts,
            health_stub=health_stub,
            recreate_channel_callback=recreate_channel_callback,
            reregister_callback=reregister_callback
        )

    # ---------- FDW overrides ----------
    def get_rel_size(self, quals, columns):
        log_to_postgres(f'Getting rel size for table {self.table_id} with quals: {quals}, columns: {columns}', DEBUG)

        request = GetTableEstimateRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            quals=multicorn_quals_to_grpc_quals(quals),
            columns=columns
        )
        log_to_postgres(f'GetTableEstimateRequest: {request}', DEBUG)

        response = self._grpc_table_call("GetTableEstimate", request)
        log_to_postgres(
            f'Got rel size: {response.rows} rows, {response.bytes} bytes for table {self.table_id}',
            DEBUG
        )

        return (response.rows, response.bytes)

    def can_sort(self, sortkeys):
        log_to_postgres(f'Checking if can sort for table {self.table_id} with sortkeys: {sortkeys}', DEBUG)

        request = GetTableSortOrdersRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            sort_keys=multicorn_sortkeys_to_grpc_sortkeys(sortkeys)
        )
        log_to_postgres(f'GetTableSortOrdersRequest: {request}', DEBUG)

        response = self._grpc_table_call("GetTableSortOrders", request)
        log_to_postgres(f'Can sort: {response} for table {self.table_id}', DEBUG)

        out = []
        for sk in response.sort_keys:
            out.append(
                SortKey(
                    attname=sk.name,
                    attnum=sk.pos,
                    is_reversed=sk.is_reversed,
                    nulls_first=sk.nulls_first,
                    collate=sk.collate
                )
            )
        return out

    def get_path_keys(self):
        log_to_postgres(f'Getting path keys for table {self.table_id}', DEBUG)

        request = GetTablePathKeysRequest(
            das_id=self.das_id,
            table_id=self.table_id
        )
        log_to_postgres(f'GetTablePathKeysRequest: {request}', DEBUG)

        response = self._grpc_table_call("GetTablePathKeys", request)
        log_to_postgres(f'Got path keys: {response.path_keys} for table {self.table_id}', DEBUG)

        out = []
        for pk in response.path_keys:
            out.append((pk.key_columns, pk.expected_rows))
        return out

    def explain(self, quals, columns, sortkeys=None, limit=None, verbose=False, scopes=None, http_headers=None):
        log_to_postgres(
            f'Explaining for table {self.table_id} with quals: {quals}, columns: {columns}, '
            f'sortkeys: {sortkeys}, limit: {limit}, verbose: {verbose}',
            DEBUG
        )

        env = Environment(
            scopes = scopes if scopes is not None else [],
            http_headers = http_headers if http_headers is not None else {}
        )
        request = ExplainTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            query=Query(
                quals=multicorn_quals_to_grpc_quals(quals),
                columns=columns,
                sort_keys=multicorn_sortkeys_to_grpc_sortkeys(sortkeys),
                limit=limit
            ),
            env=env
        )
        log_to_postgres(f'ExplainTableRequest: {request}', DEBUG)

        response = self._grpc_table_call("ExplainTable", request)
        return response.stmts

    def execute(self, quals, columns, sortkeys=None, limit=None, planid=None, scopes=None, http_headers=None):
        log_to_postgres(
            f'Executing for table {self.table_id} with quals: {quals}, columns: {columns}, '
            f'sortkeys: {sortkeys}, limit: {limit}, planid: {planid}',
            DEBUG
        )

        env = Environment(
            scopes = scopes if scopes is not None else [],
            http_headers = http_headers if http_headers is not None else {}
        )
        request = ExecuteTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            query=Query(
                quals=multicorn_quals_to_grpc_quals(quals),
                columns=columns,
                sort_keys=multicorn_sortkeys_to_grpc_sortkeys(sortkeys),
                limit=limit
            ),
            plan_id=str(planid),
            max_batch_size_bytes=4 * 1024 * 1024,
            env = env
        )

        log_to_postgres(f'ExecuteTableRequest request: {request}', DEBUG)

        rows_stream = self._grpc_table_call("ExecuteTable", request)
        return GrpcStreamIterator(self.das_name, self.das_type, self.das_url, self.table_name, self.table_id, rows_stream)

    @property
    def modify_batch_size(self):
        log_to_postgres(f'Getting modify batch size {self.table_id}', DEBUG)

        request = GetBulkInsertTableSizeRequest(
            das_id=self.das_id,
            table_id=self.table_id
        )
        log_to_postgres(f'GetBulkInsertTableSizeRequest: {request}', DEBUG)

        response = self._grpc_table_call("GetBulkInsertTableSize", request)
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

        response = self._grpc_table_call("GetTableUniqueColumn", request)
        log_to_postgres(f'Got unique column: {response.column} for table {self.table_id}', DEBUG)

        return response.column

    def insert(self, values):
        log_to_postgres(f'Inserting values: {values} into table {self.table_id}', DEBUG)

        columns = []
        for name, value in values.items():
            columns.append(Column(name=name, data=python_value_to_das(value)))

        request = InsertTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row=Row(columns=columns)
        )
        log_to_postgres(f'InsertTableRequest: {request}', DEBUG)

        response = self._grpc_table_call("InsertTable", request)
        output_row = {}
        for col in response.row.columns:
            name = col.name
            data = col.data
            output_row[name] = das_value_to_python(data)

        log_to_postgres(f'Inserted row: {output_row} into table {self.table_id}', DEBUG)
        return output_row

    def bulk_insert(self, all_values):
        log_to_postgres(f'Bulk inserting values: {all_values} into table {self.table_id}', DEBUG)

        rows = []
        for row in all_values:
            columns = []
            for name, value in row.items():
                columns.append(Column(name=name, data=python_value_to_das(value)))
            rows.append(Row(columns=columns))

        request = BulkInsertTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            rows=rows
        )
        log_to_postgres(f'BulkInsertTableRequest: {request}', DEBUG)

        response = self._grpc_table_call("BulkInsertTable", request)
        output_rows = [
            {col.name: das_value_to_python(col.data) for col in row.columns}
            for row in response.rows
        ]
        log_to_postgres(f'Bulk insert of {len(all_values)} rows into table {self.table_id}', DEBUG)

        return output_rows

    def update(self, rowid, new_values):
        log_to_postgres(f'Updating rowid: {rowid} with new values: {new_values} in table {self.table_id}', DEBUG)

        columns = []
        for name, value in new_values.items():
            columns.append(Column(name=name, data=python_value_to_das(value)))

        request = UpdateTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row_id=python_value_to_das(rowid),
            new_row=Row(columns=columns)
        )
        log_to_postgres(f'UpdateTableRequest: {request}', DEBUG)

        response = self._grpc_table_call("UpdateTable", request)
        output_row = {}
        for col in response.row.columns:
            name = col.name
            data = col.data
            output_row[name] = das_value_to_python(data)

        log_to_postgres(f'Updated row: {output_row} in table {self.table_id}', DEBUG)
        return output_row

    def delete(self, rowid):
        log_to_postgres(f'Deleting rowid: {rowid} from table {self.table_id}', DEBUG)

        request = DeleteTableRequest(
            das_id=self.das_id,
            table_id=self.table_id,
            row_id=python_value_to_das(rowid)
        )
        log_to_postgres(f'DeleteTableRequest: {request}', DEBUG)

        self._grpc_table_call("DeleteTable", request)
        log_to_postgres(f'Deleted row: {rowid} from table {self.table_id}', DEBUG)

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        """
        Called when the user does IMPORT FOREIGN SCHEMA. Retrieves table definitions
        from the remote DAS and returns a list of TableDefinition objects.

        :param schema: The target PostgreSQL schema name.
        :param srv_options: Server-level options.
        :param options: Options passed in the IMPORT FOREIGN SCHEMA command.
        :param restriction_type: (Import all or limit to some tables?)
        :param restricts: The list of tables to include/exclude if restriction is set.
        """
        das_name = srv_options.get('das_name', srv_options['das_url'])
        das_type = srv_options.get('das_type', None)
        das_url = srv_options['das_url']

        log_to_postgres(f'Import schema for {das_url}', DEBUG)

        channel = None
        table_stub = None
        registration_stub = None
        health_stub = None

        def recreate_channel():
            nonlocal channel, table_stub, registration_stub, health_stub
            channel = grpc.insecure_channel(das_url, options=[
                ('grpc.max_receive_message_length', 4 * 1024 * 1024),
                ('grpc.max_send_message_length', 10 * 1024 * 1024),
            ])
            table_stub = TablesServiceStub(channel)
            registration_stub = RegistrationServiceStub(channel)
            health_stub = HealthCheckServiceStub(channel)

        recreate_channel()  # initial creation

        def safe_call(method_name, get_stub, request):
            """
            A local function that calls _grpc_call_internal with a lambda referencing
            the *current* stub. If we re-create stubs, it picks up the new one.
            """
            def stub_caller(req):
                stub = get_stub()
                return getattr(stub, method_name)(req)

            return cls._grpc_call_internal(
                das_name,
                das_type,
                das_url,
                None, # We don't have a table name at this point
                stub_caller,
                request,
                attempts=30,
                health_stub=health_stub,
                recreate_channel_callback=recreate_channel,
                reregister_callback=reregister_callback
            )

        def reregister_callback():
            # Typically, if we get NOT_FOUND here, we try to register again
            if das_type is None:
                DASFdw._raise_registration_failed("das_type not in srv_options", das_name=das_name, das_type=das_type, das_url=das_url)

            log_to_postgres(f'Re-registering DAS with type={das_type} ...', WARNING)
            req = RegisterRequest(
                definition=DASDefinition(type=das_type, options={**srv_options, **options}),
                id=new_das_id
            )
            safe_call("Register", lambda: registration_stub, req)

        # Check if we already have a DAS ID or not
        if 'das_id' in srv_options:
            new_das_id = DASId(id=srv_options['das_id'])
            log_to_postgres(f'Using existing das_id={new_das_id.id}', DEBUG)
        else:
            # Must create new DAS
            if das_type is None:
                DASFdw._raise_registration_failed("das_type is required if das_id not provided", das_name=das_name, das_type=das_type, das_url=das_url)

            log_to_postgres(f'Creating new DAS of type={das_type} ...', DEBUG)
            register_req = RegisterRequest(
                definition=DASDefinition(type=das_type, options={**srv_options, **options})
            )
            register_resp = safe_call("Register", lambda: registration_stub, register_req)

            if register_resp.error:
                DASFdw._raise_registration_failed(register_resp.error, das_name=das_name, das_type=das_type, das_url=das_url)

            new_das_id = register_resp.id
            log_to_postgres(f'Created new DAS with ID={new_das_id.id}', DEBUG)

        # Now get table definitions
        log_to_postgres(f'Getting definitions for DAS ID: {new_das_id.id}', DEBUG)
        request = GetTableDefinitionsRequest(das_id=new_das_id)
        response = safe_call("GetTableDefinitions", lambda: table_stub, request)
        log_to_postgres(f'Got definitions: {response.definitions}', DEBUG)

        table_definitions = []
        for table in response.definitions:
            log_to_postgres(f'About to process table {table.table_id.name}', DEBUG)
            column_definitions = []

            table_name = table.table_id.name
            columns = table.columns
            for column in columns:
                column_definitions.append(
                    ColumnDefinition(column.name, type_name=das_type_to_postgresql(column.type))
                )
            table_options = dict(
                das_name=das_name,
                das_type=das_type,
                das_url=das_url,
                das_id=new_das_id.id,
                das_table_name=table_name,
                das_startup_cost=str(table.startup_cost)
            )

            table_definitions.append(
                TableDefinition(table_name, columns=column_definitions, options=table_options)
            )
            log_to_postgres(f'Processed table {table_name}', DEBUG)

        return table_definitions



class DASFunction(DASBase, ForeignFunction):
    """
    DAS implementation of ForeignFunction.
    """

    def __init__(self, options):
        """
        Initialize the function caller with the relevant FDW options (function name, DAS ID, etc.).
        """
        das_id = options['das_id']
        das_name = options.get('das_name', options['das_url'])
        das_type = options.get('das_type', None)
        das_url = options['das_url']
        function_name = options['das_function_name']
        super().__init__(das_id, das_name, das_type, das_url, function_name, options)

        self.function_name = function_name
        self.function_id = FunctionId(name=function_name)
        self._create_channel_and_stubs()

        log_to_postgres(
            f'Initialized DASFunction with function={function_name}, FDW options={options}',
            DEBUG
        )

    def _create_channel_and_stubs(self):
        """
        Override to also create the FunctionsServiceStub after the base stubs.
        """
        super()._create_channel_and_stubs()
        # Now create function_service
        self.function_service = FunctionsServiceStub(self.channel)

    def execute(
        self,
        named_args=None,
        scopes=None,
        http_headers=None
    ):
        """
        We build an ExecuteFunctionRequest with the named args,
        send it via gRPC, and convert the result to a Python object.
        """
        log_to_postgres(
            f"Executing function {self.function_name} with args={named_args}",
            DEBUG
        )

        # 2. Build the list of Argument messages
        #    - For positional arguments, set argument.arg=Value
        #    - For named arguments, set argument.named_arg=NamedArgument
        final_args = []

        if named_args:
            for key, val in named_args.items():
                val_msg = python_value_to_das(val)
                final_args.append(Argument(named_arg=NamedArgument(name=key, value=val_msg)))

        env = Environment(
            scopes = scopes if scopes is not None else [],
            http_headers = http_headers if http_headers is not None else {}
        )

        # 3. Build the ExecuteFunctionRequest
        request = ExecuteFunctionRequest(
            das_id=self.das_id,
            function_id=self.function_id,
            args=final_args,
            env=env
        )
        log_to_postgres(f"ExecuteFunctionRequest: {request}", DEBUG)

        # 4. Call the remote function via a gRPC unary RPC
        #    We reuse our _grpc_function_call, which is analogous to _grpc_table_call
        response = self._grpc_function_call("ExecuteFunction", request)
        # or if you have a direct stub, something like:
        # response = self.functions_service.ExecuteFunction(request)

        log_to_postgres(f"Got ExecuteFunctionResponse: {response}", DEBUG)

        # 5. Convert the output Value to a Python object
        #    If there's no output or it's empty, you might return None
        if not response.HasField("output"):
            log_to_postgres("ExecuteFunction returned an empty output Value.", DEBUG)
            return None

        python_result = das_value_to_python(response.output)
        log_to_postgres(f"Converted function output to Python object: {python_result}", DEBUG)
        return python_result

    def _grpc_function_call(self, method_name, request, attempts=30,
                         health_stub=None, recreate_channel_callback=None,
                         reregister_callback=None):
        """
        Convenience wrapper that calls `method_name` on self.function_service using the
        standard `_grpc_call_internal`.
        """
        if health_stub is None:
            health_stub = self.health_service
        if recreate_channel_callback is None:
            recreate_channel_callback = self._create_channel_and_stubs
        if reregister_callback is None:
            reregister_callback = self._maybe_reregister_das

        def stub_caller(req):
            method = getattr(self.function_service, method_name)
            return method(req)

        return self._grpc_call_internal(
            self.das_name,
            self.das_type,
            self.das_url,
            self.function_name,
            stub_caller,
            request,
            attempts=attempts,
            health_stub=health_stub,
            recreate_channel_callback=recreate_channel_callback,
            reregister_callback=reregister_callback
        )

#
# ========== Helper Functions ==========
#

def das_type_to_postgresql(t):
    """
    Convert a DAS Type definition from DAS to a PostgreSQL type string.
    Adjust the mapping logic as needed.
    """
    type_name = t.WhichOneof('type')
    if type_name == 'any':
        return 'JSONB'
    if type_name == 'byte':
        # Postgres does not have a BYTE type, so SMALLINT is closest
        return 'SMALLINT' + (' NULL' if t.byte.nullable else '')
    if type_name == 'short':
        return 'SMALLINT' + (' NULL' if t.short.nullable else '')
    if type_name == 'int':
        return 'INTEGER' + (' NULL' if t.int.nullable else '')
    if type_name == 'long':
        return 'BIGINT' + (' NULL' if t.long.nullable else '')
    if type_name == 'float':
        return 'REAL' + (' NULL' if t.float.nullable else '')
    if type_name == 'double':
        return 'DOUBLE PRECISION' + (' NULL' if t.double.nullable else '')
    if type_name == 'decimal':
        return 'DECIMAL' + (' NULL' if t.decimal.nullable else '')
    if type_name == 'bool':
        return 'BOOLEAN' + (' NULL' if t.bool.nullable else '')
    if type_name == 'string':
        return 'TEXT' + (' NULL' if t.string.nullable else '')
    if type_name == 'binary':
        return 'BYTEA' + (' NULL' if t.binary.nullable else '')
    if type_name == 'date':
        return 'DATE' + (' NULL' if t.date.nullable else '')
    if type_name == 'time':
        return 'TIME' + (' NULL' if t.time.nullable else '')
    if type_name == 'timestamp':
        return 'TIMESTAMP' + (' NULL' if t.timestamp.nullable else '')
    if type_name == 'interval':
        return 'INTERVAL' + (' NULL' if t.interval.nullable else '')
    if type_name == 'record':
        # Records were originally advertised as HSTORE, which only supports records where all values are strings.
        # To maintain backward compatibility, we retain this logic when possible.
        # If the record type has no declared attributes, it implies that it can have any attribute of any type,
        # so we also declare it as JSONB.
        att_types = [f.tipe.WhichOneof('type') for f in t.record.atts]
        if att_types == [] or any([att_type != 'string' for att_type in att_types]):
            return 'JSONB' + (' NULL' if t.record.nullable else '')
        else:
            return 'HSTORE' + (' NULL' if t.record.nullable else '')
    if type_name == 'list':
        inner_type = t.list.inner_type
        # Postgres arrays can always hold NULL values. Their inner type IS nullable.
        inner_type_str = das_type_to_postgresql(inner_type)
        # When declaring the array type, the syntax doesn't accept that NULLABLE is specified.
        # We remove ' NULL' if found.
        if inner_type_str.endswith(' NULL'):
            inner_type_str = inner_type_str[:-5]
        column_schema = f'{inner_type_str}[]' + (' NULL' if t.list.nullable else '')
        return column_schema

    raise ValueError(f"Unsupported DAS type: {type_name}")


def das_value_to_python(v):
    """
    Convert a DAS Value protobuf to a Python object.
    """
    value_name = v.WhichOneof('value')
    # 'null' is obtained when the value was explicitly set to a null value.
    # if the value wasn't set at all, `v` won't be None, instead it's an
    # object which .WhichOneof returns a Python None.
    if value_name == 'null' or value_name is None:
        return None
    if value_name == 'byte':
        return v.byte.v
    if value_name == 'short':
        return v.short.v
    if value_name == 'int':
        return v.int.v
    if value_name == 'long':
        return v.long.v
    if value_name == 'float':
        return v.float.v
    if value_name == 'double':
        return v.double.v
    if value_name == 'decimal':
        # Return as string or Decimal. E.g.:
        return Decimal(v.decimal.v)
    if value_name == 'bool':
        return v.bool.v
    if value_name == 'string':
        return v.string.v
    if value_name == 'binary':
        return v.binary.v
    if value_name == 'date':
        try:
            return date(v.date.year, v.date.month, v.date.day)
        except ValueError as exc:
            log_to_postgres(f'Unsupported date: {v.date}: {exc}', WARNING)
            return None
    if value_name == 'time':
        return time(
            v.time.hour,
            v.time.minute,
            v.time.second,
            v.time.nano // 1000
        )
    if value_name == 'timestamp':
        try:
            return datetime(
                v.timestamp.year,
                v.timestamp.month,
                v.timestamp.day,
                v.timestamp.hour,
                v.timestamp.minute,
                v.timestamp.second,
                v.timestamp.nano // 1000
            )
        except ValueError as exc:
            log_to_postgres(f'Unsupported timestamp: {v.timestamp} {exc}', WARNING)
            return None
    if value_name == 'interval':
        # convert the interval into an ISO 8601 duration string.

        years   = v.interval.years
        months  = v.interval.months
        days    = v.interval.days
        hours   = v.interval.hours
        minutes = v.interval.minutes
        seconds = v.interval.seconds
        micros  = v.interval.micros
        
        # Combine seconds and micros into a single floating-point number
        total_seconds = seconds + micros / 1e6

        # Format the seconds value to 6 decimal places,
        # then remove trailing zeros and an orphaned decimal point.
        sec_str = f"{total_seconds:.6f}".rstrip('0').rstrip('.')
        # If the seconds part becomes empty (i.e. total_seconds was 0), set it to "0"
        if not sec_str:
            sec_str = "0"
        # Build the ISO 8601 duration string.
        iso_duration = f"P{years}Y{months}M{days}DT{hours}H{minutes}M{sec_str}S"
        return iso_duration
    if value_name == 'record':
        record_dict = {}
        for f in v.record.atts:
            record_dict[f.name] = das_value_to_python(f.value)
        return record_dict
    if value_name == 'list':
        return [das_value_to_python(i) for i in v.list.values]

    raise ValueError(f"Unsupported DAS value: {value_name}")


def python_value_to_das(v):
    """
    Convert a Python value to a DAS Value.
    Return None if unsupported or unknown.
    """
    log_to_postgres(f'Converting Python value to DAS: {v} (Python type: {type(v)})', DEBUG)

    if v is None:
        return Value(null=ValueNull())

    if isinstance(v, bool):
        return Value(bool=ValueBool(v=v))
    if isinstance(v, int):
        return Value(int=ValueInt(v=v))
    if isinstance(v, float):
        return Value(double=ValueDouble(v=v))
    if isinstance(v, str):
        return Value(string=ValueString(v=v))
    if isinstance(v, bytes):
        return Value(binary=ValueBinary(v=v))
    if isinstance(v, Decimal):
        return Value(decimal=ValueDecimal(v=str(v)))
    if isinstance(v, time):
        return Value(time=ValueTime(
            hour=v.hour,
            minute=v.minute,
            second=v.second,
            nano=v.microsecond * 1000
        ))
    if isinstance(v, datetime):
        return Value(timestamp=ValueTimestamp(
            year=v.year,
            month=v.month,
            day=v.day,
            hour=v.hour,
            minute=v.minute,
            second=v.second,
            nano=v.microsecond * 1000
        ))
    if isinstance(v, date):
        # Note that datetime is also a date, so we check datetime first above
        return Value(date=ValueDate(
            year=v.year,
            month=v.month,
            day=v.day
        ))

    if isinstance(v, dict):
        # Possibly an interval-like or record-like
        # This is up to you how you interpret dict -> interval
        # Example interval dict:
        #   {'years':..., 'months':..., 'days':..., 'hours':..., 'minutes':..., 'seconds':..., 'micros':...}
        # If that matches, build a ValueInterval. Otherwise treat as record.
        if all(k in v for k in ('years', 'months', 'days', 'hours', 'minutes', 'seconds', 'micros')):
            return Value(interval=ValueInterval(
                years=v['years'],
                months=v['months'],
                days=v['days'],
                hours=v['hours'],
                minutes=v['minutes'],
                seconds=v['seconds'],
                micros=v['micros']
            ))
        else:
            # treat as record
            atts = []
            for name, value in v.items():
                atts.append(ValueRecordAttr(name=name, value=python_value_to_das(value)))
            return Value(record=ValueRecord(atts=atts))

    if isinstance(v, list):
        inner_values = []
        for value in v:
            inner_values.append(python_value_to_das(value))
        return Value(list=ValueList(values=inner_values))

    log_to_postgres(f'Unsupported Python value: {v}', WARNING)
    return None


def operator_to_grpc_operator(operator):
    """
    Map a Python/SQL operator string to the gRPC Operator enum.
    Returns None if not recognized.
    """
    op = operator.upper()
    if op in ['=', '==']:
        return Operator.EQUALS
    if op in ['<>', '!=']:
        return Operator.NOT_EQUALS
    if op == '<':
        return Operator.LESS_THAN
    if op == '<=':
        return Operator.LESS_THAN_OR_EQUAL
    if op == '>':
        return Operator.GREATER_THAN
    if op == '>=':
        return Operator.GREATER_THAN_OR_EQUAL
    if op in ['~~', 'LIKE']:
        return Operator.LIKE
    if op in ['!~~', 'NOT LIKE']:
        return Operator.NOT_LIKE
    if op in ['~~*', 'ILIKE']:
        return Operator.ILIKE
    if op in ['!~~*', 'NOT ILIKE']:
        return Operator.NOT_ILIKE
    if op == '+':
        return Operator.PLUS
    if op == '-':
        return Operator.MINUS
    if op == '*':
        return Operator.TIMES
    if op == '/':
        return Operator.DIV
    if op == '%':
        return Operator.MOD
    if op == 'OR':
        return Operator.OR
    if op == 'AND':
        return Operator.AND
    return None


def multicorn_quals_to_grpc_quals(quals):
    """
    Translate Multicorn Qual objects to a list of DASQual messages.
    """
    grpc_quals = []
    for qual in quals:
        log_to_postgres(f'Processing qual: {qual}', DEBUG)
        field_name = qual.field_name

        if qual.is_list_operator:
            # qual.operator = array of operators (like ['='])
            # qual.value = list of values
            skip = False
            das_values = []
            for value in qual.value:
                das_value = python_value_to_das(value)
                if das_value is None:
                    skip = True
                    break
                das_values.append(das_value)

            if not skip:
                # Usually qual.operator is something like ['='] => use the first operator
                operator = operator_to_grpc_operator(qual.operator[0])
                if operator is None:
                    log_to_postgres(f'Unsupported operator: {qual.operator[0]}', WARNING)
                    continue
                if qual.list_any_or_all == 0:  # ANY in multicorn is 0, ALL is 1
                    grpc_qual = IsAnyQual(values=das_values, operator=operator)
                    grpc_quals.append(DASQual(name=field_name, is_any_qual=grpc_qual))
                else:
                    grpc_qual = IsAllQual(values=das_values, operator=operator)
                    grpc_quals.append(DASQual(name=field_name, is_all_qual=grpc_qual))

        else:
            operator = operator_to_grpc_operator(qual.operator)
            if operator is None:
                log_to_postgres(f'Unsupported operator: {qual.operator}', WARNING)
                continue

            das_value = python_value_to_das(qual.value)
            if das_value is not None:
                grpc_qual = DASSimpleQual(operator=operator, value=das_value)
                grpc_quals.append(DASQual(name=field_name, simple_qual=grpc_qual))

    log_to_postgres(f'Converted quals: {grpc_quals}', DEBUG)
    return grpc_quals


def multicorn_sortkeys_to_grpc_sortkeys(sortkeys):
    """
    Convert a list of SortKey objects into gRPC DASSortKey messages.
    """
    grpc_sort_keys_list = []

    if sortkeys:
        for sortkey in sortkeys:
            log_to_postgres(f'Processing sortkey: {sortkey}', DEBUG)
            attname = sortkey.attname
            attnum = sortkey.attnum
            is_reversed = sortkey.is_reversed
            nulls_first = sortkey.nulls_first
            collate = sortkey.collate

            grpc_sort_key = DASSortKey(
                name=attname,
                pos=attnum,
                is_reversed=is_reversed,
                nulls_first=nulls_first,
                collate=collate
            )
            grpc_sort_keys_list.append(grpc_sort_key)

    log_to_postgres(f'Converted sortkeys: {grpc_sort_keys_list}', DEBUG)
    return grpc_sort_keys_list


class GrpcStreamIterator:
    """
    A closeable iterator over a gRPC stream of row chunks.

    Once the iterator is closed, further iteration stops.
    If an error occurs or StopIteration occurs, it is closed automatically.
    """

    def __init__(self, das_name, das_type, das_url, table_name, table_id, rows_stream):
        self._das_name = das_name
        self._das_type = das_type
        self._das_url = das_url
        self._table_name = table_name
        self._table_id = table_id
        self._stream = rows_stream   # The gRPC streaming response
        self._iterator = None        # Will hold our Python generator
        self._closed = False         # Track whether we've called close()

    def __enter__(self):
        self._iterator = self._rows_generator()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _rows_generator(self):
        try:
            for chunk in self._stream:
                if self._closed:
                    break
                for row in chunk.rows:
                    yield build_row(row)
        except GeneratorExit:
            pass

    def __iter__(self):
        if self._iterator is None:
            self._iterator = self._rows_generator()
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration("Iterator already closed.")
        try:
            return next(self._iterator)
        except StopIteration:
            self.close()
            raise
        except grpc.RpcError as e:
            self.close()
            code = e.code()
            if code == grpc.StatusCode.UNAVAILABLE:
                DASFdw._raise_unavailable(self._das_name, self._das_type, self._das_url, self._table_name, cause=e)
            elif code == grpc.StatusCode.UNIMPLEMENTED:
                DASFdw._raise_unsupported_operation(e.details(), self._das_name, self._das_type, self._das_url, self._table_name, cause=e)
            elif code == grpc.StatusCode.UNAUTHENTICATED:
                DASFdw._raise_unauthenticated(e.details(), self._das_name, self._das_type, self._das_url, self._table_name, cause=e)
            elif code == grpc.StatusCode.PERMISSION_DENIED:
                DASFdw._raise_permission_denied(e.details(), self._das_name, self._das_type, self._das_url, self._table_name, cause=e)
            elif code == grpc.StatusCode.INVALID_ARGUMENT:
                DASFdw._raise_invalid_argument(e.details(), self._das_name, self._das_type, self._das_url, self._table_name, cause=e)
            else:
                # Notably, we do not handle NOT_FOUND here, as that should not happen in a stream.
                # Instead, we expect it to be handled in a previous call to get_rel_size.
                DASFdw._raise_internal_error("gRPC error in stream iterator", cause=e)
        except Exception:
            self.close()
            raise

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._stream.cancel()  # Cancel any further streaming
            except Exception as e:
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
        output_row[name] = das_value_to_python(data)
    return output_row

# Helper that serializes our Multicorn Python objects as JSON, used when turning
# such objects into JSONB. The function is called from C code. The resulting string
# is eventually passed to a Postgres internal function that decodes the JSON and
# builds a JSONB Datum.
def multicorn_serialize_as_json(obj):
    def default_serializer(obj):
        if isinstance(obj, time):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, date):
            # `date` also catches `datetime`
            return obj.isoformat()
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    return json.dumps(obj, default=default_serializer)



