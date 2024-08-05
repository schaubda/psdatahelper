import pandas as pd
import json
from .credential import Credential
from .log import Log
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2.rfc6749.errors import InvalidClientError
from requests.models import Response
from requests_oauthlib import OAuth2Session
from typing import Optional


class API:
    """
    A class used to simplify interactions with the PowerSchool API.

    Methods
    -------
    __init__(credential, log)
        Initialize the API instance.
    __del__()
        Close the API session.
    pq_set_prefix(self, pq_prefix: str)
        Set the prefix for PowerQuery names.
    pq_run(pq_name, pq_parameters)
        Run the given PowerQuery and return the results as a Pandas DataFrame.
    table_get_record(table_name, record_id, projection)
        Retrieve a specific record from a table.
    table_get_records(table_name, query_expression, projection, page, pagesize, sort, sortdescending)
        Retrieve records from a table based on a query expression.
    table_get_record_count(table_name, query_expression)
        Retrieve the count of records from a table based on a query expression.
    table_insert_records(table_name, records)
        Insert records contained in the given Pandas DataFrame into the given table.
    table_update_records(table_name, id_column_name, records)
        Update multiple records in a specified table.
    table_delete_record(table_name, record_id)
        Delete a record with the given ID from the given table.
    table_delete_records(table_name, id_column_name, records)
        Delete multiple records contained in the given Pandas DataFrame from the given table.
    """

    def __init__(self, credential: Credential, log: Log = Log('ps_api')):
        """
        Initialize the API instance and connect to the PowerSchool API.

        Parameters
        ----------
        credential : Credential
            The Credential instance containing API credentials.
        log : Log, optional
            The log instance to use for logging (default is Log('ps_api')).
        """

        # Store the provided credential and log instances
        self._credential = credential
        self._log = log

        # Initialize connection status and prefix for queries
        self._api_connected = False
        self._pq_prefix = ''

        # Define log messages for no records found and empty DataFrame
        self._NO_RECORDS_LOG_MSG = 'No records found.'
        self._EMPTY_DF_LOG_MSG = 'Input DataFrame is empty. No records processed.'

        # Check if the credentials have been loaded successfully
        if not self._credential.loaded:
            # Log an error if the API cannot connect due to unloaded credentials
            self._log.error('API not connected because credentials are not loaded')

            # Exit the function if credentials are not loaded
            return

        try:
            # Create an OAuth2 session using the client ID and access token from the credentials
            self.session = OAuth2Session(
                    client=BackendApplicationClient(client_id=self._credential.fields['client_id']),
                    token={
                        'token_type':   'Bearer',
                        'access_token': self._credential.fields['access_token']
                    }
            )

        except InvalidClientError as e:
            # Log an error if there is an invalid client error during connection
            self._log.error(f"Error connecting to the PowerSchool API: {e}")

        except Exception as e:
            # Log any other errors that occur during the connection attempt
            self._log.error(f"Error connecting to the PowerSchool API: {e}")

        else:
            # Log a debug message indicating successful connection to the API
            self._log.debug('Connected to the PowerSchool API')

            # Set the headers for the API session
            self.session.headers = {
                'Content-Type': 'application/json',
                'Accept':       'application/json'
            }

            # Mark the API as connected
            self._api_connected = True

    def __del__(self):
        """
        Close the API session.
        """

        # Log a debug message indicating that the API session is being closed
        self._log.debug('Closing API session')

        # Close the API session to release resources
        self.session.close()

    def _access_requests_parse(self, response: Response, read_only: bool = True) -> list[str]:
        # Check if the response status code indicates a forbidden access (403)
        if response.status_code == 403:
            access_requests = []

            # Determine the access level based on the read_only flag
            if read_only:
                access_level = 'ViewOnly'

            else:
                access_level = 'FullAccess'

            try:
                # Parse the JSON response to extract access request errors
                for error in response.json()['errors']:
                    # Format and append each access request as a Plugin XML access request string
                    access_requests.append(f'\n<field table="{error["resource"]}" field="{error["field"]}" '
                                           f'access="{access_level}"/>')

            except Exception as e:
                # Log an error if there is an issue parsing the access requests
                self._log.error(f"Error parsing access requests: {e}")

            # Sort the access requests before returning
            access_requests.sort()

            return access_requests

        else:
            # Log an error if the response status code is not 403
            self._log.error(f"Response status code is not 403\n\t{response.status_code} - {response.text}")

            # Return an empty list
            return []

    def _response_log_status_code(self, resource: str, method: str, response: Response):
        # Log error messages based on the HTTP status code of the response
        match response.status_code:
            case 400:
                # Log a bad request error with the resource and response text
                self._log.error(f"Bad request to {resource}:\n\t{response.text}")

            case 401:
                # Log an unauthorized request error with the resource and response text
                self._log.error(f"Unauthorized request to {resource}:\n\t{response.text}")

            case 404:
                # Log a resource not found error
                self._log.error(f"Resource not found: {resource}")

            case 405:
                # Log an error for method not allowed on the specified resource
                self._log.error(f'Method "{method}" not allowed for {resource}')

            case 409:
                # Log a conflict error with the resource and response text
                self._log.error(f"Conflict with {resource}:\n\t{response.text}")

            case 415:
                # Log an unsupported media type error with the resource and response text
                self._log.error(f"Unsupported media type for {resource}:\n\t{response.text}")

            case 500:
                # Log an internal server error with the resource and response text
                self._log.error(f"Internal server error for {resource}:\n\t{response.text}")

            case 509:
                # Log a resource throttling error with the resource and response text
                self._log.error(f"Resource throttling currently in place for {resource}:\n\t{response.text}")

    def _request(self, method: str, resource: str, read_only: bool = True, suppress_log: bool = False, **kwargs):
        # Send an HTTP request using the specified method and resource URL
        response = self.session.request(method=method, url=f"{self._credential.server_address}{resource}", **kwargs)

        # Check if the response indicates forbidden access (403)
        if response.status_code == 403:
            # Parse access requests from the response
            access_requests = self._access_requests_parse(response, read_only=read_only)

            # If there are access requests, log an error unless suppressed
            if access_requests:
                if not suppress_log:
                    self._log.error(f"Plugin {self._credential.plugin} doesn't have access to one or more of the "
                                    f"requested fields.\n"
                                    f"Access requests to add to the plugin:{''.join(access_requests)}")

                # Attach the access requests to the response object
                response.access_requests = access_requests

        # Log the status code for responses that are not successful (not 200) and not suppressed
        elif response.status_code != 200 and not suppress_log:
            self._response_log_status_code(resource, method, response)

        # Return the HTTP response object
        return response

    def _pq_parse_response(self, response: Response) -> pd.DataFrame:
        # Parse the JSON response from the PowerQuery API
        response_json = response.json()

        # Check if the response contains records and tables
        if 'record' in response_json and 'tables' in response_json['record'][0]:
            # Log the number of records returned from the API
            self._log.debug(f"{len(response_json['record'])} records returned from PQ")

            records = []  # List to hold parsed records
            fields = {}  # Dictionary to hold field values
            tables = response_json['record'][0]['tables'].keys()  # Get table names from the first record

            # Iterate through each record in the response
            for record in response_json['record']:
                # Iterate through each table associated with the record
                for table in tables:
                    # Extract key-value pairs from the table and update the fields dictionary
                    for key, value in record['tables'][table].items():
                        fields.update({f"{table}.{key}": value})

                # Append a copy of the fields dictionary to the records list
                records.append(fields.copy())

                # Clear fields for the next record
                fields.clear()

            # Return the records as a pandas DataFrame
            return pd.DataFrame(records)

        # If only records are present without tables, return them as a DataFrame
        elif 'record' in response_json:
            # Log the number of records returned from the API
            self._log.debug(f"{len(response_json['record'])} records returned from PQ")

            # Return the records as a pandas DataFrame
            return pd.DataFrame(response_json['record'])

        # Log a message if no records were found in the response
        else:
            self._log.debug(self._NO_RECORDS_LOG_MSG)

            # Return an empty DataFrame if no records are found
            return pd.DataFrame()

    def _table_parse_response(self, response: Response, table_name: str) -> pd.DataFrame:
        # Parse the JSON response from the API for a specific table
        response_json = response.json()

        # Check if the response contains records and tables
        if 'record' in response_json and 'tables' in response_json['record'][0]:
            # Log the number of records returned for the specified table
            self._log.debug(f"{len(response_json['record'])} records returned from {table_name}")

            records = []  # List to hold parsed records
            fields = {}  # Dictionary to hold field values

            # Iterate through each record in the response
            for record in response_json['record']:
                # Add the record ID to the fields dictionary
                fields.update({'id': record['id']})

                # Extract key-value pairs from the specified table and update the fields dictionary
                for key, value in record['tables'][table_name].items():
                    fields.update({key: value})

                # Append a copy of the fields dictionary to the records list
                records.append(fields.copy())

                # Clear fields for the next record
                fields.clear()

            # Return the records as a pandas DataFrame
            return pd.DataFrame(records)

        # If only records are present without tables, return them as a DataFrame
        elif 'record' in response_json:
            # Log the number of records returned for the specified table
            self._log.debug(f"{len(response_json['record'])} records returned from {table_name}")

            # Return the records as a pandas DataFrame
            return pd.DataFrame(response_json['record'])

        # Log a message if no records were found in the response
        else:
            self._log.debug(self._NO_RECORDS_LOG_MSG)

            # Return an empty DataFrame if no records are found
            return pd.DataFrame()

    def pq_set_prefix(self, pq_prefix: str):
        """
        Set the prefix for PowerQuery names.

        The prefix is appended to the beginning of the PowerQuery name passed to the run_pq method.
        It should not include a period at the end.

        Parameters
        ----------
        pq_prefix : str
            The prefix to set for PowerQuery names. Do not include a period at the end.

        Examples
        --------
        If you wanted to run several attendance-related PowerQueries, you could set the prefix to
        'com.pearson.core.attendance' and then use the last part of their names to run them.

        >>> api.pq_set_prefix('com.pearson.core.attendance')
        >>> api.run_pq('attendance_totals')
        >>> api.run_pq('daily_attendance')
        >>> api.run_pq('student_attendance_detail')
        """

        # Store the provided prefix for PowerQuery names
        self._pq_prefix = pq_prefix

        # Log a debug message indicating that the PowerQuery prefix has been set
        self._log.debug(f"PowerQuery prefix set to {self._pq_prefix}")

    def pq_run(self, pq_name: str, pq_parameters: Optional[dict] = None) -> pd.DataFrame:
        """
        Run the given PowerQuery and return the results as a Pandas DataFrame.

        Parameters
        ----------
        pq_name : str
            The name of the PowerQuery to run.
        pq_parameters : dict, optional
            A dictionary of parameters to pass to the PowerQuery (default is None).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the results of the PowerQuery.
            Returns an empty DataFrame if the query fails or the API is not connected.
        """

        # Check if the API is connected before running the PowerQuery
        if not self._api_connected:
            self._log.error(f"PowerQuery {pq_name} not run because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Initialize parameters as an empty dictionary if none are provided
        if pq_parameters is None:
            pq_parameters = dict()

        # Construct the full PowerQuery name with the prefix if it exists
        if self._pq_prefix != '':
            full_pq_name = f"{self._pq_prefix}.{pq_name}"

        else:
            full_pq_name = pq_name

        # Log the PowerQuery that is about to be run
        self._log.debug(f"Running PQ: {full_pq_name}")

        # Check if there are parameters to include in the request
        if pq_parameters is not None and len(pq_parameters) > 0:
            # Convert the parameters to JSON format for the request
            payload = json.dumps(pq_parameters)

            # Send a POST request to run the PowerQuery with parameters
            response = self._request('post', resource=f"/ws/schema/query/{full_pq_name}?pagesize=0",
                                     data=payload)

        else:
            # Send a POST request to run the PowerQuery without parameters
            response = self._request('post', resource=f"/ws/schema/query/{full_pq_name}?pagesize=0")

        # Check if the request was successful
        if response.status_code == 200:
            self._log.debug('Query successful')

            # Parse and return the response as a DataFrame
            return self._pq_parse_response(response)

        # Handle unsuccessful requests
        else:
            self._log.error('Query failed. See above for response details.')

            # Return an empty DataFrame if the query fails
            return pd.DataFrame()

    def table_get_record(self, table_name: str, record_id: str | int, projection: str = '*') -> pd.DataFrame:
        """
        Retrieve a specific record from a table.

        Parameters
        ----------
        table_name : str
            The name of the table from which to retrieve the record.
        record_id : str or int
            The ID of the record to retrieve.
        projection : str, optional
            A comma-separated list of fields to include in the response (default is '*' to return all fields).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the requested record.
            Returns an empty DataFrame if the API is not connected, if the record is not found,
            or if there is an error during the request.
        """

        # Check if the API is connected before attempting to retrieve the record
        if not self._api_connected:
            self._log.error(f"Record {record_id} not retrieved from {table_name} because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Log the attempt to get the specified record from the table
        self._log.debug(f"Getting record from {table_name}")

        # Send a GET request to retrieve the record with the specified projection
        response = self._request('get',
                                 resource=f"/ws/schema/table/{table_name}/{record_id}?projection={projection}")

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the response as a JSON object
            response_json = response.json()

            # Check if the specified table exists in the response
            if table_name in response_json['tables']:
                self._log.debug('Record found')

                # Return the record as a DataFrame with the index set to 0
                return pd.DataFrame(response_json['tables'][table_name], index=[0])

            else:
                # Log a message if no records were found for the specified table
                self._log.debug(self._NO_RECORDS_LOG_MSG)

                # Return an empty DataFrame if no records are found
                return pd.DataFrame()

        else:
            # Log an error if the request was not successful
            self._log.error(f"Error getting record from {table_name}: {response.status_code} - {response.text}")

            # Return an empty DataFrame if the request fails
            return pd.DataFrame()

    def table_get_records(self, table_name: str, query_expression: str, projection: str = '*', page: int = 0,
                          pagesize: int = 0, sort: str = '', sortdescending: bool = False) -> pd.DataFrame:
        """
        Retrieve records from a table based on a query expression.

        Parameters
        ----------
        table_name : str
            The name of the table from which to retrieve records.
        query_expression : str
            The query expression to filter the records. Refer to the PowerSchool developer documentation for the
            requirements of the query expression. Documentation is located here:
            https://support.powerschool.com/developer/#/page/table-resources
        projection : str, optional
            The fields to include in the response (default is '*' to return all fields).
        page : int, optional
            The page number to retrieve (default is 0).
        pagesize : int, optional
            The number of records per page (default is 0, which represents the Maximum Pagesize setting in the data
            configuration of the plugin that you are using to connect to the PowerSchool API).
        sort : str, optional
            A comma-separated list of fields by which to sort the records (default is '').
        sortdescending : bool, optional
            Flag indicating if the sorting should be in descending order (default is False).

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the requested records.
            Returns an empty DataFrame if the API is not connected or if there is an error during the request.
        """

        # Check if the API is connected before attempting to retrieve records
        if not self._api_connected:
            self._log.error(f"Records not retrieved from {table_name} because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Log the attempt to get records from the specified table
        self._log.debug(f"Getting records from {table_name}")

        # Construct the resource URL with the query expression and projection
        resource = f"/ws/schema/table/{table_name}?q={query_expression}&projection={projection}"

        # Append pagination parameters if provided
        if page > 0:
            resource += f"&page={page}"

        if pagesize > 0:
            resource += f"&pagesize={pagesize}"

        # Append sorting parameters if provided
        if sort != '':
            resource += f"&sort={sort}"

            if sortdescending:
                resource += "&sortdescending=true"

        # Send a GET request to retrieve the records
        response = self._request('get', resource=resource)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse and return the response as a DataFrame
            return self._table_parse_response(response, table_name)

        else:
            # Log an error if the request was not successful
            self._log.error(f"Error getting records from {table_name}: {response.status_code} - {response.text}")

            # Return an empty DataFrame if the request fails
            return pd.DataFrame()

    def table_get_record_count(self, table_name: str, query_expression: str) -> int:
        """
        Retrieve the count of records from a table based on a query expression.

        Parameters
        ----------
        table_name : str
            The name of the table from which to retrieve the record count.
        query_expression : str
            The query expression to filter the records.

        Returns
        -------
        int
            The count of records matching the query expression.
            Returns 0 if the API is not connected, if there is an error during the request,
            or if no count is found in the response.
        """

        # Check if the API is connected before attempting to retrieve the record count
        if not self._api_connected:
            self._log.error(f"Record count not retrieved from {table_name} because the API is not connected")

            # Return 0 if not connected
            return 0

        # Log the attempt to get the record count from the specified table
        self._log.debug(f"Getting record count from {table_name}")

        # Send a GET request to retrieve the record count using the provided query expression
        response = self._request('get', resource=f"/ws/schema/table/{table_name}/count?q={query_expression}")

        # Parse the JSON response
        response_json = response.json()

        # Check if the request was successful
        if response.status_code != 200:
            # Log an error if the request was not successful
            self._log.error(f"Error getting record count from {table_name}: {response.status_code} - {response.text}")

            # Return 0 if the query fails
            return 0

        # Check if the 'count' key is present in the response
        if 'count' in response_json:
            # Return the count of records
            return response_json['count']

        else:
            # Log a debug message if no record count was found in the response
            self._log.debug(f"No record count found in response from {table_name} with the provided query expression")

            # Return 0 if no count is found
            return 0

    # Insert records contained in the given Pandas DataFrame into the given table
    def table_insert_records(self, table_name: str, records: pd.DataFrame) -> pd.DataFrame:
        """
        Insert multiple records into a specified table.

        Parameters
        ----------
        table_name : str
            The name of the table into which records will be inserted.
        records : pd.DataFrame
            A DataFrame containing the records to be inserted. Each row represents a record.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the results of the insert operations, including response status codes and texts.
            Returns an empty DataFrame if the API is not connected or if the input DataFrame is empty.
        """

        # Check if the API is connected before attempting to insert records
        if not self._api_connected:
            self._log.error(f"Records not inserted into {table_name} because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Check if the input DataFrame is empty
        if records.empty:
            self._log.debug(self._EMPTY_DF_LOG_MSG)

            # Return an empty DataFrame if the input DataFrame is empty
            return pd.DataFrame()

        # Log the attempt to insert records into the specified table
        self._log.debug(f"Inserting records into {table_name}")

        access_requests_needed = False  # Flag to track if access requests are needed
        suppress_log = False  # Flag to suppress logging for specific cases

        # Define a function to insert a single record
        def insert_records(row):
            nonlocal access_requests_needed
            nonlocal suppress_log

            # Convert the row to JSON format, dropping any null values
            row_json = row.dropna().to_json()

            # Create the payload for the API request with the correct formatting
            payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'

            # Send a POST request to insert the record into the specified table
            response = self._request('post', resource=f"/ws/schema/table/{table_name}", read_only=False,
                                     suppress_log=suppress_log, data=payload)

            # Suppress further logging after the first request
            suppress_log = True

            # Check if access requests are needed based on the response status code
            if not access_requests_needed and response.status_code == 403:
                access_requests_needed = True

            # Store the response status code and text in the row for tracking
            row['response_status_code'] = response.status_code
            row['response_text'] = response.text

            # Return the updated row
            return row

        # Apply the insert_records function to each row in the DataFrame
        results = records.apply(insert_records, axis=1)

        # Identify rows where the response status code indicates failure (not 200)
        errors = results.loc[results['response_status_code'] != 200]

        # If there are errors, log them
        if not errors.empty:
            if not access_requests_needed:
                self._log.error(f"Errors inserting records into {table_name}\n"
                                f"{errors.to_string(index=False, justify='left')}")

        else:
            # Log a success message if all records were inserted successfully
            self._log.debug(f"{len(results.index)} record(s) successfully inserted into {table_name}")

        # Return the results DataFrame containing the status of each insert operation
        return results

    def table_update_records(self, table_name: str, id_column_name: str, records: pd.DataFrame) -> pd.DataFrame:
        """
        Update multiple records in a specified table.

        Parameters
        ----------
        table_name : str
            The name of the table in which records will be updated.
        id_column_name : str
            The name of the column that contains the unique identifier for each record.
        records : pd.DataFrame
            A DataFrame containing the records to be updated. Each row represents a record.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the results of the update operations, including response status codes and texts.
            Returns an empty DataFrame if the API is not connected, if the input DataFrame is empty,
            or if the ID column is not found in the records.
        """

        # Check if the API is connected before attempting to update records
        if not self._api_connected:
            self._log.error(f"Records not updated in {table_name} because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Check if the input DataFrame is empty
        if records.empty:
            self._log.debug(self._EMPTY_DF_LOG_MSG)

            # Return an empty DataFrame if the input DataFrame is empty
            return pd.DataFrame()

        # Check if the specified ID column exists in the DataFrame
        if id_column_name not in records.columns:
            self._log.error(f"ID column '{id_column_name}' not found in records. No records updated.")

            # Return an empty DataFrame if the ID column is not found
            return pd.DataFrame()

        # Log the attempt to update records in the specified table
        self._log.debug(f"Updating records in {table_name}")

        # Fill any NaN values in the DataFrame with empty strings
        records = records.fillna('')

        access_requests_needed = False  # Flag to track if access requests are needed
        suppress_log = False  # Flag to suppress logging for specific cases

        # Define a function to update a single record
        def update_records(row):
            nonlocal access_requests_needed
            nonlocal suppress_log

            # Convert the row to JSON format, excluding the ID column
            row_json = row.drop(id_column_name).to_json()

            # Create the payload for the API request with the correct formatting
            payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'

            # Send a PUT request to update the record in the specified table
            response = self._request('put', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}",
                                     read_only=False, suppress_log=suppress_log, data=payload)

            suppress_log = True  # Suppress further logging after the first request

            # Check if access requests are needed based on the response status code
            if response.status_code == 403 and not access_requests_needed:
                access_requests_needed = True

            # Store the response status code and text in the row for tracking
            row['response_status_code'] = response.status_code
            row['response_text'] = response.text

            # Return the updated row
            return row

        # Apply the update_records function to each row in the DataFrame
        results = records.apply(update_records, axis=1)

        # Identify rows where the response status code indicates failure (not 200)
        errors = results.loc[results['response_status_code'] != 200]

        # If there are errors, log them
        if not errors.empty:
            if not access_requests_needed:
                self._log.error(f"Errors updating records in {table_name}\n"
                                f"{errors.to_string(index=False, justify='left')}")

        else:
            # Log a success message if all records were updated successfully
            self._log.debug(f"{len(results.index)} records successfully updated in {table_name}")

        # Return the results DataFrame containing the status of each update operation
        return results

    def table_delete_record(self, table_name: str, record_id: str | int) -> bool:
        """
        Delete a specific record from a table.

        Parameters
        ----------
        table_name : str
            The name of the table from which the record will be deleted.
        record_id : str or int
            The ID of the record to delete.

        Returns
        -------
        bool
            True if the record was successfully deleted or not found; False if the API is not connected
            or if there was an error during the deletion process.
        """

        # Check if the API is connected before attempting to delete the record
        if not self._api_connected:
            self._log.error(f"Record {record_id} not deleted from {table_name} because the API is not connected")

            # Return False if not connected
            return False

        # Log the attempt to delete the specified record from the table
        self._log.debug(f"Deleting record from {table_name}")

        # Send a DELETE request to remove the record from the specified table
        response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{record_id}", read_only=False)

        # Check if the request was successful (204 No Content indicates successful deletion)
        if response.status_code == 204:
            self._log.debug(f"Record successfully deleted from {table_name}")

            # Return True if the record was successfully deleted
            return True

        # Handle the case where the record was not found (404 Not Found)
        else:
            if response.status_code == 404:
                self._log.debug(f"Record {record_id} not found in {table_name}")

                # Return True if the record was not found (considered successful)
                return True

            # Log an error for any other status codes that indicate failure
            elif response.status_code != 403:
                self._log.error(
                        f"Error deleting record from {table_name}: {response.status_code} - {response.text}")

                # Return False if there was an error during deletion
                return False

    def table_delete_records(self, table_name: str, id_column_name: str, records: pd.DataFrame) -> pd.DataFrame:
        """
        Delete multiple records from a specified table.

        Parameters
        ----------
        table_name : str
            The name of the table from which records will be deleted.
        id_column_name : str
            The name of the column that contains the unique identifier for each record.
        records : pd.DataFrame
            A DataFrame containing the records to be deleted. Each row represents a record.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the results of the delete operations, including response status codes and texts.
            Returns an empty DataFrame if the API is not connected or if the input DataFrame is empty.
        """

        # Check if the API is connected before attempting to delete records
        if not self._api_connected:
            self._log.error(f"Records not deleted from {table_name} because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Check if the input DataFrame is empty
        if records.empty:
            self._log.debug(self._EMPTY_DF_LOG_MSG)

            # Return an empty DataFrame if no records to delete
            return pd.DataFrame()

        # Log the attempt to delete records from the specified table
        self._log.debug(f"Deleting records from {table_name}")

        access_requests_needed = False  # Flag to track if access requests are needed
        suppress_log = False  # Flag to suppress logging for specific cases

        # Define a function to delete a single record
        def delete_records(row):
            nonlocal access_requests_needed
            nonlocal suppress_log

            # Send a DELETE request to remove the record identified by the ID column
            response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}",
                                     read_only=False, suppress_log=suppress_log)

            suppress_log = True  # Suppress further logging after the first request

            # Check if access requests are needed based on the response status code
            if response.status_code == 403 and not access_requests_needed:
                access_requests_needed = True

            # Store the response status code and text in the row for tracking
            row['response_status_code'] = response.status_code
            row['response_text'] = response.text

            # Return the updated row
            return row

        # Apply the delete_records function to each row in the DataFrame
        results = records.apply(delete_records, axis=1)

        # Identify rows where the response status code indicates failure (not 204)
        errors = results.loc[results['response_status_code'] != 204]

        # Split the errors DataFrame into failed and not found records
        failed = errors.loc[errors['response_status_code'] != 404]
        not_found = errors.loc[errors['response_status_code'] == 404]

        # Log details about records that were not found
        if not not_found.empty:
            self._log.debug(
                    f"Records not found in {table_name}\n{not_found.to_string(index=False, justify='left')}")

        # If there are errors, log them
        if not failed.empty:
            if not access_requests_needed:
                self._log.error(f"Errors deleting records from {table_name}\n"
                                f"{failed.to_string(index=False, justify='left')}")

        else:
            # Log success messages if all records were deleted successfully
            self._log.debug(f"{len(results.index) - len(not_found.index)} records successfully deleted from"
                            f" {table_name}")
            self._log.debug(f"{len(not_found.index)} records not found in {table_name}")

        # Return the results DataFrame containing the status of each delete operation
        return results

    def student_get(self, student_id: int) -> pd.DataFrame:
        """
        Retrieve a specific student record by student ID.

        Parameters
        ----------
        student_id : int
            The unique identifier for the student whose record is to be retrieved.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the student's record.
            Returns an empty DataFrame if the API is not connected,
            if the request fails, or if the student record is not found.
        """

        # Check if the API is connected before attempting to retrieve the record
        if not self._api_connected:
            self._log.error(f"Student with ID {student_id} not retrieved because the API is not connected")

            # Return an empty DataFrame if not connected
            return pd.DataFrame()

        # Log the attempt to get the specified record from the table
        self._log.debug(f"Getting student with ID {student_id}")

        # Send a GET request to retrieve the student record with the specified ID
        response = self._request('get',
                                 resource=f"/ws/v1/student/{student_id}")

        # Check if the request was successful
        if response.status_code != 200:
            # Log an error if the request was not successful
            self._log.error(f"Error getting student with ID {student_id}: {response.status_code} - {response.text}")

            # Return an empty DataFrame if the request fails
            return pd.DataFrame()

        # Log a message indicating that the record was found
        self._log.debug('Record found')

        # Parse the response as a JSON object
        response_json = response.json()

        # Create a dictionary with the student record
        student_record = {
            'id': response_json['student']['id'],
        }

        # Copy optional fields to the student record dictionary if they contain values
        if 'local_id' in response_json['student']:
            student_record['student_number'] = response_json['student']['local_id']

        if 'state_province_id' in response_json['student']:
            student_record['state_studentnumber'] = response_json['student']['state_province_id']

        if 'student_username' in response_json['student']:
            student_record['student_web_id'] = response_json['student']['student_username']

        for key, value in response_json['student']['name'].items():
            student_record[key] = value

        # Return the student record as a DataFrame with the index set to 0
        return pd.DataFrame(student_record, index=[0])

    def student_get_expansions(self, student_id: int) -> pd.Series:
        """
        Retrieve the expansions for a specific student by student ID.

        Parameters
        ----------
        student_id : int
            The unique identifier for the student whose expansions are to be retrieved.

        Returns
        -------
        pd.Series
            A Series containing the expansions for the student.
            Returns an empty Series if the API is not connected, if the request fails,
            or if the student record is not found.
        """

        # Check if the API is connected before attempting to retrieve the record
        if not self._api_connected:
            self._log.error(f"Student with ID {student_id} not retrieved because the API is not connected")

            # Return an empty list if not connected
            return pd.Series()

        # Log the attempt to get the specified record from the table
        self._log.debug(f"Getting expansions for student with ID {student_id}")

        # Send a GET request to retrieve the student record with the specified ID
        response = self._request('get',
                                 resource=f"/ws/v1/student/{student_id}")

        # Check if the request was successful
        if response.status_code != 200:
            # Log an error if the request was not successful
            self._log.error(f"Error getting student with ID {student_id}: {response.status_code} - {response.text}")

            # Return an empty DataFrame if the request fails
            return pd.Series()

        # Log a message indicating that the record was found
        self._log.debug('Record found')

        # Parse the response as a JSON object
        response_json = response.json()

        # Return the expansions for the student
        return pd.Series(response_json['student']['@expansions'].split(', '), name='expansions')
