import pandas as pd
import json
from .credential import Credential
from .log import Log
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2.rfc6749.errors import InvalidClientError
from requests.models import Response
from requests_oauthlib import OAuth2Session
from typing import Optional
from unittest.mock import Mock


class API:
    def __init__(self, credential: Credential, log: Log = Log('ps_api')):
        self._credential = credential
        self._log = log
        self._api_connected = False
        self._pq_prefix = ''

        if self._credential.loaded:
            try:
                self.session = OAuth2Session(
                        client=BackendApplicationClient(client_id=self._credential.fields['client_id']),
                        token={
                            'token_type':   'Bearer',
                            'access_token': self._credential.fields['access_token']
                        }
                )
            except InvalidClientError as e:
                self._log.error(f"Error connecting to the PowerSchool API: {e}")
            except Exception as e:
                self._log.error(f"Error connecting to the PowerSchool API: {e}")
            else:
                self._log.debug(f"Connected to the PowerSchool API")
                self.session.headers = {
                    'Content-Type': 'application/json',
                    'Accept':       'application/json'
                }
                self._api_connected = True
        else:
            self._log.error(f"API not connected because credentials are not loaded")

    def _request(self, method: str, resource: str, read_only: bool = True, suppress_log: bool = False, **kwargs):
        if self._api_connected:
            response = self.session.request(method=method, url=f"{self._credential.server_address}{resource}", **kwargs)

            if response.status_code == 403:
                access_requests = self._parse_access_requests(response, read_only=read_only)

                if access_requests:
                    if not suppress_log:
                        self._log.error(f"Plugin doesn't have access to one or more of the requested fields.\n"
                                        f"Access requests to add to the plugin:{''.join(access_requests)}")

                    response.access_requests = access_requests
            elif response.status_code != 200 and not suppress_log:
                if response.status_code == 400:
                    self._log.error(f"Bad request to {resource}:\n\t{response.text}")
                elif response.status_code == 401:
                    self._log.error(f"Unauthorized request to {resource}:\n\t{response.text}")
                elif response.status_code == 404:
                    self._log.error(f"Resource not found: {resource}")
                elif response.status_code == 405:
                    self._log.error(f'Method "{method}" not allowed for {resource}')
                elif response.status_code == 409:
                    self._log.error(f"Conflict with {resource}:\n\t{response.text}")
                elif response.status_code == 415:
                    self._log.error(f"Unsupported media type for {resource}:\n\t{response.text}")
                elif response.status_code == 500:
                    self._log.error(f"Internal server error for {resource}:\n\t{response.text}")
                elif response.status_code == 509:
                    self._log.error(f"Resource throttling currently in place for {resource}:\n\t{response.text}")

            return response
        else:
            if not suppress_log:
                self._log.error(f"API request not made because the API is not connected")

            response = Mock(spec=Response)
            response.status_code = 404
            response.json.return_value = {}

            return response

    def _parse_access_requests(self, response: Response, read_only: bool = True) -> list[str]:
        if response.status_code == 403:
            access_requests = []

            if read_only:
                access_level = 'ViewOnly'
            else:
                access_level = 'FullAccess'

            try:
                for error in response.json()['errors']:
                    access_requests.append(f'\n<field table="{error["resource"]}" field="{error["field"]}" '
                                           f'access="{access_level}"/>')
            except Exception as e:
                self._log.error(f"Error parsing access requests: {e}")

            access_requests.sort()

            return access_requests
        else:
            self._log.error(f"Response status code is not 403\n\t{response.status_code} - {response.text}")

            return []

    # Set the prefix for PowerQueries
    def set_pq_prefix(self, pq_prefix: str):
        self._pq_prefix = pq_prefix
        self._log.debug(f"PowerQuery prefix set to {self._pq_prefix}")

    def _parse_pq_response(self, response: Response) -> pd.DataFrame:
        # Store the response as JSON
        response_json = response.json()

        # If the response contains records
        if 'record' in response_json and 'tables' in response_json['record'][0]:
            self._log.debug(f"{len(response_json['record'])} records returned from PQ")

            records = []
            fields = {}
            tables = response_json['record'][0]['tables'].keys()

            for record in response_json['record']:
                for table in tables:
                    for key, value in record['tables'][table].items():
                        fields.update({f"{table}.{key}": value})

                records.append(fields.copy())
                fields.clear()

            return pd.DataFrame(records)
        elif 'record' in response_json:
            # Return the records as a pandas DataFrame
            return pd.DataFrame(response_json['record'])
        # If the response does not contain records
        else:
            self._log.debug('No records found')

            # Return an empty DataFrame
            return pd.DataFrame()

    # Run the given PowerQuery and return the results as a Pandas DataFrame
    def run_pq(self, pq_name: str, pq_parameters: Optional[dict] = None) -> pd.DataFrame:
        if pq_parameters is None:
            pq_parameters = dict()
        if self._api_connected:
            if self._pq_prefix != '':
                full_pq_name = f"{self._pq_prefix}.{pq_name}"
            else:
                full_pq_name = pq_name

            self._log.debug(f"Running PQ: {full_pq_name}")

            if pq_parameters is not None and len(pq_parameters) > 0:
                # Convert the parameters to JSON
                payload = json.dumps(pq_parameters)

                # Send a POST request to run the PQ with parameters
                response = self._request('post', resource=f"/ws/schema/query/{full_pq_name}?pagesize=0",
                                         data=payload)
            else:
                # Send a POST request to run the PQ
                response = self._request('post', resource=f"/ws/schema/query/{full_pq_name}?pagesize=0")

            # If the request was successful
            if response.status_code == 200:
                self._log.debug(f"Query successful")

                return self._parse_pq_response(response)
            # If the request was not successful
            else:
                self._log.error(f"Query failed. See above for response details.")

                # Return an empty DataFrame
                return pd.DataFrame()
        else:
            self._log.error(f"PowerQuery {pq_name} not run because the API is not connected")

            return pd.DataFrame()

    def get_table_record(self, table_name: str, record_id: str | int, projection: str = '') -> pd.DataFrame:
        if self._api_connected:
            if projection == '':
                projection = '*'

            self._log.debug(f"Getting record from {table_name}")

            # Send a GET request to retrieve the record
            response = self._request('get',
                                     resource=f"/ws/schema/table/{table_name}/{record_id}?projection={projection}")

            if response.status_code == 200:
                # Store the response as JSON
                response_json = response.json()

                # If the response contains requested record
                if table_name in response_json['tables']:
                    self._log.debug(f"Record found")

                    # Return the records as a pandas DataFrame
                    return pd.DataFrame(response_json['tables'][table_name], index=[0])
                # If the response does not contain requested record
                else:
                    self._log.debug(f"No records found")

                    # Return an empty DataFrame
                    return pd.DataFrame()
            else:
                self._log.error(f"Error getting record from {table_name}: {response.status_code} - {response.text}")

                return pd.DataFrame()
        else:
            self._log.error(f"Record {record_id} not retrieved from {table_name} because the API is not connected")

            return pd.DataFrame()

    def _parse_table_response(self, response: Response, table_name: str) -> pd.DataFrame:
        # Store the response as JSON
        response_json = response.json()

        # If the response contains records
        if 'record' in response_json and 'tables' in response_json['record'][0]:
            self._log.debug(f"{len(response_json['record'])} records returned from {table_name}")

            records = []
            fields = {}

            for record in response_json['record']:
                fields.update({'id': record['id']})

                for key, value in record['tables'][table_name].items():
                    fields.update({key: value})

                records.append(fields.copy())
                fields.clear()

            return pd.DataFrame(records)
        elif 'record' in response_json:
            # Return the records as a pandas DataFrame
            return pd.DataFrame(response_json['record'])
        # If the response does not contain records
        else:
            self._log.debug('No records found')

            # Return an empty DataFrame
            return pd.DataFrame()

    def get_table_records(self, table_name: str, query_expression: str, projection: str = '', page: int = 0,
                          pagesize: int = 0, sort: str = '', sortdescending: bool = False) -> pd.DataFrame:
        if self._api_connected:
            if projection == '':
                projection = '*'

            self._log.debug(f"Getting records from {table_name}")

            resource = f"/ws/schema/table/{table_name}?q={query_expression}&projection={projection}"

            if page > 0:
                resource += f"&page={page}"

            if pagesize > 0:
                resource += f"&pagesize={pagesize}"

            if sort != '':
                resource += f"&sort={sort}"

                if sortdescending:
                    resource += "&sortdescending=true"

            response = self._request('get', resource=resource)

            if response.status_code == 200:
                return self._parse_table_response(response, table_name)
            else:
                self._log.error(f"Error getting records from {table_name}: {response.status_code} - {response.text}")

                return pd.DataFrame()
        else:
            self._log.error(f"Records not retrieved from {table_name} because the API is not connected")

            return pd.DataFrame()

    # Insert records contained in the given Pandas DataFrame into the given table
    def insert_table_records(self, table_name: str, records: pd.DataFrame) -> pd.DataFrame:
        if self._api_connected:
            self._log.debug(f"Inserting records into {table_name}")

            # If the records DataFrame is not empty
            if not records.empty:
                access_requests_needed = False
                suppress_log = False

                # Define a function to insert a single record
                def insert_records(row):
                    nonlocal access_requests_needed
                    nonlocal suppress_log

                    # Convert the row to JSON, dropping any null values
                    row_json = row.dropna().to_json()

                    # Create the payload for the API request with correct formatting
                    payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'

                    # Send a POST request to insert the record
                    response = self._request('post', resource=f"/ws/schema/table/{table_name}", read_only=False,
                                             suppress_log=suppress_log, data=payload)

                    suppress_log = True

                    if not access_requests_needed and response.status_code == 403:
                        access_requests_needed = True

                    # Store the response status code and text in the row
                    row['response_status_code'] = response.status_code
                    row['response_text'] = response.text

                    # Return the row
                    return row

                # Apply the insert_records function to each row in the DataFrame
                results = records.apply(insert_records, axis=1)
                # Get the rows where the response status code is not 200 (success)
                errors = results.loc[results['response_status_code'] != 200]

                # If there are errors, log them
                if not errors.empty:
                    if not access_requests_needed:
                        self._log.error(f"Errors inserting records into {table_name}\n"
                                        f"{errors.to_string(index=False, justify='left')}")
                else:
                    self._log.debug(f"{len(results.index)} record(s) successfully inserted into {table_name}")

                return results
            else:
                self._log.debug(f"Input records DataFrame is empty. No records inserted.")

                return pd.DataFrame()
        else:
            self._log.error(f"Records not inserted into {table_name} because the API is not connected")

            return pd.DataFrame()

    # Update records contained in the given Pandas DataFrame in the given table
    def update_table_records(self, table_name: str, id_column_name: str, records: pd.DataFrame) -> pd.DataFrame:
        if self._api_connected:
            self._log.debug(f"Updating records in {table_name}")

            records = records.fillna('')

            # If the records DataFrame is not empty
            if not records.empty:
                access_requests_needed = False
                suppress_log = False

                # Check if the specified ID column is in the records DataFrame
                if id_column_name in records.columns:
                    # Function to update a single record
                    def update_records(row):
                        nonlocal access_requests_needed
                        nonlocal suppress_log

                        row_json = row.drop(id_column_name).to_json()
                        payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'
                        response = self._request('put', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}",
                                                 read_only=False, suppress_log=suppress_log, data=payload)

                        suppress_log = True

                        if response.status_code == 403 and not access_requests_needed:
                            access_requests_needed = True

                        row['response_status_code'] = response.status_code
                        row['response_text'] = response.text

                        return row

                    # Apply the update_records function to each row in the DataFrame
                    results = records.apply(update_records, axis=1)
                    # Get the rows where the response status code is not 200 (success)
                    errors = results.loc[results['response_status_code'] != 200]

                    # If there are errors, log them
                    if not errors.empty:
                        if not access_requests_needed:
                            self._log.error(f"Errors updating records in {table_name}\n"
                                            f"{errors.to_string(index=False, justify='left')}")
                    else:
                        self._log.debug(f"{len(results.index)} records successfully updated in {table_name}")

                    return results
                # If the specified ID column is not in the records DataFrame, log an error
                else:
                    self._log.error(f"ID column '{id_column_name}' not found in records")

                    return pd.DataFrame()
            else:
                self._log.debug(f"Input records DataFrame is empty")

                return pd.DataFrame()
        else:
            self._log.error(f"Records not updated in {table_name} because the API is not connected")

            return pd.DataFrame()

    # Delete a record with the given ID from the given table
    def delete_table_record(self, table_name: str, record_id: str | int) -> bool:
        if self._api_connected:
            self._log.debug(f"Deleting record from {table_name}")

            # Send a DELETE request to remove the record
            response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{record_id}", read_only=False)

            if response.status_code == 204:
                self._log.debug(f"Record successfully deleted from {table_name}")

                return True
            else:
                if response.status_code == 404:
                    self._log.debug(f"Record {record_id} not found in {table_name}")

                    return True
                elif response.status_code != 403:
                    self._log.error(
                            f"Error deleting record from {table_name}: {response.status_code} - {response.text}")

                    return False
        else:
            self._log.error(f"Record {record_id} not deleted from {table_name} because the API is not connected")

            return False

    # Delete records contained in the given Pandas DataFrame from the given table
    def delete_table_records(self, table_name: str, id_column_name: str, records: pd.DataFrame) -> pd.DataFrame:
        if self._api_connected:
            self._log.debug(f"Deleting records from {table_name}")

            if not records.empty:
                access_requests_needed = False
                suppress_log = False

                # Function to delete a single record
                def delete_records(row):
                    nonlocal access_requests_needed
                    nonlocal suppress_log

                    response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}",
                                             read_only=False, suppress_log=suppress_log)

                    suppress_log = True

                    if response.status_code == 403 and not access_requests_needed:
                        access_requests_needed = True

                    row['response_status_code'] = response.status_code
                    row['response_text'] = response.text

                    return row

                # Apply the delete_records function to each row in the DataFrame
                results = records.apply(delete_records, axis=1)

                # Separate errors into failed deletions and not found records
                errors = results.loc[results['response_status_code'] != 204]
                failed = errors.loc[errors['response_status_code'] != 404]
                not_found = errors.loc[errors['response_status_code'] == 404]

                if not not_found.empty:
                    self._log.debug(
                            f"Records not found in {table_name}\n{not_found.to_string(index=False, justify='left')}")

                if not failed.empty:
                    if not access_requests_needed:
                        self._log.error(f"Errors deleting records from {table_name}\n"
                                        f"{failed.to_string(index=False, justify='left')}")
                else:
                    self._log.debug(f"{len(results.index) - len(not_found.index)} records successfully deleted from"
                                    f" {table_name}")
                    self._log.debug(f"{len(not_found.index)} records not found in {table_name}")

                return results
            else:
                self._log.debug(f"Input records DataFrame is empty")

                return pd.DataFrame()
        else:
            self._log.error(f"Records not deleted from {table_name} because the API is not connected")

            return pd.DataFrame()

    def __del__(self):
        self._log.debug(f"Closing API session")
        self.session.close()
