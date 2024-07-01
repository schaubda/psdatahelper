import pandas as pd
import xml.etree.ElementTree as ET
from .log import Log
from .credential import Credential
from requests_oauthlib import OAuth2Session
from unittest.mock import Mock
from requests.models import Response
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2.rfc6749.errors import InvalidClientError


class API:
    def __init__(self, credential: Credential, log=Log('ps_api')):
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

    def _request(self, method: str, resource: str, **kwargs):
        if self._api_connected:
            response = self.session.request(method=method, url=f"{self._credential.server_address}{resource}", **kwargs)

            if response.status_code == 401:
                self._log.error(f"Unauthorized request to {resource}:\n\t{response.text}")
            elif response.status_code == 403:
                access_requests = []

                try:
                    if response.headers["Content-Type"] == 'application/json':
                        for error in response.json()['errors']:
                            access_requests.append(error['field'])

                    if response.headers["Content-Type"] == 'application/xml':
                        tree = ET.ElementTree(ET.fromstring(response.text))
                        root = tree.getroot()

                        for field in root.findall('./errors/field'):
                            access_requests.append(field.text)

                    access_requests = sorted([f"<field table='{field.split('.')[0]}' field='{field.split('.')[1]}' "
                                              f"access='ViewOnly' />\n" for field in access_requests])

                    self._log.error(f"No access to field. Access requests:\n{access_requests}")
                except Exception as e:
                    self._log.error(f"Error parsing access requests: {e}")
                else:
                    response.access_requests = access_requests

            return response
        else:
            self._log.error(f"API request not made because the API is not connected")

            response = Mock(spec=Response)
            response.status_code = 404
            response.json.return_value = {}

            return response

    # Set the prefix for PowerQueries
    def set_pq_prefix(self, pq_prefix: str):
        self._pq_prefix = pq_prefix
        self._log.debug(f"PowerQuery prefix set to {self._pq_prefix}")

    # Run the given PowerQuery and return the results as a Pandas DataFrame
    def run_pq(self, pq_name: str) -> pd.DataFrame:
        if self._api_connected:
            if self._pq_prefix != '':
                full_pq_name = f"{self._pq_prefix}.{pq_name}"
            else:
                full_pq_name = pq_name

            self._log.debug(f"Running PQ: {full_pq_name}")

            # Send a POST request to run the PQ
            response = self._request('post', resource=f"/ws/schema/query/{full_pq_name}?pagesize=0")

            # If the request was successful
            if response.status_code == 200:
                self._log.debug(f"Query successful")

                # Store the response as JSON
                response_json = response.json()

                # If the response contains records
                if 'record' in response_json:
                    self._log.debug(f"Records found: {len(response_json['record'])} record(s)")

                    # Return the records as a pandas DataFrame
                    return pd.DataFrame(response_json['record'])
                # If the response does not contain records
                else:
                    self._log.debug(f"No records found")

                    # Return an empty DataFrame
                    return pd.DataFrame()
            # If the request was not successful
            else:
                self._log.error(f"Query failed: {response.status_code} - {response.text}")

                # Return an empty DataFrame
                return pd.DataFrame()
        else:
            self._log.error(f"PowerQuery {pq_name} not run because the API is not connected")

            return pd.DataFrame()

    # Insert records contained in the given Pandas DataFrame into the given table
    def insert_table_records(self, table_name: str, records: pd.DataFrame) -> pd.DataFrame:
        if self._api_connected:
            self._log.debug(f"Inserting records into {table_name}")

            # If the records DataFrame is not empty
            if not records.empty:
                # Define a function to insert a single record
                def insert_records(row):
                    # Convert the row to JSON, dropping any null values
                    row_json = row.dropna().to_json()

                    # Create the payload for the API request with correct formatting
                    payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'

                    # Send a POST request to insert the record
                    response = self._request('post', resource=f"/ws/schema/table/{table_name}", data=payload)

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
                    self._log.error(f"Errors inserting records into {table_name}\n{errors.to_string(index=False,
                                                                                                    justify='left')}")
                else:
                    self._log.debug(f"Records successfully inserted into {table_name}")

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
                # Check if the specified ID column is in the records DataFrame
                if id_column_name in records.columns:
                    # Function to update a single record
                    def update_records(row):
                        row_json = row.drop(id_column_name).to_json()
                        payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'
                        response = self._request('put', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}",
                                                 data=payload)

                        row['response_status_code'] = response.status_code
                        row['response_text'] = response.text

                        return row

                    # Apply the update_records function to each row in the DataFrame
                    results = records.apply(update_records, axis=1)
                    # Get the rows where the response status code is not 200 (success)
                    errors = results.loc[results['response_status_code'] != 200]

                    # If there are errors, log them
                    if not errors.empty:
                        self._log.error(
                                f"Errors updating records in {table_name}\n"
                                f"{errors.to_string(index=False, justify='left')}")
                    else:
                        self._log.debug(f"Records successfully updated in {table_name}")

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
            response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{record_id}")

            if response.status_code == 204:
                self._log.debug(f"Record successfully deleted from {table_name}")

                return True
            else:
                if response.status_code == 404:
                    self._log.debug(f"Record {record_id} not found in {table_name}")

                    return True
                else:
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
                # Function to delete a single record
                def delete_records(row):
                    response = self._request('delete', resource=f"/ws/schema/table/{table_name}/{row[id_column_name]}")

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
                    self._log.error(
                            f"Errors deleting records from {table_name}\n"
                            f"{failed.to_string(index=False, justify='left')}")
                else:
                    self._log.debug(f"Records successfully deleted from {table_name}")

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
