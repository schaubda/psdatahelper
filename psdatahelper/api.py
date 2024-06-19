import acme_powerschool
import pandas as pd
import smtplib
from .log import Log
from email.utils import formataddr
from email.message import EmailMessage


class API:
    def __init__(self, server_url: str, plugin: str, log=Log('ps_api')):
        self._log = log
        self._api_connected = False
        self._pq_prefix = ''
        # Initialize a dictionary for report email header
        self._report_email_header = {
            'sender_address': '',
            'sender_name':    '',
            'recipients':     '',
            'subject':        '',
            'body':           '',
            'attachments':    []
        }
        self._smtp_server = ''

        # Create an instance of the ACME PowerSchool library
        try:
            self._ps = acme_powerschool.api(server_url, plugin=plugin)
        except Exception as e:
            self._log.error(f"Error connecting to the PowerSchool API: {e}")
        else:
            self._log.debug(f"Connected to the PowerSchool API")
            self._api_connected = True

    # Set the prefix for PowerQueries
    def set_pq_prefix(self, pq_prefix: str):
        self._pq_prefix = pq_prefix
        self._log.debug(f"PowerQuery prefix set to {self._pq_prefix}")

    # Run the given PowerQuery and return the results as a Pandas DataFrame
    def run_pq(self, pq_name: str) -> pd.DataFrame:
        if self._api_connected:
            full_pq_name = ''

            if self._pq_prefix != '':
                full_pq_name = f"{self._pq_prefix}.{pq_name}"
            else:
                full_pq_name = pq_name

            self._log.debug(f"Running PQ: {full_pq_name}")

            # Send a POST request to run the PQ
            response = self._ps.post(f'ws/schema/query/{full_pq_name}?pagesize=0')

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
                    response = self._ps.post(f'ws/schema/table/{table_name}', data=payload)

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
                self._log.debug(f"Input records DataFrame is empty")

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
                        row_id = row[id_column_name]
                        row_json = row.drop(id_column_name).to_json()
                        payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'
                        response = self._ps.put(f'ws/schema/table/{table_name}/{row[id_column_name]}', data=payload)

                        row['response_status_code'] = response.status_code
                        row['response_text'] = response.text

                        return row

                    # Apply the update_records function to each row in the DataFrame
                    results = records.apply(update_records, axis=1)
                    # Get the rows where the response status code is not 200 (success)
                    errors = results.loc[results['response_status_code'] != 200]

                    # If there are errors, fall back to deleting the rows first then re-inserting them with updated data
                    if not errors.empty:
                        # Function to update a single record with delete and insert
                        # def update_records_with_delete(row):
                        #     delete_response = self._ps.delete(f'ws/schema/table/{table_name}/{row[id_column_name]}')
                        #
                        #     if delete_response.status_code == 204 or delete_response.status_code == 404:
                        #         row_json = row.dropna().to_json()
                        #         payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'
                        #         response = self._ps.post(f'ws/schema/table/{table_name}', data=payload)
                        #
                        #         row['response_status_code'] = response.status_code
                        #         row['response_text'] = response.text
                        #     else:
                        #         row['response_status_code'] = delete_response.status_code
                        #         row['response_text'] = delete_response.text
                        #
                        #     return row
                        #
                        # # Drop the response columns from the errors DataFrame
                        # errors = errors.drop(columns=['response_status_code', 'response_text'])
                        # # Apply the update_records_with_delete function to each row in the errors DataFrame
                        # results = errors.apply(update_records_with_delete, axis=1)
                        # errors = results.loc[results['response_status_code'] != 200]

                        # If there are still errors, log them
                        if not errors.empty:
                            self._log.error(
                                    f"Errors updating records in {table_name}\n"
                                    f"{errors.to_string(index=False, justify='left')}")
                        else:
                            self._log.debug(f"Records successfully updated in {table_name}")

                        return results
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
            response = self._ps.delete(f'ws/schema/table/{table_name}/{record_id}')

            if response.status_code == 204:
                self._log.debug(f"Record successfully deleted from {table_name}")

                return True
            else:
                if response.status_code == 404:
                    self._log.debug(f"Record {record_id} not found in {table_name}")

                    return True
                else:
                    self._log.error(f"Error deleting record from {table_name}: {response.status_code} - {response.text}")

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
                    response = self._ps.delete(f'ws/schema/table/{table_name}/{row[id_column_name]}')

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
                        f"Errors deleting records from {table_name}\n{failed.to_string(index=False, justify='left')}")
                else:
                    self._log.debug(f"Records successfully deleted from {table_name}")

                return results
            else:
                self._log.debug(f"Input records DataFrame is empty")

                return pd.DataFrame()
        else:
            self._log.error(f"Records not deleted from {table_name} because the API is not connected")

            return pd.DataFrame()

    # Set the SMTP server for sending emails
    def set_smtp_server(self, smtp_server):
        self._smtp_server = smtp_server

    # Set the sender address for the report email
    def set_report_sender_address(self, sender_address):
        self._report_email_header['sender_address'] = sender_address

    # Set the sender name for the report email
    def set_report_sender_name(self, sender_name):
        self._report_email_header['sender_name'] = sender_name

    # Set the recipients for the report email
    def set_report_recipients(self, recipients):
        self._report_email_header['recipients'] = recipients

    # Set the subject for the report email
    def set_report_subject(self, subject):
        self._report_email_header['subject'] = subject

    # Set the body for the report email
    def set_report_body(self, body):
        self._report_email_header['body'] = body

    # Add an attachment to the report email
    def add_report_attachment(self, attachment):
        self._report_email_header['attachments'].append(attachment)

    # Send the report email
    def send_report(self):
        if self._smtp_server != '':
            self._log.debug("Sending report")

            # Create a new EmailMessage object
            msg = EmailMessage()

            # Set the From, To, and Subject headers, and the message body
            msg['From'] = formataddr(
                    (self._report_email_header['sender_name'], self._report_email_header['sender_address']))
            msg['To'] = self._report_email_header['recipients']
            msg['Subject'] = self._report_email_header['subject']
            msg.set_content(self._report_email_header['body'])

            # Add attachments to the email
            for attachment in self._report_email_header['attachments']:
                with open(attachment, 'rb') as f:
                    file_data = f.read()
                    file_name = f.name

                msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

            try:
                # Send the email using SMTP
                with smtplib.SMTP(self._smtp_server) as s:
                    s.send_message(msg)
            except Exception as e:
                self._log.error(f"Error sending report: {e}")
            else:
                self._log.debug("Report sent successfully")
        else:
            self._log.error("No SMTP server specified")
