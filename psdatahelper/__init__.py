import acme_powerschool
import pandas as pd
import logging
import smtplib
import json
from io import StringIO
from email.utils import formataddr
from email.message import EmailMessage


class Helper:
    def __init__(self, server_url, script_name, plugin, debug=False):
        self._script_name = script_name
        self.debug = debug
        self.has_errors = False
        self._api_connected = False
        # Initialize a dictionary for report email header
        self._report_email_header = {
            'sender_address': '',
            'sender_name':    '',
            'recipients':     '',
            'subject':        '',
            'body':           '',
            'attachments':    []
        }
        # Initialize a dictionary for error email header
        self._error_email_header = {
            'sender_address': '',
            'sender_name':    '',
            'recipients':     ''
        }
        self._smtp_server = ''
        # Create a logger instance with the script name and disable propagation to higher loggers
        self.logger = logging.getLogger(script_name)
        self.logger.propagate = False

        # If the logger has handlers, clear them
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Create a formatter for log messages
        self._formatter = logging.Formatter("{levelname}: {asctime} - {message}", style="{",
                                            datefmt="%b %d %Y %I:%M:%S %p")

        # Create a file handler for logging to a file named after the script name
        self._file_handler = logging.FileHandler(f"{script_name}.log", mode="a")
        self._file_handler.setFormatter(self._formatter)

        # Create a StringIO object for logging to a string that will be used for the error report email body
        self._log_stream = StringIO()

        # Create a stream handler for logging to the StringIO object
        self._stream_handler = logging.StreamHandler(stream=self._log_stream)
        self._stream_handler.setFormatter(self._formatter)

        # If debug mode is enabled, set logging levels to DEBUG and add a console handler for logging
        # to the console
        if self.debug:
            self.logger.setLevel(logging.DEBUG)

            self._file_handler.setLevel(logging.DEBUG)
            self._stream_handler.setLevel(logging.DEBUG)

            self._console_handler = logging.StreamHandler()
            self._console_handler.setFormatter(self._formatter)
            self._console_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(self._console_handler)
        # If debug mode is disabled, set logging levels to ERROR
        else:
            self.logger.setLevel(logging.ERROR)
            self._file_handler.setLevel(logging.ERROR)
            self._stream_handler.setLevel(logging.ERROR)

        # Add the file handler and stream handlers to the logger
        self.logger.addHandler(self._file_handler)
        self.logger.addHandler(self._stream_handler)

        # Create an instance of the ACME PowerSchool library
        try:
            self._ps = acme_powerschool.api(server_url, plugin=plugin)
        except Exception as e:
            self.logger.error(f"Error connecting to the PowerSchool API: {e}")
            self.has_errors = True
        else:
            self.logger.debug(f"Connected to the PowerSchool API")
            self._api_connected = True

    # Define a method to run a PowerQuery and return the results as a pandas DataFrame
    def run_pq(self, pq_name):
        if self._api_connected:
            self.logger.debug(f"Running PQ: {pq_name}")

            # Send a POST request to run the PQ
            response = self._ps.post(f'ws/schema/query/{pq_name}?pagesize=0')

            # If the request was successful
            if response.status_code == 200:
                self.logger.debug(f"Query successful")

                # Store the response as JSON
                response_json = response.json()

                # If the response contains records
                if 'record' in response_json:
                    self.logger.debug(f"Records found: {len(response_json['record'])} record(s)")

                    # Return the records as a pandas DataFrame
                    return pd.DataFrame(response_json['record'])
                # If the response does not contain records
                else:
                    self.logger.debug(f"No records found")

                    # Return an empty DataFrame
                    return pd.DataFrame()
            # If the request was not successful
            else:
                self.logger.error(f"Query failed: {response.status_code} - {response.text}")
                self.has_errors = True

                # Return an empty DataFrame
                return pd.DataFrame()
        else:
            self.logger.error(f"PowerQuery {pq_name} not run because the API is not connected")
            self.has_errors = True

    # Define a method to insert records into a table
    def insert_table_records(self, table_name, records):
        if self._api_connected:
            self.logger.debug(f"Inserting records into {table_name}")

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
                    self.logger.error(f"Errors inserting records into {table_name}\n{errors.to_string(index=False,
                                                                                                      justify='left')}")
                    self.has_errors = True
                else:
                    self.logger.debug(f"Records successfully inserted into {table_name}")
        else:
            self.logger.error(f"Records not inserted into {table_name} because the API is not connected")
            self.has_errors = True

    # Define a method to update records in a table
    def update_table_records(self, table_name, id_column_name, records):
        if self._api_connected:
            self.logger.debug(f"Updating records in {table_name}")

            # If the records DataFrame is not empty
            if not records.empty:
                # Check if the specified ID column is in the records DataFrame
                if id_column_name in records.columns:
                    # Function to update a single record
                    def update_records(row):
                        row_json = row.dropna().to_json()
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
                        def update_records_with_delete(row):
                            delete_response = self._ps.delete(f'ws/schema/table/{table_name}/{row[id_column_name]}')

                            if delete_response.status_code == 204 or delete_response.status_code == 404:
                                row_json = row.dropna().to_json()
                                payload = f'{{"tables":{{"{table_name}":{row_json}}}}}'
                                response = self._ps.post(f'ws/schema/table/{table_name}', data=payload)

                                row['response_status_code'] = response.status_code
                                row['response_text'] = response.text
                            else:
                                row['response_status_code'] = delete_response.status_code
                                row['response_text'] = delete_response.text

                            return row

                        # Drop the response columns from the errors DataFrame
                        errors = errors.drop(columns=['response_status_code', 'response_text'])
                        # Apply the update_records_with_delete function to each row in the errors DataFrame
                        results = errors.apply(update_records_with_delete, axis=1)
                        errors = results.loc[results['response_status_code'] != 200]

                        # If there are still errors, log them
                        if not errors.empty:
                            self.logger.error(
                                    f"Errors updating records in {table_name}\n"
                                    f"{errors.to_string(index=False, justify='left')}")
                            self.has_errors = True
                        else:
                            self.logger.debug(f"Records successfully updated in {table_name}")
                    else:
                        self.logger.debug(f"Records updated in {table_name}")
                # If the specified ID column is not in the records DataFrame, log an error
                else:
                    self.logger.error(f"ID column '{id_column_name}' not found in records")
                    self.has_errors = True
        else:
            self.logger.error(f"Records not updated in {table_name} because the API is not connected")
            self.has_errors = True

    # Set the SMTP server for sending emails
    def set_smtp_server(self, smtp_server):
        self._smtp_server = smtp_server

    # Set the sender address for the report email
    def set_report_sender_address(self, sender_address):
        self._report_email_header['sender_address'] = sender_address

    # Set the sender name for the report email
    def set_report_sender_name(self, sender_name):
        self._report_email_header['sender_name'] = sender_name

    # Set the sender address for the error email
    def set_error_sender_address(self, sender_address):
        self._error_email_header['sender_address'] = sender_address

    # Set the sender name for the error email
    def set_error_sender_name(self, sender_name):
        self._error_email_header['sender_name'] = sender_name

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
            self.logger.debug("Sending report")

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
                self.logger.error(f"Error sending report: {e}")
                self.has_errors = True
            else:
                self.logger.debug("Report sent successfully")
        else:
            self.logger.error("No SMTP server specified")
            self.has_errors = True

    # Set the recipients for the error email
    def set_error_recipients(self, recipients):
        self._error_email_header['recipients'] = recipients

    # Send the error report email
    def send_error_report(self):
        if self.has_errors:
            if self._smtp_server != '':
                self.logger.debug("Sending error report")

                # Create a new EmailMessage object
                msg = EmailMessage()

                # Set the From, To, and Subject headers, and the message body
                msg['From'] = formataddr(
                        (self._error_email_header['sender_name'], self._error_email_header['sender_address']))
                msg['To'] = self._error_email_header['recipients']
                msg['Subject'] = f"Error Report for {self._script_name}"
                msg.set_content(self._log_stream.getvalue())

                try:
                    # Send the email using SMTP
                    with smtplib.SMTP(self._smtp_server) as s:
                        s.send_message(msg)
                except Exception as e:
                    self.logger.error(f"Error sending error report: {e}")
                    self.has_errors = True
                else:
                    self.logger.debug("Error report sent successfully")
            else:
                self.logger.error("No SMTP server specified")
                self.has_errors = True
        else:
            self.logger.debug("No errors to report")