import logging
import smtplib
from io import StringIO
from email.utils import formataddr
from email.message import EmailMessage


class Log:
    """
    A class used to handle logging and error reporting.

    Attributes
    ----------
    has_errors : bool
        Indicates if any errors have been logged.
    text : StringIO
        Contains all the messages logged in the current session.

    Methods
    -------
    set_email_config(smtp_server, sender_address, sender_name, recipients)
        Set the email configuration for sending error reports.
    set_formatter(formatter)
        Set the formatter for the log handlers.
    debug(msg, *args, **kwargs)
        Log a message with severity 'DEBUG'.
    info(msg, *args, **kwargs)
        Log a message with severity 'INFO'.
    warning(msg, *args, **kwargs)
        Log a message with severity 'WARNING'.
    error(msg, *args, **kwargs)
        Log a message with severity 'ERROR' and set has_errors to True.
    critical(msg, *args, **kwargs)
        Log a message with severity 'CRITICAL' and set has_errors to True.
    log(level, msg, *args, **kwargs)
        Log a message with a specified severity level.
    exception(msg, *args, **kwargs)
        Log an exception with severity 'ERROR' and set has_errors to True.
    send_error_report()
        Send an error report via email if any errors have been logged.
    """

    def __init__(self, log_file_name: str, debug=False):
        """
        Initialize the Log instance.

        Parameters
        ----------
        log_file_name : str
            The name of the log file without any file extension.
        debug : bool, optional
            If True, set the log level to DEBUG. Otherwise, set it to ERROR (default is False).
        """

        # Set the log file name and debug mode
        self._file_name = f'{log_file_name}.log'
        self._debug = debug

        # Initialize error tracking and text storage
        self.has_errors = False
        self.text = StringIO()

        # Create a logger with the specified name
        self._logger = logging.getLogger(log_file_name)

        # Define the log message format
        self._formatter = logging.Formatter("{levelname}: {asctime} - {message}", style="{",
                                            datefmt="%b %d %Y %I:%M:%S %p")

        # Set up file and stream handlers for logging
        self._file_handler = logging.FileHandler(f"{self._file_name}", mode="a")
        self._text_handler = logging.StreamHandler(stream=self.text)

        # Prevent log messages from being propagated to the root logger
        self._logger.propagate = False

        # Clear existing handlers if any
        if self._logger.hasHandlers():
            self._logger.handlers.clear()

        # Configure logging levels based on debug mode
        if self._debug:
            self._logger.setLevel(logging.DEBUG)
            self._file_handler.setLevel(logging.DEBUG)
            self._text_handler.setLevel(logging.DEBUG)

            # Add console handler for debug output
            self._console_handler = logging.StreamHandler()
            self._console_handler.setLevel(logging.DEBUG)
            self._console_handler.setFormatter(self._formatter)
            self._logger.addHandler(self._console_handler)

        else:
            self._logger.setLevel(logging.ERROR)
            self._file_handler.setLevel(logging.ERROR)
            self._text_handler.setLevel(logging.ERROR)

        # Apply the formatter to the file and text handlers
        self._file_handler.setFormatter(self._formatter)
        self._text_handler.setFormatter(self._formatter)

        # Add the handlers to the logger
        self._logger.addHandler(self._file_handler)
        self._logger.addHandler(self._text_handler)

        # Initialize SMTP server and email header information
        self._smtp_server = ''
        self._email_header = {
            'sender_address': '',
            'sender_name':    '',
            'recipients':     ''
        }

        # Log initialization message
        self._logger.debug("Logging initialized")

    def set_email_config(self, smtp_server: str, sender_address: str, sender_name: str, recipients: str):
        """
        Set the email configuration for sending error reports.

        Parameters
        ----------
        smtp_server : str
            The SMTP server address.
        sender_address : str
            The sender's email address.
        sender_name : str
            The sender's name.
        recipients : str
            The recipient email addresses, either a single address or comma-separated list.
        """

        # Assign the provided SMTP server and email header details
        self._smtp_server = smtp_server
        self._email_header['sender_address'] = sender_address
        self._email_header['sender_name'] = sender_name
        self._email_header['recipients'] = recipients

        # Log the updated email configuration for debugging purposes
        self._logger.debug(f"Email configuration set.\n\tSMTP Server: {self._smtp_server}\n\tEmail Header: "
                           f"{self._email_header}")

    def set_formatter(self, formatter: logging.Formatter):
        """
        Set the formatter for the log handlers to override psdatahelper.Log's default formatting.

        Parameters
        ----------
        formatter : logging.Formatter
            The formatter to use for the log handlers.
        """

        # Assign the provided formatter to the instance variable
        self._formatter = formatter

        # Apply the formatter to the file and text handlers
        self._file_handler.setFormatter(self._formatter)
        self._text_handler.setFormatter(self._formatter)

        # If in debug mode, also apply the formatter to the console handler
        if self._debug:
            self._console_handler.setFormatter(self._formatter)

        # Log a message indicating that the formatter has been set
        self._logger.debug("Formatter set")

    def debug(self, msg: object, *args, **kwargs):
        """
        Log a message with severity 'DEBUG'.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the DEBUG level using the internal logger
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: object, *args, **kwargs):
        """
        Log a message with severity 'INFO'.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the INFO level using the internal logger
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: object, *args, **kwargs):
        """
        Log a message with severity 'WARNING'.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the WARNING level using the internal logger
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: object, *args, **kwargs):
        """
        Log a message with severity 'ERROR' and set has_errors to True.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the ERROR level using the internal logger
        self._logger.error(msg, *args, **kwargs)

        # Indicate that an error has occurred by setting the has_errors flag to True
        self.has_errors = True

    def critical(self, msg: object, *args, **kwargs):
        """
        Log a message with severity 'CRITICAL' and set has_errors to True.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the CRITICAL level using the internal logger
        self._logger.critical(msg, *args, **kwargs)

        # Indicate that an error has occurred by setting the has_errors flag to True
        self.has_errors = True

    def log(self, level, msg: object, *args, **kwargs):
        """
        Log a message with a specified severity level.

        Parameters
        ----------
        level : int
            The severity level at which to log the message.
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the message at the specified severity level using the internal logger
        self._logger.log(level, msg, *args, **kwargs)

    def exception(self, msg: object, *args, **kwargs):
        """
        Log an exception with severity 'ERROR' and set has_errors to True.

        Parameters
        ----------
        msg : object
            The message to log.
        *args
            Variable length argument list.
        **kwargs
            Arbitrary keyword arguments.
        """

        # Log the exception message at the ERROR level using the internal logger
        self._logger.exception(msg, *args, **kwargs)

        # Indicate that an error has occurred by setting the has_errors flag to True
        self.has_errors = True

    def send_error_report(self):
        """
        Send an error report via email if any errors have been logged.

        Raises
        ------
        Exception
            If there is an error sending the error report email.
        """

        # Check if there are any errors to report
        if not self.has_errors:
            self._logger.debug("No errors to report")

            # If no errors are logged, return without sending an email
            return

        # Ensure that an SMTP server is configured
        if not self._smtp_server:
            # Log an error if no SMTP server is specified
            self._logger.error("No SMTP server specified")
            self.has_errors = True

            # If no SMTP server is specified, return without sending an email
            return

        if not self._email_header['sender_address'] or not self._email_header['recipients']:
            # Log an error if sender address or recipients are missing
            self._logger.error("Sender address or recipients not specified")
            self.has_errors = True

            # If sender address or recipients are missing, return without sending an email
            return

        self._logger.debug("Sending error report")

        # Create a new EmailMessage object for the email
        msg = EmailMessage()

        # Set the From, To, and Subject headers, and the message body
        msg['From'] = formataddr(
                (self._email_header['sender_name'], self._email_header['sender_address']))
        msg['To'] = self._email_header['recipients']
        msg['Subject'] = f"Error Report for {self._file_name}"
        msg.set_content(self.text.getvalue())

        try:
            # Send the email using SMTP
            with smtplib.SMTP(self._smtp_server) as s:
                s.send_message(msg)

        except Exception as e:
            # Log an error if sending the email fails
            self._logger.error(f"Error sending error report: {e}")
            self.has_errors = True

        else:
            # Log success if the email was sent successfully
            self._logger.debug("Error report sent successfully")
