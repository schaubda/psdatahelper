import logging
import smtplib
from io import StringIO
from email.utils import formataddr
from email.message import EmailMessage


class Log:
    """
    TODO: Fill in the docstring for Log
    A class that simplifies logging and error reporting.
    """

    def __init__(self, log_file_name: str, debug=False):
        """
        TODO: Fill in the docstring for Log.__init__
        Initialize the Log object.
        :param log_file_name:
        :param debug:
        """
        self._file_name = f'{log_file_name}.log'
        self._debug = debug

        self.has_errors = False
        self.text = StringIO()

        self._logger = logging.getLogger(log_file_name)

        self._formatter = logging.Formatter("{levelname}: {asctime} - {message}", style="{",
                                            datefmt="%b %d %Y %I:%M:%S %p")
        self._file_handler = logging.FileHandler(f"{self._file_name}", mode="a")
        self._text_handler = logging.StreamHandler(stream=self.text)

        self._logger.propagate = False

        if self._logger.hasHandlers():
            self._logger.handlers.clear()

        if self._debug:
            self._logger.setLevel(logging.DEBUG)
            self._file_handler.setLevel(logging.DEBUG)
            self._text_handler.setLevel(logging.DEBUG)

            self._console_handler = logging.StreamHandler()
            self._console_handler.setLevel(logging.DEBUG)
            self._console_handler.setFormatter(self._formatter)
            self._logger.addHandler(self._console_handler)
        else:
            self._logger.setLevel(logging.ERROR)
            self._file_handler.setLevel(logging.ERROR)
            self._text_handler.setLevel(logging.ERROR)

        self._file_handler.setFormatter(self._formatter)
        self._text_handler.setFormatter(self._formatter)

        self._logger.addHandler(self._file_handler)
        self._logger.addHandler(self._text_handler)

        self._smtp_server = ''
        self._email_header = {
            'sender_address': '',
            'sender_name':    '',
            'recipients':     ''
        }

        self._logger.debug("Logging initialized")

    def set_email_config(self, smtp_server: str, sender_address: str, sender_name: str, recipients: str):
        """
        TODO: Fill in the docstring for Log.set_email_config
        :param smtp_server:
        :param sender_address:
        :param sender_name:
        :param recipients:
        :return:
        """
        self._smtp_server = smtp_server
        self._email_header['sender_address'] = sender_address
        self._email_header['sender_name'] = sender_name
        self._email_header['recipients'] = recipients

        self._logger.debug(f"Email configuration set.\n\tSMTP Server: {self._smtp_server}\n\tEmail Header: "
                           f"{self._email_header}")

    def set_formatter(self, formatter: logging.Formatter):
        """
        TODO: Fill in the docstring for Log.set_formatter
        :param formatter:
        :return:
        """
        self._formatter = formatter
        self._file_handler.setFormatter(self._formatter)
        self._text_handler.setFormatter(self._formatter)

        if self._debug:
            self._console_handler.setFormatter(self._formatter)

        self._logger.debug("Formatter set")

    def debug(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.debug
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.info
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.warning
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.error
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.error(msg, *args, **kwargs)
        self.has_errors = True

    def critical(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.critical
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.critical(msg, *args, **kwargs)
        self.has_errors = True

    def log(self, level, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.log
        :param level:
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.log(level, msg, *args, **kwargs)

    def exception(self, msg: object, *args, **kwargs):
        """
        TODO: Fill in the docstring for Log.exception
        :param msg:
        :param args:
        :param kwargs:
        :return:
        """
        self._logger.exception(msg, *args, **kwargs)
        self.has_errors = True

    def send_error_report(self):
        """
        TODO: Fill in the docstring for Log.send_error_report
        :return:
        """
        if self.has_errors:
            if self._smtp_server != '':
                self._logger.debug("Sending error report")

                # Create a new EmailMessage object
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
                    self._logger.error(f"Error sending error report: {e}")
                    self.has_errors = True
                else:
                    self._logger.debug("Error report sent successfully")
            else:
                self._logger.error("No SMTP server specified")
                self.has_errors = True
        else:
            self._logger.debug("No errors to report")
