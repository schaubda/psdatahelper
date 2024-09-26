import smtplib
from .log import Log
from email.utils import formataddr
from email.message import EmailMessage


class Report:
    """
    A class used to generate and send email reports.

    Methods
    -------
    set_email_settings(smtp_server, sender_address, sender_name, recipients)
        Set the email configuration for sending reports.
    set_email_subject(subject)
        Set the subject for the report email.
    set_report_body(body)
        Set the body content for the report email.
    add_attachment(attachment)
        Add an attachment to the report email.
    send_email()
        Send the report email with any attachments.
    """

    def __init__(self, log=Log('reporting')):
        """
        Initialize the Report instance.

        Parameters
        ----------
        log : Log, optional
            The Log instance to use for logging (default is Log('reporting')).
        """

        # Store the provided log instance for logging purposes
        self._log = log

        # Initialize email settings with default empty values
        self._email_settings = {
            'smtp_server':    '',
            'sender_address': '',
            'sender_name':    '',
            'recipients':     '',
        }

        # Initialize email header information
        self._email_header = {
            'subject':     '',
            'attachments': []
        }

        # Initialize the body of the report as an empty string
        self._report_body = ''

        # Log a debug message indicating that the reporting instance has been initialized
        self._log.debug("Reporting initialized")

    # TODO: Rename to set_email_config for consistency with Log class
    def set_email_settings(self, smtp_server: str, sender_address: str, sender_name: str,
                           recipients: str):
        """
        Set the email configuration for sending reports.

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

        # Update the email settings with the provided parameters
        self._email_settings['smtp_server'] = smtp_server
        self._email_settings['sender_address'] = sender_address
        self._email_settings['sender_name'] = sender_name
        self._email_settings['recipients'] = recipients

        # Log the updated email configuration for debugging purposes
        self._log.debug(
            f"Email configuration set.\n\tSMTP Server: {self._email_settings['smtp_server']}\n"
            f"\tSender address: {self._email_settings['sender_address']}\n"
            f"\tSender name: {self._email_settings['sender_name']}\n"
            f"\tRecipients: {self._email_settings['recipients']}")

    def set_email_subject(self, subject: str):
        """
        Set the subject for the report email.

        Parameters
        ----------
        subject : str
            The subject of the report email.
        """

        # Update the email header with the provided subject
        self._email_header['subject'] = subject

        # Log the updated email subject for debugging purposes
        self._log.debug(f"Email subject set.\n\tSubject: {self._email_header['subject']}")

    def set_report_body(self, body: str):
        """
        Set the body content for the report email.

        Parameters
        ----------
        body : str
            The body content of the report email.
        """

        # Update the report body with the provided content
        self._report_body = body

        # Log a debug message indicating that the report body has been set
        self._log.debug('Report body set.')

    def add_attachment(self, attachment: str):
        """
        Add an attachment to the report email.

        Parameters
        ----------
        attachment : str
            The file path and name of the attachment to be added to the report email.
        """

        # Append the provided attachment file path to the list of attachments in the email header
        self._email_header['attachments'].append(attachment)

        # Log a debug message indicating that the attachment has been added
        self._log.debug(f"Attachment added.\n\tAttachment: {attachment}")

    def send_email(self):
        """
        Send the report email with any attachments.

        Raises
        ------
        Exception
            If there is an error sending the report email.
        """

        # Flag to track if any email settings are missing
        bad_settings = False

        # Check each email setting for completeness
        for setting_name, setting_value in self._email_settings.items():
            if len(setting_value) == 0:
                bad_settings = True

                # Log an error for each missing email setting
                self._log.error(f"Email setting not set: {setting_name}")

        if bad_settings:
            # Log an error if any email settings are missing
            self._log.error("Report not sent due to missing email settings")

            # Exit the method early if any email settings are missing
            return

        self._log.debug("Sending report")

        # Create a new EmailMessage object for the email
        email_message = EmailMessage()

        # Set the email headers and content using the configured settings
        email_message['From'] = formataddr(
                (self._email_settings['sender_name'], self._email_settings['sender_address']))
        email_message['To'] = self._email_settings['recipients']
        email_message['Subject'] = self._email_header['subject']
        email_message.set_content(self._report_body)

        # Add any specified attachments to the email
        for attachment in self._email_header['attachments']:
            try:
                # Open the attachment file and read its content
                with open(attachment, 'rb') as file:
                    email_message.add_attachment(file.read(), maintype='application',
                                                 subtype='octet-stream',
                                                 filename=attachment)

            except FileNotFoundError:
                # Log an error if the attachment file is not found
                self._log.error(f"Attachment not found: {attachment}")

            else:
                # Log a debug message indicating the attachment was loaded successfully
                self._log.debug(f"Attachment loaded: {attachment}")

        # Attempt to send the email using the configured SMTP server
        try:
            with smtplib.SMTP(self._email_settings['smtp_server']) as smtp:
                smtp.send_message(email_message)

        except Exception as e:
            # Log any exceptions that occur during the sending process
            self._log.exception(f"Error sending report: {e}")

        else:
            # Log a debug message indicating the report was sent successfully
            self._log.debug("Report sent")
