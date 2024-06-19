import smtplib
from .log import Log
from email.utils import formataddr
from email.message import EmailMessage


class Report:
    def __init__(self, log=Log('reporting')):
        self._log = log
        self._email_message = EmailMessage()
        self._email_settings = {
            'smtp_server': '',
            'sender_address': '',
            'sender_name': '',
            'recipients': '',
        }
        self._email_header = {
            'subject':        '',
            'attachments':    []
        }
        self._report_body = ''

        self._log.debug("Reporting initialized")

    def set_email_settings(self, smtp_server: str, sender_address: str, sender_name: str, recipients: str):
        self._email_settings['smtp_server'] = smtp_server
        self._email_settings['sender_address'] = sender_address
        self._email_settings['sender_name'] = sender_name
        self._email_settings['recipients'] = recipients

        self._log.debug(f"Email configuration set.\n\tSMTP Server: {self._email_settings['smtp_server']}\n"
                        f"\tSender address: {self._email_settings['sender_address']}\n"
                        f"\tSender name: {self._email_settings['sender_name']}\n"
                        f"\tRecipients: {self._email_settings['recipients']}")

    def set_email_subject(self, subject: str):
        self._email_header['subject'] = subject

        self._log.debug(f"Email subject set.\n\tSubject: {self._email_header['subject']}")

    def set_report_body(self, body: str):
        self._report_body = body

        self._log.debug(f"Report body set.")

    def add_attachment(self, attachment: str):
        self._email_header['attachments'].append(attachment)

        self._log.debug(f"Attachment added.\n\tAttachment: {attachment}")

    def send(self):
        bad_settings = False

        for setting_name, setting_value in self._email_settings.items():
            if len(setting_value) == 0:
                bad_settings = True
                self._log.error(f"Email setting not set: {setting_name}")

        if not bad_settings:
            self._log.debug("Sending report")

            self._email_message['From'] = formataddr(
                (self._email_header['sender_name'], self._email_header['sender_address']))
            self._email_message['To'] = self._email_header['recipients']
            self._email_message['Subject'] = self._email_header['subject']
            self._email_message.set_content(self._email_header['body'])

            for attachment in self._email_header['attachments']:
                try:
                    with open(attachment, 'rb') as file:
                        self._email_message.add_attachment(file.read(), maintype='application', subtype='octet-stream',
                                                           filename=attachment)
                except FileNotFoundError:
                    self._log.error(f"Attachment not found: {attachment}")
                else:
                    self._log.debug(f"Attachment loaded: {attachment}")

            try:
                with smtplib.SMTP(self._email_settings['smtp_server']) as smtp:
                    smtp.send_message(self._email_message)
            except Exception as e:
                self._log.exception(f"Error sending report: {e}")
            else:
                self._log.debug("Report sent")
        else:
            self._log.error("Report not sent due to missing email settings")
