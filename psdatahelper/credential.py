import json
import keyring as kr
import re
import getpass
from .log import Log
from enum import Enum
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from oauthlib.oauth2.rfc6749.errors import InvalidClientError
from requests.auth import HTTPBasicAuth


class CredentialType(Enum):
    API = 1
    ODBC = 2


class Credential:
    def __init__(self, server_address: str, plugin: str, cred_type=CredentialType.API, log=Log('credential')):
        self._plugin = plugin
        self._cred_type = cred_type
        self._log = log
        self._session = None

        self.server_name = server_address
        self.server_address = server_address
        self.fields = {}
        self.loaded = False

        if self._cred_type == CredentialType.API:
            self.server_address = re.sub(r"^https?://", "", server_address)
            self.server_address = re.sub(r"/+$", "", self.server_address)
            self.server_address = f"https://{self.server_address}"

            api_fields = {
                'client_id':     None,
                'client_secret': None,
                'access_token':  None
            }

            self.fields.update(api_fields)

            if self._plugin:
                self._load_api_credentials()

                if self.fields['client_id'] and self.fields['client_secret']:
                    self._get_api_access_token()

                    if self.fields['access_token']:
                        self._save_api_credentials()
                        self.loaded = True
                else:
                    self._log.error("Client ID or client secret not provided. Unable to store credentials.")
            else:
                self._log.error("Plugin name not provided. Unable to load credentials.")
        elif self._cred_type == CredentialType.ODBC:
            self._log.error("ODBC credential type not yet implemented")
        else:
            self._log.error("Invalid credential type specified")

    def _load_api_credentials(self):
        try:
            self.fields.update(json.loads(kr.get_password(self.server_name, self._plugin)))
        except Exception:
            self._log.debug(f"No credentials found for plugin {self._plugin} on "
                            f"{self.server_address}.")

        if not self.fields['client_id'] or not self.fields['client_secret']:
            while not self.fields['client_id']:
                self.fields['client_id'] = (
                    getpass.getpass(f"No client_id found for plugin {self._plugin} on {self.server_name}. "
                                    f"Please enter: "))

            while not self.fields['client_secret']:
                self.fields['client_secret'] = (
                    getpass.getpass(f"No client_secret found for plugin {self._plugin} on {self.server_name}. "
                                    f"Please enter: "))

            kr.set_password(self.server_name, self._plugin, json.dumps(self.fields))
            self._log.debug("Credentials stored")

    def _save_api_credentials(self):
        kr.set_password(self.server_name, self._plugin, json.dumps(self.fields))
        self._log.debug("Credentials stored")

    def _get_api_access_token(self):
        self._log.debug(f"Opening session for {self.server_name} to obtain access token")
        self._session = OAuth2Session(client=BackendApplicationClient(client_id=self.fields['client_id']))

        try:
            response = self._session.fetch_token(
                    token_url=f"{self.server_address}/oauth/access_token",
                    auth=HTTPBasicAuth(self.fields['client_id'], self.fields['client_secret'])
            )
        except InvalidClientError as e:
            self._log.error(f"Invalid client error: {e}")
        except Exception as e:
            self._log.error(f"Error fetching token: {e}")
        else:
            self.fields['access_token'] = response['access_token']
            self._log.debug("Access token obtained")

        self._session.close()

    def __repr__(self):
        if self._cred_type == CredentialType.API:
            return (f"Credential(server_name='{self.server_name}', plugin='{self._plugin}', "
                    f"cred_type='{self._cred_type}')")
        else:
            return f"Credential(server_name='{self.server_name}', '{self._cred_type}')"
