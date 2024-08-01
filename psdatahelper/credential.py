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
    """
    An enumeration to represent different types of credentials.

    Attributes
    ----------
    API : int
        Represents PowerSchool API credentials.
    ODBC : int
        Represents ODBC connection credentials.
    """
    API = 1
    ODBC = 2


class Credential:
    """
    A class used to manage credentials for PowerSchool API and ODBC connections.

    Attributes
    ----------
    server_name : str
        The name of the server.
    server_address : str
        The address of the server.
    fields : dict
        A dictionary to store credential fields.
    loaded : bool
        Indicates if the credentials have been successfully loaded.

    Methods
    -------
    __repr__()
        Return a string representation of the Credential object.
    """

    def __init__(self, server_address: str, plugin: str, cred_type=CredentialType.API, log=Log('credential')):
        """
        Initialize the Credential instance.

        Parameters
        ----------
        server_address : str
            The address of the server.
        plugin : str
            The name of the plugin.
        cred_type : CredentialType, optional
            The type of credentials (default is CredentialType.API, which represents PowerSchool API credentials).
        log : Log, optional
            The Log instance to use for logging (default is Log('credential')).
        """

        # Log an error if the server address is not provided
        if not server_address:
            log.error("Server address not provided. Unable to load credentials.")

            # Exit the function early if the server address is missing
            return

        # Initialize private attributes
        self._cred_type = cred_type
        self._log = log
        self._session = None

        # Initialize public attributes
        self.plugin = plugin
        self.server_name = server_address
        self.server_address = server_address
        self.fields = {}
        self.loaded = False

        # Check the credential type and initialize the credentials accordingly
        match self._cred_type:
            case CredentialType.API:
                self._initialize_api_credentials()

            case CredentialType.ODBC:
                self._initialize_odbc_credentials()

            # Log an error if an invalid credential type is specified
            case _:
                self._log.error("Invalid credential type specified")

    def __repr__(self):
        """
        Return a string representation of the Credential object.

        Returns
        -------
        str
            A string representation of the Credential object.
        """

        # Check if the credential type is API
        if self._cred_type == CredentialType.API:
            # Return a detailed representation including server name, plugin, and credential type
            return (f"Credential(server_name='{self.server_name}', plugin='{self.plugin}', "
                    f"cred_type='{self._cred_type}')")

        else:
            # Return a simpler representation for ODBC credentials
            return f"Credential(server_name='{self.server_name}', cred_type='{self._cred_type}')"

    def _initialize_api_credentials(self):
        # Log an error if the plugin name is not provided
        if not self.plugin:
            self._log.error("Plugin name not provided. Unable to load credentials.")

            return

        # Clean up the server address by removing protocol and trailing slashes
        self.server_address = re.sub(r"^https?://", "", self.server_address)
        self.server_address = re.sub(r"/+$", "", self.server_address)
        self.server_address = f"https://{self.server_address}"

        # Define required API fields
        api_fields = {
            'client_id':     None,
            'client_secret': None,
            'access_token':  None
        }

        # Update the fields dictionary with API fields
        self.fields.update(api_fields)

        # Load API credentials from secure storage
        self._load_api_credentials()

        # Check if client ID and secret are provided
        if self.fields['client_id'] and self.fields['client_secret']:
            self._get_api_access_token()

            # If access token is obtained, save credentials and mark as loaded
            if self.fields['access_token']:
                self._save_api_credentials()
                self.loaded = True

        else:
            # Log an error if client ID or secret is missing
            self._log.error("Client ID or client secret not provided. Unable to store credentials.")

    def _initialize_odbc_credentials(self):
        # Log an error to indicate ODBC credentials are not yet implemented
        self._log.error("ODBC credential type not yet implemented")

    def _load_api_credentials(self):
        # Attempt to load API credentials from a secure storage
        try:
            self.fields.update(json.loads(kr.get_password(self.server_name, self.plugin)))

        except Exception:
            # Log a debug message if no credentials are found for the specified plugin
            self._log.debug(f"No credentials found for plugin {self.plugin} on "
                            f"{self.server_address}.")

        # Check if client_id and client_secret are missing
        if not self.fields['client_id'] or not self.fields['client_secret']:
            # Prompt for client_id if it is not provided
            while not self.fields['client_id']:
                self.fields['client_id'] = (
                    getpass.getpass(f"No client_id found for plugin {self.plugin} on {self.server_name}. "
                                    f"Please enter: "))

            # Prompt for client_secret if it is not provided
            while not self.fields['client_secret']:
                self.fields['client_secret'] = (
                    getpass.getpass(f"No client_secret found for plugin {self.plugin} on {self.server_name}. "
                                    f"Please enter: "))

            # Store the entered credentials securely
            kr.set_password(self.server_name, self.plugin, json.dumps(self.fields))
            # Log a debug message indicating that credentials have been stored
            self._log.debug("Credentials stored")

    def _save_api_credentials(self):
        # Store the API credentials securely using the specified server name and plugin
        kr.set_password(self.server_name, self.plugin, json.dumps(self.fields))

        # Log a debug message indicating that the credentials have been successfully stored
        self._log.debug("Credentials stored")

    def _get_api_access_token(self):
        # Log the attempt to open a session for obtaining the access token
        self._log.debug(f"Opening session for {self.server_name} to obtain access token")

        # Create an OAuth2 session using the client ID from the stored fields
        self._session = OAuth2Session(client=BackendApplicationClient(client_id=self.fields['client_id']))

        try:
            # Fetch the access token from the specified token URL using HTTP Basic Authentication
            response = self._session.fetch_token(
                    token_url=f"{self.server_address}/oauth/access_token",
                    auth=HTTPBasicAuth(self.fields['client_id'], self.fields['client_secret'])
            )

        except InvalidClientError as e:
            # Log an error if there is an invalid client error during token fetching
            self._log.error(f"Invalid client error: {e}")

        except Exception as e:
            # Log any other errors that occur while fetching the token
            self._log.error(f"Error fetching token: {e}")

        else:
            # Store the obtained access token in the fields
            self.fields['access_token'] = response['access_token']
            # Log a debug message indicating that the access token has been successfully obtained
            self._log.debug("Access token obtained")

        # Close the session after the operation is complete
        self._session.close()
