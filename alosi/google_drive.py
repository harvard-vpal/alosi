from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
from pandas import read_csv
from pandas.compat import StringIO
import gspread
import os

# default scope allows full access to google drive
SCOPES = ['https://www.googleapis.com/auth/drive']


def get_oauth2_credentials(client_secrets_file, port=5555, scopes=SCOPES):

    """
    Get google credentials via oauth flow
    Opens up a browser for user to sign in
    Requires client secrets file (select application type = 'other' when creating client in google console)
    Sets default local web server port to 5555, since 8080 not usually available locally, but port can be specified as
    an arg, for example if you are already using port 5555 for something

    :param client_secrets_file: (str) location of client secrets file (e.g. '/path/to/file.json`)
    :param port: port to use for local webserver to listen on for auth response
    :param scopes: (list) auth scope, e.g. ['https://www.googleapis.com/auth/drive']
    :return: (google.oauth2.credentials.Credentials) google credentials object
    """
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes=scopes)
    return flow.run_local_server(port=port)


def get_service_account_credentials(credential_file=None, scopes=SCOPES):
    """
    Get credentials using a service account credential file
    Supports retrieving credential file location from GOOGLE_APPLICATION_CREDENTIALS env variable

    :param credential_file: (str) location of service account credential file (e.g. '/path/to/file.json`)
    :param scopes: (list) auth scope, e.g. ['https://www.googleapis.com/auth/drive']
    :return: (google.auth.service_account.Credentials) google credentials object
    """
    if not credential_file:
        credential_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_file:
        raise Exception("Service account file not found (Is GOOGLE_APPLICATION_CREDENTIALS env var set?)")
    return service_account.Credentials.from_service_account_file(credential_file, scopes=scopes)


def export_sheet_to_dataframe(file_id, credentials, worksheet_title=None, encoding='utf-8'):
    """
    Get google sheet as pandas dataframe, using authenticated request to
    https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}
    Expects worksheet data to be reasonably table-like

    :param file_id: drive file id
    :param credentials: google-auth credentials object
    :param worksheet_title: (str) title of spreadsheet, defaults to getting first spreadsheet if not specified
    :param encoding: character encoding system, e.g. 'utf-8'
    :return: (pd.DataFrame) worksheet data as pandas DataFrame
    """
    worksheet_id = _get_worksheet_id(file_id, credentials, worksheet_title=worksheet_title)
    url = "https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}".format(
        id=file_id, gid=worksheet_id
    )
    response = AuthorizedSession(credentials).get(url)
    response.encoding = encoding
    df = read_csv(StringIO(response.text))
    return df


def _get_worksheet_id(file_id, credentials=None, worksheet_title=None):
    """
    Retrieve Google Sheet worksheet id from worksheet title

    :param file_id: drive file id
    :param credentials: google-auth credentials object
    :param worksheet_title: (str) title of spreadsheet, defaults to returning id of first spreadsheet if not specified
    :return: spreadsheet id
    """
    gc = gspread.Client(auth=credentials)
    gc.session = AuthorizedSession(credentials)
    sheet = gc.open_by_key(file_id)
    if worksheet_title:
        worksheet = sheet.worksheet(worksheet_title)
    else:
        worksheet = sheet.get_worksheet(0)
    return worksheet.id
