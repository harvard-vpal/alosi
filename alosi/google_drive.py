from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
from pandas import read_csv
from pandas.compat import StringIO
import gspread

# default scope allows full access to google drive
SCOPES = ['https://www.googleapis.com/auth/drive']


def get_google_credentials_oauth2(client_secrets_file, port=5555, scopes=SCOPES):

    """
    Get google credentials via oauth flow
    Opens up a browser for user to sign in
    Requires client secrets file (select application type = 'other' when creating client in google console)
    Sets default local web server port to 5555, since 8080 not usually available locally, but port can be specified as
    an arg, for example if you are already using port 5555 for something

    :param client_secrets_file: (str) location of client secrets file (e.g. '/path/to/file.json`)
    :param port: port to use for local webserver to listen on for auth response
    :param scope: (list) auth scope, e.g. ['https://www.googleapis.com/auth/drive']
    :return: (google.oauth2.credentials.Credentials) google credentials object
    """
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes=scopes)

    return flow.run_local_server(port=port)


def get_google_credentials_service_account(credential_file=None, scopes=SCOPES):
    """
    Get credentials using a service account credential file

    :param credential_file: (str) location of service account credential file (e.g. '/path/to/file.json`)
    :param scopes: (list) auth scope, e.g. ['https://www.googleapis.com/auth/drive']
    :return: (google.auth.service_account.Credentials) google credentials object
    """
    if not credential_file:
        credential_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not credential_file:
        raise Exception("Service account file not found (Is GOOGLE_APPLICATION_CREDENTIALS env var set?)")
    return service_account.Credentials.from_service_account_file(credential_file, scopes=scopes)


def get_google_sheet_df(file_id=None, worksheet_title=None, credentials=None):
    """
    Get google sheet as pandas dataframe, using authenticated request to
    https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}
    Expects worksheet data to be reasonably table-like

    :param file_id: drive file id
    :param worksheet_title: (str) title of spreadsheet, defaults to first spreadsheet if not specified
    :param credentials: google-auth credentials object
    :return: (pd.DataFrame) worksheet data as pandas DataFrame
    """
    worksheet_id = get_worksheet_id(file_id, worksheet_title, credentials)
    url = "https://docs.google.com/spreadsheets/d/{id}/export?format=csv&id={id}&gid={gid}".format(
        id=file_id,gid=worksheet_id
    )
    response = AuthorizedSession(credentials).get(url)
    df = read_csv(StringIO(response.text))
    return df


def get_worksheet_id(file_id, worksheet_title=None, credentials=None):
    gc = gspread.Client(auth=credentials)
    gc.session = AuthorizedSession(credentials)
    sheet = gc.open_by_key(file_id)
    if worksheet_title:
        worksheet = sheet.worksheet(worksheet_title)
    else:
        worksheet = sheet.get_worksheet(0)
    return worksheet.id
