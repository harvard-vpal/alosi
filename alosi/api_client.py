import requests
import pprint
from json import JSONDecodeError

class ApiClient:
    """
    Base class for api clients
    """
    api_path = None  # populated in subclass
    trailing_slash = False  # override in subclass if needed

    def __init__(self, host, token):
        self.host = host
        self.base_url = self.host + self.api_path
        self.client = self._get_client(token)

    @staticmethod
    def _get_client(token=None):
        """
        Constructs a request.Session with auth header
        :param token: API key
        :rtype: requests.Session
        """
        client = requests.Session()
        headers = {'Authorization': 'Token {}'.format(token)} if token else {}
        client.headers.update(headers)
        return client

    def _absolute_url(self, path):
        """
        Construct absolute url given a relative api path
        :param path: endpoint path (may or may not have starting slash)
        :return: absolute url
        :rtype: str
        """
        url = self.base_url + '/' + path.strip('/')
        if self.trailing_slash:
            url += '/'
        return url

    def prepare(self, method, path, **kwargs):
        """
        Construct a request but don't send
        :param method:
        :param path:
        :param kwargs:
        :return:
        """
        url = self._absolute_url(path)
        request = requests.Request(method, url, **kwargs)
        return self.client.prepare_request(request)

    def request(self, method, path, **kwargs):
        """
        Makes a generic request using client and base url
        :param method: HTTP method, e.g. 'GET'
        :param path: endpoint path, e.g. 'activity' or '/knowledge_component'
        :param kwargs: keyword arguments to pass to requests.request()
        :rtype: requests.Response
        """
        request = self.prepare(method, path, **kwargs)
        return self.client.send(request)


class ApiError(Exception):
    def __init__(self, response, message=''):
        self.response = response
        self.message = message

    def __str__(self):
        try:
            pp = pprint.PrettyPrinter(indent=2)
            response_data = pp.pformat(self.response.json())
        except JSONDecodeError:
            response_data = self.response.text

        return (
            f"{self.message}\n"
            f"Request: \n"
            f"{self.response.request.method} {self.response.request.url}\n"
            f"Response: {self.response.status_code}\n"
            f"{response_data}"
        )


def _log_request(request, response):
    """Log info about a HTTP request
    
    :param request: Requests request
    :param response: Requests response. Optional for dry runs where response not available
    :param response: [type], optional
    :raises ApiError: [description]
    """

    request_metadata = [request.method, request.url]  #f'{request.method} {request.url}'
    request_data = request.body if request.method in ['POST', 'PUT'] else None
    # if response:
    request_metadata.append(str(response.status_code))

    # determine logging level based on response status
    if response.ok:
        log_level = log.debug
    else:
        log_level = log.error

    # else:

        # assume dry run if no response
        # log.info(f'(dry run) {request.method} {request.url}')
        # request_metadata = ['(dry run)'] + request_metadata  # f'(dry run) {request_metadata}'
        # log_level = log.info

    log_level(' '.join(request_metadata))
    if request_data:
        log_level(request_data)
    if response:
        if response.ok:
            log_level(response.json())
        else:
            raise ApiError(request, response)