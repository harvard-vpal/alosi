import requests


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
