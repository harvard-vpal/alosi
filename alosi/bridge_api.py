import requests


class BridgeApi(object):
    """
    General API interface to bridge application
    """

    def __init__(self, host="http://localhost:8008", token=None):
        self.host = host
        self.base_url = "{}/api".format(self.host)
        self.client = self.get_client(token)

    @staticmethod
    def get_client(token=None):
        client = requests.Session()
        headers = {'Authorization': 'Token {}'.format(token)} if token else {}
        client.headers.update(headers)
        return client

    def get_collection(self, pk):
        return self.client.get(
            "{}/collection/{}".format(self.base_url, pk)
        )

    def create_collection(self, **kwargs):
        """
        Collection fields:
            name
            metadata
            strict_forward
            owner
        """
        return self.client.post(
            self.base_url + '/collection/',
            json=kwargs
        )

    def delete_collection(self, **kwargs):
        return self.client.delete(
            self.base_url + '/collection/',
            json=kwargs
        )

    def get_activity(self, pk):
        return self.client.get(
            "{}/activity/{}".format(self.base_url, pk)
        )

    def create_activity(self, **kwargs):
        return self.client.post(
            self.base_url + '/activity/',
            json=kwargs
        )

    def delete_activity(self, **kwargs):
        return self.client.delete(
            self.base_url + '/activity/',
            json=kwargs
        )
