import requests


class BridgeApi(object):
    """
    General API interface to bridge application
    """

    def __init__(self, host="http://localhost:8008", token=None):
        self.base_url = host + '/api'
        self.headers = {'Authorization': 'Token {}'.format(token)} if token else {}
        self.client = self.get_client()
        self.client.headers.update(self.headers)

    def get_client(self):
        return requests.Session()

    def get_collection(self, **kwargs):
        return self.client.get(
            self.base_url + '/collection',
            json=kwargs
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

    def get_activity(self, **kwargs):
        return self.client.get(
            self.base_url + '/activity/',
            json=kwargs
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
