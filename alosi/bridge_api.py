from alosi.api_client import ApiClient


class BridgeApi(ApiClient):
    """
    General API interface to bridge application
    """
    api_path = "/api"
    trailing_slash = True

    def __init__(self, host="http://localhost:8008", token=None):
        super().__init__(host, token)

    def get_collection(self, pk):
        return self.client.get(
            self._absolute_url("collection/{}".format(pk))
        )

    def create_collection(self, **kwargs):
        """
        Collection fields:
            name
            metadata
            strict_forward
            owner
            slug
        """
        return self.client.post(
            self._absolute_url('collection'),
            json=kwargs
        )

    def delete_collection(self, **kwargs):
        return self.client.delete(
            self._absolute_url('collection'),
            json=kwargs
        )

    def get_activity(self, pk):
        return self.client.get(
            "{}/activity/{}".format(self.base_url, pk)
        )

    def create_activity(self, **kwargs):
        return self.client.post(
            self._absolute_url('activity'),
            json=kwargs
        )

    def delete_activity(self, **kwargs):
        return self.client.delete(
            self._absolute_url('activity'),
            json=kwargs
        )
