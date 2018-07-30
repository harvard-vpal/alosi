import requests


class EngineApi(object):
    """
    General API interface to engine application
    """

    def __init__(self, host="http://localhost:8000", token=None):
        self.host = host
        self.base_url = "{}/api/v2".format(self.host)
        self.client = self.get_client(token)

    @staticmethod
    def get_client(token=None):
        client = requests.Session()
        headers = {'Authorization': 'Token {}'.format(token)} if token else {}
        client.headers.update(headers)
        return client

    def create_activity(self, **kwargs):
        return self.client.post(
            self.base_url + '/activity',
            json=kwargs
        )

    def recommend(self, learner=None, collection=None, sequence=None):
        return self.client.post(
            self.base_url + '/activity/recommend',
            json=dict(learner=learner, collection=collection, sequence=sequence)
        )

    def submit_score(self, learner=None, activity=None, score=None):
        return self.client.post(
            self.base_url + '/score',
            json=dict(learner=learner, activity=activity, score=score)
        )

    def bulk_update_mastery(self, data):
        return self.client.put(
            self.base_url + '/mastery/bulk_update',
            json=data
        )

    def create_knowledge_component(self, **kwargs):
        return self.client.post(
            self.base_url + '/knowledge_component',
            json=kwargs
        )
