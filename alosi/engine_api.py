from alosi.api_client import ApiClient


class EngineApi(ApiClient):
    """
    General API interface to engine application
    """
    api_path = "/api/v2"

    def __init__(self, host="http://localhost:8000", token=None):
        super().__init__(host, token)

    def create_activity(self, **kwargs):
        return self.client.post(
            self._absolute_url('activity'),
            json=kwargs
        )

    def recommend(self, learner=None, collection=None, sequence=None):
        return self.client.post(
            self._absolute_url('activity/recommend'),
            json=dict(learner=learner, collection=collection, sequence=sequence)
        )

    def submit_score(self, learner=None, activity=None, score=None):
        return self.client.post(
            self._absolute_url('score'),
            json=dict(learner=learner, activity=activity, score=score)
        )

    def bulk_update_mastery(self, data):
        return self.client.put(
            self._absolute_url('mastery/bulk_update'),
            json=data
        )

    def create_knowledge_component(self, **kwargs):
        return self.client.post(
            self._absolute_url('knowledge_component'),
            json=kwargs
        )
