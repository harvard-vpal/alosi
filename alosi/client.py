from .bridge_api import BridgeApi
from .engine_api import EngineApi
from .models import Activity, Collection, KnowledgeComponent


class AlosiClient:
    def __init__(self, *, bridge_host=None, bridge_token=None, bridge_owner_pk=None, engine_host=None, engine_token=None, content_source_pk=None):
        """
        Initialize client with configuration and credentials.
        For some use cases involving subsets of systems, some groups of parameters may not be needed
        
        :param bridge_host: Base URL of bridge application
        :param bridge_token: API token for bridge
        :param bridge_owner_pk: Primary key of bridge user to use as owner for created collections
        :type bridge_owner_pk: int
        :param content_source_pk: Primary key of content source associated with created activities
        :param engine_host: Base URL of engine application
        :param engine_token: API token for engine
        """
        if bridge_host:
            self.bridge_api = BridgeApi(bridge_host, token=bridge_token)
        if engine_host:
            self.engine_api = EngineApi(engine_host, token=engine_token)
        self.bridge_owner_pk = bridge_owner_pk
        self.content_source_pk = 1

    def Activity(self, *args, **kwargs):
        """Activity factory method with reference to client
        
        """
        return Activity(*args, client=self, **kwargs)

    def Collection(self, *args, **kwargs):
        """Collection factory method with reference to client
        
        """
        return Collection(*args, client=self, **kwargs)

    
    def KnowledgeComponent(self, *args, **kwargs):
        """KnowledgeComponent factory method with reference to client
        
        """
        return KnowledgeComponent(*args, client=self, **kwargs)

    def push(self, *, collections, activities, knowledge_components):
        """
        Main user entrypoint for pushing client objects to remote systems

        '*' in arguments requires subsequent arguments to be passed with keywords
        
        :param collections: collections
        :type collections: list Collection
        :param activities: activities
        :type activities: list Activity
        :param knowledge_components: knowledge components
        :type knowledge_components: list KnowledgeComponent
        """
        # necessary KC's would be created during activity_set push, but explicit update will catch orphan KC's not associated with any other activity/kc
        # for kc in knowledge_components:
        #     kc.push()

        for collection in collections:
            # this also has the effect of initializing the activities and tagging / kc dependencies if they are not already initialized
            activity_set = [activity for activity in activities if activity.collection is collection]
            collection.push(activity_set)
