import json


class Collection:
    def __init__(self, slug, name):
        self.slug = slug
        self.name = name

    def to_bridge_params(self):
        return dict(
            slug=self.slug,
            name=self.name,
            strict_forward=True,
            owner=1
        )


class Activity:
    def __init__(self, collection, url, name, activity_type='problem', difficulty=None, 
        knowledge_components=None, prerequisite_activities=None):
        """Activity
        
        :param collection: collection to which activity belongs
        :type collection: Collection
        :param url: unique source url for activity
        :type url: str
        :param name: name
        :type name: str
        :param activity_type: content type, e.g. problem, html
        :type activity_type: str
        """
        self.collection = collection
        self.url = url
        self.name = name
        self.activity_type = activity_type
        self.difficulty = difficulty
        # tagging
        self.knowledge_components = knowledge_components or set()
        # prereq activities
        self.prerequisite_activities = prerequisite_activities or set()

    def to_bridge_params(self):
        return dict(
            collection = None,  #TODO get pk
            source_launch_url=self.url,
            lti_consumer=1,  #TODO content_source_pk, 
            source_name=self.name,
            name=self.name,
            atype='G',
            stype=self.activity_type  # make modifiable
        )

    def add_prerequisite(self, prerequisite):
        self.prerequisite_activities.add(prerequisite)


class KnowledgeComponent:
    """
    TODO consider modifying hash() to compute based on id attribute or object id()
    
    """

    def __init__(self, name, id, prerequisite_knowledge_components=None):
        self.name = name
        self.id = id
        # TODO account for prereq connection strength
        self.prerequisite_knowledge_components = prerequisite_knowledge_components or set()
            
    def add_prerequisite(self, prerequisite):
        self.prerequisite_knowledge_components.add(prerequisite)


# class BridgeCollection:
#     def __init__(self, slug, name, strict_forward, owner):
#         self.slug=slug
#         self.name=name
#         self.strict_forward=strict_forward

#     @classmethod
#     def from_collection(cls, collection):
#         return cls.__init__(
#             slug=collection.slug,
#             name=collection.name,
#             strict_forward=True,
#             owner=1
#         )
    
#     def to_api_params(self):
#         return dict(
#             slug=self.slug,
#             name=self.name,
#             strict_forward=self.strict_forward,
#             owner=self.owner
#         )
    
# class EngineActivity:
#     def __init__(self, collection, url, name, lti_consumer, atype, stype):
#         pass

#     @classmethod
#     def from_activiy(cls, activity):
#         return cls.__init__(
            
#         )

#     def to_api_params(self):
#         return dict(
#             collection = None,  #TODO get pk
#             source_launch_url=self.url,
#             lti_consumer=None,  #TODO content_source_pk, 
#             source_name=self.name,
#             name=self.name
#             atype='G',
#             stype='problem'  # make modifiable
#         )
