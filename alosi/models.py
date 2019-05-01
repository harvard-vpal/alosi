import json
from cached_property import cached_property
from .api_client import ApiError

from functools import wraps

"""
ideas:

could cache be located in client instead?
this could facilitate cross-object identification, e.g.:
    - collection to associated activities
    - looking up parent object in factory methods instead of passing as arg
"""

def requires_existing_bridge_pk(func):
    """
    Decorates a function that requires existing object to function (e.g. update, get)
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.bridge_pk:
            raise Exception('Bridge object not found')
        return func(self, *args, **kwargs)
    return wrapper


class BridgeApiModel:
    """
    Provides methods for updating state on remote bridge system
    
    Additional methods that should be implemented in subclass:
    - .bridge_data
    - to_bridge_params()
    - from_bridge_params()
    
    """

    # model_name = ... e.g. 'collection'
    # unique_field = ... e.g. 'slug'

    @property
    def bridge_pk(self):
        return self.bridge_data['id'] if self.bridge_data else None

    def bridge_create(self):
        """Create collection object in bridge
        
        :raises ApiError: [description]
        :return: model data
        :rtype: dict
        """
        response = self.client.bridge_api.request('POST', self.model_name, json=self.to_bridge_params())
        if response.ok:
            self.__dict__['bridge_data'] = response.json()
        else:
            raise ApiError(response, message=f'Error creating bridge {self.model_name}')
        return self.bridge_data

    def bridge_update(self):
        """
        Create or update corresponding object in bridge using instance data
        :return: model data
        :rtype: dict
        """
        # create
        if not self.bridge_pk:
            return self.bridge_create()
        # update
        response = self.client.bridge_api.request('PUT', f'{self.model_name}/{self.bridge_pk}', json=self.to_bridge_params())
        if not response.ok:
            raise ApiError(response, message=f'Error updating bridge {self.model_name}')
        self.__dict__['bridge_data'] = response.json()  # manually update cache
        return self.bridge_data

    def bridge_delete(self):
        """Delete item from bridge
        
        """
        if not self.bridge_pk:
            return Exception('Primary key not found - cannot delete')
        response = self.client.bridge_api.request('DELETE', f'{self.model_name}/{self.bridge_pk}')
        if not response.ok:
            raise ApiError(response, message=f'Error updating bridge {self.model_name}')
        del self.__dict__['bridge_data']  # clear cache


class EngineApiModel:
    """Provides methods for updating state on remote engine system

    Assumes implementation of property '.engine_data' on subclass
    
    :raises ApiError: [description]
    :raises ApiError: [description]
    :raises ApiError: [description]
    :return: [description]
    :rtype: [type]
    """

    # model_name = ... e.g. 'collection'
    # unique_field = ... e.g. 'slug'

    @property
    def engine_pk(self):
        return self.engine_data['id'] if self.engine_data else None

    def engine_create(self):
        """Create object in engine
        
        :raises ApiError: [description]
        :return: model data
        :rtype: dict
        """
        response = self.client.engine_api.request('POST', self.model_name, json=self.to_engine_params())
        if response.ok:
            self.__dict__['engine_data'] = response.json()
        else:
            raise ApiError(response, message=f'Error creating engine {self.model_name}')
        return self.engine_data

    def engine_update(self):
        """
        Create or update corresponding object in engine using instance data
        :return: model data
        :rtype: dict
        """
        # create
        if not self.engine_pk:
            return self.engine_create()
        # update
        response = self.client.engine_api.request('PUT', f'{self.model_name}/{self.engine_pk}', json=self.to_engine_params())
        if not response.ok:
            raise ApiError(response, message=f'Error updating engine {self.model_name}')
        self.__dict__['engine_data'] = response.json()  # manually update cache
        return self.engine_data

    def engine_delete(self):
        """
        Delete item from bridge
        """
        if not self.bridge_pk:
            return Exception('Primary key not found - cannot delete')
        response = self.client.engine_api.request('DELETE', f'{self.model_name}/{self.engine_pk}')
        if not response.ok:
            raise ApiError(response, message=f'Error updating engine {self.model_name}')
        del self.__dict__['engine_data']  # clear cache


class Collection(EngineApiModel, BridgeApiModel):
    """
    TODO should collection model implement collection activity bulk update?
    TODO should collection have list field for activities
    
    :param ApiModel: [description]
    :type ApiModel: [type]
    :return: [description]
    :rtype: [type]
    """

    model_name = 'collection'

    def __init__(self, client, slug, name):
        self.slug = slug
        self.name = name
        self.client = client
        self.bridge_owner_pk = client.bridge_owner_pk

    def to_bridge_params(self):
        return dict(
            slug=self.slug,
            name=self.name,
            strict_forward=True,
            owner=1
        )
    
    @classmethod
    def from_bridge_params(cls, client, data):
        """Factory method from bridge dict (includes id)
        
        :param data: [description]
        :type data: dict
        """
        collection = cls(
            client=client,
            slug=data['slug'],
            name=data['name'],
            owner_pk=data['owner']
        )
        collection.__dict__['bridge_data'] = data
        return collection

    @cached_property
    def bridge_data(self):
        queryset = self.client.bridge_api.request(
            'GET', 
            self.model_name, 
            params={'slug':self.slug}  # pseudo primary key that identifies single object
        ).json()
        if len(queryset)>0:
            return queryset[0]
        else:
            return None

    @requires_existing_bridge_pk
    def bridge_update_activity_set(self, activities):
        # get existing activities
        response = self.client.bridge_api.request('GET', 'activity', params={'collection': self.bridge_pk})
        if not response.ok:
            raise ApiError(response, message='Error getting activity set')
        
        # dict of {pk:constructed Activity objects}  

        existing_activities = {
            x['source_launch_url']: Activity.from_bridge_params(
                    client=self.client,
                    collection=self,
                    data=x
                )
            for x in response.json()
        }
        # create new activities
        for activity in activities:
            if activity.url in existing_activities:
                existing_data = existing_activities[activity.url].bridge_data
                # compare field values only for field set in to_bridge_params()
                if {**existing_data, **activity.to_bridge_params()} == existing_data:
                    continue
            activity.bridge_update()

        # # delete old activities
        new_ids = set(x.url for x in activities)
        for existing_activity in existing_activities.values():
            if existing_activity.url not in new_ids:
                existing_activity.bridge_delete()
    
    @requires_existing_bridge_pk
    def bridge_engine_sync(self):
        response = self.client.bridge_api.request('GET', f'collection/{self.slug}/sync/')
        if not response.ok:
            raise ApiError(response, message='Error transferring activity set from bridge to engine')
        return response.json()
    
    def to_engine_params(self):
        return dict(
            collection_id=self.slug,
            name=self.name,
            # max_problems=None,
        )

    @classmethod
    def from_engine_params(cls, client, data, **kwargs):
        return cls(
            client=client,
            slug=data['collection_id'],
            **kwargs,
            # bridge owner pk?
        )

    @cached_property
    def engine_data(self):
        r = self.client.engine_api.request('GET',f'{self.model_name}/{self.slug}')
        if r.ok:
            return r.json()
        else:
            return None

    def push(self, activity_set=None):
        self.engine_update()
        self.bridge_update()
        # also push activity set if one is provided, otherwise just update collection metadata
        if activity_set is not None:
            self.bridge_update_activity_set(activity_set)
            # self.engine_update_activity_set()


class Activity(EngineApiModel, BridgeApiModel):
    
    model_name = 'activity'

    def __init__(self, client, collection, url, name, activity_type='problem', difficulty=None, 
        knowledge_components=set(), prerequisite_activities=set()):
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
        self.client = client
        self.collection = collection
        self.url = url
        self.name = name
        self.activity_type = activity_type
        self.difficulty = difficulty
        # tagging
        self.knowledge_components = knowledge_components
        # prereq activities
        self.prerequisite_activities = prerequisite_activities

    @cached_property
    def bridge_data(self):
        """Return bridge json representation, fetching from bridge if not already cached
        
        :return: collection data
        :rtype: dict
        """
        queryset = self.client.bridge_api.request(
            'GET', 'activity',
            params={'collection':self.collection.bridge_pk, 'source_launch_url':self.url}
        ).json()
        if len(queryset)>0:
            return queryset[0]
        else:
            return None

    def bridge_create(self):
        # Modify base method to ensure related collection exists and is updated before attempting activity update
        if not self.collection.bridge_pk:
            self.collection.bridge_update()
        return super().bridge_create()

    def to_bridge_params(self):
        defaults = dict(
            repetition=1,
            atype='G'
        )
        if not self.collection.bridge_pk:
            raise Exception('Referenced collection not yet created in bridge')
        return dict(
            **defaults,
            collection = self.collection.bridge_pk,
            source_launch_url=self.url,
            lti_content_source=self.client.content_source_pk, 
            source_name=self.name,
            name=self.name,
            stype=self.activity_type  # make modifiable
        )

    @classmethod
    def from_bridge_params(cls, client, collection, data):
        """Factory method from bridge params
        
        :param bridge_api: bridge api instance
        :type bridge_api: BridgeApi
        :param collection: collection instance
        :type collection: Collection
        :param data: bridge param dict
        :type data: dict
        :return: Activity instance
        :rtype: Activity
        """
        activity = cls(
            client=client,
            collection=collection,
            url=data['source_launch_url'],
            # lti_consumer_pk=data['lti_consumer'],
            name=data['source_name'],
            activity_type = data['stype']
        )
        activity.__dict__['bridge_data'] = data
        return activity

    def add_prerequisite(self, prerequisite):
        self.prerequisite_activities.add(prerequisite)

    def to_engine_params(self):
        return dict(
            url=self.url,
            name=self.name,
            collections=[self.collection.engine_pk],
            knowledge_components=[kc.engine_pk for kc in self.knowledge_components],
            difficulty=self.difficulty,
            tags='',
            type=self.activity_type,
            prerequisite_activities=[],
        )

    @classmethod
    def from_engine_params(cls, client, collection, data, knowledge_components=set(), prerequisite_activities=set()):
        """
        Factory method to instantiate Activity object from engine params
        Need to provide foreign objects because they cannot be retrieved by pk
        :param client: AlosiClient instance
        :param collection: Collection instance
        :param knowledge_components: list/set of KnowledgeComponent instances
        :param prerequisite_activities: list/set of Activity instances
        :return: Activity instance
        :rtype: Activity
        """

        return cls(
            client=client,
            collection=collection,
            url=data['url'],
            name=data['name'],
            activity_type=data['type'],
            knowledge_components=knowledge_components,
            prerequisite_activities=prerequisite_activities,
        )

    @cached_property
    def engine_data(self):
        """Return engine json representation of activity, fetching from remote system in not already cached
        """
        r = self.client.engine_api.request('GET', f'activity/{self.engine_pk}')
        if r.ok:
            return r.json()
        else:
            return None

    def engine_create(self):
        """Override base engine create method to create related api objects if they do not already exist
        # Modify base method to ensure related collection exists and is updated before attempting activity update
        """
        if not self.collection.engine_pk:
            self.collection.engine_create()
        for kc in self.knowledge_components:
            if not kc.engine_pk:
                kc.engine_update()
        return super().engine_create()

    def engine_update(self):
        """
        Override to ensure related models exist
        """
        if not self.collection.engine_pk:
            self.collection.engine_create()
        for kc in self.knowledge_components:
            if not kc.engine_pk:
                kc.engine_update()
        return super().engine_update()

    def push(self):
        self.engine_update()
        self.bridge_update()


class KnowledgeComponent(EngineApiModel):
    """
    Knowledge component
    """
    def __init__(self, client, name, slug, prerequisite_knowledge_components=None, mastery_prior=0.5):
        self.name = name
        self.slug = slug  # "kc_id"
        # key is prereq kc object, value is connection strength
        self.prerequisite_knowledge_components = prerequisite_knowledge_components or {}
        self.mastery_prior = mastery_prior

    def add_prerequisite_knowledge_component(self, prerequisite, connection_strength):
        """Add prerequisite knowledge component
        
        :param prerequisite: prerequisite knowledge component 
        :type prerequisite: KnowledgeComponent
        :param connection_strength: connection strength between knowledge component, between 0 and 1
        :type connection_strength: float
        """
        self.prerequisite_knowledge_components[prerequisite] = connection_strength

    def to_engine_params(self):
        return dict(
            name=self.name,
            kc_id=self.slug,
            mastery_prior=self.mastery_prior,
        )

    @classmethod
    def from_engine_params(cls, client, data, prerequisite_knowledge_components=set()):
        return cls(
            client=client,
            name=data['name'],
            slug=data['kc_id'],
            prerequisite_knowledge_components=prerequisite_knowledge_components,
        )

    @cached_property
    def engine_data(self):
        queryset = self.client.engine_api.request(
            'GET', 
            self.model_name, 
            params={'slug':self.slug}  # pseudo primary key that identifies single object
        ).json()
        if len(queryset)>0:
            return queryset[0]
        else:
            return None

    def engine_create(self):
        """Overrides method to ensure related models are created
        
        """
        for kc in self.prerequisite_knowledge_components:
            if not kc.engine_pk:
                kc.engine_update()
        return super().engine_create()

    def engine_update(self):
        """Overrides method to ensure related models are created
        
        """
        for kc in self.prerequisite_knowledge_components:
            if not kc.engine_pk:
                kc.engine_update()
        return super().engine_update()

    def push(self):
        """
        Initialize KC on engine
        """
        return self.engine_update()
