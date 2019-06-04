import json
from abc import ABC, abstractmethod
from cached_property import cached_property
from .api_client import ApiError
import urllib
from functools import wraps

"""
ideas:

could cache be located in client instead?
this could facilitate cross-object identification, e.g.:
    - collection to associated activities
    - looking up parent object in factory methods instead of passing as arg

possible useful attributes
# unique_param = 'slug'
# unique_attr = 'slug'  # name of unique attribute in parent model
# lookup_attr = 'id'  # name of attribute used inline in url endpoint, e.g. /api/{model_name}/{lookup_attr}

"""

def requires_remote_state(func):
    """
    Decorates a function that requires existing remote object to function (e.g. update, get)
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.id:
            raise Exception('Existing object not found')
        return func(self, *args, **kwargs)
    return wrapper


class ApiModel(ABC):
    """
    Provides methods for updating state on remote bridge system
    
    Additional methods that should be implemented in subclass:
    - .data
    - to_api_params()
    - from_api_params()  (optional - will need to figure out disabling functionality when linked generic model not available on init)
    
    """

    # model_name = ... e.g. 'collection'
    # lookup_field = 'id'  # default primary/surrogate key field name

    def __init__(self, client, model=None):
        """
        :param generic_model: parent ApiModel instance
        :type generic_model: linked ApiModel; if present, appropriate fields will be a dynamic lookup from this model. else, assumes there is some initial data
        """
        self.client = client
        self.model = model  # need attribute from model in .data()

    # @classmethod
    # def from_api_params(cls, ...)
    #     pass

    @property
    @abstractmethod
    def data(self):
        pass

    @abstractmethod
    def to_api_params(self):
        return dict()

    @property
    def pk(self):
        return self.data[self.lookup_field] if self.data else None

    @property
    def id(self):
        return self.data['id'] if self.data else None

    def create(self):
        """Create collection object in bridge
        
        :return: model data
        :rtype: dict
        """
        response = self.api.request('POST', self.model_name, json=self.to_api_params())
        if response.ok:
            self.__dict__['data'] = response.json()
        else:
            raise ApiError(response, message=f'Create error: {self}')
        return self.data

    def update(self):
        """
        Create or update corresponding object in bridge using instance data
        :return: model data
        :rtype: dict
        """
        # create
        if not self.id:
            return self.create()
        # update
        new_data = self.to_api_params()
        # only send request if a change is being made
        if not {**self.data, **new_data} == self.data:
            response = self.api.request('PUT', f'{self.model_name}/{self.data[self.lookup_field]}', json=self.to_api_params())
            if not response.ok:
                raise ApiError(response, message=f'Update error: {self}')
            self.__dict__['data'] = response.json()  # manually update cache
        return self.data

    def delete(self):
        """Delete item from bridge
        
        """
        if not self.id:
            return Exception('Primary key not found - cannot delete')
        response = self.api.request('DELETE', f'{self.model_name}/{self.data[self.lookup_field]}')
        if not response.ok:
            raise ApiError(response, message=f'Error updating bridge {self.model_name}')
        del self.__dict__['data']  # clear cache


class BridgeApiModelMixin:
    @property
    def api(self):
        return self.client.bridge_api


class EngineApiModelMixin:
    @property
    def api(self):
        return self.client.engine_api


class BridgeCollection(BridgeApiModelMixin, ApiModel):
    model_name = 'collection'
    lookup_field = 'id'

    @cached_property
    def data(self):
        qset = self.api.request('GET', self.model_name, params={'slug':self.model.slug}).json()
        return qset[0] if len(qset)>0 else None

    def to_api_params(self):
        return dict(
            slug=self.model.slug,
            name=self.model.name,
            strict_forward=True,
            owner=self.client.bridge_owner_pk
        )

    @requires_remote_state
    def update_activity_set(self, activity_set):
        """
        Replace activity set of bridge collection
        Activities existing previously in collection activity set but not part of new activity set
        will be removed from the collection activity set
        
        :param activities: activities to assign to collection
        :type activities: list BridgeActivity
        """

        # fetch existing activities by filtering the activity list api by collection pk
        response = self.api.request('GET', 'activity', params={'collection': self.pk})
        if not response.ok:
            raise ApiError(response, message='Error getting activity set')
        
        # dict of {pk:activity_dicts}  
        existing_activities = {x['source_launch_url']: x for x in response.json()}

        # create new activities
        for activity in activity_set:
            if activity.model.url in existing_activities:
                existing_data = existing_activities[activity.model.url]
                # compare field values only for field set in to_bridge_params()
                if {**existing_data, **activity.to_api_params()} == existing_data:
                    continue
            activity.update()

        # # remove old activities from set
        new_ids = set(x.model.url for x in activity_set)
        for existing_id, existing_activity in existing_activities.items():
            if existing_id not in new_ids:
                # activity model in bridge context is limited to within single collection, so delete method is appropriate here
                # TODO not correct
                # existing_activity.delete()
                activity_pk = existing_activity['id']
                self.api.request('DELETE', f'activity/{activity_pk}')

    @requires_remote_state
    def bridge_engine_sync(self):
        response = self.client.bridge_api.request('GET', f'collection/{self.model.slug}/sync/')
        if not response.ok:
            raise ApiError(response, message='Error transferring activity set from bridge to engine')
        return response.json()


class EngineCollection(EngineApiModelMixin, ApiModel):
    model_name = 'collection'
    lookup_field = 'collection_id'

    def __init__(self, *args, **kwargs):
        ApiModel.__init__(self, *args, **kwargs)

    # def pk(self):
    #     print(self.data)
    #     return self.data[self.lookup_field]

    def to_api_params(self):
        # TODO divert to .data if not initialized with linked generic model
        # if self.model ...
        return dict(
            collection_id=self.model.slug,
            name=self.model.name,
            # max_problems=None,
        )

    @cached_property
    def data(self):
        r = self.api.request('GET',f'{self.model_name}/{self.model.slug}')
        if r.ok:
            return r.json()
        else:
            return None

    def paginate(self, response):
        """Fetch all results from paginated list api view
        TODO probably move to api client
        :param response: response from first page
        :type response: reqeusts Response
        """
        if not response.ok:
            raise ApiError(response)
        page = response.json()
        if 'results' not in page:
            raise Exception('Paginator: "results" key not found in page')
        results = page['results']
        while page['next']:
            page = self.api.client.get(page['next']).json()
            results.extend(page['results'])
        return results
        

    def update_activity_set(self, activity_set):
        """
        Replace activity set of engine collection
        Activities existing previously in collection activity set but not part of new activity set
        will be removed from the collection activity set

        TODO potential optimization: populating Activity cache from collection activity list endpoint results,
        to reduce need for individual GET requests for each activity when fetching state
        
        :param activity_set: activities to assign to collection
        :type activity_set: list EngineActivity
        """
        # get existing activities using collection/slug/activities endpoint
        # TODO use https://engine.vpal.io/api/v2/activity?collections__collection_id=HarvardX-QMB1-2T2017__CG0-3 instead and iterate through pages
        # response = self.api.request('GET', f'collection/{self.pk}/activities')
        response = self.api.request('GET', 'activity', params={'collections__collection_id':self.model.slug})
        if not response.ok:
            raise ApiError(response, message='Error getting activity set')
        
        # dict of {url:activity_dicts}  
        existing_activities_data = {x['source_launch_url']: x for x in self.paginate(response)}

        # create new activities
        for activity in activity_set:
            if activity.model.url in existing_activities_data:
                existing_activity_data = existing_activities_data[activity.model.url]
                # compare field values only for field set in to_bridge_params()
                if {**existing_activity_data, **activity.to_api_params()} == existing_activity_data:
                    continue
            activity.update()

        # # delete old activities
        new_ids = set(x.model.url for x in activity_set)
        for url, existing_activity_data in existing_activities_data.items():
            if url not in new_ids:
                # get pk of membership relation
                r = self.api.request('GET','collection_activity', params=dict(
                        collection=self.data['id'],
                        activity=existing_activity_data['id']
                    )
                )
                if not r.ok:
                    raise ApiError(response, message='Error identifying collection-activity membership')
                pk = r.json()['results'][0]['id']
                
                # delete membership relation
                r = self.api.request('DELETE',f'collection_activity/{pk}')
                if not r.ok:
                    raise ApiError(response, message='Error removing activity from collection')
                

    # def add_activity(activity):
    #     qset = self.api.request('GET', f'collection_activity', params=dict(
    #         collection=
    #     ))

class BridgeActivity(BridgeApiModelMixin, ApiModel):
    model_name = 'activity'
    lookup_field = 'id'
    
    @cached_property
    def data(self):
        qset = self.api.request('GET', self.model_name, params={
            'collection':self.model.collection.bridge.pk,
            'source_launch_url':self.model.url
        }).json()#['results']
        return qset[0] if len(qset)>0 else None


    def create(self):
        # Modify base method to ensure related collection exists and is updated before attempting activity update
        if not self.model.collection.bridge.pk:
            self.model.collection.bridge.update()
        return super().create()

    def to_api_params(self):
        defaults = dict(
            repetition=1,
            atype='G'
        )
        if not self.model.collection.bridge.pk:
            raise Exception('Referenced collection not yet created in bridge')
        return dict(
            **defaults,
            collection = self.model.collection.bridge.pk,
            source_launch_url=self.model.url,
            lti_content_source=self.client.content_source_pk, 
            source_name=self.model.name,
            name=self.model.name,
            stype=self.model.activity_type  # make modifiable
        )

class EngineActivity(EngineApiModelMixin, ApiModel):
    model_name = 'activity'
    lookup_field = 'id'

    def to_api_params(self):
        return dict(
            source_launch_url=self.model.url,
            name=self.model.name,
            collections=[self.model.collection.engine.data['id']],
            knowledge_components=[kc.engine.id for kc in self.model.knowledge_components],
            difficulty=self.model.difficulty,
            tags='',
            # type=self.model.activity_type,
            prerequisite_activities=[],
        )

    @cached_property
    def data(self):
        """Return engine json representation of activity, fetching from remote system in not already cached
        Can't use self.pk in data() - leads to recursive error since .pk depends on .data()
        """
        # look up pk from url field (unique)
        qset = self.api.request(
            'GET', 
            self.model_name, 
            params=dict(url=self.model.url),
        ).json()['results']
        return qset[0] if len(qset)>0 else None

    def create(self):
        """Override base engine create method to create related api objects if they do not already exist
        # Modify base method to ensure related collection exists and is updated before attempting activity update
        """
        if not self.model.collection.engine.id:
            self.model.collection.create()
        for kc in self.model.knowledge_components:
            if not kc.engine.id:
                kc.engine.update()
        return super().create()

    def update(self):
        """
        Override to ensure related models exist
        """
        if not self.model.collection.engine.id:
            self.model.collection.create()
        for kc in self.model.knowledge_components:
            if not kc.engine.id:
                kc.engine.update()
        return super().update()

    # @classmethod
    # def from_api_params(cls, client, collection, data):
    #     """Factory method from bridge params
        
    #     :param bridge_api: bridge api instance
    #     :type bridge_api: BridgeApi
    #     :param collection: collection instance
    #     :type collection: Collection
    #     :param data: bridge param dict
    #     :type data: dict
    #     :return: Activity instance
    #     :rtype: Activity
    #     """
    #     activity = cls(
    #         client=client,
    #         collection=collection,
    #         url=data['source_launch_url'],
    #         # lti_consumer_pk=data['lti_consumer'],
    #         name=data['source_name'],
    #         activity_type = data['stype']
    #     )
    #     activity.__dict__['bridge_data'] = data
    #     return activity

    # @classmethod
    # def from_engine_params(cls, client, collection, data, knowledge_components=set(), prerequisite_activities=set()):
    #     """
    #     Factory method to instantiate Activity object from engine params
    #     Need to provide foreign objects because they cannot be retrieved by pk
    #     :param client: AlosiClient instance
    #     :param collection: Collection instance
    #     :param knowledge_components: list/set of KnowledgeComponent instances
    #     :param prerequisite_activities: list/set of Activity instances
    #     :return: Activity instance
    #     :rtype: Activity
    #     """

    #     return cls(
    #         client=client,
    #         collection=collection,
    #         url=data['url'],
    #         name=data['name'],
    #         activity_type=data['type'],
    #         knowledge_components=knowledge_components,
    #         prerequisite_activities=prerequisite_activities,
    #     )

class EngineKnowledgeComponent(EngineApiModelMixin, ApiModel):
    """
    Knowledge component
    """
    model_name = 'knowledge_component'
    lookup_field = 'kc_id'  # field in api params dict that corresponds to url lookup field

    def add_prerequisite_knowledge_component(self, prerequisite, connection_strength):
        """Add prerequisite knowledge component
        
        :param prerequisite: prerequisite knowledge component 
        :type prerequisite: KnowledgeComponent
        :param connection_strength: connection strength between knowledge component, between 0 and 1
        :type connection_strength: float
        """
        self.model.prerequisite_knowledge_components[prerequisite] = connection_strength

    def to_api_params(self):
        return dict(
            name=self.model.name,
            kc_id=self.model.slug,
            mastery_prior=self.model.mastery_prior,
        )

    @cached_property
    def data(self):
        qset = self.client.engine_api.request('GET', self.model_name, 
            params={'kc_id':self.model.slug}  # pseudo primary key that identifies single object
        ).json()['results']
        return qset[0] if len(qset)>0 else None

    def create(self):
        """Overrides base method to ensure related models are created
        
        """
        for kc in self.model.prerequisite_knowledge_components:
            if not kc.engine.id:
                kc.engine.update()
        return super().create()

    def update(self):
        """Overrides base method to ensure related models are created
        
        """
        for kc in self.model.prerequisite_knowledge_components:
            if not kc.engine.id:
                kc.engine.update()
        return super().update()

    def push(self):
        """
        Initialize KC on engine
        """
        return self.update()


class Activity:
    def __init__(self, client, collection, url, name, activity_type='problem', difficulty=None, 
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
        self.bridge = BridgeActivity(client, self)
        self.engine = EngineActivity(client, self)

        self.collection = collection
        self.url = url
        self.name = name
        self.activity_type = activity_type
        self.difficulty = difficulty
        # tagging
        self.knowledge_components = knowledge_components or []
        # prereq activities
        self.prerequisite_activities = prerequisite_activities or []

    def push(self):
        self.engine.update()
        self.bridge.update()


class Collection:
    def __init__(self, client, *, slug, name):
        self.engine = EngineCollection(client, self)
        self.bridge = BridgeCollection(client, self)

        self.slug = slug
        self.name = name

    def push(self, activity_set=None):
        self.engine.update()
        self.bridge.update()
        # also push activity set if one is provided, otherwise just update collection metadata
        if activity_set is not None:
            self.engine.update_activity_set([a.engine for a in activity_set])
            self.bridge.update_activity_set([a.bridge for a in activity_set])


class KnowledgeComponent:
    """
    Knowledge component
    """
    def __init__(self, client, *, name, slug, prerequisite_knowledge_components=None, mastery_prior=0.5):
        self.engine = EngineKnowledgeComponent(client, self)

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

    def push(self):
        self.engine.update()
