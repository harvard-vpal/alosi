import logging
from alosi.engine_api import EngineApi
from alosi.bridge_api import BridgeApi
from time import sleep
import pprint
from json import JSONDecodeError
import pandas as pd
import numpy as np
from alosi.data.google_drive import export_sheet_to_dataframe
import os

# time to wait before request, for spacing out requests to server
REQUEST_DELAY = float(os.getenv('ALOSI_REQUEST_DELAY', 0.1))

log = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, request, response):
        self.request = request
        self.response = response

    def __str__(self):
        try:
            response_data = self.response.json()
        except:
            response_data = self.response.text

        return (
            f"APIError: \n"
            f"request: {self.request.method} {self.request.url} {self.response.status_code}"
            f"response: \n"
            f"{response_data}"
        )


def _request(api, method, model_name, **kwargs):
    """
    Construct and request object with custom logging
    For non-GET requests, only send request if dry_run is False
    :return: dictionary of response data [request.json()]
    """
    request = api.prepare(method, model_name, **kwargs)
    # if os.getenv('ALOSI_DRY_RUN') and not method=='GET':
    #     _log_request(request)
    #     return
    # else:
    sleep(REQUEST_DELAY)
    response = api.client.send(request)
    _log_request(request, response)
    return response.json()


def _log_request(request, response):
    """Log info about a HTTP request
    
    :param request: Requests request
    :param response: Requests response. Optional for dry runs where response not available
    :param response: [type], optional
    :raises ApiError: [description]
    """

    request_metadata = [request.method, request.url]  #f'{request.method} {request.url}'
    request_data = request.body if request.method in ['POST', 'PUT'] else None
    # if response:
    request_metadata.append(str(response.status_code))

    # determine logging level based on response status
    if response.ok:
        log_level = log.debug
    else:
        log_level = log.error

    # else:

        # assume dry run if no response
        # log.info(f'(dry run) {request.method} {request.url}')
        # request_metadata = ['(dry run)'] + request_metadata  # f'(dry run) {request_metadata}'
        # log_level = log.info

    log_level(' '.join(request_metadata))
    if request_data:
        log_level(request_data)
    if response:
        if response.ok:
            log_level(response.json())
        else:
            raise ApiError(request, response)


def _plan_api_actions(key_field, new_objects, existing_objects=[]):
    """
    Merge and partition objects from new_objects and old_objects into 4 groups:
        create: objects in new that are not in existing
        update: objects in new that have corresponding object in existing
        delete: objects in existing that are not in new.
        no_action: objects in existing that are not modified
    
    Data structure for keys are dicts keyed by pk, so that pk can be used to construct endpoint, except for create,
        which is a list, since pk not needed for endpoint

    :param key_field: dict key name of value to use as id for determining object correspondence
        e.g. 'source_launch_url'
    :param new_objects: list of dicts representing objects to create or update
    :param existing_objects: list of dicts representing objects currently existing in the remote system
    :return:
    """
    partition = dict(create=[], update={}, delete={}, no_action={})

    # index by given field
    new_objects_indexed = {d[key_field]: d for d in new_objects}
    existing_objects_indexed = {d[key_field]: d for d in existing_objects}

    # label objects for create or update
    for key, new_obj in new_objects_indexed.items():
        # if key in old_objects_indexed:
        # get item from existing objects that matches key
        existing_obj = existing_objects_indexed.get(key)
        if existing_obj:
            # assumes there is an id field in existing object representation, needed to construct update url
            pk = existing_obj['id']
            if new_obj == existing_obj:  # based on field values comparison
                partition['no_action'][pk] = new_obj
            else:
                partition['update'][pk] = new_obj
                # partition['update_old'].append(old_objects_indexed[key])
        else:
            # create action doesn't need pk
            partition['create'].append(new_obj)

    # label objects for deletion
    for key, existing_obj in existing_objects_indexed.items():
        if key not in new_objects_indexed:
            pk = existing_obj['id']
            partition['delete'][pk].append(existing_obj)

    return partition


def _apply_api_actions(api, objects, object_name):
    """
    :param objects: dict with keys: create, update, delete, each containing list of dicts representing api models
    :param object_name: api endpoint name (e.g. activity, collection) to construct endpoint url with
    :param dry_run: if True, doesn't actually send requests
    :return:
    """
    results = {'create':[], 'update':[], 'delete':[]}
    # handle create / update / delete
    for obj in objects.get('create',[]):
        r = _request(api, 'POST', f'{object_name}', json=obj)
        results['create'].append(r)
    for pk, obj in objects.get('update',{}).items():
        r = _request(api, 'PUT', f'{object_name}/{pk}', json=obj)
        results['update'].append(r)
    for pk, obj in objects.get('delete',{}).items():
        r = _request(api, 'DELETE', f'{object_name}/{pk}', json=obj)
        results['delete'].append(r)
    return results


class BridgeInitializer:
    def __init__(self, bridge_host, bridge_token,
                bridge_user_pk=1):
        """
        Class to initialize data in ALOSI bridge and engine systems
        :param bridge_api: alosi.bridge_api.BridgeApi
        :param engine_api: alosi.engine_api.EngineApi
        :param collections: list of Collection objects
        :param activities: list of Activity objects
        :param knowledge components: list of KnowledgeComponent objects
        :param course_id: course code to use for url construction (e.g. 'HarvardX+SPU30x+2T2018')
        :param prefix: course code to append to kcs, etc, optional
        :param content_source_host: content source host (e.g. https://example.openedx.org)
        :param bridge_owner_pk: pk of the bridge user that owns created collection
            TODO revise if there is a way to avoid pk use
        :param bridge_content_source_pk: pk of the content source to assign to created activity
            TODO revise if there is a way to avoid pk use
        """
        self.bridge_api = BridgeApi(bridge_host, token=bridge_token)

        self.bridge_user_pk = bridge_user_pk

    def update_collection_metadata(self, collection):
        """Create or update collection
        
        :param collection: Collection model
        :type collection: Collection
        :param activities: list of Activity models
        :type activities: list [Activity]
        """
        # create or update collection
        existing_collection = _request(self.bridge_api, 'GET', 'collection', params={'slug':collection.slug})
        collection_actions = _plan_api_actions('slug', [collection.to_bridge_params()], existing_collection)
        collection_actions_applied = _apply_api_actions(self.bridge_api, collection_actions, 'collection')
        return collection_actions_applied

    def update_collection_activities(self, collection, activities):
        """
        Create or update a collection populated with activities.
        Identifies existing collection and activities, and figures out what needs to be updated/created/deleted.
        If the collection with the specified slug does not exist, creates the collection before populating with activities.
        """

        # ensure collection is created and updated
        self.update_collection_metadata(collection)
        
        # get collection pk
        existing_collection = _request(self.bridge_api, 'GET', 'collection', params={'slug':collection.slug})
        collection_pk = existing_collection[0]['id']

        # filter existing activity objects by collection
        existing_activities = _request(self.bridge_api, 'GET', 'activity', params={'collection': collection_pk})

        # merge existing api model instances and objects to be created, using 'source_launch_url' as the id field
        objects_to_apply = _plan_api_actions('source_launch_url', [a.to_bridge_params() for a in activities], existing_activities)

        objects_applied = _apply_api_actions(self.bridge_api, objects_to_apply, 'activity')
        
        return objects_applied


class EngineInitializer:
    def __init__(self, engine_api, collections, activities, knowledge_components, request_delay=0.1):
        """
        Class to initialize data in ALOSI bridge and engine systems
        :param bridge_api: alosi.bridge_api.BridgeApi
        :param engine_api: alosi.engine_api.EngineApi
        :param collections: list of Collection objects
        :param activities: list of Activity objects
        :param knowledge components: list of KnowledgeComponent objects
        :param course_id: course code to use for url construction (e.g. 'HarvardX+SPU30x+2T2018')
        :param prefix: course code to append to kcs, etc, optional
        :param content_source_host: content source host (e.g. https://example.openedx.org)
        :param bridge_owner_pk: pk of the bridge user that owns created collection
            TODO revise if there is a way to avoid pk use
        :param bridge_content_source_pk: pk of the content source to assign to created activity
            TODO revise if there is a way to avoid pk use
        """
        self.engine_api = engine_api

        self.collections = collections
        self.activities = activities
        self.knowledge_components = knowledge_components
