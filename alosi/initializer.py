import logging
from time import sleep
from json import JSONDecodeError
import pandas as pd
import numpy as np
from alosi.google_drive import export_sheet_to_dataframe


log = logging.getLogger(__name__)


class GoogleSheetDataSource:
    # required sheets (but can provide mapping to these)
    sheet_mapping_keys = ['activity','kc','activity-kc', 'collection', 'prerequisite activity','kc-kc']
    default_sheet_mapping = {
        'activity':'activity',
        'kc':'kc',
        'activity-kc':'activity-kc',
        'collection':'collection',
        'prerequisite activity':'prerequisite activity',
        'kc-kc':'kc-kc',
    }
    # required column names in sheets
    required_columns = {
        'collection': ['collection_id','collection_name'],
        'kc': ['kc_id','kc_name'],
        'activity': ['activity_id','activity_name','difficulty','collection_id'],
        'activity-kc': ['activity_id','kc_id'],
        'prerequisite activities': ['dependent_activity_id','prerequisite_activity_id']
    }
    activity_id_formats = ['location_id',]


    def __init__(self, file_id, credentials, sheet_mapping=None, activity_id_format='location_id', prefix=None):
        """
        :param file_id: file id of google sheet from url
        :param credentials: google-auth credentials object
        :param sheet_mapping: dict with keys: {activity, kc, item-kc, collection} that map to the corresponding
            worksheet label on the google sheet
        :param activity_id_type: indicator what kind of activity id is being used (e.g. full xblock urls vs location ids)
        """
        self.file_id = file_id
        self.credentials = credentials
        self.sheets = sheet_mapping or self.default_sheet_mapping
        self._validate_sheet_mapping(sheet_mapping)  # check sheet mapping validity
        self.activity_id_format = activity_id_format
        self.prefix = prefix

    def _validate_sheet_mapping(self, sheet_mapping):
        """
        Check that all required sheet keys are in sheet mapping dict
        :param sheet_mapping: sheet_mapping argument from init
        :return:
        """
        if not all([key in self.sheet_mapping_keys for key in sheet_mapping]):
            raise ValueError("Unknown sheet key found: {}".format(self.sheet_mapping_keys))
        return sheet_mapping

    @staticmethod
    def _standardize_name(name):
        """
        Returns lowercase version of a string, with spaces replaced with underscores
        :param name: string to clean
        :return: cleaned version of string
        """
        return name.lower().replace(' ','_')


    def get_data(self, sheet_label):
        """
        Retrieve spreadsheet data from a worksheet given the sheet label
        :param sheet_label: data type to retrieve (activity, kc, item-kc, collection)
        :return: dataframe of google sheet data
        """
        df = export_sheet_to_dataframe(
            self.file_id,
            worksheet_title=self.sheets[sheet_label],
            credentials=self.credentials
        )
        df.columns = [self._standardize_name(c) for c in df.columns]
        df = self._add_prefix(df)
        return df

    def _validate_columns(self, df, sheet_type):
        """
        Check if required column ids are in sheets, based on column requirements in self.required_columns
        :return:
        """
        if not all([c in self.columns[sheet_type] for c in df]):
            raise ValueError('Not all required columns found: sheet={}'.format(sheet_type))

    def _add_prefix(self, df):
        """
        Prepend prefix to column values if prefix specified and relevant columns present
        :param df: dataframe
        :return: dataframe with prefix added to relevant column values
        """
        if not self.prefix:
            return df
        if 'kc' in self.prefix and 'kc_id' in df.columns:
            kc_prefix = self.prefix['kc']
            df['kc_id'] = df['kc_id'].apply(lambda x: kc_prefix + x)
        # TODO refactor for less code repetition
        if 'kc' in self.prefix and 'dependent_kc_id' in df.columns:
            kc_prefix = self.prefix['kc']
            df['dependent_kc_id'] = df['dependent_kc_id'].apply(lambda x: kc_prefix + x)
        if 'kc' in self.prefix and 'prerequisite_kc_id' in df.columns:
            kc_prefix = self.prefix['kc']
            df['prerequisite_kc_id'] = df['prerequisite_kc_id'].apply(lambda x: kc_prefix + x)
        if 'collection' in self.prefix and 'collection_id'in df.columns:
            collection_prefix = self.prefix['collection']
            df['collection_id'] = df['collection_id'].apply(lambda x: collection_prefix + x)
        return df


class Initializer:
    PROBLEM = 'problem'
    READING = 'reading'

    def __init__(self, bridge_api, engine_api, google_sheet, course_id=None, prefix=None, content_source_host=None,
                 bridge_user_pk=1, bridge_content_source_pk=1, mastery_prior=0.2):
        """
        Class to initialize data in ALOSI bridge and engine systems
        :param engine_api: alosi.engine_api.EngineApi
        :param bridge_api: alosi.bridge_api.BridgeApi
        :param google_sheet: GoogleSheetDataSource
        :param course_id: course code to use for url construction (e.g. 'HarvardX+SPU30x+2T2018')
        :param prefix: course code to append to kcs, etc, optional
        :param content_source_host: content source host (e.g. https://example.openedx.org)
        :param bridge_owner_pk: pk of the bridge user that owns created collection
            TODO revise if there is a way to avoid pk use
        :param bridge_content_source_pk: pk of the content source to assign to created activity
            TODO revise if there is a way to avoid pk use
        """
        self.bridge_api = bridge_api
        self.engine_api = engine_api
        self.google_sheet = google_sheet
        self.course_id = course_id
        self.prefix = prefix
        self.content_source_host = content_source_host

        self.bridge_user_pk = bridge_user_pk
        self.bridge_content_source_pk = bridge_content_source_pk
        self.mastery_prior = mastery_prior



    def create_collections(self, dry_run=False):
        """
        Scan collection sheet on google doc, and create collections in bridge
        if they don't exist yet, based on the collection_id field
        TODO use of owner pk is not ideal - could use user associated with API token
        :return: created object data
        """
        if dry_run:
            log.info("Dry run (create collections)")
        # get collections that already exist in bridge
        r = self.bridge_api.request('GET', 'collection')
        df_collection_bridge = pd.DataFrame(r.json())


        def collection_exists_in_bridge(collection_id):
            """
            Check whether collection exists in bridge by its collection.
            Assumes df_bridge_collection is in scope - which is a dataframe of results from collection list api endpoint
            """
            # note collection_id field name is "slug" in api
            q = df_collection_bridge.query('slug==@collection_id')
            return False if len(q) == 0 else True

        # create collections that don't already exist in bridge

        created_objects = []  # keep collection data here for created objects

        for collection in self.google_sheet.get_data('collection').itertuples():
            if collection_exists_in_bridge(collection.collection_id):
                continue
            data = {
                'slug': collection.collection_id,
                'name': collection.collection_name,
                'strict_forward': True,
                'owner': self.bridge_user_pk,
            }
            request = self.bridge_api.prepare('POST', 'collection', json=data)
            if dry_run:
                created_objects.append(data)
            else:
                created_object = self._send_request(self.bridge_api, request)
                if created_object:
                    created_objects.append(created_object)

        if not created_objects:
            log.debug("No collections created in bridge")

        return created_objects

    @staticmethod
    def _send_request(api, request):
        r = api.client.send(request)
        sleep(0.1)
        try:
            response_data = r.json()
        except JSONDecodeError:
            # response_data = r.text
            pass
        if r.ok:
            log.info(response_data)
            return response_data
        else:
            log.error(response_data)
            return None

    def _standardize_activity_type(self, activity_type):
        """
        Convert activity type to standard values (either self.PROBLEM or self.READING)
        :param activity_type: e.g. Question, problem, Reading, html
        :return: cleaned string (either self.PROBLEM or self.READING)
        """
        mapping = {
            'question':self.PROBLEM,
            'problem':self.PROBLEM,
            'reading':self.READING,
            'html':self.READING
        }
        return mapping[activity_type.lower()]

    def _construct_lti_provider_url(self, location_id, activity_type):
        """
        Construct the lti provider url for an activity, based on activity metadata
        :param location_id: location id for activity (e.g. aade69e87a1c4a5fab10616157cbae5c)
        :param activity_type: type of activity (e.g. Question, problem, Reading, html)
        :return: lti provider url / source launch url (e.g. https://example.com/lti_provider/courses/course-v1:HarvardX+SPU30x+2T2018/block-v1:HarvardX+SPU30x+2T2018+type@problem+block@aade69e87a1c4a5fab10616157cbae5c)
        """
        # mapping from possible cleaned activity_type input values to corresponding values to use in url
        mapping = {
            self.READING: 'html+block',
            self.PROBLEM: 'problem+block'
        }

        block_type = mapping[self._standardize_activity_type(activity_type)]
        if not self.content_source_host:
            raise ValueError("Invalid content source host")

        return "{}/lti_provider/courses/course-v1:{}/block-v1:{}+type@{}@{}".format(
            self.content_source_host, self.course_id, self.course_id, block_type, location_id
        )

    def create_activities(self, dry_run=False):
        """
        Scan activity sheet on google doc and create activities that don't exist
        Validate collections associated with unpopulated activities to warn user
        if there are collection ids that don't yet exist in bridge
        :return: created object data
        """

        # get collection_id - collection_pk mapping from bridge collection data
        df_collection_bridge = pd.DataFrame(self.bridge_api.request('GET','/collection').json())
        df_collection_bridge_prep = (df_collection_bridge
            [['id', 'slug', 'name']]
            .rename(columns={'id': 'collection_pk', 'slug': 'collection_id', 'name': 'collection_name'})
        )

        # get activities currently in bridge so that we know which ones we don't need to create again
        r = self.bridge_api.request('GET','/activity')
        df_activity_bridge = pd.DataFrame(r.json())

        # create dataframe of bridge activities joined with collection_id values from collection table
        # needed by activity_exists_in_bridge_collection()
        df_activity_bridge_prep = (df_activity_bridge
            [['source_launch_url','collection']]
            .merge(df_collection_bridge[['id','slug']], left_on='collection', right_on='id')
        )

        def activity_exists_in_bridge_collection(url, collection_id):
            """
            Check whether activity exists in bridge collection by its url and the particular collection id.
            Assumes df_activity_bridge_prep is available - which is a dataframe of results from activity list api endpoint
            :param url: source_launch_url of the activity to check
            :param collection_id: collection id to check existence of activity in
            :return: True if activity exists else False
            """
            q = df_activity_bridge_prep.query('source_launch_url==@url and slug==@collection_id')
            return False if len(q) == 0 else True

        # activities (from google sheet) to populate
        df_activity_sheet = self.google_sheet.get_data('activity')

        # rest of function assumes activity_id column is in location_id format so check this
        # TODO remove when support for other activity_id_format values implemented
        if not self.google_sheet.activity_id_format == 'location_id':
            raise NotImplementedError("Use with non location_id formatted activity ids not supported yet")

        # openedx lti provider url constructor function to use with pandas apply
        construct_url = lambda x: self._construct_lti_provider_url(x.activity_id, x.activity_type)

        # create dataframe of activities with columns ['name', 'collection_pk', 'collection_id', 'url']
        df_activity_prep = (df_activity_sheet
            [['activity_id', 'activity_name', 'collection_id', 'activity_type']]
            .merge(df_collection_bridge_prep, on='collection_id')
            .assign(url=lambda x: x.apply(construct_url, 1))
            .rename(columns={'activity_name': 'name'})
        )

        # TODO can warn if there are activities referencing a collection that doesn't exist in bridge,
        #   and as a result won't be created

        created_objects = []  # store created object data here
        for activity in df_activity_prep.itertuples(index=False):
            if not activity_exists_in_bridge_collection(activity.url, activity.collection_id):
                data = dict(
                    collection=activity.collection_pk,  # this is the collection pk
                    source_launch_url=activity.url,  # full lti provider url
                    lti_consumer=self.bridge_content_source_pk,  # TODO would like better way to handle this
                    source_name=activity.name,  # name from content source
                    name=activity.name,  # custom name
                    source_context_id=self.course_id,  # TODO to remove when source_context_id field depreciated
                    atype='G',  # G = general, as opposed to questions to pin before/after adaptive portion
                    stype='problem' if self._standardize_activity_type(activity.activity_type)==self.PROBLEM else 'html'
                )
                request = self.bridge_api.prepare('POST', 'activity', json=data)
                if dry_run:
                    created_objects.append(data)
                else:
                    created_object = self._send_request(self.bridge_api, request)
                    if created_object:
                        created_objects.append(created_object)

        if not created_objects:
            log.warning("No objects created")

        return created_objects


    def create_knowledge_components(self, dry_run=False):
        """
        Create knowledge components in engine
        :return: created object data
        """
        # optional - append prefix (e.g. course code) to beginning of kc id
        add_prefix = lambda x: "{}_{}".format(self.prefix, x.kc_id) if self.prefix else x.kc_id
        df_kc_sheet = self.google_sheet.get_data('kc')

        # clean up and append prefix / course code to beginning of kc ids
        df_kc_sheet_prep = (df_kc_sheet
            .rename(columns={
                'kc_name': 'name',
            })
            .assign(kc_id=lambda x: x.apply(add_prefix, 1))
            [['kc_id', 'name']]
        )

        # get existing list of kc's in engine
        df_kc_engine = pd.DataFrame(self.engine_api.request('GET','knowledge_component').json())

        # function that checks whether kc with specified kc_id exists in engine
        kc_exists_in_engine = lambda kc_id: True if len(df_kc_engine.query('kc_id==@kc_id'))>0 else False

        created_objects = []

        # create kc in engine if it doesn't already exist
        for kc in df_kc_sheet_prep.itertuples():
            if not kc_exists_in_engine(kc.kc_id):
                data = {
                    'kc_id': kc.kc_id,
                    'name': kc.name,
                    'mastery_prior': self.mastery_prior
                }
                request = self.engine_api.prepare('POST', 'knowledge_component', json=data)
                if dry_run:
                    created_objects.append(data)
                else:
                    created_object = self._send_request(self.engine_api, request)
                    if created_object:
                        created_objects.append(created_object)

        if not created_objects:
            log.warning("No objects created")

        return created_objects


    def update_activity_difficulty(self, dry_run=False):
        """
        Updates difficulty values for activities in engine
        :return: list of updated object data
        """
        DIFFICULTY_MAPPING = {
            'easy':0.25,
            'medium':0.5,
            'advanced':0.75
        }
        difficulty_value = lambda x: DIFFICULTY_MAPPING.get(x.difficulty.lower(), np.nan)

        # openedx lti provider url constructor function to use with pandas apply
        construct_url = lambda x: self._construct_lti_provider_url(x.activity_id, x.activity_type)

        # need difficulty value from activity sheet
        df_activity_sheet = self.google_sheet.get_data('activity')
        df_activity_sheet_prep = (df_activity_sheet
            .assign(url=lambda x: x.apply(construct_url, 1))
            # .rename(columns={'activity_name': 'name'})  # TODO necessary?
            .assign(difficulty=lambda x: x.apply(difficulty_value, 1))
            [['url', 'difficulty']]
        )

        # get activity pks from engine to use to call patch requests to modify activity metadata (KC tagging, difficulty)
        df_activity_engine = pd.DataFrame(self.engine_api.request('GET', 'activity').json())
        df_activity_engine_prep = (df_activity_engine
            .rename(columns={'id': 'activity_pk', 'source_launch_url': 'url'})
            [['url', 'activity_pk']]
        )

        # has columns [activity_pk, url, difficulty]
        df_activity_merged = (df_activity_engine_prep
            .merge(df_activity_sheet_prep, on='url')
        )

        # update difficulty values for activities in engine

        updated_objects = []

        for activity in df_activity_merged.itertuples(index=False):
            if not pd.isnull(activity.difficulty):
                data = {'difficulty': activity.difficulty}
                request = self.engine_api.prepare('PATCH', 'activity/{}'.format(activity.activity_pk), json=data)
                if dry_run:
                    updated_objects.append(dict(activity._asdict()))
                else:
                    updated_object = self._send_request(self.engine_api, request)
                    if updated_object:
                        updated_objects.append(updated_object)

        return updated_objects


    def update_activity_kc_tagging(self, dry_run=False):
        """
        Updates activity kc tagging in engine
        :return: updated object data
        """
        # has columns [url, activity_pk, activity_id]
        df_activity_base = self._get_update_engine_activity_base_df()

        # download activity-kc sheet and add in activity pk and url
        df_activity_kc_sheet = self.google_sheet.get_data('activity-kc')  # has columns kc_id, activity_id
        df_activity_kc_sheet_prep = (df_activity_kc_sheet
            .merge(df_activity_base, on='activity_id')  # get activity_pk and url from this join
            [['url', 'kc_id', 'activity_pk', 'activity_id']]
        )

        # get kc engine pk's from this table
        df_kc_engine = pd.DataFrame(self.engine_api.request('GET', 'knowledge_component').json())
        df_kc_engine_prep = (df_kc_engine
            .rename(columns={'id': 'kc_pk'})
            [['kc_pk', 'kc_id']]
        )

        # find those activities with more than one kc tagging and make array of pk ids

        def get_kc_pks(x):
            """
            Return kc pk array with all kc pks for the activity
            operates on potentially multiple rows corresponding to same activity but different tagging
            assumes kc_id attribute present
            :param x: dataframe row
            :return: array of kc primary keys, e.g. [123, 45]
            """
            return x.kc_pk.tolist()

        # this is a series with index=url and value is an array of kc ids
        s_activity_kcs = (df_activity_kc_sheet_prep
            .merge(df_kc_engine_prep, on='kc_id')  # get kc_pk from this join
            .groupby('activity_id')
            .apply(get_kc_pks)
        )
        s_activity_kcs.name = 'knowledge_components'  # provide a name so corresponding column in joined df has a name

        # activity data table with kc tagging
        # columns: [activity_pk, url, kc_pks]
        df_activity_merged = (df_activity_kc_sheet_prep
            .join(s_activity_kcs, on='activity_id', how='inner')
        )

        # update activity tagging in engine:
        updated_objects = []
        for activity in df_activity_merged.itertuples(index=False):
            data = dict(knowledge_components=activity.knowledge_components)
            request = self.engine_api.prepare('PATCH',"activity/{}".format(activity.activity_pk),json=data)
            if dry_run:
                updated_objects.append(dict(activity._asdict()))
            else:
                updated_object = self._send_request(self.engine_api, request)
                if updated_object:
                    updated_objects.append(updated_object)

        return updated_objects

    def _get_update_engine_activity_base_df(self):
        """
        Get dataframe of activity data with columns [activity_id, url, activity_pk]
        which is useful when updating a metadata field for engine activities
        :return: dataframe
        """
        # openedx lti provider url constructor function to use with pandas apply
        construct_url = lambda x: self._construct_lti_provider_url(x.activity_id, x.activity_type)

        # construct url from activity sheet
        df_activity_sheet = self.google_sheet.get_data('activity')
        df_activity_sheet_prep = (df_activity_sheet
            .assign(url=lambda x: x.apply(construct_url, 1))
            [['activity_id','url']]
        )

        # get activity pks from engine to use to call patch requests to modify activity metadata (KC tagging, difficulty)
        df_activity_engine = pd.DataFrame(self.engine_api.request('GET','activity').json())
        df_activity_engine_prep = (df_activity_engine
            .rename(columns={'id': 'activity_pk', 'source_launch_url': 'url'})
            [['activity_pk', 'url']]
        )

        # merge
        # has columns [activity_pk, url, activity_id]
        df_activity_merged = (df_activity_engine_prep
            .merge(df_activity_sheet_prep, on='url')
        )

        return df_activity_merged


    def update_activity_prerequisites(self, dry_run=False):
        """

        :param dry_run: True for test run, False to modify live bridge/engine
        :return: updated objects
        """
        df_activity_base = self._get_update_engine_activity_base_df()

        # download prereq activity sheet
        df_prereq_activity = self.google_sheet.get_data('prerequisite activity')
        # get engine pks for activities in df_prereq_activity
        df_prereq_activity_prep = (df_prereq_activity
            .merge(df_activity_base[['activity_pk','activity_id']], left_on='dependent_activity_id',right_on='activity_id')
            .rename(columns={'activity_pk':'dependent_activity_pk'})
            .merge(df_activity_base[['activity_pk','activity_id']], left_on='prerequisite_activity_id',right_on='activity_id')
            .rename(columns={'activity_pk':'prerequisite_activity_pk'})
        )

        get_prereq_activity_pks = lambda x: x.prerequisite_activity_pk.tolist()
        s_activity_prereqs = (df_prereq_activity_prep
            .groupby('dependent_activity_pk')
            .apply(get_prereq_activity_pks)
        )
        s_activity_prereqs.name = 'prerequisite_activities'
        s_activity_prereqs.index.name = 'activity_pk'

        # columns: [activity_pk, url, name, difficulty, kc_pks]
        df_activity_merged = (df_activity_base
            .join(s_activity_prereqs, on='activity_pk', how='inner')
        )

        updated_objects = []
        for activity in df_activity_merged.itertuples(index=False):
            data = dict(prerequisite_activities=activity.prerequisite_activities)
            request = self.engine_api.prepare('PATCH',"activity/{}".format(activity.activity_pk),json=data)
            if dry_run:
                updated_objects.append(dict(activity._asdict()))
            else:
                updated_object = self._send_request(self.engine_api, request)
                if updated_object:
                    updated_objects.append(updated_object)

        return updated_objects

    def create_kc_prerequisites(self, dry_run=False):
        df_kc_prereqs = self.google_sheet.get_data('kc-kc')

        # get mapping between kc id and pk
        # get kc engine pk's from this table
        df_kc_engine = pd.DataFrame(self.engine_api.request('GET', 'knowledge_component').json())
        df_kc_engine_prep = (df_kc_engine
            .rename(columns={'id': 'kc_pk'})
            [['kc_pk', 'kc_id']]
        )

        # mapping between connection strength category and numeric value
        CONNECTION_STRENGTH_MAPPING = {
            'strong': 1.0,
            'weak': 0.5,
        }

        strength_value = lambda x: CONNECTION_STRENGTH_MAPPING.get(x.connection_strength.lower(), np.nan)

        # merge in pks with kc-kc data
        df_kc_prereqs_merged = (df_kc_prereqs
            .merge(df_kc_engine_prep, left_on='dependent_kc_id',right_on='kc_id')
            .rename(columns={'kc_pk':'dependent_kc_pk'})
            .merge(df_kc_engine_prep, left_on='prerequisite_kc_id', right_on='kc_id')
            .rename(columns={'kc_pk':'prerequisite_kc_pk'})
            .assign(connection_strength=lambda x: x.apply(strength_value, 1))
            [['dependent_kc_pk','prerequisite_kc_pk','connection_strength']]
        )

        # create prereqs

        created_objects = []
        for kc_relation in df_kc_prereqs_merged.itertuples(index=False):
            data = dict(
                prerequisite=kc_relation.prerequisite_kc_pk,
                knowledge_component=kc_relation.dependent_kc_pk,
                value=kc_relation.connection_strength,
            )
            request = self.engine_api.prepare('POST',"prerequisite_knowledge_component",json=data)
            if dry_run:
                created_objects.append(dict(kc_relation._asdict()))
            else:
                created_object = self._send_request(self.engine_api, request)
                if created_object:
                    created_objects.append(created_object)

        return created_objects
