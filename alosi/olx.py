import os
import tarfile
import shutil
from types import FunctionType
import tempfile
from lxml import etree
from .google_drive import export_sheet_to_dataframe
from pandas import Categorical
import pandas as pd


etree_write_default_params = dict(encoding="utf-8", pretty_print=True)


def write_xml_to_file(element_tree, target_path, **write_params):
    """
    Write etree.ElementTree to file by relative location within course export directory with default formatting
    :param element_tree: etree.ElementTree object
    :param relative_path: relative location within course export directory (after tmpdir/course/...)
    """
    default_write_params = {'encoding':'utf-8', 'pretty_print':True, **write_params}
    element_tree.write(target_path, **default_write_params)


class Node:
    """
    Base class for olx nodes
    Subclass this and specify the 'node_type' class attribute in the subclass
    """
    def __init__(self, display_name, url_name=None):
        self.display_name = display_name
        self.url_name = url_name


class ParentNode(Node):
    """
    Node that has children. Can be a child of another parent node
    """
    def __init__(self, display_name, url_name=None, children=None):
        super().__init__(display_name, url_name)
        self.children = children or []

    def to_xml(self):
        """
        Generate xml content representing the node
        :rtype: etree.ElementTree
        """
        root = etree.Element(self.node_type, display_name=str(self.display_name))
        for child in self.children:
            child_node = etree.SubElement(root, child.node_type, url_name=str(child.url_name))
        return etree.ElementTree(root)


class Chapter(ParentNode):
    """
    OLX chapter node. A Chapter contains multiple Sequentials.
    """
    node_type = 'chapter'

    @property
    def sequentials(self):
        return self.children


class Sequential(ParentNode):
    """
    OLX sequential node. A Sequential contains multiple Verticals.
    """
    node_type = 'sequential'

    @property
    def verticals(self):
        return self.children


class Vertical(ParentNode):
    """
    OLX vertical node. A Vertical contains multiple Components.
    """
    node_type = 'vertical'

    @property
    def components(self):
        return self.children


class Problem(Node):
    """
    OLX problem node ("component"-level)
    """
    node_type = 'problem'

    def __init__(self, display_name, url_name=None, body=None, options=None, correct_option=None, explanation=None,
                 max_attempts=None):
        """
        :param display_name: component title
        :param url_name: filename
        :param body: problem/question content
        :param options: list of option text content
        :param correct_option: 0-index of the correct answer choice in 'options' list
        :param explanation: explanation text
        :param max_attempts: maximum number of attempts allowed for problem
        """
        super().__init__(display_name, url_name)
        # self.children = None
        self.body = body
        self.options = options
        self.correct_option = correct_option
        self.explanation = explanation
        self.max_attempts = max_attempts

    def to_xml(self):
        """
        Override base to_xml() method
        Build xml problem content
        {body, options, correct_option} are required
        display_name, explanation are optional
        :rtype: etree.ElementTree
        """
        # component properties
        root = etree.Element('problem', display_name=str(self.display_name), markdown='null')
        if self.max_attempts is not None:
            root.set('max_attempts', str(self.max_attempts))
        # question
        question = etree.SubElement(root, 'multiplechoiceresponse')
        # question body
        question.append(self._process_body())
        # choices
        choicegroup = etree.SubElement(question, 'choicegroup')
        for i, option in enumerate(self.options):
            etree.SubElement(choicegroup, 'choice', correct=str(i == self.correct_option)).text = option
        # solution
        solution = etree.SubElement(root, 'solution')
        etree.SubElement(solution, 'p').text = self.explanation
        return etree.ElementTree(root)

    def _process_body(self):
        """
        Prepare text by converting it to an xml subtree, consisting of <p> element with line breaks in original text replaced with <br/> subelements
        :param text: text to prepare
        :rtype: etree.Element
        """
        text = self.body
        e = etree.Element('p')
        lines = [l.strip() for l in text.split('\n')]
        e.text = lines[0]
        for line in lines[1:]:
            br = etree.SubElement(e, 'br')
            br.tail = line  # put text after <br>
        return e


class TemplateComponent(Node):
    """
    Component that generates its xml by rendering a template
    """
    # default parser settings
    default_parser = etree.XMLParser(remove_blank_text=True)

    def __init__(self, display_name, url_name, template, params, node_type='problem'):
        """
        :param display_name: display_name value
        :param url_name: url_name value
        :param template: jinja template for component
        :param params: input variables to render jinja template with
        :param node_type: xml node type
        """
        super().__init__(display_name, url_name)
        self.template = template
        self.params = params
        self.node_type = node_type

    def render_template(self):
        """
        :return: rendered template string
        """
        return self.template.render(self.params)

    def to_xml(self):
        """
        Render template and return problem xml etree
        :return: problem xml
        :rtype: etree.ElementTree
        """
        rendered_template = self.render_template()

        try:
            return etree.ElementTree(etree.fromstring(self.render_template()))
        except Exception as e:
            print(self.render_template())
            raise e

    def to_file(self, target_path):
        """
        Create xml file in course export folder
        Renders template and writes xml data to file
        :param target_path: output location
        :return: None
        """
        write_xml_to_file(self.to_xml(), target_path)


class FileComponent(Node):
    """
    Component that generates its xml by reading from file
    """
    # default parser settings
    default_parser = etree.XMLParser(remove_blank_text=True)

    def __init__(self, source_path, node_type='problem', url_name=None):
        """
        :param filepath: location where source xml file can be found
        """
        self.source_path = source_path
        self.node_type = node_type
        self.url_name = url_name


    def to_xml(self):
        etree.parse(self.source_path, self.default_parser)

    def to_file(self, target_path):
        """
        :param target_path: output location
        :return:
        """
        shutil.copyfile(self.source_path, target_path)


class Course:
    """
    OLX course. A course contains chapters. Also tracks course-level metadata (start time, labels)
    """
    # default options for xml output formatting
    xml_formatting = dict(encoding="utf-8", pretty_print=True)

    def __init__(self, display_name=None, org=None, number=None, start='2030-01-01T00:00:00+00:00',
                 template=None, chapters=[]):
        """
        :param name: course display name; optional only if template is provided
        :param org: organization label (optional since course organization is not editable via import)
        :param number: course number (optional since course organization is not editable via import)
        :param start: start date/time, defaults to 2030-01-01T00:00:00+00:00
        :param template: course export (location of tarball archive) that can be used as the starting point for a new export
        :param chapters: Chapter objects to include in the course
        """
        self.template = template  # path to tar.gz of empty course export
        self.chapters = chapters
        self.display_name = display_name  # display_name for course
        self.start = start
        # org/number of an existing course is immutable so the values here can be arbitrary,
        # but still need to be present since import will not validate otherwise
        self.org = org or 'default'
        self.number = number or 'default'

    def iter_sequentials(self):
        for chapter in self.chapters:
            for sequential in chapter.sequentials:
                yield sequential

    def iter_verticals(self):
        for sequential in self.iter_sequentials():
            for vertical in sequential.verticals:
                yield vertical

    def iter_components(self):
        for vertical in self.iter_verticals():
            for component in vertical.components:
                yield component

    def export(self, output_name=None, template=None, as_tarball=True):
        """
        Creates assets in chapter, sequential, vertical, problem folders
        Creates folders if needed, overwrites items with same url_name
        Assumes self.chapters is populated (with sequential, vertical etc nested)
        """

        # create temporary directory to build course export in
        with tempfile.TemporaryDirectory() as tmpdir:

            template = template or self.template
            if template:
                # extract template in temp directory
                with tarfile.open(template) as f:
                    def is_within_directory(directory, target):
                        
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                    
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        
                        return prefix == abs_directory
                    
                    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                    
                        for member in tar.getmembers():
                            member_path = os.path.join(path, member.name)
                            if not is_within_directory(path, member_path):
                                raise Exception("Attempted Path Traversal in Tar File")
                    
                        tar.extractall(path, members, numeric_owner=numeric_owner) 
                        
                    
                    safe_extract(f, tmpdir)

            else:
                # create base course resources
                self._build_course_base(tmpdir)

            # shortcut for "top-level" olx course directory
            course_dir = f'{tmpdir}/course'

            # ensure required folders are present
            for folder in ['chapter', 'sequential', 'vertical', 'problem']:
                os.makedirs(os.path.join(course_dir, folder), exist_ok=True)

            # build chapters
            for chapter in self.chapters:
                self._write_to_xml(chapter.to_xml(), f'{course_dir}/chapter/{chapter.url_name}.xml')

                # create nested sequentials
                for sequential in chapter.sequentials:
                    self._write_to_xml(sequential.to_xml(), f'{course_dir}/sequential/{sequential.url_name}.xml')

                    # create nested verticals
                    for vertical in sequential.verticals:
                        self._write_to_xml(vertical.to_xml(), f'{course_dir}/vertical/{vertical.url_name}.xml')

                        # create nested components
                        for problem in vertical.components:
                            problem.to_file(f'{course_dir}/problem/{problem.url_name}.xml')

            # modify course/course.xml to include chapter references
            # https://lxml.de/FAQ.html#why-doesn-t-the-pretty-print-option-reformat-my-xml-output
            parser = etree.XMLParser(remove_blank_text=True)
            course_etree = etree.parse("{}/course/course/course.xml".format(tmpdir), parser)
            course_etree = self._add_chapters(course_etree)
            course_etree.write("{}/course/course/course.xml".format(tmpdir), **self.xml_formatting)

            if as_tarball:
                # determine new archive name - append .tar.gz if not already part of target name
                if not output_name:
                    base_filename = template.partition('.tar.gz')[0]
                    output_filename = "{}_modified.tar.gz".format(base_filename)
                else:
                    output_filename = output_name if output_name.endswith('.tar.gz') else f"{output_name}.tar.gz"

                # create archive
                self._make_tarfile(output_filename, "{}/course".format(tmpdir))

            # move contents from temp dir to target folder
            else:
                # if target already exists, remove before writing (shutil.move doesn't overwrite)
                if os.path.isdir(output_name):
                    shutil.rmtree(output_name)
                shutil.copytree(tmpdir, output_name)  # shutil works better for docker-mounted volumes


    @staticmethod
    def _write_to_xml(element_tree, target_path, **kwargs):
        """
        Write etree.ElementTree to file by relative location within course export directory with default formatting
        :param element_tree: etree.ElementTree object
        :param target_path: location to write file to
        :param kwargs: keyword args to pass to ElementTree.write()
        """
        write_params = {'encoding':'utf-8', 'pretty_print':True, **kwargs}
        element_tree.write(target_path, **write_params)

    def _make_tarfile(self, output_filename, source_dir):
        """
        Create tar.gz archive from specified source folder
        :param output_filename: name of archive file to create
        :param source_dir: source directory to create archive from
        """
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    def _add_chapters(self, course_etree):
        """
        Add chapter references to course/course.xml
        Currently adds all chapters in self.chapters without checking for existing chapters with conflicting url_name's
        :param course_etree: etree.ElementTree of course/course.xml
        """
        root = course_etree.getroot()
        for i, chapter in enumerate(self.chapters):
            root.insert(i, etree.Element('chapter', url_name=chapter.url_name))
        return etree.ElementTree(root)

    def _build_course_base(self, tmpdir):
        """
        Create the course-level olx resources, i.e. course.xml, and course/course.xml.
        Used if no base export template is provided.
        :param tmpdir: directory to create resources in
        :return:
        """
        course_dir = os.path.join(tmpdir, 'course')
        os.makedirs(os.path.join(course_dir, 'course'))
        self._write_to_xml(self._build_top_level_course_xml(), os.path.join(course_dir, 'course.xml'))
        self._write_to_xml(self._build_base_course_xml(), os.path.join(course_dir, 'course/course.xml'))

    def _build_top_level_course_xml(self):
        """
        Generate the xml for the course.xml located in the top-level directory
        Only contains org and course code.
        :return: ElementTree for top-level course.xml
        """
        root = etree.Element('course', url_name='course', org=self.org, course=self.number)
        return etree.ElementTree(root)

    def _build_base_course_xml(self):

        """
        Generate the xml for the course/course.xml file
        Contains course config like advanced modules, lti passport config, start time/date
        Chapter references will go in this file, but are added outside of this method.
        :return: ElementTree for detailed course.xml
        """
        # component properties
        root = etree.Element('course', language='en', display_name=self.display_name, start=self.start)
        return etree.ElementTree(root)


class SheetSource:
    """
    Data source with info about components (possibly content) and how to organize them in export output
    """
    def __init__(self, df, levels={}):
        """
        Download google sheet as dataframe, and standardize column names and values
        :param df: pandas dataframe
        :param levels: dict mapping standard olx heirarchy levels [chapter, sequential, vertical, component]
            to corresponding column names in sheet. e.g. dict(chapter=part, sequential=lesson, ... )
        """
        self.levels = levels
        self.df = self.prepare_sheet_df(df)

    @classmethod
    def from_google_sheet(cls, file_id, credentials, worksheet_title=None, **kwargs):
        """
        Initialize from google sheet
        :param file_id: google sheet file id
        :param credentials: google credential object
        :param worksheet_title: title of google sheet worksheet to get items from
        :param kwargs: additional kwargs (e.g. level) to pass to SheetSource constructor
        :return: SheetSource instance
        """
        df = export_sheet_to_dataframe(file_id, credentials, worksheet_title)
        return cls(df, **kwargs)

    @classmethod
    def from_csv(cls, filename, **kwargs):
        """
        Initialize from local csv file
        :param filename: path to local csv file
        :param kwargs:
        :return:
        """
        return cls(pd.read_csv(filename), **kwargs)

    def prepare_sheet_df(self, df, sort_order={}, defaults={}):
        """
        Clean dataframe, apply any custom column transforms or renaming
        :return: dataframe
        """
        # convert columns to categorical if custom sorting provided
        for column_to_sort, sorted_values in sort_order.items():
            df[column_to_sort] = Categorical(df[column_to_sort], sorted_values)

        # populate defaults
        for column, default_value in defaults.items():
            if column not in df.columns:
                df[column] = default_value

        # build standard level columns (chapter/sequential/...)
        # apply callables passed in via levels parameter, to rename columns or apply transforms
        df = df.assign(**self.levels)

        return df


class OlxCourseBuilder:
    """
    Build Course (including Chapters, Sequentials, Verticals, Components) from google sheet
    Fields in table are mapped to node attributes
    """
    default_column_map = {c: c for c in [
        'chapter', 'sequential', 'vertical', 'component', 'question_name',
    ]}
    # default_choice_map = dict(a=0, b=1, c=2, d=3)
    # default functions used for creating url name
    url_name = {
        'chapter': lambda chapter: f'{chapter}',
        'sequential': lambda chapter, sequential: f'{chapter}{sequential}',
        'vertical': lambda chapter, sequential, vertical: f'{chapter}{sequential}{vertical}'
    }

    def __init__(self, data_source, component_factory, course_params=None,
                 sort_order={}, url_name=None, template=None):
        """
        #TODO validation checks to scan for blank values (e.g. body)
        :param data: data source object (e.g. GoogleSheetSource)
        :param component_builder: callable that takes as input spreadsheet row and returns a Component instance
        :param course_params: course-level info (i.e. display_name, start date), required if no template

            example: dict(colname1 = [val2, val1, val3], colname2 = [v1, v3, v2])
            keys are colnames as they appear in sheet (not necessarily standardized version)
        :param template: name of .tar.gz file to base new course off of
        :param url_name: optional dict where values are functions that can be used to generate url_names for sequentials or
            verticals based on chapter_label, sequential_label, vertical_label
            {
                sequential: lambda chapter_label, sequential_label: ...
                vertical: lambda chapter_label, sequential_label, vertical_label: ...
            }
        :param defaults: dict where keys are column names and keys are default value to use, e.g.
            {
                max_attempts: 1
            }
        :param sort_order: dict mapping from standard olx levels to list of values if non-alphabetical sort order desired
        :param worksheet_title: worksheet title in google sheet

        """
        self.df = data_source.df
        self.component_factory = component_factory
        self.sort_order = sort_order
        self.url_name = {**self.url_name, **url_name}
        self.course_params = course_params
        self.template = template  # course export template

        # arg validation
        if not course_params:
            if not template:
                raise ValueError("Template argument required if no course metadata provided")

    def to_course(self):
        """
        Create and populate course object with chapters, sequentials, verticals and components based on google sheet
        :return: populated Course object
        """
        # parse spreadsheet of items and create chapter objects (with nested objects)
        course = Course(**self.course_params)

        for chapter_label, chapter_group in self.df.groupby('chapter'):
            # create a chapter object
            chapter = Chapter(
                chapter_label,
                url_name=self.url_name['chapter'](chapter_label)
            )
            for sequential_label, sequential_group in chapter_group.groupby('sequential'):
                if sequential_group.empty: continue  # in case categorical values are used but group is empty
                # create a sequential object
                sequential = Sequential(
                    sequential_label,
                    url_name=self.url_name['sequential'](chapter_label, sequential_label)
                )
                # print(f"Created sequential object: {sequential}")
                for vertical_label, vertical_group in sequential_group.groupby('vertical'):
                    if vertical_group.empty: continue  # in case categorical values are used but group is empty
                    # create a vertical object
                    vertical = Vertical(
                        vertical_label,
                        url_name=self.url_name['vertical'](chapter_label, sequential_label, vertical_label)
                    )
                    # create problems
                    for row in vertical_group.itertuples():
                        # create Problem instance using the component factory
                        problem = self.component_factory(row)
                        vertical.components.append(problem)
                    # append vertical to sequential
                    sequential.verticals.append(vertical)
                # append sequential to chapter
                chapter.sequentials.append(sequential)
            # append chapter to course-level object
            course.chapters.append(chapter)
        return course
