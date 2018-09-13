import os
import tarfile
import shutil
from lxml import etree
from alosi.google_drive import export_sheet_to_dataframe
from pandas import Categorical


class Node:
    """
    Base class for olx nodes
    Subclass this and specify the 'node_type' class attribute in the subclass
    """
    def __init__(self, display_name, url_name=None):
        self.display_name = display_name
        self.url_name = url_name
        self.children = []

    def to_xml(self):
        """
        :rtype: etree.ElementTree
        """
        root = etree.Element(self.node_type, display_name=str(self.display_name))
        for child in self.children:
            child_node = etree.SubElement(root, child.node_type, url_name=child.url_name)
        return etree.ElementTree(root)


class Chapter(Node):
    """
    OLX chapter node. A Chapter contains multiple Sequentials.
    """
    node_type = 'chapter'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def sequentials(self):
        return self.children


class Sequential(Node):
    """
    OLX sequential node. A Sequential contains multiple Verticals.
    """
    node_type = 'sequential'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def verticals(self):
        return self.children


class Vertical(Node):
    """
    OLX vertical node. A Vertical contains multiple Components.
    """
    node_type = 'vertical'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def components(self):
        return self.children


class Component(Node):
    """
    Base class for OLX component node, e.g. problem
    """

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)


class Problem(Component):
    """
    OLX problem node. A Problem is a type of Component.
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
        self.children = None
        self.body = body
        self.options = options
        self.correct_option = correct_option
        self.explanation = explanation
        self.max_attempts = max_attempts

    def to_xml(self):
        """
        Override base to_xml() method
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


class Course:
    """
    OLX course. A course contains chapters
    """
    def __init__(self, template=None):
        self.children = []
        self.template = template  # path to tar.gz of empty course export

    @property
    def chapters(self):
        return self.children

    def components(self):
        for chapter in self.chapters:
            for sequential in chapter.sequentials:
                for vertical in sequential.verticals:
                    for component in vertical.components:
                        yield component

    def build_export_from_template(self, output_filename=None, template=None):
        """
        Creates assets in chapter, sequential, vertical, problem folders
        Creates folders if needed, overwrites items with same url_name
        Assumes self.chapters is populated (with sequential, vertical etc nested)
        """
        TEMP_DIR = 'tmp'
        template = template or self.template
        if template is None:
            raise NotImplementedError  # TODO support for creating blank template if needed

        # extract export
        with tarfile.open(template) as f:
            f.extractall(TEMP_DIR)

        # operate on temp dir contents

        # ensure required folders are present
        for folder in ['chapter', 'sequential', 'vertical', 'problem']:
            os.makedirs(os.path.join(TEMP_DIR, 'course', folder), exist_ok=True)

        # build chapters
        for chapter in self.chapters:
            chapter.to_xml().write("{}/course/chapter/{}.xml".format(TEMP_DIR, chapter.url_name), encoding="utf-8",
                                   pretty_print=True)

            # create nested sequentials
            for sequential in chapter.sequentials:
                sequential.to_xml().write("{}/course/sequential/{}.xml".format(TEMP_DIR, sequential.url_name),
                                          encoding="utf-8", pretty_print=True)

                # create nested verticals
                for vertical in sequential.verticals:
                    vertical.to_xml().write("{}/course/vertical/{}.xml".format(TEMP_DIR, vertical.url_name),
                                            encoding="utf-8", pretty_print=True)

                    # create nested components
                    for problem in vertical.components:
                        problem.to_xml().write("{}/course/problem/{}.xml".format(TEMP_DIR, problem.url_name),
                                               encoding="utf-8", pretty_print=True)

        # modify course/course.xml to include chapter references
        # https://lxml.de/FAQ.html#why-doesn-t-the-pretty-print-option-reformat-my-xml-output
        parser = etree.XMLParser(remove_blank_text=True)
        course_etree = etree.parse("{}/course/course/course.xml".format(TEMP_DIR), parser)
        course_etree = self._add_chapters(course_etree)
        course_etree.write("{}/course/course/course.xml".format(TEMP_DIR), encoding="utf-8", pretty_print=True)

        # determine new archive name
        if not output_filename:
            base_filename = template.partition('.tar.gz')[0]
            output_filename = "{}_modified.tar.gz".format(base_filename)

        # create archive
        self._make_tarfile(output_filename, "{}/course".format(TEMP_DIR))

        # clean up tmp dir
        shutil.rmtree(TEMP_DIR)

    def _make_tarfile(self, output_filename, source_dir):
        """
        Create tar.gz archive from folder
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


class OlxCourseBuilder:
    """
    Build Course from google sheet
    """
    default_column_map = {c: c for c in [
        'chapter', 'sequential', 'vertical', 'component', 'choice1', 'choice2', 'choice3', 'choice4', 'correct_choice',
        'question_name', 'explanation', 'body', 'max_attempts',
    ]}
    default_choice_map = dict(a=0, b=1, c=2, d=3)
    # default functions used for creating url name
    url_name = {
        'sequential': lambda chapter, sequential: "{}{}".format(chapter, sequential),
        'vertical': lambda chapter, sequential, vertical: "{}{}{}".format(chapter, sequential, vertical)
    }

    def __init__(self, file_id, credentials, worksheet_title=None, template=None, column_map={}, choice_map={},
                 sort_order=None, url_name=None, defaults={}):
        """
        #TODO validation checks to scan for blank values (e.g. body)
        :param file_id: google sheet file id
        :param credentials: google credential object
        :param worksheet_title: title of google sheet worksheet to get items from
        :param column_map: dict mapping standard column names to corresponding columns in sheet
            valid keys: {chapter, sequential, vertical, component, choice1, choice2, choice3, choice4, correct}
        :param sort_order: dict mapping from standard olx levels to list of values if non-alphabetical sort order desired
            example: dict(colname1 = [val2, val1, val3], colname2 = [v1, v3, v2])
            keys are colnames as they appear in sheet (not necessarily standardized version)
        :param template: name of .tar.gz file to base new course off of
        :param url_name: dict where values are functions that can be used to generate url_names for sequentials or
            verticals based on chapter_lable, sequential_label, vertical_label
            {
                sequential: lambda chapter_label, sequential_label: ...
                vertical: lambda chapter_label, sequential_label, vertical_label: ...
            }
        :param defaults: dict where keys are column names and keys are default value to use, e.g.
            {
                max_attempts: 1
            }

        """
        self.column_map = column_map
        self.choice_map = choice_map
        self.sort_order = sort_order
        self.template = template
        self.worksheet_title = worksheet_title
        self.credentials = credentials
        self.column_map = {**self.default_column_map, **column_map}
        self.choice_map = {**self.default_choice_map, **choice_map}
        self.url_name = {**self.url_name, **url_name}
        self.sheet_df = self.prepare_sheet_df(file_id, credentials, worksheet_title=worksheet_title,
                                              sort_order=sort_order, defaults=defaults)
        self.defaults = defaults
        self.course = self._build_course()

    def prepare_sheet_df(self, file_id, credentials, worksheet_title=None, sort_order={}, defaults={}):
        """
        Download google sheet as dataframe, and standardize column names and values
        :return: dataframe
        """
        df = export_sheet_to_dataframe(file_id, credentials, worksheet_title=worksheet_title)

        # convert columns to categorical if custom sorting provided
        for column_to_sort, sorted_values in sort_order.items():
            df[column_to_sort] = Categorical(df[column_to_sort], sorted_values)

        # populate defaults
        for column, default_value in defaults.items():
            if column not in df.columns:
                df[column] = default_value

        # rename columns to standard names
        df = df.rename(columns={v:k for k,v in self.column_map.items()})

        # convert 'correct' option to numeric index
        df['correct_choice'] = df.correct_choice.apply(lambda x: self.choice_map[x])

        return df

    def export_course(self, output_file):
        """
        Export course to .tar.gz file
        :param output_file: name of .tar.gz file to create, e.g. "mycourse.tar.gz"
        """
        self.course.build_export_from_template(output_file, self.template)

    def _build_course(self):
        """
        Create and populate course object with chapters, sequentials, verticals and components based on google sheet
        :return: populated Course object
        """
        # parse spreadsheet of items and create chapter objects (with nested objects)

        course = Course()

        for chapter_label, chapter_group in self.sheet_df.groupby('chapter'):
            # create a chapter object
            chapter = Chapter(chapter_label, chapter_label)
            for sequential_label, sequential_group in chapter_group.groupby('sequential'):
                if sequential_group.empty: continue  # in case categorical values are used but group is empty
                # create a sequential object
                sequential = Sequential(
                    sequential_label,
                    url_name=self.url_name['sequential'](chapter_label, sequential_label)
                )
                for vertical_label, vertical_group in sequential_group.groupby('vertical'):
                    if vertical_group.empty: continue  # in case categorical values are used but group is empty
                    # create a vertical object
                    vertical = Vertical(
                        vertical_label,
                        url_name=self.url_name['vertical'](chapter_label, sequential_label, vertical_label)
                    )
                    # create problems
                    for row in vertical_group.itertuples():
                        problem = Problem(
                            display_name=row.question_name,
                            url_name=getattr(row, 'component'),
                            body=row.body,
                            options=[row.choice1, row.choice2, row.choice3, row.choice4],
                            correct_option=row.correct_choice,
                            explanation=row.explanation,
                            max_attempts=row.max_attempts,
                        )
                        vertical.components.append(problem)
                    # append vertical to sequential
                    sequential.verticals.append(vertical)
                # append sequential to chapter
                chapter.sequentials.append(sequential)
            # append chapter to course-level object
            course.chapters.append(chapter)

        return course
