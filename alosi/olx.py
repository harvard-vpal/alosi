import os
import tarfile
import shutil
from lxml import etree


class Node:
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
    node_type = 'chapter'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def sequentials(self):
        return self.children


class Sequential(Node):
    node_type = 'sequential'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def verticals(self):
        return self.children


class Vertical(Node):
    node_type = 'vertical'

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)

    @property
    def components(self):
        return self.children


class Component(Node):
    """
    Base class for component types, e.g. problem
    """

    def __init__(self, display_name, url_name=None):
        super().__init__(display_name, url_name)


class Problem(Component):
    node_type = 'problem'

    def __init__(self, display_name, url_name=None, body=None, options=None, correct_option=None, explanation=None):
        super().__init__(display_name, url_name)
        self.children = None
        self.body = body
        self.options = options
        self.correct_option = correct_option
        self.explanation = explanation

    def to_xml(self):
        """
        Override base to_xml() method
        {body, options, correct_option} are required
        display_name, explanation are optional
        :param correct_option: 0-index of the correct answer choice in 'options' list
        :rtype: etree.ElementTree
        """
        # component properties
        root = etree.Element('problem', display_name=str(self.display_name), markdown='null')
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

    def build_export_from_template(self, template=None, output_filename=None):
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
        parser = etree.XMLParser(
            remove_blank_text=True)  # https://lxml.de/FAQ.html#why-doesn-t-the-pretty-print-option-reformat-my-xml-output
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
