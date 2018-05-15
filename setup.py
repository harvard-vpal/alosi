from setuptools import setup

setup(
    name='alosi',
    version='0.0.2',
    description='Adaptive learning engine',
    url='https://github.com/harvard-vpal/alosi',
    author='Andrew Ang',
    author_email='andrew_ang@harvard.edu',
    license='Apache-2.0',
    packages=['alosi'],
    install_requires=[
        'requests==2.18.4',
        'numpy==1.13.1'
    ]
)
