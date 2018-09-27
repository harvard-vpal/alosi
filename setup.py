from setuptools import setup

setup(
    name='alosi',
    version='1.2.1',
    description='Utilities for the ALOSI adaptive learning system',
    url='https://github.com/harvard-vpal/alosi',
    author='Andrew Ang',
    author_email='andrew_ang@harvard.edu',
    license='Apache-2.0',
    packages=['alosi'],
    install_requires=[
        'requests',
        'numpy',
        'pandas',
        'lxml',
        'google-auth>=1.5.0',
        'google-auth-oauthlib>=0.2.0',
        'gspread>=3.0.0',
    ]
)
