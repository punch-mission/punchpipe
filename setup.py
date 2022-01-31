from setuptools import setup

setup(
    name='punchpipe',
    version='1.0',
    packages=['punchpipe',
              'punchpipe.tests',
              'punchpipe.level0',
              'punchpipe.level0.tests',
              'punchpipe.infrastructure',
              'punchpipe.infrastructure.tests'],
    url='',
    license='',
    author='hughes.jmb',
    author_email='marcus.hughes@swri.org',
    description='PUNCH mission\'s data reduction pipeline'
)
