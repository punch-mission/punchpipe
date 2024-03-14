import pathlib

from setuptools import find_packages, setup

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='punchpipe',
    version='0.0.1',
    # package_dir={"": "punchpipe"},
    packages=find_packages(),
    python_requires=">=3.7, <4",
    url='',
    license='',
    author='PUNCH SOC',
    author_email='marcus.hughes@swri.org',
    description='PUNCH mission\'s data reduction pipeline',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        'punchbowl @ git+ssh://git@github.com/punch-mission/punchbowl@main',
        'prefect',
        'pymysql',
        'pandas',
        'pydantic==1.10.12',
        'sqlalchemy',
        'dash',
        'coolname',
        'numpy',
        'plotly',
        'pyyaml'],
    extras_require={
            'dev': ["flake8",
                    'pre-commit',
                    'hypothesis',
                    'pytest',
                    'coverage',
                    'pytest-cov',
                    'pytest-mock-resources[mysql]',
                    'freezegun'],
            'docs': ['sphinx',
                     'pydata-sphinx-theme',
                     'sphinx-autoapi',
                     'sphinx-favicon',
                     'sphinxcontrib-mermaid',
                     'sphinx-automodapi']
        },
)
