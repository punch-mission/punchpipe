from setuptools import setup, find_packages
import pathlib

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
        'prefect',
        'pymysql',
        'pydantic==2.4.2',
        'sqlalchemy',
        'punchbowl',
        'datapane',
        'plotly',
        'pyyaml'],
    extras_require={
            'dev': [],
            'test': ['pytest', 'coverage', 'pytest-mock-resources[mysql]', 'freezegun'],
            'docs': ['sphinx', 'sphinx-rtd-theme', 'sphinx-automodapi']
        },
)
