from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='punchpipe',
    version='1.0',
    package_dir={"": "punchpipe"},
    packages=find_packages(where="punchpipe"),
    python_requires=">=3.6, <4",
    url='',
    license='',
    author='hughes.jmb',
    author_email='marcus.hughes@swri.org',
    description='PUNCH mission\'s data reduction pipeline',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        'controlsegment',
        'sqlalchemy',
        'numpy',
        'astropy',
        'sunpy',
        'pandas',
        'scikit-image',
        'lmfit'],
    extras_require={
            'dev': [],
            'test': ['pytest', 'coverage'],
        },
)
