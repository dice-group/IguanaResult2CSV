from pkg_resources import parse_requirements
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='iguanaresult2csv',
    version='3.2.0',
    description='Convert IGUANA <https://github.com/dice-group/IGUANA> RDF result files to CSV.',
    long_description=readme,
    author='Alexander Bigerl',
    author_email='info@dice-research.org',
    url='https://github.com/dice-group/IguanaResult2CSV',
    license=license,
    packages=find_packages(),
    include_package_data=True,
    package_data={'iguanaresult2csv': ['sparql/*.sparql']},
    install_requires=[
        "rdflib>=5.0.0",
        "python-dateutil>=2.8.1",
        "click>=7.1.2",
        "requests>=2.25.1"
    ],
    entry_points='''
        [console_scripts]
        iguanaresult2csv=iguanaresult2csv.exec.run:cli
    '''
)
