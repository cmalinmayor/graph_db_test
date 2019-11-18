from setuptools import setup

setup(
        name='graph_db_test',
        version='0.1',
        description='Performance testing of spatial graph backends',
        url='https://github.com/cmalinmayor/graph_db_test',
        author='Caroline Malin-Mayor',
        license='MIT',
        packages=[
            'graph_db_test',
            'graph_db_test.mongo',
            'graph_db_test.postgre'
        ],
        install_requires=[
            "pymongo"
        ]
)
