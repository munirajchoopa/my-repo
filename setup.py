from setuptools import setup,find_packages

setup(
    name='hellofresh',
    version='1.0',
    packages=find_packages(),
    package_data = {
        # If any package contains *.json files, include them:
        '': ['*.json','*.logs']
    },
    url='',
    license='',
    author='mchoop',
    author_email='',
    description='hello fresh data analytics'
)
