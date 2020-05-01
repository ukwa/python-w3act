import subprocess
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

def get_version():
    try:
        return subprocess.check_output(['git', 'describe', '--tags', '--always']).strip().decode("utf-8")
    except:
        return "?.?.?"

setup(
    name='python-w3act',
    version=get_version(),
    packages=find_packages(),
    install_requires=requirements,
    package_data={
        # If any package contains *.txt or *.md files, include them:
        '': ['*.txt', '*.md'],
    },
    license='Apache 2.0',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'w3act-api=w3act.api.cmd:main',
            'w3act=w3act.dbc.cmd:main'
        ]
    }
)
