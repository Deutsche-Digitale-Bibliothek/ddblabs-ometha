"""
Installs:
    - ometha
"""

from setuptools import setup
from setuptools import find_packages

setup(
    name='ometha',
    version='1.9.5',
    description='OAI Metadata Harvester',
    long_description="A robust and fast OAI PMH Metadata Harvester with TUI and CLI and extensive logging",
    long_description_content_type='text/markdown',
    author='Karl-Ulrich Krägelin',
    author_email='kraegelin@sub.uni-goettingen.de',
    url='https://gitlab.gwdg.de/maps/ometha',
    license='MIT',
    packages=find_packages(),
    include_package_data=True,
    install_requires=open('requirements.txt').read().split('\n'),
    entry_points={
        'console_scripts': [
            'ometha=ometha:main',
        ]
    }
)