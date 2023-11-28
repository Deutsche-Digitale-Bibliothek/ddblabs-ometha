"""
Installs:
    - ometha
"""

from setuptools import find_packages, setup

from ometha._version import __version__

setup(
    name="ometha",
    version=__version__,
    description="OAI Metadata Harvester",
    long_description="A robust and fast OAI PMH Metadata Harvester with TUI and CLI and extensive logging",
    long_description_content_type="text/markdown",
    author="Karl-Ulrich Krägelin",
    author_email="karlkraegelin@outlook.com",
    url="https://gitlab.gwdg.de/maps/ometha",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=open("requirements.txt").read().split("\n"),
    entry_points={
        "console_scripts": [
            "ometha=ometha.main:start_process",
        ]
    },
)
