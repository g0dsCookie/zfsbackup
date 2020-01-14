#!/usr/bin/env python3
import codecs
import os
import re
from setuptools import setup, find_packages


def abspath(*args):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), *args)


def get_contents(*args):
    with codecs.open(abspath(*args), "r", "utf-8") as handle:
        return handle.read()


contents = get_contents("zfs_backup", "__init__.py")
metadata = dict(re.findall(r'__([a-z]+)__\s+=\s+[\'"]([^\'"]+)', contents))


setup(
    name="zfs_backup",
    version=metadata["version"],
    description=metadata["desc"],
    author=metadata["author"],
    packages=find_packages(),
    entry_points=dict(console_scripts=[
        "zfs_backup = zfs_backup.cli:main"
    ]),
    install_required=[
        "humanfriendly"
    ]
)
