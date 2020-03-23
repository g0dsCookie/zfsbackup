#!/usr/bin/env python3
import codecs
import os
import re
from setuptools import setup, find_packages


def requirements():
    with open("requirements.txt", "r") as f:
        return [l.rstrip() for l in f.readlines()]


def abspath(*args):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), *args)


def get_contents(*args):
    with codecs.open(abspath(*args), "r", "utf-8") as handle:
        return handle.read()


contents = get_contents("zfsbackup", "__init__.py")
metadata = dict(re.findall(r'__([a-z]+)__\s+=\s+[\'"]([^\'"]+)', contents))


setup(
    name="zfsbackup",
    version=metadata["version"],
    description=metadata["desc"],
    author=metadata["author"],
    packages=find_packages(),
    entry_points=dict(console_scripts=[
        "zfsbackup = zfsbackup.cli:main"
    ]),
    install_required=list(requirements())
)
