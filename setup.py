# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

with open("VERSION", "r") as f:
    VERSION = f.read().strip("\n")

with open("requirements.txt", "r") as f:
    requirements = []
    for line in f.readlines():
        if len(line) == 0 or line[0] == "#" or "://" in line:
            continue
        requirements.append(line.strip())

with open("test-requirements.txt", "r") as f:
    test_requirements = []
    for line in f.readlines():
        if len(line) == 0 or line[0] == "#" or "://" in line:
            continue
        test_requirements.append(line)

setup(
    name="kafka-agent",
    version=VERSION,
    long_description=(open("README.md").read() + "\n" + open("CHANGELOG.md").read()),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    url="https://github.com/atlasense/kafka-topic-schema",
    license="Private License",
    setup_requires=["pytest-runner"],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages("src", exclude=["integration_tests", "tests"]),
    package_dir={"": "src"},
    install_requires=requirements,
    extras_require={"test": test_requirements},
    entry_points={"console_scripts": ["kafka-agent = core.commands:run"]},
)
