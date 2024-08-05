import os
from typing import List

from setuptools import find_packages, setup


def parse_requirements(fname: str) -> List[str]:
    """Loads requirements from a pip requirements file with fname"""
    with open(
        os.path.join(os.path.dirname(__file__), fname), encoding="utf8"
    ) as fhandle:
        reader = fhandle.readlines()
    return [line for line in reader if line and not line.startswith("#")]


setup(
    name="data-augmentation-service",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=parse_requirements("requirements.txt"),
    author="Taha Sadiki",
    author_email="tahasadiki.pro@example.com",
    description="A service for augmenting job posting data with seniority information",
    python_requires=">=3.9",
    entry_points={
        "console_scripts": [
            "data-augmentation-service=main:main",
        ],
    },
)
