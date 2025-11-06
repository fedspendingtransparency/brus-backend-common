"""
Allowing the scripts, models, and libraries to be imported into other apps (data-act-broker-backend, usaspending-api)
"""

from setuptools import setup, find_packages

setup(
    name="brus_backend_common",
    version="0.0.1",
    packages=find_packages(),
    author="fedspendingtransparency",
    description="A shared package of scripts, models, and libraries to be shared across fedspendingtransparency repos",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pyspark>=3.5",
        "pandas>=2.1.4",
        "deltalake==1.1.4",
        "polars==1.34.0",
    ],
)
