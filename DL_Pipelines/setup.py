import setuptools
import os

with open("../README.md", "r") as fh:
    long_description = fh.read()


curr_dir = os.path.dirname(os.path.realpath(__file__))
requirements_path = curr_dir + '/requirements.txt'
install_requires = []
if os.path.isfile(requirements_path):
    with open(requirements_path) as f:
        install_requires = f.read().splitlines()

print(install_requires)

setuptools.setup(
    name="elvis-datalake",
    version="artemis",
    author="Siva Veera",
    author_email="sivaveera@gmail.com",
    description="Elvis Data Lake Project",
    long_description=long_description,
    install_requires=install_requires,
    long_description_content_type="text/markdown",
    url="https://github.com/spark-streaming/tree/development",
    packages=['etl', 'utils'],
    package_dir={'etl': 'src/etl', 'utils': 'src/utils'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
