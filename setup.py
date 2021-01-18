import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bcpyaz",
    version="0.2.0",
    author="Arcturus",
    author_email="dan@arcturus.io",
    description="Microsoft SQL Server bcp (Bulk Copy) wrapper with Azure Synapse Blob alternative",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arcturus-io/bcpyaz",
    packages=setuptools.find_packages(),
    keywords="bcp mssql",
    classifiers=[
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: SQL",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
