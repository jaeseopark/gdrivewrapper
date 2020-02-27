import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gdrivewrapper",
    version="VERSION_SUBSTITUTION",
    author="Jaeseo Park",
    description="A wrapper around Google Drive SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jaeseopark/gdrivewrapper",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3",
    install_requires=["google-api-python-client", "oauth2client"]
)
