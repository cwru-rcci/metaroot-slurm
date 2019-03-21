import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="metarootslurm",
    version="0.5.0",
    author="CWRU HPC Administrators",
    author_email="hpc-support@cwru.edu",
    description="A package for managing a SLURM database through the metaroot framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cwru-rcci/metarootslurm",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3.5",
    ],
    python_requires='~=3.5',
    install_requires=[
        'metaroot'
    ]
)
