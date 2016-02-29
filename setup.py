from setuptools import setup

setup(
    name="arkimet-tools",
    version="0.3",
    description="Arkimet tools",
    url="https://github.com/edigiacomo/arkimet-tools",
    author="Emanuele Di Giacomo",
    author_email="edigiacomo@arpa.emr.it",
    license="GPLv2+",
    packages=["arkitools"],
    entry_points={
        'console_scripts': ['arkitools-cli=arkitools.cli:main'],
    },
)
