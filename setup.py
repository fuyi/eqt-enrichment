from setuptools import setup

setup(
    name="eqt-etl",
    version="0.1",
    author="yvesfu@gmail.com",
    install_requires=[
        "pyspark==3.0.1",
        "Scrapy==2.4.1",
        "click==7.1.2"
    ],
    extras_require={"tests": [
        "pytest==6.2.1",
        "pytest-flake8==1.0.6"
        ]
    },
    entry_points={
        "console_scripts": [
            "eqt_etl = eqt_pipeline:pipeline",
        ]
    },
)
