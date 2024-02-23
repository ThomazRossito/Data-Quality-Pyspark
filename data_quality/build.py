# Execute: python build.py bdist_wheel

from setuptools import setup, find_packages

setup(
    name='lib-data-quality',
    version="0.0.1",
    author_email='thomaz.rossito@terra.com.br',
    description='Lib Data Quality',
    install_requires=[
        'setuptools'
    ]
)
