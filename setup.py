# -*- coding: utf-8 -*-
import setuptools
import scrapefm

setuptools.setup(
    name='scrapefm',
    version= scrapefm.__version__,
    author='João Taveira Araújo ',
    author_email='joao.taveira@gmail.com',
    packages=['scrapefm'],
    url='https://github.com/jta/scrape.fm',
    license='LICENSE.txt',
    description='A Last.fm scraper.',
    long_description=open('README.md').read(),
    install_requires=[
        "pylast >= 0.5.11",
        "peewee == 2.0.7",
    ],
    entry_points={
        'console_scripts': [
            'scrapefm = scrapefm.scrapefm:main',
        ]
    },
)
