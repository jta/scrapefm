scrape.fm
========

scrape.fm is a command line script to collect Last.fm data through the existing API onto local storage.
Currently you can scrape:

- users 
- friendships
- weekly artist charts
- artists
- artist tags

Armed with this data you can entertain yourself with data mining, recommender systems, social network analysis or simply finding out more about your favourite bands.

Installation
========

You can install scrapefm through the usual method:

    $ python setup.py install

Requirements
========

scrape.fm currently requires `peewee` and `pylast`. Both can installed through pip. Alternatively, both packages are retrieved automatically if you decide to install the package.


Run it
========

To run scrapefm you will first require an API key from Last.fm. You can then export it as an environment variable: ::

    $ export LAST_FM_API_KEY = *KEY*

Alternatively, you can provide the key as a command line argument through the `-k` or `--key` option.

Assuming the key is provided and scrapefm was installed, you can run it directly by providing a file to write the output database to:

    $ scrapefm *DB*

Alternatively, you can run the script directly in `scrapefm/scrapefm.py` with the same command line options.

Configuration
========

Configuration options are set in `config.py`. The recommended way to set configuration options is to copy this file to your preferred directory, adjust the values appropriately, and then have scrapefm load these options through the configuration file option:

    $ scrapefm -c *CONFIG_FILE* *DB*

The configuration file is a python dictionary of key value pairs. Documentation for these keys is provided in the comments of `scrapefm/config.py`.

Warnings
========

This is not yet properly packaged or documented, proceed with caution.

The recommended time between requests for the Last.fm API is 1 second. The current version of the pylast library does not wait between requests. If you are mining data for extended periods, you should import the `pylast` library from [here](https://github.com/jta/pylast.git), and then set the `delay` option in the configuration file to 1.
