scrape.fm
========

scrape.fm is a command line script to collect Last.fm data through the existing API onto local storage.
Currently you can scrape:

- users 
- friendships
- weekly artist charts
- artists
- artist tags

God, why?
========

Armed with this data you can entertain yourself with data mining, recommender systems, social network analysis or simply finding out more about your favourite bands.
I'll have a few projects to showcase the dataset soon.

Requirements
========

scrape.fm currently requires *peewee* and *pylast*. Both can be installed through pip.

You will also require an API key from Last.fm. You can then export it as an environment variable: ::

    $ export LAST_FM_API_KEY = *KEY*

Alternatively, you can provide the key as a command line argument.


Warnings
========

This is not yet properly packaged or documented, proceed with caution.
