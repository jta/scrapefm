options = { 
    # Maximum number of friends to collect per user. The last.fm API
    # can only retrieve up to 200 friends at a time, so this value 
    # determines how long we spend scraping a user.
    'maxfriends': 1000,

    # Delay between requests. Recommended is 1s.
    # Requires pylast from https://github.com/jta/pylast.git
    # 'delay': 0

    # Export friendship relationships. If skipped, we only query friend 
    # list in order to populate random walk.
    'do_connect': False,

    # Dates for which to crawl data. Datematch provides the desired
    # period in string format, while datefmt describes how that format
    # should be converted to the native date representation.
    'datematch': "2013-01",
    'datefmt': "%Y-%m",

    # Username to start random walk at.
    'userseed': 'RJ',

    # Maximum number of users to scrape.
    'limit': 1000,

    # Seed for random, used for selecting next user in random walk.
    'numseed': 666 
}


